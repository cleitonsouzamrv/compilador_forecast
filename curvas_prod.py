# curvas_prod.py
import io
import re
import unicodedata
from typing import Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st

# ==========================
# Config / Const
# ==========================
MAX_FILES = 300
CURVE_REQUIRED_COLS = [
    "IdEmpreendimento",
    "IdModulo",
    "DataReferencia",
    "VPCurva",
    "PesoModulo",
    "Unidades",
    "VPModulo",
    "VPObra",
    "Obra",
    "SimulacaoId",
]

# ==========================
# Utils
# ==========================
def _parse_number_br(value):
    """Converte string numérica PT-BR/EN p/ float (sem %)."""
    if pd.isna(value):
        return pd.NA
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if s == "":
        return pd.NA
    s = re.sub(r"[^0-9,.\-]", "", s)
    if s in {"", "-", ",", "."}:
        return pd.NA
    last_comma = s.rfind(",")
    last_dot = s.rfind(".")
    if last_comma == -1 and last_dot == -1:
        try:
            return float(s)
        except Exception:
            return pd.NA
    dec = "," if last_comma > last_dot else "."
    parts = s.rsplit(dec, 1)
    int_part = re.sub(r"[^0-9\-]", "", parts[0])
    frac_part = re.sub(r"[^0-9]", "", parts[1]) if len(parts) == 2 else ""
    num_str = int_part + ("." + frac_part if frac_part else "")
    try:
        return float(num_str)
    except Exception:
        return pd.NA


def _parse_number_br_pct(value):
    """Converte '1,01%' -> 0.0101; se não tiver %, usa _parse_number_br."""
    if pd.isna(value):
        return pd.NA
    s = str(value).strip()
    has_pct = "%" in s
    val = _parse_number_br(s)
    if pd.isna(val):
        return pd.NA
    return val / 100.0 if has_pct else val


def _formatar_data_referencia(data_str: str) -> str:
    """Converte formatos como '2026-05', '2026-5' para '01/05/2026'."""
    if pd.isna(data_str):
        return ""
    data_str = str(data_str).strip()
    padrao_ano_mes = re.match(r"^(\d{4})-(\d{1,2})$", data_str)
    if padrao_ano_mes:
        ano = padrao_ano_mes.group(1)
        mes = padrao_ano_mes.group(2).zfill(2)
        return f"01/{mes}/{ano}"
    return data_str


def _padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={c: c.strip() for c in df.columns})


def _deduzir_chaves_curva(df: pd.DataFrame) -> List[str]:
    """
    Define chaves de agrupamento que identificam UMA CURVA.
    Preferência: IdEmpreendimento + Obra + SimulacaoId.
    """
    candidatos = [
        ["IdEmpreendimento", "Obra", "SimulacaoId"],
        ["IdEmpreendimento", "SimulacaoId"],
        ["IdEmpreendimento", "Obra"],
        ["IdEmpreendimento"],
    ]
    for keys in candidatos:
        if all(k in df.columns for k in keys):
            return keys
    # Fallback: qualquer coluna disponível intersectando os requeridos
    return [c for c in ["IdEmpreendimento", "Obra", "SimulacaoId"] if c in df.columns] or ["IdEmpreendimento"]


def _ajustar_vp_obra_com_3_decimais(df: pd.DataFrame, group_keys: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Ajusta VPObra para garantir que cada CURVA some 100% (1.000) com 3 casas.
    Curva ≈ grupo de (IdEmpreendimento, Obra, SimulacaoId) quando disponíveis.
    """
    if df.empty:
        return df
    df_ajustado = df.copy()

    keys = group_keys or _deduzir_chaves_curva(df_ajustado)
    # Comentário (por que): garantir 100% por curva; se existirem 2 curvas da mesma obra, cada uma fecha 100%.
    grupos = df_ajustado.groupby(keys, dropna=False, sort=False)

    for _, grupo in grupos:
        idxs = grupo.index.tolist()
        vals = [float(v) if pd.notna(v) else 0.0 for v in grupo["VPObra"].values]

        # Arredondar primeiro
        vals = [round(v, 3) for v in vals]
        soma_atual = sum(vals)

        # Correção
        if abs(soma_atual - 1.000) > 0.0005:
            fator = (1.000 / soma_atual) if soma_atual > 0 else (1.000 / len(vals) if len(vals) > 0 else 1.0)
            vals = [round(v * fator, 3) for v in vals]

        # Ajuste fino para fechar 1.000
        soma_ajustada = sum(vals)
        diff = round(1.000 - soma_ajustada, 3)
        if abs(diff) > 0 and len(vals) > 0:
            # Empurrar diferença para o maior valor
            idx_max = max(range(len(vals)), key=lambda i: vals[i])
            vals[idx_max] = round(vals[idx_max] + diff, 3)

        # Garantia final
        soma_final = round(sum(vals), 3)
        if abs(soma_final - 1.000) > 0.001 and len(vals) > 0:
            vals[0] = round(vals[0] + round(1.000 - soma_final, 3), 3)

        for i, idx in enumerate(idxs):
            df_ajustado.at[idx, "VPObra"] = vals[i]

    return df_ajustado


def _limitar_casas_decimais(df: pd.DataFrame) -> pd.DataFrame:
    """Limita as casas decimais das colunas numéricas."""
    if df.empty:
        return df
    df_limpo = df.copy()
    colunas_numericas = ["VPCurva", "PesoModulo", "Unidades", "VPModulo"]
    for coluna in colunas_numericas:
        if coluna in df_limpo.columns:
            df_limpo[coluna] = pd.to_numeric(df_limpo[coluna], errors="coerce").round(2)
    if "VPObra" in df_limpo.columns:
        df_limpo["VPObra"] = pd.to_numeric(df_limpo["VPObra"], errors="coerce").round(3)
    return df_limpo


# ==========================
# Core
# ==========================
def _tratar_curva(df: pd.DataFrame, fonte: str) -> Tuple[pd.DataFrame, List[str]]:
    """Seleciona/normaliza colunas de curvas e retorna (df, avisos)."""
    avisos: List[str] = []
    df = _padronizar_colunas(df)

    faltantes = [c for c in CURVE_REQUIRED_COLS if c not in df.columns]
    if faltantes:
        avisos.append(f"Fonte '{fonte}': colunas ausentes: {', '.join(faltantes)}. Ignorada.")
        return pd.DataFrame(columns=CURVE_REQUIRED_COLS), avisos

    out = df[CURVE_REQUIRED_COLS].copy()

    # Numéricos e percentuais
    out["VPCurva"] = out["VPCurva"].apply(_parse_number_br)
    out["PesoModulo"] = out["PesoModulo"].apply(_parse_number_br)
    out["Unidades"] = out["Unidades"].apply(_parse_number_br)
    out["VPModulo"] = out["VPModulo"].apply(_parse_number_br_pct)
    out["VPObra"] = out["VPObra"].apply(_parse_number_br_pct)

    # Strings
    for c in ["IdEmpreendimento", "IdModulo", "Obra", "SimulacaoId"]:
        out[c] = out[c].astype(str).str.strip()

    # DataReferencia
    out["DataReferencia"] = out["DataReferencia"].astype(str).str.strip().apply(_formatar_data_referencia)

    # Ajuste VPObra por CURVA
    out = _ajustar_vp_obra_com_3_decimais(out)

    # Limites de casas
    out = _limitar_casas_decimais(out)

    return out, avisos


@st.cache_data(show_spinner=False)
def ler_varios_excels_curvas(files: List) -> Dict[str, pd.DataFrame]:
    """Lê até MAX_FILES arquivos .xlsx e devolve { 'arquivo.xlsx::guia': df }."""
    dfs: Dict[str, pd.DataFrame] = {}
    for up in files[:MAX_FILES]:
        xls = pd.read_excel(up, sheet_name=None, dtype=str)
        for sheet, df in xls.items():
            dfs[f"{up.name}::{sheet}"] = pd.DataFrame(df)
    return dfs


def empilhar_curvas(dfs_por_guia: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """Empilha todas as guias das curvas e adiciona coluna 'Fonte'."""
    emp: List[pd.DataFrame] = []
    ok_list: List[str] = []
    warn_list: List[str] = []

    for fonte, df in dfs_por_guia.items():
        if df is None or df.empty:
            warn_list.append(f"Fonte '{fonte}' vazia. Ignorada.")
            continue

        tratado, avisos = _tratar_curva(df, fonte)
        if avisos:
            warn_list.extend(avisos)
        if not tratado.empty:
            tratado = tratado.copy()
            tratado.insert(0, "Fonte", fonte)
            emp.append(tratado)
            ok_list.append(fonte)

    if emp:
        stacked = pd.concat(emp, ignore_index=True)

        # Ajuste final de VPObra por CURVA no dataset empilhado
        stacked = _ajustar_vp_obra_com_3_decimais(stacked)

        # Limitação final de casas
        stacked = _limitar_casas_decimais(stacked)

        ord_cols = [c for c in ["SimulacaoId", "IdEmpreendimento", "Obra", "IdModulo", "DataReferencia"] if c in stacked.columns]
        if ord_cols:
            stacked = stacked.sort_values(by=ord_cols, kind="mergesort", ignore_index=True)
    else:
        stacked = pd.DataFrame(columns=["Fonte"] + CURVE_REQUIRED_COLS)

    return stacked, ok_list, warn_list


def gerar_excel_curvas(stacked: pd.DataFrame, ok_list: List[str], warn_list: List[str]) -> bytes:
    """Gera Excel com Curvas empilhadas e logs."""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        stacked.to_excel(writer, index=False, sheet_name="Curvas_empilhadas")

        if ok_list:
            pd.DataFrame({"Fontes_processadas": ok_list}).to_excel(writer, index=False, sheet_name="_logs_ok")
        if warn_list:
            pd.DataFrame({"Avisos": warn_list}).to_excel(writer, index=False, sheet_name="_logs_warn")

    buffer.seek(0)
    return buffer.read()


def _verificar_somas_vp_obra(df: pd.DataFrame, group_keys: Optional[List[str]] = None) -> pd.DataFrame:
    """Verifica somas de VPObra por CURVA (ex.: IdEmpreendimento+Obra+SimulacaoId)."""
    if df.empty:
        return pd.DataFrame(columns=["IdEmpreendimento", "Obra", "SimulacaoId", "Soma_VPObra", "Diferenca_100"])
    keys = group_keys or _deduzir_chaves_curva(df)
    somas = df.groupby(keys, dropna=False)["VPObra"].sum().reset_index()
    somas["Soma_VPObra"] = pd.to_numeric(somas["VPObra"], errors="coerce").round(3)
    somas["Diferenca_100"] = ((somas["Soma_VPObra"] - 1.0) * 100).round(3)
    somas = somas.drop(columns=["VPObra"])
    # Garantir colunas chave presentes na ordem
    for k in ["IdEmpreendimento", "Obra", "SimulacaoId"]:
        if k not in somas.columns:
            somas[k] = ""
    return somas[["IdEmpreendimento", "Obra", "SimulacaoId", "Soma_VPObra", "Diferenca_100"]]


# ==========================
# UI (aba Curvas)
# ==========================
def render_tab():
    st.markdown(
        f"""
**Carregue os arquivos das Curvas (xlsx)**\n
Curvas de Produção (VP) — Multi-arquivos
Envie **até {MAX_FILES} arquivos .xlsx**, cada um com **uma curva por guia**.  
O app empilha todas as guias de todos os arquivos em **uma única planilha** e gera um Excel para download.

**Formato de Data:**  
Valores como '2026-05', '2026-5' serão automaticamente convertidos para '01/05/2026'.

**Ajuste de VPObra (por CURVA):**  
- A soma de `VPObra` é garantida em **100% para cada curva** identificada por *(IdEmpreendimento, Obra, SimulacaoId)*  
- Se houver 2 curvas da mesma obra (p. ex., SimulacaoId diferentes), **cada uma fecha 100%** (total 200% somando ambas)  
- `VPObra` limitado a **3 casas decimais** e demais numéricos a **2 casas**
"""
    )

    uploaded_files_curvas = st.file_uploader(
        f"Selecione arquivos Excel (.xlsx) — até {MAX_FILES}",
        type=["xlsx"],
        accept_multiple_files=True,
        key="uploader_curvas",
        help="Você pode arrastar e soltar vários arquivos aqui."
    )

    if uploaded_files_curvas:
        if len(uploaded_files_curvas) > MAX_FILES:
            st.error(f"Por favor, selecione no máximo {MAX_FILES} arquivos por vez.")
            return

        with st.spinner("Lendo planilhas de curvas..."):
            dfs_curvas = ler_varios_excels_curvas(uploaded_files_curvas)

        st.success(f"Arquivos carregados. Total de guias lidas (Curvas): {len(dfs_curvas)}")
        with st.expander("Pré-visualizar (primeiras 3 linhas de algumas guias)"):
            shown = 0
            for fonte, df in dfs_curvas.items():
                st.caption(f"Fonte: {fonte}")
                st.dataframe(df.head(3), use_container_width=True)
                shown += 1
                if shown >= 10:
                    st.caption("Exibindo apenas as 10 primeiras guias na prévia.")
                    break

        if st.button("Empilhar curvas de todos os arquivos"):
            with st.spinner("Empilhando e ajustando curvas..."):
                stacked_curvas, ok_list_curvas, warn_list_curvas = empilhar_curvas(dfs_curvas)

            if not stacked_curvas.empty:
                st.subheader("Resultado (Curvas empilhadas) — Amostra")
                st.dataframe(stacked_curvas.head(50), use_container_width=True)

                with st.spinner("Verificando somas de VPObra por CURVA..."):
                    somas_verificadas = _verificar_somas_vp_obra(stacked_curvas)

                st.subheader("Verificação de Somas VPObra por Curva (IdEmpreendimento, Obra, SimulacaoId)")
                st.dataframe(somas_verificadas, use_container_width=True)

                total_curvas = len(somas_verificadas)
                curvas_com_diferenca = somas_verificadas[abs(somas_verificadas["Diferenca_100"]) > 0.001]
                qtd_ajustadas = len(curvas_com_diferenca)

                st.info(
                    f"""
**Estatísticas de Ajuste (por Curva):**
- Total de curvas: {total_curvas}
- Curvas com diferença significativa: {qtd_ajustadas}
- Precisão alvo: 100.000% ± 0.001% por curva
- Casas decimais: VPObra com 3 casas
"""
                )

                if not curvas_com_diferenca.empty:
                    st.warning(f"⚠️ {len(curvas_com_diferenca)} curva(s) com diferença significativa:")
                    st.dataframe(curvas_com_diferenca, use_container_width=True)

                xlsx_bytes_curvas = gerar_excel_curvas(stacked_curvas, ok_list_curvas, warn_list_curvas)
                st.download_button(
                    label="Baixar Excel (Curvas_empilhadas.xlsx)",
                    data=xlsx_bytes_curvas,
                    file_name="Curvas_empilhadas.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

                with st.expander("Logs"):
                    st.write("Fontes processadas:", ok_list_curvas if ok_list_curvas else "—")
                    st.write("Avisos:", warn_list_curvas if warn_list_curvas else "—")
            else:
                st.warning("Nenhuma guia válida foi encontrada com todas as colunas requeridas (Curvas).")
    else:
        st.info("Aguardando arquivos .xlsx para Curvas…")
