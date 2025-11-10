# marco_parede.py
import io
import re
import unicodedata
from typing import Dict, List
import pandas as pd
import streamlit as st

MAX_FILES = 300
EXCEL_SHEETNAME_MAX = 31

TARGET_COLS = [
    "Arquivo",
    "NET",
    "Nome",
    "Duração",
    "Início",
    "Término",
    "Obra",
    "M",
    "IdEmpreendimento",
    "SimulacaoId",
    "Início PC Obra",
    "Término PC Obra",
]

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def _strip_accents_upper(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    s = re.sub(r"\s+", " ", s).strip()
    return s.upper()


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza os nomes das colunas para padrão interno"""
    if df is None or df.empty:
        return df
    canon_map = {
        "NET": "NET",
        "NOME": "Nome",
        "DURACAO": "Duração",
        "INICIO": "Início",
        "INÍCIO": "Início",
        "TERMINO": "Término",
        "TÉRMINO": "Término",
        "CUSTO OBRA": "Custo Obra",
        "OBRA": "Obra",
        "NOME OBRA": "Obra",
        "IDEMPREENDIMENTO": "IdEmpreendimento",
        "SIMULACAOID": "SimulacaoId",
        "M": "M",
        "MODULO": "M",
    }
    rename_dict = {}
    for c in df.columns:
        key = _strip_accents_upper(c)
        if key in canon_map:
            rename_dict[c] = canon_map[key]
    return df.rename(columns=rename_dict)


def _add_periodo_construcao(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona colunas Início/Término PC Obra (menor e maior data por obra)."""
    if df is None or df.empty or "Obra" not in df.columns or "Nome" not in df.columns:
        df["Início PC Obra"] = pd.NaT
        df["Término PC Obra"] = pd.NaT
        return df

    df = df.copy()
    df["Obra_norm"] = (
        df["Obra"]
        .astype(str)
        .apply(lambda x: unicodedata.normalize("NFKD", x)
               .encode("ASCII", "ignore").decode("ASCII").upper())
    )

    for col in ["Início", "Término"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df_parede = df[df["Nome"].astype(str)
                   .str.contains("ALVENARIA / PAREDE DE CONCRETO", case=False, na=False)]

    df_pc = (
        df_parede.groupby("Obra_norm", dropna=False)[["Início", "Término"]]
        .agg({"Início": "min", "Término": "max"})
        .reset_index()
        .rename(columns={"Início": "Início PC Obra", "Término": "Término PC Obra"})
    )

    df = df.merge(df_pc, on="Obra_norm", how="left")
    df = df.drop(columns=["Obra_norm"], errors="ignore")
    return df


def _add_periodo_construcao_modulo(df: pd.DataFrame) -> pd.DataFrame:
    """Gera DataFrame com Início/Término por módulo dentro de cada obra."""
    if df is None or df.empty or "Obra" not in df.columns or "Nome" not in df.columns or "M" not in df.columns:
        return pd.DataFrame(columns=["IdEmpreendimento", "Obra", "Módulo", "Início PC Módulo", "Término PC Módulo"])

    df = df.copy()
    df["Obra_norm"] = (
        df["Obra"]
        .astype(str)
        .apply(lambda x: unicodedata.normalize("NFKD", x)
               .encode("ASCII", "ignore").decode("ASCII").upper())
    )

    for col in ["Início", "Término"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Filtra parede de concreto
    df_parede = df[df["Nome"].astype(str)
                   .str.contains("ALVENARIA / PAREDE DE CONCRETO", case=False, na=False)]

    # Agrupa por obra, módulo e IdEmpreendimento
    df_mod = (
        df_parede.groupby(["Obra_norm", "M", "IdEmpreendimento"], dropna=False)[["Início", "Término"]]
        .agg({"Início": "min", "Término": "max"})
        .reset_index()
        .rename(columns={"Início": "Início PC Módulo", "Término": "Término PC Módulo"})
    )

    # Ajusta M para +1
    def _ajustar_modulo(x):
        try:
            return int(float(x)) + 1
        except:
            return x

    df_mod["M"] = df_mod["M"].apply(_ajustar_modulo)

    df_mod = df_mod.rename(columns={"Obra_norm": "Obra", "M": "Módulo"})
    df_mod = df_mod[["IdEmpreendimento", "Obra", "Módulo", "Início PC Módulo", "Término PC Módulo"]]
    return df_mod


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

def _ensure_and_reorder(df: pd.DataFrame, arquivo_name: str) -> pd.DataFrame:
    """Garante colunas e estrutura padronizada."""
    if df is None or df.empty:
        base = pd.DataFrame(columns=TARGET_COLS)
        base["Arquivo"] = arquivo_name
        return base

    df = _normalize_columns(df).copy()
    df["Arquivo"] = arquivo_name
    df = df.drop(columns=["Custo Obra"], errors="ignore")
    df = _add_periodo_construcao(df)

    for col in TARGET_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    return df[TARGET_COLS]


def ler_todas_linhas_excels(files: List) -> Dict[str, Dict[str, pd.DataFrame]]:
    """Lê todas as abas de todos os arquivos Excel."""
    todos = {}
    for up in files[:MAX_FILES]:
        try:
            xls = pd.read_excel(up, sheet_name=None)
        except Exception as e:
            st.warning(f"Falha ao ler '{up.name}': {e}")
            continue

        tabelas = {sheet: (df if isinstance(df, pd.DataFrame) else pd.DataFrame()) for sheet, df in xls.items()}
        todos[up.name] = tabelas
    return todos


def compilar_parede(tabelas: Dict[str, Dict[str, pd.DataFrame]]) -> pd.DataFrame:
    """Empilha linhas com 'Alvenaria / Parede de Concreto'."""
    frames = []
    for arquivo, guias in tabelas.items():
        for _, df in guias.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            df = _ensure_and_reorder(df, arquivo)
            filtro = df["Nome"].astype(str).str.contains("ALVENARIA / PAREDE DE CONCRETO", case=False, na=False)
            df_filtrado = df[filtro]
            if not df_filtrado.empty:
                frames.append(df_filtrado)

    if not frames:
        return pd.DataFrame(columns=TARGET_COLS)

    emp = pd.concat(frames, ignore_index=True, sort=False)
    return emp[TARGET_COLS]


def compilar_parede_modulo(tabelas: Dict[str, Dict[str, pd.DataFrame]]) -> pd.DataFrame:
    """Compila datas de parede de concreto por módulo."""
    frames = []
    for arquivo, guias in tabelas.items():
        for _, df in guias.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            df = _normalize_columns(df)
            df["Arquivo"] = arquivo
            if "Obra" not in df.columns or "M" not in df.columns:
                continue
            mod_df = _add_periodo_construcao_modulo(df)
            if not mod_df.empty:
                frames.append(mod_df)

    if not frames:
        return pd.DataFrame(columns=["IdEmpreendimento", "Obra", "Módulo", "Início PC Módulo", "Término PC Módulo"])

    emp = pd.concat(frames, ignore_index=True, sort=False)
    emp = emp.drop_duplicates(subset=["IdEmpreendimento", "Obra", "Módulo"], keep="first")
    return emp[["IdEmpreendimento", "Obra", "Módulo", "Início PC Módulo", "Término PC Módulo"]]


def gerar_excel_parede(tabelas: Dict[str, Dict[str, pd.DataFrame]]) -> bytes:
    """Gera o Excel final com as duas guias."""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        emp = compilar_parede(tabelas)
        emp.to_excel(writer, index=False, sheet_name="Parede_compilado")
        mod = compilar_parede_modulo(tabelas)
        mod.to_excel(writer, index=False, sheet_name="mód._PC")
    buffer.seek(0)
    return buffer.read()


# ============================================================
# INTERFACE STREAMLIT
# ============================================================

def render_tab():
    st.markdown(
        """
        ### 📊 Compilar Cronogramas — Alvenaria / Parede de Concreto
        **Arquivos para upload:** Cronogramas (`.xlsx`)  
        Os arquivos podem conter múltiplas abas; o app faz leitura completa e gera:
        - Compilado de etapas de *Alvenaria / Parede de Concreto*  
        - Cálculo de Início/Término por obra e módulo  
        - Exportação em Excel com duas guias (`Parede_compilado` e `mód._PC`)
        """,
        unsafe_allow_html=True,
    )

    uploaded_files = st.file_uploader(
        "Selecione um ou mais arquivos Excel (.xlsx)",
        type=["xlsx"],
        accept_multiple_files=True,
    )

    if not uploaded_files:
        st.info("Aguardando arquivos .xlsx...")
        return

    col1, col2 = st.columns([1, 1])
    gerar_clicked = col1.button("Gerar Excel (Parede de Concreto)", type="primary")
    preview_clicked = col2.button("Pré-visualizar (linhas filtradas)")

    if gerar_clicked:
        with st.spinner("Lendo e processando arquivos..."):
            tabelas = ler_todas_linhas_excels(uploaded_files)
            xlsx_bytes = gerar_excel_parede(tabelas)
        st.success("✅ Arquivo pronto para download.")
        st.download_button(
            "Baixar Excel (Cronogramas - Parede).xlsx",
            data=xlsx_bytes,
            file_name="Cronogramas_Parede.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    if preview_clicked:
        with st.spinner("Gerando pré-visualização..."):
            tabelas = ler_todas_linhas_excels(uploaded_files)
            emp = compilar_parede(tabelas)
        st.dataframe(emp.head(100), use_container_width=True)
