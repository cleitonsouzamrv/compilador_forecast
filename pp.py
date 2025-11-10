import io
import os
import re
import unicodedata
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st

MAX_FILES = 1000
REQUIRED_COLS_PP = ["SimulacaoId", "IdEmpreendimento", "NET", "Nome", "M", "Custo"]
CACHE_DIR = Path("temp_cache_pp")
CACHE_DIR.mkdir(exist_ok=True)

# ==========================
# Utils
# ==========================
def parse_number_br(value):
    if pd.isna(value):
        return pd.NA
    s = str(value).strip()
    if not s:
        return pd.NA
    s = re.sub(r"[^0-9,.\-]", "", s)
    if s in {"", "-", ",", "."}:
        return pd.NA
    last_comma, last_dot = s.rfind(","), s.rfind(".")
    dec = "," if last_comma > last_dot else "."
    parts = s.rsplit(dec, 1)
    num_str = re.sub(r"[^0-9\-]", "", parts[0]) + (
        "." + re.sub(r"[^0-9]", "", parts[1]) if len(parts) == 2 else ""
    )
    try:
        return float(num_str)
    except Exception:
        return pd.NA


def strip_accents_upper(text: str) -> str:
    if text is None:
        return ""
    text = unicodedata.normalize("NFKD", str(text))
    text = text.encode("ASCII", "ignore").decode("ASCII")
    return re.sub(r"\s+", " ", text).strip().upper()


def normalize_hyphen_spaces(text: str) -> str:
    return re.sub(r"\s*-\s*", " - ", text)


def format_mod_label(v):
    try:
        idx = int(float(str(v).replace(",", ".")))
    except Exception:
        idx = 0
    return f"MÓD. {idx + 1:02d}"

# ==========================
# Núcleo
# ==========================
def selecionar_e_tratar_colunas_pp(df: pd.DataFrame, guia: str) -> Tuple[pd.DataFrame, List[str]]:
    avisos: List[str] = []
    faltantes = [c for c in REQUIRED_COLS_PP if c not in df.columns]
    if faltantes:
        avisos.append(f"Guia '{guia}': colunas ausentes: {', '.join(faltantes)}. Ignorada.")
        return pd.DataFrame(columns=REQUIRED_COLS_PP), avisos

    out = df[REQUIRED_COLS_PP].copy()
    net_num = pd.to_numeric(out["NET"], errors="coerce")
    nome_norm = out["Nome"].astype(str).map(strip_accents_upper).map(normalize_hyphen_spaces)

    mask1 = net_num.eq(1)
    mask2 = net_num.eq(2) & nome_norm.str.contains(r"\bMODULO\b", na=False)
    mask3 = net_num.eq(4) & (nome_norm == "PRE - PROJETO")

    out = out.loc[mask1 | mask2 | mask3].copy()
    out["NET"] = net_num.loc[out.index].astype("Int64")
    out["Custo"] = out["Custo"].apply(parse_number_br)
    out["M"] = out["M"].apply(format_mod_label)

    for c in ["Nome", "SimulacaoId", "IdEmpreendimento"]:
        out[c] = out[c].astype(str).str.strip()
    return out, avisos


def _read_excel_with_cache(file_bytes: bytes, file_name: str):
    """Lê o arquivo Excel e salva cache .parquet (executa no subprocesso)."""
    cache_name = CACHE_DIR / f"{file_name}.parquet"
    if cache_name.exists():
        try:
            df = pd.read_parquet(cache_name)
            return {f"{file_name}::Planilha1": df}
        except Exception:
            pass

    xls = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None, dtype=str, usecols=lambda c: c.strip() in REQUIRED_COLS_PP)
    try:
        for sheet, df in xls.items():
            df.to_parquet(cache_name, index=False)
            break
    except Exception:
        pass
    return {f"{file_name}::{list(xls.keys())[0]}": list(xls.values())[0]}


def ler_varios_excels_pp(files: List, progress_bar, status_text, tempo_text) -> Dict[str, pd.DataFrame]:
    """Leitura com ProcessPoolExecutor + barra de progresso + cronômetro + estimativa."""
    dfs: Dict[str, pd.DataFrame] = {}
    total = len(files)
    start = time.time()

    with ProcessPoolExecutor(max_workers=min(6, os.cpu_count() or 4)) as executor:
        futures = []
        for f in files[:MAX_FILES]:
            file_bytes = f.read()
            f.seek(0)
            futures.append(executor.submit(_read_excel_with_cache, file_bytes, f.name))

        for i, future in enumerate(futures, 1):
            try:
                result = future.result()
                dfs.update(result)
            except Exception:
                continue

            elapsed = time.time() - start
            avg_per_file = elapsed / i
            remaining = (total - i) * avg_per_file
            tempo_text.text(f"📖 Lendo: {i}/{total} | ⏱ {elapsed:.1f}s | ⌛ Restante ~{remaining:.1f}s")
            progress_bar.progress(min(i / total, 1.0))
            status_text.text(f"Lendo arquivo {i}/{total}")

    total_time = time.time() - start
    tempo_text.text(f"✅ Leitura concluída em {total_time:.1f}s")
    return dfs


def stack_schedules_pp(dfs_por_guia: Dict[str, pd.DataFrame], progress_bar, tempo_text, etapa: str) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """Empilha e mostra progresso de processamento."""
    empilhados, ok_list, warn_list = [], [], []
    total = len(dfs_por_guia)
    start = time.time()

    for i, (fonte, df) in enumerate(dfs_por_guia.items(), 1):
        tratado, avisos = selecionar_e_tratar_colunas_pp(df, fonte)
        warn_list.extend(avisos)
        if not tratado.empty:
            tratado.insert(0, "Fonte", fonte)
            empilhados.append(tratado)
            ok_list.append(fonte)
        if i % 10 == 0 or i == total:
            elapsed = time.time() - start
            progress_bar.progress(0.6 + 0.3 * (i / total))  # empilhamento = 30% da barra
            tempo_text.text(f"⚙️ {etapa}: {i}/{total} guias | ⏱ {elapsed:.1f}s")

    if not empilhados:
        return pd.DataFrame(columns=["Fonte"] + REQUIRED_COLS_PP), ok_list, warn_list
    return pd.concat(empilhados, ignore_index=True), ok_list, warn_list


def calcular_pesos_pp(df: pd.DataFrame, progress_bar, tempo_text):
    start = time.time()
    tempo_text.text("📊 Calculando pesos PP (obra e módulo)...")
    progress_bar.progress(0.95)
    if df.empty:
        df["Peso PP Obra"] = pd.NA
        df["Peso PP Módulo"] = pd.NA
        return df
    grp_obra = ["SimulacaoId", "IdEmpreendimento"]
    grp_mod = ["SimulacaoId", "IdEmpreendimento", "M"]
    den_obra = df.loc[df["NET"] == 1, grp_obra + ["Custo"]].groupby(grp_obra)["Custo"].sum().rename("DenObra")
    den_mod = df.loc[df["NET"] == 2, grp_mod + ["Custo"]].groupby(grp_mod)["Custo"].sum().rename("DenMod")
    df = df.join(den_obra, on=grp_obra).join(den_mod, on=grp_mod)
    nome_norm = df["Nome"].astype(str).map(strip_accents_upper).map(normalize_hyphen_spaces)
    mask_target = (df["NET"] == 4) & (nome_norm == "PRE - PROJETO")
    df["Peso PP Obra"] = df.loc[mask_target, "Custo"] / df.loc[mask_target, "DenObra"]
    df["Peso PP Módulo"] = df.loc[mask_target, "Custo"] / df.loc[mask_target, "DenMod"]
    df.drop(columns=["DenObra", "DenMod"], inplace=True, errors="ignore")
    tempo_text.text(f"✅ Cálculos concluídos em {time.time() - start:.1f}s")
    progress_bar.progress(1.0)
    return df


def gerar_excel_pp(df, ok_list, warn_list) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        workbook = writer.book
        fmt_pct = workbook.add_format({"num_format": "0.00%"})
        df.to_excel(writer, index=False, sheet_name="PP_empilhado")
        ws = writer.sheets["PP_empilhado"]
        for col in ["Peso PP Obra", "Peso PP Módulo"]:
            if col in df.columns:
                ci = df.columns.get_loc(col)
                ws.set_column(ci, ci, None, fmt_pct)
        pd.DataFrame({"Guias OK": ok_list}).to_excel(writer, index=False, sheet_name="_ok")
        pd.DataFrame({"Avisos": warn_list}).to_excel(writer, index=False, sheet_name="_warn")
    buffer.seek(0)
    return buffer.read()


def limpar_cache():
    removed = 0
    total_size = 0
    for f in CACHE_DIR.glob("*.parquet"):
        try:
            total_size += f.stat().st_size
            f.unlink()
            removed += 1
        except Exception:
            pass
    total_mb = round(total_size / (1024 * 1024), 2)
    return removed, total_mb

# ==========================
# UI
# ==========================
def render_tab():
    st.title("Empilhar Cronogramas (PP)")
    st.write("Carregar os arquivos dos Cronogramas (.xlsx)")

    with st.expander("⚙️ Opções avançadas"):
        if st.button("🧹 Limpar cache (.parquet)"):
            n, size = limpar_cache()
            st.success(f"Cache limpo! {n} arquivos removidos ({size} MB).")

    uploaded_files_pp = st.file_uploader(
        f"Selecione arquivos Excel (.xlsx) — até {MAX_FILES}",
        type=["xlsx"],
        accept_multiple_files=True,
        key="uploader_pp"
    )

    if not uploaded_files_pp:
        st.info("Aguardando arquivos .xlsx para PP...")
        return

    total = len(uploaded_files_pp)
    st.write(f"Total de arquivos: **{total}**")

    if st.button("🚀 Iniciar Processamento (PP)"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        tempo_text = st.empty()

        start_all = time.time()

        st.info("📥 Lendo e processando arquivos, aguarde...")
        dfs_por_guia = ler_varios_excels_pp(uploaded_files_pp, progress_bar, status_text, tempo_text)

        st.info("⚙️ Empilhando e aplicando filtros...")
        stacked_pp, ok_list_pp, warn_list_pp = stack_schedules_pp(dfs_por_guia, progress_bar, tempo_text, "Empilhamento")

        st.info("📊 Calculando pesos e finalizando...")
        stacked_pp = calcular_pesos_pp(stacked_pp, progress_bar, tempo_text)

        total_time_all = time.time() - start_all
        tempo_text.text(f"🏁 Tempo total de execução: {total_time_all:.1f}s")
        status_text.text("✅ Processamento completo!")

        if not stacked_pp.empty:
            st.success("✅ Processamento finalizado com sucesso!")
            st.dataframe(stacked_pp.head(50), use_container_width=True)
            xlsx_bytes_pp = gerar_excel_pp(stacked_pp, ok_list_pp, warn_list_pp)
            st.download_button(
                "💾 Baixar Excel (PP_empilhado.xlsx)",
                data=xlsx_bytes_pp,
                file_name="PP_empilhado.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("Nenhuma guia válida encontrada.")
