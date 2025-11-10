# marco_cronograma_otimizado.py
import io
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List
import pandas as pd
import streamlit as st

# ==========================
# Configurações
# ==========================
MAX_FILES = 1000
TOP_N = 10
MAX_WORKERS = 8  # threads em paralelo
EXCEL_SHEETNAME_MAX = 31

TARGET_COLS = [
    "Arquivo",
    "NET",
    "Nome",
    "Duração",
    "Início",
    "Término",
    "Custo Obra",
    "Obra",
    "IdEmpreendimento",
    "SimulacaoId",
    "Duração obra (meses)",
]

# ==========================
# Helpers
# ==========================
def _strip_accents_upper(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return re.sub(r"\s+", " ", s).strip().upper()


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia colunas conforme o padrão esperado."""
    if df.empty:
        return df
    canon_map = {
        "NET": "NET", "NOME": "Nome", "DURACAO": "Duração", "DURACAO (DIAS)": "Duração",
        "INICIO": "Início", "INÍCIO": "Início", "TERMINO": "Término", "TÉRMINO": "Término",
        "CUSTO": "Custo Obra", "CUSTO OBRA": "Custo Obra", "OBRA": "Obra",
        "NOME OBRA": "Obra", "IDEMPREENDIMENTO": "IdEmpreendimento",
        "SIMULACAOID": "SimulacaoId", "ID SIMULACAO": "SimulacaoId", "ID_SIMULACAO": "SimulacaoId"
    }
    rename_dict = {c: canon_map.get(_strip_accents_upper(c), c) for c in df.columns}
    return df.rename(columns=rename_dict)


def _convert_excel_dates(series: pd.Series) -> pd.Series:
    if series.empty:
        return series
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def _calculate_work_duration(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula duração da obra (Fundação → Fim Físico)"""
    if df.empty or "Obra" not in df.columns:
        df["Duração obra (meses)"] = None
        return df

    inicio = _convert_excel_dates(df.get("Início", pd.Series([pd.NaT] * len(df))))
    termino = _convert_excel_dates(df.get("Término", pd.Series([pd.NaT] * len(df))))
    duracoes = {}

    for obra, subset in df.groupby("Obra"):
        fundacao = inicio[subset["Nome"].str.contains("Fundação", case=False, na=False)].min()
        fim_fisico = termino[subset["Nome"].str.contains("Fim Físico", case=False, na=False)].max()
        if pd.notna(fundacao) and pd.notna(fim_fisico):
            diff = (fim_fisico.year - fundacao.year) * 12 + (fim_fisico.month - fundacao.month)
            if fim_fisico.day < fundacao.day:
                diff -= 1
            duracoes[obra] = max(0, diff)
        else:
            duracoes[obra] = None

    df["Duração obra (meses)"] = df["Obra"].map(duracoes)
    return df


def _ensure_and_reorder(df: pd.DataFrame, arquivo_name: str) -> pd.DataFrame:
    """Normaliza, calcula duração e garante colunas fixas."""
    if df.empty:
        base = pd.DataFrame(columns=TARGET_COLS)
        base["Arquivo"] = arquivo_name
        return base

    df = _normalize_columns(df)
    df = _calculate_work_duration(df)
    df["Arquivo"] = arquivo_name
    for c in TARGET_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df[TARGET_COLS]


def _shorten_filename(fn: str, max_chars: int = 16) -> str:
    base = str(fn)
    return base if len(base) <= max_chars else f"{base[:max_chars-3]}..."


def _sanitize_sheet_name(name: str) -> str:
    name = re.sub(r'[:\\/\?\*\[\]]', "_", str(name)).rstrip(".")
    return name[:EXCEL_SHEETNAME_MAX]


# ==========================
# Núcleo otimizado
# ==========================
def _read_excel_file(file, n: int = TOP_N) -> Dict[str, pd.DataFrame]:
    """Lê todas as guias do arquivo Excel (head n)."""
    try:
        xls = pd.ExcelFile(file)
        return {sheet: xls.parse(sheet, nrows=n) for sheet in xls.sheet_names}
    except Exception as e:
        st.warning(f"Erro ao ler '{file.name}': {e}")
        return {}


def _process_excel_file(file, n: int = TOP_N) -> Dict[str, pd.DataFrame]:
    """Lê e trata cada guia de um arquivo."""
    sheets = _read_excel_file(file, n)
    return {sheet: _ensure_and_reorder(df.head(n), file.name) for sheet, df in sheets.items()}


def processar_arquivos(files: List):
    """Processa arquivos Excel com barras e tempos."""
    total = len(files[:MAX_FILES])
    files = files[:MAX_FILES]

    start_total = time.time()

    # === Etapa 1: Leitura dos arquivos ===
    st.info("📂 Etapa 1: Lendo arquivos Excel...")
    bar_load = st.progress(0, text="Iniciando leitura...")
    start_load = time.time()

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_process_excel_file, f, TOP_N): f for f in files}
        for i, fut in enumerate(as_completed(futures)):
            results.append(fut.result())
            bar_load.progress((i + 1) / total, text=f"Lendo arquivo {i+1}/{total}")

    load_time = time.time() - start_load
    bar_load.empty()
    st.success(f"✅ Etapa 1 concluída em {load_time:.2f} segundos ({total} arquivos lidos).")

    # === Etapa 2: Consolidação ===
    st.info("⚙️ Etapa 2: Consolidando dados...")
    bar_proc = st.progress(0, text="Consolidando DataFrames...")
    start_proc = time.time()

    compiled = []
    for idx, item in enumerate(results):
        for _, df in item.items():
            compiled.append(df)
        bar_proc.progress((idx + 1) / len(results), text=f"Consolidando ({idx+1}/{len(results)})")

    combined = pd.concat(compiled, ignore_index=True) if compiled else pd.DataFrame(columns=TARGET_COLS)

    proc_time = time.time() - start_proc
    bar_proc.empty()
    total_time = time.time() - start_total

    st.success(
        f"🏁 Processamento finalizado!\n\n"
        f"⏱️ Tempo total: {total_time:.1f}s\n"
        f"📘 Leitura: {load_time:.1f}s | 🔧 Consolidação: {proc_time:.1f}s\n"
        f"📄 Linhas resultantes: {len(combined)}"
    )

    return combined


def gerar_excel_compilado(dados: pd.DataFrame, incluir_abas_individuais=False) -> bytes:
    """Gera Excel consolidado com opção de abas individuais."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        dados.to_excel(writer, index=False, sheet_name="Compilado_TopN")

        if incluir_abas_individuais:
            for arquivo, df_grp in dados.groupby("Arquivo"):
                sheet_name = _sanitize_sheet_name(_shorten_filename(arquivo))
                df_grp.head(TOP_N).to_excel(writer, index=False, sheet_name=sheet_name)
    return buf.getvalue()


# ==========================
# UI Streamlit
# ==========================
def render_tab():
    st.header("📊 Compilar Cronogramas — Marco Empreendimento")
    st.write("""
    **Arquivos para upload: Cronogramas (xlsx)**
    
    Os arquivos poderão ser compilados ou individuais.
    """)

    uploaded_files = st.file_uploader(
        f"Selecione até {MAX_FILES} arquivos Excel",
        type=["xlsx"],
        accept_multiple_files=True
    )

    if not uploaded_files:
        st.info("Aguardando arquivos...")
        return

    incluir_abas = st.checkbox("Também criar uma aba por arquivo (Top N)", value=False)
    start = time.time()

    if st.button("🚀 Iniciar Compilação", type="primary", use_container_width=True):
        dados = processar_arquivos(uploaded_files)
        if dados.empty:
            st.error("Nenhum dado válido encontrado.")
            return

        excel_bytes = gerar_excel_compilado(dados, incluir_abas_individuais=incluir_abas)
        st.download_button(
            "⬇️ Baixar Excel Compilado",
            data=excel_bytes,
            file_name="Cronogramas_Marco_Compilado.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        st.metric("Linhas Totais", len(dados))
        st.metric("Arquivos Processados", len(uploaded_files))
        st.metric("Tempo Total (s)", f"{time.time()-start:.1f}")

    if st.button("👀 Prévia (amostra de 50 linhas)", use_container_width=True):
        dados = processar_arquivos(uploaded_files)
        if not dados.empty:
            st.dataframe(dados.head(50), use_container_width=True)
