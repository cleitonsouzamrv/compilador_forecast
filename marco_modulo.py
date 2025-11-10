import io
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
import pandas as pd
import streamlit as st

# ==========================
# Configurações
# ==========================
MAX_FILES = 1000
MAX_ROWS_PER_SHEET = 100
MAX_WORKERS = 8  # número de threads simultâneas

COLUNAS = [
    "Arquivo", "NET", "Nome", "Duração", "Início", "Término",
    "Custo Obra", "Obra", "IdEmpreendimento", "SimulacaoId",
    "Duração obra (meses)", "Módulo"
]

# ==========================
# Helpers rápidos
# ==========================
def _strip_accents_upper(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return re.sub(r"\s+", " ", s).strip().upper()


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    canon_map = {
        "NET": "NET", "NOME": "Nome", "DURACAO": "Duração", "INICIO": "Início",
        "INÍCIO": "Início", "TERMINO": "Término", "TÉRMINO": "Término",
        "CUSTO": "Custo Obra", "OBRA": "Obra", "NOME OBRA": "Obra",
        "IDEMPREENDIMENTO": "IdEmpreendimento", "SIMULACAOID": "SimulacaoId"
    }
    rename_dict = {c: canon_map.get(_strip_accents_upper(c), c) for c in df.columns}
    return df.rename(columns=rename_dict)


def _convert_excel_dates(series: pd.Series) -> pd.Series:
    if series.empty:
        return series
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def _calculate_work_duration(df: pd.DataFrame) -> pd.DataFrame:
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
            diff_meses = (fim_fisico.year - fundacao.year) * 12 + (fim_fisico.month - fundacao.month)
            if fim_fisico.day < fundacao.day:
                diff_meses -= 1
            duracoes[obra] = max(diff_meses, 0)
        else:
            duracoes[obra] = None
    df["Duração obra (meses)"] = df["Obra"].map(duracoes)
    return df


def _create_module_column(df: pd.DataFrame) -> pd.DataFrame:
    if "Nome" not in df.columns:
        df["Módulo"] = ""
        return df
    nome_upper = df["Nome"].astype(str).str.upper()
    mask = nome_upper.str.match(r".*M(0[1-9]|1[0-5])$")
    df["Módulo"] = ""
    df.loc[mask, "Módulo"] = "MÓD. " + nome_upper.str[-2:]
    return df


def _process_single_file(arquivo) -> pd.DataFrame:
    """Processa um arquivo completo"""
    try:
        xls = pd.ExcelFile(arquivo)
        all_sheets = []
        for nome_aba in xls.sheet_names:
            df = xls.parse(nome_aba, nrows=MAX_ROWS_PER_SHEET)
            if df.empty:
                continue
            df = _normalize_columns(df)
            df = _calculate_work_duration(df)
            df = _create_module_column(df)
            df["Arquivo"] = arquivo.name
            for c in COLUNAS:
                if c not in df.columns:
                    df[c] = ""
            all_sheets.append(df[COLUNAS])
        return pd.concat(all_sheets, ignore_index=True) if all_sheets else pd.DataFrame(columns=COLUNAS)
    except Exception as e:
        st.warning(f"Erro em {arquivo.name}: {e}")
        return pd.DataFrame(columns=COLUNAS)

# ==========================
# Funções principais
# ==========================
def processar_arquivos(uploaded_files: List):
    """Processa e mostra tempo de cada etapa"""
    total = len(uploaded_files[:MAX_FILES])

    # ---------------------
    # Etapa 1 - Carregamento
    # ---------------------
    st.info("⏳ Etapa 1: Carregando arquivos Excel...")
    load_bar = st.progress(0, text="Carregando arquivos...")
    start_load = time.time()

    arquivos = uploaded_files[:MAX_FILES]
    excel_objects = []
    for i, arq in enumerate(arquivos):
        excel_objects.append(arq)
        load_bar.progress((i + 1) / total, text=f"Lendo arquivo {i+1}/{total}")
    load_time = time.time() - start_load
    load_bar.empty()
    st.success(f"✅ {total} arquivos carregados em {load_time:.2f} segundos.")

    # ---------------------
    # Etapa 2 - Processamento
    # ---------------------
    st.info("⚙️ Etapa 2: Processando dados...")
    process_bar = st.progress(0, text="Processando arquivos...")
    start_proc = time.time()
    resultados = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_process_single_file, arq): arq for arq in excel_objects}
        for i, future in enumerate(as_completed(futures)):
            df_res = future.result()
            resultados.append(df_res)
            process_bar.progress((i + 1) / total, text=f"Processado {i+1}/{total} arquivos")

    process_time = time.time() - start_proc
    process_bar.empty()

    df_final = pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame(columns=COLUNAS)
    df_final = df_final[df_final["Módulo"] != ""]

    total_time = load_time + process_time
    st.success(
        f"🏁 Processamento finalizado: "
        f"{len(df_final)} linhas consolidadas em {total_time:.1f}s "
        f"(Leitura: {load_time:.1f}s | Processamento: {process_time:.1f}s)"
    )

    return df_final


def gerar_excel(df):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="DadosConsolidados")
    return buf.getvalue()


def render_tab():
    st.header("📊 Compilar Cronogramas — Marco por Módulos")
    st.write("""
    **Arquivos para upload: Cronogramas (xlsx)**
    
    Os arquivos poderão ser compilados ou individuais.
    """)

    arquivos = st.file_uploader(
        "Selecione os arquivos Excel",
        type=["xlsx"],
        accept_multiple_files=True
    )

    if not arquivos:
        st.info("Aguardando upload de arquivos...")
        return

    st.info(f"{len(arquivos)} arquivo(s) carregado(s). Máximo: {MAX_FILES}")

    if st.button("🚀 Iniciar Consolidação", use_container_width=True, type="primary"):
        dados = processar_arquivos(arquivos)

        if dados.empty:
            st.error("Nenhum dado válido encontrado com Módulo M01–M15.")
            return

        st.subheader("✅ Prévia dos Dados Consolidados")
        st.dataframe(dados.head(50), use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            excel_bytes = gerar_excel(dados)
            st.download_button(
                "⬇️ Baixar Consolidado",
                data=excel_bytes,
                file_name="consolidado_modulos.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with col2:
            st.metric("Total de Linhas", len(dados))
            st.metric("Obras Únicas", dados["Obra"].nunique())
