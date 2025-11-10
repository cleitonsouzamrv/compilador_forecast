# pp_unico.py
import streamlit as st
import pandas as pd
from io import BytesIO

def render_tab():
    st.title("📊 Título da Aba")
    st.write("Conteúdo informativo para a aba.")

    # Exemplo de filtro de dados, gráficos ou tabelas
    st.write("Aqui vai o conteúdo filtrado, gráfico ou tabela que você deseja mostrar.")

# Aqui começa o seu código principal para filtrar a última data por Empreendimento
def filtrar_ultima_data():
    st.set_page_config("Filtrar últimas datas por Emp.", layout="wide")

    st.title("📅 Filtrar últimas datas por Empreendimento")

    uploaded_file = st.file_uploader("Selecione o arquivo Excel", type=["xlsx"])

    if uploaded_file:
        try:
            # Lê sempre a aba "PP_empilhado"
            df = pd.read_excel(uploaded_file, sheet_name="PP_empilhado")
            st.success("Guia 'PP_empilhado' carregada com sucesso!")

            # Normaliza nomes das colunas
            df.columns = df.columns.str.strip()

            # Localiza colunas principais
            col_data = [c for c in df.columns if c.lower() == "data geração".lower()]
            col_emp = [c for c in df.columns if c.lower().startswith("emp")]

            if not col_data or not col_emp:
                st.error("A planilha deve conter as colunas 'Emp.' e 'Data Geração'.")
            else:
                col_data = col_data[0]
                col_emp = col_emp[0]

                # Converte a coluna de data
                df[col_data] = pd.to_datetime(df[col_data], errors="coerce")

                # Identifica a maior data por Emp.
                max_datas = (
                    df.groupby(col_emp, as_index=False)[col_data]
                    .max()
                    .rename(columns={col_data: "Data_Max"})
                )

                # Junta para manter TODAS as linhas da maior data
                df_filtrado = df.merge(
                    max_datas,
                    how="inner",
                    left_on=[col_emp, col_data],
                    right_on=[col_emp, "Data_Max"]
                ).drop(columns=["Data_Max"])

                # Ordena
                df_filtrado = df_filtrado.sort_values([col_emp, col_data]).reset_index(drop=True)

                st.success(f"{len(df_filtrado)} linhas mantidas (todas da última data por Emp.)")

                st.dataframe(df_filtrado, use_container_width=True)

                # Cria arquivo Excel para download
                buffer = BytesIO()
                with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                    df_filtrado.to_excel(writer, index=False, sheet_name="Filtrado")

                st.download_button(
                    label="📤 Baixar Excel filtrado",
                    data=buffer.getvalue(),
                    file_name="Empresas_filtradas.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        except Exception as e:
            st.error(f"Erro ao ler a guia 'PP_empilhado': {e}")

    else:
        st.info("Envie uma planilha Excel contendo a guia 'PP_empilhado'.")
