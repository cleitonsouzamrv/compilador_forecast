import importlib
import streamlit as st
import os
from dividir_guias import dividir_planilha_em_guia  # Importa a função corretamente

st.set_page_config(page_title="Forecast", layout="wide")
st.title("Compilador Forecast")


def import_module_any(candidates):
    last_err = None
    for name in candidates:
        try:
            return importlib.import_module(name), name
        except Exception as e:
            last_err = e
    raise last_err or ImportError(f"Não foi possível importar: {candidates}")


# Função para exibir a aba "Dividir Planilhas"
def render_tab_dividir_planilhas():
    st.header("Dividir Planilhas por Guia")
    
    # Upload de arquivo Excel
    uploaded_file = st.file_uploader("Escolha um arquivo Excel", type="xlsx")
    if uploaded_file is not None:
        # Diretório de saída (pode ser temporário ou fornecido pelo usuário)
        output_directory = st.text_input("Diretório para salvar os arquivos", "/tmp")
        
        # Botão para dividir as guias
        if st.button("Dividir Planilhas"):
            if not os.path.exists(output_directory):
                os.makedirs(output_directory)
            
            # Chamar a função para dividir as guias e gerar o arquivo ZIP
            zip_file_path = dividir_planilha_em_guia(uploaded_file, output_directory)
            st.success("As planilhas foram divididas com sucesso!")
            
            # Botão de download para o arquivo ZIP
            with open(zip_file_path, "rb") as f:
                st.download_button(
                    label="Baixar as Planilhas Divididas",
                    data=f,
                    file_name="planilhas_divididas.zip",
                    mime="application/zip",
                )


# (título, [módulos candidatos], função)
TABS = [
    ("Marco Cronograma", ["marco_cronograma"], "render_tab"),
    ("Marco Parede", ["marco_parede"], "render_tab"),
    ("Marco Módulo", ["marco_modulo"], "render_tab"),
    ("PP", ["pp"], "render_tab"),
    ("Curvas", ["curvas_prod", "curva_prod"], "render_tab"),
    ("Dividir Planilhas", [], None),  # Não depende de função render_tab(), será tratado diretamente no app.py
]

loaded = []
for title, module_candidates, fn_name in TABS:
    fn = None
    modname_used = None
    err = None
    try:
        if fn_name and module_candidates:  # Verificar se há módulos para importar
            mod, modname_used = import_module_any(module_candidates)
            if not hasattr(mod, fn_name):
                raise AttributeError(f"Módulo '{modname_used}' não possui função '{fn_name}()'.")
            fn = getattr(mod, fn_name)
    except Exception as e:
        err = e
    loaded.append((title, fn, err, module_candidates, fn_name, modname_used))

tabs = st.tabs([t[0] for t in loaded])

for (title, fn, err, module_candidates, fn_name, modname_used), container in zip(loaded, tabs):
    with container:
        if err:
            candidates_str = ", ".join(module_candidates)
            st.error(f"Falha ao carregar **{title}** ({candidates_str}.{fn_name}): {err}")
            st.caption("Verifique o nome do arquivo e a existência de render_tab().")
            st.exception(err)
            continue
        try:
            if title == "Dividir Planilhas":  # Tratamento para a aba Dividir Planilhas
                render_tab_dividir_planilhas()  # Chama a função diretamente
            else:
                fn()
        except Exception as e:
            st.error(f"Erro em **{title}** [{modname_used}.{fn_name}]")
            st.exception(e)
