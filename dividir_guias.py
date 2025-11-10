import pandas as pd
import os
import zipfile

def dividir_planilha_em_guia(uploaded_file, output_directory):
    # Usar o arquivo enviado diretamente, sem precisar de um caminho fixo
    excel_data = pd.ExcelFile(uploaded_file)

    # Se o diretório de saída não existir, criá-lo
    if not os.path.exists(output_directory):    
        os.makedirs(output_directory)

    # Nome do arquivo ZIP
    zip_file_path = os.path.join(output_directory, "planilhas_divididas.zip")

    # Cria o arquivo ZIP
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Iterar sobre todas as guias (abas) do arquivo Excel
        for sheet_name in excel_data.sheet_names:
            # Carregar os dados da guia atual em um DataFrame
            df = excel_data.parse(sheet_name)

            # Criar um caminho temporário para salvar o novo arquivo Excel com o nome da guia
            temp_file_path = os.path.join(output_directory, f"{sheet_name}.xlsx")
            
            # Salvar a guia em um novo arquivo Excel
            df.to_excel(temp_file_path, index=False, engine='openpyxl')

            # Adicionar o arquivo Excel ao arquivo ZIP
            zipf.write(temp_file_path, os.path.basename(temp_file_path))
            
            # Remover o arquivo temporário após adicionar ao ZIP
            os.remove(temp_file_path)

    return zip_file_path  # Retorna o caminho do arquivo ZIP
