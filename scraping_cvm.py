from typing import Dict
import requests
import zipfile
from requests import Response
from bs4 import BeautifulSoup
from bs4.element import Tag
import os
import pandas as pd

def main(): 
    Caminho: str = 'out'
    url: str = "https://dados.cvm.gov.br/dados/FII/DOC/INF_TRIMESTRAL/DADOS/"

    # Get CVM's data
    response: Response = requests.get(url)
    soup: BeautifulSoup = BeautifulSoup(response.content, "html.parser")

    # Get URLs to download CVM's .zip files
    files_url: list[str] = get_files_url(soup, url)
    filtered_files_url: list[str] = filter_by_missing_years(files_url, Caminho)

    # Download files and unzip it
    for file_url in filtered_files_url:
        download_zip_file(file_url, Caminho)
        unzip_file(get_path_from(file_url, Caminho))

    # Unify files
    unified_files: dict[str, pd.DataFrame] = get_unified_files(Caminho)
    unified_dir: str = os.path.join(Caminho, 'unified')

    # Persist unified files to Excel format
    save_unified_files_to_excel(unified_files, unified_dir)

def save_unified_files_to_excel(unified_files: Dict[str, pd.DataFrame], output_dir: str):
     # Create dir if not exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Save to Excel
    unified_xlsx_path = os.path.join(output_dir, 'unified_files.xlsx')
    with pd.ExcelWriter(unified_xlsx_path, engine = 'openpyxl') as writer:
        for file_key, df in unified_files.items():
            df = df.fillna('')

            illegal_chars_pattern = r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]'

            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str).str.replace(illegal_chars_pattern, '', regex=True)

                # Replace m² to empty string
                if col == 'Area':
                    df[col] = df[col].str.replace(' m²', '', regex=False)
                    df[col] = df[col].str.replace(' m', '', regex=False)

            df.to_excel(writer, sheet_name = file_key, index = False)

def get_unified_files(Caminho: str) -> dict[str, pd.DataFrame]:
    sub_dirs_full_path: list[str] = [entry.path for entry in os.scandir(Caminho) if entry.is_dir()]

    unified_files: dict[str, pd.DataFrame] = dict()

    last_year: str = str(get_higher_year(sub_dirs_full_path))

    # Search for each subdirectory
    for path in sub_dirs_full_path:
        files_path: list[str] = [file.path for file in os.scandir(path) if not file.is_dir()]
        files_name: list[str] = [os.path.basename(file_path) for file_path in files_path]

        # Get each file in the subdirectory
        for file_path, file_name in zip(files_path, files_name):
            if 'geral' in file_name and last_year not in file_name:
                continue

            delimiter: str = ' '
            parts: list[str] = file_name.split('_')
            parts_without_last_element: list[str] = parts[:-1]
            new_file_name: str = delimiter.join(parts_without_last_element) 

            file_dataframe: pd.DataFrame = pd.read_csv(file_path, sep=';', encoding='latin1', dtype=str)

            if 'geral' in file_name:
                print(f"[SUCESSO]: UNIFICANDO ARQUIVO {file_name}...")
                # Filter CNPJ and remove duplicates by getting the last occurrence

                # Ensure 'CNPJ_Fundo' is a DateTime column
                file_dataframe['Data_Referencia'] = pd.to_datetime(file_dataframe['Data_Referencia'])

                # Sort by 'Data_Referencia' in descending order and drop duplicates
                file_dataframe = file_dataframe.sort_values(by='Data_Referencia', ascending=False).drop_duplicates(subset='CNPJ_Fundo_Classe', keep='first')
                file_dataframe['Data_Referencia'] = file_dataframe['Data_Referencia'].dt.strftime('%Y-%m-%d')
            
            if file_dataframe.__contains__('CNPJ_Fundo'):
                file_dataframe['CNPJ_Fundo_Classe'] = file_dataframe['CNPJ_Fundo']
                file_dataframe.drop('CNPJ_Fundo', axis=1, inplace=True)

            if file_dataframe.__contains__('Nome_Fundo'):
                file_dataframe['Nome_Fundo_Classe'] = file_dataframe['Nome_Fundo']
                file_dataframe.drop('Nome_Fundo', axis=1, inplace=True)

            if not file_dataframe.__contains__('Tipo_Fundo_Classe'):
                file_dataframe['Tipo_Fundo_Classe'] = 'Fundo'

            if not (new_file_name in unified_files):
                unified_files[new_file_name] = file_dataframe
                continue

            if set(unified_files[new_file_name].columns) != set(file_dataframe.columns):
                raise ValueError(
                    f"File path: {file_path}\n"
                    f"Column mismatch in file '{file_name}'.\n"
                    f"Expected: {list(unified_files[new_file_name].columns)}\n"
                    f"Got:      {list(file_dataframe.columns)}\n"
                    f"Differ Column: {set(unified_files[new_file_name].columns)  - set(file_dataframe.columns)} \n"
                    f"Differ Column: {set(file_dataframe.columns) - set(unified_files[new_file_name].columns)}  \n"
                )

            combined_dataframe = pd.concat([unified_files[new_file_name], file_dataframe], ignore_index=True)
            unified_files[new_file_name] = combined_dataframe 

    return unified_files

def get_higher_year(files_url: list[str]) -> int:
    years: list[int] = []

    for file_url in files_url:
        year: str = file_url.split('/').pop().split('_').pop().split('.').pop(0)
        if year.isdigit():
            years.append(int(year))

    if not years:
        return 0

    return max(years)

def filter_by_missing_years(files_url: list[str], output_dir: str) -> list[str]:
    filtered_urls: list[str] = []
    years: list[str] = []

    if not os.path.exists(output_dir):
        return files_url

    # Get all years that can be downloaded
    for file_url in files_url:
        year: str = file_url.split('/').pop().split('_').pop().split('.').pop(0)
        years.append(year)

    # Filter already download years
    for year in years:
        has_year: bool = False
        for dir_name in os.listdir(output_dir):
            if not os.path.isdir(os.path.join(output_dir, dir_name)):
               continue 

            if year in dir_name:
                has_year = True
                break

        if has_year:
            continue

        for file_url in files_url:
            if year in file_url:
                filtered_urls.append(file_url)

    return filtered_urls 

def unzip_file(path: str):
    dir_name = path.split('.').pop(0)

    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(dir_name)
        print(f"[SUCESSO]: ARQUIVO {path} EXTRAÍDO")

def get_path_from(url: str, output_dir: str) -> str:
    return os.path.join(output_dir, url.split('/').pop())

def download_zip_file(url: str, output_dir: str):
    file_path: str = get_path_from(url, output_dir)
    try:
        response: Response = requests.get(url)
        response.raise_for_status()

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(file_path, 'wb') as file:
            file.write(response.content)

        print(f"[SUCESSO]: ARQUIVO {file_path} BAIXADO")

    except requests.exceptions.RequestException as e:
        print(f"[ERRO]: UM ERRO OCORREU DURANTE O DOWNLOAD: {e}")

def get_files_url(soup: BeautifulSoup, base_url: str) -> list[str]:
    links: list[str] = []

    for a_tag in soup.find_all('a'):
        if not isinstance(a_tag, Tag):
            print("[ERRO]: FALHA AO ENCONTRAR TAG <a>")
            continue
    
        href = a_tag.get('href')
    
        if not isinstance(href, str):
            continue 
    
        if not href.endswith('.zip'):
            continue
        
        links.append(base_url + href)

    return links
    
main()

