import pandas as pd
import calendar
import datetime
import fitz
import requests
from lxml import html
from collections import defaultdict
import os
import warnings
import urllib3
from tqdm import tqdm 
import camelot
import shutil
import re
import utilidad as util
from utilidad import Base

try:
    from actualizar_lista_operador import procesar_actualizacion_faltantes
except ImportError:
    procesar_actualizacion_faltantes = None

warnings.simplefilter('ignore', urllib3.exceptions.InsecureRequestWarning)

class PeruPetroD(Base):
    
    def __init__(self):
        super(PeruPetroD, self).__init__()
        self.dictionary_bls = defaultdict(dict)
        self.dictionary_mpc = defaultdict(dict)
        self.range_col_lgn = 0 

    def read_csv_faltantes(self):
        csv_path = util.csv_path_for_faltantes
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path, delimiter=",", encoding='utf-8-sig')
            except:
                df = pd.read_csv(csv_path, delimiter=",", encoding='latin-1')
            
            df['LOTE'] = df['LOTE'].astype(str).str.strip()
            return df[['LOTE', 'OPERADOR']]
        return pd.DataFrame(columns=['LOTE', 'OPERADOR'])

    def clean_header_string(self, text):
        if not text: return ""
        text = str(text).upper()
        clean = re.sub(r'\s+', ' ', text).strip()
        clean = clean.replace(' : ', ':').replace(': ', ':').replace(' :', ':').replace('.', '')
        return clean

    def extract_page_content(self):
        response = requests.get(util.url, headers=util.headers, verify = False)
        return response.text
    
    def extract_data(self, type_data):
        data_hidro = []
        page_content = self.extract_page_content()
        tree = html.fromstring(page_content)
        
        texto_produccion = tree.xpath("//span[contains(text(), 'Producción al cierre de mes en el')]/text()")
        
        year_found = None
        if texto_produccion:
            match_year = re.search(r'(\d{4})', texto_produccion[0])
            if match_year:
                year_found = match_year.group(1)
        
        if not year_found:
            year_found = str(datetime.datetime.now().year)

            
        xpath = util.xpath_to_bls_data if type_data == 'bls' else util.xpath_to_mpc_data
        xpath_content_data = tree.xpath(xpath)
        date_text_to_extract = tree.xpath(util.xpath_elemnt_to_extract)
        
        meses_es = {
            'JAN': 'ENE', 'FEB': 'FEB', 'MAR': 'MAR', 'APR': 'ABR',
            'MAY': 'MAY', 'JUN': 'JUN', 'JUL': 'JUL', 'AUG': 'AGO',
            'SEP': 'SET', 'OCT': 'OCT', 'NOV': 'NOV', 'DEC': 'DIC'
        }
        
        for i in range(0, len(xpath_content_data)):
            raw_text = str(date_text_to_extract[i]).upper().strip()
            month_abbr_en = re.sub(r'[^A-Z]', '', raw_text)[:3]
            month_abbr_es = meses_es.get(month_abbr_en, month_abbr_en)
            
            month_id = f"{month_abbr_es}{year_found}"
            data_hidro.append([month_id, util.url_base + xpath_content_data[i]])
            
        return data_hidro

    def download_pdf(self, data, type_data):
        file_name = util.file_name(type_data)
        for value in tqdm(data, desc = f'Downloading {type_data.upper()} PDFs', total = len(data), leave = True):
            try:
                pdf_path = util.pdf_folder_path + file_name + value[0][:3].lower() + '_' + value[0][3:] + '.pdf' 
                r = requests.get(value[1], verify=False)
                with open(pdf_path, 'wb') as f: f.write(r.content)
                value[1] = pdf_path
            except Exception as e: self.log.info(f'Error en descarga: {e}')
        return data

    def read_csv_of_zones(self):
        csv_path = util.csv_path_for_zone_lote
        try:
            df = pd.read_csv(csv_path, delimiter=",", encoding='utf-8-sig')
        except:
            df = pd.read_csv(csv_path, delimiter=",", encoding='latin-1')
        return df[['LOTE', 'ZONA']]

    def extract_values_mpc(self, df):
        df.columns = df.iloc[0]
        df = df.drop(0)
        values_list = [item for col in df.columns if '\n' in str(col) for item in str(col).split('\n')]
        lotesraw = [item for item in values_list if 'Lote' in item]
        lotes_clean = [re.sub(r'Lote|[:.\s]', '', x).replace('_', '/') for x in lotesraw]
        zones_from_pdf = [item for item in values_list if 'Lote' not in item and item != 'Mcf' and item != 'TOTAL' and item.strip() != '']
        
        if len(zones_from_pdf) == len(lotesraw):
            zones = zones_from_pdf
        else:
            df_faltantes = self.read_csv_faltantes()
            new_zones = []
            for i, l_clean in enumerate(lotes_clean):
                if i < len(zones_from_pdf):
                    new_zones.append(zones_from_pdf[i])
                else:
                    op = df_faltantes.loc[df_faltantes['LOTE'] == l_clean, 'OPERADOR'].values
                    if op.size > 0:
                        new_zones.append(op[0])
                    else:
                        new_zones.append("SIN OPERADOR")
            zones = new_zones
        values = []
        fila_datos = df.iloc[-1].tolist()
        for col in fila_datos:
            items = str(col).split('\n') if '\n' in str(col) else [str(col)]
            for item in items:
                val = item.replace(',', '').strip()
                if val != '' and not any(w in val.upper() for w in ['TOTAL', 'DIA']):
                    values.append(val)
        return lotesraw, zones, values[:len(lotesraw)]

    def format_df_to_mpc(self, lotes, zones, values, month):
        df_zones_ref = self.read_csv_of_zones()
        columns_name = []
        for i in range(len(zones)):
            lote_id_norm = re.sub(r'Lote|[:.\s]', '', lotes[i]).replace('_', '/')
            zone_info = df_zones_ref.loc[df_zones_ref['LOTE'] == lote_id_norm, 'ZONA'].values
            final_zone = zone_info[0] if zone_info.size > 0 else "SIN ZONA"
            raw_h = f'GAS NATURAL (MPC) | {final_zone} | {zones[i]} | {lote_id_norm}'
            columns_name.append(self.clean_header_string(raw_h))
        df = pd.DataFrame([values], columns=columns_name)
        df.insert(0, 'FECHA', month)
        return df

    def read_mpc_pdf_table(self, file, month):
        tablas = camelot.read_pdf(file, pages="1", flavor="lattice")
        if not tablas: return pd.DataFrame()
        lotes, zones, values = self.extract_values_mpc(tablas[0].df)
        return self.format_df_to_mpc(lotes, zones, values, month)

    def read_bls_pdf_table(self, file, month):
        tablas = camelot.read_pdf(file, pages="1", flavor="lattice")
        if len(tablas) > 0:
            df_1 = tablas[0].df
            total_count = sum(df_1.iloc[1].str.contains('TOTAL'))
            if total_count == 2:
                result = df_1
            elif total_count < 2 and len(tablas) > 1:
                df_2 = tablas[1].df
                df_2 = self.expanded_table_rows(df_2)
                result = pd.concat([df_1, df_2], axis=1)
                total_count = 3
            else:
                result = df_1
            return self.clean_columns(result, total_count, month)
        return pd.DataFrame()

    def clean_columns(self, df, start_column, month):
        df = df.iloc[1:, 1:].reset_index(drop = True)
        df.columns = df.iloc[0]
        df = df.drop(0).reset_index(drop = True)
        df_columns_zones = [str(col).replace('\n', ' ') for col in list(df.columns)]
        for i in range(1, len(df_columns_zones)):
            if df_columns_zones[i] == '': df_columns_zones[i] = df_columns_zones[i-1]
        df.columns = df.iloc[0]
        df = df.drop(0).reset_index(drop = True)
        df_columns_lotes = [re.sub(r'[.\s]', '', str(col)).replace('_', '/') for col in df.columns]
        df_zones = self.read_csv_of_zones()
        df_columns_aux = []
        range_lgn = False
        for i in range(0, len(df_columns_lotes)):
            if str(df_columns_lotes[i]) == '':
                df_columns_aux.append('')
                range_lgn = True
            else:
                lote_id = df_columns_lotes[i].split('|')[1].strip() if '|' in df_columns_lotes[i] else df_columns_lotes[i].strip()
                zone = df_zones.loc[df_zones['LOTE'] == lote_id.rstrip('.'), 'ZONA'].values
                if zone.size > 0:
                    prefix = 'LÍQUIDOS DE GAS NATURAL (BLS)' if range_lgn else 'PETRÓLEO (BLS)'
                    header_liq = f'{prefix} | {zone[0]} | {df_columns_zones[i]} | {df_columns_lotes[i]}'
                    df_columns_aux.append(self.clean_header_string(header_liq))
                else:
                    df_columns_aux.append(df_columns_lotes[i])
        df.columns = df_columns_aux
        df = self.delete_columns(df)
        if not df.empty: 
            df = df.iloc[[-1]].reset_index(drop=True)
            df = df.apply(lambda x: x.astype(str).str.replace(',', '', regex=False))
            
        df.insert(0, 'FECHA', month)
        return df

    def delete_columns(self, df):
        cols = [c for c in df.columns if c == '' or re.search(r"DIA|TOTAL", str(c), re.IGNORECASE)]
        return df.drop(columns = cols, errors = 'ignore')

    def expanded_table_rows(self, df):
        rows = []
        for col in df.columns:
            v = list(df[col])
            if len(v) > 1 and '\n' in str(v[1]):
                s = str(v[1]).rsplit('\n', 1)
                v.pop(1); v.insert(1, s[0]); v.insert(2, s[1])
            else:
                v.insert(2, '')
            rows.append(v) 
        return pd.DataFrame(rows).T

    def process_table_bls(self):
        data = self.download_pdf(self.extract_data('bls'), 'bls')
        dfs = []
        for p in tqdm(data, desc='Procesando BLS'):
            df_bls = self.read_bls_pdf_table(p[1], p[0])
            if not df_bls.empty:
                dfs.append(df_bls)
            if os.path.exists(p[1]): os.remove(p[1])
        if dfs:
            res = pd.concat(dfs, ignore_index=True, sort=False).fillna('ND')
            print("\nResultados Líquidos (BLS):")
            print(res.head()) 
            res.to_csv(f'{util.csv_result_path}{util.generate_file_name_bls(data[-1][0])}', sep=";", index=False, encoding='utf-8-sig')

    def process_table_mpc(self):
        data = self.download_pdf(self.extract_data('mpc'), 'mpc')
        dfs = []
        relaciones_encontradas = []

        for p in tqdm(data, desc='Procesando MPC'):
            df_mpc = self.read_mpc_pdf_table(p[1], p[0])
            if not df_mpc.empty:
                dfs.append(df_mpc)
                if procesar_actualizacion_faltantes:
                    cols_gas = [c for c in df_mpc.columns if "GAS NATURAL (MPC)" in str(c)]
                    for col in cols_gas:
                        partes = col.split('|')
                        if len(partes) >= 4:
                            operador = partes[2].strip()
                            lote = partes[3].strip()
                            if (lote, operador) not in relaciones_encontradas:
                                relaciones_encontradas.append((lote, operador))

            if os.path.exists(p[1]): os.remove(p[1])

        if dfs:
            res = pd.concat(dfs, ignore_index=True, sort=False).fillna('ND')
            print("\nResultados Gas Natural (MPC):")
            print(res.head())
            res.to_csv(f'{util.csv_result_path}{util.generate_file_name_mpc(data[-1][0])}', sep=";", index=False, encoding='utf-8-sig')

            if relaciones_encontradas and procesar_actualizacion_faltantes:
                print("\nSincronizando lista de operadores...")
                procesar_actualizacion_faltantes(relaciones_encontradas)

    def run(self):
        if util.verify_folder_content():
            try:
                self.process_table_bls()
                self.process_table_mpc()
            except Exception as e: 
                self.log.info(f"Error durante el procesamiento: {e}")
                print(f"Error: {e}")
        else:
            self.log.info('Archivos ya actualizados.')
            print("Archivos ya actualizados.")

if __name__ == '__main__': 
    perupetro = PeruPetroD()
    perupetro.run()