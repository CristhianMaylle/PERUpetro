import pandas as pd
import os
import sys
import utilidad as util

# Forzamos a Python a encontrar los archivos en la carpeta actual
ruta_actual = os.path.dirname(os.path.abspath(__file__))
if ruta_actual not in sys.path:
    sys.path.append(ruta_actual)

try:
    from perupetro_diario import PeruPetroD
except ImportError:
    # Si sale este error, es porque el archivo perupetro_diario.py no está en la misma carpeta
    PeruPetroD = None

def procesar_actualizacion_faltantes(relaciones_pdf):
    csv_path = util.csv_path_for_faltantes
    
    if not os.path.exists(csv_path):
        print(f"Aviso: No se encontró el archivo en {csv_path}. Se creará uno nuevo.")
        df_faltantes = pd.DataFrame(columns=['LOTE', 'OPERADOR'])
    else:
        try:
            df_faltantes = pd.read_csv(csv_path, delimiter=",", encoding='utf-8-sig')
        except:
            df_faltantes = pd.read_csv(csv_path, delimiter=",", encoding='latin-1')

    df_faltantes['LOTE'] = df_faltantes['LOTE'].astype(str).str.strip()
    df_faltantes['OPERADOR'] = df_faltantes['OPERADOR'].astype(str).str.strip()

    conteo_actualizados = 0
    conteo_nuevos = 0

    for lote_nuevo, operador_nuevo in relaciones_pdf:
        existe_lote = df_faltantes['LOTE'] == lote_nuevo
        
        if existe_lote.any():
            idx = df_faltantes.index[existe_lote][0]
            op_actual = str(df_faltantes.at[idx, 'OPERADOR']).upper().strip()
            
            if op_actual in ["SIN OPERADOR", "ND", "NAN", "", "NONE", "UNKNOWN"]:
                df_faltantes.at[idx, 'OPERADOR'] = operador_nuevo
                conteo_actualizados += 1
        else:
            nueva_fila = pd.DataFrame({'LOTE': [lote_nuevo], 'OPERADOR': [operador_nuevo]})
            df_faltantes = pd.concat([df_faltantes, nueva_fila], ignore_index=True)
            conteo_nuevos += 1

    if conteo_actualizados > 0 or conteo_nuevos > 0:
        df_faltantes.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"ACTUALIZACIÓN COMPLETADA:")
        print(f"- Operadores corregidos: {conteo_actualizados}")
        print(f"- Nuevos lotes agregados: {conteo_nuevos}")
    else:
        print("Sincronización finalizada: El archivo lista_operador.csv ya estaba al día.")

def ejecutar_correccion_operador():
    if PeruPetroD is None:
        print("Error crítico: No se puede ejecutar porque perupetro_diario.py no es accesible.")
        return

    pp = PeruPetroD()
    print("Obteniendo datos actuales de PeruPetro...")
    
    data_mpc = pp.extract_data('mpc') 
    pdfs = pp.download_pdf(data_mpc, 'mpc')
    
    relaciones_encontradas = []
    
    for p in pdfs:
        df_mes = pp.read_mpc_pdf_table(p[1], p[0])
        if not df_mes.empty:
            cols_gas = [c for c in df_mes.columns if "GAS NATURAL (MPC)" in str(c)]
            for col in cols_gas:
                partes = col.split('|')
                if len(partes) >= 4:
                    operador = partes[2].strip()
                    lote = partes[3].strip()
                    if (lote, operador) not in relaciones_encontradas:
                        relaciones_encontradas.append((lote, operador))
    
    if relaciones_encontradas:
        procesar_actualizacion_faltantes(relaciones_encontradas)
    else:
        print("No se detectaron datos en los PDFs para actualizar.")

if __name__ == "__main__":
    ejecutar_correccion_operador()