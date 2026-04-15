# PERUpetro — Pipeline ETL de Producción Diaria de Hidrocarburos

Sistema Python que automatiza la extracción, transformación y carga (ETL) de los datos de producción diaria de hidrocarburos publicados por [PeruPetro](https://www.perupetro.com.pe). Genera archivos CSV mensuales normalizados listos para análisis.

---

## Descripción general

El sistema extrae dos tipos de datos desde el portal de PeruPetro:

| Tipo | Descripción | Unidad |
|------|-------------|--------|
| **BLS** | Producción de hidrocarburos líquidos (petróleo + líquidos de gas natural) | Barriles (BLS) |
| **MPC** | Producción de gas natural | Miles de pies cúbicos (MPC) |

Cada ejecución produce un CSV por tipo, consolidando la información mensual de todos los lotes petroleros del país, enriquecida con zona geográfica y nombre de operador.

---

## Estructura del repositorio

```
PERUpetro/
├── perupetro_diario.py         # Núcleo del pipeline ETL
├── actualizar_lista_operador.py # Script auxiliar para actualizar operadores
├── utilidad.py                  # Configuración global, logger, helpers
├── zone_lote.csv                # Catálogo: LOTE → ZONA geográfica
├── lista_operador.csv           # Catálogo: LOTE → OPERADOR (se autoactualiza)
├── pdf/                         # Carpeta temporal de PDFs descargados
├── data/                        # Carpeta de salida de CSVs resultantes
│   └── <YYYYMM>01/
│       ├── peru_petro_hidrocarburosliquidos_d_<mes>_<año>.csv
│       └── peru_petro_gasnatural_d_<mes>_<año>.csv
└── logs/                        # Archivos de log por ejecución
```

---

## Componentes principales

### `perupetro_diario.py` — Clase `PeruPetroD`

Núcleo del proceso. Hereda de `Base` (definida en `utilidad.py`) y orquesta todo el flujo ETL.

| Método | Rol |
|--------|-----|
| `extract_page_content` | Descarga el HTML de la página de producción de PeruPetro |
| `extract_data` | Parsea el HTML y obtiene la lista de PDFs por mes (BLS o MPC) |
| `download_pdf` | Descarga cada PDF al directorio `pdf/` con nombre estandarizado |
| `read_bls_pdf_table` | Parsea la tabla BLS del PDF con Camelot y construye el DataFrame |
| `read_mpc_pdf_table` | Parsea la tabla MPC del PDF con Camelot y construye el DataFrame |
| `clean_columns` | Normaliza encabezados de columnas BLS (petróleo + LGN) |
| `extract_values_mpc` | Extrae lotes, operadores y valores de la tabla MPC |
| `format_df_to_mpc` | Arma el DataFrame MPC con columnas normalizadas |
| `delete_columns` | Elimina columnas irrelevantes (TOTAL, DIA, vacías) |
| `process_table_bls` | Orquesta la descarga y parseo BLS completo |
| `process_table_mpc` | Orquesta la descarga y parseo MPC completo y sincroniza operadores |
| `run` | Punto de entrada; valida estado y ejecuta BLS + MPC |

### `utilidad.py`

Configuración global del sistema:

- URL objetivo de PeruPetro y headers HTTP simulando navegador.
- Rutas de carpetas (`pdf/`, `data/`, `logs/`, `wsresult`).
- XPaths para extraer enlaces BLS y MPC del HTML.
- Diccionario de meses EN→ES para normalizar los nombres de período.
- Funciones de generación de nombres de archivo y validación de carpeta de salida.
- Clase abstracta `Base` con logger (`TimedRotatingFileHandler`) y creación de carpetas.

### `actualizar_lista_operador.py`

Script auxiliar que puede ejecutarse de forma independiente para:

- Descargar los PDFs MPC del mes actual.
- Extraer las relaciones `(LOTE, OPERADOR)` de las tablas.
- Actualizar `lista_operador.csv` sin ejecutar el pipeline BLS completo.

---

## Archivos de referencia (catálogos)

### `zone_lote.csv`

Mapeo estático de lote a zona geográfica del Perú:

```
ZONA         | Ejemplos de lotes
-------------|----------------------------------
SELVA NORTE  | 192, 95, 67, 8, 1-AB
SELVA CENTRAL| 131, 31-C, 31B/D
SELVA SUR    | 88, 56, 57
NOROESTE     | I, II, III, IV, V, VI, VII, IX, X
ZÓCALO       | Z-1, Z-6, Z-2B, Z-69
```

### `lista_operador.csv`

Mapeo dinámico de lote a empresa operadora. Se crea la primera vez y se actualiza automáticamente con cada ejecución MPC.

---

## Flujo completo de ejecución

```
python perupetro_diario.py
```

### Paso 1 — Control de ejecución (`verify_folder_content`)

- Calcula el período objetivo (mes anterior al mes actual).
- Verifica si los dos CSV de salida ya existen en `data/<periodo>/` o en el path de servidor `wsresult`.
- Si ya están: termina con mensaje `"Archivos ya actualizados"`.
- Si no: crea las carpetas necesarias y continúa.

### Paso 2 — Scraping web (`extract_page_content` + `extract_data`)

- Realiza una petición GET a la página de producción diaria de PeruPetro (verificación SSL desactivada).
- Extrae el año de referencia del texto de la página.
- Usa XPath para obtener:
  - **Columna 2** de la tabla HTML → enlaces PDF BLS.
  - **Columna 3** de la tabla HTML → enlaces PDF MPC.
  - Texto de celdas → nombre del mes en inglés, convertido a abreviatura en español.
- Resultado: lista `[[MES_AÑO, URL_PDF], ...]`, ej. `[["ENE2026", "https://...pdf"], ...]`.

### Paso 3 — Descarga de PDFs (`download_pdf`)

- Para cada entrada de la lista anterior, descarga el PDF.
- Lo guarda en `pdf/` con nombre convencional:
  - BLS: `peru_petro_hidrocarburosliquidos_d_<mes>_<año>.pdf`
  - MPC: `peru_petro_gasnatural_d_<mes>_<año>.pdf`
- Actualiza la lista con la ruta local del PDF descargado.

### Paso 4 — Parseo de tablas con Camelot

#### Tablas BLS (`read_bls_pdf_table`)

1. Lee la página 1 del PDF con Camelot en modo `lattice`.
2. Detecta si la tabla está partida en dos (cuenta columnas TOTAL):
   - Si está completa: usa el único DataFrame.
   - Si está partida: concatena las dos tablas horizontalmente.
3. Limpia la tabla (`clean_columns`):
   - Elimina filas de cabecera intermedias.
   - Normaliza nombres de columna eliminando espacios/puntos.
   - Busca la zona en `zone_lote.csv` para cada lote.
   - Detecta el punto de transición de petróleo a LGN (columna vacía).
   - Construye encabezados normalizados:
     - `PETRÓLEO (BLS) | ZONA | OPERADOR | LOTE`
     - `LÍQUIDOS DE GAS NATURAL (BLS) | ZONA | OPERADOR | LOTE`
4. Conserva solo la última fila de datos (producción del cierre del mes).
5. Elimina columnas TOTAL, DIA y vacías.
6. Inserta columna `FECHA`.

#### Tablas MPC (`read_mpc_pdf_table`)

1. Lee la página 1 del PDF con Camelot en modo `lattice`.
2. Extrae (`extract_values_mpc`):
   - Nombres de lote.
   - Nombres de operador (desde el encabezado de la columna; si faltan, consulta `lista_operador.csv`; si no existen ahí, asigna `"SIN OPERADOR"`).
   - Valores de producción (última fila de datos).
3. Construye encabezados (`format_df_to_mpc`):
   - `GAS NATURAL (MPC) | ZONA | OPERADOR | LOTE`
4. Inserta columna `FECHA`.

### Paso 5 — Consolidación y exportación

- Concatena todos los DataFrames mensuales en uno solo.
- Rellena valores nulos con `ND`.
- Exporta el CSV con separador `;` y codificación `utf-8-sig`:
  - `data/<YYYYMM>01/peru_petro_hidrocarburosliquidos_d_<mes>_<año>.csv`
  - `data/<YYYYMM>01/peru_petro_gasnatural_d_<mes>_<año>.csv`

### Paso 6 — Limpieza

- Elimina cada PDF temporal una vez procesado para no acumular archivos en `pdf/`.

### Paso 7 — Sincronización de operadores

- Durante el procesamiento MPC, recopila los pares `(LOTE, OPERADOR)` encontrados en los encabezados de los PDFs.
- Llama a `procesar_actualizacion_faltantes` (`actualizar_lista_operador.py`):
  - Si el lote ya existe en `lista_operador.csv` con operador vacío/ND/SIN OPERADOR → lo corrige.
  - Si el lote no existe → lo agrega como nueva fila.
- Guarda el CSV actualizado.

---

## Flujo del script auxiliar `actualizar_lista_operador.py`

Se puede ejecutar de forma independiente con:

```
python actualizar_lista_operador.py
```

Flujo:
1. Instancia `PeruPetroD`.
2. Extrae los enlaces MPC del mes actual.
3. Descarga los PDFs MPC.
4. Parsea las tablas y extrae pares `(LOTE, OPERADOR)`.
5. Actualiza `lista_operador.csv` con los datos encontrados.

---

## Formato de los CSV de salida

### CSV Líquidos (BLS)

| FECHA | PETRÓLEO (BLS) \| NOROESTE \| UNNA \| III | LÍQUIDOS DE GAS NATURAL (BLS) \| SELVA SUR \| PLUSPETROL \| 88 | … |
|-------|----------------------------------------|---------------------------------------------------------------|---|
| ENE2026 | 1234 | 5678 | … |

### CSV Gas Natural (MPC)

| FECHA | GAS NATURAL (MPC) \| SELVA SUR \| PLUSPETROL \| 88 | GAS NATURAL (MPC) \| SELVA SUR \| REPSOL \| 57 | … |
|-------|-----------------------------------------------------|------------------------------------------------|---|
| ENE2026 | 9876 | 5432 | … |

---

## Logging

- Cada ejecución genera un archivo `.log` en `logs/` rotado por día.
- Nivel DEBUG en archivo, nivel INFO en consola.
- Los logs se conservan hasta 14 días (`backupCount=14`).

---

## Dependencias principales

| Librería | Uso |
|----------|-----|
| `requests` | Descarga del HTML y PDFs |
| `lxml` | Parseo XPath del HTML |
| `camelot-py` | Extracción de tablas desde PDFs |
| `pandas` | Transformación y exportación de datos |
| `fitz` (PyMuPDF) | Soporte PDF (usado por Camelot) |
| `tqdm` | Barras de progreso en consola |
| `python-dateutil` | Cálculo de mes anterior |
| `re` | Limpieza y normalización de texto |
