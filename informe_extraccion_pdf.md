# Informe Técnico: Extracción de Datos desde PDFs de PeruPetro

**Sistema:** Pipeline ETL — Producción Diaria de Hidrocarburos  
**Fuente oficial:** [perupetro.com.pe](https://www.perupetro.com.pe/wps/portal/corporativo/PerupetroSite/estadisticas/producción%20hidrocarburos/producción%20diaria/)  
**Elaborado a partir del análisis del código fuente:** `perupetro_diario.py`, `utilidad.py`, `actualizar_lista_operador.py`

---

## 1. Contexto y Origen de los PDFs

PeruPetro S.A. publica mensualmente, en su portal web oficial, los reportes de producción diaria de hidrocarburos en formato PDF. Para cada mes se publican **dos tipos de documentos**:

| Tipo | Contenido | Unidad de medida |
|------|-----------|-----------------|
| **BLS** | Producción de hidrocarburos líquidos: petróleo crudo y líquidos de gas natural (LGN) | Barriles por día (BLS) |
| **MPC** | Producción de gas natural | Miles de pies cúbicos por día (MPC) |

Ambos tipos de PDF contienen tablas estructuradas con datos desagregados por lote petrolero. El sistema los descarga automáticamente scrapeando la página de PeruPetro, los procesa y consolida en archivos CSV mensuales.

---

## 2. Estructura General de los PDFs

Ambos tipos de PDF presentan una tabla en la **primera página** con el siguiente esquema visual:

```
┌─────────────────────────────────────────────────────────────────────┐
│              PRODUCCIÓN DE HIDROCARBUROS — [MES] [AÑO]             │
├──────────┬──────────────────┬──────────────────┬────────────────────┤
│  FECHA   │  LOTE  X         │  LOTE  Y         │  ...               │
│          │  OPERADOR A      │  OPERADOR B      │                    │
├──────────┼──────────────────┼──────────────────┼────────────────────┤
│  DÍA 1   │     1.234        │      567         │  ...               │
│  DÍA 2   │     1.250        │      580         │  ...               │
│   ...    │      ...         │      ...         │  ...               │
│  TOTAL   │    38.250        │   17.890         │  ...               │
└──────────┴──────────────────┴──────────────────┴────────────────────┘
```

La herramienta **Camelot** (modo `lattice`) se encarga de leer esta tabla y convertirla en un DataFrame de pandas para su posterior procesamiento.

---

## 3. PDF Tipo BLS — Hidrocarburos Líquidos

### 3.1. ¿Qué contiene este PDF?

El PDF BLS reporta la producción diaria de cada lote para dos categorías de hidrocarburo líquido:

- **Petróleo crudo** (parte izquierda de la tabla)
- **Líquidos de Gas Natural — LGN** (parte derecha de la tabla)

La tabla está dividida en bloques de columnas por lote. Cada bloque encabeza con el nombre del lote y, debajo, el nombre del operador.

### 3.2. Campos extraídos del PDF BLS

| Campo extraído | Ubicación en el PDF | Campo generado en el CSV |
|----------------|---------------------|--------------------------|
| **Identificador de lote** | Fila de encabezado (fila 2 de la tabla) | Parte del nombre de columna: `... \| LOTE` |
| **Nombre del operador** | Fila de encabezado (fila 3 de la tabla) | Parte del nombre de columna: `... \| OPERADOR \| ...` |
| **Valor de producción al cierre del mes** | Última fila de datos de la tabla (antes de TOTAL) | Valor numérico de la columna correspondiente |
| **Tipo de hidrocarburo** | Posición relativa de la columna en la tabla (antes/después de columna vacía) | Prefijo en el nombre de columna: `PETRÓLEO (BLS)` o `LÍQUIDOS DE GAS NATURAL (BLS)` |

### 3.3. ¿Por qué se extraen esos campos?

**Identificador de lote:** Permite individualizar la producción por concesión petrolera. Cada lote es una unidad de exploración/explotación con contrato propio ante PeruPetro, por lo que es el identificador primario de cualquier análisis sectorial.

**Nombre del operador:** Permite atribuir la producción a la empresa responsable de la extracción. Es necesario para análisis por empresa, cumplimiento regulatorio y estudios de concentración de mercado.

**Valor de producción al cierre del mes:** La fila final de la tabla contiene la producción acumulada hasta el último día con datos reportados en ese mes. Se usa el cierre (y no el promedio ni el total) para tener el dato más representativo del estado de producción al final del período.

**Tipo de hidrocarburo (petróleo vs. LGN):** El petróleo crudo y los líquidos de gas natural tienen mercados, precios y procesos de refinación distintos. Diferenciarlos en columnas separadas permite análisis específicos por tipo de producto.

### 3.4. Proceso de limpieza del PDF BLS

```
PDF BLS → Camelot (lattice, pág. 1)
    ↓
¿Tabla completa o dividida en dos?
    ├─ Completa (2 columnas TOTAL): usa el DataFrame tal cual
    └─ Dividida: concatena horizontalmente las dos tablas

    ↓
clean_columns():
    - Elimina fila vacía inicial y columna de fechas (fila/col 0)
    - Lee zona geográfica del encabezado (filas 1–2)
    - Lee nombre de lote del encabezado (fila 3)
    - Normaliza lotes: elimina espacios, puntos; reemplaza "_" por "/"
    - Busca zona en zone_lote.csv por código de lote
    - Detecta columna vacía → inicio del bloque LGN
    - Construye encabezado normalizado:
        "PETRÓLEO (BLS) | ZONA | OPERADOR | LOTE"
        "LÍQUIDOS DE GAS NATURAL (BLS) | ZONA | OPERADOR | LOTE"
    - Elimina columnas TOTAL, DIA y vacías (delete_columns)
    - Conserva únicamente la última fila de datos (producción al cierre)
    - Inserta columna FECHA

    ↓
DataFrame con una fila por mes y una columna por lote/tipo
```

### 3.5. Columnas del CSV resultante (BLS)

```
FECHA | PETRÓLEO (BLS) | NOROESTE | UNNA | III | LÍQUIDOS DE GAS NATURAL (BLS) | SELVA SUR | PLUSPETROL | 88 | ...
```

Ejemplo de fila:

| FECHA | PETRÓLEO (BLS) \| NOROESTE \| UNNA \| III | LÍQUIDOS DE GAS NATURAL (BLS) \| SELVA SUR \| PLUSPETROL \| 88 |
|-------|------------------------------------------|--------------------------------------------------------------|
| ENE2026 | 1234 | 5678 |

---

## 4. PDF Tipo MPC — Gas Natural

### 4.1. ¿Qué contiene este PDF?

El PDF MPC reporta la producción diaria de gas natural por lote. La estructura de la tabla es similar a la BLS, con columnas por lote y filas por día, pero todos los valores corresponden a una sola categoría de producto.

### 4.2. Campos extraídos del PDF MPC

| Campo extraído | Ubicación en el PDF | Campo generado en el CSV |
|----------------|---------------------|--------------------------|
| **Identificador de lote** | Encabezado de columna (sub-texto que contiene "Lote") | Parte del nombre de columna: `... \| LOTE` |
| **Nombre del operador** | Encabezado de columna (sub-texto que no es lote, Mcf ni TOTAL) | Parte del nombre de columna: `... \| OPERADOR \| ...` |
| **Valor de producción al cierre del mes** | Última fila de la tabla | Valor numérico de la columna correspondiente |

### 4.3. ¿Por qué se extraen esos campos?

**Identificador de lote:** Igual que en BLS, es la unidad contractual de referencia del sector. Sin él no es posible conocer la contribución individual de cada concesión.

**Nombre del operador:** Se extrae directamente del encabezado del PDF cuando está disponible. Cuando falta (lo cual puede ocurrir en algunos meses por variaciones de formato), el sistema consulta `lista_operador.csv` como respaldo. Si tampoco está ahí, asigna `"SIN OPERADOR"` para mantener la integridad estructural del CSV. Esto permite que el CSV sea siempre completo, aunque con valores incompletos marcados explícitamente.

**Valor de producción al cierre del mes:** Mismo criterio que BLS: el dato del cierre refleja el estado productivo al final del período reportado, y es el valor que PeruPetro considera representativo del mes.

### 4.4. Proceso de limpieza del PDF MPC

```
PDF MPC → Camelot (lattice, pág. 1)
    ↓
extract_values_mpc():
    - Toma la primera tabla del PDF
    - Establece primera fila como encabezado
    - Para cada columna con salto de línea (\n) en el encabezado:
        ├─ Sub-textos con "Lote" → lista de lotes raw
        ├─ Sub-textos sin "Lote", sin "Mcf" y no vacíos → lista de operadores
    - Limpia lotes: quita "Lote", ":", ".", espacios; reemplaza "_" por "/"
    - Verifica que cantidad de operadores == cantidad de lotes:
        └─ Si faltan operadores: consulta lista_operador.csv;
           si no está: asigna "SIN OPERADOR"
    - Extrae valores de la última fila de datos:
        ├─ Divide celdas con \n
        └─ Filtra valores vacíos y palabras "TOTAL", "DIA"

    ↓
format_df_to_mpc():
    - Busca zona geográfica de cada lote en zone_lote.csv
    - Si no encuentra: asigna "SIN ZONA"
    - Construye encabezado normalizado:
        "GAS NATURAL (MPC) | ZONA | OPERADOR | LOTE"
    - Inserta columna FECHA

    ↓
DataFrame con una fila por mes y una columna por lote
```

### 4.5. Columnas del CSV resultante (MPC)

```
FECHA | GAS NATURAL (MPC) | SELVA SUR | PLUSPETROL | 88 | GAS NATURAL (MPC) | SELVA SUR | REPSOL | 57 | ...
```

Ejemplo de fila:

| FECHA | GAS NATURAL (MPC) \| SELVA SUR \| PLUSPETROL \| 88 | GAS NATURAL (MPC) \| SELVA SUR \| REPSOL \| 57 |
|-------|-----------------------------------------------------|------------------------------------------------|
| ENE2026 | 9876 | 5432 |

---

## 5. Campos Comunes a Ambos Tipos de PDF

### 5.1. Campo `FECHA`

| Atributo | Detalle |
|----------|---------|
| Origen | No proviene del PDF directamente; se construye a partir del nombre del archivo PDF y el año extraído del texto de la página web de PeruPetro |
| Formato | `MMMAAAA` en español, por ejemplo `ENE2026`, `FEB2026` |
| Posición en CSV | Primera columna de cada fila |
| Razón | Permite identificar el período de la producción y facilita la serie de tiempo mensual al concatenar múltiples archivos |

### 5.2. Zona Geográfica

| Atributo | Detalle |
|----------|---------|
| Origen | No proviene del PDF directamente; se enriquece consultando `zone_lote.csv` por código de lote |
| Valores posibles | `SELVA NORTE`, `SELVA CENTRAL`, `SELVA SUR`, `NOROESTE`, `ZÓCALO` |
| Posición en CSV | Parte del nombre de columna: `... \| ZONA \| ...` |
| Razón | La zona geográfica es una dimensión analítica clave para estudios de producción regional, logística de transporte y supervisión ambiental. No aparece en los PDFs por lo que se incorpora desde el catálogo estático `zone_lote.csv` |

---

## 6. Campos Descartados y Razones

| Campo del PDF | Razón del descarte |
|---------------|--------------------|
| **Valores diarios** (días 1 al N-1) | El sistema conserva únicamente el dato de producción del último día del mes reportado (cierre). Los datos diarios intermedios no forman parte del CSV de salida |
| **Columnas `TOTAL`** | Son sumatorias calculadas dentro del propio PDF. Se descartan porque el CSV consolida el cierre, no el total acumulado mensual |
| **Columna `DIA`** | Contiene el número del día y sirve como índice interno de la tabla del PDF. No aporta valor analítico en el CSV de salida, donde la granularidad es mensual |
| **Columnas vacías** | Son separadores visuales dentro del PDF (delimitan bloques de lotes). No tienen contenido de datos |

---

## 7. Manejo de Valores Faltantes

| Situación | Tratamiento |
|-----------|-------------|
| Operador no encontrado en el PDF MPC | Se consulta `lista_operador.csv`; si tampoco está: `"SIN OPERADOR"` |
| Zona no encontrada en `zone_lote.csv` | Se asigna `"SIN ZONA"` |
| Celda vacía al concatenar múltiples meses | Se rellena con `"ND"` (No Disponible) |

---

## 8. Archivos de Catálogo de Soporte

### `zone_lote.csv`

Catálogo estático que mapea cada código de lote a su zona geográfica. Se utiliza como fuente de enriquecimiento durante el procesamiento de ambos tipos de PDF.

| Columna | Descripción |
|---------|-------------|
| `LOTE` | Código del lote (e.g. `88`, `Z-1`, `1-AB`) |
| `ZONA` | Zona geográfica del Perú (e.g. `SELVA SUR`, `NOROESTE`) |

Zonas registradas: **SELVA NORTE**, **SELVA CENTRAL**, **SELVA SUR**, **NOROESTE**, **ZÓCALO**  
Total de lotes catalogados: **41**

### `lista_operador.csv`

Catálogo dinámico que mapea cada lote a su empresa operadora. Se actualiza automáticamente con cada ejecución del pipeline MPC, extrayendo los operadores directamente de los encabezados de los PDFs.

| Columna | Descripción |
|---------|-------------|
| `LOTE` | Código del lote |
| `OPERADOR` | Nombre de la empresa operadora del lote |

---

## 9. Diagrama Resumido del Flujo de Extracción

```
Portal PeruPetro (HTML)
        │
        ├─ XPath columna 2 → URLs de PDFs BLS
        └─ XPath columna 3 → URLs de PDFs MPC
                │
                ▼
        Descarga de PDFs (requests)
                │
        ┌───────┴───────┐
        ▼               ▼
    PDF BLS          PDF MPC
    (Camelot)        (Camelot)
        │               │
        ▼               ▼
  Campos extraídos:   Campos extraídos:
  - Lote              - Lote
  - Operador          - Operador (PDF → lista_operador.csv → "SIN OPERADOR")
  - Producción        - Producción
    (cierre mes)        (cierre mes)
        │               │
        ▼               ▼
  Enriquecimiento con zone_lote.csv → ZONA
        │               │
        ▼               ▼
  Encabezado normalizado:
  "TIPO | ZONA | OPERADOR | LOTE"
        │               │
        ▼               ▼
  CSV BLS             CSV MPC
  (sep=";", utf-8-sig) (sep=";", utf-8-sig)
```

---

## 10. Conclusión

El sistema extrae de los PDFs de PeruPetro exclusivamente los campos que permiten construir una **serie de tiempo mensual de producción de hidrocarburos**, desagregada por lote, operador y zona geográfica. Los criterios de selección de campos responden a:

1. **Identificación de la unidad productiva:** lote y operador son los identificadores mínimos necesarios para atribuir la producción a una concesión y empresa específicas.
2. **Valor representativo del período:** se conserva el dato del cierre del mes, descartando los valores diarios intermedios y los totales calculados.
3. **Contexto geográfico:** la zona se enriquece desde un catálogo estático dado que los PDFs no incluyen esta dimensión explícitamente.
4. **Integridad del output:** los valores faltantes (operador o zona no encontrados) se marcan con etiquetas explícitas (`SIN OPERADOR`, `SIN ZONA`, `ND`) para preservar la estructura tabular del CSV sin pérdida de filas.
