# Torneos de Futbol

Este proyecto tiene como objetivo la extracción de datos de partidos de fútbol desde páginas web mediante técnicas de web scraping. Posteriormente, estos datos serán consultados a través de una API que proporcionará endpoints para realizar peticiones y obtener los datos de los partidos en formato JSON. La idea es utilizar estos datos para alimentar un data warehouse, utilizando PostgreSQL, y luego emplearlos en análisis o modelos de machine learning.

## Tecnologías Utilizadas

- **Web Scraping**: Se utilizaron las bibliotecas Requests y Beautiful Soup para la extracción de datos desde las páginas web.
  ![Requests](https://upload.wikimedia.org/wikipedia/commons/thumb/c/c3/Python-logo-notext.svg/300px-Python-logo-notext.svg.png)
  ![Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/_images/6.1.jpg)

- **API**: Se implementó la API utilizando FastAPI.
  ![FastAPI](https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png)

- **Base de Datos**: Se empleó PostgreSQL para el almacenamiento de datos, gestionado a través de SQL Alchemy y Psycopg2.
  ![PostgreSQL](https://upload.wikimedia.org/wikipedia/commons/thumb/2/29/Postgresql_elephant.svg/300px-Postgresql_elephant.svg.png)

- **Manipulación de Datos**: Para el proceso de ETL (Extract, Transform, Load), se utilizó la biblioteca Pandas.
  ![Pandas](https://miro.medium.com/max/1024/1*9iu8_mhn6Yzt3G8Pw_rfzA.png)

## Configuración del Entorno Virtual

1. Instalar `virtualenv`:
  ```
  pip install virtualenv
  ```
2. Crear un entorno virtual:
  ```
  virtualenv myenv
  ```
3. Activar el entorno virtual:
- En Windows:
  ```
  myenv\Scripts\activate
  ```
- En macOS/Linux:
  ```
  source myenv/bin/activate
  ```
4. Instalar las dependencias del proyecto:
  ```
  pip install -r requirements.txt
  ```

## Ejecución del Script para la Ingesta de Datos

Para realizar la ingesta de datos desde las páginas web, se debe ejecutar el siguiente script Python:

1. Navegar al directorio donde se encuentra el script:
   ```
   cd C:\Users\PC\Documents\Matias\torneos_primera_division_arg\scripts
   ```

2. Ejecutar el script con el año deseado como argumento (por ejemplo, 2024):
   ```
   python save_teams_matchs.py 2024
   ```

## Levantar el Servidor de la API

Para iniciar el servidor de la API y permitir que los usuarios realicen consultas a los datos, siga estos pasos:

1. Navegar al directorio donde se encuentra la API:
  ```
  cd C:\Users\PC\Documents\Matias\torneos_primera_division_arg\api
  ```
2. Iniciar el servidor utilizando Uvicorn:
  ```
  uvicorn main:app --reload
  ```


## Documentación de la API

### Obtener enlaces de los equipos
- **Descripción:** Obtiene los enlaces de los equipos.
- **Ruta:** `/teams/links`
- **Método HTTP:** GET
- **Respuesta Exitosa (200):** Lista de objetos JSON con los enlaces de los equipos.

### Obtener todos los equipos y sus datos de partidos
- **Descripción:** Obtiene todos los equipos y sus datos de partidos.
- **Ruta:** `/teams`
- **Método HTTP:** GET
- **Respuesta Exitosa (200):** Lista de objetos JSON con los datos de partidos de todos los equipos.

### Obtener equipos y sus datos de partidos para años y códigos específicos
- **Descripción:** Obtiene los equipos y sus datos de partidos para los años y códigos especificados.
- **Ruta:** `/teams`
- **Método HTTP:** POST
- **Parámetros de la Solicitud:**
- `years`: Lista de años (ejemplo: `[2023, 2024]`).
- `codes`: Lista de códigos de equipos (ejemplo: `["ABC", "DEF"]`).
- **Respuesta Exitosa (200):** Lista de objetos JSON con los datos de partidos de los equipos seleccionados.
