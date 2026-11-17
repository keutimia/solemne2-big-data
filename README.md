# Validaciones de transporte público: Kafka + PySpark

Proyecto que simula validaciones de tarjeta en el transporte público (Metro, Bus y Tren), las envía a Kafka, las guarda en un archivo JSON Lines y las analiza con PySpark.

## Flujo

1. **Productor Python** (`productor.py`): genera y publica eventos en un tópico de Kafka.
2. **Tópico de Kafka** (`validaciones_transporte`): organiza los eventos.
3. **Consumidor Python** (`consumidor.py`): recibe los eventos y los guarda en `eventos.jsonl` (un objeto JSON por línea).
4. **PySpark** (`analisis_pyspark.py`): lee el archivo una vez terminada la captura, limpia los datos y hace el análisis exploratorio con tres gráficos.

> Spark lee el archivo, no lee directamente desde Kafka ni usa Structured Streaming.

## Preguntas del análisis

1. **Descriptiva:** ¿Cuál es el monto típico de una validación y cómo se distribuye?
2. **Comparación:** ¿Qué transporte recauda más?
3. **Evolución:** ¿Cómo varían las validaciones por hora y por día?

## Datos del evento

| Campo | Tipo | Descripción |
|---|---|---|
| `id_evento` | texto | Identificador único (UUID) |
| `marca_temporal` | texto `YYYY-MM-DD HH:MM:SS` | Momento de la validación |
| `tipo_transporte` | categórica | Metro, Bus o Tren |
| `monto` | numérica (CLP) | Valor pagado; puede venir nulo |
| `tipo_tarjeta` | categórica | General, Estudiante o Adulto mayor |
| `id_tarjeta` | texto | Identificador de la tarjeta |

Los datos son **sintéticos** e incluyen reglas intencionales:
- El Metro tiene más viajes y tarifa mayor que el Bus.
- Las horas 7–9 y 18–20 concentran más viajes.
- Un 3 % de los eventos trae `monto` nulo (problema de calidad controlado, que se trata en PySpark).

## Requisitos

- Python 3.9 o superior
- Docker (para levantar Kafka) o una instalación local de Kafka
- Java 17 o superior (lo necesita PySpark)

## Librerías

| Librería | Versión | Uso | Instalación |
|---|---|---|---|
| `kafka-python` | 3.0.13 | Productor y consumidor | `pip install kafka-python` |
| `pyspark` | 4.2.1 | Carga, limpieza y análisis | `pip install pyspark` |
| `pandas` | 3.1.1 | Pasar resultados agregados a gráficos | `pip install pandas` |
| `matplotlib` | 3.11.1 | Visualizaciones | `pip install matplotlib` |

Todo junto:

```bash
pip install kafka-python pyspark pandas matplotlib
```

## Ejecución

Los pasos se hacen en este orden, cada uno en su propia terminal cuando se indica.

### 1. Levantar el broker de Kafka

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:latest
```

Queda escuchando en `localhost:9092`. Para detenerlo: `docker stop kafka`.

### 2. Iniciar el consumidor

En otra terminal, antes de enviar los eventos:

```bash
python consumidor.py
```

### 3. Ejecutar el productor

```bash
python productor.py
```

Por defecto envía 5.000 eventos al tópico `validaciones_transporte`. Opciones:

| Parámetro | Descripción | Por defecto |
|---|---|---|
| `--n-eventos` | Cantidad de eventos a generar | `5000` |
| `--topico` | Nombre del tópico | `validaciones_transporte` |
| `--servidor` | Dirección del broker | `localhost:9092` |
| `--semilla` | Semilla aleatoria (datos reproducibles) | `42` |

Ejemplo: `python productor.py --n-eventos 10000`

Al terminar muestra los eventos generados, los enviados y los errores detectados.

### 4. Detener el consumidor

Cuando el productor termine, se detiene el consumidor y queda listo el archivo `eventos.jsonl`.

### 5. Análisis con PySpark

```bash
python analisis_pyspark.py
```

## Solución de problemas

- **"No se pudo conectar con Kafka":** el broker no está corriendo o el puerto 9092 está ocupado. Revisa con `docker ps` y `docker logs kafka`.
- **Error con Java en PySpark:** verifica que Java 17 o superior esté instalado (`java -version`).

## Alternativa sin Kafka

`generador_eventos.py` genera los mismos eventos y los guarda directamente en `eventos.jsonl`, sin necesidad de broker. Sirve de respaldo para probar el análisis en PySpark.

```bash
python generador_eventos.py
```
