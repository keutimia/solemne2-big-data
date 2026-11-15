"""
Productor Kafka - Validaciones de transporte público.

Genera eventos sintéticos de validación de tarjeta (pago de pasaje) y los
publica en un tópico de Kafka en formato JSON.

Uso:
    python productor.py                      # 5.000 eventos (por defecto)
    python productor.py --n-eventos 10000    # otra cantidad
    python productor.py --topico validaciones --servidor localhost:9092

Dependencia:
    pip install kafka-python
"""

import argparse
import json
import random
import sys
import uuid
from datetime import datetime, timedelta

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Parámetros del modelo de datos sintético
# ---------------------------------------------------------------------------

# Variable categórica: tipo de transporte, con su tarifa base (CLP) y su peso
# (probabilidad relativa de ser usado). PATRÓN 1: el Metro tiene más viajes
# y tarifa mayor, por lo que debería acumular la mayor recaudación.
TRANSPORTES = {
    "Metro": {"tarifa": 800, "peso": 0.45},
    "Bus":   {"tarifa": 700, "peso": 0.40},
    "Tren":  {"tarifa": 1100, "peso": 0.15},
}

# Atributo adicional: tipo de tarjeta, con descuento sobre la tarifa base.
TARJETAS = {
    "General":    {"factor": 1.0, "peso": 0.70},
    "Estudiante": {"factor": 0.5, "peso": 0.25},
    "Adulto mayor": {"factor": 0.5, "peso": 0.05},
}

# PATRÓN 2: la cantidad de viajes depende de la hora. Las horas punta
# (7-9 y 18-20) tienen un peso mucho mayor que el resto.
HORAS_PUNTA = {7, 8, 9, 18, 19, 20}
PESO_HORA_PUNTA = 6
PESO_HORA_NORMAL = 1

# PROBLEMA DE CALIDAD (uno solo, controlado): porcentaje de eventos cuyo
# monto se envía como nulo (null), simulando una falla del validador.
PROB_MONTO_NULO = 0.03

DIAS_PERIODO = 7          # el periodo analizado abarca 7 días
N_TARJETAS = 1500         # cantidad de tarjetas distintas simuladas


# ---------------------------------------------------------------------------
# Generación de eventos
# ---------------------------------------------------------------------------

def elegir_ponderado(opciones):
    """Elige una clave del diccionario según su 'peso'."""
    claves = list(opciones.keys())
    pesos = [opciones[k]["peso"] for k in claves]
    return random.choices(claves, weights=pesos, k=1)[0]


def generar_marca_temporal(fecha_inicio):
    """Genera un instante dentro del periodo, favoreciendo las horas punta."""
    dia = random.randrange(DIAS_PERIODO)
    horas = list(range(24))
    pesos = [PESO_HORA_PUNTA if h in HORAS_PUNTA else PESO_HORA_NORMAL for h in horas]
    hora = random.choices(horas, weights=pesos, k=1)[0]
    return fecha_inicio + timedelta(
        days=dia,
        hours=hora,
        minutes=random.randrange(60),
        seconds=random.randrange(60),
    )


def generar_evento(fecha_inicio):
    """Construye un evento de validación (diccionario serializable a JSON)."""
    transporte = elegir_ponderado(TRANSPORTES)
    tarjeta = elegir_ponderado(TARJETAS)
    marca = generar_marca_temporal(fecha_inicio)

    monto = round(TRANSPORTES[transporte]["tarifa"] * TARJETAS[tarjeta]["factor"])

    # Problema de calidad: con probabilidad PROB_MONTO_NULO el monto se pierde.
    if random.random() < PROB_MONTO_NULO:
        monto = None

    return {
        "id_evento": str(uuid.uuid4()),                      # identificador
        "marca_temporal": marca.strftime("%Y-%m-%d %H:%M:%S"),  # marca temporal
        "tipo_transporte": transporte,                       # categórica
        "monto": monto,                                      # numérica (CLP)
        "tipo_tarjeta": tarjeta,                             # atributo adicional
        "id_tarjeta": f"T{random.randint(1, N_TARJETAS):05d}",  # atributo adicional
    }


# ---------------------------------------------------------------------------
# Programa principal
# ---------------------------------------------------------------------------

def parsear_argumentos():
    p = argparse.ArgumentParser(description="Productor Kafka de validaciones de transporte")
    p.add_argument("--n-eventos", type=int, default=5000,
                   help="cantidad de eventos a generar (mínimo requerido: 5000)")
    p.add_argument("--topico", default="validaciones_transporte",
                   help="nombre del tópico de Kafka")
    p.add_argument("--servidor", default="localhost:9092",
                   help="dirección del broker de Kafka")
    p.add_argument("--semilla", type=int, default=42,
                   help="semilla aleatoria para reproducir los datos")
    return p.parse_args()


def main():
    args = parsear_argumentos()
    random.seed(args.semilla)

    try:
        productor = KafkaProducer(
            bootstrap_servers=args.servidor,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            retries=3,
        )
    except KafkaError as e:
        print(f"No se pudo conectar con Kafka en {args.servidor}. "
              f"¿Está levantado el broker? Detalle: {e}")
        sys.exit(1)

    contadores = {"enviados": 0, "errores": 0}

    def al_exito(_metadata):
        contadores["enviados"] += 1

    def al_error(excepcion):
        contadores["errores"] += 1
        print(f"Error al enviar un evento: {excepcion}")

    # Periodo: los últimos DIAS_PERIODO días completos, desde las 00:00.
    hoy = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    fecha_inicio = hoy - timedelta(days=DIAS_PERIODO)

    print(f"Generando {args.n_eventos} eventos para el tópico '{args.topico}'...")

    for i in range(args.n_eventos):
        evento = generar_evento(fecha_inicio)
        productor.send(args.topico, value=evento).add_callback(al_exito).add_errback(al_error)

        if i < 3:  # pequeña muestra de lo que se envía
            print("Ejemplo:", evento)

    productor.flush()   # espera a que todos los mensajes se confirmen
    productor.close()

    print("-" * 50)
    print(f"Eventos generados : {args.n_eventos}")
    print(f"Eventos enviados  : {contadores['enviados']}")
    print(f"Errores detectados: {contadores['errores']}")


if __name__ == "__main__":
    main()
