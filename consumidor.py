# Consumidor Kafka - validaciones de transporte
#Recibe los eventos JSON que envia el productoe y los guarda en un archivo JSON Lines
# Al terminar informa la cantidad de eventos guardados y los mensajes que no pudieron interpretrarse
# Uso:
# python consumidor.py                          5.000 eventos esperados
# python consumidor.py --n-eventos 999          otra cantidad
# python consumidor.py --topico validaciones_transporte
# python consumidor.py --salida eventos.json
# DEpendencia: pip install kafka-python

import argparse
import json
import sys
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError
#----------------------------------
#Parametros generales
#----------------------------------
TIMEOUT_INACTIVIDAD = 15 #tiempo maximo sin actividad
N_MUESTRA = 5   #eventos de muestra que se imprimen
#----------------------------------
# Creacion del consumidor
#----------------------------------

def crear_consumidor(servidor, topico, grupo):
    #Crea y devuelve un KafkaConsumer
    try:
        consumer = KafkaConsumer(
            topico,
            bootstrap_servers=servidor,
            group_id=grupo,
            # Leer desde el inicio
            auto_offset_reset="earliest",
            # Confirmar los offsets
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=TIMEOUT_INACTIVIDAD * 1000,
            # Permitir leer registros aunque no tengan valor (control interno).
            api_version=(3, 7, 0),
        )
    except KafkaError as e:
        print(f"[ERROR] Error al crear el consumidor: {e}")
        sys.exit(1)

    return consumer

#----------------------------------
#Recepcion y guardado de eventos
#----------------------------------

def consumir_y_guardar(consumer, salida, n_esperados):
    #lee consajes del consumidor hasta n_esperados o al agotar tiempo de inactividad
    # el archivo salida devuelve (guardados,errores)
    guardados= 0
    errores=0

    with open(salida, "w", encoding="utf-8") as f:

        print(f"Consumiendo eventos desde Kafka y guardandolos en '{salida}'...")
        print(f"Se esperan aproximadamente {n_esperados} eventos.")
        print("Si el productor ya termino, el consumidor cerrara solo tras "
              f"{TIMEOUT_INACTIVIDAD} s sin mensajes nuevos.")
        print("-" * 60)

        for record in consumer:

            evento = record.value  # ya viene como dict por el value_deserializer

            try:
                #validacion diccionario
                if not isinstance(evento, dict):
                    raise ValueError(f"El evento no es un objeto JSON: {evento!r}")

                # Un objeto JSON por linea (formato JSON Lines)
                f.write(json.dumps(evento, ensure_ascii=False) + "\n")
                guardados += 1

                # Muestra inicial en consola para evidenciar la recepcion
                if guardados <= N_MUESTRA:
                    print(f"  [{guardados}] {evento}")

                # Cierre anticipado si ya recibimos todos los esperados.
                if guardados >= n_esperados:
                    print(f"\n[OK] Se alcanzo el objetivo de {n_esperados} eventos.")
                    break

            except (TypeError, ValueError) as e:
                errores += 1
                print(f"[WARN] No se pudo interpretar un mensaje: {e}")

    return guardados, errores


#----------------------------------
# Programa principal
#----------------------------------
def parsear_argumentos():
    p = argparse.ArgumentParser(
        description="Consumidor Kafka de validaciones de transporte")
    p.add_argument("--n-eventos", type=int, default=5000,
                   help="cantidad de eventos que se esperan recibir")
    p.add_argument("--topico", default="validaciones_transporte",
                   help="nombre del topico de Kafka")
    p.add_argument("--servidor", default="localhost:9092",
                   help="direccio" \
                   "n del broker de Kafka")
    p.add_argument("--grupo", default="grupo_consumidor",
                   help="identificador del grupo de consumidores")
    p.add_argument("--salida", default="eventos.jsonl",
                   help="archivo JSON Lines de salida")
    return p.parse_args()

def main():
    args = parsear_argumentos()
    inicio = datetime.now()

    consumer = crear_consumidor(args.servidor, args.topico, args.grupo)
    guardados, errores = consumir_y_guardar(consumer, args.salida, args.n_eventos)

    # Cierre: libera la conexion con el broker.
    consumer.close()

    duracion = (datetime.now() - inicio).total_seconds()

    print("-" * 60)
    print(f"Eventos guardados : {guardados}")
    print(f"Eventos con error : {errores}")
    print(f"Archivo generado : {args.salida}")
    print(f"Duración          : {duracion:.1f} s")


if __name__ == "__main__":
    main()