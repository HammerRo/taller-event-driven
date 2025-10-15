import pika
import time
import os

def callback(ch, method, properties, body):
    print(f" [x] Recibido: {body.decode()}")
    time.sleep(body.count(b'.'))
    print(" [x] Tarea completada")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def connect():
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    while True:
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=5672))
        except pika.exceptions.AMQPConnectionError:
            print("No se pudo conectar a RabbitMQ, reintentando en 5 segundos...")
            time.sleep(5)

connection = connect()
channel = connection.channel()
channel.queue_declare(queue='tareas')
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='tareas', on_message_callback=callback)

print(' [*] Esperando mensajes. Para salir presiona CTRL+C')
channel.start_consuming()
