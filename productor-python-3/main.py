import pika
import time
import os
import random

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

while True:
    task_id = random.randint(1000, 9999)
    message = f"Nueva tarea {task_id} desde Python-3"
    channel.basic_publish(exchange='', routing_key='tareas', body=message)
    print(f" [x] Enviado '{message}'")
    time.sleep(random.uniform(3, 7))
