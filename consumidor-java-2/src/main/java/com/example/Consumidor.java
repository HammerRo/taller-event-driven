package com.example;

import com.rabbitmq.client.*;

public class Consumidor {
    private final static String QUEUE_NAME = "tareas";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        String rabbitmqHost = System.getenv().getOrDefault("RABBITMQ_HOST", "localhost");
        factory.setHost(rabbitmqHost);
        
        while (true) {
            try {
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                
                // Configuración de la cola
                channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                channel.basicQos(1); // Procesar un mensaje a la vez
                
                System.out.println(" [*] Esperando mensajes. Para salir presiona CTRL+C");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    try {
                        String message = new String(delivery.getBody(), "UTF-8");
                        System.out.println(" [x] Recibido '" + message + "'");
                        
                        // Procesar el mensaje
                        doWork(message);
                        
                        // Confirmar procesamiento exitoso
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        
                    } catch (Exception e) {
                        System.out.println("Error procesando mensaje: " + e.getMessage());
                        // Rechazar el mensaje en caso de error
                        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    }
                };

                channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
                
                // Esperar indefinidamente
                Thread.sleep(Long.MAX_VALUE);
                
            } catch (Exception e) {
                System.out.println("Error de conexión: " + e.getMessage());
                System.out.println("Reintentando en 5 segundos...");
                Thread.sleep(5000);
            }
        }
    }

    private static void doWork(String message) throws InterruptedException {
        // Simulamos algún trabajo
        Thread.sleep(1000);
        System.out.println(" [x] Mensaje procesado: " + message);
    }
}
