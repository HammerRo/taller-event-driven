package com.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Productor {
    private final static String QUEUE_NAME = "tareas";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        String rabbitmqHost = System.getenv().getOrDefault("RABBITMQ_HOST", "localhost");
        factory.setHost(rabbitmqHost);

        while (true) {
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                String message = "Hola desde el productor de Java!";
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
                System.out.println(" [x] Enviado '" + message + "'");
            } catch (Exception e) {
                System.out.println("No se pudo conectar a RabbitMQ, reintentando en 5s...");
                Thread.sleep(5000);
                continue;
            }
            Thread.sleep(6000);
        }
    }
}
