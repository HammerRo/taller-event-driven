const amqp = require('amqplib');

const rabbitmqHost = process.env.RABBITMQ_HOST || 'localhost';
const amqpUrl = `amqp://${rabbitmqHost}:5672`;

async function connectAndConsume() {
    while (true) {
        try {
            const connection = await amqp.connect(amqpUrl);
            const channel = await connection.createChannel();
            await channel.assertQueue('tareas', { durable: false });
            console.log(" [*] Esperando mensajes en 'tareas'. Para salir presiona CTRL+C");

            channel.consume('tareas', (msg) => {
                if (msg !== null) {
                    console.log(` [x] Recibido: ${msg.content.toString()}`);
                    channel.ack(msg);
                }
            });
            return;
        } catch (err) {
            console.error("No se pudo conectar a RabbitMQ, reintentando en 5 segundos...");
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }
}

connectAndConsume();
