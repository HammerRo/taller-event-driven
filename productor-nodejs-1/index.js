const amqp = require('amqplib');

const rabbitmqHost = process.env.RABBITMQ_HOST || 'localhost';
const amqpUrl = `amqp://${rabbitmqHost}:5672`;
let connection, channel;

async function connect() {
    while (true) {
        try {
            connection = await amqp.connect(amqpUrl);
            channel = await connection.createChannel();
            await channel.assertQueue('tareas', { durable: false });
            console.log("Conectado a RabbitMQ");
            return;
        } catch (err) {
            console.error("No se pudo conectar a RabbitMQ, reintentando en 5 segundos...");
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }
}

async function startSending() {
    await connect();
    setInterval(() => {
        const message = `Mensaje desde Node.js-1 - ${new Date().toISOString()}`;
        channel.sendToQueue('tareas', Buffer.from(message));
        console.log(` [x] Enviado '${message}'`);
    }, 5000);
}

startSending();
