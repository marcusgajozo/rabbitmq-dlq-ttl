const amqp = require("amqplib");

const EX_MAIN = "ex.appointments";
const EX_RETRY = "ex.retry";
const Q_MAIN = "notifications.appointment";
const Q_PARKING = "notifications.parking";
const DEGRAUS = [
    { chave: "retry.1", fila: "notifications.retry.30s", ttl: 30000 },
    { chave: "retry.2", fila: "notifications.retry.2m", ttl: 120000 },
    { chave: "retry.3", fila: "notifications.retry.10m", ttl: 600000 },
];

async function main() {
    const conn = await amqp.connect("amqp://manicure:manicure@localhost:5672");
    const ch = await conn.createChannel();

    await ch.assertExchange(EX_MAIN, "topic", { durable: true });
    await ch.assertExchange(EX_RETRY, "direct", { durable: true });

    await ch.assertQueue(Q_MAIN, {
        durable: true,
        arguments: {
            "x-dead-letter-exchange": EX_RETRY,
            "x-dead-letter-routing-key": "retry.1",
        },
    });
    await ch.bindQueue(Q_MAIN, EX_MAIN, "appointment.*");

    for (const d of DEGRAUS) {
        await ch.assertQueue(d.fila, {
            durable: true,
            arguments: {
                "x-message-ttl": d.ttl,
                "x-dead-letter-exchange": EX_MAIN,
                "x-dead-letter-routing-key": "appointment.created",
            },
        });
        await ch.bindQueue(d.fila, EX_RETRY, d.chave);
    }

    await ch.assertQueue(Q_PARKING, { durable: true });
    await ch.bindQueue(Q_PARKING, EX_RETRY, "parking");

    console.log("topologia criada. Confira em http://localhost:15672");
    await ch.close();
    await conn.close();
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
