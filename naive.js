const amqp = require("amqplib");

async function main() {
    const conn = await amqp.connect("amqp://manicure:manicure@localhost:5672");
    const ch = await conn.createChannel();

    await ch.assertExchange("ex.teste", "direct", { durable: false });
    await ch.assertQueue("teste.consumida", { durable: true });
    await ch.bindQueue("teste.consumida", "ex.teste", "volta");

    await ch.assertQueue("espera.unica", {
        durable: true,
        arguments: {
            "x-dead-letter-exchange": "ex.teste",
            "x-dead-letter-routing-key": "volta",
        },
    });

    ch.publish(
        "",
        "espera.unica",
        Buffer.from("A: deveria esperar 10 minutos"),
        { expiration: "600000" },
    );
    ch.publish("", "espera.unica", Buffer.from("B: deveria esperar 1 minuto"), {
        expiration: "60000",
    });

    console.log(
        "A enviada com TTL de 10 min, B com TTL de 1 min. Olhe a fila espera.unica no painel.",
    );
    setTimeout(() => {
        conn.close();
        process.exit(0);
    }, 2000);
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
