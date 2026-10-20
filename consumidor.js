const amqp = require("amqplib");

const EX_RETRY = "ex.retry";
const Q_MAIN = "notifications.appointment";
const PROXIMO = { 1: "retry.2", 2: "retry.3" };
const MAX_TENTATIVAS = 3;

async function main() {
    const conn = await amqp.connect("amqp://manicure:manicure@localhost:5672");
    const ch = await conn.createChannel();
    await ch.prefetch(1)

    ch.consume(Q_MAIN, (msg) => {
        if (!msg) return;
        const tentativa = Number(
            (msg.properties.headers || {})["x-attempts"] || 1,
        );
        console.log(
            "recebida | tentativa",
            tentativa,
            "|",
            msg.content.toString(),
        );

        const passar = (msg.properties.headers || {})["x-passar"] === "sim";
        if (passar) {
            console.log(" processada com sucesso");
            ch.ack(msg);
            return;
        }

        if (tentativa >= MAX_TENTATIVAS) {
            ch.publish(EX_RETRY, "parking", msg.content, msg.properties);
            ch.ack(msg);
            console.log(" tentativas esgotadas, foi para o estacionamento");
            return;
        }

        const proxima = tentativa + 1;
        ch.publish(EX_RETRY, PROXIMO[tentativa], msg.content, {
            ...msg.properties,
            headers: {
                ...(msg.properties.headers || {}),
                "x-attempts": proxima,
            },
        });
        ch.ack(msg);
        console.log(
            " falhou de proposito, mandei para o degrau",
            PROXIMO[tentativa],
        );
    });
}

main().catch((e) => {
    (console.log(e), process.exit(1));
});
