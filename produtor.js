const amqp = require("amqplib");

async function main() {
    const conn = await amqp.connect("amqp://manicure:manicure@localhost:5672");
    const ch = await conn.createChannel();
    const conteudo = Buffer.from("agentamento" + Date.now() + " criado");
    ch.publish("ex.appointments", "appointment.created", conteudo, {
        persistent: true,
        headers: { "x-attemps": 1 },
    });
    console.log("publicado");
    setTimeout(() => {
        conn.close();
        process.exit(0);
    }, 500);
}

main().catch((e) => {
    console.log(e);
    process.exit(1);
});
