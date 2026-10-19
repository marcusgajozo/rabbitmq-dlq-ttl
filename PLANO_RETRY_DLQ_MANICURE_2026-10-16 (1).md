# Plano passo a passo: retry com espera crescente, fila de estacionamento e idempotência

Versão 2, reescrita em 16/10/2026 depois do seu retorno. Esta versão é autossuficiente: todos os links dos artigos estão aqui dentro, cada etapa traz os comandos exatos e diz como você sabe que terminou. Não precisa abrir outro arquivo.

O material de consulta mais profundo, com a explicação de cada artigo, continua em `ESTUDO_RETRY_DLQ_SAGA_2026-10-16.md`, mas nada aqui depende dele.

Projeto: `api-sistema-manicure`. Objetivo: tirar a notificação de dentro da requisição, levá-la para um broker, e tratar falha com retry limitado, fila de estacionamento e consumidor idempotente.

---

## Antes de começar: o ambiente

Você precisa de Docker rodando na máquina. Para conferir, no terminal:

```bash
docker --version
docker compose version
```

Se os dois comandos responderem versão, está pronto. Se não responderem, instale o Docker Desktop antes de seguir, porque todas as etapas dependem dele.

Node já está na máquina, porque o projeto roda com ele. Confirme com `node -v`.

---

## Etapa 0: o spike descartável (2 a 3 horas)

**O que é**: uma pasta separada, fora do projeto, com RabbitMQ em Docker e três scripts de mentira, para você ver o comportamento acontecer antes de escrever código de verdade. No fim, a pasta pode ser apagada.

**Onde**: crie em qualquer lugar do seu computador, por exemplo `~/spikes/000-retry-dlq`.

### Comandos

```bash
mkdir -p ~/spikes/000-retry-dlq
cd ~/spikes/000-retry-dlq
npm init -y
npm install amqplib
```

### Arquivo 1: `docker-compose.yml`

Salve exatamente isto na pasta:

```yaml
services:
  rabbitmq:
    image: rabbitmq:4-management
    container_name: rabbitmq-spike
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: manicure
      RABBITMQ_DEFAULT_PASS: manicure
    volumes:
      - rabbitmq-data:/var/lib/rabbitmq

volumes:
  rabbitmq-data:
```

### Subir o RabbitMQ

```bash
docker compose up -d
```

Depois abra **http://localhost:15672** no navegador, entre com usuário `manicure` e senha `manicure`. Você vai ver o painel de administração. É nele que você vai acompanhar tudo, e é ele que você substitui pelo `rabbitmqctl` quando a coisa for automática.

### Arquivo 2: `topologia.js`

Este arquivo cria o desenho: exchange principal, exchange de retry, três degraus de espera e a fila de estacionamento.

```javascript
const amqp = require('amqplib')

const EX_MAIN = 'ex.appointments'
const EX_RETRY = 'ex.retry'
const Q_MAIN = 'notifications.appointment'
const Q_PARKING = 'notifications.parking'
const DEGRAUS = [
  { chave: 'retry.1', fila: 'notifications.retry.30s', ttl: 30000 },
  { chave: 'retry.2', fila: 'notifications.retry.2m', ttl: 120000 },
  { chave: 'retry.3', fila: 'notifications.retry.10m', ttl: 600000 },
]

async function main() {
  const conn = await amqp.connect('amqp://manicure:manicure@localhost:5672')
  const ch = await conn.createChannel()

  await ch.assertExchange(EX_MAIN, 'topic', { durable: true })
  await ch.assertExchange(EX_RETRY, 'direct', { durable: true })

  await ch.assertQueue(Q_MAIN, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': EX_RETRY,
      'x-dead-letter-routing-key': 'retry.1',
    },
  })
  await ch.bindQueue(Q_MAIN, EX_MAIN, 'appointment.*')

  for (const d of DEGRAUS) {
    await ch.assertQueue(d.fila, {
      durable: true,
      arguments: {
        'x-message-ttl': d.ttl,
        'x-dead-letter-exchange': EX_MAIN,
        'x-dead-letter-routing-key': 'appointment.created',
      },
    })
    await ch.bindQueue(d.fila, EX_RETRY, d.chave)
  }

  await ch.assertQueue(Q_PARKING, { durable: true })
  await ch.bindQueue(Q_PARKING, EX_RETRY, 'parking')

  console.log('topologia criada. Confira em http://localhost:15672')
  await ch.close()
  await conn.close()
}

main().catch((e) => { console.error(e); process.exit(1) })
```

Rode com `node topologia.js` e confira no painel, na aba Queues: devem aparecer as cinco filas.

### Arquivo 3: `consumidor.js`

Um consumidor de mentira, que falha de propósito. Este é o coração da etapa 0.

```javascript
const amqp = require('amqplib')

const EX_RETRY = 'ex.retry'
const Q_MAIN = 'notifications.appointment'
const PROXIMO = { 1: 'retry.2', 2: 'retry.3' }
const MAX_TENTATIVAS = 3

async function main() {
  const conn = await amqp.connect('amqp://manicure:manicure@localhost:5672')
  const ch = await conn.createChannel()
  await ch.prefetch(1)

  ch.consume(Q_MAIN, (msg) => {
    if (!msg) return
    const tentativa = Number((msg.properties.headers || {})['x-attempts'] || 1)
    console.log('recebida | tentativa', tentativa, '|', msg.content.toString())

    const passar = (msg.properties.headers || {})['x-passar'] === 'sim'
    if (passar) {
      console.log('  processada com sucesso')
      ch.ack(msg)
      return
    }

    if (tentativa >= MAX_TENTATIVAS) {
      ch.publish(EX_RETRY, 'parking', msg.content, msg.properties)
      ch.ack(msg)
      console.log('  tentativas esgotadas, foi para o estacionamento')
      return
    }

    const proxima = tentativa + 1
    ch.publish(EX_RETRY, PROXIMO[tentativa], msg.content, {
      ...msg.properties,
      headers: { ...(msg.properties.headers || {}), 'x-attempts': proxima },
    })
    ch.ack(msg)
    console.log('  falhou de proposito, mandei para o degrau', PROXIMO[tentativa])
  })
}

main().catch((e) => { console.error(e); process.exit(1) })
```

### Arquivo 4: `produtor.js`

```javascript
const amqp = require('amqplib')

async function main() {
  const conn = await amqp.connect('amqp://manicure:manicure@localhost:5672')
  const ch = await conn.createChannel()
  const conteudo = Buffer.from('agendamento ' + Date.now() + ' criado')
  ch.publish('ex.appointments', 'appointment.created', conteudo, {
    persistent: true,
    headers: { 'x-attempts': 1 },
  })
  console.log('publicado')
  setTimeout(() => { conn.close(); process.exit(0) }, 500)
}

main().catch((e) => { console.error(e); process.exit(1) })
```

### O experimento, na ordem

Abra três terminais. No primeiro, deixe `node consumidor.js` rodando. No segundo, rode `node produtor.js` e observe o primeiro terminal e o painel.

**Pergunta 0a: a mensagem volta?** Você deve ver no terminal do consumidor a mensagem reaparecendo três vezes, com a tentativa incrementando, e no painel a fila `notifications.retry.30s` enchendo por 30 segundos antes de devolver. Na quarta vez ela aparece em `notifications.parking`. Se isso aconteceu, o mecanismo está entendido.

**Pergunta 0b: o erro do TTL único.** Este é o experimento mais importante da etapa. Crie um arquivo `naive.js`:

```javascript
const amqp = require('amqplib')

async function main() {
  const conn = await amqp.connect('amqp://manicure:manicure@localhost:5672')
  const ch = await conn.createChannel()

  await ch.assertExchange('ex.teste', 'direct', { durable: false })
  await ch.assertQueue('teste.consumida', { durable: false })
  await ch.bindQueue('teste.consumida', 'ex.teste', 'volta')

  await ch.assertQueue('espera.unica', {
    durable: false,
    arguments: {
      'x-dead-letter-exchange': 'ex.teste',
      'x-dead-letter-routing-key': 'volta',
    },
  })

  ch.publish('', 'espera.unica', Buffer.from('A: deveria esperar 10 minutos'), { expiration: '600000' })
  ch.publish('', 'espera.unica', Buffer.from('B: deveria esperar 1 minuto'), { expiration: '60000' })

  console.log('A enviada com TTL de 10 min, B com TTL de 1 min. Olhe a fila espera.unica no painel.')
  setTimeout(() => { conn.close(); process.exit(0) }, 2000)
}

main().catch((e) => { console.error(e); process.exit(1) })
```

Rode `node naive.js` e abra a fila `espera.unica` no painel. A mensagem B, que deveria sair em um minuto, vai esperar dez, porque o RabbitMQ só remove mensagem do início da fila. Você acabou de ver, na sua frente, o motivo pelo qual a fila única de espera não serve.

**Pergunta 0c: os degraus resolvem?** Volte ao `consumidor.js` e observe que cada tentativa espera o seu próprio degrau: 30 segundos, 2 minutos, 10 minutos, independente da ordem de chegada. Compare com o que você viu no `naive.js`. É a resposta.

### Pronto quando

Você tem um `README.md` na pasta com a resposta das três perguntas e o que te surpreendeu, e consegue explicar em duas frases por que a fila única de espera falha.

### Os artigos desta etapa

- **RabbitMq Delayed Retry Approaches (That Work)**, de Jack Vanlightly, que explica o comportamento de remoção pelo início da fila: `https://jack-vanlightly.com/blog/2017/3/24/rabbitmq-delayed-retry-approaches-that-work`
- **Exponential Backoff in RabbitMQ**, de Brian Storti, em português, chegando à mesma conclusão com o raciocínio passo a passo: `https://www.brianstorti.com/rabbitmq-exponential-backoff`

---

## Etapa 1: base teórica (5 a 6 horas)

Leitura com anotação. As anotações viram o rascunho do `docs/guia-mensageria.md` da etapa 9.

| Artigo | Link | O que responde | Tempo |
| --- | --- | --- | --- |
| Delay Retry Approaches That Work | `https://jack-vanlightly.com/blog/2017/3/24/rabbitmq-delayed-retry-approaches-that-work` | Por que a fila única de espera falha e quais desenhos funcionam | 45 min |
| Quorum Queues (documentação oficial) | `https://www.rabbitmq.com/docs/quorum-queues` | Retry com atraso nativo, `delivery-limit` e `x-delivery-count` | 50 min |
| Dead Lettering (documentação oficial) | `https://www.rabbitmq.com/docs/dlx` | As condições exatas em que uma mensagem vira carta morta | 40 min |
| Reliability Guide (documentação oficial) | `https://www.rabbitmq.com/docs/reliability` | Confirmação de publicação e ack de consumo sem perder mensagem | 40 min |
| Exponential Backoff in RabbitMQ | `https://www.brianstorti.com/rabbitmq-exponential-backoff` | O mesmo assunto do primeiro, em português | 30 min |
| How to Configure Dead Letter Exchanges | `https://oneuptime.com/blog/post/2026-01-25-rabbitmq-dead-letter-exchanges/view` | A topologia completa, com degraus de 1s, 5s e 30s, e o código | 40 min |
| Stop Poison Messages with Bounded Retries | `https://oneuptime.com/blog/post/2026-08-14-poison-message-dead-letter-backoff/view` | Limite de tentativas, idade máxima e a armadilha do `nack` | 40 min |
| Reliable Notification Systems com RabbitMQ e Node.js | `https://dev.to/abhivyaktii/reliable-notification-systems-implementing-dead-letter-queues-with-rabbitmq-and-nodejs-4mnn` | A diferença entre `nack(msg, false, false)` e `nack(msg, false, true)` | 30 min |
| Transactional Outbox with RabbitMQ, parte 2 | `https://dev.to/sagarmaheshwary/transactional-outbox-with-rabbitmq-part-2-handling-retries-dead-letter-queues-and-observability-4h19` | Degraus, jitter, esgotamento e observabilidade | 45 min |
| Microserviços com RabbitMQ no NestJS | `https://docs.nestjs.com/microservices/rabbitmq` | A camada de transporte do seu stack | 30 min |
| Discussão sobre DLQ no @golevelup/nestjs-rabbitmq | `https://github.com/golevelup/nestjs/issues/138` | A receita em NestJS, com contador no cabeçalho | 30 min |
| Pattern: Idempotent Consumer | `https://microservices.io/patterns/communication-style/idempotent-consumer.html` | O padrão que protege o reenvio automático | 25 min |
| Handling duplicate messages | `https://microservices.io/post/microservices/patterns/2020/10/16/idempotent-consumer.html` | O mesmo padrão com código | 25 min |

O material sobre Saga e sobre idempotência está detalhado em `ESTUDO_RETRY_DLQ_SAGA_2026-10-16.md`, e a conclusão que importa: **o Saga não resolve este problema**, ele trata de transação distribuída com compensação, e depende de você já ter retry, estacionamento e idempotência funcionando.

---

## Etapa 2: RabbitMQ no projeto (3 a 4 horas)

Mesma imagem do spike, agora dentro do `docker-compose.local.yaml` do repositório, na mesma rede da API e do banco.

### Comandos

```bash
cd /workspace/repos/api-sistema-manicure
git checkout prd && git pull
git checkout -b feat/mensageria-rabbitmq
```

Acrescente ao `docker-compose.local.yaml` o serviço, abaixo do Postgres:

```yaml
  rabbitmq-sistema-manicure:
    image: rabbitmq:4-management
    container_name: rabbitmq-sistema-manicure
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: ${RABBITMQ_USER:-manicure}
      RABBITMQ_DEFAULT_PASS: ${RABBITMQ_PASS:-manicure}
    volumes:
      - rabbitmq-data:/var/lib/rabbitmq
    networks:
      - api-sistema-manicure-network
```

E `rabbitmq-data:` na seção de volumes. Depois `docker compose -f docker-compose.local.yaml up -d`.

### Variáveis de ambiente

No `.env.example` e no `.env`:

```text
RABBITMQ_URL=amqp://manicure:manicure@localhost:5672
RABBITMQ_QUEUE_PREFIX=notifications
```

Atenção ao host: rodando a API fora do Docker, é `localhost`. Rodando a API em container, é o nome do serviço, `rabbitmq-sistema-manicure`.

### Onde o código mora

Pelo `AGENTS.md` do projeto, código novo nasce no padrão Clean Architecture, com adaptador de framework apenas em `infrastructure/`. Então:

```text
src/infrastructure/messaging/
  rabbitmq.module.ts
  rabbitmq.service.ts          conexão, canais e declaração da topologia
  rabbitmq-publisher.ts        implementação da porta de publicação
src/application/ports/
  event-publisher.port.ts      interface que o domínio enxerga
```

### Dependências

```bash
pnpm add amqp-connection-manager amqplib
pnpm add -D @types/amqplib
```

`amqp-connection-manager` cuida da reconexão, que o `amqplib` sozinho não faz. Sem ele, uma queda momentânea do broker derruba o consumidor em silêncio.

### Pronto quando

`docker compose ps` mostra os três serviços de pé, o painel em `http://localhost:15672` abre, e o módulo de conexão sobe junto com a API sem erro no log.

---

## Etapa 3: publicar os eventos no broker (3 a 4 horas)

Hoje o `appointments.service.ts` publica em memória, nas linhas 110, 209 e 224, com `emitAsync`. O objetivo é publicar também no broker, com confirmação de publicação ligada.

### A regra que importa

Nunca publique dentro da transação do banco. Se a mensagem sair e a transação voltar atrás, você notifica um agendamento que não existe. Publique depois do commit. O padrão Transactional Outbox resolve isso de forma completa e é assunto de uma evolução futura, não desta etapa.

### Como conferir

```bash
curl -u manicure:manicure http://localhost:15672/api/queues
```

Cada mensagem publicada deve aparecer na fila principal, e o log da API deve mostrar a confirmação do broker. Com `publisher confirms` ligado, uma publicação sem confirmação vira erro no log, e não um silêncio.

### Pronto quando

Criar um agendamento pela API faz a mensagem aparecer na fila `notifications.appointment` no painel, e você sabe dizer, olhando o log, se o broker confirmou.

---

## Etapa 4: consumir com retry e estacionamento (5 a 7 horas)

O núcleo do trabalho, e onde a etapa 0 se paga.

### As três decisões desta etapa

1. **Classificação de erro.** Erro de validação ou de dado nunca deve ser retentado, porque vai falhar para sempre. Vai direto para o estacionamento. Só falha temporária, como banco fora do ar ou serviço externo fora, entra no retry. Essa classificação vale mais que a topologia inteira.
2. **Contador no cabeçalho.** O consumidor lê `x-attempts`, incrementa e publica no degrau seguinte. Foi o que você fez no spike.
3. **Ack manual.** Sem ack manual, a mensagem é confirmada antes de você processar, e uma falha no meio do caminho perde a mensagem em silêncio.

### Pronto quando

Com o consumidor derrubado de propósito, a mensagem passa pelos três degraus e para no estacionamento, e você vê isso no painel. Com um erro de validação forçado, ela vai direto para o estacionamento sem ocupar degrau.

---

## Etapa 5: idempotência (4 a 5 horas)

Sem isso, o retry automático é risco de efeito duplicado: a mesma notificação enviada duas vezes.

### O que fazer

Uma tabela nova por migration, seguindo o `docs/guia-banco-de-dados.md`:

```bash
pnpm migration:generate src/database/migrations/CreateProcessedMessages
```

A tabela guarda o identificador da mensagem e o resultado, com índice único no identificador. O consumidor tenta inserir antes de executar o efeito: se o insert falhar por duplicidade, a mensagem já foi processada, e ele apenas confirma sem repetir.

### O teste que prova

Enviar a mesma mensagem duas vezes e conferir que a notificação foi registrada uma vez só. É o teste mais valioso do projeto inteiro, porque é o que separa retry profissional de retry ingênuo.

---

## Etapa 6: reenvio em lote (4 a 5 horas)

O fim do trabalho manual de abrir o painel.

### O que fazer

Um endpoint administrativo, por exemplo `POST /admin/messages/reprocess`, que recebe a quantidade máxima e move mensagens do estacionamento de volta para o exchange principal. Ele grava em uma tabela de auditoria quem pediu, quantas mensagens e quando. A rota exige usuário autenticado.

### Pronto quando

Você consegue esvaziar o estacionamento por comando, e existe registro de quem fez isso e quando. É a versão automatizada do que você faz hoje clicando no painel do RabbitMQ.

---

## Etapa 7: testes de integração no CI (3 a 4 horas)

A suíte E2E do projeto já sobe banco de verdade. O broker entra do mesmo jeito, como serviço do GitHub Actions. No `.github/workflows/ci-cd.yaml`, no job de teste:

```yaml
    services:
      postgres:
        image: postgres:18-alpine
        env:
          POSTGRES_PASSWORD: postgres
        options: >-
          --health-cmd pg_isready --health-interval 10s --health-timeout 5s --health-retries 5
        ports: ["5432:5432"]
      rabbitmq:
        image: rabbitmq:4
        options: >-
          --health-cmd "rabbitmq-diagnostics -q ping" --health-interval 10s --health-timeout 5s --health-retries 5
        ports: ["5672:5672"]
```

### Os três cenários que provam o comportamento

1. Mensagem que falha duas vezes e é processada na terceira.
2. Mensagem que falha até o limite e termina no estacionamento.
3. Mensagem entregue duas vezes que não gera efeito duplicado.

### Pronto quando

Os três testes rodam no pipeline e ficam verdes, e você consegue mostrar o log da execução verde.

---

## Etapa 8: observabilidade (3 a 4 horas)

1. Expor a profundidade das filas, por um `GET /admin/queues` que consulta a API de gerenciamento do RabbitMQ.
2. Registrar em log toda entrada em retry e em estacionamento, com o identificador da mensagem.
3. Alerta quando o estacionamento deixar de estar vazio, porque fila morta com mensagem significa inconsistência acumulando.

### Pronto quando

Um erro conta a história inteira: a requisição que criou o agendamento, a mensagem que falhou, quantas vezes tentou e onde ela parou. Isso também prepara o incremento 5 do seu `FOCO_2026-10.md`, o de tracing.

---

## Etapa 9: documentação (4 a 6 horas)

1. **Um ADR** em `docs/adr/`, no padrão dos três que já existem, com a decisão, o contexto, as alternativas descartadas e as consequências. O modelo está em `docs/adr/0001`.
2. **`docs/guia-mensageria.md`**, com a topologia desenhada, os nomes das filas, o significado de cada cabeçalho, o procedimento de reenvio e o que fazer quando a fila morta cresce.
3. **README e AGENTS.md** atualizados, com o serviço do RabbitMQ no compose e o comando para subir o ambiente.

### Pronto quando

Alguém que nunca viu o projeto consegue subir o ambiente, entender a topologia e reprocessar uma mensagem usando só a documentação.

---

## Resumo das horas

| Etapa | Horas |
| --- | --- |
| 0, spike descartável | 2 a 3 |
| 1, base teórica | 5 a 6 |
| 2, RabbitMQ no projeto | 3 a 4 |
| 3, publicação | 3 a 4 |
| 4, consumo com retry | 5 a 7 |
| 5, idempotência | 4 a 5 |
| 6, reenvio em lote | 4 a 5 |
| 7, testes no CI | 3 a 4 |
| 8, observabilidade | 3 a 4 |
| 9, documentação | 4 a 6 |
| **Total** | **36 a 48** |

A 6 ou 8 horas por semana, de 5 a 7 semanas. A 10 horas por semana, contando fim de semana, de 3 a 4 semanas.

---

## Apêndice: erros que você vai encontrar

| Erro | Causa | Solução |
| --- | --- | --- |
| `ACCESS_REFUSED` | Usuário ou senha diferente do que subiu no container | Confira `RABBITMQ_DEFAULT_USER` e a `RABBITMQ_URL` |
| `PRECONDITION_FAILED - inequivalent arg` | Você mudou um argumento de uma fila que já existia, como o TTL | Apague a fila no painel, ou troque o nome dela, porque fila existente não aceita argumento diferente |
| Mensagem não expira nunca | TTL declarado por mensagem em fila com mensagens na frente | Use TTL no nível da fila, como na topologia da etapa 0 |
| Consumidor morre e não volta | Ausência de reconexão | Use `amqp-connection-manager` |
| Mensagem processada duas vezes | Sem idempotência | Etapa 5 |
| Fila morta crescendo sempre | Erro de validação entrando no retry | Etapa 4, classificação de erro |

## Apêndice: todos os links, em um lugar

```text
https://jack-vanlightly.com/blog/2017/3/24/rabbitmq-delayed-retry-approaches-that-work
https://www.brianstorti.com/rabbitmq-exponential-backoff
https://www.rabbitmq.com/docs/quorum-queues
https://www.rabbitmq.com/docs/dlx
https://www.rabbitmq.com/docs/reliability
https://oneuptime.com/blog/post/2026-01-25-rabbitmq-dead-letter-exchanges/view
https://oneuptime.com/blog/post/2026-08-14-poison-message-dead-letter-backoff/view
https://dev.to/abhivyaktii/reliable-notification-systems-implementing-dead-letter-queues-with-rabbitmq-and-nodejs-4mnn
https://dev.to/sagarmaheshwary/transactional-outbox-with-rabbitmq-part-2-handling-retries-dead-letter-queues-and-observability-4h19
https://docs.nestjs.com/microservices/rabbitmq
https://github.com/golevelup/nestjs/issues/138
https://microservices.io/patterns/communication-style/idempotent-consumer.html
https://microservices.io/post/microservices/patterns/2020/10/16/idempotent-consumer.html
https://microservices.io/patterns/data/saga.html
https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-rabbitmq-oci/4-3/tanzu-rabbitmq-oci-image/site-delayed-queues.html
```

## Apêndice: os nomes das filas

| Nome | Papel |
| --- | --- |
| `ex.appointments` | Exchange principal, recebe os eventos de agendamento |
| `ex.retry` | Exchange de retry, roteia para os degraus e para o estacionamento |
| `notifications.appointment` | Fila principal, consumida pelo módulo de notificações |
| `notifications.retry.30s` | Primeiro degrau, espera 30 segundos |
| `notifications.retry.2m` | Segundo degrau, espera 2 minutos |
| `notifications.retry.10m` | Terceiro degrau, espera 10 minutos |
| `notifications.parking` | Estacionamento, só recebe falha permanente |
