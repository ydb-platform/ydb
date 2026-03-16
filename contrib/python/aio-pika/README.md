# aio-pika

[![Coveralls](https://coveralls.io/repos/github/mosquito/aio-pika/badge.svg?branch=master)](https://coveralls.io/github/mosquito/aio-pika)
[![Github Actions](https://github.com/mosquito/aio-pika/workflows/tests/badge.svg)](https://github.com/mosquito/aio-pika/actions?query=workflow%3Atests)
[![Latest Version](https://img.shields.io/pypi/v/aio-pika.svg)](https://pypi.python.org/pypi/aio-pika/)
[![](https://img.shields.io/pypi/wheel/aio-pika.svg)](https://pypi.python.org/pypi/aio-pika/)
[![](https://img.shields.io/pypi/pyversions/aio-pika.svg)](https://pypi.python.org/pypi/aio-pika/)
[![](https://img.shields.io/pypi/l/aio-pika.svg)](https://pypi.python.org/pypi/aio-pika/)


A wrapper around [aiormq](http://github.com/mosquito/aiormq/) for asyncio and humans.

Check out the examples and the tutorial in the [documentation](https://docs.aio-pika.com/).

If you are a newcomer to RabbitMQ, please start with the [adopted official RabbitMQ tutorial](https://docs.aio-pika.com/rabbitmq-tutorial/index.html).

> **Note:** Since version `5.0.0` this library doesn't use `pika` as AMQP connector.
> Versions below `5.0.0` contains or requires `pika`'s source code.

> **Note:** The version 7.0.0 has breaking API changes, see CHANGELOG.md
> for migration hints.


## Features

* Completely asynchronous API.
* Object oriented API.
* Transparent auto-reconnects with complete state recovery with `connect_robust`
  (e.g. declared queues or exchanges, consuming state and bindings).
* Python 3.10+ compatible.
* Transparent [publisher confirms](https://www.rabbitmq.com/confirms.html) support.
* [Transactions](https://www.rabbitmq.com/semantics.html) support.
* Complete type-hints coverage.


## Installation

```shell
pip install aio-pika
```


## Usage example

Simple consumer:

```python
import asyncio
import aio_pika
import aio_pika.abc


async def main(loop):
    # Connecting with the given parameters is also possible.
    # aio_pika.connect_robust(host="host", login="login", password="password")
    # You can only choose one option to create a connection, url or kw-based params.
    connection: aio_pika.abc.AbstractRobustConnection = await aio_pika.connect_robust(
        "amqp://guest:guest@127.0.0.1/", loop=loop
    )

    async with connection:
        queue_name = "test_queue"

        # Creating channel
        channel: aio_pika.abc.AbstractChannel = await connection.channel()

        # Declaring queue
        queue: aio_pika.abc.AbstractQueue = await channel.declare_queue(
            queue_name,
            auto_delete=True
        )

        async with queue.iterator() as queue_iter:
            # Cancel consuming after __aexit__
            async for message in queue_iter:
                async with message.process():
                    print(message.body)

                    if queue.name in message.body.decode():
                        break


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
```

Simple publisher:

```python
import asyncio
import aio_pika
import aio_pika.abc


async def main(loop):
    # Explicit type annotation
    connection: aio_pika.abc.AbstractRobustConnection = await aio_pika.connect_robust(
        "amqp://guest:guest@127.0.0.1/", loop=loop
    )

    routing_key = "test_queue"

    channel: aio_pika.abc.AbstractChannel = await connection.channel()

    await channel.default_exchange.publish(
        aio_pika.Message(
            body='Hello {}'.format(routing_key).encode()
        ),
        routing_key=routing_key
    )

    await connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
```


Get single message example:

```python
import asyncio
import aio_pika.abc
from aio_pika import connect_robust, Message


async def main(loop):
    connection: aio_pika.abc.AbstractRobustConnection = await connect_robust(
        "amqp://guest:guest@127.0.0.1/",
        loop=loop
    )

    queue_name = "test_queue"
    routing_key = "test_queue"

    # Creating channel
    channel = await connection.channel()

    # Declaring exchange
    exchange = await channel.declare_exchange('direct', auto_delete=True)

    # Declaring queue
    queue = await channel.declare_queue(queue_name, auto_delete=True)

    # Binding queue
    await queue.bind(exchange, routing_key)

    await exchange.publish(
        Message(
            bytes('Hello', 'utf-8'),
            content_type='text/plain',
            headers={'foo': 'bar'}
        ),
        routing_key
    )

    # Receiving message
    incoming_message = await queue.get(timeout=5)

    # Confirm message
    await incoming_message.ack()

    await queue.unbind(exchange, routing_key)
    await queue.delete()
    await connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
```


There are more examples and the RabbitMQ tutorial in the [documentation](https://docs.aio-pika.com/).

## See also

### [aiormq](http://github.com/mosquito/aiormq/)

`aiormq` is a pure python AMQP client library. It is under the hood of **aio-pika** and might to be used when you really loving works with the protocol low level.
Following examples demonstrates the user API.

Simple consumer:

```python
import asyncio
import aiormq

async def on_message(message):
    """
    on_message doesn't necessarily have to be defined as async.
    Here it is to show that it's possible.
    """
    print(f" [x] Received message {message!r}")
    print(f"Message body is: {message.body!r}")
    print("Before sleep!")
    await asyncio.sleep(5)   # Represents async I/O operations
    print("After sleep!")

async def main():
    # Perform connection
    connection = await aiormq.connect("amqp://guest:guest@localhost/")

    # Creating a channel
    channel = await connection.channel()

    # Declaring queue
    declare_ok = await channel.queue_declare('helo')
    consume_ok = await channel.basic_consume(
        declare_ok.queue, on_message, no_ack=True
    )

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.run_forever()
```

Simple publisher:

```python
import asyncio
from typing import Optional

import aiormq
from aiormq.abc import DeliveredMessage

MESSAGE: Optional[DeliveredMessage] = None

async def main():
    global MESSAGE
    body = b'Hello World!'

    # Perform connection
    connection = await aiormq.connect("amqp://guest:guest@localhost//")

    # Creating a channel
    channel = await connection.channel()
    declare_ok = await channel.queue_declare("hello", auto_delete=True)

    # Sending the message
    await channel.basic_publish(body, routing_key='hello')
    print(f" [x] Sent {body}")

    MESSAGE = await channel.basic_get(declare_ok.queue)
    print(f" [x] Received message from {declare_ok.queue!r}")

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

assert MESSAGE is not None
assert MESSAGE.routing_key == "hello"
assert MESSAGE.body == b'Hello World!'
```

### The [patio](https://github.com/patio-python/patio) and the [patio-rabbitmq](https://github.com/patio-python/patio-rabbitmq)

**PATIO** is an acronym for Python Asynchronous Tasks for AsyncIO - an easily extensible library, for distributed task execution, like celery, only targeting asyncio as the main design approach.

**patio-rabbitmq** provides you with the ability to use *RPC over RabbitMQ* services with extremely simple implementation:

```python
from patio import Registry, ThreadPoolExecutor
from patio_rabbitmq import RabbitMQBroker

rpc = Registry(project="patio-rabbitmq", auto_naming=False)

@rpc("sum")
def sum(*args):
    return sum(args)

async def main():
    async with ThreadPoolExecutor(rpc, max_workers=16) as executor:
        async with RabbitMQBroker(
            executor, amqp_url="amqp://guest:guest@localhost/",
        ) as broker:
            await broker.join()
```

And the caller side might be written like this:

```python
import asyncio
from patio import NullExecutor, Registry
from patio_rabbitmq import RabbitMQBroker

async def main():
    async with NullExecutor(Registry(project="patio-rabbitmq")) as executor:
        async with RabbitMQBroker(
            executor, amqp_url="amqp://guest:guest@localhost/",
        ) as broker:
            print(await asyncio.gather(
                *[
                    broker.call("mul", i, i, timeout=1) for i in range(10)
                 ]
            ))
```


### [FastStream](https://github.com/airtai/faststream)

**FastStream** is a powerful and easy-to-use Python library for building asynchronous services that interact with event streams..

If you need no deep dive into **RabbitMQ** details, you can use more high-level **FastStream** interfaces:

```python
from faststream import FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)

@broker.subscriber("user")
async def user_created(user_id: int):
    assert isinstance(user_id, int)
    return f"user-{user_id}: created"

@app.after_startup
async def pub_smth():
    assert (
        await broker.publish(1, "user", rpc=True)
    ) ==  "user-1: created"
```

Also, **FastStream** validates messages by **pydantic**, generates your project **AsyncAPI** spec, supports In-Memory testing, RPC calls, and more.

In fact, it is a high-level wrapper on top of **aio-pika**, so you can use both of these libraries' advantages at the same time.

### [python-socketio](https://python-socketio.readthedocs.io/en/latest/intro.html)

[Socket.IO](https://socket.io/) is a transport protocol that enables real-time bidirectional event-based communication between clients (typically, though not always, web browsers) and a server. This package provides Python implementations of both, each with standard and asyncio variants.

Also this package is suitable for building messaging services over **RabbitMQ** via **aio-pika** adapter:

```python
import socketio
from aiohttp import web

sio = socketio.AsyncServer(client_manager=socketio.AsyncAioPikaManager())
app = web.Application()
sio.attach(app)

@sio.event
async def chat_message(sid, data):
    print("message ", data)

if __name__ == '__main__':
    web.run_app(app)
```

And a client is able to call `chat_message` the following way:

```python
import asyncio
import socketio

sio = socketio.AsyncClient()

async def main():
    await sio.connect('http://localhost:8080')
    await sio.emit('chat_message', {'response': 'my response'})

if __name__ == '__main__':
    asyncio.run(main())
```

### The [taskiq](https://github.com/taskiq-python/taskiq) and the [taskiq-aio-pika](https://github.com/taskiq-python/taskiq-aio-pika)

**Taskiq** is an asynchronous distributed task queue for python. The project takes inspiration from big projects such as Celery and Dramatiq. But taskiq can send and run both the sync and async functions.

The library provides you with **aio-pika** broker for running tasks too.

```python
from taskiq_aio_pika import AioPikaBroker

broker = AioPikaBroker()

@broker.task
async def test() -> None:
    print("nothing")

async def main():
    await broker.startup()
    await test.kiq()
```

### [Rasa](https://rasa.com/docs/rasa/)

With over 25 million downloads, Rasa Open Source is the most popular open source framework for building chat and voice-based AI assistants.

With **Rasa**, you can build contextual assistants on:

* Facebook Messenger
* Slack
* Google Hangouts
* Webex Teams
* Microsoft Bot Framework
* Rocket.Chat
* Mattermost
* Telegram
* Twilio

Your own custom conversational channels or voice assistants as:

* Alexa Skills
* Google Home Actions

**Rasa** helps you build contextual assistants capable of having layered conversations with lots of back-and-forth. In order for a human to have a meaningful exchange with a contextual assistant, the assistant needs to be able to use context to build on things that were previously discussed – **Rasa** enables you to build assistants that can do this in a scalable way.

And it also uses **aio-pika** to interact with **RabbitMQ** deep inside!

## Versioning

This software follows [Semantic Versioning](http://semver.org/)


## For contributors

### Setting up development environment

Clone the project:

```shell
git clone https://github.com/mosquito/aio-pika.git
cd aio-pika
```

Install uv if you haven't already:

```shell
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Install all requirements for [aio-pika](https://github.com/mosquito/aio-pika/):

```shell
uv sync
```


### Running Tests

**NOTE: In order to run the tests locally you need to run a RabbitMQ instance with default user/password (guest/guest) and port (5672).**

The Makefile provides a command to run an appropriate RabbitMQ Docker image:

```bash
make rabbitmq
```

To test just run:

```bash
make test
```


### Editing Documentation

To iterate quickly on the documentation live in your browser, try:

```bash
nox -s docs -- serve
```

### Creating Pull Requests

Please feel free to create pull requests, but you should describe your use cases and add some examples.

Changes should follow a few simple rules:

* When your changes break the public API, you must increase the major version.
* When your changes are safe for public API (e.g. added an argument with default value)
* You have to add test cases (see `tests/` folder)
* You must add docstrings
* Feel free to add yourself to ["thank's to" section](https://github.com/mosquito/aio-pika/blob/master/docs/source/index.rst#thanks-for-contributing)
