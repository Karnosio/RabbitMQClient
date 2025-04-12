# rabbitmq-client
[![Python versions](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-green)](https://www.python.org/downloads/)

A simple and lightweight wrapper around `aio-pika` to streamline RabbitMQ integration in your Python projects, whether big or small.

Just inherit the `Client` class, define your initialization logic, and you're ready to produce and consume RabbitMQ messages with ease!

---

## Features

- **Simple Abstraction**: Simple `Client` class to quickly set up RabbitMQ producers and consumers.
- **Asyncio-Friendly**: Built for seamless integration with Python's `asyncio` ecosystem.
- **Lightweight**: Minimal dependencies and straightforward API.

---

## Installation

Install the package via pip:

```bash
pip install https://github.com/BDI-OFFICIAL/RabbitMQClient/archive/refs/heads/main.zip
```

---

## Quick Start
Here’s an example of how to use the `rabbitmq_client` library by extending the `Client` class.

```python
import asyncio
import logging

from rabbitmq_client import Client
from aio_pika import Message
from aio_pika.abc import AbstractIncomingMessage


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    client = Client(
        url='amqp://user:password@host',
        exchange_name='exchange_test',
        prefetch_count=1
    )
    await client.connect()
    await asyncio.gather(
        client.listen_messages(queue_name='test', callback_func=on_message),
        send_messages(client)
    )


async def send_messages(client: Client):
    for i in range(10):
        await client.send(
            message=Message(
                body=str(i).encode(),
                delivery_mode=2
            ),
            queue_name='test'
        )
        await asyncio.sleep(1)


async def on_message(message: AbstractIncomingMessage) -> None:
    async with message.process():
        print(f'- [x] Received {message.body!r}')


if __name__ == '__main__':
    asyncio.run(main())
```

## License
This project is licensed under the MIT License.