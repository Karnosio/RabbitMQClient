import asyncio
import logging
from abc import ABC

import aio_pika
from aio_pika import Message
from aiormq import AMQPConnectionError

logger = logging.getLogger(__name__)


class Client(ABC):
    def __init__(self, url: str, exchange_name: str, prefetch_count: int):
        self.url = url
        self.exchange_name = exchange_name
        self.prefetch_count = prefetch_count
        self.connection = None
        self.channel = None
        self.exchange = None

    async def connect(self, max_retries: int = 15, retry_delay: int = 3) -> None:
        for attempt in range(max_retries):
            try:
                self.connection = await aio_pika.connect_robust(url=self.url)
                self.channel = await self.connection.channel()
                self.exchange = await self.channel.declare_exchange(name=self.exchange_name, durable=True)

                await self.channel.set_qos(prefetch_count=self.prefetch_count)

                logger.info(f'RabbitMQClient is ready to accept RabbitMQ connections')

                break

            except AMQPConnectionError as e:
                logger.warning(f'RabbitMQ not ready (attempt {attempt + 1}/{max_retries}): {e}')

                if attempt + 1 == max_retries:
                    raise AMQPConnectionError(f'RabbitMQ not available after {max_retries} retries')

                await asyncio.sleep(retry_delay)

    async def send(self, message: Message, queue_name: str):
        if not self.connection:
            raise ConnectionError('You need to have at least one connection active!')

        queue = await self.channel.declare_queue(name=queue_name, durable=True)

        await self.exchange.publish(
            message=message,
            routing_key=queue.name
        )

        logger.info(f'- [x] Sent: {message.body}')

    async def listen_messages(self, queue_name: str, callback_func: callable):
        if not self.connection:
            raise ConnectionError('You need to have at least one connection active!')

        queue = await self.channel.declare_queue(name=queue_name, durable=True)
        await queue.bind(self.exchange, routing_key=queue_name)

        await queue.consume(callback_func)

        logger.info('- [*] Waiting for messages. To exit press CTRL+C')
        await asyncio.Future()
