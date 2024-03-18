import logging
import asyncio
from azure.eventhub.aio import EventHubConsumerClient

connection_str = 'Endpoint=sb://eventhack.servicebus.windows.net/;SharedAccessKeyName=hack;SharedAccessKey=HeHeVaVqyVkntO2FnjQcs2Ilh/4MUDo4y+AEhKp8z+g=;EntityPath=simulacoes'
consumer_group = '$Default'
eventhub_name = 'simulacoes'


logger = logging.getLogger("azure.eventhub")
logging.basicConfig(level=logging.INFO)

async def on_event(partition_context, event):
    logger.info("Received de event {} from partition {}".format(event.body_as_str(encoding="UTF-8"),partition_context.partition_id))
    await partition_context.update_checkpoint(event)

async def receive():
    client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group, eventhub_name=eventhub_name)
    async with client:
        await client.receive(
            on_event=on_event,
            partition_id='0',
            starting_position="-1",
        )

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(receive())