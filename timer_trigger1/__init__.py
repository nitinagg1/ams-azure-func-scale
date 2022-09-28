import datetime
import os
import azure.functions as func
from azure.storage.queue.aio import QueueClient
from azure.storage.queue import TextBase64EncodePolicy
import asyncio
import json
import logging
import time
import typing

async def main(mytimer: func.TimerRequest, msg: func.Out[typing.List[str]], context: func.Context) -> None:
    logger = logging.getLogger()
    logger.info(f"website instance id is  {os.environ.get('WEBSITE_INSTANCE_ID')}")
    invocation_id = context.invocation_id
    data = dict()
    data['invocation_id'] = invocation_id

    msg_count = int(os.environ.get('msg_count', 1))
    start_time = datetime.datetime.now()
    result = {}
    result['invocatoin_id'] = invocation_id
    result['data'] = []
    message_list = []
    for i in range(msg_count):
        message = f'message_{i}'
        message_obj = {"message": message, "invocation_id": invocation_id, "epoch_time": int(time.time())}
        message_obj = json.dumps(message_obj)
        message_list.append(message_obj)

    msg.set(message_list)

    result['total_time'] = (datetime.datetime.now() - start_time).seconds
    result['total_messages'] = msg_count
    result['function_name'] = context.function_name
    logger.info(json.dumps(result))
    logger.info(f'messages pushed {msg_count}:{invocation_id}')