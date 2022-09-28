
import logging
import random
from unittest.result import failfast
from urllib import request
import azure.functions as func
import json
import time
import time
import os
from ams_lib.laws import AzureLogAnalyticsSync
from ams_lib.laws_async import AzureLogAnalyticsAsync
from ams_lib.utils import get_default_log_analytics_data
import requests
import aiohttp

from typing import List, Dict

async def main(msg: func.QueueMessage, context:func.Context) -> None:
    start_time = time.time()
    sleep_time = float(os.environ.get('queue_sleep_time', 10))
    activity_data = json.loads(msg.get_body().decode('utf-8'))
    time_pushed = activity_data['epoch_time']
    data = dict()
    data['msg'] = activity_data
    data['parent_invocation_id'] = activity_data['invocation_id']
    data['start_time_from_queue_push'] = int(time.time()) - time_pushed
    read_blob_failure_count = 0
    laws_insertion_failure_count = 0
    ams_code_start = time.time()
    try:
        timeout = aiohttp.ClientTimeout(2)
        async with aiohttp.ClientSession() as session:
            blob_url = "https://nitinagarwalscalete95b0.blob.core.windows.net/dummy-data-1/mock_data.json"
            async with session.get(blob_url, timeout=timeout) as resp:
                blob_data = await resp.json()
    except:
        logging.exception("failed to get blob data")
        read_blob_failure_count = 1
        with open("dummy_data/mock_data.json", 'r') as debug_config:
            blob_data = debug_config.read()
        blob_data = json.loads(blob_data)

    laws_start_time = time.time()
    try:
        laws_data = generate_json_string(blob_data['cols'], blob_data['results'])
        table_name = f'Table{random.randint(1,10)}'
        # azure_laws_async = AzureLogAnalyticsAsync()
        # await azure_laws_async.post_data(table_name, json.dumps(laws_data))
        azure_laws_sync = AzureLogAnalyticsAsync()
        azure_laws_sync.post_data(table_name, json.dumps(laws_data))
    except:
        logging.exception("failed to push data to laws")
        laws_insertion_failure_count = 1

    finally:
        data['laws_finish_time'] = time.time() - laws_start_time
        data['ams_code_execution_time'] = time.time() - ams_code_start

    sleep_time = max(0, sleep_time - (int(time.time()) - start_time))
    time.sleep(sleep_time)
    data['ams_code_sleep_time'] = sleep_time
    data['ams_control_start_time_epoch'] = convert_epoch_to_datetime(int(start_time))
    data['finish_time_from_queue_push'] = int(time.time()) - time_pushed
    data['time_used_by_ams'] = time.time() - start_time
    data['ams_control_end_time_epoch'] = convert_epoch_to_datetime(int(time.time()))
    data['provider_version'] = os.environ.get('provider_version', None)
    data['laws_insertion_failure_count'] = laws_insertion_failure_count
    data['read_blob_failure_count'] = read_blob_failure_count
    logging.info(json.dumps(data))


def convert_epoch_to_datetime(epoch):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch))

def generate_json_string(col_index: Dict, result_rows: List) -> list:
        """convert sql server metrics into json string which can be pushed to laws.

        Args:
            col_index (Dict): column names
            result_rows (List): result rows for the metrics check

        Returns:
            str: jsonstring of the metrics data
        """
        # The correlation_id can be used to group fields from the same metrics
        # call
        log_data = []

        # Iterate through all rows of the last query result
        for row in result_rows:
            log_item = get_default_log_analytics_data("foobar",
                                                    {})
            idx = 0
            while idx < (col_index.__len__()):
                # Unless it's the column mapped to TimeGenerated, remove
                # internal fields
                if (col_index[idx].startswith("_") or col_index[idx] == "DUMMY"):
                    continue
                #
                log_item[col_index[idx]] = row[idx]
                idx=idx+1

            log_data.append(log_item)

        # create the json file, set the default to str (string) to cope with date/time columns

        return log_data
