"""
This module provides functionality to log metrics to Log Analytics.
"""
import base64
import hmac
import datetime
import hashlib
import os
import logging
from ams_lib.constants import LAWS_ENDPOINT_FORMAT
import aiohttp
import time


class AzureLogAnalyticsAsync:
    """
    This class provides the functionality to log metrics to Log Analytics Workspace (LAWS).
    """
    def __init__(self):
        self.shared_key = os.environ.get('laws_shared_key')
        self.workspace_id = os.environ.get('laws_workspace_id', "659eaa28-9f7e-4176-958d-a37f0ca99754")

    def build_authorization_signature(self, date, content_length,
                                      method, content_type, resource):
        """
        Returns authorization header which will be used when
            sending data into Azure Log Analytics.
        """

        x_headers = 'x-ms-date:' + date
        string_to_hash = method + "\n" \
                                + str(content_length) + "\n" \
                                + content_type + "\n" \
                                + x_headers + "\n" \
                                + resource
        bytes_to_hash = bytes(string_to_hash, 'UTF-8')
        decoded_key = base64.b64decode(self.shared_key)
        encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash,
                                                 digestmod=hashlib.sha256).digest()).\
            decode('utf-8')
        authorization = f"SharedKey {self.workspace_id}:{encoded_hash}"
        return authorization

    async def post_data(self, custom_log, body)->bool:
        """
        method to push json data to LAWS.

        Args:
            custom_log: the log type for LAWS.
            body: json string that needs to be pushed into LAWS.

        Returns:
           requests.Response: Response to Http request to post data to log analytics workspace.
        """

        method = 'POST'
        content_type = 'application/json'
        resource = '/api/logs'
        rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        content_length = len(body)
        signature = self.build_authorization_signature(rfc1123date, content_length, method,
                                                       content_type, resource)

        #TODO(nitinagarwal): the uri will not work for other clouds like national clouds. handle it properly.
        uri = LAWS_ENDPOINT_FORMAT.format(laws_workspace_id= self.workspace_id,
                                        resource= resource)
        headers = {
            'content-type': content_type,
            'Authorization': signature,
            'Log-Type': custom_log,
            'x-ms-date': rfc1123date
        }
        try:
            start_time = time.time()
            timeout = aiohttp.ClientTimeout(3)
            async with aiohttp.ClientSession() as session:
                async with session.post(uri, data=body, headers=headers, timeout=timeout) as response:
                    await response.read()
                    if not (response.status >= 200 and response.status <= 299):
                        logging.error(
                            f"unable to Write: code {response.status} message {response.text}")
                        raise Exception("got exception from LAWS")
                    else:
                        logging.debug("total time taken to push data to log analytics "+\
                            f"workspace: {time.time()-start_time}")

        except Exception as exception:
            raise

        return True