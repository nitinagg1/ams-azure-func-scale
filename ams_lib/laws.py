"""
This module provides functionality to log metrics to Log Analytics.
"""
import json
import base64
import hmac
import datetime
import hashlib
import os
import logging
import requests
from requests.adapters import HTTPAdapter, Retry
from ams_lib.constants import LAWS_ENDPOINT_FORMAT, LAWS_MAX_RETRIES, \
    LAWS_BACKOFF_FACTOR, LAWS_RETRY_ERROR_CODES, REQUESTS_RETRY_URL_PREFIX, \
    LAWS_RETRY_ALLOWED_METHODS

class AzureLogAnalyticsSync:
    """
    This class provides the functionality to log metrics to Log Analytics Workspace (LAWS).
    """
    def __init__(self):
        self.shared_key = os.environ.get('laws_shared_key')
        self.workspace_id = os.environ.get('laws_workspace_id')

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

    def post_data(self, custom_log, body)->bool:
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
            session = requests.Session()
            retries = Retry(total=LAWS_MAX_RETRIES, backoff_factor= LAWS_BACKOFF_FACTOR,
                            status_forcelist= LAWS_RETRY_ERROR_CODES,
                            allowed_methods=LAWS_RETRY_ALLOWED_METHODS)

            session.mount(REQUESTS_RETRY_URL_PREFIX, HTTPAdapter(max_retries=retries))
            response = session.post(uri, data=body, headers=headers, timeout=5)
            if not (response.status_code >= 200 and response.status_code <= 299):
                logging.error(
                    f"unable to Write: code {response.status_code} message {response.text}")
                raise Exception("got exception from LAWS")

        except Exception as exception:
            raise

        return True