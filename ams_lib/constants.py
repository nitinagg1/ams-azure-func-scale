"""constants for ams lib.
"""
LAWS_MAX_RETRIES = 3

LAWS_BACKOFF_FACTOR = 0.5

LAWS_RETRY_ERROR_CODES = [413, 429, 500, 503]

LAWS_RETRY_ALLOWED_METHODS = ['GET', 'POST']

REQUESTS_RETRY_URL_PREFIX = "https://"

TIMEOUT_REQUEST_SEC = 5

LAWS_CONNECTION_TEST_TABLE = "Connection_test"

LAWS_POST_DATA_TIMEOUT_SEC = 5

LAWS_ENDPOINT_FORMAT = "https://{laws_workspace_id}.ods.opinsights.azure.com{resource}"+\
    "?api-version=2016-04-01"