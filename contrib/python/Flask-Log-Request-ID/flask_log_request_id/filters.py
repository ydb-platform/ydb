import logging
from .request_id import current_request_id


class RequestIDLogFilter(logging.Filter):
    """
    Log filter to inject the current request id of the request under `log_record.request_id`
    """

    def filter(self, log_record):
        log_record.request_id = current_request_id()
        return log_record
