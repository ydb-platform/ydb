from typing import List, Optional


class RequestMetrics(object):
    def __init__(
        self,
        request_id,
        request_duration_ms,
        usage: Optional[List[str]] = None,
    ):
        self.request_id = request_id
        self.request_duration_ms = request_duration_ms
        self.usage = usage

    def payload(self):
        ret = {
            "request_id": self.request_id,
            "request_duration_ms": self.request_duration_ms,
        }

        if self.usage:
            ret["usage"] = self.usage
        return ret
