# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
from email.utils import mktime_tz, parsedate_tz

__all__ = ["BaseHTTPError", "RateLimitReachedError", "exception_from_message"]


class BaseHTTPError(Exception):
    """
    The base exception class for all HTTP related exceptions.
    """

    def __init__(self, code, message, headers=None):
        self.code = code
        self.message = message
        self.headers = headers
        # preserve old exception behavior for tests that
        # look for e.args[0]
        super().__init__(message)

    def __str__(self):
        return self.message


class RateLimitReachedError(BaseHTTPError):
    """
    HTTP 429 - Rate limit: you've sent too many requests for this time period.
    """

    code = 429
    message = "%s Rate limit exceeded" % (code)

    def __init__(self, *args, **kwargs):
        headers = kwargs.pop("headers", None)
        super().__init__(self.code, self.message, headers)
        if self.headers is not None:
            self.retry_after = float(self.headers.get("retry-after", 0))
        else:
            self.retry_after = 0


_error_classes = [RateLimitReachedError]
_code_map = {c.code: c for c in _error_classes}


def exception_from_message(code, message, headers=None):
    """
    Return an instance of BaseHTTPException or subclass based on response code.

    If headers include Retry-After, RFC 2616 says that its value may be one of
    two formats: HTTP-date or delta-seconds, for example:

    Retry-After: Fri, 31 Dec 1999 23:59:59 GMT
    Retry-After: 120

    If Retry-After comes in HTTP-date, it'll be translated to a positive
    delta-seconds value when passing it to the exception constructor.

    Also, RFC 2616 says that Retry-After isn't just only applicable to 429
    HTTP status, but also to other responses, like 503 and 3xx.

    Usage::
        raise exception_from_message(code=self.status,
                                     message=self.parse_error(),
                                     headers=self.headers)
    """
    kwargs = {"code": code, "message": message, "headers": headers}

    if headers and "retry-after" in headers:
        http_date = parsedate_tz(headers["retry-after"])
        if http_date is not None:
            # Convert HTTP-date to delay-seconds
            delay = max(0, int(mktime_tz(http_date) - time.time()))
            headers["retry-after"] = str(delay)
    cls = _code_map.get(code, BaseHTTPError)
    return cls(**kwargs)
