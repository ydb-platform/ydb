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

import ssl
import time
import socket
import logging
from datetime import datetime, timedelta
from functools import wraps

from libcloud.utils.py3 import httplib
from libcloud.common.exceptions import RateLimitReachedError

__all__ = [
    "Retry",
    "RetryForeverOnRateLimitError",
]

_logger = logging.getLogger(__name__)
# Error message which indicates a transient SSL error upon which request
# can be retried
TRANSIENT_SSL_ERROR = "The read operation timed out"


class TransientSSLError(ssl.SSLError):
    """Represent transient SSL errors, e.g. timeouts"""

    pass


# Constants used by the ``retry`` class
# All the time values (timeout, delay, backoff) are in seconds
DEFAULT_TIMEOUT = 30  # default retry timeout
DEFAULT_DELAY = 1  # default sleep delay used in each iterator
DEFAULT_BACKOFF = 1  # retry backup multiplier
RETRY_EXCEPTIONS = (
    RateLimitReachedError,
    socket.error,
    socket.gaierror,
    httplib.NotConnected,
    httplib.ImproperConnectionState,
    TransientSSLError,
)


class MinimalRetry:
    def __init__(
        self,
        retry_delay=DEFAULT_DELAY,
        timeout=DEFAULT_TIMEOUT,
        backoff=DEFAULT_BACKOFF,
    ):
        """
        Wrapper around retrying that helps to handle common transient
        exceptions.

        This minimalistic version only retries SSL errors and rate limiting.

        :param retry_delay: retry delay between the attempts.
        :param timeout: maximum time to wait.
        :param backoff: multiplier added to delay between attempts.

        :Example:

        retry_request = MinimalRetry(timeout=1, retry_delay=1, backoff=1)
        retry_request(self.connection.request)()
        """

        if retry_delay is None:
            retry_delay = DEFAULT_DELAY
        if timeout is None:
            timeout = DEFAULT_TIMEOUT
        if backoff is None:
            backoff = DEFAULT_BACKOFF

        timeout = max(timeout, 0)

        self.retry_delay = retry_delay
        self.timeout = timeout
        self.backoff = backoff

    def __call__(self, func):
        def transform_ssl_error(function, *args, **kwargs):
            try:
                return function(*args, **kwargs)
            except ssl.SSLError as exc:
                if TRANSIENT_SSL_ERROR in str(exc):
                    raise TransientSSLError(*exc.args)

                raise exc

        @wraps(func)
        def retry_loop(*args, **kwargs):
            current_delay = self.retry_delay
            end = datetime.now() + timedelta(seconds=self.timeout)
            last_exc = None

            while datetime.now() < end:
                try:
                    return transform_ssl_error(func, *args, **kwargs)
                except Exception as exc:
                    last_exc = exc

                    if isinstance(exc, RateLimitReachedError):
                        _logger.debug("You are being rate limited, backing " "off...")

                        # NOTE: Retry after defaults to 0 in the
                        # RateLimitReachedError class so we a use more
                        # reasonable default in case that attribute is not
                        # present. This way we prevent busy waiting, etc.
                        retry_after = exc.retry_after if exc.retry_after else 2
                        time.sleep(retry_after)

                        # Reset delay if we're told to wait due to rate
                        # limiting
                        current_delay = self.retry_delay
                    elif self.should_retry(exc):
                        time.sleep(current_delay)
                        current_delay *= self.backoff
                    else:
                        raise

            raise last_exc

        return retry_loop

    def should_retry(self, exception):
        return False


class Retry(MinimalRetry):
    def __init__(
        self,
        retry_exceptions=RETRY_EXCEPTIONS,
        retry_delay=DEFAULT_DELAY,
        timeout=DEFAULT_TIMEOUT,
        backoff=DEFAULT_BACKOFF,
    ):
        """
        Wrapper around retrying that helps to handle common transient
        exceptions.

        This version retries the errors that
        `libcloud.utils.retry:MinimalRetry` retries and all errors of the
        exception types that are given.

        :param retry_exceptions: types of exceptions to retry on.
        :param retry_delay: retry delay between the attempts.
        :param timeout: maximum time to wait.
        :param backoff: multiplier added to delay between attempts.

        :Example:

        retry_request = Retry(retry_exceptions=(httplib.NotConnected,),
                              timeout=1, retry_delay=1, backoff=1)
        retry_request(self.connection.request)()
        """

        super().__init__(retry_delay=retry_delay, timeout=timeout, backoff=backoff)
        if retry_exceptions is None:
            retry_exceptions = RETRY_EXCEPTIONS
        self.retry_exceptions = retry_exceptions

    def should_retry(self, exception):
        return isinstance(exception, tuple(self.retry_exceptions))


class RetryForeverOnRateLimitError(Retry):
    """
    This class is only here for backward compatibility reasons with
    pre-Libcloud v3.3.2.

    If works by ignoring timeout argument and retrying forever until API
    is returning 429 RateLimitReached errors.

    In most cases using this class is not a good idea since it can cause code
    to hang and retry for ever in case API continues to return retry limit
    reached.
    """

    def __call__(self, func):
        def transform_ssl_error(function, *args, **kwargs):
            try:
                return function(*args, **kwargs)
            except ssl.SSLError as exc:
                if TRANSIENT_SSL_ERROR in str(exc):
                    raise TransientSSLError(*exc.args)

                raise exc

        @wraps(func)
        def retry_loop(*args, **kwargs):
            current_delay = self.retry_delay
            end = datetime.now() + timedelta(seconds=self.timeout)

            while True:
                try:
                    return transform_ssl_error(func, *args, **kwargs)
                except Exception as exc:
                    if isinstance(exc, RateLimitReachedError):
                        time.sleep(exc.retry_after)

                        # Reset retries if we're told to wait due to rate
                        # limiting
                        current_delay = self.retry_delay
                        end = datetime.now() + timedelta(seconds=exc.retry_after + self.timeout)
                    elif datetime.now() >= end:
                        raise
                    elif self.should_retry(exc):
                        time.sleep(current_delay)
                        current_delay *= self.backoff
                    else:
                        raise

        return retry_loop
