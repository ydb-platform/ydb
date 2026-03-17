from typing import Awaitable

import requests
from pyhanko_certvalidator._asyncio_compat import to_thread

from ..api import DEFAULT_USER_AGENT
from ..common_utils import queue_fetch_task

__all__ = ['RequestsFetcherMixin']


class RequestsFetcherMixin:
    def __init__(self, user_agent=None, per_request_timeout=10):
        self.user_agent = user_agent or DEFAULT_USER_AGENT
        self.per_request_timeout = per_request_timeout
        self.__results = {}
        self.__result_events = {}

    def get_results(self):
        return {
            v for v in self.__results.values() if not isinstance(v, Exception)
        }

    def get_results_for_tag(self, tag):
        result = self.__results[tag]
        if isinstance(result, Exception):
            raise KeyError

    def _iter_results(self):
        for k, v in self.__results.items():
            if not isinstance(v, Exception):
                yield k, v

    async def _perform_fetch(self, tag, fetch_fun):
        return await queue_fetch_task(
            self.__results, self.__result_events, tag, fetch_fun
        )

    def _get(
        self, url, *, acceptable_content_types
    ) -> Awaitable[requests.Response]:
        def task():
            headers = {
                'Accept': ','.join(acceptable_content_types),
                'User-Agent': self.user_agent,
            }
            response = requests.get(
                url=url, timeout=self.per_request_timeout, headers=headers
            )
            if response.status_code != 200:
                raise requests.RequestException(
                    f"status code {response.status_code}"
                )
            return response

        return to_thread(task)

    def _post(
        self, url, data, *, content_type, acceptable_content_types
    ) -> Awaitable[requests.Response]:
        def task():
            headers = {
                'Accept': ','.join(acceptable_content_types),
                'User-Agent': self.user_agent,
                'Content-Type': content_type,
            }
            response = requests.post(
                url=url,
                timeout=self.per_request_timeout,
                headers=headers,
                data=data,
            )
            if response.status_code != 200:
                raise requests.RequestException(
                    f"status code {response.status_code}"
                )
            return response

        return to_thread(task)
