__all__ = [
    'AsyncTolokaClient',
]
import asyncio
import datetime
import functools
import logging
import threading
from decimal import Decimal
from typing import Dict, Optional, Callable, List

import attr
import httpx
from toloka.client.batch_create_results import FieldValidationError

from ..client import TolokaClient, structure, unstructure
from ..client.exceptions import (
    raise_on_api_error,
    ValidationApiError,
)
from ..client.operations import Operation
from ..client.primitives.parameter import IdempotentOperationParameters
from ..client.primitives.retry import AsyncRetryingOverURLLibRetry
from ..util._managing_headers import add_headers
from ..util.async_utils import generate_async_methods_from

logger = logging.getLogger(__name__)


@generate_async_methods_from(TolokaClient)
class AsyncTolokaClient:
    """Class that implements interaction with [Toloka API](https://toloka.ai/docs/api/api-reference/), in an asynchronous way.

    All methods are wrapped as async. So all methods calls must be awaited.
    All arguments, same as in TolokaClient.
    """

    @functools.wraps(TolokaClient.__init__)
    def __init__(
        self,
        *args, **kwargs
    ):
        self._sync_client = TolokaClient(*args, **kwargs)
        self.retrying = AsyncRetryingOverURLLibRetry(
            base_url=str(self._session.base_url), retry=self.retryer_factory(), reraise=True,
            exception_to_retry=self.EXCEPTIONS_TO_RETRY,
        )

    def __getattr__(self, name):
        """Access non function fields.

        All function fields should be already overridden with `generate_async_methods_from`."""
        return getattr(self._sync_client, name)

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state

    @classmethod
    def from_sync_client(cls, client: TolokaClient) -> 'AsyncTolokaClient':
        async_client = cls.__new__(cls)
        async_client.__init__(
            token=client.token, url=client.url, retries=client.retryer_factory(), timeout=client.default_timeout,
            act_under_account_id=client.act_under_account_id, retry_quotas=None, verify=client.verify,
        )
        async_client._sync_client = client
        return async_client

    @property
    def sync_client(self) -> TolokaClient:
        return self._sync_client

    @functools.lru_cache(maxsize=128)
    def _session_for_thread_for_event_loop(self, thread_id: int, event_loop_id: int) -> httpx.AsyncClient:
        client = httpx.AsyncClient(headers=self._headers, base_url=self.url, verify=self.verify)
        return client

    @property
    def _session(self):
        event_loop_id = id(asyncio.get_event_loop())
        return self._session_for_thread_for_event_loop(threading.current_thread().ident, event_loop_id)

    async def _do_request_with_retries(self, method, path, **kwargs):
        @self.retrying.wraps
        async def wrapped(method, url, **kwargs):
            response = await self._session.request(method, url, **kwargs)
            raise_on_api_error(response)
            return response

        return await wrapped(method, path, **kwargs)

    async def _request(self, method, path, **kwargs):
        return (await self._raw_request(method, path, **kwargs)).json(parse_float=Decimal)

    async def _find_all(self, find_function, request, sort_field: str = 'id',
                        items_field: str = 'items', batch_size: Optional[int] = None):
        result = await find_function(request, sort=[sort_field], limit=batch_size)
        items = getattr(result, items_field)
        while result.has_more:
            request = attr.evolve(request, **{f'{sort_field}_gt': getattr(items[-1], sort_field)})
            for item in items:
                yield item
            result = await find_function(request, sort=[sort_field])
            items = getattr(result, items_field)

        for item in items:
            yield item

    @add_headers('async_client')
    async def wait_operation(
        self,
        op: Operation,
        timeout: datetime.timedelta = datetime.timedelta(minutes=10),
        logger=logger,
    ) -> Operation:
        """Asynchronous version of wait_operation"""
        default_time_to_wait = datetime.timedelta(seconds=1)
        default_initial_delay = datetime.timedelta(milliseconds=500)

        if op.is_completed():
            return op

        utcnow = datetime.datetime.now(datetime.timezone.utc)
        wait_until_time = utcnow + timeout

        if not op.started or utcnow - op.started < default_initial_delay:
            await asyncio.sleep(default_initial_delay.total_seconds())

        while True:
            op = await self.get_operation(op.id)
            if op.is_completed():
                return op
            await asyncio.sleep(default_time_to_wait.total_seconds())
            if datetime.datetime.now(datetime.timezone.utc) > wait_until_time:
                raise TimeoutError

    async def _sync_via_async_pool_related(
            self,
            objects,
            parameters: IdempotentOperationParameters,
            url: str,
            result_type,
            operation_type: Operation,
            output_id_field: str,
            get_method: Callable,
    ):
        if not parameters.async_mode:
            response = await self._request('post', url, json=unstructure(objects), params=unstructure(parameters))
            return structure(response, result_type)
        is_single = not isinstance(objects, list)
        insert_operation = await self._start_sync_via_async(objects, parameters, url, operation_type)

        pools = {}
        validation_errors = {}
        for log_item in await self.get_operation_log(insert_operation.id):
            if '__item_idx' in log_item.input:
                index = log_item.input['__item_idx']
            else:
                continue  # operation could be not just creating objects (e.g. open_pool while creating_object)
            if log_item.success:
                numerated_ids = pools.setdefault(log_item.input['pool_id'], {})
                numerated_ids[log_item.output[output_id_field]] = index
            else:
                validation_errors[index] = structure(log_item.output, Dict[str, FieldValidationError])

        # Like in sync methods Exception will raise
        # even if the skip_invalid_items=True but no objects are created
        if validation_errors and not pools:
            raise ValidationApiError(
                code='VALIDATION_ERROR',
                message='Validation failed',
                payload=validation_errors,
            )

        if is_single:
            pool_id = list(pools.keys())[0]
            item_id = list(pools[pool_id].keys())[0]
            return await get_method(item_id)
        else:
            items = await self._collect_from_pools(get_method, pools)
            return result_type(items=items, validation_errors=validation_errors or {})

    async def _collect_from_pools(self, get_method, pools):
        items = {}
        for pool_id, numerated_ids in pools.items():
            obj_it = get_method(
                pool_id=pool_id,
                id_gte=min(numerated_ids.keys()),
                id_lte=max(numerated_ids.keys()),
            )
            async for obj in obj_it:
                if obj.id in numerated_ids:
                    items[numerated_ids[obj.id]] = obj
        return items

    async def _sync_via_async(
            self,
            objects: List,
            parameters: IdempotentOperationParameters,
            url: str,
            result_type,
            operation_type: Operation,
            output_id_field: str,
            get_method: Callable,
    ):
        if not parameters.async_mode:
            response = await self._request('post', url, json=unstructure(objects), params=unstructure(parameters))
            return structure(response, result_type)
        is_single = not isinstance(objects, list)
        insert_operation = await self._start_sync_via_async(objects, parameters, url, operation_type)

        item_id_to_idx = {}
        validation_errors = {}
        for log_item in await self.get_operation_log(insert_operation.id):
            if '__item_idx' in log_item.input:
                index = log_item.input['__item_idx']
            else:
                continue  # operation could be not just creating objects (e.g. open_pool while creating_object)
            if log_item.success:
                item_id_to_idx[log_item.output[output_id_field]] = index
            else:
                validation_errors[index] = log_item.output

        # Like as in sync methods Exception will raise
        # even if the skip_invalid_items=True but no objects are created
        if validation_errors and not item_id_to_idx:
            raise ValidationApiError(
                code='VALIDATION_ERROR',
                message='Validation failed',
                payload=validation_errors,
            )

        if is_single:
            item_id = list(item_id_to_idx.keys())[0]
            return await get_method(item_id)
        else:
            items = {}
            obj_it = get_method(
                id_gte=min(item_id_to_idx.keys()),
                id_lte=max(item_id_to_idx.keys()),
            )
            async for obj in obj_it:
                if obj.id in item_id_to_idx:
                    items[item_id_to_idx[obj.id]] = obj
            return result_type(items=items, validation_errors=validation_errors or {})
