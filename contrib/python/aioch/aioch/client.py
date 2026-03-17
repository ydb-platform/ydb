import asyncio

from clickhouse_driver import Client as BlockingClient
from clickhouse_driver.result import QueryInfo
from .result import ProgressQueryResult, QueryResult, IterQueryResult
from .utils import run_in_executor


class Client(object):
    def __init__(self, *args, **kwargs):
        self._loop = kwargs.pop('loop', None) or asyncio.get_event_loop()
        self._executor = kwargs.pop('executor', None)

        if '_client' not in kwargs:
            self._client = BlockingClient(*args, **kwargs)
        else:
            self._client = kwargs.pop('_client')

        super(Client, self).__init__()

    @classmethod
    def from_url(cls, url, loop=None, executor=None):
        """
        *New in version 0.0.2.*
        """

        _client = BlockingClient.from_url(url)
        return cls(_client=_client, loop=loop, executor=executor)

    def run_in_executor(self, *args, **kwargs):
        return run_in_executor(self._executor, self._loop, *args, **kwargs)

    async def disconnect(self):
        return await self.run_in_executor(self._client.disconnect)

    async def execute(self, *args, **kwargs):
        return await self.run_in_executor(self._client.execute, *args,
                                          **kwargs)

    async def execute_with_progress(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None, settings=None,
            types_check=False, columnar=False):
        self._client.make_query_settings(settings)

        await self.run_in_executor(self._client.connection.force_connect)

        self._client.last_query = QueryInfo()

        return await self.process_ordinary_query_with_progress(
            query, params=params, with_column_types=with_column_types,
            external_tables=external_tables,
            query_id=query_id, types_check=types_check, columnar=columnar
        )

    async def execute_iter(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None, settings=None,
            types_check=False):
        """
        *New in version 0.0.2.*
        """

        self._client.make_query_settings(settings)

        await self.run_in_executor(self._client.connection.force_connect)

        self._client.last_query = QueryInfo()

        return await self.iter_process_ordinary_query(
            query, params=params, with_column_types=with_column_types,
            external_tables=external_tables,
            query_id=query_id, types_check=types_check
        )

    async def process_ordinary_query_with_progress(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None,
            types_check=False, columnar=False):

        if params is not None:
            query = self._client.substitute_params(query, params)

        await self.run_in_executor(
            self._client.connection.send_query, query, query_id=query_id
        )
        await self.run_in_executor(
            self._client.connection.send_external_tables, external_tables,
            types_check=types_check
        )

        return await self.receive_result(
            with_column_types=with_column_types, progress=True,
            columnar=columnar
        )

    async def iter_process_ordinary_query(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None,
            types_check=False):

        if params is not None:
            query = self._client.substitute_params(query, params)

        await self.run_in_executor(
            self._client.connection.send_query, query, query_id=query_id
        )
        await self.run_in_executor(
            self._client.connection.send_external_tables, external_tables,
            types_check=types_check
        )
        return self.iter_receive_result(with_column_types=with_column_types)

    async def cancel(self, with_column_types=False):
        # TODO: Add warning if already cancelled.
        await self.run_in_executor(self._client.connection.send_cancel)
        # Client must still read until END_OF_STREAM packet.
        return await self.receive_result(with_column_types=with_column_types)

    async def iter_receive_result(self, with_column_types=False):
        gen = self.packet_generator()
        rows_gen = IterQueryResult(gen, with_column_types=with_column_types)

        async for rows in rows_gen:
            for row in rows:
                yield row

    async def packet_generator(self):
        receive_packet = self._client.receive_packet
        while True:
            try:
                packet = await self.run_in_executor(receive_packet)
                if not packet:
                    break

                if packet is True:
                    continue

                yield packet

            except (Exception, KeyboardInterrupt):
                await self.disconnect()
                raise

    async def receive_result(
            self, with_column_types=False, progress=False, columnar=False):

        gen = self.packet_generator()

        if progress:
            return ProgressQueryResult(
                gen, with_column_types=with_column_types, columnar=columnar
            )

        else:
            result = QueryResult(
                gen, with_column_types=with_column_types, columnar=columnar
            )
            return await result.get_result()
