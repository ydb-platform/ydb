from .graphite_encoder import GraphiteEncoder
import asyncio
import time
from aiographite.protocol import PlaintextProtocol, PickleProtocol
from typing import Tuple, List, Callable

DEFAULT_GRAPHITE_PICKLE_PORT = 2004
DEFAULT_GRAPHITE_PLAINTEXT_PORT = 2003


async def connect(host, port=DEFAULT_GRAPHITE_PLAINTEXT_PORT,
                  protocol=PlaintextProtocol(), loop=None):
    """
    A factory for connecting to Graphite Server.

    args: host, port, protocol, loop.

    Returns an instantiated AIOGraphite .
    """
    conn = AIOGraphite(host, port, protocol, loop)
    await conn._connect()
    return conn


class AioGraphiteSendException(Exception):
    pass


class AIOGraphite:
    """
    AIOGraphite is a Graphite client class, ultilizing asyncio,
    designed to help Graphite users to send data into graphite easily.

    Loop parameter is removed from this version as in python 3.10 and above parameter is removed
    """

    def __init__(self, graphite_server,
                 graphite_port=DEFAULT_GRAPHITE_PLAINTEXT_PORT,
                 protocol=PlaintextProtocol(), timeout=None):
        if not isinstance(protocol, (PlaintextProtocol, PickleProtocol)):
            raise AioGraphiteSendException("Unsupported Protocol!")
        self._graphite_server = graphite_server
        self._graphite_port = graphite_port
        self._graphite_server_address = (graphite_server, graphite_port)
        self._reader, self._writer = None, None
        self._timeout = timeout
        self.protocol = protocol

    async def __aenter__(self):
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc_val, tb):
        await self.close()

    async def send(self, metric: str, value: int, timestamp: int=None) -> None:
        """
        send a single metric.

        args: metric, value, timestamp. (str, int, int).
        """
        if not metric:
            return
        timestamp = int(timestamp or time.time())
        # Generate message based on protocol
        listOfMetricTuples = [(metric, value, timestamp)]
        message = self.protocol.generate_message(listOfMetricTuples)
        # Sending Data
        await self._send_message(message)

    async def send_multiple(self, dataset: List[Tuple],
                            timestamp: int=None) -> None:
        """
        send a list of tuples.

        args: a list of tuples (metric, value, timestamp), and timestamp
        is optional.
        """
        if not dataset:
            return
        timestamp = int(timestamp or time.time())
        # Generate message based on protocol
        message = self._generate_message_for_data_list(
            dataset,
            timestamp,
            self.protocol.generate_message)
        # Sending Data
        await self._send_message(message)

    async def close(self) -> None:
        """
        Close the TCP connection to graphite server.
        """
        await self._disconnect()

    async def _connect(self) -> None:
        """
        Connect to Graphite Server based on Provided Server Address
        """
        try:
            self._reader, self._writer = await asyncio.open_connection(
                self._graphite_server,
                self._graphite_port)
        except Exception as e:
            raise AioGraphiteSendException(f"Unable to connect to the provided server address "
                                           f"{self._graphite_server_address} due to error : {e}")

    async def _disconnect(self) -> None:
        """
        Close the TCP connection to graphite server.
        """
        try:
            if self._writer:
                self._writer.close()
        finally:
            self._writer = None
            self._reader = None

    def clean_and_join_metric_parts(self, metric_parts: List[str]) -> str:
        """
        This method helps encode any input metric to valid metric for graphite
        in case that the metric name includes any special character which is
        not supported by Graphite.

        args: a list of metric parts(string).

        returns a valid metric name for graphite.

        example:

        .. code:: python

            metric = aiographite.clean_and_join_metric_parts(metric_parts)
        """
        return ".".join([
                GraphiteEncoder.encode(dir_name) for dir_name in metric_parts
            ])

    async def _send_message(self, message: bytes) -> None:
        """
            @message: data ready to sent to graphite server
        """
        if not self._writer:
            await self._connect()
        attempts = 3
        while attempts > 0:
            try:
                self._writer.write(message)
                await asyncio.wait_for(
                    self._writer.drain(),
                    timeout=self._timeout
                    )
                return
            except Exception:
                # If failed to send data, then try to set up a
                # new connection
                try:
                    await self._disconnect()
                    await self._connect()
                except Exception:
                    # if all attempts failed, then raise exception
                    if attempts == 1:
                        raise AioGraphiteSendException(
                                "Failed to send metrics after"
                                "reaching max retries!"
                            )
                    else:
                        pass
                attempts = attempts - 1

    def _generate_message_for_data_list(
                self, dataset: List[Tuple], timestamp: int,
                generate_message_function: Callable[
                        [List[Tuple[str, int, int]]], bytes
                    ]
            ) -> bytes:
        """
            generate proper formatted message
            @param:
            Support two kinds of dataset
                1)  dataset = [(metric1, value1), (metric2, value2), ...]
                or
                2)  dataset = [(metric1, value1, timestamp1),
                               (metric2, value2, timestamp2), ...]
        """
        listofData = []
        for data in dataset:
            # unpack metric data
            if len(data) == 2:
                (metric, value) = data
            else:
                (metric, value, data_timestamp) = data
                timestamp = data_timestamp
            listofData.append((metric, value, timestamp))
        message = generate_message_function(listofData)
        return message
