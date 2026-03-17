from udsoncan.connections import BaseConnection
from udsoncan.exceptions import TimeoutException


class DoIPClientUDSConnector(BaseConnection):
    """
    A udsoncan connector which uses an existing DoIPClient as a DoIP transport layer for UDS (instead of ISO-TP).

    :param doip_layer: The DoIP Transport layer object coming from the ``doipclient`` package.
    :type doip_layer: :class:`doipclient.DoIPClient<python_doip.DoIPClient>`

    :param name: This name is included in the logger name so that its output can be redirected. The logger name will be ``Connection[<name>]``
    :type name: string

    :param close_connection: True if the wrapper's close() function should close the associated DoIP client. This is not the default
    :type name: bool

    """

    def __init__(self, doip_layer, name=None, close_connection=False):
        BaseConnection.__init__(self, name)
        self._connection = doip_layer
        self._close_connection = close_connection
        self.opened = False

    def open(self):
        self.opened = True

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        if self._close_connection:
            self._connection.close()
        self.opened = False

    def is_open(self):
        return self.opened

    def specific_send(self, payload):
        self._connection.send_diagnostic(bytearray(payload))

    def specific_wait_frame(self, timeout=2):
        try:
            return bytes(self._connection.receive_diagnostic(timeout=timeout))
        except TimeoutError as e:
            raise TimeoutException(str(e)) from e

    def empty_rxqueue(self):
        self._connection.empty_rxqueue()

    def empty_txqueue(self):
        self._connection.empty_txqueue()
