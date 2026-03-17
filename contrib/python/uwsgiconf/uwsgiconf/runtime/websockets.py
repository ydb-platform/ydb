from typing import Union

from .. import uwsgi


handshake = uwsgi.websocket_handshake


def recv(request_context=None, non_blocking: bool = False) -> bytes:
    """Receives data from websocket.

    :param request_context:
        .. note:: uWSGI 2.1+

    :param non_blocking:

    :raises IOError: If unable to receive a message.

    """
    return uwsgi.websocket_recv_nb() if non_blocking else uwsgi.websocket_recv()


def send(message: Union[str, bytes], request_context=None):
    """Sends a message to websocket.

    :param message: data to send

    :param request_context:
        .. note:: uWSGI 2.1+

    :raises IOError: If unable to send a message.

    """
    return uwsgi.websocket_send(message) if isinstance(message, str) else uwsgi.websocket_send_binary(message)
