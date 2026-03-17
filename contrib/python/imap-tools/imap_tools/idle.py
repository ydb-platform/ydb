import sys
import socket
import select
import imaplib
from typing import Optional, List

from .utils import check_command_status
from .errors import MailboxTaggedResponseError

imaplib.Commands.setdefault("IDLE", ("NONAUTH", "AUTH", "SELECTED"))  # noqa

SUPPORTS_SELECT_POLL = hasattr(select, 'poll')


def get_socket_poller(sock: socket.socket, timeout: Optional[int] = None):
    """
    Polls the socket for events telling us it's available to read
    :param sock: socket.socket
    :param timeout: seconds or None - seconds
    :return: polling object
    """
    if SUPPORTS_SELECT_POLL:
        # select.poll allows your process to have more than 1024 file descriptors
        poller = select.poll()
        poller.register(sock.fileno(), select.POLLIN)
        timeout = None if timeout is None else timeout * 1000
        return poller.poll(timeout)
    else:
        # select.select fails if your process has more than 1024 file descriptors, needs for windows and some other
        return select.select([sock], [], [], timeout)[0]


class IdleManager:
    """
    Mailbox IDLE logic
    Info about IMAP4 IDLE command at rfc2177
    Workflow examples:
    1.
        mailbox.idle.start()
        resps = mailbox.idle.poll(timeout=60)
        mailbox.idle.stop()
    2.
        resps = mailbox.idle.wait(timeout=60)
    """

    def __init__(self, mailbox) -> None:
        self.mailbox = mailbox
        self._idle_tag = None

    def start(self):
        """Switch on mailbox IDLE mode"""
        self._idle_tag = self.mailbox.client._command('IDLE')  # example: b'KLIG3'
        result = self.mailbox.client._get_response()
        check_command_status((result, 'IDLE start'), MailboxTaggedResponseError, expected=None)
        return result

    def stop(self):
        """Switch off mailbox IDLE mode"""
        self.mailbox.client.send(b"DONE\r\n")
        return self.mailbox.consume_until_tagged_response(self._idle_tag)

    def poll(self, timeout: Optional[float]) -> List[bytes]:
        """
        Poll for IDLE responses
        timeout = None
            Blocks until an IDLE response is received
        timeout = float
            Blocks until IDLE response is received or the timeout will expire
        :param timeout: seconds or None
        :return: list of raw responses
        result examples:
            [b'* 36 EXISTS', b'* 1 RECENT']
            [b'* 7 EXISTS']

        socket.settimeout modes:
            0 - non-blocking
            int - timeout mode
            None - blocking
        """
        if timeout is not None:
            timeout = float(timeout)
            if timeout > 29 * 60:
                raise ValueError(
                    'rfc2177 are advised to terminate the IDLE '
                    'and re-issue it at least every 29 minutes to avoid being logged off.'
                )
        sock = self.mailbox.client.sock
        old_timeout = sock.gettimeout()
        # make socket non-blocking so the timeout can be implemented for this call
        sock.settimeout(0)
        try:
            response_set = []
            events = get_socket_poller(sock, timeout)
            if events:
                while True:
                    try:
                        line = self.mailbox.client._get_line()
                    except (socket.timeout, socket.error):
                        break
                    except imaplib.IMAP4.abort:  # noqa
                        etype, evalue, etraceback = sys.exc_info()
                        if "EOF" in evalue.args[0]:
                            break
                        else:
                            raise
                    else:
                        response_set.append(line)
            return response_set
        finally:
            sock.settimeout(old_timeout)

    def wait(self, timeout: Optional[float]) -> List[bytes]:
        """
        Logic, step by step:
        1. Start idle mode
        2. Poll idle response
        3. Stop idle mode on response or timeout
        4. Return poll results
        :param timeout: for poll method
        :return: poll response
        """
        with self as idle:
            return idle.poll(timeout=timeout)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()
