import os
import fcntl
import socket
from .poller import POLL_EVENT_TYPE


class PipeNotifier(object):

    def __init__(self, poller, callback = None):
        self.__callback = callback
        self.__pipeR, self.__pipeW = os.pipe()

        flag = fcntl.fcntl(self.__pipeR, fcntl.F_GETFD)
        fcntl.fcntl(self.__pipeR, fcntl.F_SETFL, flag | os.O_NONBLOCK)

        flag = fcntl.fcntl(self.__pipeW, fcntl.F_GETFD)
        fcntl.fcntl(self.__pipeW, fcntl.F_SETFL, flag | os.O_NONBLOCK)

        poller.subscribe(self.__pipeR, self.__onNewNotification, POLL_EVENT_TYPE.READ)

    def notify(self):
        os.write(self.__pipeW, b'o')

    def __onNewNotification(self, descr, eventMask):
        try:
            while os.read(self.__pipeR, 1024):
                pass
        except OSError as e:
            if e.errno not in (socket.errno.EAGAIN, socket.errno.EWOULDBLOCK):
                raise
        if self.__callback is not None:
            self.__callback()
