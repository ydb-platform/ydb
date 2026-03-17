import os
import time

from .encryptor import getEncryptor
from .node import Node, TCPNode
from .poller import createPoller
from .tcp_connection import TcpConnection


class UtilityException(Exception):
    pass


class Utility(object):

    def __init__(self, password=None, timeout=900.0):
        """
        Initialise the utility object

        :param password: password for encryption
        :type password: str or None
        :param timeout: communication timeout
        :type timeout: float
        """

    def executeCommand(self, node, command):
        """
        Executes command on the given node.

        :param node: where to execute the command
        :type node: Node or str
        :param command: the command which should be sent
        :type command: list
        :returns: result
        :rtype: any object
        :raises: UtilityException in case of error
        """


class TcpUtility(Utility):

    def __init__(self, password=None, timeout=900.0):
        self.__timeout = timeout
        self.__poller = createPoller('auto')
        self.__connection = TcpConnection(self.__poller,
                                          onDisconnected=self.__onDisconnected,
                                          onMessageReceived=self.__onMessageReceived,
                                          onConnected=self.__onConnected,
                                          timeout=timeout)
        if password is not None:
            self.__connection.encryptor = getEncryptor(password)

        self.__result = None
        self.__error = None

    def executeCommand(self, node, command):
        self.__result = None
        self.__error = None

        if not isinstance(node, Node):
            try:
                node = TCPNode(node)
            except Exception:
                self.__error = 'invalid address to connect'
                return

        self.__isConnected = self.__connection.connect(node.ip, node.port)
        if not self.__isConnected:
            self.__error = "can't connected"
            return

        deadline = time.time() + self.__timeout

        self.__data = command
        while self.__isConnected:
            self.__poller.poll(0.5)
            if time.time() > deadline:
                self.__connection.disconnect()

        if self.__result is None:
            raise UtilityException(self.__error)

        return self.__result

    def __onMessageReceived(self, message):
        if self.__connection.encryptor and not self.__connection.sendRandKey:
            self.__connection.sendRandKey = message
            self.__connection.send(self.__data)
            return

        self.__result = message

        self.__connection.disconnect()

    def __onDisconnected(self):
        self.__isConnected = False
        if self.__result is None:
            self.__error = 'connection lost'

    def __onConnected(self):
        if self.__connection.encryptor:
            self.__connection.recvRandKey = os.urandom(32)
            self.__connection.send(self.__connection.recvRandKey)
            return

        self.__connection.send(self.__data)
