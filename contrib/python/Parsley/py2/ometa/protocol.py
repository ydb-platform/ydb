from twisted.internet.protocol import Protocol
from twisted.python.failure import Failure

from ometa.tube import TrampolinedParser


class ParserProtocol(Protocol):
    """
    A Twisted ``Protocol`` subclass for parsing stream protocols.
    """


    def __init__(self, grammar, senderFactory, receiverFactory, bindings):
        """
        Initialize the parser.

        :param grammar: An OMeta grammar to use for parsing.
        :param senderFactory: A unary callable that returns a sender given a
                              transport.
        :param receiverFactory: A unary callable that returns a receiver given
                                a sender.
        :param bindings: A dict of additional globals for the grammar rules.
        """

        self._grammar = grammar
        self._bindings = dict(bindings)
        self._senderFactory = senderFactory
        self._receiverFactory = receiverFactory
        self._disconnecting = False

    def connectionMade(self):
        """
        Start parsing, since the connection has been established.
        """

        self.sender = self._senderFactory(self.transport)
        self.receiver = self._receiverFactory(self.sender)
        self.receiver.prepareParsing(self)
        self._parser = TrampolinedParser(
            self._grammar, self.receiver, self._bindings)

    def dataReceived(self, data):
        """
        Receive and parse some data.

        :param data: A ``str`` from Twisted.
        """

        if self._disconnecting:
            return

        try:
            self._parser.receive(data)
        except Exception:
            self.connectionLost(Failure())
            self.transport.abortConnection()
            return

    def connectionLost(self, reason):
        """
        Stop parsing, since the connection has been lost.

        :param reason: A ``Failure`` instance from Twisted.
        """

        if self._disconnecting:
            return
        self.receiver.finishParsing(reason)
        self._disconnecting = True
