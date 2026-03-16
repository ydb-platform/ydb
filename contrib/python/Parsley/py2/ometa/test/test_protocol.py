import unittest

import pytest

from ometa.grammar import OMeta
from ometa.runtime import ParseError


protocol = pytest.importorskip('ometa.protocol')
ParserProtocol = protocol.ParserProtocol

testingGrammarSource = """

someA = ('a' 'a') -> receiver('a')
someB = ('b' 'b') -> receiver('b')
someC = ('c' 'c') -> receiver('c')
someExc = 'e' -> receiver.raiseSomething()

initial = someA | someExc

"""
testGrammar = OMeta(testingGrammarSource).parseGrammar('testGrammar')


class SenderFactory(object):
    def __init__(self, transport):
        self.transport = transport


class SomeException(Exception):
    pass


class ReceiverFactory(object):
    currentRule = 'initial'

    def __init__(self, sender):
        self.sender = sender
        self.calls = []
        self.returnMap = {}
        self.connected = False
        self.lossReason = None
        self.parser = None

    def prepareParsing(self, parser):
        self.connected = True
        self.parser = parser

    def __call__(self, v):
        self.calls.append(v)
        if v in self.returnMap:
            self.currentRule = self.returnMap[v]

    def raiseSomething(self):
        raise SomeException()

    def finishParsing(self, reason):
        self.lossReason = reason


class FakeTransport(object):
    def __init__(self):
        self.aborted = False

    def abortConnection(self):
        self.aborted = True


class ParserProtocolTestCase(unittest.TestCase):
    def setUp(self):
        self.protocol = ParserProtocol(
            testGrammar, SenderFactory, ReceiverFactory, {})

    def test_transportPassed(self):
        """The sender is passed the transport recieved by the protocol."""
        transport = object()
        self.protocol.makeConnection(transport)
        self.assertEqual(transport, self.protocol.sender.transport)

    def test_senderPassed(self):
        """The sender is passed to the receiver."""
        self.protocol.makeConnection(None)
        self.assertEqual(self.protocol.sender, self.protocol.receiver.sender)

    def test_parserPassed(self):
        """The parser is passed in the prepareParsing method."""
        self.protocol.makeConnection(None)
        self.assertEqual(self.protocol, self.protocol.receiver.parser)

    def test_connectionEstablishes(self):
        """prepareParsing is called on the receiver after connection establishment."""
        self.protocol.makeConnection(None)
        self.assert_(self.protocol.receiver.connected)

    def test_basicParsing(self):
        """Rules can be parsed multiple times for the same effect."""
        self.protocol.makeConnection(None)
        self.protocol.dataReceived('aa')
        self.assertEqual(self.protocol.receiver.calls, ['a'])
        self.protocol.dataReceived('aa')
        self.assertEqual(self.protocol.receiver.calls, ['a', 'a'])

    def test_parsingChunks(self):
        """Any number of rules can be called from one dataRecived."""
        self.protocol.makeConnection(None)
        self.protocol.dataReceived('a')
        self.assertEqual(self.protocol.receiver.calls, [])
        self.protocol.dataReceived('aa')
        self.assertEqual(self.protocol.receiver.calls, ['a'])
        self.protocol.dataReceived('aaa')
        self.assertEqual(self.protocol.receiver.calls, ['a', 'a', 'a'])

    def test_ruleSwitching(self):
        """The rule being parsed can specify the next rule to be parsed."""
        self.protocol.makeConnection(None)
        self.protocol.receiver.returnMap.update(dict(a='someB', b='someA'))
        self.protocol.dataReceived('aa')
        self.assertEqual(self.protocol.receiver.calls, ['a'])
        self.protocol.dataReceived('bb')
        self.assertEqual(self.protocol.receiver.calls, ['a', 'b'])
        self.protocol.dataReceived('aa')
        self.assertEqual(self.protocol.receiver.calls, ['a', 'b', 'a'])

    def test_ruleSwitchingWithChunks(self):
        """Any number of rules can be called even during rule switching."""
        self.protocol.makeConnection(None)
        self.protocol.receiver.returnMap.update(dict(a='someB', b='someA'))
        self.protocol.dataReceived('a')
        self.assertEqual(self.protocol.receiver.calls, [])
        self.protocol.dataReceived('ab')
        self.assertEqual(self.protocol.receiver.calls, ['a'])
        self.protocol.dataReceived('baa')
        self.assertEqual(self.protocol.receiver.calls, ['a', 'b', 'a'])

    def test_rulesCannotBeSwitchedDuringParsing(self):
        """
        One can set a new rule during parsing, but it won't change the rule
        currently being parsed.
        """
        self.protocol.makeConnection(None)
        self.protocol.dataReceived('aa')
        self.assertEqual(self.protocol.receiver.calls, ['a'])
        self.protocol.dataReceived('a')
        self.assertEqual(self.protocol.receiver.calls, ['a'])
        self.protocol.receiver.currentRule = 'someC'
        self.protocol.dataReceived('acc')
        self.assertEqual(self.protocol.receiver.calls, ['a', 'a', 'c'])

    def test_connectionLoss(self):
        """The reason for connection loss is forwarded to the receiver."""
        self.protocol.makeConnection(None)
        reason = object()
        self.protocol.connectionLost(reason)
        self.assertEqual(self.protocol.receiver.lossReason, reason)

    def test_parseFailure(self):
        """
        Parse failures cause connection abortion with the parse error as the
        reason.
        """
        transport = FakeTransport()
        self.protocol.makeConnection(transport)
        self.protocol.dataReceived('b')
        self.failIfEqual(self.protocol.receiver.lossReason, None)
        self.assertTrue(
            isinstance(self.protocol.receiver.lossReason.value, ParseError))
        self.assert_(transport.aborted)

    def test_exceptionsRaisedFromReceiver(self):
        """
        Raising an exception from receiver methods called from the grammar
        propagate to finishParsing.
        """
        transport = FakeTransport()
        self.protocol.makeConnection(transport)
        self.protocol.dataReceived('e')
        self.failIfEqual(self.protocol.receiver.lossReason, None)
        self.assertTrue(
            isinstance(self.protocol.receiver.lossReason.value, SomeException))
        self.assert_(transport.aborted)

    def test_dataIgnoredAfterDisconnection(self):
        """After connectionLost is called, all incoming data is ignored."""
        transport = FakeTransport()
        self.protocol.makeConnection(transport)
        reason = object()
        self.protocol.connectionLost(reason)
        self.protocol.dataReceived('d')
        self.assertEqual(self.protocol.receiver.lossReason, reason)
        self.assert_(not transport.aborted)
