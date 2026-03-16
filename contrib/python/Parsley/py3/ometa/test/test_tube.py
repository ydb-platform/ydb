from __future__ import absolute_import, unicode_literals

import unittest

from ometa.grammar import OMeta
from ometa.tube import TrampolinedParser


def iterbytes(originalBytes):
    for i in range(len(originalBytes)):
        yield originalBytes[i:i+1]


class TrampolinedReceiver():
    """
    Receive and store the passed in data.
    """

    currentRule = 'initial'

    def __init__(self):
        self.received = []

    def receive(self, data):
        self.received.append(data)


class TrampolinedParserTestCase(unittest.TestCase):
    """
    Tests for L{ometa.tube.TrampolinedParser}
    """

    def _parseGrammar(self, grammar, name="Grammar"):
        return OMeta(grammar).parseGrammar(name)

    def setUp(self):
        _grammar =  r"""
            delimiter = '\r\n'
            initial = <(~delimiter anything)*>:val delimiter -> receiver.receive(val)
            witharg :arg1 :arg2 = <(~delimiter anything)*>:a delimiter -> receiver.receive(arg1+arg2+a)
        """
        self.grammar = self._parseGrammar(_grammar)

    def test_dataNotFullyReceived(self):
        """
        Since the initial rule inside the grammar is not matched, the receiver
        shouldn't receive any byte.
        """
        receiver = TrampolinedReceiver()
        trampolinedParser = TrampolinedParser(self.grammar, receiver, {})
        buf = 'foobarandnotreachdelimiter'
        for c in iterbytes(buf):
            trampolinedParser.receive(c)
        self.assertEqual(receiver.received, [])


    def test_dataFullyReceived(self):
        """
        The receiver should receive the data according to the grammar.
        """
        receiver = TrampolinedReceiver()
        trampolinedParser = TrampolinedParser(self.grammar, receiver, {})
        buf = '\r\n'.join(('foo', 'bar', 'foo', 'bar'))
        for c in iterbytes(buf):
            trampolinedParser.receive(c)
        self.assertEqual(receiver.received, ['foo', 'bar', 'foo'])
        trampolinedParser.receive('\r\n')
        self.assertEqual(receiver.received, ['foo', 'bar', 'foo', 'bar'])


    def test_bindings(self):
        """
        The passed-in bindings should be accessible inside the grammar.
        """
        receiver = TrampolinedReceiver()
        grammar = r"""
            initial = digit:d (-> int(d)+SMALL_INT):val -> receiver.receive(val)
        """
        bindings = {'SMALL_INT': 3}
        TrampolinedParser(self._parseGrammar(grammar), receiver, bindings).receive('0')
        self.assertEqual(receiver.received, [3])


    def test_currentRuleWithArgs(self):
        """
        TrampolinedParser should be able to invoke curruent rule with args.
        """
        receiver = TrampolinedReceiver()
        receiver.currentRule = "witharg", "nice ", "day"
        trampolinedParser = TrampolinedParser(self.grammar, receiver, {})
        buf = ' oh yes\r\n'
        for c in iterbytes(buf):
            trampolinedParser.receive(c)
        self.assertEqual(receiver.received, ["nice day oh yes"])
