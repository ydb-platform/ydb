import functools

from ometa.grammar import OMeta
from ometa.runtime import ParseError, EOFError, OMetaBase
from terml.parser import parseTerm as term
from terml.quasiterm import quasiterm

__version__ = '1.3'


def wrapGrammar(g, tracefunc=None):
    def makeParser(input):
        """
        Creates a parser for the given input, with methods for
        invoking each rule.

        :param input: The string you want to parse.
        """
        parser = g(input)
        if tracefunc:
            parser._trace = tracefunc
        return _GrammarWrapper(parser, input)
    makeParser._grammarClass = g
    return makeParser


def makeGrammar(source, bindings, name='Grammar', unwrap=False,
                extends=wrapGrammar(OMetaBase), tracefunc=None):
    """
    Create a class from a Parsley grammar.

    :param source: A grammar, as a string.
    :param bindings: A mapping of variable names to objects.
    :param name: Name used for the generated class.

    :param unwrap: If True, return a parser class suitable for
                   subclassing. If False, return a wrapper with the
                   friendly API.
    :param extends: The superclass for the generated parser class.

    :param tracefunc: A 3-arg function which takes a fragment of grammar
                      source, the start/end indexes in the grammar of this
                      fragment, and a position in the input. Invoked for
                      terminals and rule applications.
    """
    g = OMeta.makeGrammar(source, name).createParserClass(
        unwrapGrammar(extends), bindings)
    if unwrap:
        return g
    else:
        return wrapGrammar(g, tracefunc=tracefunc)


def unwrapGrammar(w):
    """
    Access the internal parser class for a Parsley grammar object.
    """
    return getattr(w, '_grammarClass', None) or w


class _GrammarWrapper(object):
    """
    A wrapper for Parsley grammar instances.

    To invoke a Parsley rule, invoke a method with that name -- this
    turns x(input).foo() calls into grammar.apply("foo") calls.
    """
    def __init__(self, grammar, input):
        self._grammar = grammar
        self._input = input
        #so pydoc doesn't get trapped in the __getattr__
        self.__name__ = _GrammarWrapper.__name__

    def __getattr__(self, name):
        """
        Return a function that will instantiate a grammar and invoke the named
        rule.
        :param name: Rule name.
        """
        def invokeRule(*args, **kwargs):
            """
            Invoke a Parsley rule. Passes any positional args to the rule.
            """
            try:
                ret, err = self._grammar.apply(name, *args)
            except ParseError as e:
                self._grammar.considerError(e)
                err = self._grammar.currentError
            else:
                try:
                    extra, _ = self._grammar.input.head()
                except EOFError:
                    return ret
                else:
                    # problem is that input remains, so:
                    err = ParseError(err.input, err.position + 1,
                                     [["message", "expected EOF"]], err.trail)
            raise err
        return invokeRule


def makeProtocol(source, senderFactory, receiverFactory, bindings=None,
                 name='Grammar'):
    """
    Create a Twisted ``Protocol`` factory from a Parsley grammar.

    :param source: A grammar, as a string.
    :param senderFactory: A one-argument callable that takes a twisted
        ``Transport`` and returns a :ref:`sender <senders>`.
    :param receiverFactory: A one-argument callable that takes the sender
        returned by the ``senderFactory`` and returns a :ref:`receiver
        <receivers>`.
    :param bindings: A mapping of variable names to objects which will be
        accessible from python code in the grammar.
    :param name: The name used for the generated grammar class.
    :returns: A nullary callable which will return an instance of
        :class:`~.ParserProtocol`.
    """

    from ometa.protocol import ParserProtocol
    if bindings is None:
        bindings = {}
    grammar = OMeta(source).parseGrammar(name)
    return functools.partial(
        ParserProtocol, grammar, senderFactory, receiverFactory, bindings)


def stack(*wrappers):
    """
    Stack some senders or receivers for ease of wrapping.

    ``stack(x, y, z)`` will return a factory usable as a sender or receiver
    factory which will, when called with a transport or sender as an argument,
    return ``x(y(z(argument)))``.
    """
    if not wrappers:
        raise TypeError('at least one argument is required')
    def factory(arg):
        ret = wrappers[-1](arg)
        for wrapper in wrappers[-2::-1]:
            ret = wrapper(ret)
        return ret
    return factory

__all__ = [
    'makeGrammar', 'wrapGrammar', 'unwrapGrammar', 'term', 'quasiterm',
    'makeProtocol', 'stack',
]
