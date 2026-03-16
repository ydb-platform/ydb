# Generated from /home/runner/work/OpenMetadata/OpenMetadata/openmetadata-spec/src/main/antlr4/org/openmetadata/schema/Fqn.g4 by ANTLR 4.9.2
from antlr4 import *
from io import StringIO
import sys
if sys.version_info[1] > 5:
    from typing import TextIO
else:
    from typing.io import TextIO



def serializedATN():
    with StringIO() as buf:
        buf.write("\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\7")
        buf.write(",\b\1\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\3\2\6\2")
        buf.write("\17\n\2\r\2\16\2\20\3\3\3\3\7\3\25\n\3\f\3\16\3\30\13")
        buf.write("\3\3\3\3\3\7\3\34\n\3\f\3\16\3\37\13\3\6\3!\n\3\r\3\16")
        buf.write("\3\"\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\2\2\7\3\3\5\4\7\5")
        buf.write("\t\6\13\7\3\2\3\4\2$$\60\60\2/\2\3\3\2\2\2\2\5\3\2\2\2")
        buf.write("\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\3\16\3\2\2\2\5\22")
        buf.write("\3\2\2\2\7&\3\2\2\2\t(\3\2\2\2\13*\3\2\2\2\r\17\5\t\5")
        buf.write("\2\16\r\3\2\2\2\17\20\3\2\2\2\20\16\3\2\2\2\20\21\3\2")
        buf.write("\2\2\21\4\3\2\2\2\22\26\5\7\4\2\23\25\5\t\5\2\24\23\3")
        buf.write("\2\2\2\25\30\3\2\2\2\26\24\3\2\2\2\26\27\3\2\2\2\27 \3")
        buf.write("\2\2\2\30\26\3\2\2\2\31\35\5\13\6\2\32\34\5\t\5\2\33\32")
        buf.write("\3\2\2\2\34\37\3\2\2\2\35\33\3\2\2\2\35\36\3\2\2\2\36")
        buf.write("!\3\2\2\2\37\35\3\2\2\2 \31\3\2\2\2!\"\3\2\2\2\" \3\2")
        buf.write("\2\2\"#\3\2\2\2#$\3\2\2\2$%\5\7\4\2%\6\3\2\2\2&\'\7$\2")
        buf.write("\2\'\b\3\2\2\2()\n\2\2\2)\n\3\2\2\2*+\7\60\2\2+\f\3\2")
        buf.write("\2\2\7\2\20\26\35\"\2")
        return buf.getvalue()


class FqnLexer(Lexer):

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [ DFA(ds, i) for i, ds in enumerate(atn.decisionToState) ]

    NAME = 1
    NAME_WITH_RESERVED = 2
    QUOTE = 3
    NON_RESERVED = 4
    RESERVED = 5

    channelNames = [ u"DEFAULT_TOKEN_CHANNEL", u"HIDDEN" ]

    modeNames = [ "DEFAULT_MODE" ]

    literalNames = [ "<INVALID>",
            "'\"'", "'.'" ]

    symbolicNames = [ "<INVALID>",
            "NAME", "NAME_WITH_RESERVED", "QUOTE", "NON_RESERVED", "RESERVED" ]

    ruleNames = [ "NAME", "NAME_WITH_RESERVED", "QUOTE", "NON_RESERVED", 
                  "RESERVED" ]

    grammarFileName = "Fqn.g4"

    def __init__(self, input=None, output:TextIO = sys.stdout):
        super().__init__(input, output)
        self.checkVersion("4.9.2")
        self._interp = LexerATNSimulator(self, self.atn, self.decisionsToDFA, PredictionContextCache())
        self._actions = None
        self._predicates = None


