# Generated from /tmp/build-via-sdist-3q5kb_md/omegaconf-2.4.0.dev3/omegaconf/grammar/OmegaConfGrammarParser.g4 by ANTLR 4.11.1
# encoding: utf-8
from omegaconf.vendor.antlr4 import *
from io import StringIO
import sys
if sys.version_info[1] > 5:
	from typing import TextIO
else:
	from typing.io import TextIO

def serializedATN():
    return [
        4,1,28,181,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,
        6,2,7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,
        2,14,7,14,2,15,7,15,1,0,1,0,1,0,1,1,1,1,1,1,1,2,1,2,1,2,1,2,1,2,
        1,2,4,2,45,8,2,11,2,12,2,46,1,3,1,3,1,3,1,3,3,3,53,8,3,1,4,1,4,3,
        4,57,8,4,1,4,1,4,1,5,1,5,1,5,1,5,5,5,65,8,5,10,5,12,5,68,9,5,3,5,
        70,8,5,1,5,1,5,1,6,1,6,1,6,1,6,1,7,1,7,1,7,3,7,81,8,7,5,7,83,8,7,
        10,7,12,7,86,9,7,1,7,1,7,3,7,90,8,7,4,7,92,8,7,11,7,12,7,93,3,7,
        96,8,7,1,8,1,8,3,8,100,8,8,1,9,1,9,5,9,104,8,9,10,9,12,9,107,9,9,
        1,9,1,9,1,9,1,9,1,9,3,9,114,8,9,1,9,1,9,1,9,1,9,1,9,1,9,5,9,122,
        8,9,10,9,12,9,125,9,9,1,9,1,9,1,10,1,10,1,10,1,10,3,10,133,8,10,
        1,10,1,10,1,11,1,11,1,11,3,11,140,8,11,1,12,1,12,3,12,144,8,12,1,
        12,1,12,1,12,3,12,149,8,12,5,12,151,8,12,10,12,12,12,154,9,12,1,
        13,1,13,3,13,158,8,13,1,13,1,13,1,14,1,14,1,14,1,14,1,14,1,14,1,
        14,1,14,1,14,1,14,4,14,172,8,14,11,14,12,14,173,1,15,4,15,177,8,
        15,11,15,12,15,178,1,15,0,0,16,0,2,4,6,8,10,12,14,16,18,20,22,24,
        26,28,30,0,2,1,0,7,8,1,0,13,20,204,0,32,1,0,0,0,2,35,1,0,0,0,4,44,
        1,0,0,0,6,52,1,0,0,0,8,54,1,0,0,0,10,60,1,0,0,0,12,73,1,0,0,0,14,
        95,1,0,0,0,16,99,1,0,0,0,18,101,1,0,0,0,20,128,1,0,0,0,22,139,1,
        0,0,0,24,143,1,0,0,0,26,155,1,0,0,0,28,171,1,0,0,0,30,176,1,0,0,
        0,32,33,3,4,2,0,33,34,5,0,0,1,34,1,1,0,0,0,35,36,3,6,3,0,36,37,5,
        0,0,1,37,3,1,0,0,0,38,45,3,16,8,0,39,45,5,1,0,0,40,45,5,19,0,0,41,
        45,5,2,0,0,42,45,5,3,0,0,43,45,5,25,0,0,44,38,1,0,0,0,44,39,1,0,
        0,0,44,40,1,0,0,0,44,41,1,0,0,0,44,42,1,0,0,0,44,43,1,0,0,0,45,46,
        1,0,0,0,46,44,1,0,0,0,46,47,1,0,0,0,47,5,1,0,0,0,48,53,3,28,14,0,
        49,53,3,26,13,0,50,53,3,8,4,0,51,53,3,10,5,0,52,48,1,0,0,0,52,49,
        1,0,0,0,52,50,1,0,0,0,52,51,1,0,0,0,53,7,1,0,0,0,54,56,5,10,0,0,
        55,57,3,14,7,0,56,55,1,0,0,0,56,57,1,0,0,0,57,58,1,0,0,0,58,59,5,
        11,0,0,59,9,1,0,0,0,60,69,5,5,0,0,61,66,3,12,6,0,62,63,5,9,0,0,63,
        65,3,12,6,0,64,62,1,0,0,0,65,68,1,0,0,0,66,64,1,0,0,0,66,67,1,0,
        0,0,67,70,1,0,0,0,68,66,1,0,0,0,69,61,1,0,0,0,69,70,1,0,0,0,70,71,
        1,0,0,0,71,72,5,6,0,0,72,11,1,0,0,0,73,74,3,30,15,0,74,75,5,12,0,
        0,75,76,3,6,3,0,76,13,1,0,0,0,77,84,3,6,3,0,78,80,5,9,0,0,79,81,
        3,6,3,0,80,79,1,0,0,0,80,81,1,0,0,0,81,83,1,0,0,0,82,78,1,0,0,0,
        83,86,1,0,0,0,84,82,1,0,0,0,84,85,1,0,0,0,85,96,1,0,0,0,86,84,1,
        0,0,0,87,89,5,9,0,0,88,90,3,6,3,0,89,88,1,0,0,0,89,90,1,0,0,0,90,
        92,1,0,0,0,91,87,1,0,0,0,92,93,1,0,0,0,93,91,1,0,0,0,93,94,1,0,0,
        0,94,96,1,0,0,0,95,77,1,0,0,0,95,91,1,0,0,0,96,15,1,0,0,0,97,100,
        3,18,9,0,98,100,3,20,10,0,99,97,1,0,0,0,99,98,1,0,0,0,100,17,1,0,
        0,0,101,105,5,4,0,0,102,104,5,22,0,0,103,102,1,0,0,0,104,107,1,0,
        0,0,105,103,1,0,0,0,105,106,1,0,0,0,106,113,1,0,0,0,107,105,1,0,
        0,0,108,114,3,22,11,0,109,110,5,10,0,0,110,111,3,22,11,0,111,112,
        5,11,0,0,112,114,1,0,0,0,113,108,1,0,0,0,113,109,1,0,0,0,114,123,
        1,0,0,0,115,116,5,22,0,0,116,122,3,22,11,0,117,118,5,10,0,0,118,
        119,3,22,11,0,119,120,5,11,0,0,120,122,1,0,0,0,121,115,1,0,0,0,121,
        117,1,0,0,0,122,125,1,0,0,0,123,121,1,0,0,0,123,124,1,0,0,0,124,
        126,1,0,0,0,125,123,1,0,0,0,126,127,5,21,0,0,127,19,1,0,0,0,128,
        129,5,4,0,0,129,130,3,24,12,0,130,132,5,12,0,0,131,133,3,14,7,0,
        132,131,1,0,0,0,132,133,1,0,0,0,133,134,1,0,0,0,134,135,5,6,0,0,
        135,21,1,0,0,0,136,140,3,16,8,0,137,140,5,18,0,0,138,140,5,23,0,
        0,139,136,1,0,0,0,139,137,1,0,0,0,139,138,1,0,0,0,140,23,1,0,0,0,
        141,144,3,16,8,0,142,144,5,18,0,0,143,141,1,0,0,0,143,142,1,0,0,
        0,144,152,1,0,0,0,145,148,5,22,0,0,146,149,3,16,8,0,147,149,5,18,
        0,0,148,146,1,0,0,0,148,147,1,0,0,0,149,151,1,0,0,0,150,145,1,0,
        0,0,151,154,1,0,0,0,152,150,1,0,0,0,152,153,1,0,0,0,153,25,1,0,0,
        0,154,152,1,0,0,0,155,157,7,0,0,0,156,158,3,4,2,0,157,156,1,0,0,
        0,157,158,1,0,0,0,158,159,1,0,0,0,159,160,5,24,0,0,160,27,1,0,0,
        0,161,172,5,18,0,0,162,172,5,16,0,0,163,172,5,14,0,0,164,172,5,13,
        0,0,165,172,5,15,0,0,166,172,5,17,0,0,167,172,5,12,0,0,168,172,5,
        19,0,0,169,172,5,20,0,0,170,172,3,16,8,0,171,161,1,0,0,0,171,162,
        1,0,0,0,171,163,1,0,0,0,171,164,1,0,0,0,171,165,1,0,0,0,171,166,
        1,0,0,0,171,167,1,0,0,0,171,168,1,0,0,0,171,169,1,0,0,0,171,170,
        1,0,0,0,172,173,1,0,0,0,173,171,1,0,0,0,173,174,1,0,0,0,174,29,1,
        0,0,0,175,177,7,1,0,0,176,175,1,0,0,0,177,178,1,0,0,0,178,176,1,
        0,0,0,178,179,1,0,0,0,179,31,1,0,0,0,25,44,46,52,56,66,69,80,84,
        89,93,95,99,105,113,121,123,132,139,143,148,152,157,171,173,178
    ]

class OmegaConfGrammarParser ( Parser ):

    grammarFileName = "OmegaConfGrammarParser.g4"

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [ DFA(ds, i) for i, ds in enumerate(atn.decisionToState) ]

    sharedContextCache = PredictionContextCache()

    literalNames = [ "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>", 
                     "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>", 
                     "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>", 
                     "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>", 
                     "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>", 
                     "<INVALID>", "<INVALID>", "'.'", "<INVALID>", "<INVALID>", 
                     "<INVALID>", "<INVALID>", "'['", "']'" ]

    symbolicNames = [ "<INVALID>", "ANY_STR", "ESC_INTER", "TOP_ESC", "INTER_OPEN", 
                      "BRACE_OPEN", "BRACE_CLOSE", "QUOTE_OPEN_SINGLE", 
                      "QUOTE_OPEN_DOUBLE", "COMMA", "BRACKET_OPEN", "BRACKET_CLOSE", 
                      "COLON", "FLOAT", "INT", "BOOL", "NULL", "UNQUOTED_CHAR", 
                      "ID", "ESC", "WS", "INTER_CLOSE", "DOT", "INTER_KEY", 
                      "MATCHING_QUOTE_CLOSE", "QUOTED_ESC", "DOLLAR", "INTER_BRACKET_OPEN", 
                      "INTER_BRACKET_CLOSE" ]

    RULE_configValue = 0
    RULE_singleElement = 1
    RULE_text = 2
    RULE_element = 3
    RULE_listContainer = 4
    RULE_dictContainer = 5
    RULE_dictKeyValuePair = 6
    RULE_sequence = 7
    RULE_interpolation = 8
    RULE_interpolationNode = 9
    RULE_interpolationResolver = 10
    RULE_configKey = 11
    RULE_resolverName = 12
    RULE_quotedValue = 13
    RULE_primitive = 14
    RULE_dictKey = 15

    ruleNames =  [ "configValue", "singleElement", "text", "element", "listContainer", 
                   "dictContainer", "dictKeyValuePair", "sequence", "interpolation", 
                   "interpolationNode", "interpolationResolver", "configKey", 
                   "resolverName", "quotedValue", "primitive", "dictKey" ]

    EOF = Token.EOF
    ANY_STR=1
    ESC_INTER=2
    TOP_ESC=3
    INTER_OPEN=4
    BRACE_OPEN=5
    BRACE_CLOSE=6
    QUOTE_OPEN_SINGLE=7
    QUOTE_OPEN_DOUBLE=8
    COMMA=9
    BRACKET_OPEN=10
    BRACKET_CLOSE=11
    COLON=12
    FLOAT=13
    INT=14
    BOOL=15
    NULL=16
    UNQUOTED_CHAR=17
    ID=18
    ESC=19
    WS=20
    INTER_CLOSE=21
    DOT=22
    INTER_KEY=23
    MATCHING_QUOTE_CLOSE=24
    QUOTED_ESC=25
    DOLLAR=26
    INTER_BRACKET_OPEN=27
    INTER_BRACKET_CLOSE=28

    def __init__(self, input:TokenStream, output:TextIO = sys.stdout):
        super().__init__(input, output)
        self.checkVersion("4.11.1")
        self._interp = ParserATNSimulator(self, self.atn, self.decisionsToDFA, self.sharedContextCache)
        self._predicates = None




    class ConfigValueContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def text(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.TextContext,0)


        def EOF(self):
            return self.getToken(OmegaConfGrammarParser.EOF, 0)

        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_configValue

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterConfigValue" ):
                listener.enterConfigValue(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitConfigValue" ):
                listener.exitConfigValue(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitConfigValue" ):
                return visitor.visitConfigValue(self)
            else:
                return visitor.visitChildren(self)




    def configValue(self):

        localctx = OmegaConfGrammarParser.ConfigValueContext(self, self._ctx, self.state)
        self.enterRule(localctx, 0, self.RULE_configValue)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 32
            self.text()
            self.state = 33
            self.match(OmegaConfGrammarParser.EOF)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class SingleElementContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def element(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.ElementContext,0)


        def EOF(self):
            return self.getToken(OmegaConfGrammarParser.EOF, 0)

        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_singleElement

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSingleElement" ):
                listener.enterSingleElement(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSingleElement" ):
                listener.exitSingleElement(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitSingleElement" ):
                return visitor.visitSingleElement(self)
            else:
                return visitor.visitChildren(self)




    def singleElement(self):

        localctx = OmegaConfGrammarParser.SingleElementContext(self, self._ctx, self.state)
        self.enterRule(localctx, 2, self.RULE_singleElement)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 35
            self.element()
            self.state = 36
            self.match(OmegaConfGrammarParser.EOF)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class TextContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def interpolation(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(OmegaConfGrammarParser.InterpolationContext)
            else:
                return self.getTypedRuleContext(OmegaConfGrammarParser.InterpolationContext,i)


        def ANY_STR(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.ANY_STR)
            else:
                return self.getToken(OmegaConfGrammarParser.ANY_STR, i)

        def ESC(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.ESC)
            else:
                return self.getToken(OmegaConfGrammarParser.ESC, i)

        def ESC_INTER(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.ESC_INTER)
            else:
                return self.getToken(OmegaConfGrammarParser.ESC_INTER, i)

        def TOP_ESC(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.TOP_ESC)
            else:
                return self.getToken(OmegaConfGrammarParser.TOP_ESC, i)

        def QUOTED_ESC(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.QUOTED_ESC)
            else:
                return self.getToken(OmegaConfGrammarParser.QUOTED_ESC, i)

        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_text

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterText" ):
                listener.enterText(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitText" ):
                listener.exitText(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitText" ):
                return visitor.visitText(self)
            else:
                return visitor.visitChildren(self)




    def text(self):

        localctx = OmegaConfGrammarParser.TextContext(self, self._ctx, self.state)
        self.enterRule(localctx, 4, self.RULE_text)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 44 
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while True:
                self.state = 44
                self._errHandler.sync(self)
                token = self._input.LA(1)
                if token in [4]:
                    self.state = 38
                    self.interpolation()
                    pass
                elif token in [1]:
                    self.state = 39
                    self.match(OmegaConfGrammarParser.ANY_STR)
                    pass
                elif token in [19]:
                    self.state = 40
                    self.match(OmegaConfGrammarParser.ESC)
                    pass
                elif token in [2]:
                    self.state = 41
                    self.match(OmegaConfGrammarParser.ESC_INTER)
                    pass
                elif token in [3]:
                    self.state = 42
                    self.match(OmegaConfGrammarParser.TOP_ESC)
                    pass
                elif token in [25]:
                    self.state = 43
                    self.match(OmegaConfGrammarParser.QUOTED_ESC)
                    pass
                else:
                    raise NoViableAltException(self)

                self.state = 46 
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if not (((_la) & ~0x3f) == 0 and ((1 << _la) & 34078750) != 0):
                    break

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class ElementContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def primitive(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.PrimitiveContext,0)


        def quotedValue(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.QuotedValueContext,0)


        def listContainer(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.ListContainerContext,0)


        def dictContainer(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.DictContainerContext,0)


        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_element

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterElement" ):
                listener.enterElement(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitElement" ):
                listener.exitElement(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitElement" ):
                return visitor.visitElement(self)
            else:
                return visitor.visitChildren(self)




    def element(self):

        localctx = OmegaConfGrammarParser.ElementContext(self, self._ctx, self.state)
        self.enterRule(localctx, 6, self.RULE_element)
        try:
            self.state = 52
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [4, 12, 13, 14, 15, 16, 17, 18, 19, 20]:
                self.enterOuterAlt(localctx, 1)
                self.state = 48
                self.primitive()
                pass
            elif token in [7, 8]:
                self.enterOuterAlt(localctx, 2)
                self.state = 49
                self.quotedValue()
                pass
            elif token in [10]:
                self.enterOuterAlt(localctx, 3)
                self.state = 50
                self.listContainer()
                pass
            elif token in [5]:
                self.enterOuterAlt(localctx, 4)
                self.state = 51
                self.dictContainer()
                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class ListContainerContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def BRACKET_OPEN(self):
            return self.getToken(OmegaConfGrammarParser.BRACKET_OPEN, 0)

        def BRACKET_CLOSE(self):
            return self.getToken(OmegaConfGrammarParser.BRACKET_CLOSE, 0)

        def sequence(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.SequenceContext,0)


        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_listContainer

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterListContainer" ):
                listener.enterListContainer(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitListContainer" ):
                listener.exitListContainer(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitListContainer" ):
                return visitor.visitListContainer(self)
            else:
                return visitor.visitChildren(self)




    def listContainer(self):

        localctx = OmegaConfGrammarParser.ListContainerContext(self, self._ctx, self.state)
        self.enterRule(localctx, 8, self.RULE_listContainer)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 54
            self.match(OmegaConfGrammarParser.BRACKET_OPEN)
            self.state = 56
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if ((_la) & ~0x3f) == 0 and ((1 << _la) & 2095024) != 0:
                self.state = 55
                self.sequence()


            self.state = 58
            self.match(OmegaConfGrammarParser.BRACKET_CLOSE)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class DictContainerContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def BRACE_OPEN(self):
            return self.getToken(OmegaConfGrammarParser.BRACE_OPEN, 0)

        def BRACE_CLOSE(self):
            return self.getToken(OmegaConfGrammarParser.BRACE_CLOSE, 0)

        def dictKeyValuePair(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(OmegaConfGrammarParser.DictKeyValuePairContext)
            else:
                return self.getTypedRuleContext(OmegaConfGrammarParser.DictKeyValuePairContext,i)


        def COMMA(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.COMMA)
            else:
                return self.getToken(OmegaConfGrammarParser.COMMA, i)

        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_dictContainer

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDictContainer" ):
                listener.enterDictContainer(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDictContainer" ):
                listener.exitDictContainer(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitDictContainer" ):
                return visitor.visitDictContainer(self)
            else:
                return visitor.visitChildren(self)




    def dictContainer(self):

        localctx = OmegaConfGrammarParser.DictContainerContext(self, self._ctx, self.state)
        self.enterRule(localctx, 10, self.RULE_dictContainer)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 60
            self.match(OmegaConfGrammarParser.BRACE_OPEN)
            self.state = 69
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if ((_la) & ~0x3f) == 0 and ((1 << _la) & 2088960) != 0:
                self.state = 61
                self.dictKeyValuePair()
                self.state = 66
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==9:
                    self.state = 62
                    self.match(OmegaConfGrammarParser.COMMA)
                    self.state = 63
                    self.dictKeyValuePair()
                    self.state = 68
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)



            self.state = 71
            self.match(OmegaConfGrammarParser.BRACE_CLOSE)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class DictKeyValuePairContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def dictKey(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.DictKeyContext,0)


        def COLON(self):
            return self.getToken(OmegaConfGrammarParser.COLON, 0)

        def element(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.ElementContext,0)


        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_dictKeyValuePair

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDictKeyValuePair" ):
                listener.enterDictKeyValuePair(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDictKeyValuePair" ):
                listener.exitDictKeyValuePair(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitDictKeyValuePair" ):
                return visitor.visitDictKeyValuePair(self)
            else:
                return visitor.visitChildren(self)




    def dictKeyValuePair(self):

        localctx = OmegaConfGrammarParser.DictKeyValuePairContext(self, self._ctx, self.state)
        self.enterRule(localctx, 12, self.RULE_dictKeyValuePair)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 73
            self.dictKey()
            self.state = 74
            self.match(OmegaConfGrammarParser.COLON)
            self.state = 75
            self.element()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class SequenceContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def element(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(OmegaConfGrammarParser.ElementContext)
            else:
                return self.getTypedRuleContext(OmegaConfGrammarParser.ElementContext,i)


        def COMMA(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.COMMA)
            else:
                return self.getToken(OmegaConfGrammarParser.COMMA, i)

        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_sequence

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSequence" ):
                listener.enterSequence(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSequence" ):
                listener.exitSequence(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitSequence" ):
                return visitor.visitSequence(self)
            else:
                return visitor.visitChildren(self)




    def sequence(self):

        localctx = OmegaConfGrammarParser.SequenceContext(self, self._ctx, self.state)
        self.enterRule(localctx, 14, self.RULE_sequence)
        self._la = 0 # Token type
        try:
            self.state = 95
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [4, 5, 7, 8, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20]:
                self.enterOuterAlt(localctx, 1)
                self.state = 77
                self.element()
                self.state = 84
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==9:
                    self.state = 78
                    self.match(OmegaConfGrammarParser.COMMA)
                    self.state = 80
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if ((_la) & ~0x3f) == 0 and ((1 << _la) & 2094512) != 0:
                        self.state = 79
                        self.element()


                    self.state = 86
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

                pass
            elif token in [9]:
                self.enterOuterAlt(localctx, 2)
                self.state = 91 
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while True:
                    self.state = 87
                    self.match(OmegaConfGrammarParser.COMMA)
                    self.state = 89
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if ((_la) & ~0x3f) == 0 and ((1 << _la) & 2094512) != 0:
                        self.state = 88
                        self.element()


                    self.state = 93 
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    if not (_la==9):
                        break

                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class InterpolationContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def interpolationNode(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.InterpolationNodeContext,0)


        def interpolationResolver(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.InterpolationResolverContext,0)


        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_interpolation

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterInterpolation" ):
                listener.enterInterpolation(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitInterpolation" ):
                listener.exitInterpolation(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitInterpolation" ):
                return visitor.visitInterpolation(self)
            else:
                return visitor.visitChildren(self)




    def interpolation(self):

        localctx = OmegaConfGrammarParser.InterpolationContext(self, self._ctx, self.state)
        self.enterRule(localctx, 16, self.RULE_interpolation)
        try:
            self.state = 99
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,11,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 97
                self.interpolationNode()
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 98
                self.interpolationResolver()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class InterpolationNodeContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def INTER_OPEN(self):
            return self.getToken(OmegaConfGrammarParser.INTER_OPEN, 0)

        def INTER_CLOSE(self):
            return self.getToken(OmegaConfGrammarParser.INTER_CLOSE, 0)

        def configKey(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(OmegaConfGrammarParser.ConfigKeyContext)
            else:
                return self.getTypedRuleContext(OmegaConfGrammarParser.ConfigKeyContext,i)


        def BRACKET_OPEN(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.BRACKET_OPEN)
            else:
                return self.getToken(OmegaConfGrammarParser.BRACKET_OPEN, i)

        def BRACKET_CLOSE(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.BRACKET_CLOSE)
            else:
                return self.getToken(OmegaConfGrammarParser.BRACKET_CLOSE, i)

        def DOT(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.DOT)
            else:
                return self.getToken(OmegaConfGrammarParser.DOT, i)

        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_interpolationNode

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterInterpolationNode" ):
                listener.enterInterpolationNode(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitInterpolationNode" ):
                listener.exitInterpolationNode(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitInterpolationNode" ):
                return visitor.visitInterpolationNode(self)
            else:
                return visitor.visitChildren(self)




    def interpolationNode(self):

        localctx = OmegaConfGrammarParser.InterpolationNodeContext(self, self._ctx, self.state)
        self.enterRule(localctx, 18, self.RULE_interpolationNode)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 101
            self.match(OmegaConfGrammarParser.INTER_OPEN)
            self.state = 105
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==22:
                self.state = 102
                self.match(OmegaConfGrammarParser.DOT)
                self.state = 107
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 113
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [4, 18, 23]:
                self.state = 108
                self.configKey()
                pass
            elif token in [10]:
                self.state = 109
                self.match(OmegaConfGrammarParser.BRACKET_OPEN)
                self.state = 110
                self.configKey()
                self.state = 111
                self.match(OmegaConfGrammarParser.BRACKET_CLOSE)
                pass
            else:
                raise NoViableAltException(self)

            self.state = 123
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==10 or _la==22:
                self.state = 121
                self._errHandler.sync(self)
                token = self._input.LA(1)
                if token in [22]:
                    self.state = 115
                    self.match(OmegaConfGrammarParser.DOT)
                    self.state = 116
                    self.configKey()
                    pass
                elif token in [10]:
                    self.state = 117
                    self.match(OmegaConfGrammarParser.BRACKET_OPEN)
                    self.state = 118
                    self.configKey()
                    self.state = 119
                    self.match(OmegaConfGrammarParser.BRACKET_CLOSE)
                    pass
                else:
                    raise NoViableAltException(self)

                self.state = 125
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 126
            self.match(OmegaConfGrammarParser.INTER_CLOSE)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class InterpolationResolverContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def INTER_OPEN(self):
            return self.getToken(OmegaConfGrammarParser.INTER_OPEN, 0)

        def resolverName(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.ResolverNameContext,0)


        def COLON(self):
            return self.getToken(OmegaConfGrammarParser.COLON, 0)

        def BRACE_CLOSE(self):
            return self.getToken(OmegaConfGrammarParser.BRACE_CLOSE, 0)

        def sequence(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.SequenceContext,0)


        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_interpolationResolver

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterInterpolationResolver" ):
                listener.enterInterpolationResolver(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitInterpolationResolver" ):
                listener.exitInterpolationResolver(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitInterpolationResolver" ):
                return visitor.visitInterpolationResolver(self)
            else:
                return visitor.visitChildren(self)




    def interpolationResolver(self):

        localctx = OmegaConfGrammarParser.InterpolationResolverContext(self, self._ctx, self.state)
        self.enterRule(localctx, 20, self.RULE_interpolationResolver)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 128
            self.match(OmegaConfGrammarParser.INTER_OPEN)
            self.state = 129
            self.resolverName()
            self.state = 130
            self.match(OmegaConfGrammarParser.COLON)
            self.state = 132
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if ((_la) & ~0x3f) == 0 and ((1 << _la) & 2095024) != 0:
                self.state = 131
                self.sequence()


            self.state = 134
            self.match(OmegaConfGrammarParser.BRACE_CLOSE)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class ConfigKeyContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def interpolation(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.InterpolationContext,0)


        def ID(self):
            return self.getToken(OmegaConfGrammarParser.ID, 0)

        def INTER_KEY(self):
            return self.getToken(OmegaConfGrammarParser.INTER_KEY, 0)

        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_configKey

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterConfigKey" ):
                listener.enterConfigKey(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitConfigKey" ):
                listener.exitConfigKey(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitConfigKey" ):
                return visitor.visitConfigKey(self)
            else:
                return visitor.visitChildren(self)




    def configKey(self):

        localctx = OmegaConfGrammarParser.ConfigKeyContext(self, self._ctx, self.state)
        self.enterRule(localctx, 22, self.RULE_configKey)
        try:
            self.state = 139
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [4]:
                self.enterOuterAlt(localctx, 1)
                self.state = 136
                self.interpolation()
                pass
            elif token in [18]:
                self.enterOuterAlt(localctx, 2)
                self.state = 137
                self.match(OmegaConfGrammarParser.ID)
                pass
            elif token in [23]:
                self.enterOuterAlt(localctx, 3)
                self.state = 138
                self.match(OmegaConfGrammarParser.INTER_KEY)
                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class ResolverNameContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def interpolation(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(OmegaConfGrammarParser.InterpolationContext)
            else:
                return self.getTypedRuleContext(OmegaConfGrammarParser.InterpolationContext,i)


        def ID(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.ID)
            else:
                return self.getToken(OmegaConfGrammarParser.ID, i)

        def DOT(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.DOT)
            else:
                return self.getToken(OmegaConfGrammarParser.DOT, i)

        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_resolverName

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterResolverName" ):
                listener.enterResolverName(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitResolverName" ):
                listener.exitResolverName(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitResolverName" ):
                return visitor.visitResolverName(self)
            else:
                return visitor.visitChildren(self)




    def resolverName(self):

        localctx = OmegaConfGrammarParser.ResolverNameContext(self, self._ctx, self.state)
        self.enterRule(localctx, 24, self.RULE_resolverName)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 143
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [4]:
                self.state = 141
                self.interpolation()
                pass
            elif token in [18]:
                self.state = 142
                self.match(OmegaConfGrammarParser.ID)
                pass
            else:
                raise NoViableAltException(self)

            self.state = 152
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==22:
                self.state = 145
                self.match(OmegaConfGrammarParser.DOT)
                self.state = 148
                self._errHandler.sync(self)
                token = self._input.LA(1)
                if token in [4]:
                    self.state = 146
                    self.interpolation()
                    pass
                elif token in [18]:
                    self.state = 147
                    self.match(OmegaConfGrammarParser.ID)
                    pass
                else:
                    raise NoViableAltException(self)

                self.state = 154
                self._errHandler.sync(self)
                _la = self._input.LA(1)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class QuotedValueContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def MATCHING_QUOTE_CLOSE(self):
            return self.getToken(OmegaConfGrammarParser.MATCHING_QUOTE_CLOSE, 0)

        def QUOTE_OPEN_SINGLE(self):
            return self.getToken(OmegaConfGrammarParser.QUOTE_OPEN_SINGLE, 0)

        def QUOTE_OPEN_DOUBLE(self):
            return self.getToken(OmegaConfGrammarParser.QUOTE_OPEN_DOUBLE, 0)

        def text(self):
            return self.getTypedRuleContext(OmegaConfGrammarParser.TextContext,0)


        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_quotedValue

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterQuotedValue" ):
                listener.enterQuotedValue(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitQuotedValue" ):
                listener.exitQuotedValue(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitQuotedValue" ):
                return visitor.visitQuotedValue(self)
            else:
                return visitor.visitChildren(self)




    def quotedValue(self):

        localctx = OmegaConfGrammarParser.QuotedValueContext(self, self._ctx, self.state)
        self.enterRule(localctx, 26, self.RULE_quotedValue)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 155
            _la = self._input.LA(1)
            if not(_la==7 or _la==8):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
            self.state = 157
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if ((_la) & ~0x3f) == 0 and ((1 << _la) & 34078750) != 0:
                self.state = 156
                self.text()


            self.state = 159
            self.match(OmegaConfGrammarParser.MATCHING_QUOTE_CLOSE)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class PrimitiveContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ID(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.ID)
            else:
                return self.getToken(OmegaConfGrammarParser.ID, i)

        def NULL(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.NULL)
            else:
                return self.getToken(OmegaConfGrammarParser.NULL, i)

        def INT(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.INT)
            else:
                return self.getToken(OmegaConfGrammarParser.INT, i)

        def FLOAT(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.FLOAT)
            else:
                return self.getToken(OmegaConfGrammarParser.FLOAT, i)

        def BOOL(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.BOOL)
            else:
                return self.getToken(OmegaConfGrammarParser.BOOL, i)

        def UNQUOTED_CHAR(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.UNQUOTED_CHAR)
            else:
                return self.getToken(OmegaConfGrammarParser.UNQUOTED_CHAR, i)

        def COLON(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.COLON)
            else:
                return self.getToken(OmegaConfGrammarParser.COLON, i)

        def ESC(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.ESC)
            else:
                return self.getToken(OmegaConfGrammarParser.ESC, i)

        def WS(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.WS)
            else:
                return self.getToken(OmegaConfGrammarParser.WS, i)

        def interpolation(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(OmegaConfGrammarParser.InterpolationContext)
            else:
                return self.getTypedRuleContext(OmegaConfGrammarParser.InterpolationContext,i)


        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_primitive

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPrimitive" ):
                listener.enterPrimitive(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPrimitive" ):
                listener.exitPrimitive(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitPrimitive" ):
                return visitor.visitPrimitive(self)
            else:
                return visitor.visitChildren(self)




    def primitive(self):

        localctx = OmegaConfGrammarParser.PrimitiveContext(self, self._ctx, self.state)
        self.enterRule(localctx, 28, self.RULE_primitive)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 171 
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while True:
                self.state = 171
                self._errHandler.sync(self)
                token = self._input.LA(1)
                if token in [18]:
                    self.state = 161
                    self.match(OmegaConfGrammarParser.ID)
                    pass
                elif token in [16]:
                    self.state = 162
                    self.match(OmegaConfGrammarParser.NULL)
                    pass
                elif token in [14]:
                    self.state = 163
                    self.match(OmegaConfGrammarParser.INT)
                    pass
                elif token in [13]:
                    self.state = 164
                    self.match(OmegaConfGrammarParser.FLOAT)
                    pass
                elif token in [15]:
                    self.state = 165
                    self.match(OmegaConfGrammarParser.BOOL)
                    pass
                elif token in [17]:
                    self.state = 166
                    self.match(OmegaConfGrammarParser.UNQUOTED_CHAR)
                    pass
                elif token in [12]:
                    self.state = 167
                    self.match(OmegaConfGrammarParser.COLON)
                    pass
                elif token in [19]:
                    self.state = 168
                    self.match(OmegaConfGrammarParser.ESC)
                    pass
                elif token in [20]:
                    self.state = 169
                    self.match(OmegaConfGrammarParser.WS)
                    pass
                elif token in [4]:
                    self.state = 170
                    self.interpolation()
                    pass
                else:
                    raise NoViableAltException(self)

                self.state = 173 
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if not (((_la) & ~0x3f) == 0 and ((1 << _la) & 2093072) != 0):
                    break

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class DictKeyContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ID(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.ID)
            else:
                return self.getToken(OmegaConfGrammarParser.ID, i)

        def NULL(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.NULL)
            else:
                return self.getToken(OmegaConfGrammarParser.NULL, i)

        def INT(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.INT)
            else:
                return self.getToken(OmegaConfGrammarParser.INT, i)

        def FLOAT(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.FLOAT)
            else:
                return self.getToken(OmegaConfGrammarParser.FLOAT, i)

        def BOOL(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.BOOL)
            else:
                return self.getToken(OmegaConfGrammarParser.BOOL, i)

        def UNQUOTED_CHAR(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.UNQUOTED_CHAR)
            else:
                return self.getToken(OmegaConfGrammarParser.UNQUOTED_CHAR, i)

        def ESC(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.ESC)
            else:
                return self.getToken(OmegaConfGrammarParser.ESC, i)

        def WS(self, i:int=None):
            if i is None:
                return self.getTokens(OmegaConfGrammarParser.WS)
            else:
                return self.getToken(OmegaConfGrammarParser.WS, i)

        def getRuleIndex(self):
            return OmegaConfGrammarParser.RULE_dictKey

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDictKey" ):
                listener.enterDictKey(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDictKey" ):
                listener.exitDictKey(self)

        def accept(self, visitor:ParseTreeVisitor):
            if hasattr( visitor, "visitDictKey" ):
                return visitor.visitDictKey(self)
            else:
                return visitor.visitChildren(self)




    def dictKey(self):

        localctx = OmegaConfGrammarParser.DictKeyContext(self, self._ctx, self.state)
        self.enterRule(localctx, 30, self.RULE_dictKey)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 176 
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while True:
                self.state = 175
                _la = self._input.LA(1)
                if not(((_la) & ~0x3f) == 0 and ((1 << _la) & 2088960) != 0):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 178 
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if not (((_la) & ~0x3f) == 0 and ((1 << _la) & 2088960) != 0):
                    break

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx





