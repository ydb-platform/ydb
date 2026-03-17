# Generated from /home/runner/work/OpenMetadata/OpenMetadata/openmetadata-spec/src/main/antlr4/org/openmetadata/schema/JdbcUri.g4 by ANTLR 4.9.2
# encoding: utf-8
from antlr4 import *
from io import StringIO
import sys
if sys.version_info[1] > 5:
	from typing import TextIO
else:
	from typing.io import TextIO


def serializedATN():
    with StringIO() as buf:
        buf.write("\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\22")
        buf.write("@\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\3\2")
        buf.write("\3\2\3\2\5\2\22\n\2\3\2\5\2\25\n\2\3\2\3\2\5\2\31\n\2")
        buf.write("\3\2\3\2\3\2\3\2\7\2\37\n\2\f\2\16\2\"\13\2\5\2$\n\2\3")
        buf.write("\2\5\2\'\n\2\3\3\3\3\3\3\3\3\5\3-\n\3\3\3\3\3\3\4\5\4")
        buf.write("\62\n\4\3\5\3\5\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\5\7>\n")
        buf.write("\7\3\7\2\2\b\2\4\6\b\n\f\2\2\2E\2\16\3\2\2\2\4(\3\2\2")
        buf.write("\2\6\61\3\2\2\2\b\63\3\2\2\2\n\65\3\2\2\2\f=\3\2\2\2\16")
        buf.write("\17\7\3\2\2\17\21\7\5\2\2\20\22\5\f\7\2\21\20\3\2\2\2")
        buf.write("\21\22\3\2\2\2\22\24\3\2\2\2\23\25\7\7\2\2\24\23\3\2\2")
        buf.write("\2\24\25\3\2\2\2\25\30\3\2\2\2\26\27\7\4\2\2\27\31\5\6")
        buf.write("\4\2\30\26\3\2\2\2\30\31\3\2\2\2\31#\3\2\2\2\32\33\7\16")
        buf.write("\2\2\33 \7\r\2\2\34\35\7\21\2\2\35\37\7\r\2\2\36\34\3")
        buf.write("\2\2\2\37\"\3\2\2\2 \36\3\2\2\2 !\3\2\2\2!$\3\2\2\2\"")
        buf.write(" \3\2\2\2#\32\3\2\2\2#$\3\2\2\2$&\3\2\2\2%\'\5\4\3\2&")
        buf.write("%\3\2\2\2&\'\3\2\2\2\'\3\3\2\2\2(,\7\20\2\2)*\5\b\5\2")
        buf.write("*+\7\17\2\2+-\3\2\2\2,)\3\2\2\2,-\3\2\2\2-.\3\2\2\2./")
        buf.write("\5\n\6\2/\5\3\2\2\2\60\62\7\b\2\2\61\60\3\2\2\2\61\62")
        buf.write("\3\2\2\2\62\7\3\2\2\2\63\64\7\b\2\2\64\t\3\2\2\2\65\66")
        buf.write("\7\b\2\2\66\13\3\2\2\2\67>\7\t\2\28>\7\n\2\29>\7\13\2")
        buf.write("\2:;\7\6\2\2;>\7\b\2\2<>\7\6\2\2=\67\3\2\2\2=8\3\2\2\2")
        buf.write("=9\3\2\2\2=:\3\2\2\2=<\3\2\2\2>\r\3\2\2\2\13\21\24\30")
        buf.write(" #&,\61=")
        return buf.getvalue()


class JdbcUriParser ( Parser ):

    grammarFileName = "JdbcUri.g4"

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [ DFA(ds, i) for i, ds in enumerate(atn.decisionToState) ]

    sharedContextCache = PredictionContextCache()

    literalNames = [ "<INVALID>", "'jdbc:'", "'/'", "<INVALID>", "<INVALID>", 
                     "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>", 
                     "<INVALID>", "<INVALID>", "<INVALID>", "'?'", "'.'", 
                     "':'", "'&'" ]

    symbolicNames = [ "<INVALID>", "<INVALID>", "<INVALID>", "DATABASE_TYPE", 
                      "URI_SEPARATOR", "PORT_NUMBER", "IDENTIFIER", "HOST_NAME", 
                      "IPV4_ADDRESS", "IPV6_ADDRESS", "HEXDIGIT", "CONNECTION_ARG", 
                      "CONNECTION_ARG_INIT", "PERIOD", "COLON", "AMP", "WS" ]

    RULE_jdbcUrl = 0
    RULE_schemaTable = 1
    RULE_databaseName = 2
    RULE_schemaName = 3
    RULE_tableName = 4
    RULE_serverName = 5

    ruleNames =  [ "jdbcUrl", "schemaTable", "databaseName", "schemaName", 
                   "tableName", "serverName" ]

    EOF = Token.EOF
    T__0=1
    T__1=2
    DATABASE_TYPE=3
    URI_SEPARATOR=4
    PORT_NUMBER=5
    IDENTIFIER=6
    HOST_NAME=7
    IPV4_ADDRESS=8
    IPV6_ADDRESS=9
    HEXDIGIT=10
    CONNECTION_ARG=11
    CONNECTION_ARG_INIT=12
    PERIOD=13
    COLON=14
    AMP=15
    WS=16

    def __init__(self, input:TokenStream, output:TextIO = sys.stdout):
        super().__init__(input, output)
        self.checkVersion("4.9.2")
        self._interp = ParserATNSimulator(self, self.atn, self.decisionsToDFA, self.sharedContextCache)
        self._predicates = None




    class JdbcUrlContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def DATABASE_TYPE(self):
            return self.getToken(JdbcUriParser.DATABASE_TYPE, 0)

        def serverName(self):
            return self.getTypedRuleContext(JdbcUriParser.ServerNameContext,0)


        def PORT_NUMBER(self):
            return self.getToken(JdbcUriParser.PORT_NUMBER, 0)

        def databaseName(self):
            return self.getTypedRuleContext(JdbcUriParser.DatabaseNameContext,0)


        def CONNECTION_ARG_INIT(self):
            return self.getToken(JdbcUriParser.CONNECTION_ARG_INIT, 0)

        def CONNECTION_ARG(self, i:int=None):
            if i is None:
                return self.getTokens(JdbcUriParser.CONNECTION_ARG)
            else:
                return self.getToken(JdbcUriParser.CONNECTION_ARG, i)

        def schemaTable(self):
            return self.getTypedRuleContext(JdbcUriParser.SchemaTableContext,0)


        def AMP(self, i:int=None):
            if i is None:
                return self.getTokens(JdbcUriParser.AMP)
            else:
                return self.getToken(JdbcUriParser.AMP, i)

        def getRuleIndex(self):
            return JdbcUriParser.RULE_jdbcUrl

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterJdbcUrl" ):
                listener.enterJdbcUrl(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitJdbcUrl" ):
                listener.exitJdbcUrl(self)




    def jdbcUrl(self):

        localctx = JdbcUriParser.JdbcUrlContext(self, self._ctx, self.state)
        self.enterRule(localctx, 0, self.RULE_jdbcUrl)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 12
            self.match(JdbcUriParser.T__0)
            self.state = 13
            self.match(JdbcUriParser.DATABASE_TYPE)
            self.state = 15
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if (((_la) & ~0x3f) == 0 and ((1 << _la) & ((1 << JdbcUriParser.URI_SEPARATOR) | (1 << JdbcUriParser.HOST_NAME) | (1 << JdbcUriParser.IPV4_ADDRESS) | (1 << JdbcUriParser.IPV6_ADDRESS))) != 0):
                self.state = 14
                self.serverName()


            self.state = 18
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==JdbcUriParser.PORT_NUMBER:
                self.state = 17
                self.match(JdbcUriParser.PORT_NUMBER)


            self.state = 22
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==JdbcUriParser.T__1:
                self.state = 20
                self.match(JdbcUriParser.T__1)
                self.state = 21
                self.databaseName()


            self.state = 33
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==JdbcUriParser.CONNECTION_ARG_INIT:
                self.state = 24
                self.match(JdbcUriParser.CONNECTION_ARG_INIT)
                self.state = 25
                self.match(JdbcUriParser.CONNECTION_ARG)
                self.state = 30
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==JdbcUriParser.AMP:
                    self.state = 26
                    self.match(JdbcUriParser.AMP)
                    self.state = 27
                    self.match(JdbcUriParser.CONNECTION_ARG)
                    self.state = 32
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)



            self.state = 36
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==JdbcUriParser.COLON:
                self.state = 35
                self.schemaTable()


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class SchemaTableContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def COLON(self):
            return self.getToken(JdbcUriParser.COLON, 0)

        def tableName(self):
            return self.getTypedRuleContext(JdbcUriParser.TableNameContext,0)


        def schemaName(self):
            return self.getTypedRuleContext(JdbcUriParser.SchemaNameContext,0)


        def PERIOD(self):
            return self.getToken(JdbcUriParser.PERIOD, 0)

        def getRuleIndex(self):
            return JdbcUriParser.RULE_schemaTable

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSchemaTable" ):
                listener.enterSchemaTable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSchemaTable" ):
                listener.exitSchemaTable(self)




    def schemaTable(self):

        localctx = JdbcUriParser.SchemaTableContext(self, self._ctx, self.state)
        self.enterRule(localctx, 2, self.RULE_schemaTable)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 38
            self.match(JdbcUriParser.COLON)
            self.state = 42
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,6,self._ctx)
            if la_ == 1:
                self.state = 39
                self.schemaName()
                self.state = 40
                self.match(JdbcUriParser.PERIOD)


            self.state = 44
            self.tableName()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class DatabaseNameContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def IDENTIFIER(self):
            return self.getToken(JdbcUriParser.IDENTIFIER, 0)

        def getRuleIndex(self):
            return JdbcUriParser.RULE_databaseName

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterDatabaseName" ):
                listener.enterDatabaseName(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitDatabaseName" ):
                listener.exitDatabaseName(self)




    def databaseName(self):

        localctx = JdbcUriParser.DatabaseNameContext(self, self._ctx, self.state)
        self.enterRule(localctx, 4, self.RULE_databaseName)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 47
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==JdbcUriParser.IDENTIFIER:
                self.state = 46
                self.match(JdbcUriParser.IDENTIFIER)


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class SchemaNameContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def IDENTIFIER(self):
            return self.getToken(JdbcUriParser.IDENTIFIER, 0)

        def getRuleIndex(self):
            return JdbcUriParser.RULE_schemaName

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSchemaName" ):
                listener.enterSchemaName(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSchemaName" ):
                listener.exitSchemaName(self)




    def schemaName(self):

        localctx = JdbcUriParser.SchemaNameContext(self, self._ctx, self.state)
        self.enterRule(localctx, 6, self.RULE_schemaName)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 49
            self.match(JdbcUriParser.IDENTIFIER)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class TableNameContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def IDENTIFIER(self):
            return self.getToken(JdbcUriParser.IDENTIFIER, 0)

        def getRuleIndex(self):
            return JdbcUriParser.RULE_tableName

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTableName" ):
                listener.enterTableName(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTableName" ):
                listener.exitTableName(self)




    def tableName(self):

        localctx = JdbcUriParser.TableNameContext(self, self._ctx, self.state)
        self.enterRule(localctx, 8, self.RULE_tableName)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 51
            self.match(JdbcUriParser.IDENTIFIER)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class ServerNameContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def HOST_NAME(self):
            return self.getToken(JdbcUriParser.HOST_NAME, 0)

        def IPV4_ADDRESS(self):
            return self.getToken(JdbcUriParser.IPV4_ADDRESS, 0)

        def IPV6_ADDRESS(self):
            return self.getToken(JdbcUriParser.IPV6_ADDRESS, 0)

        def URI_SEPARATOR(self):
            return self.getToken(JdbcUriParser.URI_SEPARATOR, 0)

        def IDENTIFIER(self):
            return self.getToken(JdbcUriParser.IDENTIFIER, 0)

        def getRuleIndex(self):
            return JdbcUriParser.RULE_serverName

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterServerName" ):
                listener.enterServerName(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitServerName" ):
                listener.exitServerName(self)




    def serverName(self):

        localctx = JdbcUriParser.ServerNameContext(self, self._ctx, self.state)
        self.enterRule(localctx, 10, self.RULE_serverName)
        try:
            self.state = 59
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,8,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 53
                self.match(JdbcUriParser.HOST_NAME)
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 54
                self.match(JdbcUriParser.IPV4_ADDRESS)
                pass

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 55
                self.match(JdbcUriParser.IPV6_ADDRESS)
                pass

            elif la_ == 4:
                self.enterOuterAlt(localctx, 4)
                self.state = 56
                self.match(JdbcUriParser.URI_SEPARATOR)
                self.state = 57
                self.match(JdbcUriParser.IDENTIFIER)
                pass

            elif la_ == 5:
                self.enterOuterAlt(localctx, 5)
                self.state = 58
                self.match(JdbcUriParser.URI_SEPARATOR)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx





