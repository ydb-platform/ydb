# Generated from /home/runner/work/OpenMetadata/OpenMetadata/openmetadata-spec/src/main/antlr4/org/openmetadata/schema/EntityLink.g4 by ANTLR 4.9.2
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
        buf.write("\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\b")
        buf.write("\61\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\3\2\3\2\3")
        buf.write("\2\3\2\3\2\3\2\6\2\23\n\2\r\2\16\2\24\3\2\3\2\3\2\3\2")
        buf.write("\3\2\7\2\34\n\2\f\2\16\2\37\13\2\7\2!\n\2\f\2\16\2$\13")
        buf.write("\2\3\2\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\6\2\2")
        buf.write("\7\2\4\6\b\n\2\3\3\2\6\b\2.\2\f\3\2\2\2\4(\3\2\2\2\6*")
        buf.write("\3\2\2\2\b,\3\2\2\2\n.\3\2\2\2\f\22\7\5\2\2\r\16\5\n\6")
        buf.write("\2\16\17\5\4\3\2\17\20\5\n\6\2\20\21\5\6\4\2\21\23\3\2")
        buf.write("\2\2\22\r\3\2\2\2\23\24\3\2\2\2\24\22\3\2\2\2\24\25\3")
        buf.write("\2\2\2\25\"\3\2\2\2\26\27\5\n\6\2\27\35\5\b\5\2\30\31")
        buf.write("\5\n\6\2\31\32\5\6\4\2\32\34\3\2\2\2\33\30\3\2\2\2\34")
        buf.write("\37\3\2\2\2\35\33\3\2\2\2\35\36\3\2\2\2\36!\3\2\2\2\37")
        buf.write("\35\3\2\2\2 \26\3\2\2\2!$\3\2\2\2\" \3\2\2\2\"#\3\2\2")
        buf.write("\2#%\3\2\2\2$\"\3\2\2\2%&\7\3\2\2&\'\7\2\2\3\'\3\3\2\2")
        buf.write("\2()\7\6\2\2)\5\3\2\2\2*+\t\2\2\2+\7\3\2\2\2,-\7\7\2\2")
        buf.write("-\t\3\2\2\2./\7\4\2\2/\13\3\2\2\2\5\24\35\"")
        return buf.getvalue()


class EntityLinkParser ( Parser ):

    grammarFileName = "EntityLink.g4"

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [ DFA(ds, i) for i, ds in enumerate(atn.decisionToState) ]

    sharedContextCache = PredictionContextCache()

    literalNames = [ "<INVALID>", "'>'", "'::'", "'<#E'" ]

    symbolicNames = [ "<INVALID>", "<INVALID>", "<INVALID>", "RESERVED_START", 
                      "ENTITY_TYPE", "ENTITY_FIELD", "NAME_OR_FQN" ]

    RULE_entitylink = 0
    RULE_entity_type = 1
    RULE_nameOrFqn = 2
    RULE_entity_field = 3
    RULE_separator = 4

    ruleNames =  [ "entitylink", "entity_type", "nameOrFqn", "entity_field", 
                   "separator" ]

    EOF = Token.EOF
    T__0=1
    T__1=2
    RESERVED_START=3
    ENTITY_TYPE=4
    ENTITY_FIELD=5
    NAME_OR_FQN=6

    def __init__(self, input:TokenStream, output:TextIO = sys.stdout):
        super().__init__(input, output)
        self.checkVersion("4.9.2")
        self._interp = ParserATNSimulator(self, self.atn, self.decisionsToDFA, self.sharedContextCache)
        self._predicates = None




    class EntitylinkContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def RESERVED_START(self):
            return self.getToken(EntityLinkParser.RESERVED_START, 0)

        def EOF(self):
            return self.getToken(EntityLinkParser.EOF, 0)

        def separator(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(EntityLinkParser.SeparatorContext)
            else:
                return self.getTypedRuleContext(EntityLinkParser.SeparatorContext,i)


        def entity_type(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(EntityLinkParser.Entity_typeContext)
            else:
                return self.getTypedRuleContext(EntityLinkParser.Entity_typeContext,i)


        def nameOrFqn(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(EntityLinkParser.NameOrFqnContext)
            else:
                return self.getTypedRuleContext(EntityLinkParser.NameOrFqnContext,i)


        def entity_field(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(EntityLinkParser.Entity_fieldContext)
            else:
                return self.getTypedRuleContext(EntityLinkParser.Entity_fieldContext,i)


        def getRuleIndex(self):
            return EntityLinkParser.RULE_entitylink

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterEntitylink" ):
                listener.enterEntitylink(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitEntitylink" ):
                listener.exitEntitylink(self)




    def entitylink(self):

        localctx = EntityLinkParser.EntitylinkContext(self, self._ctx, self.state)
        self.enterRule(localctx, 0, self.RULE_entitylink)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 10
            self.match(EntityLinkParser.RESERVED_START)
            self.state = 16 
            self._errHandler.sync(self)
            _alt = 1
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt == 1:
                    self.state = 11
                    self.separator()
                    self.state = 12
                    self.entity_type()
                    self.state = 13
                    self.separator()
                    self.state = 14
                    self.nameOrFqn()

                else:
                    raise NoViableAltException(self)
                self.state = 18 
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,0,self._ctx)

            self.state = 32
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==EntityLinkParser.T__1:
                self.state = 20
                self.separator()
                self.state = 21
                self.entity_field()
                self.state = 27
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,1,self._ctx)
                while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                    if _alt==1:
                        self.state = 22
                        self.separator()
                        self.state = 23
                        self.nameOrFqn() 
                    self.state = 29
                    self._errHandler.sync(self)
                    _alt = self._interp.adaptivePredict(self._input,1,self._ctx)

                self.state = 34
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 35
            self.match(EntityLinkParser.T__0)
            self.state = 36
            self.match(EntityLinkParser.EOF)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class Entity_typeContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return EntityLinkParser.RULE_entity_type

     
        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class EntityTypeContext(Entity_typeContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a EntityLinkParser.Entity_typeContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ENTITY_TYPE(self):
            return self.getToken(EntityLinkParser.ENTITY_TYPE, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterEntityType" ):
                listener.enterEntityType(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitEntityType" ):
                listener.exitEntityType(self)



    def entity_type(self):

        localctx = EntityLinkParser.Entity_typeContext(self, self._ctx, self.state)
        self.enterRule(localctx, 2, self.RULE_entity_type)
        try:
            localctx = EntityLinkParser.EntityTypeContext(self, localctx)
            self.enterOuterAlt(localctx, 1)
            self.state = 38
            self.match(EntityLinkParser.ENTITY_TYPE)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class NameOrFqnContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def NAME_OR_FQN(self):
            return self.getToken(EntityLinkParser.NAME_OR_FQN, 0)

        def ENTITY_TYPE(self):
            return self.getToken(EntityLinkParser.ENTITY_TYPE, 0)

        def ENTITY_FIELD(self):
            return self.getToken(EntityLinkParser.ENTITY_FIELD, 0)

        def getRuleIndex(self):
            return EntityLinkParser.RULE_nameOrFqn

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNameOrFqn" ):
                listener.enterNameOrFqn(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNameOrFqn" ):
                listener.exitNameOrFqn(self)




    def nameOrFqn(self):

        localctx = EntityLinkParser.NameOrFqnContext(self, self._ctx, self.state)
        self.enterRule(localctx, 4, self.RULE_nameOrFqn)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 40
            _la = self._input.LA(1)
            if not((((_la) & ~0x3f) == 0 and ((1 << _la) & ((1 << EntityLinkParser.ENTITY_TYPE) | (1 << EntityLinkParser.ENTITY_FIELD) | (1 << EntityLinkParser.NAME_OR_FQN))) != 0)):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class Entity_fieldContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return EntityLinkParser.RULE_entity_field

     
        def copyFrom(self, ctx:ParserRuleContext):
            super().copyFrom(ctx)



    class EntityFieldContext(Entity_fieldContext):

        def __init__(self, parser, ctx:ParserRuleContext): # actually a EntityLinkParser.Entity_fieldContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def ENTITY_FIELD(self):
            return self.getToken(EntityLinkParser.ENTITY_FIELD, 0)

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterEntityField" ):
                listener.enterEntityField(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitEntityField" ):
                listener.exitEntityField(self)



    def entity_field(self):

        localctx = EntityLinkParser.Entity_fieldContext(self, self._ctx, self.state)
        self.enterRule(localctx, 6, self.RULE_entity_field)
        try:
            localctx = EntityLinkParser.EntityFieldContext(self, localctx)
            self.enterOuterAlt(localctx, 1)
            self.state = 42
            self.match(EntityLinkParser.ENTITY_FIELD)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class SeparatorContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser


        def getRuleIndex(self):
            return EntityLinkParser.RULE_separator

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSeparator" ):
                listener.enterSeparator(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSeparator" ):
                listener.exitSeparator(self)




    def separator(self):

        localctx = EntityLinkParser.SeparatorContext(self, self._ctx, self.state)
        self.enterRule(localctx, 8, self.RULE_separator)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 44
            self.match(EntityLinkParser.T__1)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx





