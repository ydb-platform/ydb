# pyright: reportOptionalMemberAccess=false, reportOptionalIterable=false

from typing import Any, Callable, Optional

from antlr4 import CommonTokenStream, InputStream

from proto_schema_parser import ast
from proto_schema_parser.antlr.ProtobufLexer import ProtobufLexer
from proto_schema_parser.antlr.ProtobufParser import ProtobufParser
from proto_schema_parser.antlr.ProtobufParserVisitor import ProtobufParserVisitor

SetupLexerCb = Callable[[ProtobufLexer], None]
"""
Callback function to modify the lexer during parsing.

Args:
    lexer (ProtobufLexer): The lexer instance being modified.
"""

SetupParserCb = Callable[[ProtobufParser], None]
"""
Callback function to modify the parser during parsing.

Args:
    parser (ProtobufParser): The parser instance being modified.
"""


class ASTConstructor(ProtobufParserVisitor):
    def visitFile(self, ctx: ProtobufParser.FileContext):
        syntax = (
            self._getText(ctx.syntaxDecl().syntaxLevel()) if ctx.syntaxDecl() else None
        )
        file_elements = [self.visit(child) for child in ctx.commentDecl()] + [
            self.visit(child) for child in ctx.fileElement()
        ]
        return ast.File(syntax=syntax, file_elements=file_elements)

    def visitCommentDecl(self, ctx: ProtobufParser.CommentDeclContext):
        return ast.Comment(text=self._getText(ctx, False))

    def visitPackageDecl(self, ctx: ProtobufParser.PackageDeclContext):
        name = self._getText(ctx.packageName())
        return ast.Package(name=name)

    def visitImportDecl(self, ctx: ProtobufParser.ImportDeclContext):
        name = self._getText(ctx.importedFileName())
        weak = ctx.WEAK() is not None
        public = ctx.PUBLIC() is not None
        return ast.Import(name=name, weak=weak, public=public)

    def visitOptionDecl(self, ctx: ProtobufParser.OptionDeclContext):
        name = ASTConstructor.normalize_option_name(self._getText(ctx.optionName()))
        value = self.visit(ctx.optionValue())
        return ast.Option(name=name, value=value)

    def visitOptionValue(self, ctx: ProtobufParser.OptionValueContext):
        if ctx.scalarValue():
            return self.visit(ctx.scalarValue())
        elif ctx.messageLiteralWithBraces():
            return self.visit(ctx.messageLiteralWithBraces())
        else:
            return self._getText(ctx)

    def visitMessageLiteralWithBraces(
        self, ctx: ProtobufParser.MessageLiteralWithBracesContext
    ):
        return self.visit(ctx.messageTextFormat())

    def visitMessageTextFormat(self, ctx: ProtobufParser.MessageTextFormatContext):
        if ctx.messageLiteralField():
            fields = [self.visit(child) for child in ctx.messageLiteralField()]
            return ast.MessageLiteral(fields=fields)
        elif ctx.commentDecl():
            return self.visit(ctx.commentDecl())
        else:
            return self._getText(ctx)

    def visitMessageLiteralField(self, ctx: ProtobufParser.MessageLiteralFieldContext):
        """Parse individual fields inside a message literal."""
        name = self._getText(ctx.messageLiteralFieldName())
        if ctx.value():
            value = self.visit(ctx.value())
        else:
            value = self.visit(ctx.messageValue())
        return ast.MessageLiteralField(name=name, value=value)

    def visitMessageDecl(self, ctx: ProtobufParser.MessageDeclContext):
        name = self._getText(ctx.messageName())
        elements = [self.visit(child) for child in ctx.messageElement()]
        return ast.Message(name=name, elements=elements)

    def visitMessageFieldDecl(self, ctx: ProtobufParser.MessageFieldDeclContext):
        if fieldWithCardinality := ctx.fieldDeclWithCardinality():
            return self.visit(fieldWithCardinality)
        else:
            name = self._getText(ctx.fieldName())
            number = int(self._getText(ctx.fieldNumber()))
            type = self._getText(ctx.messageFieldDeclTypeName())
            options = self.visit(ctx.compactOptions()) if ctx.compactOptions() else []
            return ast.Field(
                name=name,
                number=number,
                type=type,
                options=options,
            )

    def visitFieldDeclWithCardinality(
        self, ctx: ProtobufParser.FieldDeclWithCardinalityContext
    ):
        name = self._getText(ctx.fieldName())
        number = int(self._getText(ctx.fieldNumber()))
        cardinality = None
        if ctx.fieldCardinality().OPTIONAL():
            cardinality = ast.FieldCardinality.OPTIONAL
        elif ctx.fieldCardinality().REQUIRED():
            cardinality = ast.FieldCardinality.REQUIRED
        elif ctx.fieldCardinality().REPEATED():
            cardinality = ast.FieldCardinality.REPEATED
        type = self._getText(ctx.fieldDeclTypeName())
        options = self.visit(ctx.compactOptions()) if ctx.compactOptions() else []
        return ast.Field(
            name=name,
            number=number,
            cardinality=cardinality,
            type=type,
            options=options,
        )

    def visitCompactOption(self, ctx: ProtobufParser.CompactOptionContext):
        name = self._getText(ctx.optionName())
        value = self._stringToType(self._getText(ctx.optionValue()))
        return ast.Option(name=name, value=value)

    def visitCompactOptions(self, ctx: ProtobufParser.CompactOptionsContext):
        return [self.visit(child) for child in ctx.compactOption()]

    def visitOneofFieldDecl(self, ctx: ProtobufParser.OneofFieldDeclContext):
        name = self._getText(ctx.fieldName())
        number = int(self._getText(ctx.fieldNumber()))
        type = self._getText(ctx.oneofFieldDeclTypeName())
        options = self.visit(ctx.compactOptions()) if ctx.compactOptions() else []
        return ast.Field(
            name=name,
            number=number,
            type=type,
            options=options,
        )

    def visitOneofGroupDecl(self, ctx: ProtobufParser.OneofGroupDeclContext):
        name = self._getText(ctx.fieldName())
        number = int(self._getText(ctx.fieldNumber()))
        elements = [self.visit(child) for child in ctx.messageElement()]
        return ast.Group(
            name=name,
            number=number,
            elements=elements,
        )

    def visitMapFieldDecl(self, ctx: ProtobufParser.MapFieldDeclContext):
        name = self._getText(ctx.fieldName())
        number = int(self._getText(ctx.fieldNumber()))
        key_type = self._getText(ctx.mapType().mapKeyType())
        value_type = self._getText(ctx.mapType().typeName())
        options = self.visit(ctx.compactOptions()) if ctx.compactOptions() else []
        return ast.MapField(
            name=name,
            number=number,
            key_type=key_type,
            value_type=value_type,
            options=options,
        )

    def visitGroupDecl(self, ctx: ProtobufParser.GroupDeclContext):
        name = self._getText(ctx.fieldName())
        number = int(self._getText(ctx.fieldNumber()))
        cardinality = None
        if fieldCardinality := ctx.fieldCardinality():
            if fieldCardinality.OPTIONAL():
                cardinality = ast.FieldCardinality.OPTIONAL
            elif fieldCardinality.REQUIRED():
                cardinality = ast.FieldCardinality.REQUIRED
            elif fieldCardinality.REPEATED():
                cardinality = ast.FieldCardinality.REPEATED
        elements = [self.visit(child) for child in ctx.messageElement()]
        return ast.Group(
            name=name,
            number=number,
            cardinality=cardinality,
            elements=elements,
        )

    def visitOneofDecl(self, ctx: ProtobufParser.OneofDeclContext):
        name = self._getText(ctx.oneofName())
        elements = [self.visit(child) for child in ctx.oneofElement()]
        return ast.OneOf(name=name, elements=elements)

    def visitExtensionRangeDecl(self, ctx: ProtobufParser.ExtensionRangeDeclContext):
        ranges = [self._getText(child) for child in ctx.tagRanges().tagRange()]
        options = self.visit(ctx.compactOptions()) if ctx.compactOptions() else []
        return ast.ExtensionRange(ranges=ranges, options=options)

    def visitMessageReservedDecl(self, ctx: ProtobufParser.MessageReservedDeclContext):
        ranges = (
            [self._getText(child) for child in ctx.tagRanges().tagRange()]
            if ctx.tagRanges()
            else []
        )
        names = (
            [self._getText(child) for child in ctx.names().stringLiteral()]
            if ctx.names()
            else []
        )
        return ast.Reserved(ranges=ranges, names=names)

    def visitEnumDecl(self, ctx: ProtobufParser.EnumDeclContext):
        name = self._getText(ctx.enumName())
        elements = [self.visit(child) for child in ctx.enumElement()]
        return ast.Enum(name=name, elements=elements)

    def visitEnumValueDecl(self, ctx: ProtobufParser.EnumValueDeclContext):
        name = self._getText(ctx.enumValueName())
        number_text = self._getText(ctx.enumValueNumber())
        if number_text.lower().startswith("0x"):
            number = int(number_text, 16)
        else:
            number = int(number_text)
        options = self.visit(ctx.compactOptions()) if ctx.compactOptions() else []
        return ast.EnumValue(name=name, number=number, options=options)

    def visitEnumReservedDecl(self, ctx: ProtobufParser.EnumReservedDeclContext):
        ranges = (
            [self._getText(child) for child in ctx.enumValueRanges().enumValueRange()]
            if ctx.enumValueRanges()
            else []
        )
        names = (
            [self._getText(child) for child in ctx.names().stringLiteral()]
            if ctx.names()
            else []
        )
        return ast.EnumReserved(ranges=ranges, names=names)

    def visitExtensionDecl(self, ctx: ProtobufParser.ExtensionDeclContext):
        typeName = self._getText(ctx.extendedMessage())
        elements = [self.visit(child) for child in ctx.extensionElement()]
        return ast.Extension(typeName=typeName, elements=elements)

    def visitServiceDecl(self, ctx: ProtobufParser.ServiceDeclContext):
        name = self._getText(ctx.serviceName())
        elements = [self.visit(child) for child in ctx.serviceElement()]
        return ast.Service(name=name, elements=elements)

    def visitServiceElement(self, ctx: ProtobufParser.ServiceElementContext):
        if methodDecl := ctx.methodDecl():
            return self.visit(methodDecl)
        elif optionDecl := ctx.optionDecl():
            return self.visit(optionDecl)
        elif commentDecl := ctx.commentDecl():
            return self.visit(commentDecl)
        else:
            return self._getText(ctx)

    def visitMethodDecl(self, ctx: ProtobufParser.MethodDeclContext):
        name = self._getText(ctx.methodName())
        input_type = self.visit(ctx.inputType())
        output_type = self.visit(ctx.outputType())
        elements = [self.visit(child) for child in ctx.methodElement()]
        return ast.Method(
            name=name,
            input_type=input_type,
            output_type=output_type,
            elements=elements,
        )

    def visitInputType(self, ctx: ProtobufParser.InputTypeContext):
        return self.visit(ctx.messageType())

    def visitOutputType(self, ctx: ProtobufParser.OutputTypeContext):
        return self.visit(ctx.messageType())

    def visitMessageType(self, ctx: ProtobufParser.MessageTypeContext):
        name = self._getText(ctx.methodDeclTypeName())
        stream = ctx.STREAM() is not None
        return ast.MessageType(type=name, stream=stream)

    def visitMethodElement(self, ctx: ProtobufParser.MethodElementContext):
        if optionDecl := ctx.optionDecl():
            return self.visit(optionDecl)
        elif commentDecl := ctx.commentDecl():
            return self.visit(commentDecl)
        else:
            return self._getText(ctx)

    def visitScalarValue(
        self, ctx: ProtobufParser.ScalarValueContext
    ) -> ast.ScalarValue:
        if ctx.stringLiteral():
            return self._getText(ctx.stringLiteral())
        elif ctx.intLiteral():
            return self._stringToType(self._getText(ctx.intLiteral()))
        elif ctx.floatLiteral():
            return self._stringToType(self._getText(ctx.floatLiteral()))
        elif ctx.specialFloatLiteral():
            return self._stringToType(self._getText(ctx.specialFloatLiteral()))
        elif ctx.identifier():
            identifier_text = self._getText(ctx.identifier())
            if identifier_text in ["true", "false"]:
                return identifier_text == "true"
            else:
                return ast.Identifier(name=identifier_text)
        else:
            return self._getText(ctx)

    def visitIdentifier(self, ctx: ProtobufParser.IdentifierContext) -> ast.Identifier:
        return ast.Identifier(name=self._getText(ctx))

    def visitValue(self, ctx: ProtobufParser.ValueContext):
        """Parse a value, which can be a scalar, message literal, or list literal."""
        if ctx.scalarValue():
            return self.visit(ctx.scalarValue())
        elif ctx.messageLiteral():
            return self.visit(ctx.messageLiteral())
        else:  # listLiteral
            return self.visit(ctx.listLiteral())

    def visitMessageValue(self, ctx: ProtobufParser.MessageValueContext):
        """Parse a message value, which can be a message literal or list of message literals."""
        if ctx.messageLiteral():
            return self.visit(ctx.messageLiteral())
        else:  # listOfMessagesLiteral
            return self.visit(ctx.listOfMessagesLiteral())

    def visitListLiteral(self, ctx: ProtobufParser.ListLiteralContext):
        """Parse a list literal, which can contain scalar values or message literals."""
        if not ctx.listElement():
            return []
        return [self.visit(element) for element in ctx.listElement()]

    def visitListOfMessagesLiteral(
        self, ctx: ProtobufParser.ListOfMessagesLiteralContext
    ):
        """Parse a list of message literals."""
        if not ctx.messageLiteral():
            return []
        return [self.visit(msg) for msg in ctx.messageLiteral()]

    def visitAlwaysIdent(self, ctx: ProtobufParser.AlwaysIdentContext):
        if ctx.IDENTIFIER():
            # Unlike string/int/float, bools are just reated as identiifers in
            # the lexer, so we need to handle them here
            identifier_text = self._getText(ctx)
            if identifier_text in ["true", "false"]:
                return identifier_text == "true"
            return ast.Identifier(name=identifier_text)

        return super().visitAlwaysIdent(ctx)

    # ctx: ParserRuleContext, but ANTLR generates untyped code
    def _getText(self, ctx: Any, strip_quotes: bool = True):
        """ """

        token_source = (
            ctx.start.getTokenSource()
        )  # pyright: ignore [reportGeneralTypeIssues]
        input_stream = token_source.inputStream
        start, stop = (
            ctx.start.start,
            ctx.stop.stop,
        )  # pyright: ignore [reportGeneralTypeIssues]
        text = input_stream.getText(start, stop)
        text = text.strip('"') if strip_quotes else text
        return text

    def _stringToType(self, value: str):
        """
        Convert a string to a bool, int, or float if possible.

        Args:
            value: The string to convert.

        Returns:
            The converted value if possible, otherwise the original string.
        """

        if value.lower() in ["true", "false"]:
            return value.lower() == "true"
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            pass
        return value

    @staticmethod
    def normalize_option_name(s: str):
        # Remove all spaces
        s = s.replace(" ", "")

        # If the name starts with '(', find the matching ')'
        if s.startswith("("):
            depth = 0
            for i, c in enumerate(s):
                if c == "(":
                    depth += 1
                elif c == ")":
                    depth -= 1
                    if depth == 0:
                        break
            else:
                # No matching ')'
                raise ValueError("No matching closing parenthesis")
            # Extract content inside the outermost parentheses
            inside = s[1:i]
            rest = s[i + 1 :]
            # Remove any nested parentheses inside 'inside'
            inside = inside.replace("(", "").replace(")", "")
            # Rebuild the normalized option name
            option_name = f"({inside}){rest}"
        else:
            # If no starting '(', the option name is s
            option_name = s
        return option_name


class Parser:
    def __init__(
        self,
        *,
        setup_lexer: Optional[SetupLexerCb] = None,
        setup_parser: Optional[SetupParserCb] = None,
    ) -> None:
        self.setup_lexer = setup_lexer
        self.setup_parser = setup_parser

    def parse(self, text: str) -> ast.File:
        input_stream = InputStream(text)
        lexer = ProtobufLexer(input_stream)
        if self.setup_lexer:
            self.setup_lexer(lexer)

        token_stream = CommonTokenStream(lexer)
        parser = ProtobufParser(token_stream)
        if self.setup_parser:
            self.setup_parser(parser)

        parse_tree = parser.file_()
        visitor = ASTConstructor()
        return visitor.visit(parse_tree)  # pyright: ignore [reportGeneralTypeIssues]
