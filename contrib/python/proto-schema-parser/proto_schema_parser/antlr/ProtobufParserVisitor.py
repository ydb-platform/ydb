# Generated from antlr/ProtobufParser.g4 by ANTLR 4.13.0
from antlr4 import *
if "." in __name__:
    from .ProtobufParser import ProtobufParser
else:
    from ProtobufParser import ProtobufParser

# This class defines a complete generic visitor for a parse tree produced by ProtobufParser.

class ProtobufParserVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by ProtobufParser#file.
    def visitFile(self, ctx:ProtobufParser.FileContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#fileElement.
    def visitFileElement(self, ctx:ProtobufParser.FileElementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#commentDecl.
    def visitCommentDecl(self, ctx:ProtobufParser.CommentDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#syntaxDecl.
    def visitSyntaxDecl(self, ctx:ProtobufParser.SyntaxDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#syntaxLevel.
    def visitSyntaxLevel(self, ctx:ProtobufParser.SyntaxLevelContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#stringLiteral.
    def visitStringLiteral(self, ctx:ProtobufParser.StringLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#emptyDecl.
    def visitEmptyDecl(self, ctx:ProtobufParser.EmptyDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#packageDecl.
    def visitPackageDecl(self, ctx:ProtobufParser.PackageDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#packageName.
    def visitPackageName(self, ctx:ProtobufParser.PackageNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#importDecl.
    def visitImportDecl(self, ctx:ProtobufParser.ImportDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#importedFileName.
    def visitImportedFileName(self, ctx:ProtobufParser.ImportedFileNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#typeName.
    def visitTypeName(self, ctx:ProtobufParser.TypeNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#qualifiedIdentifier.
    def visitQualifiedIdentifier(self, ctx:ProtobufParser.QualifiedIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#fieldDeclTypeName.
    def visitFieldDeclTypeName(self, ctx:ProtobufParser.FieldDeclTypeNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageFieldDeclTypeName.
    def visitMessageFieldDeclTypeName(self, ctx:ProtobufParser.MessageFieldDeclTypeNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#extensionFieldDeclTypeName.
    def visitExtensionFieldDeclTypeName(self, ctx:ProtobufParser.ExtensionFieldDeclTypeNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#oneofFieldDeclTypeName.
    def visitOneofFieldDeclTypeName(self, ctx:ProtobufParser.OneofFieldDeclTypeNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#methodDeclTypeName.
    def visitMethodDeclTypeName(self, ctx:ProtobufParser.MethodDeclTypeNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#fieldDeclIdentifier.
    def visitFieldDeclIdentifier(self, ctx:ProtobufParser.FieldDeclIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageFieldDeclIdentifier.
    def visitMessageFieldDeclIdentifier(self, ctx:ProtobufParser.MessageFieldDeclIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#extensionFieldDeclIdentifier.
    def visitExtensionFieldDeclIdentifier(self, ctx:ProtobufParser.ExtensionFieldDeclIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#oneofFieldDeclIdentifier.
    def visitOneofFieldDeclIdentifier(self, ctx:ProtobufParser.OneofFieldDeclIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#methodDeclIdentifier.
    def visitMethodDeclIdentifier(self, ctx:ProtobufParser.MethodDeclIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#fullyQualifiedIdentifier.
    def visitFullyQualifiedIdentifier(self, ctx:ProtobufParser.FullyQualifiedIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#optionDecl.
    def visitOptionDecl(self, ctx:ProtobufParser.OptionDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#compactOptions.
    def visitCompactOptions(self, ctx:ProtobufParser.CompactOptionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#compactOption.
    def visitCompactOption(self, ctx:ProtobufParser.CompactOptionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#optionName.
    def visitOptionName(self, ctx:ProtobufParser.OptionNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#optionValue.
    def visitOptionValue(self, ctx:ProtobufParser.OptionValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#scalarValue.
    def visitScalarValue(self, ctx:ProtobufParser.ScalarValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#intLiteral.
    def visitIntLiteral(self, ctx:ProtobufParser.IntLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#floatLiteral.
    def visitFloatLiteral(self, ctx:ProtobufParser.FloatLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#specialFloatLiteral.
    def visitSpecialFloatLiteral(self, ctx:ProtobufParser.SpecialFloatLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageLiteralWithBraces.
    def visitMessageLiteralWithBraces(self, ctx:ProtobufParser.MessageLiteralWithBracesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageTextFormat.
    def visitMessageTextFormat(self, ctx:ProtobufParser.MessageTextFormatContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageLiteralField.
    def visitMessageLiteralField(self, ctx:ProtobufParser.MessageLiteralFieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageLiteralFieldName.
    def visitMessageLiteralFieldName(self, ctx:ProtobufParser.MessageLiteralFieldNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#specialFieldName.
    def visitSpecialFieldName(self, ctx:ProtobufParser.SpecialFieldNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#extensionFieldName.
    def visitExtensionFieldName(self, ctx:ProtobufParser.ExtensionFieldNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#typeURL.
    def visitTypeURL(self, ctx:ProtobufParser.TypeURLContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#value.
    def visitValue(self, ctx:ProtobufParser.ValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageValue.
    def visitMessageValue(self, ctx:ProtobufParser.MessageValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageLiteral.
    def visitMessageLiteral(self, ctx:ProtobufParser.MessageLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#listLiteral.
    def visitListLiteral(self, ctx:ProtobufParser.ListLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#listElement.
    def visitListElement(self, ctx:ProtobufParser.ListElementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#listOfMessagesLiteral.
    def visitListOfMessagesLiteral(self, ctx:ProtobufParser.ListOfMessagesLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageDecl.
    def visitMessageDecl(self, ctx:ProtobufParser.MessageDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageName.
    def visitMessageName(self, ctx:ProtobufParser.MessageNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageElement.
    def visitMessageElement(self, ctx:ProtobufParser.MessageElementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageFieldDecl.
    def visitMessageFieldDecl(self, ctx:ProtobufParser.MessageFieldDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#fieldDeclWithCardinality.
    def visitFieldDeclWithCardinality(self, ctx:ProtobufParser.FieldDeclWithCardinalityContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#fieldCardinality.
    def visitFieldCardinality(self, ctx:ProtobufParser.FieldCardinalityContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#fieldName.
    def visitFieldName(self, ctx:ProtobufParser.FieldNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#fieldNumber.
    def visitFieldNumber(self, ctx:ProtobufParser.FieldNumberContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#mapFieldDecl.
    def visitMapFieldDecl(self, ctx:ProtobufParser.MapFieldDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#mapType.
    def visitMapType(self, ctx:ProtobufParser.MapTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#mapKeyType.
    def visitMapKeyType(self, ctx:ProtobufParser.MapKeyTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#groupDecl.
    def visitGroupDecl(self, ctx:ProtobufParser.GroupDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#oneofDecl.
    def visitOneofDecl(self, ctx:ProtobufParser.OneofDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#oneofName.
    def visitOneofName(self, ctx:ProtobufParser.OneofNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#oneofElement.
    def visitOneofElement(self, ctx:ProtobufParser.OneofElementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#oneofFieldDecl.
    def visitOneofFieldDecl(self, ctx:ProtobufParser.OneofFieldDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#oneofGroupDecl.
    def visitOneofGroupDecl(self, ctx:ProtobufParser.OneofGroupDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#extensionRangeDecl.
    def visitExtensionRangeDecl(self, ctx:ProtobufParser.ExtensionRangeDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#tagRanges.
    def visitTagRanges(self, ctx:ProtobufParser.TagRangesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#tagRange.
    def visitTagRange(self, ctx:ProtobufParser.TagRangeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#tagRangeStart.
    def visitTagRangeStart(self, ctx:ProtobufParser.TagRangeStartContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#tagRangeEnd.
    def visitTagRangeEnd(self, ctx:ProtobufParser.TagRangeEndContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageReservedDecl.
    def visitMessageReservedDecl(self, ctx:ProtobufParser.MessageReservedDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#names.
    def visitNames(self, ctx:ProtobufParser.NamesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#enumDecl.
    def visitEnumDecl(self, ctx:ProtobufParser.EnumDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#enumName.
    def visitEnumName(self, ctx:ProtobufParser.EnumNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#enumElement.
    def visitEnumElement(self, ctx:ProtobufParser.EnumElementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#enumValueDecl.
    def visitEnumValueDecl(self, ctx:ProtobufParser.EnumValueDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#enumValueName.
    def visitEnumValueName(self, ctx:ProtobufParser.EnumValueNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#enumValueNumber.
    def visitEnumValueNumber(self, ctx:ProtobufParser.EnumValueNumberContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#enumReservedDecl.
    def visitEnumReservedDecl(self, ctx:ProtobufParser.EnumReservedDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#enumValueRanges.
    def visitEnumValueRanges(self, ctx:ProtobufParser.EnumValueRangesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#enumValueRange.
    def visitEnumValueRange(self, ctx:ProtobufParser.EnumValueRangeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#enumValueRangeStart.
    def visitEnumValueRangeStart(self, ctx:ProtobufParser.EnumValueRangeStartContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#enumValueRangeEnd.
    def visitEnumValueRangeEnd(self, ctx:ProtobufParser.EnumValueRangeEndContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#extensionDecl.
    def visitExtensionDecl(self, ctx:ProtobufParser.ExtensionDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#extendedMessage.
    def visitExtendedMessage(self, ctx:ProtobufParser.ExtendedMessageContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#extensionElement.
    def visitExtensionElement(self, ctx:ProtobufParser.ExtensionElementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#extensionFieldDecl.
    def visitExtensionFieldDecl(self, ctx:ProtobufParser.ExtensionFieldDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#serviceDecl.
    def visitServiceDecl(self, ctx:ProtobufParser.ServiceDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#serviceName.
    def visitServiceName(self, ctx:ProtobufParser.ServiceNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#serviceElement.
    def visitServiceElement(self, ctx:ProtobufParser.ServiceElementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#methodDecl.
    def visitMethodDecl(self, ctx:ProtobufParser.MethodDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#methodName.
    def visitMethodName(self, ctx:ProtobufParser.MethodNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#inputType.
    def visitInputType(self, ctx:ProtobufParser.InputTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#outputType.
    def visitOutputType(self, ctx:ProtobufParser.OutputTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#methodElement.
    def visitMethodElement(self, ctx:ProtobufParser.MethodElementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#messageType.
    def visitMessageType(self, ctx:ProtobufParser.MessageTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#identifier.
    def visitIdentifier(self, ctx:ProtobufParser.IdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#alwaysIdent.
    def visitAlwaysIdent(self, ctx:ProtobufParser.AlwaysIdentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ProtobufParser#sometimesIdent.
    def visitSometimesIdent(self, ctx:ProtobufParser.SometimesIdentContext):
        return self.visitChildren(ctx)



del ProtobufParser