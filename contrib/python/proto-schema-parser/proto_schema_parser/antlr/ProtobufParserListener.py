# Generated from antlr/ProtobufParser.g4 by ANTLR 4.13.0
from antlr4 import *
if "." in __name__:
    from .ProtobufParser import ProtobufParser
else:
    from ProtobufParser import ProtobufParser

# This class defines a complete listener for a parse tree produced by ProtobufParser.
class ProtobufParserListener(ParseTreeListener):

    # Enter a parse tree produced by ProtobufParser#file.
    def enterFile(self, ctx:ProtobufParser.FileContext):
        pass

    # Exit a parse tree produced by ProtobufParser#file.
    def exitFile(self, ctx:ProtobufParser.FileContext):
        pass


    # Enter a parse tree produced by ProtobufParser#fileElement.
    def enterFileElement(self, ctx:ProtobufParser.FileElementContext):
        pass

    # Exit a parse tree produced by ProtobufParser#fileElement.
    def exitFileElement(self, ctx:ProtobufParser.FileElementContext):
        pass


    # Enter a parse tree produced by ProtobufParser#commentDecl.
    def enterCommentDecl(self, ctx:ProtobufParser.CommentDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#commentDecl.
    def exitCommentDecl(self, ctx:ProtobufParser.CommentDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#syntaxDecl.
    def enterSyntaxDecl(self, ctx:ProtobufParser.SyntaxDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#syntaxDecl.
    def exitSyntaxDecl(self, ctx:ProtobufParser.SyntaxDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#syntaxLevel.
    def enterSyntaxLevel(self, ctx:ProtobufParser.SyntaxLevelContext):
        pass

    # Exit a parse tree produced by ProtobufParser#syntaxLevel.
    def exitSyntaxLevel(self, ctx:ProtobufParser.SyntaxLevelContext):
        pass


    # Enter a parse tree produced by ProtobufParser#stringLiteral.
    def enterStringLiteral(self, ctx:ProtobufParser.StringLiteralContext):
        pass

    # Exit a parse tree produced by ProtobufParser#stringLiteral.
    def exitStringLiteral(self, ctx:ProtobufParser.StringLiteralContext):
        pass


    # Enter a parse tree produced by ProtobufParser#emptyDecl.
    def enterEmptyDecl(self, ctx:ProtobufParser.EmptyDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#emptyDecl.
    def exitEmptyDecl(self, ctx:ProtobufParser.EmptyDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#packageDecl.
    def enterPackageDecl(self, ctx:ProtobufParser.PackageDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#packageDecl.
    def exitPackageDecl(self, ctx:ProtobufParser.PackageDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#packageName.
    def enterPackageName(self, ctx:ProtobufParser.PackageNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#packageName.
    def exitPackageName(self, ctx:ProtobufParser.PackageNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#importDecl.
    def enterImportDecl(self, ctx:ProtobufParser.ImportDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#importDecl.
    def exitImportDecl(self, ctx:ProtobufParser.ImportDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#importedFileName.
    def enterImportedFileName(self, ctx:ProtobufParser.ImportedFileNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#importedFileName.
    def exitImportedFileName(self, ctx:ProtobufParser.ImportedFileNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#typeName.
    def enterTypeName(self, ctx:ProtobufParser.TypeNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#typeName.
    def exitTypeName(self, ctx:ProtobufParser.TypeNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#qualifiedIdentifier.
    def enterQualifiedIdentifier(self, ctx:ProtobufParser.QualifiedIdentifierContext):
        pass

    # Exit a parse tree produced by ProtobufParser#qualifiedIdentifier.
    def exitQualifiedIdentifier(self, ctx:ProtobufParser.QualifiedIdentifierContext):
        pass


    # Enter a parse tree produced by ProtobufParser#fieldDeclTypeName.
    def enterFieldDeclTypeName(self, ctx:ProtobufParser.FieldDeclTypeNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#fieldDeclTypeName.
    def exitFieldDeclTypeName(self, ctx:ProtobufParser.FieldDeclTypeNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageFieldDeclTypeName.
    def enterMessageFieldDeclTypeName(self, ctx:ProtobufParser.MessageFieldDeclTypeNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageFieldDeclTypeName.
    def exitMessageFieldDeclTypeName(self, ctx:ProtobufParser.MessageFieldDeclTypeNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#extensionFieldDeclTypeName.
    def enterExtensionFieldDeclTypeName(self, ctx:ProtobufParser.ExtensionFieldDeclTypeNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#extensionFieldDeclTypeName.
    def exitExtensionFieldDeclTypeName(self, ctx:ProtobufParser.ExtensionFieldDeclTypeNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#oneofFieldDeclTypeName.
    def enterOneofFieldDeclTypeName(self, ctx:ProtobufParser.OneofFieldDeclTypeNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#oneofFieldDeclTypeName.
    def exitOneofFieldDeclTypeName(self, ctx:ProtobufParser.OneofFieldDeclTypeNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#methodDeclTypeName.
    def enterMethodDeclTypeName(self, ctx:ProtobufParser.MethodDeclTypeNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#methodDeclTypeName.
    def exitMethodDeclTypeName(self, ctx:ProtobufParser.MethodDeclTypeNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#fieldDeclIdentifier.
    def enterFieldDeclIdentifier(self, ctx:ProtobufParser.FieldDeclIdentifierContext):
        pass

    # Exit a parse tree produced by ProtobufParser#fieldDeclIdentifier.
    def exitFieldDeclIdentifier(self, ctx:ProtobufParser.FieldDeclIdentifierContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageFieldDeclIdentifier.
    def enterMessageFieldDeclIdentifier(self, ctx:ProtobufParser.MessageFieldDeclIdentifierContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageFieldDeclIdentifier.
    def exitMessageFieldDeclIdentifier(self, ctx:ProtobufParser.MessageFieldDeclIdentifierContext):
        pass


    # Enter a parse tree produced by ProtobufParser#extensionFieldDeclIdentifier.
    def enterExtensionFieldDeclIdentifier(self, ctx:ProtobufParser.ExtensionFieldDeclIdentifierContext):
        pass

    # Exit a parse tree produced by ProtobufParser#extensionFieldDeclIdentifier.
    def exitExtensionFieldDeclIdentifier(self, ctx:ProtobufParser.ExtensionFieldDeclIdentifierContext):
        pass


    # Enter a parse tree produced by ProtobufParser#oneofFieldDeclIdentifier.
    def enterOneofFieldDeclIdentifier(self, ctx:ProtobufParser.OneofFieldDeclIdentifierContext):
        pass

    # Exit a parse tree produced by ProtobufParser#oneofFieldDeclIdentifier.
    def exitOneofFieldDeclIdentifier(self, ctx:ProtobufParser.OneofFieldDeclIdentifierContext):
        pass


    # Enter a parse tree produced by ProtobufParser#methodDeclIdentifier.
    def enterMethodDeclIdentifier(self, ctx:ProtobufParser.MethodDeclIdentifierContext):
        pass

    # Exit a parse tree produced by ProtobufParser#methodDeclIdentifier.
    def exitMethodDeclIdentifier(self, ctx:ProtobufParser.MethodDeclIdentifierContext):
        pass


    # Enter a parse tree produced by ProtobufParser#fullyQualifiedIdentifier.
    def enterFullyQualifiedIdentifier(self, ctx:ProtobufParser.FullyQualifiedIdentifierContext):
        pass

    # Exit a parse tree produced by ProtobufParser#fullyQualifiedIdentifier.
    def exitFullyQualifiedIdentifier(self, ctx:ProtobufParser.FullyQualifiedIdentifierContext):
        pass


    # Enter a parse tree produced by ProtobufParser#optionDecl.
    def enterOptionDecl(self, ctx:ProtobufParser.OptionDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#optionDecl.
    def exitOptionDecl(self, ctx:ProtobufParser.OptionDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#compactOptions.
    def enterCompactOptions(self, ctx:ProtobufParser.CompactOptionsContext):
        pass

    # Exit a parse tree produced by ProtobufParser#compactOptions.
    def exitCompactOptions(self, ctx:ProtobufParser.CompactOptionsContext):
        pass


    # Enter a parse tree produced by ProtobufParser#compactOption.
    def enterCompactOption(self, ctx:ProtobufParser.CompactOptionContext):
        pass

    # Exit a parse tree produced by ProtobufParser#compactOption.
    def exitCompactOption(self, ctx:ProtobufParser.CompactOptionContext):
        pass


    # Enter a parse tree produced by ProtobufParser#optionName.
    def enterOptionName(self, ctx:ProtobufParser.OptionNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#optionName.
    def exitOptionName(self, ctx:ProtobufParser.OptionNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#optionValue.
    def enterOptionValue(self, ctx:ProtobufParser.OptionValueContext):
        pass

    # Exit a parse tree produced by ProtobufParser#optionValue.
    def exitOptionValue(self, ctx:ProtobufParser.OptionValueContext):
        pass


    # Enter a parse tree produced by ProtobufParser#scalarValue.
    def enterScalarValue(self, ctx:ProtobufParser.ScalarValueContext):
        pass

    # Exit a parse tree produced by ProtobufParser#scalarValue.
    def exitScalarValue(self, ctx:ProtobufParser.ScalarValueContext):
        pass


    # Enter a parse tree produced by ProtobufParser#intLiteral.
    def enterIntLiteral(self, ctx:ProtobufParser.IntLiteralContext):
        pass

    # Exit a parse tree produced by ProtobufParser#intLiteral.
    def exitIntLiteral(self, ctx:ProtobufParser.IntLiteralContext):
        pass


    # Enter a parse tree produced by ProtobufParser#floatLiteral.
    def enterFloatLiteral(self, ctx:ProtobufParser.FloatLiteralContext):
        pass

    # Exit a parse tree produced by ProtobufParser#floatLiteral.
    def exitFloatLiteral(self, ctx:ProtobufParser.FloatLiteralContext):
        pass


    # Enter a parse tree produced by ProtobufParser#specialFloatLiteral.
    def enterSpecialFloatLiteral(self, ctx:ProtobufParser.SpecialFloatLiteralContext):
        pass

    # Exit a parse tree produced by ProtobufParser#specialFloatLiteral.
    def exitSpecialFloatLiteral(self, ctx:ProtobufParser.SpecialFloatLiteralContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageLiteralWithBraces.
    def enterMessageLiteralWithBraces(self, ctx:ProtobufParser.MessageLiteralWithBracesContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageLiteralWithBraces.
    def exitMessageLiteralWithBraces(self, ctx:ProtobufParser.MessageLiteralWithBracesContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageTextFormat.
    def enterMessageTextFormat(self, ctx:ProtobufParser.MessageTextFormatContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageTextFormat.
    def exitMessageTextFormat(self, ctx:ProtobufParser.MessageTextFormatContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageLiteralField.
    def enterMessageLiteralField(self, ctx:ProtobufParser.MessageLiteralFieldContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageLiteralField.
    def exitMessageLiteralField(self, ctx:ProtobufParser.MessageLiteralFieldContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageLiteralFieldName.
    def enterMessageLiteralFieldName(self, ctx:ProtobufParser.MessageLiteralFieldNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageLiteralFieldName.
    def exitMessageLiteralFieldName(self, ctx:ProtobufParser.MessageLiteralFieldNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#specialFieldName.
    def enterSpecialFieldName(self, ctx:ProtobufParser.SpecialFieldNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#specialFieldName.
    def exitSpecialFieldName(self, ctx:ProtobufParser.SpecialFieldNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#extensionFieldName.
    def enterExtensionFieldName(self, ctx:ProtobufParser.ExtensionFieldNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#extensionFieldName.
    def exitExtensionFieldName(self, ctx:ProtobufParser.ExtensionFieldNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#typeURL.
    def enterTypeURL(self, ctx:ProtobufParser.TypeURLContext):
        pass

    # Exit a parse tree produced by ProtobufParser#typeURL.
    def exitTypeURL(self, ctx:ProtobufParser.TypeURLContext):
        pass


    # Enter a parse tree produced by ProtobufParser#value.
    def enterValue(self, ctx:ProtobufParser.ValueContext):
        pass

    # Exit a parse tree produced by ProtobufParser#value.
    def exitValue(self, ctx:ProtobufParser.ValueContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageValue.
    def enterMessageValue(self, ctx:ProtobufParser.MessageValueContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageValue.
    def exitMessageValue(self, ctx:ProtobufParser.MessageValueContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageLiteral.
    def enterMessageLiteral(self, ctx:ProtobufParser.MessageLiteralContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageLiteral.
    def exitMessageLiteral(self, ctx:ProtobufParser.MessageLiteralContext):
        pass


    # Enter a parse tree produced by ProtobufParser#listLiteral.
    def enterListLiteral(self, ctx:ProtobufParser.ListLiteralContext):
        pass

    # Exit a parse tree produced by ProtobufParser#listLiteral.
    def exitListLiteral(self, ctx:ProtobufParser.ListLiteralContext):
        pass


    # Enter a parse tree produced by ProtobufParser#listElement.
    def enterListElement(self, ctx:ProtobufParser.ListElementContext):
        pass

    # Exit a parse tree produced by ProtobufParser#listElement.
    def exitListElement(self, ctx:ProtobufParser.ListElementContext):
        pass


    # Enter a parse tree produced by ProtobufParser#listOfMessagesLiteral.
    def enterListOfMessagesLiteral(self, ctx:ProtobufParser.ListOfMessagesLiteralContext):
        pass

    # Exit a parse tree produced by ProtobufParser#listOfMessagesLiteral.
    def exitListOfMessagesLiteral(self, ctx:ProtobufParser.ListOfMessagesLiteralContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageDecl.
    def enterMessageDecl(self, ctx:ProtobufParser.MessageDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageDecl.
    def exitMessageDecl(self, ctx:ProtobufParser.MessageDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageName.
    def enterMessageName(self, ctx:ProtobufParser.MessageNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageName.
    def exitMessageName(self, ctx:ProtobufParser.MessageNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageElement.
    def enterMessageElement(self, ctx:ProtobufParser.MessageElementContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageElement.
    def exitMessageElement(self, ctx:ProtobufParser.MessageElementContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageFieldDecl.
    def enterMessageFieldDecl(self, ctx:ProtobufParser.MessageFieldDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageFieldDecl.
    def exitMessageFieldDecl(self, ctx:ProtobufParser.MessageFieldDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#fieldDeclWithCardinality.
    def enterFieldDeclWithCardinality(self, ctx:ProtobufParser.FieldDeclWithCardinalityContext):
        pass

    # Exit a parse tree produced by ProtobufParser#fieldDeclWithCardinality.
    def exitFieldDeclWithCardinality(self, ctx:ProtobufParser.FieldDeclWithCardinalityContext):
        pass


    # Enter a parse tree produced by ProtobufParser#fieldCardinality.
    def enterFieldCardinality(self, ctx:ProtobufParser.FieldCardinalityContext):
        pass

    # Exit a parse tree produced by ProtobufParser#fieldCardinality.
    def exitFieldCardinality(self, ctx:ProtobufParser.FieldCardinalityContext):
        pass


    # Enter a parse tree produced by ProtobufParser#fieldName.
    def enterFieldName(self, ctx:ProtobufParser.FieldNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#fieldName.
    def exitFieldName(self, ctx:ProtobufParser.FieldNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#fieldNumber.
    def enterFieldNumber(self, ctx:ProtobufParser.FieldNumberContext):
        pass

    # Exit a parse tree produced by ProtobufParser#fieldNumber.
    def exitFieldNumber(self, ctx:ProtobufParser.FieldNumberContext):
        pass


    # Enter a parse tree produced by ProtobufParser#mapFieldDecl.
    def enterMapFieldDecl(self, ctx:ProtobufParser.MapFieldDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#mapFieldDecl.
    def exitMapFieldDecl(self, ctx:ProtobufParser.MapFieldDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#mapType.
    def enterMapType(self, ctx:ProtobufParser.MapTypeContext):
        pass

    # Exit a parse tree produced by ProtobufParser#mapType.
    def exitMapType(self, ctx:ProtobufParser.MapTypeContext):
        pass


    # Enter a parse tree produced by ProtobufParser#mapKeyType.
    def enterMapKeyType(self, ctx:ProtobufParser.MapKeyTypeContext):
        pass

    # Exit a parse tree produced by ProtobufParser#mapKeyType.
    def exitMapKeyType(self, ctx:ProtobufParser.MapKeyTypeContext):
        pass


    # Enter a parse tree produced by ProtobufParser#groupDecl.
    def enterGroupDecl(self, ctx:ProtobufParser.GroupDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#groupDecl.
    def exitGroupDecl(self, ctx:ProtobufParser.GroupDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#oneofDecl.
    def enterOneofDecl(self, ctx:ProtobufParser.OneofDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#oneofDecl.
    def exitOneofDecl(self, ctx:ProtobufParser.OneofDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#oneofName.
    def enterOneofName(self, ctx:ProtobufParser.OneofNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#oneofName.
    def exitOneofName(self, ctx:ProtobufParser.OneofNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#oneofElement.
    def enterOneofElement(self, ctx:ProtobufParser.OneofElementContext):
        pass

    # Exit a parse tree produced by ProtobufParser#oneofElement.
    def exitOneofElement(self, ctx:ProtobufParser.OneofElementContext):
        pass


    # Enter a parse tree produced by ProtobufParser#oneofFieldDecl.
    def enterOneofFieldDecl(self, ctx:ProtobufParser.OneofFieldDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#oneofFieldDecl.
    def exitOneofFieldDecl(self, ctx:ProtobufParser.OneofFieldDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#oneofGroupDecl.
    def enterOneofGroupDecl(self, ctx:ProtobufParser.OneofGroupDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#oneofGroupDecl.
    def exitOneofGroupDecl(self, ctx:ProtobufParser.OneofGroupDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#extensionRangeDecl.
    def enterExtensionRangeDecl(self, ctx:ProtobufParser.ExtensionRangeDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#extensionRangeDecl.
    def exitExtensionRangeDecl(self, ctx:ProtobufParser.ExtensionRangeDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#tagRanges.
    def enterTagRanges(self, ctx:ProtobufParser.TagRangesContext):
        pass

    # Exit a parse tree produced by ProtobufParser#tagRanges.
    def exitTagRanges(self, ctx:ProtobufParser.TagRangesContext):
        pass


    # Enter a parse tree produced by ProtobufParser#tagRange.
    def enterTagRange(self, ctx:ProtobufParser.TagRangeContext):
        pass

    # Exit a parse tree produced by ProtobufParser#tagRange.
    def exitTagRange(self, ctx:ProtobufParser.TagRangeContext):
        pass


    # Enter a parse tree produced by ProtobufParser#tagRangeStart.
    def enterTagRangeStart(self, ctx:ProtobufParser.TagRangeStartContext):
        pass

    # Exit a parse tree produced by ProtobufParser#tagRangeStart.
    def exitTagRangeStart(self, ctx:ProtobufParser.TagRangeStartContext):
        pass


    # Enter a parse tree produced by ProtobufParser#tagRangeEnd.
    def enterTagRangeEnd(self, ctx:ProtobufParser.TagRangeEndContext):
        pass

    # Exit a parse tree produced by ProtobufParser#tagRangeEnd.
    def exitTagRangeEnd(self, ctx:ProtobufParser.TagRangeEndContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageReservedDecl.
    def enterMessageReservedDecl(self, ctx:ProtobufParser.MessageReservedDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageReservedDecl.
    def exitMessageReservedDecl(self, ctx:ProtobufParser.MessageReservedDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#names.
    def enterNames(self, ctx:ProtobufParser.NamesContext):
        pass

    # Exit a parse tree produced by ProtobufParser#names.
    def exitNames(self, ctx:ProtobufParser.NamesContext):
        pass


    # Enter a parse tree produced by ProtobufParser#enumDecl.
    def enterEnumDecl(self, ctx:ProtobufParser.EnumDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#enumDecl.
    def exitEnumDecl(self, ctx:ProtobufParser.EnumDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#enumName.
    def enterEnumName(self, ctx:ProtobufParser.EnumNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#enumName.
    def exitEnumName(self, ctx:ProtobufParser.EnumNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#enumElement.
    def enterEnumElement(self, ctx:ProtobufParser.EnumElementContext):
        pass

    # Exit a parse tree produced by ProtobufParser#enumElement.
    def exitEnumElement(self, ctx:ProtobufParser.EnumElementContext):
        pass


    # Enter a parse tree produced by ProtobufParser#enumValueDecl.
    def enterEnumValueDecl(self, ctx:ProtobufParser.EnumValueDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#enumValueDecl.
    def exitEnumValueDecl(self, ctx:ProtobufParser.EnumValueDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#enumValueName.
    def enterEnumValueName(self, ctx:ProtobufParser.EnumValueNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#enumValueName.
    def exitEnumValueName(self, ctx:ProtobufParser.EnumValueNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#enumValueNumber.
    def enterEnumValueNumber(self, ctx:ProtobufParser.EnumValueNumberContext):
        pass

    # Exit a parse tree produced by ProtobufParser#enumValueNumber.
    def exitEnumValueNumber(self, ctx:ProtobufParser.EnumValueNumberContext):
        pass


    # Enter a parse tree produced by ProtobufParser#enumReservedDecl.
    def enterEnumReservedDecl(self, ctx:ProtobufParser.EnumReservedDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#enumReservedDecl.
    def exitEnumReservedDecl(self, ctx:ProtobufParser.EnumReservedDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#enumValueRanges.
    def enterEnumValueRanges(self, ctx:ProtobufParser.EnumValueRangesContext):
        pass

    # Exit a parse tree produced by ProtobufParser#enumValueRanges.
    def exitEnumValueRanges(self, ctx:ProtobufParser.EnumValueRangesContext):
        pass


    # Enter a parse tree produced by ProtobufParser#enumValueRange.
    def enterEnumValueRange(self, ctx:ProtobufParser.EnumValueRangeContext):
        pass

    # Exit a parse tree produced by ProtobufParser#enumValueRange.
    def exitEnumValueRange(self, ctx:ProtobufParser.EnumValueRangeContext):
        pass


    # Enter a parse tree produced by ProtobufParser#enumValueRangeStart.
    def enterEnumValueRangeStart(self, ctx:ProtobufParser.EnumValueRangeStartContext):
        pass

    # Exit a parse tree produced by ProtobufParser#enumValueRangeStart.
    def exitEnumValueRangeStart(self, ctx:ProtobufParser.EnumValueRangeStartContext):
        pass


    # Enter a parse tree produced by ProtobufParser#enumValueRangeEnd.
    def enterEnumValueRangeEnd(self, ctx:ProtobufParser.EnumValueRangeEndContext):
        pass

    # Exit a parse tree produced by ProtobufParser#enumValueRangeEnd.
    def exitEnumValueRangeEnd(self, ctx:ProtobufParser.EnumValueRangeEndContext):
        pass


    # Enter a parse tree produced by ProtobufParser#extensionDecl.
    def enterExtensionDecl(self, ctx:ProtobufParser.ExtensionDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#extensionDecl.
    def exitExtensionDecl(self, ctx:ProtobufParser.ExtensionDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#extendedMessage.
    def enterExtendedMessage(self, ctx:ProtobufParser.ExtendedMessageContext):
        pass

    # Exit a parse tree produced by ProtobufParser#extendedMessage.
    def exitExtendedMessage(self, ctx:ProtobufParser.ExtendedMessageContext):
        pass


    # Enter a parse tree produced by ProtobufParser#extensionElement.
    def enterExtensionElement(self, ctx:ProtobufParser.ExtensionElementContext):
        pass

    # Exit a parse tree produced by ProtobufParser#extensionElement.
    def exitExtensionElement(self, ctx:ProtobufParser.ExtensionElementContext):
        pass


    # Enter a parse tree produced by ProtobufParser#extensionFieldDecl.
    def enterExtensionFieldDecl(self, ctx:ProtobufParser.ExtensionFieldDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#extensionFieldDecl.
    def exitExtensionFieldDecl(self, ctx:ProtobufParser.ExtensionFieldDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#serviceDecl.
    def enterServiceDecl(self, ctx:ProtobufParser.ServiceDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#serviceDecl.
    def exitServiceDecl(self, ctx:ProtobufParser.ServiceDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#serviceName.
    def enterServiceName(self, ctx:ProtobufParser.ServiceNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#serviceName.
    def exitServiceName(self, ctx:ProtobufParser.ServiceNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#serviceElement.
    def enterServiceElement(self, ctx:ProtobufParser.ServiceElementContext):
        pass

    # Exit a parse tree produced by ProtobufParser#serviceElement.
    def exitServiceElement(self, ctx:ProtobufParser.ServiceElementContext):
        pass


    # Enter a parse tree produced by ProtobufParser#methodDecl.
    def enterMethodDecl(self, ctx:ProtobufParser.MethodDeclContext):
        pass

    # Exit a parse tree produced by ProtobufParser#methodDecl.
    def exitMethodDecl(self, ctx:ProtobufParser.MethodDeclContext):
        pass


    # Enter a parse tree produced by ProtobufParser#methodName.
    def enterMethodName(self, ctx:ProtobufParser.MethodNameContext):
        pass

    # Exit a parse tree produced by ProtobufParser#methodName.
    def exitMethodName(self, ctx:ProtobufParser.MethodNameContext):
        pass


    # Enter a parse tree produced by ProtobufParser#inputType.
    def enterInputType(self, ctx:ProtobufParser.InputTypeContext):
        pass

    # Exit a parse tree produced by ProtobufParser#inputType.
    def exitInputType(self, ctx:ProtobufParser.InputTypeContext):
        pass


    # Enter a parse tree produced by ProtobufParser#outputType.
    def enterOutputType(self, ctx:ProtobufParser.OutputTypeContext):
        pass

    # Exit a parse tree produced by ProtobufParser#outputType.
    def exitOutputType(self, ctx:ProtobufParser.OutputTypeContext):
        pass


    # Enter a parse tree produced by ProtobufParser#methodElement.
    def enterMethodElement(self, ctx:ProtobufParser.MethodElementContext):
        pass

    # Exit a parse tree produced by ProtobufParser#methodElement.
    def exitMethodElement(self, ctx:ProtobufParser.MethodElementContext):
        pass


    # Enter a parse tree produced by ProtobufParser#messageType.
    def enterMessageType(self, ctx:ProtobufParser.MessageTypeContext):
        pass

    # Exit a parse tree produced by ProtobufParser#messageType.
    def exitMessageType(self, ctx:ProtobufParser.MessageTypeContext):
        pass


    # Enter a parse tree produced by ProtobufParser#identifier.
    def enterIdentifier(self, ctx:ProtobufParser.IdentifierContext):
        pass

    # Exit a parse tree produced by ProtobufParser#identifier.
    def exitIdentifier(self, ctx:ProtobufParser.IdentifierContext):
        pass


    # Enter a parse tree produced by ProtobufParser#alwaysIdent.
    def enterAlwaysIdent(self, ctx:ProtobufParser.AlwaysIdentContext):
        pass

    # Exit a parse tree produced by ProtobufParser#alwaysIdent.
    def exitAlwaysIdent(self, ctx:ProtobufParser.AlwaysIdentContext):
        pass


    # Enter a parse tree produced by ProtobufParser#sometimesIdent.
    def enterSometimesIdent(self, ctx:ProtobufParser.SometimesIdentContext):
        pass

    # Exit a parse tree produced by ProtobufParser#sometimesIdent.
    def exitSometimesIdent(self, ctx:ProtobufParser.SometimesIdentContext):
        pass



del ProtobufParser