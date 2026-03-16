from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum as PyEnum
from typing import List, Union


class FieldCardinality(str, PyEnum):
    REQUIRED = "REQUIRED"
    OPTIONAL = "OPTIONAL"
    REPEATED = "REPEATED"


# file: BYTE_ORDER_MARK? syntaxDecl? fileElement* EOF;
@dataclass
class File:
    syntax: Union[str, None] = None
    file_elements: List[FileElement] = field(default_factory=list)


# commentDecl: (LINE_COMMENT | BLOCK_COMMENT)
@dataclass
class Comment:
    text: str


# packageDecl: PACKAGE packageName SEMICOLON;
@dataclass
class Package:
    name: str


# importDecl: IMPORT ( WEAK | PUBLIC )? importedFileName SEMICOLON;
@dataclass
class Import:
    name: str
    weak: bool = False
    public: bool = False


@dataclass
class MessageLiteralField:
    name: str
    value: MessageValue


@dataclass
class MessageLiteral:
    fields: List[MessageLiteralField] = field(default_factory=list)


# optionDecl: OPTION optionName EQUALS optionValue SEMICOLON;
@dataclass
@dataclass
class Option:
    name: str
    value: Union[ScalarValue, MessageLiteral]


# messageDecl: MESSAGE messageName L_BRACE messageElement* R_BRACE;
@dataclass
class Message:
    name: str
    elements: List[MessageElement] = field(default_factory=list)


# messageFieldDecl: fieldDeclWithCardinality |
#                   messageFieldDeclTypeName fieldName EQUALS fieldNumber
#                        compactOptions? SEMICOLON;
@dataclass
class Field:
    name: str
    number: int
    type: str
    cardinality: Union[FieldCardinality, None] = None
    options: List[Option] = field(default_factory=list)


# mapFieldDecl: mapType fieldName EQUALS fieldNumber compactOptions? SEMICOLON;
# TODO Seems like the Buf .g4 grammar doesn't allow for cardinality on map fields?
@dataclass
class MapField:
    name: str
    number: int
    key_type: str
    value_type: str
    options: List[Option] = field(default_factory=list)


# groupDecl: fieldCardinality? GROUP fieldName EQUALS fieldNumber
#              compactOptions? L_BRACE messageElement* R_BRACE;
@dataclass
class Group:
    name: str
    number: int
    cardinality: Union[FieldCardinality, None] = None
    elements: List[MessageElement] = field(default_factory=list)


# oneofDecl: ONEOF oneofName L_BRACE oneofElement* R_BRACE;
@dataclass
class OneOf:
    name: str
    elements: List[OneOfElement] = field(default_factory=list)


# extensionRangeDecl: EXTENSIONS tagRanges compactOptions? SEMICOLON;
@dataclass
class ExtensionRange:
    ranges: List[str]
    options: List[Option] = field(default_factory=list)


# messageReservedDecl: RESERVED ( tagRanges | names ) SEMICOLON;
@dataclass
class Reserved:
    ranges: List[str] = field(default_factory=list)
    names: List[str] = field(default_factory=list)


# enumDecl: ENUM enumName L_BRACE enumElement* R_BRACE;
@dataclass
class Enum:
    name: str
    elements: List[EnumElement] = field(default_factory=list)


# enumValueDecl: enumValueName EQUALS enumValueNumber compactOptions? SEMICOLON;
@dataclass
class EnumValue:
    name: str
    number: int
    options: List[Option] = field(default_factory=list)


# enumReservedDecl: RESERVED ( enumValueRanges | names ) SEMICOLON;
@dataclass
class EnumReserved:
    ranges: List[str] = field(default_factory=list)
    names: List[str] = field(default_factory=list)


# extensionDecl: EXTEND extendedMessage L_BRACE extensionElement* R_BRACE;
@dataclass
class Extension:
    typeName: str
    elements: List[ExtensionElement] = field(default_factory=list)


# serviceDecl: SERVICE serviceName L_BRACE serviceElement* R_BRACE;
@dataclass
class Service:
    name: str
    elements: List[ServiceElement] = field(default_factory=list)


# methodDecl: RPC methodName inputType RETURNS outputType SEMICOLON |
#             RPC methodName inputType RETURNS outputType L_BRACE methodElement* R_BRACE;
@dataclass
class Method:
    name: str
    input_type: MessageType
    output_type: MessageType
    elements: List[MethodElement] = field(default_factory=list)


# messageType: L_PAREN STREAM? methodDeclTypeName R_PAREN;
@dataclass
class MessageType:
    type: str
    stream: bool = False


# IDENTIFIER: LETTER ( LETTER | DECIMAL_DIGIT )*;
@dataclass
class Identifier:
    """
    Identifier is a simple dataclass to represent an unquoted identifier (such
    as an enumerator name). It's used as a value for scalar types that can't be
    parsed into a string, int, float, or bool.
    """

    name: str


# fileElement: importDecl |
#                packageDecl |
#                optionDecl |
#                messageDecl |
#                enumDecl |
#                extensionDecl |
#                serviceDecl |
#                commentDecl |
#                emptyDecl;
FileElement = Union[Import, Package, Option, Message, Enum, Extension, Service, Comment]

# messageElement: messageFieldDecl |
#                   groupDecl |
#                   oneofDecl |
#                   optionDecl |
#                   extensionRangeDecl |
#                   messageReservedDecl |
#                   messageDecl |
#                   enumDecl |
#                   extensionDecl |
#                   mapFieldDecl |
#                   emptyDecl |
#                   commentDecl;
MessageElement = Union[
    Comment,
    Field,
    Group,
    OneOf,
    Option,
    ExtensionRange,
    Reserved,
    Message,
    Enum,
    Extension,
    MapField,
]

# oneofElement: optionDecl |
#                 oneofFieldDecl |
#                 oneofGroupDecl |
#                 commentDecl;
OneOfElement = Union[Option, Field, Group, Comment]

# enumElement: optionDecl |
#                enumValueDecl |
#                enumReservedDecl |
#                emptyDecl |
#                commentDecl;
EnumElement = Union[Option, EnumValue, EnumReserved, Comment]

# extensionElement: extensionFieldDecl |
#                     groupDecl;
ExtensionElement = Union[Field, Group, Comment]

# serviceElement: optionDecl |
#                   methodDecl |
#                   commentDecl |
#                   emptyDecl;
ServiceElement = Union[Option, Method, Comment]

# methodElement: optionDecl |
#                 commentDecl |
#                 emptyDecl;
MethodElement = Union[Option, Comment]

# Define a type alias for scalar values
ScalarValue = Union[str, int, float, bool, Identifier]

# Define a recursive type alias for message values
MessageValue = Union[ScalarValue, MessageLiteral, List["MessageValue"]]
