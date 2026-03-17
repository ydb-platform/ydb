# Copyright (c) 2013-2021, Manfred Moitzi
# License: MIT-License
# mypy: ignore_errors=True
"""
EntityQueryParser implemented with the pyparsing module created by Paul T. McGuire.

QueryString := EntityQuery ("[" AttribQuery "]" "i"?)*

The QueryString consist of two queries, first the required entity query and the second optional attribute query,
enclosed in square brackets an optional appended "i" indicates ignore case for string queries.

1. EntityQuery (required)

The EntityQuery is a whitespace separated list of names (DXF entity names) or the special name "*".

2. AttribQuery (optional)

The AttribQuery is a boolean expression, supported boolean operators are:

  - '!' not: !term if true, if tern is false.
  - '&' and: term & term is true, if both terms are true.
  - '|' or: term | term is true, if one term is true.

The query itself consist of a name a comparator and a value, like "color < 7".
Supported comparators are:

  - '==': equal
  - '!=': not equal
  - '<': lower than
  - '<=': lower or equal than
  - '>': greater than
  - '>=': greater or equal than
  - '?': match a regular expression
  - '!?': does not match a regular expression

Values can be integers, floats or strings, strings have to be quoted ("I am a string" or 'I am a string').

examples:
    'LINE CIRCLE[layer=="construction"]' => all LINE and CIRCLE entities on layer "construction"
    '*[!(layer=="construction" & color<7)]' => all entities except those on layer == "construction" and color < 7
    'LINE[layer=="construction"]i' => all lines on layer named 'construction' with case insensitivity, "CoNsTrUcTiOn" is valid
"""

from pyparsing import *

LBRK = Suppress("[")
RBRK = Suppress("]")

number = Regex(r"[+-]?\d+(:?\.\d*)?(:?[eE][+-]?\d+)?")
number.addParseAction(lambda t: float(t[0]))  # convert to float
string_ = quotedString.addParseAction(lambda t: t[0][1:-1])  # remove quotes

EntityName = Word(alphanums + "_")
# ExcludeEntityName = Word(alphanums + '_!')
ExcludeEntityName = Regex(r"[!][\w]+")
AttribName = Word(alphanums + "_")
Relation = oneOf(["==", "!=", "<", "<=", ">", ">=", "?", "!?"])

AttribValue = string_ | number
AttribQuery = Group(AttribName + Relation + AttribValue)
EntityNames = Group(
    (Literal("*") + ZeroOrMore(ExcludeEntityName)) | OneOrMore(EntityName)
).setResultsName("EntityQuery")

InfixBoolQuery = infixNotation(
    AttribQuery,
    (
        ("!", 1, opAssoc.RIGHT),
        ("&", 2, opAssoc.LEFT),
        ("|", 2, opAssoc.LEFT),
    ),
).setResultsName("AttribQuery")

AttribQueryOptions = Literal("i").setResultsName("AttribQueryOptions")

EntityQueryParser = EntityNames + Optional(
    LBRK + InfixBoolQuery + RBRK + Optional(AttribQueryOptions)
)
