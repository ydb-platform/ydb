"""
This file contains the lexer class that is used to tokenize a given SCIM
filter query.

See https://tools.ietf.org/html/rfc7644#section-3.4.2.2 for more details.

::

   The operators supported in the expression are listed in Table 3.

   +----------+-------------+------------------------------------------+
   | Operator | Description | Behavior                                 |
   +----------+-------------+------------------------------------------+
   | eq       | equal       | The attribute and operator values must   |
   |          |             | be identical for a match.                |
   |          |             |                                          |
   | ne       | not equal   | The attribute and operator values are    |
   |          |             | not identical.                           |
   |          |             |                                          |
   | co       | contains    | The entire operator value must be a      |
   |          |             | substring of the attribute value for a   |
   |          |             | match.                                   |
   |          |             |                                          |
   | sw       | starts with | The entire operator value must be a      |
   |          |             | substring of the attribute value,        |
   |          |             | starting at the beginning of the         |
   |          |             | attribute value.  This criterion is      |
   |          |             | satisfied if the two strings are         |
   |          |             | identical.                               |
   |          |             |                                          |
   | ew       | ends with   | The entire operator value must be a      |
   |          |             | substring of the attribute value,        |
   |          |             | matching at the end of the attribute     |
   |          |             | value.  This criterion is satisfied if   |
   |          |             | the two strings are identical.           |
   |          |             |                                          |
   | pr       | present     | If the attribute has a non-empty or      |
   |          | (has value) | non-null value, or if it contains a      |
   |          |             | non-empty node for complex attributes,   |
   |          |             | there is a match.                        |
   |          |             |                                          |
   | gt       | greater     | If the attribute value is greater than   |
   |          | than        | the operator value, there is a match.    |
   |          |             | The actual comparison is dependent on    |
   |          |             | the attribute type.  For string          |
   |          |             | attribute types, this is a               |
   |          |             | lexicographical comparison, and for      |
   |          |             | DateTime types, it is a chronological    |
   |          |             | comparison.  For integer attributes, it  |
   |          |             | is a comparison by numeric value.        |
   |          |             | Boolean and Binary attributes SHALL      |
   |          |             | cause a failed response (HTTP status     |
   |          |             | code 400) with "scimType" of             |
   |          |             | "invalidFilter".                         |
   |          |             |                                          |
   | ge       | greater     | If the attribute value is greater than   |
   |          | than or     | or equal to the operator value, there is |
   |          | equal to    | a match.  The actual comparison is       |
   |          |             | dependent on the attribute type.  For    |
   |          |             | string attribute types, this is a        |
   |          |             | lexicographical comparison, and for      |
   |          |             | DateTime types, it is a chronological    |
   |          |             | comparison.  For integer attributes, it  |
   |          |             | is a comparison by numeric value.        |
   |          |             | Boolean and Binary attributes SHALL      |
   |          |             | cause a failed response (HTTP status     |
   |          |             | code 400) with "scimType" of             |
   |          |             | "invalidFilter".                         |
   |          |             |                                          |
   | lt       | less than   | If the attribute value is less than the  |
   |          |             | operator value, there is a match.  The   |
   |          |             | actual comparison is dependent on the    |
   |          |             | attribute type.  For string attribute    |
   |          |             | types, this is a lexicographical         |
   |          |             | comparison, and for DateTime types, it   |
   |          |             | is a chronological comparison.  For      |
   |          |             | integer attributes, it is a comparison   |
   |          |             | by numeric value.  Boolean and Binary    |
   |          |             | attributes SHALL cause a failed response |
   |          |             | (HTTP status code 400) with "scimType"   |
   |          |             | of "invalidFilter".                      |
   |          |             |                                          |
   | le       | less than   | If the attribute value is less than or   |
   |          | or equal to | equal to the operator value, there is a  |
   |          |             | match.  The actual comparison is         |
   |          |             | dependent on the attribute type.  For    |
   |          |             | string attribute types, this is a        |
   |          |             | lexicographical comparison, and for      |
   |          |             | DateTime types, it is a chronological    |
   |          |             | comparison.  For integer attributes, it  |
   |          |             | is a comparison by numeric value.        |
   |          |             | Boolean and Binary attributes SHALL      |
   |          |             | cause a failed response (HTTP status     |
   |          |             | code 400) with "scimType" of             |
   |          |             | "invalidFilter".                         |
   +----------+-------------+------------------------------------------+

                       Table 3: Attribute Operators

   +----------+-------------+------------------------------------------+
   | Operator | Description | Behavior                                 |
   +----------+-------------+------------------------------------------+
   | and      | Logical     | The filter is only a match if both       |
   |          | "and"       | expressions evaluate to true.            |
   |          |             |                                          |
   | or       | Logical     | The filter is a match if either          |
   |          | "or"        | expression evaluates to true.            |
   |          |             |                                          |
   | not      | "Not"       | The filter is a match if the expression  |
   |          | function    | evaluates to false.                      |
   +----------+-------------+------------------------------------------+

                        Table 4: Logical Operators


   +----------+-------------+------------------------------------------+
   | Operator | Description | Behavior                                 |
   +----------+-------------+------------------------------------------+
   | ( )      | Precedence  | Boolean expressions MAY be grouped using |
   |          | grouping    | parentheses to change the standard order |
   |          |             | of operations, i.e., to evaluate logical |
   |          |             | "or" operators before logical "and"      |
   |          |             | operators.                               |
   |          |             |                                          |
   | [ ]      | Complex     | Service providers MAY support complex    |
   |          | attribute   | filters where expressions MUST be        |
   |          | filter      | applied to the same value of a parent    |
   |          | grouping    | attribute specified immediately before   |
   |          |             | the left square bracket ("[").  The      |
   |          |             | expression within square brackets ("["   |
   |          |             | and "]") MUST be a valid filter          |
   |          |             | expression based upon sub-attributes of  |
   |          |             | the parent attribute.  Nested            |
   |          |             | expressions MAY be used.  See examples   |
   |          |             | below.                                   |
   +----------+-------------+------------------------------------------+

                        Table 5: Grouping Operators

"""

# ruff: noqa: F821
from sly import Lexer


class SCIMLexer(Lexer):
    tokens = {
        # Attribute Operators
        EQ,
        NE,
        GT,
        GE,
        LT,
        LE,
        CO,
        SW,
        EW,
        PR,
        # Logical Operators
        AND,
        OR,
        NOT,
        FALSE,
        TRUE,
        NULL,
        # Grouping Operators
        LPAREN,
        RPAREN,
        LBRACKET,
        RBRACKET,
        # Other
        NUMBER,
        COMP_VALUE,
        ATTRNAME,
        SCHEMA_URI,
        SUBATTR,
    }

    # Filters MUST be evaluated using the following order of operations, in
    # order of precedence:
    # 1.  Grouping operators
    # 2.  Logical operators - where "not" takes precedence over "and",
    #     which takes precedence over "or"
    # 3.  Attribute operators

    ignore = " \t"

    @_(r"\.[a-zA-Z$][a-zA-Z0-9_$-]*")
    def SUBATTR(self, t):
        t.value = t.value.lstrip(".")
        return t

    # Grouping Operators
    LPAREN = r"\("
    RPAREN = r"\)"
    LBRACKET = r"\["
    RBRACKET = r"\]"

    # compValue literals
    # false / null / true / number / string
    # Rules from https://tools.ietf.org/html/rfc7159
    FALSE = r"false"
    TRUE = r"true"
    NULL = r"null"
    NUMBER = r"[0-9]"  # only support integers at this time

    # attrPath parts
    @_(r"[a-zA-Z]+:[a-zA-Z0-9:\._-]+:")
    def SCHEMA_URI(self, t):
        t.value = t.value.rstrip(":")
        return t

    # "$" is not allowed as part of an ATTRNAME per RFC 7643. It is allowed
    # here so that ATTRNAME can be used in tokenization of a complex query
    # without further complicating the parsing logic with complex query
    # specific tokens.
    ATTRNAME = r"[a-zA-Z$][a-zA-Z0-9_$-]*"

    # Attribute Operators
    ATTRNAME["eq"] = EQ
    ATTRNAME["Eq"] = EQ
    ATTRNAME["eQ"] = EQ
    ATTRNAME["EQ"] = EQ

    ATTRNAME["ne"] = NE
    ATTRNAME["Ne"] = NE
    ATTRNAME["nE"] = NE
    ATTRNAME["NE"] = NE

    ATTRNAME["co"] = CO
    ATTRNAME["Co"] = CO
    ATTRNAME["cO"] = CO
    ATTRNAME["CO"] = CO

    ATTRNAME["sw"] = SW
    ATTRNAME["Sw"] = SW
    ATTRNAME["sW"] = SW
    ATTRNAME["SW"] = SW

    ATTRNAME["ew"] = EW
    ATTRNAME["Ew"] = EW
    ATTRNAME["eW"] = EW
    ATTRNAME["EW"] = EW

    ATTRNAME["pr"] = PR
    ATTRNAME["Pr"] = PR
    ATTRNAME["pR"] = PR
    ATTRNAME["PR"] = PR

    ATTRNAME["gt"] = GT
    ATTRNAME["Gt"] = GT
    ATTRNAME["gT"] = GT
    ATTRNAME["GT"] = GT

    ATTRNAME["ge"] = GE
    ATTRNAME["Ge"] = GE
    ATTRNAME["gE"] = GE
    ATTRNAME["GE"] = GE

    ATTRNAME["lt"] = LT
    ATTRNAME["Lt"] = LT
    ATTRNAME["lT"] = LT
    ATTRNAME["LT"] = LT

    ATTRNAME["le"] = LE
    ATTRNAME["Le"] = LE
    ATTRNAME["lE"] = LE
    ATTRNAME["LE"] = LE

    # Logical Operators
    ATTRNAME["and"] = AND
    ATTRNAME["And"] = AND
    ATTRNAME["aNd"] = AND
    ATTRNAME["ANd"] = AND
    ATTRNAME["anD"] = AND
    ATTRNAME["AnD"] = AND
    ATTRNAME["aND"] = AND
    ATTRNAME["AND"] = AND

    ATTRNAME["or"] = OR
    ATTRNAME["Or"] = OR
    ATTRNAME["oR"] = OR
    ATTRNAME["OR"] = OR

    ATTRNAME["not"] = NOT
    ATTRNAME["Not"] = NOT
    ATTRNAME["nOt"] = NOT
    ATTRNAME["NOt"] = NOT
    ATTRNAME["noT"] = NOT
    ATTRNAME["NoT"] = NOT
    ATTRNAME["nOT"] = NOT
    ATTRNAME["NOT"] = NOT

    @_(r'"([^"]*)"')
    def COMP_VALUE(self, t):
        t.value = t.value.strip('"')
        return t

    def error(self, t):
        raise ValueError(f"Illegal character in filter query '{t.value[0]}'")


def main(argv=None):
    """
    Main program. Used for testing.
    """
    import argparse
    import sys

    argv = argv or sys.argv[1:]

    parser = argparse.ArgumentParser("SCIM 2.0 Filter Parser Lexer")
    parser.add_argument("filter", help="""Eg. 'userName eq "bjensen"'""")
    args = parser.parse_args(argv)

    token_stream = SCIMLexer().tokenize(args.filter)
    for token in token_stream:
        print(token)


if __name__ == "__main__":
    main()
