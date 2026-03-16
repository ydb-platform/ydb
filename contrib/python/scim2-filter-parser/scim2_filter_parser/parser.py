"""
This file defines the parser class that is used to parse a given SCIM query.

See https://tools.ietf.org/html/rfc7644#section-3.4.2.2 for more details

::

    SCIM filters MUST conform to the following ABNF [RFC5234] rules as
    specified below:

      FILTER    = attrExp / logExp / valuePath / *1"not" "(" FILTER ")"

      valuePath = attrPath "[" valFilter "]"
                  ; FILTER uses sub-attributes of a parent attrPath

      valFilter = attrExp / logExp / *1"not" "(" valFilter ")"

      attrExp   = (attrPath SP "pr") /
                  (attrPath SP compareOp SP compValue)

      logExp    = FILTER SP ("and" / "or") SP FILTER

      compValue = false / null / true / number / string
                  ; rules from JSON (RFC 7159)

      compareOp = "eq" / "ne" / "co" /
                         "sw" / "ew" /
                         "gt" / "lt" /
                         "ge" / "le"

      attrPath  = [URI ":"] ATTRNAME *1subAttr
                  ; SCIM attribute name
                  ; URI is SCIM "schema" URI

      ATTRNAME  = ALPHA *(nameChar)

      nameChar  = "-" / "_" / DIGIT / ALPHA

      subAttr   = "." ATTRNAME
                  ; a sub-attribute of a complex attribute

                Figure 1: ABNF Specification of SCIM Filters

    In the above ABNF rules, the "compValue" (comparison value) rule is
    built on JSON Data Interchange format ABNF rules as specified in
    [RFC7159], "DIGIT" and "ALPHA" are defined per Appendix B.1 of
    [RFC5234], and "URI" is defined per Appendix A of [RFC3986].

    Filters MUST be evaluated using the following order of operations, in
    order of precedence:

    1.  Grouping operators

    2.  Logical operators - where "not" takes precedence over "and",
        which takes precedence over "or"

    3.  Attribute operators

    If the specified attribute in a filter expression is a multi-valued
    attribute, the filter matches if any of the values of the specified
    attribute match the specified criterion; e.g., if a User has multiple
    "emails" values, only one has to match for the entire User to match.
    For complex attributes, a fully qualified sub-attribute MUST be
    specified using standard attribute notation (Section 3.10).  For
    example, to filter by userName, the parameter value is "userName".
    To filter by first name, the parameter value is "name.givenName".

    When applying a comparison (e.g., "eq") or presence filter (e.g.,
    "pr") to a defaulted attribute, the service provider SHALL use the
    value that was returned to the client that last created or modified
    the attribute.

    Providers MAY support additional filter operations if they choose.
    Providers MUST decline to filter results if the specified filter
    operation is not recognized and return an HTTP 400 error with a
    "scimType" error of "invalidFilter" and an appropriate human-readable
    response as per Section 3.12.  For example, if a client specified an
    unsupported operator named 'regex', the service provider should
    specify an error response description identifying the client error,
    e.g., 'The operator 'regex' is not supported.'

    When comparing attributes of type String, the case sensitivity for
    String type attributes SHALL be determined by the attribute's
    "caseExact" characteristic (see Section 2.2 of [RFC7643]).

    Clients MAY query by schema or schema extensions by using a filter
    expression including the "schemas" attribute (as shown in Figure 2).
"""

from sly import Parser

from . import ast, lexer


class SCIMParserError(Exception):
    pass


class SCIMParser(Parser):
    # debugfile = 'debug.log'

    # Same token set as defined in the lexer
    tokens = lexer.SCIMLexer.tokens

    # Operator precedence table.
    #   Filters MUST be evaluated using the following order of operations, in
    #   order of precedence:
    #   1.  Grouping operators
    #   2.  Logical operators - where "not" takes precedence over "and",
    #       which takes precedence over "or"
    #   3.  Attribute operators
    precedence = (
        ("left", OR, AND),  # noqa: F821
        ("right", NOT),  # noqa: F821
    )

    # FILTER    = attrExp / logExp / valuePath / *1"not" "(" FILTER ")"
    #                                           ; 0 or 1 "not"s
    @_("attr_exp")  # noqa: F821
    def filter(self, p):
        return ast.Filter(p.attr_exp, False, None)

    @_("log_exp")  # noqa: F821
    def filter(self, p):  # noqa: F811
        return ast.Filter(p.log_exp, False, None)

    @_("value_path")  # noqa: F821
    def filter(self, p):  # noqa: F811
        return ast.Filter(p.value_path, False, None)

    @_(  # noqa: F821
        "LPAREN filter RPAREN",
        "NOT LPAREN filter RPAREN",
    )
    def filter(self, p):  # noqa: F811
        negate = p[0].lower() == "not"
        return ast.Filter(p.filter, negate, None)

    # valuePath = attrPath "[" valFilter "]"
    #            ; FILTER uses sub-attributes of a parent attrPath
    # valFilter = attrExp / logExp / *1"not" "(" valFilter ")"
    #                               ; 0 or 1 "not"s
    #
    # Since valFilter and Filter have the same structure, we have to
    # parse valFilter as a Filter. Else we'll run into reduce/reduce conflicts.
    #
    # attr_path here acts like a namespace for the filter operation inside
    # the brackets. Thus we need to distribute the namespace to the leaf nodes
    # of the filter node. We will attach attr_path to the filter and use
    # it in the transpiler.
    @_("attr_path LBRACKET filter RBRACKET")  # noqa: F821
    def value_path(self, p):
        return ast.Filter(p.filter, False, p.attr_path)

    # attrExp   = (attrPath SP "pr") /
    #             (attrPath SP compareOp SP compValue)
    @_(  # noqa: F821
        "attr_path PR",
        "attr_path EQ comp_value",
        "attr_path NE comp_value",
        "attr_path CO comp_value",
        "attr_path SW comp_value",
        "attr_path EW comp_value",
        "attr_path GT comp_value",
        "attr_path LT comp_value",
        "attr_path GE comp_value",
        "attr_path LE comp_value",
    )
    def attr_exp(self, p):
        comp_value = p.comp_value if len(p) == 3 else None
        return ast.AttrExpr(p[1], p.attr_path, comp_value)

    # logExp    = FILTER SP ("and" / "or") SP FILTER
    @_(  # noqa: F821
        "filter OR filter",
        "filter AND filter",
    )
    def log_exp(self, p):
        return ast.LogExpr(p[1], p.filter0, p.filter1)

    # compValue = false / null / true / number / string
    #            ; rules from JSON (RFC 7159)
    @_(  # noqa: F821
        "FALSE",
        "NULL",
        "TRUE",
        "NUMBER",
        "COMP_VALUE",
    )
    def comp_value(self, p):
        return ast.CompValue(p[0])

    # attrPath  = [URI ":"] ATTRNAME *1subAttr
    #             ; SCIM attribute name
    #             ; URI is SCIM "schema" URI
    @_(  # noqa: F821
        "ATTRNAME",
        "ATTRNAME sub_attr",
        "SCHEMA_URI ATTRNAME",
        "SCHEMA_URI ATTRNAME sub_attr",
        # Next clause is not in spec but allows us to handle things
        # like 'members[value eq "6784"] eq ""' which are helpful for
        # AttrPath parsing for PATCH calls.
        "value_path",
        # Not sure this last clause is in the ABNF spec
        # but Azure/Microsoft uses it so let's be prepared.
        "value_path sub_attr",
    )
    def attr_path(self, p):
        if len(p) == 1:
            return ast.AttrPath(p[0], None, None)

        elif (
            len(p) == 2
            and isinstance(p[0], ast.Filter)
            and isinstance(p[1], ast.SubAttr)
        ):
            sub_attr = p[0].namespace.sub_attr
            if sub_attr is not None:
                raise SCIMParserError(f"Parsing error at: {p}")

            # For easier transpiling, convert complex queries like so:
            # emails[type eq "Primary"].value -> emails.value[type eq "Primary"]
            p[0].namespace.sub_attr = p[1]

            return ast.AttrPath(p[0], None, None)

        elif len(p) == 2 and isinstance(p[1], ast.SubAttr):
            return ast.AttrPath(p[0], p[1], None)

        elif len(p) == 2:
            return ast.AttrPath(p[1], None, p[0])

        else:
            return ast.AttrPath(p[1], p[2], p[0])

    # subAttr   = "." ATTRNAME
    #             ; a sub-attribute of a complex attribute
    @_("SUBATTR")  # noqa: F821
    def sub_attr(self, p):
        return ast.SubAttr(p[0])

    def error(self, p):
        raise SCIMParserError(f"Parsing error at: {p}")


def main(argv=None):
    """
    Main program. Used for testing.
    """
    import argparse
    import sys

    argv = argv or sys.argv[1:]

    parser = argparse.ArgumentParser("SCIM 2.0 Filter Parser Parser")
    parser.add_argument("filter", help="""Eg. 'userName eq "bjensen"'""")
    args = parser.parse_args(argv)

    token_stream = lexer.SCIMLexer().tokenize(args.filter)
    ast_nodes = SCIMParser().parse(token_stream)

    # Output the resulting parse tree structure
    for depth, node in ast.flatten(ast_nodes):
        print("    " * depth, node)


if __name__ == "__main__":
    main()
