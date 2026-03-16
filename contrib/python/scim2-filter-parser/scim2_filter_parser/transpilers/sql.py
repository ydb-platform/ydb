"""
The logic in this module builds a portion of a WHERE SQL
clause based on a SCIM filter.
"""

import ast
import collections
import string

from .. import ast as scim2ast

AttrPath = collections.namedtuple("AttrPath", ["attr_name", "sub_attr", "uri"])


class Transpiler(ast.NodeTransformer):
    """
    Transpile a SCIM AST into a SQL WHERE clause (not including the "WHERE" keyword)
    """

    binary_op_by_scim_op = {
        "eq": "=",
        "ne": "!=",
        "co": "LIKE",
        "sw": "LIKE",
        "ew": "LIKE",
        "pr": "IS NOT NULL",
        "gt": ">",
        "ge": ">=",
        "lt": "<",
        "le": "<=",
    }

    matching_op_by_scim_op = {
        "co": ("%", "%"),
        "sw": ("", "%"),
        "ew": ("%", ""),
    }

    def __init__(self, attr_map, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.attr_map = attr_map
        self.params = {}
        self.attr_paths = []

    def transpile(self, ast) -> (str, dict):
        sql = self.visit(ast)

        return sql, self.params

    def visit_Filter(self, node):
        if node.namespace:
            # push the namespace from value path down the tree
            if isinstance(node.expr, scim2ast.Filter):
                node.expr = scim2ast.Filter(
                    node.expr.expr, node.expr.negated, node.namespace
                )
            elif isinstance(node.expr, scim2ast.LogExpr):
                expr1 = scim2ast.Filter(
                    node.expr.expr1.expr, node.expr.expr1.negated, node.namespace
                )
                expr2 = scim2ast.Filter(
                    node.expr.expr2.expr, node.expr.expr2.negated, node.namespace
                )
                node.expr = scim2ast.LogExpr(node.expr.op, expr1, expr2)
            elif isinstance(node.expr, scim2ast.AttrExpr):
                # namespace takes place of previous attr_name in attr_path
                sub_attr = scim2ast.SubAttr(node.expr.attr_path.attr_name)
                attr_path = scim2ast.AttrPath(
                    node.namespace.attr_name, sub_attr, node.expr.attr_path.uri
                )
                node.expr = scim2ast.AttrExpr(
                    node.expr.value, attr_path, node.expr.comp_value
                )
            else:
                raise NotImplementedError(f"Node {node} can not pass on namespace")

        expr = self.visit(node.expr)

        if expr and node.negated:
            expr = f"NOT ({expr})"

        return expr

    def visit_LogExpr(self, node):
        expr1 = self.visit(node.expr1)
        expr2 = self.visit(node.expr2)
        op = node.op.upper()

        if expr1 and expr2:
            return f"({expr1}) {op} ({expr2})"
        elif expr1:
            return expr1
        elif expr2:
            return expr2
        else:
            return None

    def visit_PartialAttrExpr(self, node):
        """
        Dissect rather complex queries like the following::

            emails[type eq "Primary"].value eq "001750ca-8202-47cd-b553-c63f4f245940"

        First we restructure to something like this::

            emails.value[type eq "Primary"] eq "001750ca-8202-47cd-b553-c63f4f245940"

        Then we get SQL like this 'emails.type = {0}' and 'emails.value'.

        We need to take these two snippets and AND them together.
        """
        # visit full filter first and restructure AST
        # ie. visit -> 'emails.type = {0}'
        full = self.visit(node)

        # get second part of query
        # ie. visit -> 'emails.value'
        partial = self.visit(node.namespace)

        return full, partial

    def visit_AttrExpr(self, node):
        if isinstance(node.attr_path.attr_name, scim2ast.Filter):
            full, partial = self.visit_PartialAttrExpr(node.attr_path.attr_name)
            if full and partial:
                value = self.visit_AttrExprValue(node)
                return f"({full} AND {partial} {value})"
            elif full:
                return full
            elif partial:
                value = self.visit_AttrExprValue(node)
                return f"{partial} {value}"
            else:
                return None
        else:
            # Case-insensitivity only needs to be checked in this branch
            # because userName is currently the only attribute that can be case
            # insensitive and userName can not be a nested part of a complex query (eg.
            # emails.type in emails[type eq "Primary"]...).
            # https://datatracker.ietf.org/doc/html/rfc7643#section-4.1.1
            attr = self.visit(node.attr_path)
            if attr is None:
                return None

            value = self.visit_AttrExprValue(node)

            if node.case_insensitive:
                return f"UPPER({attr}) {value}"

            return f"{attr} {value}"

    def visit_AttrExprValue(self, node):
        op_sql = self.lookup_op(node.value)

        item_id = self.get_next_id()

        if not node.comp_value:
            self.params[item_id] = None
            return op_sql

        # There is a comp_value, so visit node and build SQL.

        # prep item_id to be a str replacement placeholder
        item_id_placeholder = "{" + item_id + "}"

        if node.value.lower() in self.matching_op_by_scim_op.keys():
            # Add appropriate % signs to values in LIKE clause
            prefix, suffix = self.lookup_like_matching(node.value)
            value = prefix + self.visit(node.comp_value) + suffix

        else:
            value = self.visit(node.comp_value)

        self.params[item_id] = value

        if node.case_insensitive:
            return f"{op_sql} UPPER({item_id_placeholder})"

        return f"{op_sql} {item_id_placeholder}"

    def visit_AttrPath(self, node):
        attr_name_value = node.attr_name

        sub_attr_value = None
        if node.sub_attr:
            sub_attr_value = node.sub_attr.value

        uri_value = None
        if node.uri:
            uri_value = node.uri

        # Convert attr_name to another value based on map.
        # Otherwise, return None.
        attr_path_tuple = AttrPath(attr_name_value, sub_attr_value, uri_value)
        self.attr_paths.append(attr_path_tuple)
        return self.attr_map.get(attr_path_tuple)

    def visit_CompValue(self, node):
        if node.value in ("true", "false", "null"):
            return node.value.upper()

        # TODO: Handle timestamps!

        return node.value

    def get_next_id(self):
        index = len(self.params)
        if index >= len(string.ascii_lowercase):
            raise IndexError("Too many params in query. Can not store all of them.")
        return string.ascii_lowercase[index]

    def lookup_op(self, node_value):
        op_code = node_value.lower()

        sql = self.binary_op_by_scim_op.get(op_code)

        if not sql:
            raise ValueError(f"Unknown SQL op {op_code}")

        return sql or node_value

    def lookup_like_matching(self, node_value):
        op_code = node_value.lower()

        sql = self.matching_op_by_scim_op.get(op_code)

        if not sql:
            raise ValueError(f"Unknown SQL LIKE op {op_code}")

        return sql


def main(argv=None):
    """
    Main program. Used for testing.
    """
    import argparse
    import sys

    from scim2_filter_parser.lexer import SCIMLexer
    from scim2_filter_parser.parser import SCIMParser

    argv = argv or sys.argv[1:]

    parser = argparse.ArgumentParser("SCIM 2.0 Filter Parser Transpiler")
    parser.add_argument("filter", help="""Eg. 'userName eq "bjensen"'""")
    args = parser.parse_args(argv)

    token_stream = SCIMLexer().tokenize(args.filter)
    ast = SCIMParser().parse(token_stream)
    attr_map = {
        ("name", "familyname", None): "name.familyname",
        ("emails", None, None): "emails",
        ("emails", "type", None): "emails.type",
        ("emails", "value", None): "emails.value",
        ("userName", None, None): "username",
        ("title", None, None): "title",
        ("userType", None, None): "usertype",
        ("schemas", None, None): "schemas",
        ("userName", None, "urn:ietf:params:scim:schemas:core:2.0:User"): "username",
        ("meta", "lastModified", None): "meta.lastmodified",
        ("ims", "type", None): "ims.type",
        ("ims", "value", None): "ims.value",
    }
    sql, params = Transpiler(attr_map).transpile(ast)

    print("SQL:", sql)
    print("PARAMS:", params)


if __name__ == "__main__":
    main()
