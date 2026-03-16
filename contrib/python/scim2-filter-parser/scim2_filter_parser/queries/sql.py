"""
The logic in this module builds a full SQL query based on a SCIM filter.
"""

from ..lexer import SCIMLexer
from ..parser import SCIMParser
from ..transpilers.sql import Transpiler


class SQLQuery:
    placeholder = "%s"

    def __init__(self, filter_, table_name, attr_map, joins=()):
        self.filter: str = filter_
        self.table_name: str = table_name
        self.attr_map: dict = attr_map
        self.joins = joins
        self.where_sql: str = None
        self.params_dict: dict = {}

        self.token_stream = None
        self.ast = None
        self.transpiler = None

        self.build_where_sql()

    def build_where_sql(self):
        self.token_stream = SCIMLexer().tokenize(self.filter)
        self.ast = SCIMParser().parse(self.token_stream)
        self.transpiler = Transpiler(self.attr_map)
        self.where_sql, self.params_dict = self.transpiler.transpile(self.ast)

    @property
    def params(self):
        return [
            self.params_dict[k]
            for k, _v in sorted(self.params_dict.items())
            if self.params_dict[k] is not None
        ]

    @property
    def sql(self) -> str:
        lines = [
            f"SELECT DISTINCT {self.table_name}.*",
            f"FROM {self.table_name}",
        ]

        if self.joins:
            lines.extend(self.joins)

        # Replace {#} with placeholder string. Different database
        # connectors can override this with their own placeholder character.
        placeholders = {i: self.placeholder for i in self.params_dict}

        if self.where_sql:
            where_sql = self.where_sql.format(**placeholders)
            lines.append(f"WHERE {where_sql}")

        lines[-1] += ";"  # Complete all SQL with semicolon

        return "\n".join("    " + line for line in lines)

    def __str__(self) -> str:
        orig_placeholder = self.placeholder
        self.placeholder = "{}"
        sql = self.sql.format(*self.params)
        self.placeholder = orig_placeholder

        # Wrap the SQL in invalid characters so users don't accidentally
        # walk into a SQL Injection vulnerability.
        return "\n".join(
            (
                ">>> DO NOT USE THIS OUTPUT DIRECTLY",
                ">>> SQL INJECTION ATTACK RISK",
                ">>> SQL PREVIEW:",
                sql,
            )
        )


class SQLiteQuery(SQLQuery):
    placeholder = "?"


def main(argv=None):
    """
    Main program. Used for testing.
    """
    import argparse
    import sys

    argv = argv or sys.argv[1:]

    parser = argparse.ArgumentParser("SCIM 2.0 Filter Parser Transpiler")
    parser.add_argument("filter", help="""Eg. 'userName eq "bjensen"'""")
    args = parser.parse_args(argv)

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

    joins = (
        "LEFT JOIN emails ON emails.user_id = users.id",
        "LEFT JOIN schemas ON schemas.user_id = users.id",
    )

    q = SQLiteQuery(args.filter, "users", attr_map, joins)

    print(q)


if __name__ == "__main__":
    main()
