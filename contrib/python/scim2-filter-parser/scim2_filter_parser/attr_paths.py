"""
The logic in this module extracts the path sent in a filter query.

PATCH calls pass a path key-value pair in the payload::

   Valid examples of "path" are as follows:

       "path":"members"

       "path":"name.familyName"

       "path":"addresses[type eq \"work\"]"

       "path":"members[value eq
              \"2819c223-7f76-453a-919d-413861904646\"]"

       "path":"members[value eq
              \"2819c223-7f76-453a-919d-413861904646\"].displayName"

                       Figure 8: Example Path Values

Those paths can be simple or complex. The "valuePath" rule allows specific
values of a complex multi-valued attribute to be selected::

    RFC 7644, Figure 7: SCIM PATCH PATH Rule, ABNF:

        PATH = attrPath / valuePath [subAttr]

The classes in this module parse the overall PATH string for its parts so
that they can be easily used in following code.
"""

import json
import string

from .lexer import SCIMLexer
from .parser import SCIMParser
from .transpilers.sql import Transpiler


class AttrPath:
    """
    This class depends on the SQL transpiler.
    """

    def __init__(self, filter_: str, attr_map: dict):
        """
        Perform parsing of path.
        """
        self.filter: str = filter_
        self.attr_map: dict = attr_map

        self.token_stream = None
        self.ast = None
        self.transpiler = None

        self._build()

    def _build(self):
        self.token_stream = SCIMLexer().tokenize(self.filter)
        self.ast = SCIMParser().parse(self.token_stream)
        self.transpiler = Transpiler(self.attr_map)
        self.transpiler.transpile(self.ast)

    @property
    def is_complex(self) -> bool:
        """
        Return true if AttrPath has multiple levels within it. Eg::

           "members[value eq "2819c223-7f76-453a-919d-413861904646"]"

        The following example is a simple (or not complex) AttrPath::

           "name.familyName"
        """
        return len(self.transpiler.attr_paths) > 1

    @property
    def params_by_attr_paths(self) -> dict:
        """
        Return the parameters for the given path keyed by their attr paths. Eg::

           {
               ('members', 'value', None):  "2819c223-7f76-453a-919d-413861904646",
           }
        """
        params_by_paths = {}
        for i, path in enumerate(self):
            param_name = string.ascii_lowercase[i]
            param_value = self.transpiler.params.get(param_name)
            params_by_paths[path] = param_value

        return params_by_paths

    @property
    def first_path(self) -> tuple:
        """
        Return first path in list of parsed attr paths.
        """
        return self.transpiler.attr_paths[0]

    def __iter__(self):
        return iter(self.transpiler.attr_paths)

    def __str__(self) -> str:
        return json.dumps(self.transpiler.attr_paths, sort_keys=True, indent="    ")


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

    q = AttrPath(args.filter, attr_map)

    print(q)


if __name__ == "__main__":
    main()
