import os
import json
import pkgutil

root = os.path.dirname(__file__)
literal_file = os.path.join(root, "pgliterals.json")

literals = json.loads(pkgutil.get_data(__package__, "pgliterals.json"))


def get_literals(literal_type, type_=tuple):
    # Where `literal_type` is one of 'keywords', 'functions', 'datatypes',
    # returns a tuple of literal values of that type.

    return type_(literals[literal_type])
