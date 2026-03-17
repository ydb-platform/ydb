'''
Created on June 7, 2019
@author: Andrew Habib
'''

import operator
from functools import reduce


Jnumeric = set(["integer", "number"])

Jtypes = Jnumeric.union(["string", "boolean", "null", "array", "object"])

JallTypes = Jnumeric.union(Jtypes)

JtypesToKeywords = {
    "string": ["minLength", "maxLength", "pattern"],
    "number": ["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"],
    "integer": ["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"],
    "boolean": [],
    "null": [],
    "array": ["minItems", "maxItems", "items", "additionalItems", "uniqueItems"],
    "object": ["properties", "additionalProperties", "required", "minProperties", "maxProperties", "dependencies", "patternProperties"]
}

JtypesRestrictionKeywords = reduce(operator.add, JtypesToKeywords.values())

Jconnectors = set(["anyOf", "allOf", "oneOf", "not"])

Jcommonkw = Jconnectors.union(["enum", "type"])

JNonValidation = set(["$schema", "$id", "definitions", "title", "description", "format"])

# Jkeywords = Jcommonkw.union(Jtypes, reduce(operator.add, JtypesToKeywords.values())).union(["$ref"])
Jkeywords = Jcommonkw.union(Jtypes, JtypesRestrictionKeywords, ["$ref"])
                            # .union(JNonValidation) # conflicts with canonicalize_connectors

JtypesToPyTypes = {"integer": int, "number": float, "string": str,
                   "boolean": bool, "null": type(None), "array": list, "object": dict}

PyTypesToJtypes = dict([(v, k) for k, v in JtypesToPyTypes.items()])
