import os
from pathlib import Path
from warnings import warn
from xml.etree import ElementTree

import pytest

from tinyhtml5 import constants
from tinyhtml5.parser import HTMLParser

from . import Data


def convert(data, strip_chars):
    return (line[strip_chars:] if line.startswith("|") else line for line in data)


def serialize(item, indent=0, skip=False):
    if item.tag == ElementTree.Comment:
        yield f"{'  ' * indent}<!-- {item.text} -->"
        return
    elif item.tag == "<!DOCTYPE>":
        text = item.text
        if public := item.get("publicId"):
            public = f"'{public}'" if '"' in public else f'"{public}"'
            text += f" PUBLIC {public}" if item.text == "html" else f' {public} ""'
        elif system := item.get("systemId"):
            system = f'"{system}"'
            text += " SYSTEM" if item.text == "html" else ' ""'
            text += f" {system}"
        yield f"<!DOCTYPE {text}>"
        return
    if not skip:
        yield f"{'  ' * indent}<{item.tag}>"
        indent += 1
        for key, value in sorted(item.attrib.items()):
            yield f'{"  " * indent}{key}="{value}"'
    if item.text:
        yield f'{"  " * indent}"{item.text}"'
    for child in item:
        yield from serialize(child, indent)
        if child.tail:
            yield f'{"  " * indent}"{child.tail}"'


import yatest.common as yc
_tests = tuple(
    (f"{path.stem}-{i}", test)
    for path in (Path(yc.source_path(__file__)).parent / "tree-construction").glob("*.dat")
    for i, test in enumerate(Data(path)) if "script-on" not in test)
_xfails = (
    "adoption01-17",

    "blocks-12",

    "foreign-fragment-2", "foreign-fragment-3", "foreign-fragment-18",
    "foreign-fragment-19", "foreign-fragment-22", "foreign-fragment-23",
    "foreign-fragment-26", "foreign-fragment-27", "foreign-fragment-30",
    "foreign-fragment-31", "foreign-fragment-34", "foreign-fragment-35",
    "foreign-fragment-39", "foreign-fragment-41", "foreign-fragment-60",
    "foreign-fragment-61", "foreign-fragment-62", "foreign-fragment-63",
    "foreign-fragment-66",

    "isindex-0", "isindex-1", "isindex-2", "isindex-3",

    "namespace-sensitivity-0",

    "ruby-0", "ruby-1", "ruby-2", "ruby-3", "ruby-5", "ruby-7",
    "ruby-10", "ruby-12", "ruby-15", "ruby-17", "ruby-20",

    *[f"template-{i}" for i in range(7)],
    *[f"template-{i}" for i in range(8, 39)],
    *[f"template-{i}" for i in range(40, 111)],

    "tests2-6", "tests2-7",

    "tests8-5",

    "tests11-2", "tests11-4", "tests11-5", "tests11-6",

    "tests18-15",

    "tests19-7", "tests19-14", "tests19-17",

    "tests25-7",

    "tests26-16", "tests26-17", "tests26-18", "tests26-19",

    "webik02-14", "webik02-15", "webik02-16",

    # Introduced by the fork.

    # Doctype.
    "doctype01-26", "doctype01-28", "doctype01-29", "doctype01-31", "doctype01-32",
    "doctype01-33", "doctype01-34", "doctype01-35", "doctype01-36",
    "tests6-46",
    "quirks01-0", "quirks01-1", "quirks01-2", "quirks01-3",

    # <template>, <search>.
    "template-111", "search-element-0", "search-element-1",

    # Mismatched elements.
    "webkit02-11", "webkit02-15", "webkit02-16", "webkit02-17",

    # Attributes namespace.
    "webkit02-22",

    # <hr> in <options>.
    *[f"webkit02-{i}" for i in range(25, 35)],

    # Namespace in fragment.
    "foreign-fragment-59", "foreign-fragment-65",
)
if os.name == "nt":
    # Fail because of newline management on Windows.
    _xfails += (
        "domjs-unsafe-0",
        "pending-spec-changes-plain-text-unsafe-0",
        "plain-text-unsafe-0",
    )

@pytest.mark.parametrize("namespace", (False, True), ids=("nons", "ns"))
@pytest.mark.parametrize("id, test", _tests, ids=(id for id, _ in _tests))
def test_tree_construction(id, test, namespace):
    # TODO: Check error messages.
    parser = HTMLParser(namespace_html_elements=namespace)

    input = test["data"]
    container = test["document-fragment"]
    expected = "\n".join(convert(test.get("document", "").split("\n"), 2))

    if container:
        document = parser.parse_fragment(input, container=container)
    else:
        document = parser.parse(input, full_tree=True)

    output = "\n".join(convert(serialize(document, skip=True), 3))
    for prefix, value in constants.prefixes.items():
        output = output.replace(f"{{{prefix}}}", "" if value == "html" else f"{value} ")

    if id in _xfails:
        if expected == output:  # pragma: no cover
            warn(f"{id}{'-ns' if namespace else ''} passes but is marked as xfail")
        else:
            pytest.xfail()
    else:
        error_message = "".join(
            f"\n{line}\n" for line in (
                f"Input:\n{input}",
                f"Expected:\n{expected}",
                f"Received:\n{output}"))
        assert expected == output, error_message
