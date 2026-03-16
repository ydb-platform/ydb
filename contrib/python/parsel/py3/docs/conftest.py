from doctest import ELLIPSIS, NORMALIZE_WHITESPACE
from pathlib import Path

from sybil import Sybil

try:
    from sybil.parsers.codeblock import PythonCodeBlockParser
except ImportError:
    from sybil.parsers.codeblock import CodeBlockParser as PythonCodeBlockParser
from sybil.parsers.doctest import DocTestParser
from sybil.parsers.skip import skip

from parsel import Selector


def load_selector(filename, **kwargs):
    input_path = Path(__file__).parent / "_static" / filename
    return Selector(text=input_path.read_text(encoding="utf-8"), **kwargs)


def setup(namespace):
    namespace["load_selector"] = load_selector


pytest_collect_file = Sybil(
    parsers=[
        DocTestParser(optionflags=ELLIPSIS | NORMALIZE_WHITESPACE),
        PythonCodeBlockParser(future_imports=["print_function"]),
        skip,
    ],
    pattern="*.rst",
    setup=setup,
).pytest()
