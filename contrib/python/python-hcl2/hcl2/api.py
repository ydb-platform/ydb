"""The API that will be exposed to users of this package"""
from typing import TextIO

from lark.tree import Tree
from hcl2.parser import parser, reconstruction_parser
from hcl2.transformer import DictTransformer
from hcl2.reconstructor import HCLReconstructor, HCLReverseTransformer


def load(file: TextIO, with_meta=False) -> dict:
    """Load a HCL2 file.
    :param file: File with hcl2 to be loaded as a dict.
    :param with_meta: If set to true then adds `__start_line__` and `__end_line__`
    parameters to the output dict. Default to false.
    """
    return loads(file.read(), with_meta=with_meta)


def loads(text: str, with_meta=False) -> dict:
    """Load HCL2 from a string.
    :param text: Text with hcl2 to be loaded as a dict.
    :param with_meta: If set to true then adds `__start_line__` and `__end_line__`
    parameters to the output dict. Default to false.
    """
    # append new line as a workaround for https://github.com/lark-parser/lark/issues/237
    # Lark doesn't support EOF token so our grammar can't look for "new line or end of file"
    # This means that all blocks must end in a new line even if the file ends
    # Append a new line as a temporary fix
    tree = parser().parse(text + "\n")
    return DictTransformer(with_meta=with_meta).transform(tree)


def parse(file: TextIO) -> Tree:
    """Load HCL2 syntax tree from a file.
    :param file: File with hcl2 to be loaded as a dict.
    """
    return parses(file.read())


def parses(text: str) -> Tree:
    """Load HCL2 syntax tree from a string.
    :param text: Text with hcl2 to be loaded as a dict.
    """
    return reconstruction_parser().parse(text)


def transform(ast: Tree, with_meta=False) -> dict:
    """Convert an HCL2 AST to a dictionary.
    :param ast: HCL2 syntax tree, output from `parse` or `parses`
    :param with_meta: If set to true then adds `__start_line__` and `__end_line__`
        parameters to the output dict. Default to false.
    """
    return DictTransformer(with_meta=with_meta).transform(ast)


def reverse_transform(hcl2_dict: dict) -> Tree:
    """Convert a dictionary to an HCL2 AST.
    :param hcl2_dict: a dictionary produced by `load` or `transform`
    """
    return HCLReverseTransformer().transform(hcl2_dict)


def writes(ast: Tree) -> str:
    """Convert an HCL2 syntax tree to a string.
    :param ast: HCL2 syntax tree, output from `parse` or `parses`
    """
    return HCLReconstructor(reconstruction_parser()).reconstruct(ast)
