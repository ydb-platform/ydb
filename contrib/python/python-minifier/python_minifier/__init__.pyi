import ast

from typing import Any, List, Optional, Text, Union

from .transforms.remove_annotations_options import RemoveAnnotationsOptions as RemoveAnnotationsOptions


class UnstableMinification(RuntimeError):
    def __init__(self, exception: Any, source: Any, minified: Any): ...


def minify(
    source: Union[str, bytes],
    filename: Optional[str] = ...,
    remove_annotations: Union[bool, RemoveAnnotationsOptions] = ...,
    remove_pass: bool = ...,
    remove_literal_statements: bool = ...,
    combine_imports: bool = ...,
    hoist_literals: bool = ...,
    rename_locals: bool = ...,
    preserve_locals: Optional[List[Text]] = ...,
    rename_globals: bool = ...,
    preserve_globals: Optional[List[Text]] = ...,
    remove_object_base: bool = ...,
    convert_posargs_to_args: bool = ...,
    preserve_shebang: bool = ...,
    remove_asserts: bool = ...,
    remove_debug: bool = ...,
    remove_explicit_return_none: bool = ...,
    remove_builtin_exception_brackets: bool = ...,
    constant_folding: bool = ...,
    prefer_single_line: bool = ...
) -> Text: ...


def unparse(
    module: ast.Module,
    prefer_single_line: bool = ...
) -> Text: ...


def awslambda(
    source: Union[str, bytes],
    filename: Optional[Text] = ...,
    entrypoint: Optional[Text] = ...
) -> Text: ...
