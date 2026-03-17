__all__ = [
    'attribute',
    'fix_attrs_converters',
    'universal_decorator',
    'expand',
]
import typing


def universal_decorator(*, has_parameters):
    """Metadecorator used for writing universal decorators that does not change function-like object type.

    Wrapped decorator will preserve function-like object type: plain functions, async functions, generators and
    async generators will keep their type after being decorated. Warning: if your decorator changes the
    type of the function-like object (e.g. yields plain function result) do NOT use this (meta)decorator.

    Args:
        has_parameters: does wrapped decorator use parameters
    """
    ...


def expand(
    arg_name: str,
    arg_type: typing.Optional[typing.Type] = None,
    check_type: bool = True
) -> typing.Callable:
    """Allows you to call function also via passing values for creating "arg_name", not only via passing an instance of "arg_name"

    If used on class, expands some argument in __init__

    Args:
        arg_name: Parameter that will be expanded.
        arg_type: Specify the type of this parameter. Defaults to None and calc it themselves.
        check_type: If True, check that one argument has a compatible type for none expanded version. If not, calls an expanded version. Defaults to True.
    """
    ...


def attribute(
    *args,
    required: bool = False,
    origin: typing.Optional[str] = None,
    readonly: bool = False,
    autocast: bool = False,
    **kwargs
):
    """Proxy for attr.attrib(...). Adds several keywords.

    Args:
        *args: All positional arguments from attr.attrib
        required: If True makes attribute not Optional. All other attributes are optional by default. Defaults to False.
        origin: Sets field name in dict for attribute, when structuring/unstructuring from dict. Defaults to None.
        readonly: Affects only when the class 'expanding' as a parameter in some function. If True, drops this attribute from expanded parameters. Defaults to None.
        autocast: If True then converter.structure will be used to convert input value
        **kwargs: All keyword arguments from attr.attrib
    """
    ...


def fix_attrs_converters(cls):
    """Due to [https://github.com/Toloka/toloka-kit/issues/37](https://github.com/Toloka/toloka-kit/issues/37)
    we have to support attrs>=20.3.0.
    This version lacks a feature that uses converters' annotations in class's __init__
    (see [https://github.com/python-attrs/attrs/pull/710](https://github.com/python-attrs/attrs/pull/710))).
    This decorator brings this feature to older attrs versions.
    """
    ...
