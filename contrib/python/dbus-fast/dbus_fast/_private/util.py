from __future__ import annotations

import ast
import inspect
import re
from collections.abc import Callable
from typing import Annotated, Any, get_args, get_origin

from dbus_fast.annotations import DBusSignature

from ..signature import SignatureTree, SignatureType, Variant, get_signature_tree


def signature_contains_type(
    signature: str | SignatureTree, body: list[Any], token: str
) -> bool:
    """For a given signature and body, check to see if it contains any members
    with the given token"""
    if type(signature) is str:
        signature = get_signature_tree(signature)

    queue = list(signature.types)  # type: ignore[union-attr]
    contains_variants = False

    while True:
        if not queue:
            break
        st = queue.pop()
        if st.token == token:
            return True
        if st.token == "v":
            contains_variants = True
        queue.extend(st.children)

    if not contains_variants:
        return False

    for member in body:
        queue.append(member)

    while True:
        if not queue:
            return False
        member = queue.pop()
        if type(member) is Variant and signature_contains_type(
            member.signature, [member.value], token
        ):
            return True
        if type(member) is list:
            queue.extend(member)
        elif type(member) is dict:
            queue.extend(member.values())


def replace_fds_with_idx(
    signature: str | SignatureTree, body: list[Any]
) -> tuple[list[Any], list[int]]:
    """Take the high level body format and convert it into the low level body
    format. Type 'h' refers directly to the fd in the body. Replace that with
    an index and return the corresponding list of unix fds that can be set on
    the Message"""
    if type(signature) is str:
        signature = get_signature_tree(signature)

    if not signature_contains_type(signature, body, "h"):
        return body, []

    unix_fds: list[Any] = []

    def _replace(fd: Any) -> int:
        try:
            return unix_fds.index(fd)
        except ValueError:
            unix_fds.append(fd)
            return len(unix_fds) - 1

    _replace_fds(body, signature.types, _replace)  # type: ignore[union-attr]

    return body, unix_fds


def replace_idx_with_fds(
    signature: str | SignatureTree, body: list[Any], unix_fds: list[Any]
) -> list[Any]:
    """Take the low level body format and return the high level body format.
    Type 'h' refers to an index in the unix_fds array. Replace those with the
    actual file descriptor or `None` if one does not exist."""
    if type(signature) is str:
        signature = get_signature_tree(signature)

    if not signature_contains_type(signature, body, "h"):
        return body

    def _replace(idx: int) -> Any:
        try:
            return unix_fds[idx]
        except IndexError:
            return None

    _replace_fds(body, signature.types, _replace)  # type: ignore[union-attr]

    return body


def parse_annotation(annotation: Any, module: Any) -> str:
    """
    Because of PEP 563, if `from __future__ import annotations` is used in code
    or on Python version >=3.10 where this is the default, return annotations
    from the `inspect` module will return annotations as "forward definitions".
    In this case, we must eval the result which we do only when given a string
    constant.
    """

    if not annotation or annotation is inspect.Signature.empty:
        return ""

    if type(annotation) is str:
        # This is a bit annoying because strings are special in annotations
        # because they are assumed to be forward references. There isn't really
        # a way to distinguish between a string constant and a forward reference
        # other than by heuristics.

        # If it looks like a dbus signature, return it directly. These are sorted
        # in the order of the "Summary of types" table in the D-Bus spec to make
        # verification easier.
        if re.match(r"^[ybnqiuxtdsoga\(\)v\{\}h]+$", annotation):
            return annotation

        # Otherwise, assume deferred evaluation of annotations.

        try:
            # It could be a string literal, e.g "'s'", in which case this will
            # effectively strip the quotes. Other literals would pass here, but
            # they aren't expected, so we just let those fail later.
            return ast.literal_eval(annotation)
        except ValueError:
            # Anything that isn't a Python literal will raise ValueError.
            pass

        # Deferred evaluation of annotations, so evaluate it now.
        annotation = eval(annotation, module.__dict__, {})  # noqa: S307

    if get_origin(annotation) is Annotated:
        try:
            sig = next(s for s in get_args(annotation) if type(s) is DBusSignature)
        except StopIteration:
            raise ValueError(
                f"Annotated D-Bus type must include a DBusSignature annotation (got {annotation!r})"
            )
        return sig.signature

    raise ValueError(
        f"D-Bus signature annotations must be a string constant or typing.Annotated with DBusSignature (got {annotation!r})"
    )


def _replace_fds(
    body_obj: dict[Any, Any] | list[Any],
    children: list[SignatureType],
    replace_fn: Callable[[Any], Any],
) -> None:
    """Replace any type 'h' with the value returned by replace_fn() given the
    value of the fd field. This is used by the high level interfaces which
    allow type 'h' to be the fd directly instead of an index in an external
    array such as in the spec."""
    for index, st in enumerate(children):
        if not any(sig in st.signature for sig in "hv"):
            continue
        if st.signature == "h":
            body_obj[index] = replace_fn(body_obj[index])
        elif st.token == "a":
            if st.children[0].token == "{":
                _replace_fds(body_obj[index], st.children, replace_fn)
            else:
                for i, child in enumerate(body_obj[index]):
                    if st.signature == "ah":
                        body_obj[index][i] = replace_fn(child)
                    else:
                        _replace_fds([child], st.children, replace_fn)
        elif st.token in "(":
            _replace_fds(body_obj[index], st.children, replace_fn)
        elif st.token in "{":
            for key, value in list(body_obj.items()):  # type: ignore[union-attr]
                body_obj.pop(key)
                if st.children[0].signature == "h":
                    key = replace_fn(key)
                if st.children[1].signature == "h":
                    value = replace_fn(value)
                else:
                    _replace_fds([value], [st.children[1]], replace_fn)
                body_obj[key] = value

        elif st.signature == "v":
            if body_obj[index].signature == "h":
                body_obj[index].value = replace_fn(body_obj[index].value)
            else:
                _replace_fds(
                    [body_obj[index].value], [body_obj[index].type], replace_fn
                )

        elif st.children:
            _replace_fds(body_obj[index], st.children, replace_fn)
