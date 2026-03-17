import inspect
import sys
from typing import Any, Dict, Optional, Tuple, Type

from beanie.odm.fields import IndexedAnnotation

from .pydantic import IS_PYDANTIC_V2, get_field_type

if sys.version_info >= (3, 8):
    from typing import get_args, get_origin
else:
    from typing_extensions import get_args, get_origin


def extract_id_class(annotation) -> Type[Any]:
    if get_origin(annotation) is not None:
        try:
            annotation = next(
                arg for arg in get_args(annotation) if arg is not type(None)
            )
        except StopIteration:
            annotation = None
    if inspect.isclass(annotation):
        return annotation
    raise ValueError("Unknown annotation: {}".format(annotation))


def get_index_attributes(field) -> Optional[Tuple[int, Dict[str, Any]]]:
    """Gets the index attributes from the field, if it is indexed.

    :param field: The field to get the index attributes from.

    :return: The index attributes, if the field is indexed. Otherwise, None.
    """
    # For fields that are directly typed with `Indexed()`, the type will have
    # an `_indexed` attribute.
    field_type = get_field_type(field)
    if hasattr(field_type, "_indexed"):
        return getattr(field_type, "_indexed", None)

    # For fields that are use `Indexed` within `Annotated`, the field will have
    # metadata that might contain an `IndexedAnnotation` instance.
    if IS_PYDANTIC_V2:
        # In Pydantic 2, the field has a `metadata` attribute with
        # the annotations.
        metadata = getattr(field, "metadata", None)
    elif hasattr(field, "annotation") and hasattr(
        field.annotation, "__metadata__"
    ):
        # In Pydantic 1, the field has an `annotation` attribute with the
        # type assigned to the field. If the type is annotated, it will
        # have a `__metadata__` attribute with the annotations.
        metadata = field.annotation.__metadata__
    else:
        return None

    if metadata is None:
        return None

    try:
        iter(metadata)
    except TypeError:
        return None

    indexed_annotation = next(
        (
            annotation
            for annotation in metadata
            if isinstance(annotation, IndexedAnnotation)
        ),
        None,
    )

    return getattr(indexed_annotation, "_indexed", None)
