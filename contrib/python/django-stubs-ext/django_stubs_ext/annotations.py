from typing import Any, Generic, Mapping, TypeVar

from django.db.models.base import Model
from typing_extensions import Annotated

# Really, we would like to use TypedDict as a bound, but it's not possible
_Annotations = TypeVar("_Annotations", covariant=True, bound=Mapping[str, Any])


class Annotations(Generic[_Annotations]):
    """Use as `Annotations[MyTypedDict]`"""

    pass


_T = TypeVar("_T", bound=Model)

WithAnnotations = Annotated[_T, Annotations[_Annotations]]
"""Alias to make it easy to annotate the model `_T` as having annotations
`_Annotations` (a `TypedDict` or `Any` if not provided).

Use as `WithAnnotations[MyModel]` or `WithAnnotations[MyModel, MyTypedDict]`.
"""
