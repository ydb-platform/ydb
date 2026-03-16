from dataclasses import replace

from ..definitions import InputShape, IntrospectionError, Shape
from .callable import get_callable_shape


def get_class_init_shape(tp) -> Shape[InputShape, None]:
    if not isinstance(tp, type):
        raise IntrospectionError

    shape = get_callable_shape(
        tp.__init__,  # type: ignore[misc]
        slice(1, None),
    )
    return replace(
        shape,
        input=replace(
            shape.input,
            constructor=tp,
        ),
    )
