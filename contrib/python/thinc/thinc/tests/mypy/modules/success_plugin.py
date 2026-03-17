from typing import Any, TypeVar

from thinc.api import Model, Relu, Softmax, add, chain, reduce_max

good_model = chain(Relu(10), Relu(10), Softmax())
reveal_type(good_model)

good_model2 = add(Relu(10), Relu(10), Softmax())
reveal_type(good_model2)

bad_model_undetected = chain(Relu(10), Relu(10), Relu(10), Relu(10), Softmax())
reveal_type(bad_model_undetected)

bad_model_undetected2 = add(Relu(10), Relu(10), Relu(10), Relu(10), Softmax())
reveal_type(bad_model_undetected2)


def forward() -> None:
    pass


OtherType = TypeVar("OtherType")


def other_function(
    layer1: Model, layer2: Model, *layers: Model
) -> Model[Any, OtherType]:
    return Model("some_model", forward)


non_combinator_model = other_function(
    Model("x", forward), Model("y", forward), Model("z", forward)
)
reveal_type(non_combinator_model)
