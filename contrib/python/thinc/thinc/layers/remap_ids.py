from typing import Any, Callable, Dict, Hashable, Optional, Sequence, Tuple, Union, cast

from ..config import registry
from ..model import Model
from ..types import DTypes, Ints1d, Ints2d
from ..util import is_xp_array, to_numpy

InT = Union[Sequence[Hashable], Ints1d, Ints2d]
OutT = Ints2d

InT_v1 = Sequence[Any]
OutT_v1 = Ints2d


@registry.layers("remap_ids.v1")
def remap_ids(
    mapping_table: Dict[Any, int] = {}, default: int = 0, dtype: DTypes = "i"
) -> Model[InT_v1, OutT_v1]:
    """Remap string or integer inputs using a mapping table, usually as a
    preprocess before embeddings. The mapping table can be passed in on input,
    or updated after the layer has been created. The mapping table is stored in
    the "mapping_table" attribute.
    """
    return Model(
        "remap_ids",
        forward,
        attrs={"mapping_table": mapping_table, "dtype": dtype, "default": default},
    )


def forward(
    model: Model[InT_v1, OutT_v1], inputs: InT_v1, is_train: bool
) -> Tuple[OutT, Callable]:
    table = model.attrs["mapping_table"]
    default = model.attrs["default"]
    dtype = model.attrs["dtype"]
    values = [table.get(x, default) for x in inputs]
    arr = model.ops.asarray2i(values, dtype=dtype)
    output = model.ops.reshape2i(arr, -1, 1)

    def backprop(dY: OutT_v1) -> InT:
        return []

    return output, backprop


@registry.layers("remap_ids.v2")
def remap_ids_v2(
    mapping_table: Optional[Union[Dict[int, int], Dict[str, int]]] = None,
    default: int = 0,
    *,
    column: Optional[int] = None
) -> Model[InT, OutT]:
    """Remap string or integer inputs using a mapping table,
    usually as a preprocessing step before embeddings.
    The mapping table can be passed in on input,
    or updated after the layer has been created.
    The mapping table is stored in the "mapping_table" attribute.
    Two dimensional arrays can be provided as input in which case
    the 'column' chooses which column to process. This is useful
    to work together with FeatureExtractor in spaCy.
    """
    return Model(
        "remap_ids",
        forward_v2,
        attrs={"mapping_table": mapping_table, "default": default, "column": column},
    )


def forward_v2(
    model: Model[InT, OutT], inputs: InT, is_train: bool
) -> Tuple[OutT, Callable]:
    table = model.attrs["mapping_table"]
    if table is None:
        raise ValueError("'mapping table' not set")
    default = model.attrs["default"]
    column = model.attrs["column"]
    if is_xp_array(inputs):
        xp_input = True
        if column is not None:
            idx = to_numpy(cast(Ints2d, inputs)[:, column])
        else:
            idx = to_numpy(inputs)
    else:
        xp_input = False
        idx = inputs
    values = [table.get(x, default) for x in idx]
    arr = model.ops.asarray2i(values, dtype="i")
    output = model.ops.reshape2i(arr, -1, 1)

    def backprop(dY: OutT) -> InT:
        if xp_input:
            return model.ops.xp.empty(dY.shape)  # type: ignore
        else:
            return []

    return output, backprop
