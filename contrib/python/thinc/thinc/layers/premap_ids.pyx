# cython: binding=True, infer_types=True, profile=False
from libc.stdint cimport int64_t
import numpy

from preshed.maps cimport PreshMap

from typing import Callable, Dict, Mapping, Optional, Tuple, Union, cast

from ..config import registry
from ..model import Model
from ..types import Ints1d, Ints2d
from ..util import to_numpy

InT = Union[Ints1d, Ints2d]
OutT = Ints2d


cdef lookup(PreshMap mapping, int64_t[:] keys, int64_t default):
    """
    Faster dict.get(keys, default) for the case when
    the "dict" is a Dict[int, int] converted to PreshMap
    and the "keys" is a numpy integer vector.
    """
    cdef int maxi = len(keys)
    result = numpy.empty(maxi, dtype="int")
    cdef int64_t[:] result_view = result
    for i in range(maxi):
        v = mapping[keys[i]]
        if v is None:
            result_view[i] = default
        else:
            result_view[i] = v
    return result


@registry.layers("premap_ids.v1")
def premap_ids(
    mapping_table: Mapping[int, int],
    default: int = 0,
    *,
    column: Optional[int] = None
):
    """Remap integer inputs to integers a mapping table, usually as a
    preprocess before embeddings."""
    mapper = PreshMap(initial_size=len(mapping_table))
    for k, v in mapping_table.items():
        if not (isinstance(k, int) and isinstance(v, int)):
            raise ValueError(
                "mapping_table has to be of type Mapping[int, int], "
                f"but found {k}, {type(k)} and {v}, {type(v)}"
            )
        mapper[k] = v
    return Model(
        "premap_ids",
        forward,
        attrs={
            "mapping_table": mapper, "default": default, "column": column
        }
    )


def forward(
    model: Model, inputs: InT, is_train: bool
) -> Tuple[OutT, Callable]:
    table = model.attrs["mapping_table"]
    default = model.attrs["default"]
    column = model.attrs["column"]
    # Have to convert to numpy anyways, because
    # cupy ints don't work together with Python ints.
    if column is None:
        idx = to_numpy(inputs)
    else:
        idx = to_numpy(cast(Ints2d, inputs)[:, column])
    result = lookup(table, idx, default)
    arr = model.ops.asarray2i(result)
    output = model.ops.reshape2i(arr, -1, 1)

    def backprop(dY: OutT) -> InT:
        return model.ops.xp.empty(dY.shape)  # type: ignore

    return output, backprop
