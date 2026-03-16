#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import operator
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Literal,
    Mapping,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
)

if TYPE_CHECKING:
    from _operator import _SupportsComparison

from typing_extensions import TypeAlias

from .utils import AttrDict

ComparisonOperators: TypeAlias = Literal["lt", "lte", "gt", "gte"]
RangeValT = TypeVar("RangeValT", bound="_SupportsComparison")

__all__ = ["Range"]


class Range(AttrDict[RangeValT]):
    OPS: ClassVar[
        Mapping[
            ComparisonOperators,
            Callable[["_SupportsComparison", "_SupportsComparison"], bool],
        ]
    ] = {
        "lt": operator.lt,
        "lte": operator.le,
        "gt": operator.gt,
        "gte": operator.ge,
    }

    def __init__(
        self,
        d: Optional[Dict[str, RangeValT]] = None,
        /,
        **kwargs: RangeValT,
    ):
        if d is not None and (kwargs or not isinstance(d, dict)):
            raise ValueError(
                "Range accepts a single dictionary or a set of keyword arguments."
            )

        if d is None:
            data = kwargs
        else:
            data = d

        for k in data:
            if k not in self.OPS:
                raise ValueError(f"Range received an unknown operator {k!r}")

        if "gt" in data and "gte" in data:
            raise ValueError("You cannot specify both gt and gte for Range.")

        if "lt" in data and "lte" in data:
            raise ValueError("You cannot specify both lt and lte for Range.")

        super().__init__(data)

    def __repr__(self) -> str:
        return "Range(%s)" % ", ".join("%s=%r" % op for op in self._d_.items())

    def __contains__(self, item: object) -> bool:
        if isinstance(item, str):
            return super().__contains__(item)

        item_supports_comp = any(hasattr(item, f"__{op}__") for op in self.OPS)
        if not item_supports_comp:
            return False

        for op in self.OPS:
            if op in self._d_ and not self.OPS[op](
                cast("_SupportsComparison", item), self._d_[op]
            ):
                return False
        return True

    @property
    def upper(self) -> Union[Tuple[RangeValT, bool], Tuple[None, Literal[False]]]:
        if "lt" in self._d_:
            return self._d_["lt"], False
        if "lte" in self._d_:
            return self._d_["lte"], True
        return None, False

    @property
    def lower(self) -> Union[Tuple[RangeValT, bool], Tuple[None, Literal[False]]]:
        if "gt" in self._d_:
            return self._d_["gt"], False
        if "gte" in self._d_:
            return self._d_["gte"], True
        return None, False


class AggregationRange(AttrDict[Any]):
    """
    :arg from: Start of the range (inclusive).
    :arg key: Custom key to return the range with.
    :arg to: End of the range (exclusive).
    """

    def __init__(
        self,
        *,
        from_: Any = None,
        key: Optional[str] = None,
        to: Any = None,
        **kwargs: Any,
    ):
        if from_ is not None:
            kwargs["from_"] = from_
        if key is not None:
            kwargs["key"] = key
        if to is not None:
            kwargs["to"] = to
        super().__init__(kwargs)
