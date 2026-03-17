# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
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
from typing import Any

from .utils import AttrDict


class Range(AttrDict):
    OPS = {
        "lt": operator.lt,
        "lte": operator.le,
        "gt": operator.gt,
        "gte": operator.ge,
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if args and (len(args) > 1 or kwargs or not isinstance(args[0], dict)):
            raise ValueError(
                "Range accepts a single dictionary or a set of keyword arguments."
            )
        data = args[0] if args else kwargs

        for k in data:
            if k not in self.OPS:
                raise ValueError(f"Range received an unknown operator {k!r}")

        if "gt" in data and "gte" in data:
            raise ValueError("You cannot specify both gt and gte for Range.")

        if "lt" in data and "lte" in data:
            raise ValueError("You cannot specify both lt and lte for Range.")

        super().__init__(args[0] if args else kwargs)

    def __repr__(self) -> str:
        return "Range(%s)" % ", ".join("%s=%r" % op for op in self._d_.items())

    def __contains__(self, item: Any) -> bool:
        if isinstance(item, str):
            return super().__contains__(item)

        for op in self.OPS:
            if op in self._d_ and not self.OPS[op](item, self._d_[op]):
                return False
        return True

    @property
    def upper(self) -> Any:
        if "lt" in self._d_:
            return self._d_["lt"], False
        if "lte" in self._d_:
            return self._d_["lte"], True
        return None, False

    @property
    def lower(self) -> Any:
        if "gt" in self._d_:
            return self._d_["gt"], False
        if "gte" in self._d_:
            return self._d_["gte"], True
        return None, False


__all__ = ["Range"]
