# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import annotations


import proto  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.enums",
    marshal="google.ads.googleads.v20",
    manifest={
        "SimulationModificationMethodEnum",
    },
)


class SimulationModificationMethodEnum(proto.Message):
    r"""Container for enum describing the method by which a
    simulation modifies a field.

    """

    class SimulationModificationMethod(proto.Enum):
        r"""Enum describing the method by which a simulation modifies a
        field.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            UNIFORM (2):
                The values in a simulation were applied to
                all children of a given resource uniformly.
                Overrides on child resources were not respected.
            DEFAULT (3):
                The values in a simulation were applied to
                the given resource. Overrides on child resources
                were respected, and traffic estimates do not
                include these resources.
            SCALING (4):
                The values in a simulation were all scaled by
                the same factor. For example, in a simulated
                TargetCpa campaign, the campaign target and all
                ad group targets were scaled by a factor of X.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        UNIFORM = 2
        DEFAULT = 3
        SCALING = 4


__all__ = tuple(sorted(__protobuf__.manifest))
