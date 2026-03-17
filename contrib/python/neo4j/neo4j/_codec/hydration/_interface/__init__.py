# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import abc

from .._common import DehydrationHooks


class HydrationHandlerABC(abc.ABC):
    def __init__(self):
        self.struct_hydration_functions = {}
        self.dehydration_hooks = DehydrationHooks(exact_types={}, subtypes={})

    @abc.abstractmethod
    def new_hydration_scope(self): ...
