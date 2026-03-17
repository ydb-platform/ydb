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


from .... import _typing as t


class Structure:
    tag: bytes
    fields: list[t.Any]

    def __init__(self, tag: bytes, *fields: t.Any):
        self.tag = tag
        self.fields = list(fields)

    def __repr__(self) -> str:
        args = ", ".join(map(repr, (self.tag, *self.fields)))
        return f"Structure({args})"

    def __eq__(self, other) -> bool:
        try:
            return self.tag == other.tag and self.fields == other.fields
        except AttributeError:
            return NotImplemented

    def __len__(self) -> int:
        return len(self.fields)

    def __getitem__(self, key):
        return self.fields[key]

    def __setitem__(self, key, value):
        self.fields[key] = value
