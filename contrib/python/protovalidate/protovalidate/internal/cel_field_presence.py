# Copyright 2023-2025 Buf Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import threading

import celpy

_has_state = threading.local()


def in_has() -> bool:
    """
    Returns true if inside of CEL interpreter `has` macro.

    This enables working around an issue in cel-python where it is not possible
    to implement protobuf semantics around the `has` macro.

    https://github.com/cloud-custodian/cel-python/issues/73
    """
    return getattr(_has_state, "in_has", False)


class InterpretedRunner(celpy.InterpretedRunner):
    def evaluate(self, context):
        class Evaluator(celpy.Evaluator):
            def macro_has_eval(self, exprlist):
                _has_state.in_has = True
                result = super().macro_has_eval(exprlist)
                _has_state.in_has = False
                return result

        e = Evaluator(ast=self.ast, activation=self.new_activation(context), functions=self.functions)
        value = e.evaluate()
        return value
