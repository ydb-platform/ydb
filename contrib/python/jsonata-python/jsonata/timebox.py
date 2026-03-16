#
# Copyright Robert Yokota
# 
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Derived from the following code:
#
#   Project name: jsonata-java
#   Copyright Dashjoin GmbH. https://dashjoin.com
#   Licensed under the Apache License, Version 2.0 (the "License")
#

import time

from jsonata import jexception


#
# Configure max runtime / max recursion depth.
# See Frame.setRuntimeBounds - usually not used directly
# 
class Timebox:
    #
    # Protect the process/browser from a runnaway expression
    # i.e. Infinite loop (tail recursion), or excessive stack growth
    #
    # @param {Object} expr - expression to protect
    # @param {Number} timeout - max time in ms
    # @param {Number} max_depth - max stack depth
    #

    timeout: int
    max_depth: int
    time: int
    depth: int

    def __init__(self, expr, timeout=10000, max_depth=100):
        self.timeout = timeout
        self.max_depth = max_depth
        self.time = Timebox.current_milli_time()
        self.depth = 0

        # register callbacks
        def entry_callback(exp, input, env):
            if env.is_parallel_call:
                return
            self.depth += 1
            self.check_runaway()

        expr.set_evaluate_entry_callback(entry_callback)

        def exit_callback(exp, input, env, res):
            if env.is_parallel_call:
                return
            self.depth -= 1
            self.check_runaway()

        expr.set_evaluate_exit_callback(exit_callback)

    def check_runaway(self) -> None:
        if self.depth > self.max_depth:
            # stack too deep
            raise jexception.JException(
                "Stack overflow error: Check for non-terminating recursive function.  Consider rewriting as tail-recursive. Depth=" + str(
                    self.depth) + " max=" + str(self.max_depth), -1)
            # stack: new Error().stack,
            # code: "U1001"
            # }
        if Timebox.current_milli_time() - self.time > self.timeout:
            # expression has run for too long
            raise jexception.JException(
                "Expression evaluation timeout: " + str(self.timeout) + "ms. Check for infinite loop", -1)
            # stack: new Error().stack,
            # code: "U1001"
            # }

    @staticmethod
    def current_milli_time() -> int:
        return round(time.time() * 1000)
