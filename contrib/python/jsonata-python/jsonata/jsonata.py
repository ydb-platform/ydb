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
#   Project name: JSONata
# © Copyright IBM Corp. 2016, 2017 All Rights Reserved
#   This project is licensed under the MIT License, see LICENSE
#

import copy
import inspect
import math
import re
import sys
import threading
from dataclasses import dataclass
from typing import Any, Callable, Mapping, MutableSequence, Optional, Sequence, Type, MutableMapping

from jsonata import functions, jexception, parser, signature as sig, timebox, utils


#
# @module JSONata
# @description JSON query and transformation language
# 
class Jsonata:
    class Frame:
        bindings: MutableMapping[str, Any]
        parent: 'Jsonata.Optional[Frame]'
        is_parallel_call: bool

        def __init__(self, parent):
            self.bindings = {}
            self.parent = parent
            self.is_parallel_call = False

        def bind(self, name: str, val: Optional[Any]) -> None:
            self.bindings[name] = val
            if getattr(val, "signature", None) is not None:
                val.signature.set_function_name(name)

        def lookup(self, name: str) -> Optional[Any]:
            # Important: if we have a null value,
            # return it
            val = self.bindings.get(name, utils.Utils.NONE)
            if val is not utils.Utils.NONE:
                return val
            if self.parent is not None:
                return self.parent.lookup(name)
            return None

        #
        # Sets the runtime bounds for this environment
        # 
        # @param timeout Timeout in millis
        # @param maxRecursionDepth Max recursion depth
        #         
        def set_runtime_bounds(self, timeout: int, max_recursion_depth: int) -> None:
            timebox.Timebox(self, timeout, max_recursion_depth)

        def set_evaluate_entry_callback(self, cb: Callable) -> None:
            self.bind("__evaluate_entry", cb)

        def set_evaluate_exit_callback(self, cb: Callable) -> None:
            self.bind("__evaluate_exit", cb)

    static_frame = None  # = createFrame(null);

    #
    # JFunction callable Lambda interface
    #
    class JFunctionCallable:
        def call(self, input: Optional[Any], args: Optional[Sequence]) -> Optional[Any]:
            pass

    class JFunctionSignatureValidation:
        def validate(self, args: Optional[Any], context: Optional[Any]) -> Optional[Any]:
            pass

    class JLambda(JFunctionCallable, JFunctionSignatureValidation):
        function: Callable

        def __init__(self, function):
            self.function = function

        def call(self, input: Optional[Any], args: Optional[Sequence]) -> Optional[Any]:
            if isinstance(args, list):
                return self.function(*args)
            else:
                return self.function()

        def validate(self, args: Optional[Any], context: Optional[Any]) -> Optional[Any]:
            return args

    #
    # JFunction definition class
    #
    class JFunction(JFunctionCallable, JFunctionSignatureValidation):
        function: 'Jsonata.JFunctionCallable'
        signature: Optional[sig.Signature]
        function_name: Optional[str]

        def __init__(self, function, signature):
            self.function = function
            if signature is not None:
                # use classname as default, gets overwritten once the function is registered
                self.signature = sig.Signature(signature, str(type(function)))
            else:
                self.signature = None

            self.function_name = None

        def call(self, input: Optional[Any], args: Optional[Sequence]) -> Optional[Any]:
            return self.function.call(input, args)

        def validate(self, args: Optional[Any], context: Optional[Any]) -> Optional[Any]:
            if self.signature is not None:
                return self.signature.validate(args, context)
            else:
                return args

        def get_number_of_args(self) -> int:
            return 0

    class JNativeFunction(JFunction):
        function_name: str
        signature: Optional[sig.Signature]
        clz: Type[functions.Functions]
        method: Optional[Any]
        nargs: int

        def __init__(self, function_name, signature, clz, impl_method_name):
            super().__init__(None, None)
            self.function_name = function_name
            self.signature = sig.Signature(signature, function_name)
            if impl_method_name is None:
                impl_method_name = self.function_name
            self.method = functions.Functions.get_function(clz, impl_method_name)
            self.nargs = len(inspect.signature(self.method).parameters) if self.method is not None else 0
            if self.method is None:
                print("Function not implemented: " + function_name + " impl=" + impl_method_name)

        def call(self, input: Optional[Any], args: Optional[Sequence]) -> Optional[Any]:
            return functions.Functions._call(self.method, self.nargs, args)

        def get_number_of_args(self) -> int:
            return self.nargs

    class Transformer(JFunctionCallable):
        _jsonata: 'Jsonata'
        _expr: Optional[parser.Parser.Symbol]
        _environment: 'Jsonata.Optional[Frame]'

        def __init__(self, jsonata, expr, environment):
            self._jsonata = jsonata
            self._expr = expr
            self._environment = environment

        def call(self, input: Optional[Any], args: Optional[Sequence]) -> Optional[Any]:
            # /* async */ Object (obj) { // signature <(oa):o>

            obj = args[0]

            # undefined inputs always return undefined
            if obj is None:
                return None

            # this Object returns a copy of obj with changes specified by the pattern/operation
            result = functions.Functions.function_clone(obj)

            matches = self._jsonata.eval(self._expr.pattern, result, self._environment)
            if matches is not None:
                if not (isinstance(matches, list)):
                    matches = [matches]
                for match_ in matches:
                    # evaluate the update value for each match
                    update = self._jsonata.eval(self._expr.update, match_, self._environment)
                    # update must be an object
                    # var updateType = typeof update
                    # if(updateType != null)

                    if update is not None:
                        if not (isinstance(update, dict)):
                            # throw type error
                            raise jexception.JException("T2011", self._expr.update.position, update)
                        # merge the update
                        for k in update.keys():
                            match_[k] = update[k]

                    # delete, if specified, must be an array of strings (or single string)
                    if self._expr.delete is not None:
                        deletions = self._jsonata.eval(self._expr.delete, match_, self._environment)
                        if deletions is not None:
                            val = deletions
                            if not (isinstance(deletions, list)):
                                deletions = [deletions]
                            if not utils.Utils.is_array_of_strings(deletions):
                                # throw type error
                                raise jexception.JException("T2012", self._expr.delete.position, val)
                            for item in deletions:
                                if isinstance(match_, dict):
                                    match_.pop(item, None)
                                    # delete match[deletions[jj]]

            return result

    #
    # Evaluate expression against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} Evaluated input data
    #     
    def eval(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any], environment: Optional[Frame]) -> Optional[Any]:
        # Thread safety:
        # Make sure each evaluate is executed on an instance per thread
        return self.get_per_thread_instance()._eval(expr, input, environment)

    def _eval(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any], environment: Optional[Frame]) -> Optional[Any]:
        result = None

        # Store the current input
        # This is required by Functions.functionEval for current $eval() input context
        self.input = input

        if self.parser.dbg:
            print("eval expr=" + str(expr) + " type=" + expr.type)  # +" input="+input);

        entry_callback = environment.lookup("__evaluate_entry")
        if entry_callback is not None:
            entry_callback(expr, input, environment)

        if getattr(expr, "type", None) is not None:
            if expr.type == "path":
                result = self.evaluate_path(expr, input, environment)
            elif expr.type == "binary":
                result = self.evaluate_binary(expr, input, environment)
            elif expr.type == "unary":
                result = self.evaluate_unary(expr, input, environment)
            elif expr.type == "name":
                result = self.evaluate_name(expr, input, environment)
                if self.parser.dbg:
                    print("evalName " + result)
            elif expr.type == "string" or expr.type == "number" or expr.type == "value":
                result = self.evaluate_literal(expr)  # , input, environment);
            elif expr.type == "wildcard":
                result = self.evaluate_wildcard(expr, input)  # , environment);
            elif expr.type == "descendant":
                result = self.evaluate_descendants(expr, input)  # , environment);
            elif expr.type == "parent":
                result = environment.lookup(expr.slot.label)
            elif expr.type == "condition":
                result = self.evaluate_condition(expr, input, environment)
            elif expr.type == "block":
                result = self.evaluate_block(expr, input, environment)
            elif expr.type == "bind":
                result = self.evaluate_bind_expression(expr, input, environment)
            elif expr.type == "regex":
                result = self.evaluate_regex(expr)  # , input, environment);
            elif expr.type == "function":
                result = self.evaluate_function(expr, input, environment, utils.Utils.NONE)
            elif expr.type == "variable":
                result = self.evaluate_variable(expr, input, environment)
            elif expr.type == "lambda":
                result = self.evaluate_lambda(expr, input, environment)
            elif expr.type == "partial":
                result = self.evaluate_partial_application(expr, input, environment)
            elif expr.type == "apply":
                result = self.evaluate_apply_expression(expr, input, environment)
            elif expr.type == "transform":
                result = self.evaluate_transform_expression(expr, input, environment)

        if getattr(expr, "predicate", None) is not None:
            for item in expr.predicate:
                result = self.evaluate_filter(item.expr, result, environment)

        if getattr(expr, "type", None) is not None and expr.type != "path" and getattr(expr, "group", None) is not None:
            result = self.evaluate_group_expression(expr.group, result, environment)

        exit_callback = environment.lookup("__evaluate_exit")
        if exit_callback is not None:
            exit_callback(expr, input, environment, result)

        # mangle result (list of 1 element -> 1 element, empty list -> null)
        if result is not None and utils.Utils.is_sequence(result) and not result.tuple_stream:
            if expr.keep_array:
                result.keep_singleton = True
            if not result:
                result = None
            elif len(result) == 1:
                result = result if result.keep_singleton else result[0]

        return result

    #
    # Evaluate path expression against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} Evaluated input data
    #    
    # async 
    def evaluate_path(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                      environment: Optional[Frame]) -> Optional[Any]:
        # expr is an array of steps
        # if the first step is a variable reference ($...), including root reference ($$),
        #   then the path is absolute rather than relative
        if isinstance(input, list) and expr.steps[0].type != "variable":
            input_sequence = input
        else:
            # if input is not an array, make it so
            input_sequence = utils.Utils.create_sequence(input)

        result_sequence = None
        is_tuple_stream = False
        tuple_bindings = None

        # evaluate each step in turn
        for ii, step in enumerate(expr.steps):

            if step.tuple is not None:
                is_tuple_stream = True

            # if the first step is an explicit array constructor, then just evaluate that (i.e. don"t iterate over a context array)
            if ii == 0 and step.consarray:
                result_sequence = self.eval(step, input_sequence, environment)
            else:
                if is_tuple_stream:
                    tuple_bindings = self.evaluate_tuple_step(step, input_sequence, tuple_bindings, environment)
                else:
                    result_sequence = self.evaluate_step(step, input_sequence, environment, ii == len(expr.steps) - 1)

            if not is_tuple_stream and (result_sequence is None or not result_sequence):
                break

            if step.focus is None:
                input_sequence = result_sequence

        if is_tuple_stream:
            if expr.tuple is not None:
                # tuple stream is carrying ancestry information - keep this
                result_sequence = tuple_bindings
            else:
                result_sequence = utils.Utils.create_sequence_from_iter(b["@"] for b in tuple_bindings)

        if expr.keep_singleton_array:

            # If we only got an ArrayList, convert it so we can set the keepSingleton flag
            if not (isinstance(result_sequence, utils.Utils.JList)):
                result_sequence = utils.Utils.JList(result_sequence)

            # if the array is explicitly constructed in the expression and marked to promote singleton sequences to array
            if (isinstance(result_sequence, utils.Utils.JList)) and result_sequence.cons and not result_sequence.sequence:
                result_sequence = utils.Utils.create_sequence(result_sequence)
            result_sequence.keep_singleton = True

        if expr.group is not None:
            result_sequence = self.evaluate_group_expression(expr.group,
                                                             tuple_bindings if is_tuple_stream else result_sequence,
                                                             environment)

        return result_sequence

    def create_frame_from_tuple(self, environment: Optional[Frame], tuple: Optional[Mapping[str, Any]]) -> Frame:
        frame = self.create_frame(environment)
        if tuple is not None:
            for prop, val in tuple.items():
                frame.bind(prop, val)
        return frame

    #
    # Evaluate a step within a path
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @param {boolean} lastStep - flag the last step in a path
    # @returns {*} Evaluated input data
    #    
    # async 
    def evaluate_step(self, expr: parser.Parser.Symbol, input: Optional[Any], environment: Optional[Frame],
                      last_step: bool) -> Optional[Any]:
        if expr.type == "sort":
            result = self.evaluate_sort_expression(expr, input, environment)
            if expr.stages is not None:
                result = self.evaluate_stages(expr.stages, result, environment)
            return result

        result = utils.Utils.create_sequence()

        for inp in input:
            res = self.eval(expr, inp, environment)
            if expr.stages is not None:
                for stage in expr.stages:
                    res = self.evaluate_filter(stage.expr, res, environment)
            if res is not None:
                result.append(res)

        result_sequence = utils.Utils.create_sequence()
        if last_step and len(result) == 1 and (isinstance(result[0], list)) and not utils.Utils.is_sequence(
                result[0]):
            result_sequence = result[0]
        else:
            # flatten the sequence
            for res in result:
                if not (isinstance(res, list)) or (isinstance(res, utils.Utils.JList) and res.cons):
                    # it's not an array - just push into the result sequence
                    result_sequence.append(res)
                else:
                    # res is a sequence - flatten it into the parent sequence
                    result_sequence.extend(res)

        return result_sequence

    # async 
    def evaluate_stages(self, stages: Optional[Sequence[parser.Parser.Symbol]], input: Any,
                        environment: Optional[Frame]) -> Any:
        result = input
        for stage in stages:
            if stage.type == "filter":
                result = self.evaluate_filter(stage.expr, result, environment)
            elif stage.type == "index":
                for ee, tuple in enumerate(result):
                    tuple[str(stage.value)] = ee
        return result

    #
    # Evaluate a step within a path
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} tupleBindings - The tuple stream
    # @param {Object} environment - Environment
    # @returns {*} Evaluated input data
    #    
    # async 
    def evaluate_tuple_step(self, expr: parser.Parser.Symbol, input: Optional[Sequence],
                            tuple_bindings: Optional[Sequence[Mapping[str, Any]]],
                            environment: Optional[Frame]) -> Optional[Any]:
        result = None
        if expr.type == "sort":
            if tuple_bindings is not None:
                result = self.evaluate_sort_expression(expr, tuple_bindings, environment)
            else:
                sorted = self.evaluate_sort_expression(expr, input, environment)
                result = utils.Utils.create_sequence_from_iter({"@": item, expr.index: ss}
                                                               for ss, item in enumerate(sorted))
                result.tuple_stream = True
            if expr.stages is not None:
                result = self.evaluate_stages(expr.stages, result, environment)
            return result

        result = utils.Utils.create_sequence()
        result.tuple_stream = True
        step_env = environment
        if tuple_bindings is None:
            tuple_bindings = [{"@": item} for item in input if item is not None]

        for tuple_binding in tuple_bindings:
            step_env = self.create_frame_from_tuple(environment, tuple_binding)
            res = self.eval(expr, tuple_binding["@"], step_env)
            # res is the binding sequence for the output tuple stream
            if res is not None:
                if not (isinstance(res, list)):
                    res = [res]
                for bb, item in enumerate(res):
                    tuple = dict(tuple_binding)
                    # Object.assign(tuple, tupleBindings[ee])
                    if (isinstance(res, utils.Utils.JList)) and res.tuple_stream:
                        tuple.update(item)
                    else:
                        if expr.focus is not None:
                            tuple[expr.focus] = item
                            tuple["@"] = tuple_binding["@"]
                        else:
                            tuple["@"] = item
                        if expr.index is not None:
                            tuple[expr.index] = bb
                        if expr.ancestor is not None:
                            tuple[expr.ancestor.label] = tuple_binding["@"]
                    result.append(tuple)

        if expr.stages is not None:
            result = self.evaluate_stages(expr.stages, result, environment)

        return result

    #
    # Apply filter predicate to input data
    # @param {Object} predicate - filter expression
    # @param {Object} input - Input data to apply predicates against
    # @param {Object} environment - Environment
    # @returns {*} Result after applying predicates
    #    
    # async 
    def evaluate_filter(self, predicate: Optional[Any], input: Optional[Any], environment: Optional[Frame]) -> Any:
        results = utils.Utils.create_sequence()
        if isinstance(input, utils.Utils.JList) and input.tuple_stream:
            results.tuple_stream = True
        if not (isinstance(input, list)):
            input = utils.Utils.create_sequence(input)
        if predicate.type == "number":
            index = int(predicate.value)  # round it down - was Math.floor
            if index < 0:
                # count in from end of array
                index = len(input) + index
            item = input[index] if index < len(input) else None
            if item is not None:
                if isinstance(item, list):
                    results = item
                else:
                    results.append(item)
        else:
            for index, item in enumerate(input):
                context = item
                env = environment
                if isinstance(input, utils.Utils.JList) and input.tuple_stream:
                    context = item["@"]
                    env = self.create_frame_from_tuple(environment, item)
                res = self.eval(predicate, context, env)
                if utils.Utils.is_numeric(res):
                    res = utils.Utils.create_sequence(res)
                if utils.Utils.is_array_of_numbers(res):
                    for ires in res:
                        # round it down
                        ii = int(ires)  # Math.floor(ires);
                        if ii < 0:
                            # count in from end of array
                            ii = len(input) + ii
                        if ii == index:
                            results.append(item)
                elif Jsonata.boolize(res):
                    results.append(item)
        return results

    #
    # Evaluate binary expression against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} Evaluated input data
    #    
    # async 
    def evaluate_binary(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                        environment: Optional[Frame]) -> Optional[Any]:
        lhs = self.eval(expr.lhs, input, environment)
        op = str(expr.value)

        if op == "and" or op == "or":

            # defer evaluation of RHS to allow short-circuiting
            evalrhs = lambda: self.eval(expr.rhs, input, environment)
            try:
                return self.evaluate_boolean_expression(lhs, evalrhs, op)
            except Exception as err:
                if not (isinstance(err, jexception.JException)):
                    raise jexception.JException("Unexpected", expr.position)
                # err.position = expr.position
                # err.token = op
                raise err

        rhs = self.eval(expr.rhs, input, environment)  # evalrhs();
        try:
            if op == "+" or op == "-" or op == "*" or op == "/" or op == "%":
                result = self.evaluate_numeric_expression(lhs, rhs, op)
            elif op == "=" or op == "!=":
                result = self.evaluate_equality_expression(lhs, rhs, op)
            elif op == "<" or op == "<=" or op == ">" or op == ">=":
                result = self.evaluate_comparison_expression(lhs, rhs, op)
            elif op == "&":
                result = self.evaluate_string_concat(lhs, rhs)
            elif op == "..":
                result = self.evaluate_range_expression(lhs, rhs)
            elif op == "in":
                result = self.evaluate_includes_expression(lhs, rhs)
            else:
                raise jexception.JException("Unexpected operator " + op, expr.position)
        except Exception as err:
            # err.position = expr.position
            # err.token = op
            raise err
        return result

    #
    # Evaluate unary expression against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} Evaluated input data
    #    
    # async 
    def evaluate_unary(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                       environment: Optional[Frame]) -> Optional[Any]:
        result = None

        value = str(expr.value)
        if value == "-":
            result = self.eval(expr.expression, input, environment)
            if result is None:
                result = None
            elif utils.Utils.is_numeric(result):
                result = utils.Utils.convert_number(-float(result))
            else:
                raise jexception.JException("D1002", expr.position, expr.value, result)
        elif value == "[":
            # array constructor - evaluate each item
            result = utils.Utils.JList()  # [];
            idx = 0
            for item in expr.expressions:
                environment.is_parallel_call = idx > 0
                value = self.eval(item, input, environment)
                if value is not None:
                    if str(item.value) == "[":
                        result.append(value)
                    else:
                        result = functions.Functions.append(result, value)
                idx += 1
            if expr.consarray:
                if not (isinstance(result, utils.Utils.JList)):
                    result = utils.Utils.JList(result)
                # System.out.println("const "+result)
                result.cons = True
        elif value == "{":
            # object constructor - apply grouping
            result = self.evaluate_group_expression(expr, input, environment)

        return result

    #
    # Evaluate name object against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} Evaluated input data
    #    
    def evaluate_name(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                      environment: Optional[Frame]) -> Optional[Any]:
        # lookup the "name" item in the input
        return functions.Functions.lookup(input, str(expr.value))

    #
    # Evaluate literal against input data
    # @param {Object} expr - JSONata expression
    # @returns {*} Evaluated input data
    #     
    def evaluate_literal(self, expr: Optional[parser.Parser.Symbol]) -> Optional[Any]:
        return expr.value if expr.value is not None else utils.Utils.NULL_VALUE

    #
    # Evaluate wildcard against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @returns {*} Evaluated input data
    #    
    def evaluate_wildcard(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any]) -> Optional[Any]:
        results = utils.Utils.create_sequence()
        if (isinstance(input, utils.Utils.JList)) and input.outer_wrapper and input:
            input = input[0]
        if input is not None and isinstance(input, dict):
            for value in input.values():
                if isinstance(value, list):
                    value = self.flatten(value, None)
                    results = functions.Functions.append(results, value)
                else:
                    results.append(value)
        elif isinstance(input, list):
            # Java: need to handle List separately
            for value in input:
                if isinstance(value, list):
                    value = self.flatten(value, None)
                    results = functions.Functions.append(results, value)
                elif isinstance(value, dict):
                    # Call recursively do decompose the map
                    results.extend(self.evaluate_wildcard(expr, value))
                else:
                    results.append(value)

        # result = normalizeSequence(results)
        return results

    #
    # Returns a flattened array
    # @param {Array} arg - the array to be flatten
    # @param {Array} flattened - carries the flattened array - if not defined, will initialize to []
    # @returns {Array} - the flattened array
    #    
    def flatten(self, arg: Any, flattened: Optional[MutableSequence]) -> Any:
        if flattened is None:
            flattened = []
        if isinstance(arg, list):
            for item in arg:
                self.flatten(item, flattened)
        else:
            flattened.append(arg)
        return flattened

    #
    # Evaluate descendants against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @returns {*} Evaluated input data
    #    
    def evaluate_descendants(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any]) -> Optional[Any]:
        result = None
        result_sequence = utils.Utils.create_sequence()
        if input is not None:
            # traverse all descendants of this object/array
            self.recurse_descendants(input, result_sequence)
            if len(result_sequence) == 1:
                result = result_sequence[0]
            else:
                result = result_sequence
        return result

    #
    # Recurse through descendants
    # @param {Object} input - Input data
    # @param {Object} results - Results
    #    
    def recurse_descendants(self, input: Optional[Any], results: MutableSequence) -> None:
        # this is the equivalent of //* in XPath
        if not (isinstance(input, list)):
            results.append(input)
        if isinstance(input, list):
            for member in input:
                self.recurse_descendants(member, results)
        elif input is not None and isinstance(input, dict):
            for value in input.values():
                self.recurse_descendants(value, results)

    #
    # Evaluate numeric expression against input data
    # @param {Object} lhs - LHS value
    # @param {Object} rhs - RHS value
    # @param {Object} op - opcode
    # @returns {*} Result
    #     
    def evaluate_numeric_expression(self, lhs: Optional[Any], rhs: Optional[Any], op: Optional[str]) -> Optional[Any]:
        result = 0

        if lhs is not None and not utils.Utils.is_numeric(lhs):
            raise jexception.JException("T2001", -1, op, lhs)
        if rhs is not None and not utils.Utils.is_numeric(rhs):
            raise jexception.JException("T2002", -1, op, rhs)

        if lhs is None or rhs is None:
            # if either side is undefined, the result is undefined
            return None

        # System.out.println("op22 "+op+" "+_lhs+" "+_rhs)
        lhs = float(lhs)
        rhs = float(rhs)

        if op == "+":
            result = lhs + rhs
        elif op == "-":
            result = lhs - rhs
        elif op == "*":
            result = lhs * rhs
        elif op == "/":
            result = lhs / rhs
        elif op == "%":
            result = int(math.fmod(lhs, rhs))
        return utils.Utils.convert_number(result)

    #
    # Evaluate equality expression against input data
    # @param {Object} lhs - LHS value
    # @param {Object} rhs - RHS value
    # @param {Object} op - opcode
    # @returns {*} Result
    #      
    def evaluate_equality_expression(self, lhs: Optional[Any], rhs: Optional[Any], op: Optional[str]) -> Optional[Any]:
        if lhs is None or rhs is None:
            # if either side is undefined, the result is false
            return False

        # JSON might come with integers,
        # convert all to double...
        # FIXME: semantically OK?
        if not isinstance(lhs, bool) and isinstance(lhs, (int, float)):
            lhs = float(lhs)
        if not isinstance(rhs, bool) and isinstance(rhs, (int, float)):
            rhs = float(rhs)

        result = None
        if op == "=":
            result = utils.Utils.is_deep_equal(lhs, rhs)
        elif op == "!=":
            result = not utils.Utils.is_deep_equal(lhs, rhs)
        return result

    #
    # Evaluate comparison expression against input data
    # @param {Object} lhs - LHS value
    # @param {Object} rhs - RHS value
    # @param {Object} op - opcode
    # @returns {*} Result
    #      
    def evaluate_comparison_expression(self, lhs: Optional[Any], rhs: Optional[Any], op: Optional[str]) -> Optional[Any]:
        result = None

        # type checks
        lcomparable = (lhs is None or isinstance(lhs, str) or
                       (not isinstance(lhs, bool) and isinstance(lhs, (int, float))))
        rcomparable = (rhs is None or isinstance(rhs, str) or
                       (not isinstance(rhs, bool) and isinstance(rhs, (int, float))))

        # if either aa or bb are not comparable (string or numeric) values, then throw an error
        if not lcomparable or not rcomparable:
            raise jexception.JException("T2010", 0, op, lhs if lhs is not None else rhs)

        # if either side is undefined, the result is undefined
        if lhs is None or rhs is None:
            return None

        # if aa and bb are not of the same type
        if type(lhs) is not type(rhs):

            if (not isinstance(lhs, bool) and isinstance(lhs, (int, float)) and
                    not isinstance(rhs, bool) and isinstance(rhs, (int, float))):
                # Java : handle Double / Integer / Long comparisons
                # convert all to double -> loss of precision (64-bit long to double) be a problem here?
                lhs = float(lhs)
                rhs = float(rhs)

            else:

                raise jexception.JException("T2009", 0, lhs, rhs)

        if op == "<":
            result = lhs < rhs
        elif op == "<=":
            result = lhs <= rhs
        elif op == ">":
            result = lhs > rhs
        elif op == ">=":
            result = lhs >= rhs
        return result

    #
    # Inclusion operator - in
    #
    # @param {Object} lhs - LHS value
    # @param {Object} rhs - RHS value
    # @returns {boolean} - true if lhs is a member of rhs
    #      
    def evaluate_includes_expression(self, lhs: Optional[Any], rhs: Optional[Any]) -> Any:
        result = False

        if lhs is None or rhs is None:
            # if either side is undefined, the result is false
            return False

        if not (isinstance(rhs, list)):
            rhs = [rhs]

        for item in rhs:
            if item == lhs:
                result = True
                break

        return result

    #
    # Evaluate boolean expression against input data
    # @param {Object} lhs - LHS value
    # @param {functions.Function} evalrhs - Object to evaluate RHS value
    # @param {Object} op - opcode
    # @returns {*} Result
    #     
    # async 
    def evaluate_boolean_expression(self, lhs: Optional[Any], evalrhs: Callable[[], Optional[Any]],
                                    op: Optional[str]) -> Optional[Any]:
        result = None

        l_bool = Jsonata.boolize(lhs)

        if op == "and":
            result = l_bool and Jsonata.boolize(evalrhs())
        elif op == "or":
            result = l_bool or Jsonata.boolize(evalrhs())
        return result

    @staticmethod
    def boolize(value: Optional[Any]) -> bool:
        booled_value = functions.Functions.to_boolean(value)
        return False if booled_value is None else booled_value

    #
    # Evaluate string concatenation against input data
    # @param {Object} lhs - LHS value
    # @param {Object} rhs - RHS value
    # @returns {string|*} Concatenated string
    #     
    def evaluate_string_concat(self, lhs: Optional[Any], rhs: Optional[Any]) -> str:
        lstr = ""
        rstr = ""
        if lhs is not None:
            lstr = functions.Functions.string(lhs, None)
        if rhs is not None:
            rstr = functions.Functions.string(rhs, None)

        result = lstr + rstr
        return result

    @dataclass
    class GroupEntry:
        data: Optional[Any]
        exprIndex: int

    #
    # Evaluate group expression against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {{}} Evaluated input data
    #     
    # async 
    def evaluate_group_expression(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                                  environment: Optional[Frame]) -> Any:
        result = {}
        groups = {}
        reduce = True if (isinstance(input, utils.Utils.JList)) and input.tuple_stream else False
        # group the input sequence by "key" expression
        if not (isinstance(input, list)):
            input = utils.Utils.create_sequence(input)

        # if the array is empty, add an undefined entry to enable literal JSON object to be generated
        if not input:
            input.append(None)

        for itemIndex, item in enumerate(input):
            env = self.create_frame_from_tuple(environment, item) if reduce else environment
            for pairIndex, pair in enumerate(expr.lhs_object):
                key = self.eval(pair[0], item["@"] if reduce else item, env)
                # key has to be a string
                if key is not None and not (isinstance(key, str)):
                    raise jexception.JException("T1003", expr.position, key)

                if key is not None:
                    entry = Jsonata.GroupEntry(item, pairIndex)
                    if groups.get(key) is not None:
                        # a value already exists in this slot
                        if groups[key].exprIndex != pairIndex:
                            # this key has been generated by another expression in this group
                            # when multiple key expressions evaluate to the same key, then error D1009 must be thrown
                            raise jexception.JException("D1009", expr.position, key)

                        # append it as an array
                        groups[key].data = functions.Functions.append(groups[key].data, item)
                    else:
                        groups[key] = entry

        # iterate over the groups to evaluate the "value" expression
        # let generators = /* await */ Promise.all(Object.keys(groups).map(/* async */ (key, idx) => {
        idx = 0
        for k, v in groups.items():
            entry = v
            context = entry.data
            env = environment
            if reduce:
                tuple = self.reduce_tuple_stream(entry.data)
                context = tuple["@"]
                tuple.pop("@", None)
                env = self.create_frame_from_tuple(environment, tuple)
            env.is_parallel_call = idx > 0
            # return [key, /* await */ eval(expr.lhs[entry.exprIndex][1], context, env)]
            res = self.eval(expr.lhs_object[entry.exprIndex][1], context, env)
            if res is not None:
                result[k] = res

            idx += 1

        #  for (let generator of generators) {
        #      var [key, value] = /* await */ generator
        #      if(typeof value !== "undefined") {
        #          result[key] = value
        #      }
        #  }

        return result

    def reduce_tuple_stream(self, tuple_stream: Optional[Any]) -> Optional[Any]:
        if not (isinstance(tuple_stream, list)):
            return tuple_stream

        result = dict(tuple_stream[0])

        # Object.assign(result, tuple_stream[0])
        for ii in range(1, len(tuple_stream)):
            el = tuple_stream[ii]
            for k, v in el.items():
                result[k] = functions.Functions.append(result[k], v)
        return result

    #
    # Evaluate range expression against input data
    # @param {Object} lhs - LHS value
    # @param {Object} rhs - RHS value
    # @returns {Array} Resultant array
    #     
    def evaluate_range_expression(self, lhs: Optional[Any], rhs: Optional[Any]) -> Optional[Any]:
        result = None

        if lhs is not None and (isinstance(lhs, bool) or not (isinstance(lhs, int))):
            raise jexception.JException("T2003", -1, lhs)
        if rhs is not None and (isinstance(rhs, bool) or not (isinstance(rhs, int))):
            raise jexception.JException("T2004", -1, rhs)

        if rhs is None or lhs is None:
            # if either side is undefined, the result is undefined
            return result

        lhs = int(lhs)
        rhs = int(rhs)

        if lhs > rhs:
            # if the lhs is greater than the rhs, return undefined
            return result

        # limit the size of the array to ten million entries (1e7)
        # this is an implementation defined limit to protect against
        # memory and performance issues.  This value may increase in the future.
        size = rhs - lhs + 1
        if size > 1e7:
            raise jexception.JException("D2014", -1, size)

        return utils.Utils.RangeList(lhs, rhs + 1)

    #
    # Evaluate bind expression against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} Evaluated input data
    #     
    # async 
    def evaluate_bind_expression(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                                 environment: Optional[Frame]) -> Optional[Any]:
        # The RHS is the expression to evaluate
        # The LHS is the name of the variable to bind to - should be a VARIABLE token (enforced by parser)
        value = self.eval(expr.rhs, input, environment)
        environment.bind(str(expr.lhs.value), value)
        return value

    #
    # Evaluate condition against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} Evaluated input data
    #     
    # async 
    def evaluate_condition(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                           environment: Optional[Frame]) -> Optional[Any]:
        result = None
        condition = self.eval(expr.condition, input, environment)
        if Jsonata.boolize(condition):
            result = self.eval(expr.then, input, environment)
        elif expr._else is not None:
            result = self.eval(expr._else, input, environment)
        return result

    #
    # Evaluate block against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} Evaluated input data
    #      
    # async 
    def evaluate_block(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                       environment: Optional[Frame]) -> Optional[Any]:
        result = None
        # create a new frame to limit the scope of variable assignments
        # TODO, only do this if the post-parse stage has flagged this as required
        frame = self.create_frame(environment)
        # invoke each expression in turn
        # only return the result of the last one
        for ex in expr.expressions:
            result = self.eval(ex, input, frame)

        return result

    #
    # Prepare a regex
    # @param {Object} expr - expression containing regex
    # @returns {functions.Function} Higher order Object representing prepared regex
    #      
    def evaluate_regex(self, expr: Optional[parser.Parser.Symbol]) -> Optional[Any]:
        # Note: in Java we just use the compiled regex Pattern
        # The apply functions need to take care to evaluate
        return expr.value

    #
    # Evaluate variable against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} Evaluated input data
    #      
    def evaluate_variable(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                          environment: Optional[Frame]) -> Optional[Any]:
        # lookup the variable value in the environment
        result = None
        # if the variable name is empty string, then it refers to context value
        if expr.value == "":
            # Empty string == "$" !
            result = input[0] if isinstance(input, utils.Utils.JList) and input.outer_wrapper else input
        else:
            result = environment.lookup(str(expr.value))
            if self.parser.dbg:
                print("variable name=" + expr.value + " val=" + result)
        return result

    #
    # sort / order-by operator
    # @param {Object} expr - AST for operator
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} Ordered sequence
    #      
    # async 
    def evaluate_sort_expression(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                                 environment: Optional[Frame]) -> Optional[Any]:
        result = None

        # evaluate the lhs, then sort the results in order according to rhs expression
        lhs = input
        is_tuple_sort = True if (isinstance(input, utils.Utils.JList) and input.tuple_stream) else False

        # sort the lhs array
        # use comparator function
        comparator = Jsonata.ComparatorWrapper(self, expr, environment, is_tuple_sort).compare

        #  var focus = {
        #      environment: environment,
        #      input: input
        #  }
        #  // the `focus` is passed in as the `this` for the invoked function
        #  result = /* await */ fn.sort.apply(focus, [lhs, comparator])

        result = functions.Functions.sort(lhs, comparator)
        return result

    class ComparatorWrapper:
        _outer_instance: 'Jsonata'
        _expr: Optional[parser.Parser.Symbol]
        _environment: 'Jsonata.Optional[Frame]'
        _is_tuple_sort: bool

        def __init__(self, outer_instance, expr, environment, is_tuple_sort):
            self._outer_instance = outer_instance
            self._expr = expr
            self._environment = environment
            self._is_tuple_sort = is_tuple_sort

        def compare(self, a, b):

            # expr.terms is an array of order-by in priority order
            comp = 0
            index = 0
            while comp == 0 and index < len(self._expr.terms):
                term = self._expr.terms[index]
                # evaluate the sort term in the context of a
                context = a
                env = self._environment
                if self._is_tuple_sort:
                    context = a["@"]
                    env = self._outer_instance.create_frame_from_tuple(self._environment, a)
                aa = self._outer_instance.eval(term.expression, context, env)

                # evaluate the sort term in the context of b
                context = b
                env = self._environment
                if self._is_tuple_sort:
                    context = b["@"]
                    env = self._outer_instance.create_frame_from_tuple(self._environment, b)
                bb = self._outer_instance.eval(term.expression, context, env)

                # type checks
                #  var atype = typeof aa
                #  var btype = typeof bb
                # undefined should be last in sort order
                if aa is None:
                    # swap them, unless btype is also undefined
                    comp = 0 if (bb is None) else 1
                    index += 1
                    continue
                if bb is None:
                    comp = -1
                    index += 1
                    continue

                # if aa or bb are not string or numeric values, then throw an error
                if (not ((not isinstance(aa, bool) and isinstance(aa, (int, float))) or isinstance(aa, str)) or
                        not ((not isinstance(bb, bool) and isinstance(bb, (int, float))) or isinstance(bb, str))):
                    raise jexception.JException("T2008", self._expr.position, aa, bb)

                # if aa and bb are not of the same type
                same_type = False
                if (not isinstance(aa, bool) and isinstance(aa, (int, float)) and
                        not isinstance(bb, bool) and isinstance(bb, (int, float))):
                    same_type = True
                elif issubclass(type(bb), type(aa)) or issubclass(type(aa), type(bb)):
                    same_type = True

                if not same_type:
                    raise jexception.JException("T2007", self._expr.position, aa, bb)
                if aa == bb:
                    # both the same - move on to next term
                    index += 1
                    continue
                elif aa < bb:
                    comp = -1
                else:
                    comp = 1
                if term.descending:
                    comp = -comp
                index += 1
            # only swap a & b if comp equals 1
            # return comp == 1
            return comp

    #
    # create a transformer function
    # @param {Object} expr - AST for operator
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} tranformer function
    #
    def evaluate_transform_expression(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                                      environment: Optional[Frame]) -> Optional[Any]:
        # create a Object to implement the transform definition
        transformer = Jsonata.Transformer(self, expr, environment)
        return Jsonata.JFunction(transformer, "<(oa):o>")

    _chain_ast = None  # = new Parser().parse("function($f, $g) { function($x){ $g($f($x)) } }");

    @staticmethod
    def chain_ast() -> Optional[parser.Parser.Symbol]:
        if Jsonata._chain_ast is None:
            # only create on demand
            Jsonata._chain_ast = (parser.Parser()).parse("function($f, $g) { function($x){ $g($f($x)) } }")
        return Jsonata._chain_ast

    #
    # Apply the Object on the RHS using the sequence on the LHS as the first argument
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} Evaluated input data
    #      
    # async 
    def evaluate_apply_expression(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                                  environment: Optional[Frame]) -> Optional[Any]:
        result = None

        lhs = self.eval(expr.lhs, input, environment)

        if expr.rhs.type == "function":
            # Symbol applyTo = new Symbol(); applyTo.context = lhs
            # this is a Object _invocation_; invoke it with lhs expression as the first argument
            result = self.evaluate_function(expr.rhs, input, environment, lhs)
        else:
            func = self.eval(expr.rhs, input, environment)

            if not self.is_function_like(func) and not self.is_function_like(lhs):
                raise jexception.JException("T2006", expr.position, func)

            if self.is_function_like(lhs):
                # this is Object chaining (func1 ~> func2)
                # λ($f, $g) { λ($x){ $g($f($x)) } }
                chain = self.eval(Jsonata.chain_ast(), None, environment)
                args = [lhs, func]
                result = self.apply(chain, args, None, environment)
            else:
                args = [lhs]
                result = self.apply(func, args, None, environment)

        return result

    def is_function_like(self, o: Optional[Any]) -> bool:
        return utils.Utils.is_function(o) or functions.Functions.is_lambda(o) or (isinstance(o, re.Pattern))

    CURRENT = threading.local()
    MUTEX = threading.Lock()

    #
    # Returns a per thread instance of this parsed expression.
    # 
    # @return
    #     
    def get_per_thread_instance(self):
        if hasattr(Jsonata.CURRENT, "jsonata"):
            return Jsonata.CURRENT.jsonata

        with Jsonata.MUTEX:
            if hasattr(Jsonata.CURRENT, "jsonata"):
                return Jsonata.CURRENT.jsonata
            thread_inst = copy.copy(self)
            Jsonata.CURRENT.jsonata = thread_inst
            return thread_inst

    #
    # Evaluate Object against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} Evaluated input data
    #      
    # async 
    def evaluate_function(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any], environment: Optional[Frame],
                          applyto_context: Optional[Any]) -> Optional[Any]:
        # this.current is set by getPerThreadInstance() at this point

        # create the procedure
        # can"t assume that expr.procedure is a lambda type directly
        # could be an expression that evaluates to a Object (e.g. variable reference, parens expr etc.
        # evaluate it generically first, then check that it is a function.  Throw error if not.
        proc = self.eval(expr.procedure, input, environment)

        if (proc is None and getattr(expr.procedure, "type", None) is not None and
                expr.procedure.type == "path" and environment.lookup(str(expr.procedure.steps[0].value)) is not None):
            # help the user out here if they simply forgot the leading $
            raise jexception.JException("T1005", expr.position, expr.procedure.steps[0].value)

        evaluated_args = []

        if applyto_context is not utils.Utils.NONE:
            evaluated_args.append(applyto_context)
        # eager evaluation - evaluate the arguments
        args = expr.arguments if expr.arguments is not None else []
        for val in args:
            arg = self.eval(val, input, environment)
            if utils.Utils.is_function(arg) or functions.Functions.is_lambda(arg):
                # wrap this in a closure
                # Java: not required, already a JFunction
                #  const closure = /* async */ Object (...params) {
                #      // invoke func
                #      return /* await */ apply(arg, params, null, environment)
                #  }
                #  closure.arity = getFunctionArity(arg)

                # JFunctionCallable fc = (ctx,params) ->
                #     apply(arg, params, null, environment)

                # JFunction cl = new JFunction(fc, "<o:o>")

                # Object cl = apply(arg, params, null, environment)
                evaluated_args.append(arg)
            else:
                evaluated_args.append(arg)
        # apply the procedure
        proc_val = expr.procedure.value if expr.procedure is not None else None
        proc_name = expr.procedure.steps[0].value if getattr(expr.procedure, "type",
                                                             None) is not None and expr.procedure.type == "path" else proc_val

        # Error if proc is null
        if proc is None:
            raise jexception.JException("T1006", expr.position, proc_name)

        try:
            if isinstance(proc, parser.Parser.Symbol):
                proc.token = proc_name
                proc.position = expr.position
            result = self.apply(proc, evaluated_args, input, environment)
        except jexception.JException as jex:
            if jex.location < 0:
                # add the position field to the error
                jex.location = expr.position
            if jex.current is None:
                # and the Object identifier
                jex.current = expr.token
            raise jex
        except Exception as err:
            raise err
        return result

    #
    # Apply procedure or function
    # @param {Object} proc - Procedure
    # @param {Array} args - Arguments
    # @param {Object} input - input
    # @param {Object} environment - environment
    # @returns {*} Result of procedure
    #      
    # async 
    def apply(self, proc: Optional[Any], args: Optional[Any], input: Optional[Any], environment: Optional[Frame]) -> Optional[Any]:
        result = self.apply_inner(proc, args, input, environment)
        while functions.Functions.is_lambda(result) and result.thunk:
            # trampoline loop - this gets invoked as a result of tail-call optimization
            # the Object returned a tail-call thunk
            # unpack it, evaluate its arguments, and apply the tail call
            next = self.eval(result.body.procedure, result.input, result.environment)
            if result.body.procedure.type == "variable":
                if isinstance(next, parser.Parser.Symbol):  # Java: not if JFunction
                    next.token = result.body.procedure.value
            if isinstance(next, parser.Parser.Symbol):  # Java: not if JFunction
                next.position = result.body.procedure.position
            evaluated_args = []
            for arg in result.body.arguments:
                evaluated_args.append(self.eval(arg, result.input, result.environment))

            result = self.apply_inner(next, evaluated_args, input, environment)
        return result

    #
    # Apply procedure or function
    # @param {Object} proc - Procedure
    # @param {Array} args - Arguments
    # @param {Object} input - input
    # @param {Object} environment - environment
    # @returns {*} Result of procedure
    #      
    # async 
    def apply_inner(self, proc: Optional[Any], args: Optional[Any], input: Optional[Any],
                    environment: Optional[Frame]) -> Optional[Any]:
        try:
            validated_args = args
            if proc is not None:
                validated_args = self.validate_arguments(proc, args, input)

            if functions.Functions.is_lambda(proc):
                result = self.apply_procedure(proc,
                                              validated_args)  # FIXME: need in Java??? else if (proc && proc._jsonata_Object == true) {
            #                 var focus = {
            #                     environment: environment,
            #                     input: input
            #                 }
            #                 // the `focus` is passed in as the `this` for the invoked function
            #                 result = proc.implementation.apply(focus, validated_args)
            #                 // `proc.implementation` might be a generator function
            #                 // and `result` might be a generator - if so, yield
            #                 if (isIterable(result)) {
            #                     result = result.next().value
            #                 }
            #                 if (isPromise(result)) {
            #                     result = /await/ result
            #                 } 
            #             } 
            elif isinstance(proc, Jsonata.JFunction):
                # typically these are functions that are returned by the invocation of plugin functions
                # the `input` is being passed in as the `this` for the invoked function
                # this is so that functions that return objects containing functions can chain
                # e.g. /* await */ (/* await */ $func())

                # handling special case of Javascript:
                # when calling a function with fn.apply(ctx, args) and args = [undefined]
                # Javascript will convert to undefined (without array)
                if isinstance(validated_args, list) and len(validated_args) == 1 and validated_args[0] is None:
                    # validated_args = null
                    pass

                result = proc.call(input, validated_args)
                #  if (isPromise(result)) {
                #      result = /* await */ result
                #  }
            elif isinstance(proc, Jsonata.JLambda):
                result = proc.call(input, validated_args)
            elif isinstance(proc, re.Pattern):
                result = [s for s in validated_args if proc.search(s) is not None]
            else:
                print("Proc not found " + str(proc))
                raise jexception.JException("T1006", 0)
        except jexception.JException as err:
            #  if(proc) {
            #      if (typeof err.token == "undefined" && typeof proc.token !== "undefined") {
            #          err.token = proc.token
            #      }
            #      err.position = proc.position
            #  }
            raise err
        return result

    #
    # Evaluate lambda against input data
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {{lambda: boolean, input: *, environment: *, arguments: *, body: *}} Evaluated input data
    #      
    def evaluate_lambda(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                        environment: Optional[Frame]) -> Optional[Any]:
        # make a Object (closure)
        procedure = parser.Parser.Symbol(self.parser)

        procedure._jsonata_lambda = True
        procedure.input = input
        procedure.environment = environment
        procedure.arguments = expr.arguments
        procedure.signature = expr.signature
        procedure.body = expr.body

        if expr.thunk:
            procedure.thunk = True

        # procedure.apply = /* async */ function(self, args) {
        #     return /* await */ apply(procedure, args, input, !!self ? self.environment : environment)
        # }
        return procedure

    #
    # Evaluate partial application
    # @param {Object} expr - JSONata expression
    # @param {Object} input - Input data to evaluate against
    # @param {Object} environment - Environment
    # @returns {*} Evaluated input data
    #      
    # async 
    def evaluate_partial_application(self, expr: Optional[parser.Parser.Symbol], input: Optional[Any],
                                     environment: Optional[Frame]) -> Optional[Any]:
        # partially apply a function
        result = None
        # evaluate the arguments
        evaluated_args = []
        for arg in expr.arguments:
            if arg.type == "operator" and (arg.value == "?"):
                evaluated_args.append(arg)
            else:
                evaluated_args.append(self.eval(arg, input, environment))
        # lookup the procedure
        proc = self.eval(expr.procedure, input, environment)
        if proc is not None and expr.procedure.type == "path" and environment.lookup(
                str(expr.procedure.steps[0].value)) is not None:
            # help the user out here if they simply forgot the leading $
            raise jexception.JException("T1007", expr.position, expr.procedure.steps[0].value)
        if functions.Functions.is_lambda(proc):
            result = self.partial_apply_procedure(proc, evaluated_args)
        elif utils.Utils.is_function(proc):
            result = self.partial_apply_native_function(proc, evaluated_args)
            #  } else if (typeof proc === "function") {
            #      result = partialApplyNativeFunction(proc, evaluated_args)
        else:
            raise jexception.JException("T1008", expr.position, expr.procedure.steps[
                0].value if expr.procedure.type == "path" else expr.procedure.value)
        return result

    #
    # Validate the arguments against the signature validator (if it exists)
    # @param {Function} signature - validator function
    # @param {Array} args - Object arguments
    # @param {*} context - context value
    # @returns {Array} - validated arguments
    #      
    def validate_arguments(self, signature: Any, args: Optional[Any], context: Optional[Any]) -> Optional[Any]:
        validated_args = args
        if utils.Utils.is_function(signature):
            validated_args = signature.validate(args, context)
        elif functions.Functions.is_lambda(signature):
            sig = signature.signature
            if sig is not None:
                validated_args = sig.validate(args, context)
        return validated_args

    #
    # Apply procedure
    # @param {Object} proc - Procedure
    # @param {Array} args - Arguments
    # @returns {*} Result of procedure
    #      
    # async 
    def apply_procedure(self, proc: Optional[Any], args: Optional[Any]) -> Optional[Any]:
        result = None
        env = self.create_frame(proc.environment)
        for i, arg in enumerate(proc.arguments):
            if i >= len(args):
                break
            env.bind(str(arg.value), args[i])
        if isinstance(proc.body, parser.Parser.Symbol):
            result = self.eval(proc.body, proc.input, env)
        else:
            raise RuntimeError("Cannot execute procedure: " + proc + " " + proc.body)
        #  if (typeof proc.body === "function") {
        #      // this is a lambda that wraps a native Object - generated by partially evaluating a native
        #      result = /* await */ applyNativeFunction(proc.body, env)
        return result

    #
    # Partially apply procedure
    # @param {Object} proc - Procedure
    # @param {Array} args - Arguments
    # @returns {{lambda: boolean, input: *, environment: {bind, lookup}, arguments: Array, body: *}} Result of partially applied procedure
    #      
    def partial_apply_procedure(self, proc: Optional[parser.Parser.Symbol], args: Sequence) -> parser.Parser.Symbol:
        # create a closure, bind the supplied parameters and return a Object that takes the remaining (?) parameters
        # Note Uli: if no env, bind to default env so the native functions can be found
        env = self.create_frame(proc.environment if proc.environment is not None else self.environment)
        unbound_args = []
        index = 0
        for param in proc.arguments:
            #         proc.arguments.forEach(Object (param, index) {
            arg = args[index] if index < len(args) else None
            if (arg is None) or (
                    isinstance(arg, parser.Parser.Symbol) and ("operator" == arg.type and "?" == arg.value)):
                unbound_args.append(param)
            else:
                env.bind(str(param.value), arg)
            index += 1
        procedure = parser.Parser.Symbol(self.parser)
        procedure._jsonata_lambda = True
        procedure.input = proc.input
        procedure.environment = env
        procedure.arguments = unbound_args
        procedure.body = proc.body

        return procedure

    #
    # Partially apply native function
    # @param {Function} native - Native function
    # @param {Array} args - Arguments
    # @returns {{lambda: boolean, input: *, environment: {bind, lookup}, arguments: Array, body: *}} Result of partially applying native function
    #      
    def partial_apply_native_function(self, native: Optional[JFunction], args: Sequence) -> parser.Parser.Symbol:
        # create a lambda Object that wraps and invokes the native function
        # get the list of declared arguments from the native function
        # this has to be picked out from the toString() value

        # var body = "function($a,$c) { $substring($a,0,$c) }"

        sig_args = []
        part_args = []
        i = 0
        while i < native.get_number_of_args():
            arg_name = "$" + chr(ord('a') + i)
            sig_args.append(arg_name)
            if i >= len(args) or args[i] is None:
                part_args.append(arg_name)
            else:
                part_args.append(args[i])
            i += 1

        body = "function(" + ", ".join(sig_args) + "){"
        body += "$" + native.function_name + "(" + ", ".join(sig_args) + ") }"

        if self.parser.dbg:
            print("partial trampoline = " + body)

        #  var sig_args = getNativeFunctionArguments(_native)
        #  sig_args = sig_args.stream().map(sigArg -> {
        #      return "$" + sigArg
        #  }).toList()
        #  var body = "function(" + String.join(", ", sig_args) + "){ _ }"

        body_ast = self.parser.parse(body)
        # body_ast.body = _native

        partial = self.partial_apply_procedure(body_ast, args)
        return partial

    #
    # Apply native function
    # @param {Object} proc - Procedure
    # @param {Object} env - Environment
    # @returns {*} Result of applying native function
    #      
    # async 
    def apply_native_function(self, proc: Optional[JFunction], env: Optional[Frame]) -> Optional[Any]:
        # Not called in Java - JFunction call directly calls native function
        return None

    #
    # Get native Object arguments
    # @param {Function} func - Function
    # @returns {*|Array} Native Object arguments
    #      
    def get_native_function_arguments(self, func: Optional[JFunction]) -> Optional[list]:
        # Not called in Java
        return None

    #
    # Creates a Object definition
    # @param {Function} func - Object implementation in Javascript
    # @param {string} signature - JSONata Object signature definition
    # @returns {{implementation: *, signature: *}} Object definition
    #      
    @staticmethod
    def define_function(func: str, signature: Optional[str], func_impl_method: Optional[str] = None) -> JFunction:
        fn = Jsonata.JNativeFunction(func, signature, functions.Functions, func_impl_method)
        Jsonata.static_frame.bind(func, fn)
        return fn

    @staticmethod
    def function(name: str, signature: Optional[str], clazz: Optional[Any], method_name: str) -> JFunction:
        return Jsonata.JNativeFunction(name, signature, clazz, method_name)

    #
    # parses and evaluates the supplied expression
    # @param {string} expr - expression to evaluate
    # @returns {*} - result of evaluating the expression
    #      
    # async  
    # Object functionEval(String expr, Object focus) {
    # moved to functions.Functions !
    # }

    #
    # Clones an object
    # @param {Object} arg - object to clone (deep copy)
    # @returns {*} - the cloned object
    #      
    # Object functionClone(Object arg) {
    # moved to functions.Functions !
    # }

    #
    # Create frame
    # @param {Object} enclosingEnvironment - Enclosing environment
    # @returns {{bind: bind, lookup: lookup}} Created frame
    #      
    def create_frame(self, enclosing_environment: Optional[Frame] = None) -> Frame:
        return Jsonata.Frame(enclosing_environment)

        # The following logic is in class Frame:
        #  var bindings = {}
        #  return {
        #      bind: Object (name, value) {
        #          bindings[name] = value
        #      },
        #      lookup: Object (name) {
        #          var value
        #          if(bindings.hasOwnProperty(name)) {
        #              value = bindings[name]
        #          } else if (enclosingEnvironment) {
        #              value = enclosingEnvironment.lookup(name)
        #          }
        #          return value
        #      },
        #      timestamp: enclosingEnvironment ? enclosingEnvironment.timestamp : null,
        #      async: enclosingEnvironment ? enclosingEnvironment./* async */ : false,
        #      isParallelCall: enclosingEnvironment ? enclosingEnvironment.isParallelCall : false,
        #      global: enclosingEnvironment ? enclosingEnvironment.global : {
        #          ancestry: [ null ]
        #      }
        #  }

    # Function registration
    @staticmethod
    def register_functions() -> None:
        Jsonata.define_function("sum", "<a<n>:n>")
        Jsonata.define_function("count", "<a:n>")
        Jsonata.define_function("max", "<a<n>:n>")
        Jsonata.define_function("min", "<a<n>:n>")
        Jsonata.define_function("average", "<a<n>:n>")
        Jsonata.define_function("string", "<x-b?:s>")
        Jsonata.define_function("substring", "<s-nn?:s>")
        Jsonata.define_function("substringBefore", "<s-s:s>", "substring_before")
        Jsonata.define_function("substringAfter", "<s-s:s>", "substring_after")
        Jsonata.define_function("lowercase", "<s-:s>")
        Jsonata.define_function("uppercase", "<s-:s>")
        Jsonata.define_function("length", "<s-:n>")
        Jsonata.define_function("trim", "<s-:s>")
        Jsonata.define_function("pad", "<s-ns?:s>")
        Jsonata.define_function("match", "<s-f<s:o>n?:a<o>>", "match_")
        Jsonata.define_function("contains", "<s-(sf):b>")  # TODO <s-(sf<s:o>):b>
        Jsonata.define_function("replace", "<s-(sf)(sf)n?:s>")  # TODO <s-(sf<s:o>)(sf<o:s>)n?:s>
        Jsonata.define_function("split", "<s-(sf)n?:a<s>>")  # TODO <s-(sf<s:o>)n?:a<s>>
        Jsonata.define_function("join", "<a<s>s?:s>")
        Jsonata.define_function("formatNumber", "<n-so?:s>", "format_number")
        Jsonata.define_function("formatBase", "<n-n?:s>", "format_base")
        Jsonata.define_function("formatInteger", "<n-s:s>", "format_integer")
        Jsonata.define_function("parseInteger", "<s-s:n>", "parse_integer")
        Jsonata.define_function("number", "<(nsb)-:n>")
        Jsonata.define_function("floor", "<n-:n>")
        Jsonata.define_function("ceil", "<n-:n>")
        Jsonata.define_function("round", "<n-n?:n>")
        Jsonata.define_function("abs", "<n-:n>")
        Jsonata.define_function("sqrt", "<n-:n>")
        Jsonata.define_function("power", "<n-n:n>")
        Jsonata.define_function("random", "<:n>")
        Jsonata.define_function("boolean", "<x-:b>", "to_boolean")
        Jsonata.define_function("not", "<x-:b>", "not_")
        Jsonata.define_function("map", "<af>")
        Jsonata.define_function("zip", "<a+>")
        Jsonata.define_function("filter", "<af>")
        Jsonata.define_function("single", "<af?>")
        Jsonata.define_function("reduce", "<afj?:j>", "fold_left")  # TODO <f<jj:j>a<j>j?:j>
        Jsonata.define_function("sift", "<o-f?:o>")
        Jsonata.define_function("keys", "<x-:a<s>>")
        Jsonata.define_function("lookup", "<x-s:x>")
        Jsonata.define_function("append", "<xx:a>")
        Jsonata.define_function("exists", "<x:b>")
        Jsonata.define_function("spread", "<x-:a<o>>")
        Jsonata.define_function("merge", "<a<o>:o>")
        Jsonata.define_function("reverse", "<a:a>")
        Jsonata.define_function("each", "<o-f:a>")
        Jsonata.define_function("error", "<s?:x>")
        Jsonata.define_function("assert", "<bs?:x>", "assert_fn")
        Jsonata.define_function("type", "<x:s>")
        Jsonata.define_function("sort", "<af?:a>")
        Jsonata.define_function("shuffle", "<a:a>")
        Jsonata.define_function("distinct", "<x:x>")
        Jsonata.define_function("base64encode", "<s-:s>")
        Jsonata.define_function("base64decode", "<s-:s>")
        Jsonata.define_function("encodeUrlComponent", "<s-:s>", "encode_url_component")
        Jsonata.define_function("encodeUrl", "<s-:s>", "encode_url")
        Jsonata.define_function("decodeUrlComponent", "<s-:s>", "decode_url_component")
        Jsonata.define_function("decodeUrl", "<s-:s>", "decode_url")
        Jsonata.define_function("eval", "<sx?:x>", "function_eval")
        Jsonata.define_function("toMillis", "<s-s?:n>", "datetime_to_millis")
        Jsonata.define_function("fromMillis", "<n-s?s?:s>", "datetime_from_millis")
        Jsonata.define_function("clone", "<(oa)-:o>", "function_clone")

        Jsonata.define_function("now", "<s?s?:s>")
        Jsonata.define_function("millis", "<:n>")

        #  environment.bind("now", defineFunction(function(picture, timezone) {
        #      return datetime.fromMillis(timestamp.getTime(), picture, timezone)
        #  }, "<s?s?:s>"))
        #  environment.bind("millis", defineFunction(function() {
        #      return timestamp.getTime()
        #  }, "<:n>"))

    #
    # lookup a message template from the catalog and substitute the inserts.
    # Populates `err.message` with the substituted message. Leaves `err.message`
    # untouched if code lookup fails.
    # @param {string} err - error code to lookup
    # @returns {undefined} - `err` is modified in place
    #      
    def populate_message(self, err: Exception) -> Exception:
        #  var template = errorCodes[err.code]
        #  if(typeof template !== "undefined") {
        #      // if there are any handlebars, replace them with the field references
        #      // triple braces - replace with value
        #      // double braces - replace with json stringified value
        #      var message = template.replace(/\{\{\{([^}]+)}}}/g, function() {
        #          return err[arguments[1]]
        #      })
        #      message = message.replace(/\{\{([^}]+)}}/g, function() {
        #          return JSON.stringify(err[arguments[1]])
        #      })
        #      err.message = message
        #  }
        # Otherwise retain the original `err.message`
        return err

    @staticmethod
    def _static_initializer() -> None:
        Jsonata.static_frame = Jsonata.Frame(None)
        Jsonata.register_functions()

        # set system recursion limit to 10K (similar to JavaScript)
        sys.setrecursionlimit(10000)

    #
    # JSONata
    # @param {Object} expr - JSONata expression
    # @returns Evaluated expression
    # @throws jexception.JException An exception if an error occured.
    #      
    @staticmethod
    def jsonata(expression: Optional[str]) -> 'Jsonata':
        return Jsonata(expression)

    #
    # Internal constructor
    # @param expr
    #

    parser: parser.Parser
    errors: Optional[Sequence[Exception]]
    environment: Frame
    ast: Optional[parser.Parser.Symbol]
    timestamp: int
    input: Optional[Any]

    def __init__(self, expr: Optional[str]) -> None:
        try:
            self.parser = Jsonata.get_parser()
            self.ast = self.parser.parse(expr)  # , optionsRecover);
            self.errors = self.ast.errors
            self.ast.errors = None  # delete ast.errors;
        except jexception.JException as err:
            # insert error message into structure
            # populateMessage(err); // possible side-effects on `err`
            raise err
        self.environment = self.create_frame(Jsonata.static_frame)

        self.timestamp = timebox.Timebox.current_milli_time()  # will be overridden on each call to evalute()

        self.input = None
        self.validate_input = True

        # Note: now and millis are implemented in Functions
        #  environment.bind("now", defineFunction(function(picture, timezone) {
        #      return datetime.fromMillis(timestamp.getTime(), picture, timezone)
        #  }, "<s?s?:s>"))
        #  environment.bind("millis", defineFunction(function() {
        #      return timestamp.getTime()
        #  }, "<:n>"))

        # FIXED: options.RegexEngine not implemented in Java
        #  if(options && options.RegexEngine) {
        #      jsonata.RegexEngine = options.RegexEngine
        #  } else {
        #      jsonata.RegexEngine = RegExp
        #  }

        # Set instance for this thread
        Jsonata.CURRENT.jsonata = self

    #
    # Flag: validate input objects to comply with JSON types
    #     

    #
    # Checks whether input validation is active
    #     
    def is_validate_input(self) -> bool:
        return self.validate_input

    #
    # Enable or disable input validation
    # @param validateInput
    #     
    def set_validate_input(self, validate_input: bool) -> None:
        self.validate_input = validate_input

    def evaluate(self, input: Optional[Any], bindings: Optional[Frame] = None) -> Optional[Any]:
        # throw if the expression compiled with syntax errors
        if self.errors is not None:
            raise jexception.JException("S0500", 0)

        exec_env = None
        if bindings is not None:
            # var exec_env
            # the variable bindings have been passed in - create a frame to hold these
            exec_env = self.create_frame(self.environment)
            for k, v in bindings.bindings.items():
                exec_env.bind(k, v)
        else:
            exec_env = self.environment
        # put the input document into the environment as the root object
        exec_env.bind("$", input)

        # capture the timestamp and put it in the execution environment
        # the $now() and $millis() functions will return this value - whenever it is called
        self.timestamp = timebox.Timebox.current_milli_time()
        # exec_env.timestamp = timestamp

        # if the input is a JSON array, then wrap it in a singleton sequence so it gets treated as a single input
        if (isinstance(input, list)) and not utils.Utils.is_sequence(input):
            input = utils.Utils.create_sequence(input)
            input.outer_wrapper = True

        if self.validate_input:
            functions.Functions.validate_input(input)

        it = None
        try:
            it = self.eval(self.ast, input, exec_env)
            #  if (typeof callback === "function") {
            #      callback(null, it)
            #  }
            it = utils.Utils.convert_nulls(it)
            return it
        except Exception as err:
            # insert error message into structure
            self.populate_message(err)  # possible side-effects on `err`
            raise err

    def assign(self, name: str, value: Optional[Any]) -> None:
        self.environment.bind(name, value)

    def register_lambda(self, name: str, implementation: Callable) -> None:
        self.environment.bind(name, Jsonata.JLambda(implementation))

    def register_function(self, name: str, function: Any) -> None:
        self.environment.bind(name, function)

    def get_errors(self) -> Optional[list[Exception]]:
        return self.errors

    PARSER = threading.local()

    @staticmethod
    def get_parser() -> parser.Parser:
        with Jsonata.MUTEX:
            if hasattr(Jsonata.PARSER, "parser"):
                return Jsonata.PARSER.parser
            p = parser.Parser()
            Jsonata.PARSER.parser = p
            return p


Jsonata._static_initializer()
