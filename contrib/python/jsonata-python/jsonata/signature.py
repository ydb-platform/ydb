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
#   Project name: JSONata4Java
#   (c) Copyright 2018, 2019 IBM Corporation
#   Licensed under the Apache License, Version 2.0 (the "License")
#   1 New Orchard Road,
#   Armonk, New York, 10504-1722
#   United States
#   +1 914 499 1900
#   support: Nathaniel Mills wnm3@us.ibm.com
#

import re
from typing import MutableSequence, Optional, Sequence, NoReturn, Any

from jsonata import jexception, utils


#
# Manages signature related functions
# 
class Signature:
    SERIAL_VERSION_UID = -450755246855587271

    class Param:

        type: Optional[str]
        regex: Optional[str]
        context: bool
        array: bool
        subtype: Optional[str]
        context_regex: Optional[str]

        def __init__(self):
            self.type = None
            self.regex = None
            self.context = False
            self.array = False
            self.subtype = None
            self.context_regex = None

        def __repr__(self):
            return "Param " + self.type + " regex=" + self.regex + " ctx=" + str(self.context) + " array=" + str(
                self.array)

    signature: str
    function_name: str

    _param: 'Signature.Param'
    _params: MutableSequence['Signature.Param']
    _prev_param: 'Signature.Param'
    _regex: Optional[re.Pattern]
    _signature: str

    def __init__(self, signature, function):
        self._param = Signature.Param()
        self._params = []
        self._prev_param = self._param
        self._regex = None
        self._signature = ""

        self.function_name = function
        self.parse_signature(signature)

    def set_function_name(self, function_name: str) -> None:
        self.function_name = function_name

    @staticmethod
    def main(args: Sequence[str]) -> None:
        s = Signature("<s-:s>", "test")  # <s-(sf)(sf)n?:s>");
        print(s._params)

    def find_closing_bracket(self, string: str, start: int, open_symbol: str, close_symbol: str) -> int:
        # returns the position of the closing symbol (e.g. bracket) in a string
        # that balances the opening symbol at position start
        depth = 1
        position = start
        while position < len(string):
            position += 1
            symbol = string[position]
            if symbol == close_symbol:
                depth -= 1
                if depth == 0:
                    # we're done
                    break  # out of while loop
            elif symbol == open_symbol:
                depth += 1
        return position

    def get_symbol(self, value: Optional[Any]) -> str:
        from jsonata import functions
        if value is None:
            symbol = "m"
        else:
            # first check to see if this is a function
            if utils.Utils.is_function(value) or functions.Functions.is_lambda(value) or isinstance(value, re.Pattern):
                symbol = "f"
            elif isinstance(value, str):
                symbol = "s"
            elif isinstance(value, bool):
                symbol = "b"
            elif isinstance(value, (int, float)):
                symbol = "n"
            elif isinstance(value, list):
                symbol = "a"
            elif isinstance(value, dict):
                symbol = "o"
            elif value is None:  # Uli: is this used???
                symbol = "l"
            else:
                # any value can be undefined, but should be allowed to match
                symbol = "m"  # m for missing
        return symbol

    def next(self) -> None:
        self._params.append(self._param)
        self._prev_param = self._param
        self._param = Signature.Param()

    #
    # Parses a function signature definition and returns a validation function
    # 
    # @param {string}
    #                 signature - the signature between the <angle brackets>
    # @returns validation pattern
    #     
    def parse_signature(self, signature: str) -> Optional[re.Pattern]:
        # create a Regex that represents this signature and return a function that when
        # invoked,
        # returns the validated (possibly fixed-up) arguments, or throws a validation
        # error
        # step through the signature, one symbol at a time
        position = 1
        while position < len(signature):
            symbol = signature[position]
            if symbol == ':':
                # TODO figure out what to do with the return type
                # ignore it for now
                break

            if symbol == 's' or symbol == 'n' or symbol == 'b' or symbol == 'l' or symbol == 'o':
                self._param.regex = ("[" + symbol + "m]")
                self._param.type = str(symbol)
                self.next()
            elif symbol == 'a':
                # normally treat any value as singleton array
                self._param.regex = "[asnblfom]"
                self._param.type = str(symbol)
                self._param.array = True
                self.next()
            elif symbol == 'f':
                self._param.regex = "f"
                self._param.type = str(symbol)
                self.next()
            elif symbol == 'j':
                self._param.regex = "[asnblom]"
                self._param.type = str(symbol)
                self.next()
            elif symbol == 'x':
                self._param.regex = "[asnblfom]"
                self._param.type = str(symbol)
                self.next()
            elif symbol == '-':
                self._prev_param.context = True
                self._prev_param.regex += "?"
            elif symbol == '?' or symbol == '+':
                self._prev_param.regex += symbol
            elif symbol == '(':
                # search forward for matching ')'
                end_paren = self.find_closing_bracket(signature, position, '(', ')')
                choice = signature[position + 1:end_paren]
                if choice.find("<") == -1:
                    # no _parameterized types, simple regex
                    self._param.regex = ("[" + choice + "m]")
                else:
                    # TODO harder
                    raise RuntimeError("Choice groups containing parameterized types are not supported")
                self._param.type = ("(" + choice + ")")
                position = end_paren
                self.next()
            elif symbol == '<':
                test = self._prev_param.type
                if test is not None:
                    type = test  # .asText();
                    if type == "a" or type == "f":
                        # search forward for matching '>'
                        end_pos = self.find_closing_bracket(signature, position, '<', '>')
                        self._prev_param.subtype = signature[position + 1:end_pos]
                        position = end_pos
                    else:
                        raise RuntimeError("Type parameters can only be applied to functions and arrays")
                else:
                    raise RuntimeError("Type parameters can only be applied to functions and arrays")
            position += 1  # end while processing symbols in signature

        regex_str = "^"
        for param in self._params:
            regex_str += "(" + param.regex + ")"
        regex_str += "$"

        self._regex = re.compile(regex_str)
        self._signature = regex_str
        return self._regex

    def throw_validation_error(self, bad_args: Optional[Sequence], bad_sig: Optional[str],
                               function_name: Optional[str]) -> NoReturn:
        # to figure out where this went wrong we need apply each component of the
        # regex to each argument until we get to the one that fails to match
        partial_pattern = "^"

        good_to = 0
        for param in self._params:
            partial_pattern += param.regex
            tester = re.compile(partial_pattern)
            match_ = tester.fullmatch(bad_sig)
            if match_ is None:
                # failed here
                raise jexception.JException("T0410", -1, (good_to + 1), function_name)
            good_to = match_.end()
        # if it got this far, it's probably because of extraneous arguments (we
        # haven't added the trailing '$' in the regex yet.
        raise jexception.JException("T0410", -1, (good_to + 1), function_name)

    def validate(self, args: Any, context: Optional[Any]) -> Optional[Any]:
        supplied_sig = ""
        for arg in args:
            supplied_sig += self.get_symbol(arg)

        is_valid = self._regex.fullmatch(supplied_sig)
        if is_valid is not None:
            validated_args = []
            arg_index = 0
            index = 0
            for param in self._params:
                arg = args[arg_index] if arg_index < len(args) else None
                match_ = is_valid.group(index + 1)
                if "" == match_:
                    if param.context and param.regex is not None:
                        # substitute context value for missing arg
                        # first check that the context value is the right type
                        context_type = self.get_symbol(context)
                        # test context_type against the regex for this arg (without the trailing ?)
                        if re.fullmatch(param.regex, context_type):
                            # if (param.contextRegex.test(context_type)) {
                            validated_args.append(context)
                        else:
                            # context value not compatible with this argument
                            raise jexception.JException("T0411", -1, context, arg_index + 1)
                    else:
                        validated_args.append(arg)
                        arg_index += 1
                else:
                    # may have matched multiple args (if the regex ends with a '+'
                    # split into single tokens
                    singles = list(match_)  # split on empty separator
                    for single in singles:
                        # match.split('').forEach(function (single) {
                        if param.type == "a":
                            if single == "m":
                                # missing (undefined)
                                arg = None
                            else:
                                arg = args[arg_index] if arg_index < len(args) else None
                                array_ok = True
                                # is there type information on the contents of the array?
                                if param.subtype is not None:
                                    if single != "a" and match_ != param.subtype:
                                        array_ok = False
                                    elif single == "a":
                                        arg_arr = arg
                                        if arg_arr:
                                            item_type = self.get_symbol(arg_arr[0])
                                            if item_type != str(param.subtype[0]):
                                                array_ok = False
                                            else:
                                                # make sure every item in the array is this type
                                                for o in arg_arr:
                                                    if self.get_symbol(o) != item_type:
                                                        array_ok = False
                                                        break
                                if not array_ok:
                                    raise jexception.JException("T0412", -1, arg, param.subtype)
                                # the function expects an array. If it's not one, make it so
                                if single != "a":
                                    arg = [arg]
                            validated_args.append(arg)
                            arg_index += 1
                        else:
                            arg = args[arg_index] if arg_index < len(args) else None
                            validated_args.append(arg)
                            arg_index += 1
            return validated_args
        self.throw_validation_error(args, supplied_sig, self.function_name)

    def get_number_of_args(self) -> int:
        return len(self._params)

    #
    # Returns the minimum # of arguments.
    # I.e. the # of all non-optional arguments.
    #     
    def get_min_number_of_args(self) -> int:
        res = 0
        for p in self._params:
            if "?" not in p.regex:
                res += 1
        return res
    #
    #    ArrayNode validate(String functionName, ExprListContext args, ExpressionsVisitor expressionVisitor) {
    #        ArrayNode result = JsonNodeFactory.instance.arrayNode()
    #        String suppliedSig = ""
    #        for (Iterator<ExprContext> it = args.expr().iterator(); it.hasNext();) {
    #            ExprContext arg = it.next()
    #            suppliedSig += getSymbol(arg)
    #        }
    #        Matcher isValid = _regex.matcher(suppliedSig)
    #        if (isValid != null) {
    #            ArrayNode validatedArgs = JsonNodeFactory.instance.arrayNode()
    #            int argIndex = 0
    #            int index = 0
    #            for (Iterator<JsonNode> it = _params.iterator(); it.hasNext();) {
    #                ObjectNode param = (ObjectNode) it.next()
    #                JsonNode arg = expressionVisitor.visit(args.expr(argIndex))
    #                String match = isValid.group(index + 1)
    #                if ("".equals(match)) {
    #                    boolean useContext = (param.get("context") != null && param.get("context").asBoolean())
    #                    if (useContext) {
    #                        // substitute context value for missing arg
    #                        // first check that the context value is the right type
    #                        JsonNode context = expressionVisitor.getVariable("$")
    #                        String contextType = getSymbol(context)
    #                        // test contextType against the regex for this arg (without the trailing ?)
    #                        if (Pattern.matches(param.get("regex").asText(), contextType)) {
    #                            validatedArgs.add(context)
    #                        } else {
    #                            // context value not compatible with this argument
    #                            throw new EvaluateRuntimeException("Context value is not a compatible type with argument \""
    #                                + argIndex + 1 + "\" of function \"" + functionName + "\"")
    #                        }
    #                    } else {
    #                        validatedArgs.add(arg)
    #                        argIndex++
    #                    }
    #                } else {
    #                    // may have matched multiple args (if the regex ends with a '+'
    #                    // split into single tokens
    #                    String[] singles = match.split("")
    #                    for (String single : singles) {
    #                        if ("a".equals(param.get("type").asText())) {
    #                            if ("m".equals(single)) {
    #                                // missing (undefined)
    #                                arg = null
    #                            } else {
    #                                arg = expressionVisitor.visit(args.expr(argIndex))
    #                                boolean arrayOK = true
    #                                // is there type information on the contents of the array?
    #                                String subtype = "undefined"
    #                                JsonNode testSubType = param.get("subtype")
    #                                if (testSubType != null) {
    #                                    subtype = testSubType.asText()
    #                                    if ("a".equals(single) == false && match.equals(subtype) == false) {
    #                                        arrayOK = false
    #                                    } else if ("a".equals(single)) {
    #                                        ArrayNode argArray = (ArrayNode) arg
    #                                        if (argArray.size() > 0) {
    #                                            String itemType = getSymbol(argArray.get(0))
    #                                            if (itemType.equals(subtype) == false) { // TODO recurse further
    #                                                arrayOK = false
    #                                            } else {
    #                                                // make sure every item in the array is this type
    #                                                ArrayNode differentItems = JsonNodeFactory.instance.arrayNode()
    #                                                for (Object val : argArray) {
    #                                                    if (itemType.equals(getSymbol(val)) == false) {
    #                                                        differentItems.add(expressionVisitor.visit((ExprListContext) val))
    #                                                    }
    #                                                }
    #                                                
    #                                                arrayOK = (differentItems.size() == 0)
    #                                            }
    #                                        }
    #                                    }
    #                                }
    #                                if (!arrayOK) {
    #                                    JsonNode type = s_arraySignatureMapping.get(subtype)
    #                                    if (type == null) {
    #                                        type = JsonNodeFactory.instance.nullNode()
    #                                    }
    #                                    throw new EvaluateRuntimeException("Argument \"" + (argIndex + 1) + "\" of function \""
    #                                        + functionName + "\" must be an array of \"" + type.asText() + "\"")
    #                                }
    #                                // the function expects an array. If it's not one, make it so
    #                                if ("a".equals(single) == false) {
    #                                    ArrayNode wrappedArg = JsonNodeFactory.instance.arrayNode()
    #                                    wrappedArg.add(arg)
    #                                    arg = wrappedArg
    #                                }
    #                            }
    #                            validatedArgs.add(arg)
    #                            argIndex++
    #                        } else {
    #                            validatedArgs.add(arg)
    #                            argIndex++
    #                        }
    #                    }
    #                }
    #                index++
    #            }
    #            return validatedArgs
    #        }
    #        throwValidationError(args, suppliedSig, functionName)
    #        // below just for the compiler as a runtime exception is thrown above
    #        return result
    #    }
    #
