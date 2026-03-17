# SPDX-Copyright: Copyright (c) Capital One Services, LLC
# SPDX-License-Identifier: Apache-2.0
# Copyright 2020 Capital One Services, LLC
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
# See the License for the specific language governing permissions and limitations under the License.

"""
Pure Python implementation of CEL.

..  todo:: Consolidate __init__ and parser into one module?

Visible interface to CEL. This exposes the :py:class:`Environment`,
the :py:class:`Evaluator` run-time, and the :py:mod:`celtypes` module
with Python types wrapped to be CEL compatible.

Example
=======

Here's an example with some details::

    >>> import celpy

    # A list of type names and class bindings used to create an environment.
    >>> types = []
    >>> env = celpy.Environment(types)

    # Parse the code to create the CEL AST.
    >>> ast = env.compile("355. / 113.")

    # Use the AST and any overriding functions to create an executable program.
    >>> functions = {}
    >>> prgm = env.program(ast, functions)

    # Variable bindings.
    >>> activation = {}

    # Final evaluation.
    >>> try:
    ...    result = prgm.evaluate(activation)
    ...    error = None
    ... except CELEvalError as ex:
    ...    result = None
    ...    error = ex.args[0]

    >>> result  # doctest: +ELLIPSIS
    DoubleType(3.14159...)

Another Example
===============

See https://github.com/google/cel-go/blob/master/examples/simple_test.go

The model Go we're sticking close to::

    d := cel.Declarations(decls.NewVar("name", decls.String))
    env, err := cel.NewEnv(d)
    if err != nil {
        log.Fatalf("environment creation error: %v\\n", err)
    }
    ast, iss := env.Compile(`"Hello world! I'm " + name + "."`)
    // Check iss for compilation errors.
    if iss.Err() != nil {
        log.Fatalln(iss.Err())
    }
    prg, err := env.Program(ast)
    if err != nil {
        log.Fatalln(err)
    }
    out, _, err := prg.Eval(map[string]interface{}{
        "name": "CEL",
    })
    if err != nil {
        log.Fatalln(err)
    }
    fmt.Println(out)
    // Output:Hello world! I'm CEL.

Here's the Pythonic approach, using concept patterned after the Go implementation::

    >>> from celpy import *
    >>> decls = {"name": celtypes.StringType}
    >>> env = Environment(annotations=decls)
    >>> ast = env.compile('"Hello world! I\\'m " + name + "."')
    >>> out = env.program(ast).evaluate({"name": "CEL"})
    >>> print(out)
    Hello world! I'm CEL.

"""

import json  # noqa: F401
import logging
import sys
from typing import Any, Dict, Optional, Type, cast

import lark

import celpy.celtypes
from celpy.adapter import (  # noqa: F401
    CELJSONDecoder,
    CELJSONEncoder,
    json_to_cel,
)
from celpy.celparser import CELParseError, CELParser  # noqa: F401
from celpy.evaluation import (  # noqa: F401
    Activation,
    Annotation,
    CELEvalError,
    CELFunction,
    Context,
    Evaluator,
    Result,
    base_functions,
)

# A parsed AST.
Expression = lark.Tree


class Runner:
    """Abstract runner.

    Given an AST, this can evaluate the AST in the context of a specific activation
    with any override function definitions.

    ..  todo:: add type adapter and type provider registries.
    """

    def __init__(
        self,
        environment: "Environment",
        ast: lark.Tree,
        functions: Optional[Dict[str, CELFunction]] = None,
    ) -> None:
        self.logger = logging.getLogger(f"celpy.{self.__class__.__name__}")
        self.environment = environment
        self.ast = ast
        self.functions = functions

    def new_activation(self, context: Context) -> Activation:
        """
        Builds the working activation from the environmental defaults.
        """
        return self.environment.activation().nested_activation(vars=context)

    def evaluate(self, activation: Context) -> celpy.celtypes.Value:  # pragma: no cover
        raise NotImplementedError


class InterpretedRunner(Runner):
    """
    Pure AST expression evaluator. Uses :py:class:`evaluation.Evaluator` class.

    Given an AST, this evauates the AST in the context of a specific activation.

    The returned value will be a celtypes type.

    Generally, this should raise an :exc:`CELEvalError` for most kinds of ordinary problems.
    It may raise an :exc:`CELUnsupportedError` for future features.

    ..  todo:: Refractor the Evaluator constructor from evaluation.
    """

    def evaluate(self, context: Context) -> celpy.celtypes.Value:
        e = Evaluator(
            ast=self.ast,
            activation=self.new_activation(context),
            functions=self.functions,
        )
        value = e.evaluate()
        return value


class CompiledRunner(Runner):
    """
    Python compiled expression evaluator. Uses Python byte code and :py:func:`eval`.

    Given an AST, this evaluates the AST in the context of a specific activation.

    Transform the AST into Python, uses :py:func:`compile` to create a code object.
    Uses :py:func:`eval` to evaluate.
    """

    def __init__(
        self,
        environment: "Environment",
        ast: lark.Tree,
        functions: Optional[Dict[str, CELFunction]] = None,
    ) -> None:
        super().__init__(environment, ast, functions)
        # Transform AST to Python.
        # compile()
        # cache executable code object.

    def evaluate(self, activation: Context) -> celpy.celtypes.Value:
        # eval() code object with activation as locals, and built-ins as gobals.
        return super().evaluate(activation)


# TODO: Refactor classes into a separate "cel_protobuf" module.
# TODO: Becomes cel_protobuf.Int32Value
class Int32Value(celpy.celtypes.IntType):
    def __new__(
        cls: Type["Int32Value"],
        value: Any = 0,
    ) -> "Int32Value":
        """TODO: Check range. This seems to matter for protobuf."""
        if isinstance(value, celpy.celtypes.IntType):
            return cast(Int32Value, super().__new__(cls, value))
        # TODO: elif other type conversions...
        else:
            convert = celpy.celtypes.int64(int)
        return cast(Int32Value, super().__new__(cls, convert(value)))


# The "well-known" types in a google.protobuf package.
# We map these to CEl types instead of defining additional Protobuf Types.
# This approach bypasses some of the range constraints that are part of these types.
# It may also cause values to compare as equal when they were originally distinct types.
googleapis = {
    "google.protobuf.Int32Value": celpy.celtypes.IntType,
    "google.protobuf.UInt32Value": celpy.celtypes.UintType,
    "google.protobuf.Int64Value": celpy.celtypes.IntType,
    "google.protobuf.UInt64Value": celpy.celtypes.UintType,
    "google.protobuf.FloatValue": celpy.celtypes.DoubleType,
    "google.protobuf.DoubleValue": celpy.celtypes.DoubleType,
    "google.protobuf.BoolValue": celpy.celtypes.BoolType,
    "google.protobuf.BytesValue": celpy.celtypes.BytesType,
    "google.protobuf.StringValue": celpy.celtypes.StringType,
    "google.protobuf.ListValue": celpy.celtypes.ListType,
    "google.protobuf.Struct": celpy.celtypes.MessageType,
}


class Environment:
    """Compiles CEL text to create an Expression object.

    From the Go implementation, there are things to work with the type annotations:

    -   type adapters registry make other native types available for CEL.

    -   type providers registry make ProtoBuf types available for CEL.

    ..  todo:: Add adapter and provider registries to the Environment.
    """

    def __init__(
        self,
        package: Optional[str] = None,
        annotations: Optional[Dict[str, Annotation]] = None,
        runner_class: Optional[Type[Runner]] = None,
    ) -> None:
        """
        Create a new environment.

        This also increases the default recursion limit to handle the defined minimums for CEL.

        :param package: An optional package name used to resolve names in an Activation
        :param annotations: Names with type annotations.
            There are two flavors of names provided here.

            - Variable names based on :py:mod:``celtypes``

            - Function names, using ``typing.Callable``.
        :param runner_class: the class of Runner to use, either InterpretedRunner or CompiledRunner
        """
        sys.setrecursionlimit(2500)
        self.logger = logging.getLogger(f"celpy.{self.__class__.__name__}")
        self.package: Optional[str] = package
        self.annotations: Dict[str, Annotation] = annotations or {}
        self.logger.debug("Type Annotations %r", self.annotations)
        self.runner_class: Type[Runner] = runner_class or InterpretedRunner
        self.cel_parser = CELParser()
        self.runnable: Runner

        # Fold in standard annotations. These (generally) define well-known protobuf types.
        self.annotations.update(googleapis)
        # We'd like to add 'type.googleapis.com/google' directly, but it seems to be an alias
        # for 'google', the path after the '/' in the uri.

    def compile(self, text: str) -> Expression:
        """Compile the CEL source. This can raise syntax error exceptions."""
        ast = self.cel_parser.parse(text)
        return ast

    def program(
        self, expr: lark.Tree, functions: Optional[Dict[str, CELFunction]] = None
    ) -> Runner:
        """Transforms the AST into an executable runner."""
        self.logger.debug("Package %r", self.package)
        runner_class = self.runner_class
        self.runnable = runner_class(self, expr, functions)
        return self.runnable

    def activation(self) -> Activation:
        """Returns a base activation"""
        activation = Activation(package=self.package, annotations=self.annotations)
        return activation
