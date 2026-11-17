"""
A Nexus service definition is a class with class attributes of type Operation. It must
be be decorated with @nexusrpc.service. The decorator validates the Operation
attributes.
"""

from __future__ import annotations

import dataclasses
import typing
from collections.abc import Mapping
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Generic,
    Optional,
    Union,
    overload,
)

from typing_extensions import dataclass_transform

from nexusrpc._common import InputT, OutputT, ServiceT
from nexusrpc._util import (
    get_annotations,
    get_service_definition,
    set_service_definition,
)


@dataclass
class Operation(Generic[InputT, OutputT]):
    """Defines a Nexus operation in a Nexus service definition.

    This class is for definition of operation name and input/output types only; to
    implement an operation, see `:py:meth:nexusrpc.handler.operation_handler`.

    Example:

    .. code-block:: python

        @nexusrpc.service
        class MyNexusService:
            my_operation: nexusrpc.Operation[MyInput, MyOutput]
    """

    name: str
    method_name: Optional[str] = dataclasses.field(default=None, init=False)
    input_type: Optional[type[InputT]] = dataclasses.field(default=None)
    output_type: Optional[type[OutputT]] = dataclasses.field(default=None)


@dataclass
class OperationDefinition(Generic[InputT, OutputT]):
    """
    Internal representation of a user's :py:class:`Operation` definition.
    """

    name: str
    method_name: str
    input_type: type[InputT]
    output_type: type[OutputT]

    @classmethod
    def from_operation(
        cls, operation: Operation[InputT, OutputT]
    ) -> OperationDefinition[InputT, OutputT]:
        if not operation.name:
            raise ValueError(
                f"Operation has no name (method_name is '{operation.method_name}')"
            )
        if not operation.method_name:
            raise ValueError(f"Operation '{operation.name}' has no method name")
        if not operation.input_type:
            raise ValueError(f"Operation '{operation.name}' has no input type")
        if not operation.output_type:
            raise ValueError(f"Operation '{operation.name}' has no output type")
        return cls(
            name=operation.name,
            method_name=operation.method_name,
            input_type=operation.input_type,
            output_type=operation.output_type,
        )


@overload
@dataclass_transform(field_specifiers=(Operation,))
def service(cls: type[ServiceT]) -> type[ServiceT]: ...


@overload
@dataclass_transform(field_specifiers=(Operation,))
def service(
    *, name: Optional[str] = None
) -> Callable[[type[ServiceT]], type[ServiceT]]: ...


def service(
    cls: Optional[type[ServiceT]] = None,
    *,
    name: Optional[str] = None,
) -> Union[
    type[ServiceT],
    Callable[[type[ServiceT]], type[ServiceT]],
]:
    """
    Decorator marking a class as a Nexus service definition.

    The decorator validates the operation definitions in the service definition: that they
    have the correct type, and that there are no duplicate operation names. The decorator
    also creates instances of the Operation class for each operation definition.

    Example:
        .. code-block:: python

            @nexusrpc.service
            class MyNexusService:
                my_op: nexusrpc.Operation[MyInput, MyOutput]
                another_op: nexusrpc.Operation[str, dict]

            @nexusrpc.service(name="custom-service-name")
            class AnotherService:
                process: nexusrpc.Operation[ProcessInput, ProcessOutput]
    """

    def decorator(cls: type[ServiceT]) -> type[ServiceT]:
        if name is not None and not name:
            raise ValueError("Service name must not be empty.")
        defn = ServiceDefinition.from_class(cls, name or cls.__name__)
        set_service_definition(cls, defn)

        # In order for callers to refer to operation definitions at run-time, a decorated user
        # service class must itself have a class attribute for every operation, even if
        # declared only via a type annotation, and whether inherited from a parent class
        # or not.
        #
        # TODO(preview): it is sufficient to do this setattr only for the subset of
        # operations that were declared on *this* class. Currently however we are
        # setting all inherited operations.
        for op_name, op_defn in defn.operation_definitions.items():
            if not hasattr(cls, op_name):
                op = Operation(
                    name=op_defn.name,
                    input_type=op_defn.input_type,
                    output_type=op_defn.output_type,
                )
                op.method_name = op_defn.method_name
                setattr(cls, op_name, op)

        return cls

    if cls is None:
        return decorator
    else:
        return decorator(cls)


@dataclass(frozen=True)
class ServiceDefinition:
    """
    Internal representation of a user's service definition class.

    A named collection of named operation definitions.

    A service definition class is a class decorated with :py:func:`nexusrpc.service`
    containing class attribute type annotations of type :py:class:`nexusrpc.Operation`.
    """

    name: str
    operation_definitions: Mapping[str, OperationDefinition[Any, Any]]

    def __post_init__(self):
        if errors := self._validation_errors():
            raise ValueError(
                f"Service definition {self.name} has validation errors: {', '.join(errors)}"
            )

    @staticmethod
    def from_class(user_class: type[ServiceT], name: str) -> ServiceDefinition:
        """Create a ServiceDefinition from a user service definition class.

        The set of service definition operations returned is the union of operations
        defined directly on this class with those inherited from ancestral service
        definitions (i.e. ancestral classes that were decorated with @nexusrpc.service).
        If multiple service definitions define an operation with the same name, then the
        usual mro() precedence rules apply.
        """
        operation_definitions = ServiceDefinition._collect_operations(user_class)

        # Obtain the set of operations to be inherited from ancestral service
        # definitions. Operations are only inherited from classes that are also
        # decorated with @nexusrpc.service. We do not permit any "overriding" by child
        # classes; both the following must be true:
        # 1. No inherited operation has the same name as that of an operation defined
        #    here. If this were violated, it would indicate two service definitions
        #    exposing potentially different operation definitions behind the same
        #    operation name.
        # 2. No inherited operation has the same method name as that of an operation
        #    defined here. If this were violated, there would be ambiguity in which
        #    operation handler is dispatched to.
        parent_service_defn = next(
            (
                defn
                for defn in (
                    get_service_definition(cls) for cls in user_class.mro()[1:]
                )
                if defn
            ),
            None,
        )
        if parent_service_defn:
            method_names = {op.method_name for op in operation_definitions.values()}
            for op_defn in parent_service_defn.operation_definitions.values():
                if op_defn.method_name in method_names:
                    raise ValueError(
                        f"Operation method name '{op_defn.method_name}' in class '{user_class}' "
                        f"also occurs in a service definition inherited from a parent class: "
                        f"'{parent_service_defn.name}'. This is not allowed."
                    )
                if op_defn.name in operation_definitions:
                    raise ValueError(
                        f"Operation name '{op_defn.name}' in class '{user_class}' "
                        f"also occurs in a service definition inherited from a parent class: "
                        f"'{parent_service_defn.name}'. This is not allowed."
                    )
                operation_definitions[op_defn.name] = op_defn

        return ServiceDefinition(name=name, operation_definitions=operation_definitions)

    def _validation_errors(self) -> list[str]:
        errors = []
        if not self.name:
            errors.append("Service has no name")
        seen_method_names = set()
        for op_defn in self.operation_definitions.values():
            if op_defn.method_name in seen_method_names:
                errors.append(
                    f"Operation method name '{op_defn.method_name}' is not unique"
                )
            seen_method_names.add(op_defn.method_name)
        return errors

    @staticmethod
    def _collect_operations(
        user_class: type[ServiceT],
    ) -> dict[str, OperationDefinition[Any, Any]]:
        """Collect operation definitions from a user service definition class.

        Does not visit parent classes.
        """

        # Form the union of all class attribute names that are either an Operation
        # instance or have an Operation type annotation, or both.
        operations: dict[str, Operation[Any, Any]] = {}
        for k, v in user_class.__dict__.items():
            if isinstance(v, Operation):
                operations[k] = v
            elif typing.get_origin(v) is Operation:
                raise TypeError(
                    "Operation definitions in the service definition should look like  "
                    "my_op: nexusrpc.Operation[InputType, OutputType]. Did you accidentally "
                    "use '=' instead of ':'?"
                )

        annotations = {
            k: v
            for k, v in get_annotations(user_class, eval_str=True).items()
            if v == Operation or typing.get_origin(v) == Operation
        }
        for key in operations.keys() | annotations.keys():
            # If the name has a type annotation, then add the input and output types to
            # the operation instance, or create the instance if there was only an
            # annotation.
            if op_type := annotations.get(key):
                args = typing.get_args(op_type)
                if len(args) != 2:
                    raise TypeError(
                        f"Operation types in the service definition should look like  "
                        f"nexusrpc.Operation[InputType, OutputType], but '{key}' in "
                        f"'{user_class}' has {len(args)} type parameters."
                    )
                input_type, output_type = args
                if key not in operations:
                    # It looked like
                    # my_op: Operation[I, O]
                    op = operations[key] = Operation(
                        name=key,
                        input_type=input_type,
                        output_type=output_type,
                    )
                else:
                    op = operations[key]
                    # It looked like
                    # my_op: Operation[I, O] = Operation(...)
                    if not op.input_type:
                        op.input_type = input_type
                    elif op.input_type != input_type:
                        raise ValueError(
                            f"Operation {key} input_type ({op.input_type}) must match type parameter {input_type}"
                        )
                    if not op.output_type:
                        op.output_type = output_type
                    elif op.output_type != output_type:
                        raise ValueError(
                            f"Operation {key} output_type ({op.output_type}) must match type parameter {output_type}"
                        )
            else:
                # It looked like
                # my_op = Operation(...)
                op = operations[key]

            # Validate that if method_name was set (via direct assignment after
            # construction), it matches the attribute name
            if op.method_name is not None and op.method_name != key:
                raise ValueError(
                    f"Operation {key} method_name ({op.method_name}) must match attribute name {key}"
                )
            op.method_name = key

        op_defns = {}
        for op in operations.values():
            if op.name in op_defns:
                raise RuntimeError(
                    f"Operation '{op.name}' in service '{user_class}' is defined multiple times"
                )
            op_defns[op.name] = OperationDefinition.from_operation(op)
        return op_defns
