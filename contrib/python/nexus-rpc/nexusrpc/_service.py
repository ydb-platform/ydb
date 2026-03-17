"""
A Nexus service definition is a class with class attributes of type Operation. It must
be be decorated with @nexusrpc.service. The decorator validates the Operation
attributes.
"""

from __future__ import annotations

import dataclasses
import typing
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Generic,
    Mapping,
    Optional,
    Type,
    Union,
    overload,
)

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
    # TODO(preview): they should not be able to set method_name in constructor
    method_name: Optional[str] = dataclasses.field(default=None)
    input_type: Optional[Type[InputT]] = dataclasses.field(default=None)
    output_type: Optional[Type[OutputT]] = dataclasses.field(default=None)

    def __post_init__(self):
        if not self.name:
            raise ValueError("Operation name cannot be empty")

    def _validation_errors(self) -> list[str]:
        errors = []
        if not self.name:
            errors.append(
                f"Operation has no name (method_name is '{self.method_name}')"
            )
        if not self.method_name:
            errors.append(f"Operation '{self.name}' has no method name")
        if not self.input_type:
            errors.append(f"Operation '{self.name}' has no input type")
        if not self.output_type:
            errors.append(f"Operation '{self.name}' has no output type")
        return errors


@overload
def service(cls: Type[ServiceT]) -> Type[ServiceT]: ...


@overload
def service(
    *, name: Optional[str] = None
) -> Callable[[Type[ServiceT]], Type[ServiceT]]: ...


def service(
    cls: Optional[Type[ServiceT]] = None,
    *,
    name: Optional[str] = None,
) -> Union[
    Type[ServiceT],
    Callable[[Type[ServiceT]], Type[ServiceT]],
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

    # TODO(preview): error on attempt foo = Operation[int, str](name="bar")
    #            The input and output types are not accessible on the instance.
    # TODO(preview): Support foo = Operation[int, str]? E.g. via
    # ops = {name: nexusrpc.Operation[int, int] for name in op_names}
    # service_cls = nexusrpc.service(type("ServiceContract", (), ops))
    # This will require forming a union of operations disovered via __annotations__
    # and __dict__

    def decorator(cls: Type[ServiceT]) -> Type[ServiceT]:
        if name is not None and not name:
            raise ValueError("Service name must not be empty.")
        defn = ServiceDefinition.from_class(cls, name or cls.__name__)
        set_service_definition(cls, defn)

        # In order for callers to refer to operations at run-time, a decorated user
        # service class must itself have a class attribute for every operation, even if
        # declared only via a type annotation, and whether inherited from a parent class
        # or not.
        #
        # TODO(preview): it is sufficient to do this setattr only for the subset of
        # operations that were declared on *this* class. Currently however we are
        # setting all inherited operations.
        for op_name, op in defn.operations.items():
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
    operations: Mapping[str, Operation[Any, Any]]

    def __post_init__(self):
        if errors := self._validation_errors():
            raise ValueError(
                f"Service definition {self.name} has validation errors: {', '.join(errors)}"
            )

    @staticmethod
    def from_class(user_class: Type[ServiceT], name: str) -> ServiceDefinition:
        """Create a ServiceDefinition from a user service definition class.

        The set of service definition operations returned is the union of operations
        defined directly on this class with those inherited from ancestral service
        definitions (i.e. ancestral classes that were decorated with @nexusrpc.service).
        If multiple service definitions define an operation with the same name, then the
        usual mro() precedence rules apply.
        """
        operations = ServiceDefinition._collect_operations(user_class)

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
        parent_defns = (
            defn
            for defn in (get_service_definition(cls) for cls in user_class.mro()[1:])
            if defn
        )
        method_names = {op.method_name for op in operations.values() if op.method_name}
        if parent_defn := next(parent_defns, None):
            for op in parent_defn.operations.values():
                if op.method_name in method_names:
                    raise ValueError(
                        f"Operation method name '{op.method_name}' in class '{user_class}' "
                        f"also occurs in a service definition inherited from a parent class: "
                        f"'{parent_defn.name}'. This is not allowed."
                    )
                if op.name in operations:
                    raise ValueError(
                        f"Operation name '{op.name}' in class '{user_class}' "
                        f"also occurs in a service definition inherited from a parent class: "
                        f"'{parent_defn.name}'. This is not allowed."
                    )
                operations[op.name] = op

        return ServiceDefinition(name=name, operations=operations)

    def _validation_errors(self) -> list[str]:
        errors = []
        if not self.name:
            errors.append("Service has no name")
        seen_method_names = set()
        for op in self.operations.values():
            if op.method_name in seen_method_names:
                errors.append(f"Operation method name '{op.method_name}' is not unique")
            seen_method_names.add(op.method_name)
            errors.extend(op._validation_errors())
        return errors

    @staticmethod
    def _collect_operations(
        user_class: Type[ServiceT],
    ) -> dict[str, Operation[Any, Any]]:
        """Collect operations from a user service definition class.

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
                        method_name=key,
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
                if not op.method_name:
                    op.method_name = key
                elif op.method_name != key:
                    raise ValueError(
                        f"Operation {key} method_name ({op.method_name}) must match attribute name {key}"
                    )

            if op.method_name is None:
                op.method_name = key

        operations_by_name = {}
        for op in operations.values():
            if op.name in operations_by_name:
                raise ValueError(
                    f"Operation '{op.name}' in class '{user_class}' is defined multiple times"
                )
            operations_by_name[op.name] = op
        return operations_by_name
