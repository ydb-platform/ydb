# -*- test-case-name: automat._test.test_type_based -*-
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    get_origin,
    Any,
    Callable,
    Generic,
    Iterable,
    Literal,
    Protocol,
    TypeVar,
    overload,
)

if TYPE_CHECKING:
    from graphviz import Digraph
try:
    from zope.interface.interface import InterfaceClass  # type:ignore[import-untyped]
except ImportError:
    hasInterface = False
else:
    hasInterface = True

if sys.version_info < (3, 10):
    from typing_extensions import Concatenate, ParamSpec, TypeAlias
else:
    from typing import Concatenate, ParamSpec, TypeAlias

from ._core import Automaton, Transitioner
from ._runtimeproto import (
    ProtocolAtRuntime,
    _liveSignature,
    actuallyDefinedProtocolMethods,
    runtime_name,
)


class AlreadyBuiltError(Exception):
    """
    The L{TypeMachine} is already built, and thus can no longer be
    modified.
    """


InputProtocol = TypeVar("InputProtocol")
Core = TypeVar("Core")
Data = TypeVar("Data")
P = ParamSpec("P")
P1 = ParamSpec("P1")
R = TypeVar("R")
OtherData = TypeVar("OtherData")
Decorator = Callable[[Callable[P, R]], Callable[P, R]]
FactoryParams = ParamSpec("FactoryParams")
OtherFactoryParams = ParamSpec("OtherFactoryParams")


def pep614(t: R) -> R:
    """
    This is a workaround for Python 3.8, which has U{some restrictions on its
    grammar for decorators <https://peps.python.org/pep-0614/>}, and makes
    C{@state.to(other).upon(Protocol.input)} invalid syntax; for code that
    needs to run on these older Python versions, you can do
    C{@pep614(state.to(other).upon(Protocol.input))} instead.
    """
    return t


@dataclass()
class TransitionRegistrar(Generic[P, P1, R]):
    """
    This is a record of a transition that need finalizing; it is the result of
    calling L{TypeMachineBuilder.state} and then ``.upon(input).to(state)`` on
    the result of that.

    It can be used as a decorator, like::

        registrar = state.upon(Proto.input).to(state2)
        @registrar
        def inputImplementation(proto: Proto, core: Core) -> Result: ...

    Or, it can be used used to implement a constant return value with
    L{TransitionRegistrar.returns}, like::

        registrar = state.upon(Proto.input).to(state2)
        registrar.returns(value)

    Type parameter P: the precise signature of the decorated implementation
    callable.

    Type parameter P1: the precise signature of the input method from the
    outward-facing state-machine protocol.

    Type parameter R: the return type of both the protocol method and the input
    method.
    """

    _signature: Callable[P1, R]
    _old: AnyState
    _new: AnyState
    _nodata: bool = False
    _callback: Callable[P, R] | None = None

    def __post_init__(self) -> None:
        self._old.builder._registrars.append(self)

    def __call__(self, impl: Callable[P, R]) -> Callable[P, R]:
        """
        Finalize it with C{__call__} to indicate that there is an
        implementation to the transition, which can be treated as an output.
        """
        if self._callback is not None:
            raise AlreadyBuiltError(
                f"already registered transition from {self._old.name!r} to {self._new.name!r}"
            )
        self._callback = impl
        builder = self._old.builder
        assert builder is self._new.builder, "states must be from the same builder"
        builder._automaton.addTransition(
            self._old,
            self._signature.__name__,
            self._new,
            tuple(self._new._produceOutputs(impl, self._old, self._nodata)),
        )
        return impl

    def returns(self, result: R) -> None:
        """
        Finalize it with C{.returns(constant)} to indicate that there is no
        method body, and the given result can just be yielded each time after
        the state transition.  The only output generated in this case would be
        the data-construction factory for the target state.
        """

        def constant(*args: object, **kwargs: object) -> R:
            return result

        constant.__name__ = f"returns({result})"
        self(constant)

    def _checkComplete(self) -> None:
        """
        Raise an exception if the user forgot to decorate a method
        implementation or supply a return value for this transition.
        """
        # TODO: point at the line where `.to`/`.loop`/`.upon` are called so the
        # user can more immediately see the incomplete transition
        if not self._callback:
            raise ValueError(
                f"incomplete transition from {self._old.name} to "
                f"{self._new.name} upon {self._signature.__qualname__}: "
                "remember to use the transition as a decorator or call "
                "`.returns` on it."
            )


@dataclass
class UponFromNo(Generic[InputProtocol, Core, P, R]):
    """
    Type parameter P: the signature of the input method.
    """

    old: TypedState[InputProtocol, Core] | TypedDataState[InputProtocol, Core, Any, ...]
    input: Callable[Concatenate[InputProtocol, P], R]

    @overload
    def to(
        self, state: TypedState[InputProtocol, Core]
    ) -> TransitionRegistrar[Concatenate[InputProtocol, Core, P], P, R]: ...
    @overload
    def to(
        self,
        state: TypedDataState[InputProtocol, Core, OtherData, P],
    ) -> TransitionRegistrar[
        Concatenate[InputProtocol, Core, P],
        Concatenate[InputProtocol, P],
        R,
    ]: ...
    def to(
        self,
        state: (
            TypedState[InputProtocol, Core]
            | TypedDataState[InputProtocol, Core, Any, P]
        ),
    ) -> (
        TransitionRegistrar[Concatenate[InputProtocol, Core, P], P, R]
        | TransitionRegistrar[
            Concatenate[InputProtocol, Core, P],
            Concatenate[InputProtocol, P],
            R,
        ]
    ):
        """
        Declare a state transition to a new state.
        """
        return TransitionRegistrar(self.input, self.old, state, True)

    def loop(self) -> TransitionRegistrar[
        Concatenate[InputProtocol, Core, P],
        Concatenate[InputProtocol, P],
        R,
    ]:
        """
        Register a transition back to the same state.
        """
        return TransitionRegistrar(self.input, self.old, self.old, True)


@dataclass
class UponFromData(Generic[InputProtocol, Core, P, R, Data]):
    """
    Type parameter P: the signature of the input method.
    """

    old: TypedDataState[InputProtocol, Core, Data, ...]
    input: Callable[Concatenate[InputProtocol, P], R]

    @overload
    def to(
        self, state: TypedState[InputProtocol, Core]
    ) -> TransitionRegistrar[
        Concatenate[InputProtocol, Core, Data, P], Concatenate[InputProtocol, P], R
    ]: ...
    @overload
    def to(
        self,
        state: TypedDataState[InputProtocol, Core, OtherData, P],
    ) -> TransitionRegistrar[
        Concatenate[InputProtocol, Core, Data, P],
        Concatenate[InputProtocol, P],
        R,
    ]: ...
    def to(
        self,
        state: (
            TypedState[InputProtocol, Core]
            | TypedDataState[InputProtocol, Core, Any, P]
        ),
    ) -> (
        TransitionRegistrar[Concatenate[InputProtocol, Core, P], P, R]
        | TransitionRegistrar[
            Concatenate[InputProtocol, Core, Data, P],
            Concatenate[InputProtocol, P],
            R,
        ]
    ):
        """
        Declare a state transition to a new state.
        """
        return TransitionRegistrar(self.input, self.old, state)

    def loop(self) -> TransitionRegistrar[
        Concatenate[InputProtocol, Core, Data, P],
        Concatenate[InputProtocol, P],
        R,
    ]:
        """
        Register a transition back to the same state.
        """
        return TransitionRegistrar(self.input, self.old, self.old)


@dataclass(frozen=True)
class TypedState(Generic[InputProtocol, Core]):
    """
    The result of L{.state() <automat.TypeMachineBuilder.state>}.
    """

    name: str
    builder: TypeMachineBuilder[InputProtocol, Core] = field(repr=False)

    def upon(
        self, input: Callable[Concatenate[InputProtocol, P], R]
    ) -> UponFromNo[InputProtocol, Core, P, R]:
        ".upon()"
        self.builder._checkMembership(input)
        return UponFromNo(self, input)

    def _produceOutputs(
        self,
        impl: Callable[..., object],
        old: (
            TypedDataState[InputProtocol, Core, OtherData, OtherFactoryParams]
            | TypedState[InputProtocol, Core]
        ),
        nodata: bool = False,
    ) -> Iterable[SomeOutput]:
        yield MethodOutput._fromImpl(impl, isinstance(old, TypedDataState))


@dataclass(frozen=True)
class TypedDataState(Generic[InputProtocol, Core, Data, FactoryParams]):
    name: str
    builder: TypeMachineBuilder[InputProtocol, Core] = field(repr=False)
    factory: Callable[Concatenate[InputProtocol, Core, FactoryParams], Data]

    @overload
    def upon(
        self, input: Callable[Concatenate[InputProtocol, P], R]
    ) -> UponFromData[InputProtocol, Core, P, R, Data]: ...
    @overload
    def upon(
        self, input: Callable[Concatenate[InputProtocol, P], R], nodata: Literal[False]
    ) -> UponFromData[InputProtocol, Core, P, R, Data]: ...
    @overload
    def upon(
        self, input: Callable[Concatenate[InputProtocol, P], R], nodata: Literal[True]
    ) -> UponFromNo[InputProtocol, Core, P, R]: ...
    def upon(
        self,
        input: Callable[Concatenate[InputProtocol, P], R],
        nodata: bool = False,
    ) -> (
        UponFromData[InputProtocol, Core, P, R, Data]
        | UponFromNo[InputProtocol, Core, P, R]
    ):
        self.builder._checkMembership(input)
        if nodata:
            return UponFromNo(self, input)
        else:
            return UponFromData(self, input)

    def _produceOutputs(
        self,
        impl: Callable[..., object],
        old: (
            TypedDataState[InputProtocol, Core, OtherData, OtherFactoryParams]
            | TypedState[InputProtocol, Core]
        ),
        nodata: bool,
    ) -> Iterable[SomeOutput]:
        if self is not old:
            yield DataOutput(self.factory)
        yield MethodOutput._fromImpl(
            impl, isinstance(old, TypedDataState) and not nodata
        )


AnyState: TypeAlias = "TypedState[Any, Any] | TypedDataState[Any, Any, Any, Any]"


@dataclass
class TypedInput:
    name: str


class SomeOutput(Protocol):
    """
    A state machine output.
    """

    @property
    def name(self) -> str:
        "read-only name property"

    def __call__(*args: Any, **kwargs: Any) -> Any: ...

    def __hash__(self) -> int:
        "must be hashable"


@dataclass
class InputImplementer(Generic[InputProtocol, Core]):
    """
    An L{InputImplementer} implements an input protocol in terms of a
    state machine.

    When the factory returned from L{TypeMachine}
    """

    __automat_core__: Core
    __automat_transitioner__: Transitioner[
        TypedState[InputProtocol, Core]
        | TypedDataState[InputProtocol, Core, object, ...],
        str,
        SomeOutput,
    ]
    __automat_data__: object | None = None
    __automat_postponed__: list[Callable[[], None]] | None = None


def implementMethod(
    method: Callable[..., object],
) -> Callable[..., object]:
    """
    Construct a function for populating in the synthetic provider of the Input
    Protocol to a L{TypeMachineBuilder}.  It should have a signature matching that
    of the C{method} parameter, a function from that protocol.
    """
    methodInput = method.__name__
    # side-effects can be re-ordered until later.  If you need to compute a
    # value in your method, then obviously it can't be invoked reentrantly.
    returnAnnotation = _liveSignature(method).return_annotation
    returnsNone = returnAnnotation is None

    def implementation(
        self: InputImplementer[InputProtocol, Core], *args: object, **kwargs: object
    ) -> object:
        transitioner = self.__automat_transitioner__
        dataAtStart = self.__automat_data__
        if self.__automat_postponed__ is not None:
            if not returnsNone:
                raise RuntimeError(
                    f"attempting to reentrantly run {method.__qualname__} "
                    f"but it wants to return {returnAnnotation!r} not None"
                )

            def rerunme() -> None:
                implementation(self, *args, **kwargs)

            self.__automat_postponed__.append(rerunme)
            return None
        postponed = self.__automat_postponed__ = []
        try:
            [outputs, tracer] = transitioner.transition(methodInput)
            result: Any = None
            for output in outputs:
                # here's the idea: there will be a state-setup output and a
                # state-teardown output. state-setup outputs are added to the
                # *beginning* of any entry into a state, so that by the time you
                # are running the *implementation* of a method that has entered
                # that state, the protocol is in a self-consistent state and can
                # run reentrant outputs.  not clear that state-teardown outputs are
                # necessary
                result = output(self, dataAtStart, *args, **kwargs)
        finally:
            self.__automat_postponed__ = None
        while postponed:
            postponed.pop(0)()
        return result

    implementation.__qualname__ = implementation.__name__ = (
        f"<implementation for {method}>"
    )
    return implementation


@dataclass(frozen=True)
class MethodOutput(Generic[Core]):
    """
    This is the thing that goes into the automaton's outputs list, and thus
    (per the implementation of L{implementMethod}) takes the 'self' of the
    InputImplementer instance (i.e. the synthetic protocol implementation) and the
    previous result computed by the former output, which will be None
    initially.
    """

    method: Callable[..., Any]
    requiresData: bool
    _assertion: Callable[[object], None]

    @classmethod
    def _fromImpl(
        cls: type[MethodOutput[Core]], method: Callable[..., Any], requiresData: bool
    ) -> MethodOutput[Core]:
        parameter = None
        annotation: type[object] = object

        def assertion(data: object) -> None:
            """
            No assertion about the data.
            """

        # Do our best to compute the declared signature, so that we caan verify
        # it's the right type.  We can't always do that.
        try:
            sig = _liveSignature(method)
        except NameError:
            ...
            # An inner function may refer to type aliases that only appear as
            # local variables, and those are just lost here; give up.
        else:
            if requiresData:
                # 0: self, 1: self.__automat_core__, 2: self.__automat_data__
                declaredParams = list(sig.parameters.values())
                if len(declaredParams) >= 3:
                    parameter = declaredParams[2]
                    annotation = parameter.annotation
                    origin = get_origin(annotation)
                    if origin is not None:
                        annotation = origin
                    if hasInterface and isinstance(annotation, InterfaceClass):

                        def assertion(data: object) -> None:
                            assert annotation.providedBy(data), (
                                f"expected {parameter} to provide {annotation} "
                                f"but got {type(data)} instead"
                            )

                    else:

                        def assertion(data: object) -> None:
                            assert isinstance(data, annotation), (
                                f"expected {parameter} to be {annotation} "
                                f"but got {type(data)} instead"
                            )

        return cls(method, requiresData, assertion)

    @property
    def name(self) -> str:
        return f"{self.method.__name__}"

    def __call__(
        self,
        machine: InputImplementer[InputProtocol, Core],
        dataAtStart: Data,
        /,
        *args: object,
        **kwargs: object,
    ) -> object:
        extraArgs = [machine, machine.__automat_core__]
        if self.requiresData:
            self._assertion(dataAtStart)
            extraArgs += [dataAtStart]
        # if anything is invoked reentrantly here, then we can't possibly have
        # set __automat_data__ and the data argument to the reentrant method
        # will be wrong.  we *need* to split out the construction / state-enter
        # hook, because it needs to run separately.
        return self.method(*extraArgs, *args, **kwargs)


@dataclass(frozen=True)
class DataOutput(Generic[Data]):
    """
    Construct an output for the given data objects.
    """

    dataFactory: Callable[..., Data]

    @property
    def name(self) -> str:
        return f"data:{self.dataFactory.__name__}"

    def __call__(
        realself,
        self: InputImplementer[InputProtocol, Core],
        dataAtStart: object,
        *args: object,
        **kwargs: object,
    ) -> Data:
        newData = realself.dataFactory(self, self.__automat_core__, *args, **kwargs)
        self.__automat_data__ = newData
        return newData


INVALID_WHILE_DESERIALIZING: TypedState[Any, Any] = TypedState(
    "automat:invalid-while-deserializing",
    None,  # type:ignore[arg-type]
)


@dataclass(frozen=True)
class TypeMachine(Generic[InputProtocol, Core]):
    """
    A L{TypeMachine} is a factory for instances of C{InputProtocol}.
    """

    __automat_type__: type[InputImplementer[InputProtocol, Core]]
    __automat_automaton__: Automaton[
        TypedState[InputProtocol, Core] | TypedDataState[InputProtocol, Core, Any, ...],
        str,
        SomeOutput,
    ]

    @overload
    def __call__(self, core: Core) -> InputProtocol: ...
    @overload
    def __call__(
        self, core: Core, state: TypedState[InputProtocol, Core]
    ) -> InputProtocol: ...
    @overload
    def __call__(
        self,
        core: Core,
        state: TypedDataState[InputProtocol, Core, OtherData, ...],
        dataFactory: Callable[[InputProtocol, Core], OtherData],
    ) -> InputProtocol: ...

    def __call__(
        self,
        core: Core,
        state: (
            TypedState[InputProtocol, Core]
            | TypedDataState[InputProtocol, Core, OtherData, ...]
            | None
        ) = None,
        dataFactory: Callable[[InputProtocol, Core], OtherData] | None = None,
    ) -> InputProtocol:
        """
        Construct an instance of C{InputProtocol} from an instance of the
        C{Core} protocol.
        """
        if state is None:
            state = initial = self.__automat_automaton__.initialState
        elif isinstance(state, TypedDataState):
            assert dataFactory is not None, "data state requires a data factory"
            # Ensure that the machine is in a state with *no* transitions while
            # we are doing the initial construction of its state-specific data.
            initial = INVALID_WHILE_DESERIALIZING
        else:
            initial = state

        internals: InputImplementer[InputProtocol, Core] = self.__automat_type__(
            core, txnr := Transitioner(self.__automat_automaton__, initial)
        )
        result: InputProtocol = internals  # type:ignore[assignment]

        if dataFactory is not None:
            internals.__automat_data__ = dataFactory(result, core)
            txnr._state = state
        return result

    def asDigraph(self) -> Digraph:
        from ._visualize import makeDigraph

        return makeDigraph(
            self.__automat_automaton__,
            stateAsString=lambda state: state.name,
            inputAsString=lambda input: input,
            outputAsString=lambda output: output.name,
        )


@dataclass(eq=False)
class TypeMachineBuilder(Generic[InputProtocol, Core]):
    """
    The main entry-point into Automat, used to construct a factory for
    instances of C{InputProtocol} that take an instance of C{Core}.

    Describe the machine with L{TypeMachineBuilder.state} L{.upon
    <automat._typed.TypedState.upon>} L{.to
    <automat._typed.UponFromNo.to>}, then build it with
    L{TypeMachineBuilder.build}, like so::

        from typing import Protocol
        class Inputs(Protocol):
            def method(self) -> None: ...
        class Core: ...

        from automat import TypeMachineBuilder
        builder = TypeMachineBuilder(Inputs, Core)
        state = builder.state("state")
        state.upon(Inputs.method).loop().returns(None)
        Machine = builder.build()

        machine = Machine(Core())
        machine.method()
    """

    # Public constructor parameters.
    inputProtocol: ProtocolAtRuntime[InputProtocol]
    coreType: type[Core]

    # Internal state, not in the constructor.
    _automaton: Automaton[
        TypedState[InputProtocol, Core] | TypedDataState[InputProtocol, Core, Any, ...],
        str,
        SomeOutput,
    ] = field(default_factory=Automaton, repr=False, init=False)
    _initial: bool = field(default=True, init=False)
    _registrars: list[TransitionRegistrar[..., ..., Any]] = field(
        default_factory=list, init=False
    )
    _built: bool = field(default=False, init=False)

    @overload
    def state(self, name: str) -> TypedState[InputProtocol, Core]: ...
    @overload
    def state(
        self,
        name: str,
        dataFactory: Callable[Concatenate[InputProtocol, Core, P], Data],
    ) -> TypedDataState[InputProtocol, Core, Data, P]: ...
    def state(
        self,
        name: str,
        dataFactory: Callable[Concatenate[InputProtocol, Core, P], Data] | None = None,
    ) -> TypedState[InputProtocol, Core] | TypedDataState[InputProtocol, Core, Data, P]:
        """
        Construct a state.
        """
        if self._built:
            raise AlreadyBuiltError(
                "Cannot add states to an already-built state machine."
            )
        if dataFactory is None:
            state = TypedState(name, self)
            if self._initial:
                self._initial = False
                self._automaton.initialState = state
            return state
        else:
            assert not self._initial, "initial state cannot require state-specific data"
            return TypedDataState(name, self, dataFactory)

    def build(self) -> TypeMachine[InputProtocol, Core]:
        """
        Create a L{TypeMachine}, and prevent further modification to the state
        machine being built.
        """
        # incompleteness check
        if self._built:
            raise AlreadyBuiltError("Cannot build a state machine twice.")
        self._built = True

        for registrar in self._registrars:
            registrar._checkComplete()

        # We were only hanging on to these for error-checking purposes, so we
        # can drop them now.
        del self._registrars[:]

        runtimeType: type[InputImplementer[InputProtocol, Core]] = type(
            f"Typed<{runtime_name(self.inputProtocol)}>",
            tuple([InputImplementer]),
            {
                method_name: implementMethod(getattr(self.inputProtocol, method_name))
                for method_name in actuallyDefinedProtocolMethods(self.inputProtocol)
            },
        )

        return TypeMachine(runtimeType, self._automaton)

    def _checkMembership(self, input: Callable[..., object]) -> None:
        """
        Ensure that ``input`` is a valid member function of the input protocol,
        not just a function that happens to take the right first argument.
        """
        if (checked := getattr(self.inputProtocol, input.__name__, None)) is not input:
            raise ValueError(
                f"{input.__qualname__} is not a member of {self.inputProtocol.__module__}.{self.inputProtocol.__name__}"
            )
