from collections.abc import Callable, Sequence
from typing import Any

from dishka.exception_base import DishkaError
from dishka.text_rendering import get_name


class NotAFactoryError(TypeError, DishkaError):
    def __init__(self, attempted_factory: Any) -> None:
        self.attempted = attempted_factory

    def __str__(self) -> str:
        return f"Cannot use {self.attempted!r} as a factory."


class CannotUseProtocolError(NotAFactoryError):
    def __str__(self) -> str:
        return (
            f"Cannot use {self.attempted} as a factory.\n"
            f"Tip: seems that this is a Protocol. "
            "Please subclass it and provide the subclass."
        )


class UnsupportedGeneratorReturnTypeError(TypeError, DishkaError):
    def __init__(
            self,
            name: str,
            guess: str,
            guessed_args: str,
            *,
            is_async: bool = False,
    ) -> None:
        self.name = name
        self.guess = guess
        self.guessed_args = guessed_args
        self.is_async = is_async

    def __str__(self) -> str:
        gen_type = "async generator" if self.is_async else "generator"
        return (
            f"Unsupported return type `{self.name}` for {gen_type}. "
            f"Did you mean {self.guess}[{self.guessed_args}]?"
        )


class MissingHintsError(ValueError, DishkaError):
    def __init__(
            self,
            source: Any,
            missing_hints: Sequence[str],
            *,
            append_init: bool = False,
    ) -> None:
        self.source = source
        self.missing = missing_hints
        self.append_init = append_init

    def __str__(self) -> str:
        name = get_name(self.source, include_module=True)
        if self.append_init:
            name += ".__init__"
        missing = ", ".join(self.missing)
        return (
            f"Failed to analyze `{name}`.\n"
            f"Some parameters do not have type hints: {missing}\n"
        )


class UndefinedTypeAnalysisError(NameError, DishkaError):
    def __init__(
            self,
            source: Any,
            type_name: str,
    ) -> None:
        self.source = source
        self.type_name = type_name
        self.message = self._construct_message()
        super().__init__(self.message, name=type_name)

    def _construct_message(self) -> str:
        name = get_name(self.source, include_module=True)
        return (
            f"Failed to analyze `{name}`. \n"
            f"Type '{self.type_name}' is not defined. \n\n"
            f"If you are using `if TYPE_CHECKING` "
            f"to import '{self.type_name}' "
            f"then try removing it. \n"
            f"Or, create a separate factory with all types imported."
        )

    def __str__(self) -> str:
        return self.message


class MissingReturnHintError(ValueError, DishkaError):
    def __init__(self, source: Any) -> None:
        self.source = source

    def __str__(self) -> str:
        name = get_name(self.source, include_module=True)
        return (
            f"Failed to analyze `{name}`. \n"
            f"Missing return type hint."
        )


class NoScopeSetInProvideError(ValueError, DishkaError):
    def __init__(
            self,
            provides_name: str,
            src_name: str,
            provider_name: str,
    ) -> None:
        self.provides_name = provides_name
        self.src_name = src_name
        self.provider_name = provider_name

    def __str__(self) -> str:
        return (
            f"No scope is set for {self.provides_name}.\n"
            f"Set in provide() call for {self.src_name} or "
            f"within {self.provider_name}"
        )


class NoScopeSetInContextError(ValueError, DishkaError):
    def __init__(
            self,
            provides_name: str,
            provider_name: str,
    ) -> None:
        self.provides_name = provides_name
        self.provider_name = provider_name

    def __str__(self) -> str:
        return (
            f"No scope is set for {self.provides_name}.\n"
            f"Set in from_context() call or within {self.provider_name}"
        )


class IndependentDecoratorError(ValueError, DishkaError):
    def __init__(self, source: Callable[..., Any] | type) -> None:
        self.source = source

    def __str__(self) -> str:
        name = get_name(self.source, include_module=True)
        return (
            f"Decorator {name} does not depend on provided type.\n"
            f"Did you mean @provide instead of @decorate?"
        )
