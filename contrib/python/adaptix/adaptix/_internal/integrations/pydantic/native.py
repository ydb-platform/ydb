from contextlib import suppress
from typing import Any, Callable, Literal, Optional, TypedDict, TypeVar, Union

from ...common import Dumper, Loader
from ...morphing.load_error import LoadError
from ...morphing.provider_template import DumperProvider, LoaderProvider
from ...morphing.request_cls import DumperRequest, LoaderRequest
from ...provider.essential import Mediator, Provider
from ...provider.facade.provider import bound_by_any
from ...provider.loc_stack_filtering import Pred

with suppress(ImportError):
    from pydantic import ConfigDict, TypeAdapter, ValidationError
    from pydantic.main import IncEx


class ValidatePythonParams(TypedDict, total=False):
    strict: Optional[bool]
    from_attributes: Optional[bool]
    context: Optional[Any]
    self_instance: Optional[Any]
    allow_partial: Union[bool, Literal["off", "on", "trailing-strings"]]


class ToPythonParams(TypedDict, total=False):
    mode: Optional[str]
    include: Optional["IncEx"]
    exclude: Optional["IncEx"]
    by_alias: bool
    exclude_unset: bool
    exclude_defaults: bool
    exclude_none: bool
    round_trip: bool
    warnings: Union[bool, Literal["none", "warn", "error"]]
    fallback: Optional[Callable[[Any], Any]]
    serialize_as_any: bool
    context: Optional[Any]


T = TypeVar("T")


class NativePydanticProvider(LoaderProvider, DumperProvider):
    def __init__(
        self,
        config: Optional["ConfigDict"],
        validate_python_params: ValidatePythonParams,
        to_python_params: ToPythonParams,
    ):
        self._config = config
        self._validate_python_params = validate_python_params
        self._to_python_params = to_python_params

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        validator = TypeAdapter(request.last_loc.type, config=self._config).validator.validate_python

        if validate_python_params := self._validate_python_params:
            def native_pydantic_loader(data):
                try:
                    return validator(data, **validate_python_params)
                except ValidationError as e:
                    raise LoadError from e

            return native_pydantic_loader

        def native_pydantic_loader_no_params(data):
            try:
                return validator(data)
            except ValidationError as e:
                raise LoadError from e

        return native_pydantic_loader_no_params

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        serializer = TypeAdapter(request.last_loc.type, config=self._config).serializer.to_python

        if to_python_params := self._to_python_params:
            def native_pydantic_dumper(data):
                return serializer(data, **to_python_params)

            return native_pydantic_dumper

        return serializer


def native_pydantic(
    *preds: Pred,
    config: Optional["ConfigDict"] = None,
    validate_python: Optional[ValidatePythonParams] = None,
    to_python: Optional[ToPythonParams] = None,
) -> Provider:
    """Provider that represents value via pydantic.
    You can use this function to validate or serialize pydantic models via pydantic itself.
    Provider constructs ``TypeAdapter`` for a type to load and dump data.

    :param preds: Predicates specifying where the provider should be used.
        The provider will be applied if any predicates meet the conditions,
        if no predicates are passed, the provider will be used for all Enums.
        See :ref:`predicate-system` for details.

    :param validate_python: Dict directly unpacked to ``.validate_python()`` method
    :param to_python: Dict directly unpacked to ``.to_python()`` method
    :param config: Parameter passed directly to ``config`` parameter of ``TypeAdapter`` constructor

    :return: Desired provider
    """
    return bound_by_any(
        preds,
        NativePydanticProvider(
            config=config,
            validate_python_params={} if validate_python is None else validate_python,
            to_python_params={} if to_python is None else to_python,
        ),
    )
