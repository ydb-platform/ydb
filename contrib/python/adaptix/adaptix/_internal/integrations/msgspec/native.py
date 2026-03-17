from collections.abc import Iterable
from contextlib import suppress
from typing import Any, Callable, Optional, TypedDict

from ...common import Dumper, Loader
from ...morphing.load_error import LoadError
from ...morphing.provider_template import DumperProvider, LoaderProvider
from ...morphing.request_cls import DumperRequest, LoaderRequest
from ...provider.essential import Mediator, Provider
from ...provider.facade.provider import bound_by_any
from ...provider.loc_stack_filtering import Pred

with suppress(ImportError):
    from msgspec import ValidationError, convert, to_builtins


class ConvertParams(TypedDict, total=False):
    builtin_types: Iterable[type]
    str_keys: bool
    strict: bool
    from_attributes: bool
    dec_hook: Callable[[Any], Any]


class ToBuiltinsParams(TypedDict, total=False):
    builtin_types: Iterable[type]
    str_keys: bool
    enc_hook: Callable[[Any], Any]


class NativeMsgspecProvider(LoaderProvider, DumperProvider):
    def __init__(
        self,
        convert_params: ConvertParams,
        to_builtins_params: ToBuiltinsParams,
    ):
        self._convert_params = convert_params
        self._to_builtins_params = to_builtins_params

    def provide_loader(self, mediator: Mediator[Loader], request: LoaderRequest) -> Loader:
        tp = request.last_loc.type
        if convert_params := self._convert_params:
            def native_msgspec_loader(data):
                try:
                    return convert(data, type=tp, **convert_params)
                except ValidationError as e:
                    raise LoadError() from e

            return native_msgspec_loader

        def native_msgspec_loader_no_params(data):
            try:
                return convert(data, type=tp)
            except ValidationError as e:
                raise LoadError() from e

        return native_msgspec_loader_no_params

    def provide_dumper(self, mediator: Mediator[Dumper], request: DumperRequest) -> Dumper:
        if to_builtins_params := self._to_builtins_params:
            def native_msgspec_dumper_with_params(data):
                return to_builtins(data, **to_builtins_params)

            return native_msgspec_dumper_with_params

        return to_builtins


def native_msgspec(
    *preds: Pred,
    convert: Optional[ConvertParams] = None,
    to_builtins: Optional[ToBuiltinsParams] = None,
) -> Provider:
    """Provider that represents value via msgspec.
    You can use this function to validate or serialize msgspec structs via msgspec itself.
    Provider calls ``convert`` and ``to_builtins`` for a type to load and dump data.

    :param preds: Predicates specifying where the provider should be used.
        The provider will be applied if any predicates meet the conditions,
        if no predicates are passed, the provider will be used for all Enums.
        See :ref:`predicate-system` for details.

    :param convert: Dict directly unpacked to ``convert()`` function
    :param to_builtins: Dict directly unpacked to ``to_builtins()`` function

    :return: Desired provider
    """
    return bound_by_any(
        preds,
        NativeMsgspecProvider(
            convert_params={} if convert is None else convert,
            to_builtins_params={} if to_builtins is None else to_builtins,
        ),
    )
