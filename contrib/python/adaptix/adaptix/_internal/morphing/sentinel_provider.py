from typing import NoReturn

from ..common import Dumper, Loader
from ..provider.essential import CannotProvide, Mediator
from ..provider.loc_stack_filtering import LocStack
from ..provider.located_request import LocatedRequest
from ..provider.methods_provider import method_handler
from .json_schema.definitions import JSONSchema
from .json_schema.request_cls import JSONSchemaRequest
from .provider_template import MorphingProvider
from .request_cls import DumperRequest, LoaderRequest


class IsSentinelRequest(LocatedRequest[bool]):
    pass


class SentinelProvider(MorphingProvider):
    def _raise_error(self, target: str) -> NoReturn:
        raise CannotProvide(
            f"Sentinels are not meant to be represented externally, so they do not have {target}."
            " It can be used only inside unions",
            is_terminal=True,
            is_demonstrative=True,
        )

    def provide_loader(self, mediator: Mediator[Loader], request: LoaderRequest) -> Loader:
        self._raise_error("loader")

    def provide_dumper(self, mediator: Mediator[Dumper], request: DumperRequest) -> Dumper:
        self._raise_error("dumper")

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        self._raise_error("JSON Schema")

    @method_handler
    def provide_is_sentinel(self, mediator: Mediator, request: IsSentinelRequest) -> bool:
        return True


def check_is_sentinel(mediator: Mediator, loc_stack: LocStack) -> bool:
    try:
        return mediator.provide(IsSentinelRequest(loc_stack=loc_stack))
    except CannotProvide:
        return False
