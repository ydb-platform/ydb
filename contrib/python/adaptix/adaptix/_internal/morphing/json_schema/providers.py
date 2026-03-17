from ...provider.essential import Mediator
from ...provider.located_request import LocatedRequestMethodsProvider
from ...provider.methods_provider import method_handler
from .definitions import RefSource
from .request_cls import InlineJSONSchemaRequest, RefSourceRequest


class InlineJSONSchemaProvider(LocatedRequestMethodsProvider):
    def __init__(self, *, inline: bool):
        self._inline = inline

    @method_handler
    def provide_inline_json_schema(self, mediator: Mediator, request: InlineJSONSchemaRequest) -> bool:
        return self._inline


class JSONSchemaRefProvider(LocatedRequestMethodsProvider):
    @method_handler
    def provide_ref_source(self, mediator: Mediator, request: RefSourceRequest) -> RefSource:
        return RefSource(
            value=None,
            json_schema=request.json_schema,
            loc_stack=request.loc_stack,
        )


class ConstantJSONSchemaRefProvider(LocatedRequestMethodsProvider):
    def __init__(self, ref_value: str):
        self._ref_value = ref_value

    @method_handler
    def provide_ref_source(self, mediator: Mediator, request: RefSourceRequest) -> RefSource:
        return RefSource(
            value=self._ref_value,
            json_schema=request.json_schema,
            loc_stack=request.loc_stack,
        )
