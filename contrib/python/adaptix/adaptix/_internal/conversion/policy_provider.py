from ..provider.essential import Mediator
from ..provider.methods_provider import MethodsProvider, method_handler
from .request_cls import UnlinkedOptionalPolicy, UnlinkedOptionalPolicyRequest


class UnlinkedOptionalPolicyProvider(MethodsProvider):
    def __init__(self, *, is_allowed: bool):
        self._is_allowed = is_allowed

    @method_handler
    def _unlinked_optional_policy(
        self,
        mediator: Mediator,
        request: UnlinkedOptionalPolicyRequest,
    ) -> UnlinkedOptionalPolicy:
        return UnlinkedOptionalPolicy(is_allowed=self._is_allowed)
