from collections.abc import Sequence

from ...utils import Omitted
from ..essential import Provider
from ..loc_stack_filtering import OrLocStackChecker, Pred, create_loc_stack_checker
from ..located_request import LocStackBoundingProvider


def bound_by_any(preds: Sequence[Pred], provider: Provider) -> Provider:
    if len(preds) == 0:
        return provider
    if len(preds) == 1:
        return LocStackBoundingProvider(create_loc_stack_checker(preds[0]), provider)
    return LocStackBoundingProvider(
        OrLocStackChecker([create_loc_stack_checker(pred) for pred in preds]),
        provider,
    )


def bound(pred: Pred, provider: Provider) -> Provider:
    if pred == Omitted():
        return provider
    return LocStackBoundingProvider(create_loc_stack_checker(pred), provider)
