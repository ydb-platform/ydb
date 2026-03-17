from typing import Any, Callable, Optional, overload

from ...common import OneArgCoercer
from ...model_tools.definitions import DefaultFactory, DefaultValue
from ...provider.essential import Provider
from ...provider.facade.provider import bound_by_any
from ...provider.loc_stack_filtering import LocStackChecker, Pred, create_loc_stack_checker
from ..coercer_provider import MatchingCoercerProvider
from ..linking_provider import ConstantLinkingProvider, FunctionLinkingProvider, MatchingLinkingProvider
from ..policy_provider import UnlinkedOptionalPolicyProvider
from ..request_filtering import FromCtxParam


def link(src: Pred, dst: Pred, *, coercer: Optional[OneArgCoercer] = None) -> Provider:
    """Basic provider to define custom linking between fields.

    :param src: Predicate specifying source point of linking. See :ref:`predicate-system` for details.
    :param dst: Predicate specifying destination point of linking. See :ref:`predicate-system` for details.
    :param coercer: Function transforming source value to target.
        It has higher priority than generic coercers defined by :func:`.coercer`.
    :return: Desired provider
    """
    return MatchingLinkingProvider(
        src_lsc=create_loc_stack_checker(src),
        dst_lsc=create_loc_stack_checker(dst),
        coercer=coercer,
    )


@overload
def link_constant(dst: Pred, *, value: Any) -> Provider:
    ...


@overload
def link_constant(dst: Pred, *, factory: Callable[[], Any]) -> Provider:
    ...


def link_constant(dst: Pred, *, value: Any = None, factory: Any = None) -> Provider:
    """Provider that passes a constant value or the result of a function call to a field.

    :param dst: Predicate specifying destination point of linking. See :ref:`predicate-system` for details.
    :param value: A value is passed to the field.
    :param factory: A callable producing value passed to the field.
    :return: Desired provider
    """
    return ConstantLinkingProvider(
        create_loc_stack_checker(dst),
        DefaultFactory(factory) if factory is not None else DefaultValue(value),
    )


def link_function(func: Callable, dst: Pred) -> Provider:
    """Provider that uses function to produce value of destination field.

    The entire model is passed to the first parameter of the function.
    The type of result and first parameter are not checked, you must ensure type compatibility yourself.

    Keyword-only parameters link to model fields and other parameters link to extra converter parameters.
    After linking, the default type coercing mechanism is applied.

    :param func: A function used to process several fields of source model.
    :param dst: Predicate specifying destination point of linking. See :ref:`predicate-system` for details.
    :return: Desired provider
    """
    return FunctionLinkingProvider(
        func=func,
        dst_lsc=create_loc_stack_checker(dst),
    )


def coercer(src: Pred, dst: Pred, func: OneArgCoercer) -> Provider:
    """Basic provider to define custom coercer.

    :param src: Predicate specifying source point of linking. See :ref:`predicate-system` for details.
    :param dst: Predicate specifying destination point of linking. See :ref:`predicate-system` for details.
    :param func: The function is used to transform input data to a destination type.
    :return: Desired provider
    """
    return MatchingCoercerProvider(
        src_lsc=create_loc_stack_checker(src),
        dst_lsc=create_loc_stack_checker(dst),
        coercer=func,
    )


def allow_unlinked_optional(*preds: Pred) -> Provider:
    """Sets policy to permit optional fields that does not linked to any source field.

    :param preds: Predicate specifying target of policy.
        Each predicate is merged via ``|`` operator.
        See :ref:`predicate-system` for details.
    :return: Desired provider.
    """
    return bound_by_any(preds, UnlinkedOptionalPolicyProvider(is_allowed=True))


def forbid_unlinked_optional(*preds: Pred) -> Provider:
    """Sets policy to prohibit optional fields that does not linked to any source field.

    :param preds: Predicate specifying target of policy.
        Each predicate is merged via ``|`` operator.
        See :ref:`predicate-system` for details.
    :return: Desired provider.
    """
    return bound_by_any(preds, UnlinkedOptionalPolicyProvider(is_allowed=False))


def from_param(param_name: str) -> LocStackChecker:
    """The special predicate form matching only top-level parameters by name"""
    return FromCtxParam(param_name)
