from typing import Optional, TypeVar, Union

from .model_tools.definitions import DefaultFactory, DefaultFactoryWithSelf, DefaultValue
from .morphing.model.crown_definitions import Sieve

as_is_stub = lambda x: x  # noqa: E731
as_is_stub_with_ctx = lambda x, ctx: x  # noqa: E731

S = TypeVar("S", bound=Sieve)


_DEFAULT_CLAUSE_ATTR_NAME = "_adaptix_default_clause"


def with_default_clause(default: Union[DefaultValue, DefaultFactory, DefaultFactoryWithSelf], sieve: S) -> S:
    setattr(sieve, _DEFAULT_CLAUSE_ATTR_NAME, default)
    return sieve


def get_default_clause(sieve: Sieve) -> Optional[Union[DefaultValue, DefaultFactory, DefaultFactoryWithSelf]]:
    return getattr(sieve, _DEFAULT_CLAUSE_ATTR_NAME, None)
