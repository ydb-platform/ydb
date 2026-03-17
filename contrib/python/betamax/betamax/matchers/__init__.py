from .base import BaseMatcher
from .body import BodyMatcher
from .digest_auth import DigestAuthMatcher
from .headers import HeadersMatcher
from .host import HostMatcher
from .method import MethodMatcher
from .path import PathMatcher
from .query import QueryMatcher
from .uri import URIMatcher

matcher_registry = {}

__all__ = ('BaseMatcher', 'BodyMatcher', 'DigestAuthMatcher',
           'HeadersMatcher', 'HostMatcher', 'MethodMatcher', 'PathMatcher',
           'QueryMatcher', 'URIMatcher', 'matcher_registry')


_matchers = [BodyMatcher, DigestAuthMatcher, HeadersMatcher, HostMatcher,
             MethodMatcher, PathMatcher, QueryMatcher, URIMatcher]
matcher_registry.update(dict((m.name, m()) for m in _matchers))
del _matchers
