"""Internal Logfire logic.

We use the `_internal` module to discourage imports from outside the package,
and thereby avoid causing breaking changes when refactoring the package.
"""

from opentelemetry.sdk.resources import Resource

from logfire._internal.utils import platform_is_emscripten

if platform_is_emscripten():  # pragma: no cover
    # Resource.create starts a thread, which is not supported in Emscripten.
    # We have to patch it early like this because it gets called just by importing OTel logs modules.
    Resource.create = Resource  # type: ignore
