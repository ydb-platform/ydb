# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.apps._secret import Secret as Secret
    from stripe.apps._secret_service import SecretService as SecretService

# name -> (import_target, is_submodule)
_import_map = {
    "Secret": ("stripe.apps._secret", False),
    "SecretService": ("stripe.apps._secret_service", False),
}
if not TYPE_CHECKING:

    def __getattr__(name):
        try:
            target, is_submodule = _import_map[name]
            module = import_module(target)
            if is_submodule:
                return module

            return getattr(
                module,
                name,
            )
        except KeyError:
            raise AttributeError()
