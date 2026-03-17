#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from ._auth import Auth, get_public_key_fingerprint, get_token_from_private_key
from .by_plugin import AuthByPlugin, AuthType
from .default import AuthByDefault
from .keypair import AuthByKeyPair
from .oauth import AuthByOAuth
from .okta import AuthByOkta
from .usrpwdmfa import AuthByUsrPwdMfa
from .webbrowser import AuthByWebBrowser

FIRST_PARTY_AUTHENTICATORS = frozenset(
    (
        AuthByDefault,
        AuthByKeyPair,
        AuthByOAuth,
        AuthByOkta,
        AuthByUsrPwdMfa,
        AuthByWebBrowser,
    )
)

__all__ = [
    "AuthByPlugin",
    "AuthByDefault",
    "AuthByKeyPair",
    "AuthByOAuth",
    "AuthByOkta",
    "AuthByUsrPwdMfa",
    "AuthByWebBrowser",
    "Auth",
    "AuthType",
    "FIRST_PARTY_AUTHENTICATORS",
    "get_public_key_fingerprint",
    "get_token_from_private_key",
]
