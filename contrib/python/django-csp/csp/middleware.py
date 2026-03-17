from __future__ import annotations

import base64
import http.client as http_client
import os
import warnings
from dataclasses import asdict, dataclass
from functools import partial
from typing import TYPE_CHECKING

from django.conf import settings
from django.utils.deprecation import MiddlewareMixin
from django.utils.functional import SimpleLazyObject, empty

from csp.constants import HEADER, HEADER_REPORT_ONLY
from csp.exceptions import CSPNonceError
from csp.utils import DIRECTIVES_T, build_policy

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponseBase


@dataclass
class PolicyParts:
    # A dataclass is used rather than a namedtuple so that the attributes are mutable
    config: DIRECTIVES_T | None = None
    update: DIRECTIVES_T | None = None
    replace: DIRECTIVES_T | None = None
    nonce: str | None = None


class CheckableLazyObject(SimpleLazyObject):
    """A SimpleLazyObject where bool(obj) returns True if no longer lazy"""

    def __bool__(self) -> bool:
        """
        If the wrapped function has been evaluated, return True.
        If the wrapped function has not been evalated, return False.
        """
        return getattr(self, "_wrapped") is not empty


class CSPMiddleware(MiddlewareMixin):
    """
    Implements the Content-Security-Policy response header, which
    conforming user-agents can use to restrict the permitted sources
    of various content.

    See http://www.w3.org/TR/CSP/

    Can be customised by subclassing and extending the get_policy_parts method.
    """

    def _make_nonce(self, request: HttpRequest) -> str:
        # Ensure that any subsequent calls to request.csp_nonce return the same value
        stored_nonce = getattr(request, "_csp_nonce", None)
        if isinstance(stored_nonce, str):
            return stored_nonce
        nonce = base64.b64encode(os.urandom(16)).decode("ascii")
        setattr(request, "_csp_nonce", nonce)
        return nonce

    @staticmethod
    def _csp_nonce_post_response() -> None:
        raise CSPNonceError(
            "The 'csp_nonce' attribute is not available after the CSP header has been written. Consider adjusting your MIDDLEWARE order."
        )

    def process_request(self, request: HttpRequest) -> None:
        nonce = partial(self._make_nonce, request)
        setattr(request, "csp_nonce", CheckableLazyObject(nonce))

    def process_response(self, request: HttpRequest, response: HttpResponseBase) -> HttpResponseBase:
        # Check for debug view
        exempted_debug_codes = (
            http_client.INTERNAL_SERVER_ERROR,
            http_client.NOT_FOUND,
        )
        if response.status_code in exempted_debug_codes and settings.DEBUG:
            return response

        policy_parts = self.get_policy_parts(request=request, response=response)
        csp = build_policy(**asdict(policy_parts))
        if csp:
            # Only set header if not already set and not an excluded prefix and not exempted.
            is_not_exempt = getattr(response, "_csp_exempt", False) is False
            no_header = HEADER not in response
            policy = getattr(settings, "CONTENT_SECURITY_POLICY", None) or {}
            prefixes = policy.get("EXCLUDE_URL_PREFIXES", None) or ()
            is_not_excluded = not request.path_info.startswith(tuple(prefixes))
            if no_header and is_not_exempt and is_not_excluded:
                response[HEADER] = csp

        policy_parts_ro = self.get_policy_parts(request=request, response=response, report_only=True)
        csp_ro = build_policy(**asdict(policy_parts_ro), report_only=True)
        if csp_ro:
            # Only set header if not already set and not an excluded prefix and not exempted.
            is_not_exempt = getattr(response, "_csp_exempt_ro", False) is False
            no_header = HEADER_REPORT_ONLY not in response
            policy = getattr(settings, "CONTENT_SECURITY_POLICY_REPORT_ONLY", None) or {}
            prefixes = policy.get("EXCLUDE_URL_PREFIXES", None) or ()
            is_not_excluded = not request.path_info.startswith(tuple(prefixes))
            if no_header and is_not_exempt and is_not_excluded:
                response[HEADER_REPORT_ONLY] = csp_ro

        # Once we've written the header, accessing the `request.csp_nonce` will no longer trigger
        # the nonce to be added to the header. Instead we throw an error here to catch this since
        # this has security implications.
        if getattr(request, "_csp_nonce", None) is None:
            setattr(request, "csp_nonce", CheckableLazyObject(self._csp_nonce_post_response))

        return response

    def build_policy(self, request: HttpRequest, response: HttpResponseBase) -> str:
        warnings.warn("deprecated in favor of get_policy_parts", DeprecationWarning)
        policy_parts = self.get_policy_parts(request=request, response=response, report_only=False)
        return build_policy(**asdict(policy_parts))

    def build_policy_ro(self, request: HttpRequest, response: HttpResponseBase) -> str:
        warnings.warn("deprecated in favor of get_policy_parts", DeprecationWarning)
        policy_parts_ro = self.get_policy_parts(request=request, response=response, report_only=True)
        return build_policy(**asdict(policy_parts_ro), report_only=True)

    def get_policy_parts(
        self,
        request: HttpRequest,
        response: HttpResponseBase,
        report_only: bool = False,
    ) -> PolicyParts:
        if report_only:
            config = getattr(response, "_csp_config_ro", None)
            update = getattr(response, "_csp_update_ro", None)
            replace = getattr(response, "_csp_replace_ro", None)
        else:
            config = getattr(response, "_csp_config", None)
            update = getattr(response, "_csp_update", None)
            replace = getattr(response, "_csp_replace", None)

        nonce = getattr(request, "_csp_nonce", None)

        return PolicyParts(config, update, replace, nonce)
