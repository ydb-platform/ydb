#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from ..compat import unescape, urlencode, urlsplit
from ..constants import (
    HTTP_HEADER_ACCEPT,
    HTTP_HEADER_CONTENT_TYPE,
    HTTP_HEADER_SERVICE_NAME,
    HTTP_HEADER_USER_AGENT,
)
from ..errorcode import ER_IDP_CONNECTION_ERROR, ER_INCORRECT_DESTINATION
from ..errors import DatabaseError, Error
from ..network import CONTENT_TYPE_APPLICATION_JSON, PYTHON_CONNECTOR_USER_AGENT
from ..sqlstate import SQLSTATE_CONNECTION_WAS_NOT_ESTABLISHED
from . import Auth
from .by_plugin import AuthByPlugin, AuthType

if TYPE_CHECKING:
    from .. import SnowflakeConnection

logger = logging.getLogger(__name__)


def _is_prefix_equal(url1, url2):
    """Checks if URL prefixes are identical.

    The scheme, hostname and port number are compared. If the port number is not specified and the scheme is https,
    the port number is assumed to be 443.
    """
    parsed_url1 = urlsplit(url1)
    parsed_url2 = urlsplit(url2)

    port1 = parsed_url1.port
    if not port1 and parsed_url1.scheme == "https":
        port1 = "443"
    port2 = parsed_url1.port
    if not port2 and parsed_url2.scheme == "https":
        port2 = "443"

    return (
        parsed_url1.hostname == parsed_url2.hostname
        and port1 == port2
        and parsed_url1.scheme == parsed_url2.scheme
    )


def _get_post_back_url_from_html(html):
    """Gets the post back URL.

    Since the HTML is not well-formed, minidom cannot be used to convert to
    DOM. The first discovered form is assumed to be the form to post back
    and the URL is taken from action attributes.
    """
    logger.debug(html)

    idx = html.find("<form")
    start_idx = html.find('action="', idx)
    end_idx = html.find('"', start_idx + 8)
    return unescape(html[start_idx + 8 : end_idx])


class AuthByOkta(AuthByPlugin):
    """Authenticate user by OKTA."""

    def __init__(self, application: str) -> None:
        super().__init__()
        self._saml_response = None
        self._application = application

    def reset_secrets(self) -> None:
        pass

    @property
    def type_(self) -> AuthType:
        return AuthType.OKTA

    @property
    def assertion_content(self) -> str:
        return self._saml_response

    def update_body(self, body: dict[Any, Any]) -> None:
        body["data"]["RAW_SAML_RESPONSE"] = self._saml_response

    def prepare(
        self,
        *,
        conn: SnowflakeConnection,
        authenticator: str,
        service_name: str | None,
        account: str,
        user: str,
        password: str,
        **kwargs: Any,
    ) -> None:
        """SAML Authentication.

        Steps are:
        1.  query GS to obtain IDP token and SSO url
        2.  IMPORTANT Client side validation:
            validate both token url and sso url contains same prefix
            (protocol + host + port) as the given authenticator url.
            Explanation:
            This provides a way for the user to 'authenticate' the IDP it is
            sending his/her credentials to.  Without such a check, the user could
            be coerced to provide credentials to an IDP impersonator.
        3.  query IDP token url to authenticate and retrieve access token
        4.  given access token, query IDP URL snowflake app to get SAML response
        5.  IMPORTANT Client side validation:
            validate the post back url come back with the SAML response
            contains the same prefix as the Snowflake's server url, which is the
            intended destination url to Snowflake.
        Explanation:
            This emulates the behavior of IDP initiated login flow in the user
            browser where the IDP instructs the browser to POST the SAML
            assertion to the specific SP endpoint.  This is critical in
            preventing a SAML assertion issued to one SP from being sent to
            another SP.
        """
        logger.debug("authenticating by SAML")
        headers, sso_url, token_url = self._step1(
            conn,
            authenticator,
            service_name,
            account,
            user,
        )
        self._step2(conn, authenticator, sso_url, token_url)
        one_time_token = self._step3(conn, headers, token_url, user, password)
        response_html = self._step4(conn, one_time_token, sso_url)
        self._step5(conn, response_html)

    def reauthenticate(self, **kwargs: Any) -> dict[str, bool]:
        return {"success": False}

    def _step1(
        self,
        conn: SnowflakeConnection,
        authenticator: str,
        service_name: str | None,
        account: str,
        user: str,
    ) -> tuple[dict[str, str], str, str]:
        logger.debug("step 1: query GS to obtain IDP token and SSO url")

        headers = {
            HTTP_HEADER_CONTENT_TYPE: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_ACCEPT: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_USER_AGENT: PYTHON_CONNECTOR_USER_AGENT,
        }
        if service_name:
            headers[HTTP_HEADER_SERVICE_NAME] = service_name
        url = "/session/authenticator-request"
        body = Auth.base_auth_data(
            user,
            account,
            conn.application,
            conn._internal_application_name,
            conn._internal_application_version,
            conn._ocsp_mode(),
            conn._login_timeout,
            conn._network_timeout,
        )

        body["data"]["AUTHENTICATOR"] = authenticator
        logger.debug(
            "account=%s, authenticator=%s",
            account,
            authenticator,
        )
        ret = conn._rest._post_request(
            url,
            headers,
            json.dumps(body),
            timeout=conn._rest._connection.login_timeout,
            socket_timeout=conn._rest._connection.login_timeout,
        )

        if not ret["success"]:
            self._handle_failure(conn=conn, ret=ret)

        data = ret["data"]
        token_url = data["tokenUrl"]
        sso_url = data["ssoUrl"]
        return headers, sso_url, token_url

    def _step2(
        self,
        conn: SnowflakeConnection,
        authenticator: str,
        sso_url: str,
        token_url: str,
    ) -> None:
        logger.debug(
            "step 2: validate Token and SSO URL has the same prefix as authenticator"
        )
        if not _is_prefix_equal(authenticator, token_url) or not _is_prefix_equal(
            authenticator, sso_url
        ):
            Error.errorhandler_wrapper(
                conn._rest._connection,
                None,
                DatabaseError,
                {
                    "msg": (
                        "The specified authenticator is not supported: "
                        f"{authenticator}, token_url: {token_url}, "
                        f"sso_url: {sso_url}"
                    ),
                    "errno": ER_IDP_CONNECTION_ERROR,
                    "sqlstate": SQLSTATE_CONNECTION_WAS_NOT_ESTABLISHED,
                },
            )

    @staticmethod
    def _step3(
        conn: SnowflakeConnection,
        headers: dict[str, str],
        token_url: str,
        user: str,
        password: str,
    ) -> str:
        logger.debug(
            "step 3: query IDP token url to authenticate and " "retrieve access token"
        )
        data = {
            "username": user,
            "password": password,
        }
        ret = conn._rest.fetch(
            "post",
            token_url,
            headers,
            data=json.dumps(data),
            timeout=conn._rest._connection.login_timeout,
            socket_timeout=conn._rest._connection.login_timeout,
            catch_okta_unauthorized_error=True,
        )
        one_time_token = ret.get("sessionToken", ret.get("cookieToken"))
        if not one_time_token:
            Error.errorhandler_wrapper(
                conn._rest._connection,
                None,
                DatabaseError,
                {
                    "msg": (
                        "The authentication failed for {user} "
                        "by {token_url}.".format(
                            token_url=token_url,
                            user=user,
                        )
                    ),
                    "errno": ER_IDP_CONNECTION_ERROR,
                    "sqlstate": SQLSTATE_CONNECTION_WAS_NOT_ESTABLISHED,
                },
            )
        return one_time_token

    @staticmethod
    def _step4(
        conn: SnowflakeConnection,
        one_time_token: str,
        sso_url: str,
    ):
        logger.debug("step 4: query IDP URL snowflake app to get SAML " "response")
        url_parameters = {
            "RelayState": "/some/deep/link",
            "onetimetoken": one_time_token,
        }
        sso_url = sso_url + "?" + urlencode(url_parameters)
        headers = {
            HTTP_HEADER_ACCEPT: "*/*",
        }
        response_html = conn._rest.fetch(
            "get",
            sso_url,
            headers,
            timeout=conn.login_timeout,
            socket_timeout=conn.login_timeout,
            is_raw_text=True,
        )
        return response_html

    def _step5(
        self,
        conn: SnowflakeConnection,
        response_html: str,
    ) -> None:
        logger.debug("step 5: validate post_back_url matches Snowflake URL")
        post_back_url = _get_post_back_url_from_html(response_html)
        full_url = "{protocol}://{host}:{port}".format(
            protocol=conn._rest._protocol,
            host=conn._rest._host,
            port=conn._rest._port,
        )
        if not _is_prefix_equal(post_back_url, full_url):
            Error.errorhandler_wrapper(
                conn._rest._connection,
                None,
                DatabaseError,
                {
                    "msg": (
                        "The specified authenticator and destination "
                        "URL in the SAML assertion do not match: "
                        "expected: {url}, "
                        "post back: {post_back_url}".format(
                            url=full_url,
                            post_back_url=post_back_url,
                        )
                    ),
                    "errno": ER_INCORRECT_DESTINATION,
                    "sqlstate": SQLSTATE_CONNECTION_WAS_NOT_ESTABLISHED,
                },
            )
        self._saml_response = response_html
