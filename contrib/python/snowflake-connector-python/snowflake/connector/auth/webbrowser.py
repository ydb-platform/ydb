#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import json
import logging
import os
import socket
import time
import webbrowser
from types import ModuleType
from typing import TYPE_CHECKING, Any

from ..compat import parse_qs, urlparse, urlsplit
from ..constants import (
    HTTP_HEADER_ACCEPT,
    HTTP_HEADER_CONTENT_TYPE,
    HTTP_HEADER_SERVICE_NAME,
    HTTP_HEADER_USER_AGENT,
)
from ..errorcode import (
    ER_IDP_CONNECTION_ERROR,
    ER_NO_HOSTNAME_FOUND,
    ER_UNABLE_TO_OPEN_BROWSER,
)
from ..errors import OperationalError
from ..network import (
    CONTENT_TYPE_APPLICATION_JSON,
    EXTERNAL_BROWSER_AUTHENTICATOR,
    PYTHON_CONNECTOR_USER_AGENT,
)
from . import Auth
from .by_plugin import AuthByPlugin, AuthType

if TYPE_CHECKING:
    from .. import SnowflakeConnection

logger = logging.getLogger(__name__)

BUF_SIZE = 16384


# global state of web server that receives the SAML assertion from
# Snowflake server


class AuthByWebBrowser(AuthByPlugin):
    """Authenticates user by web browser. Only used for SAML based authentication."""

    def __init__(
        self,
        application: str,
        webbrowser_pkg: ModuleType | None = None,
        socket_pkg: type[socket.socket] | None = None,
        protocol: str | None = None,
        host: str | None = None,
        port: str | None = None,
    ) -> None:
        super().__init__()
        self.consent_cache_id_token = True
        self._token: str | None = None
        self._application = application
        self._proof_key = None
        self._webbrowser: ModuleType = (
            webbrowser if webbrowser_pkg is None else webbrowser_pkg
        )
        self._socket: type[socket.socket] = (
            socket.socket if socket_pkg is None else socket_pkg
        )
        self._protocol = protocol
        self._host = host
        self._port = port
        self._origin = None

    def reset_secrets(self) -> None:
        self._token = None

    @property
    def type_(self) -> AuthType:
        return AuthType.EXTERNAL_BROWSER

    @property
    def assertion_content(self) -> str:
        """Returns the token."""
        return self._token

    def update_body(self, body: dict[Any, Any]) -> None:
        """Used by Auth to update the request that gets sent to /v1/login-request.

        Args:
            body: existing request dictionary
        """
        body["data"]["AUTHENTICATOR"] = EXTERNAL_BROWSER_AUTHENTICATOR
        body["data"]["TOKEN"] = self._token
        body["data"]["PROOF_KEY"] = self._proof_key

    def prepare(
        self,
        *,
        conn: SnowflakeConnection,
        authenticator: str,
        service_name: str | None,
        account: str,
        user: str,
        **kwargs: Any,
    ) -> None:
        """Web Browser based Authentication."""
        logger.debug("authenticating by Web Browser")

        socket_connection = self._socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            try:
                socket_connection.bind(
                    (
                        os.getenv("SF_AUTH_SOCKET_ADDR", "localhost"),
                        int(os.getenv("SF_AUTH_SOCKET_PORT", 0)),
                    )
                )
            except socket.gaierror as ex:
                if ex.args[0] == socket.EAI_NONAME:
                    raise OperationalError(
                        msg="localhost is not found. Ensure /etc/hosts has "
                        "localhost entry.",
                        errno=ER_NO_HOSTNAME_FOUND,
                    )
                else:
                    raise ex
            socket_connection.listen(0)  # no backlog
            callback_port = socket_connection.getsockname()[1]

            print(
                "Initiating login request with your identity provider. A "
                "browser window should have opened for you to complete the "
                "login. If you can't see it, check existing browser windows, "
                "or your OS settings. Press CTRL+C to abort and try again..."
            )

            logger.debug("step 1: query GS to obtain SSO url")
            sso_url = self._get_sso_url(
                conn, authenticator, service_name, account, callback_port, user
            )

            logger.debug("step 2: open a browser")
            if not self._webbrowser.open_new(sso_url):
                print(
                    "We were unable to open a browser window for you, "
                    "please open the following url manually then paste the "
                    "URL you are redirected to into the terminal."
                )
                print(f"URL: {sso_url}")
                url = input("Enter the URL the SSO URL redirected you to: ")
                self._process_get_url(url)
                if not self._token:
                    # Input contained no token, either URL was incorrectly pasted,
                    # empty or just wrong
                    self._handle_failure(
                        conn=conn,
                        ret={
                            "code": ER_UNABLE_TO_OPEN_BROWSER,
                            "message": (
                                "Unable to open a browser in this environment and "
                                "SSO URL contained no token"
                            ),
                        },
                    )
                    return
            else:
                logger.debug("step 3: accept SAML token")
                self._receive_saml_token(conn, socket_connection)
        finally:
            socket_connection.close()

    def reauthenticate(
        self,
        *,
        conn: SnowflakeConnection,
        **kwargs: Any,
    ) -> dict[str, bool]:
        conn.authenticate_with_retry(self)
        return {"success": True}

    def _receive_saml_token(self, conn: SnowflakeConnection, socket_connection) -> None:
        """Receives SAML token from web browser."""
        while True:
            socket_client, _ = socket_connection.accept()
            try:
                # Receive the data in small chunks and retransmit it
                data = socket_client.recv(BUF_SIZE).decode("utf-8").split("\r\n")

                if not self._process_options(data, socket_client):
                    self._process_receive_saml_token(conn, data, socket_client)
                    break
            finally:
                socket_client.shutdown(socket.SHUT_RDWR)
                socket_client.close()

    def _process_options(self, data: list[str], socket_client: socket.socket) -> bool:
        """Allows JS Ajax access to this endpoint."""
        for line in data:
            if line.startswith("OPTIONS "):
                break
        else:
            return False

        self._get_user_agent(data)
        requested_headers, requested_origin = self._check_post_requested(data)
        if not requested_headers:
            return False

        if not self._validate_origin(requested_origin):
            # validate Origin and fail if not match with the server.
            return False

        self._origin = requested_origin
        content = [
            "HTTP/1.1 200 OK",
            "Date: {}".format(
                time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
            ),
            "Access-Control-Allow-Methods: POST, GET",
            f"Access-Control-Allow-Headers: {requested_headers}",
            "Access-Control-Max-Age: 86400",
            f"Access-Control-Allow-Origin: {self._origin}",
            "",
            "",
        ]
        socket_client.sendall("\r\n".join(content).encode("utf-8"))
        return True

    def _validate_origin(self, requested_origin: str) -> bool:
        ret = urlsplit(requested_origin)
        netloc = ret.netloc.split(":")
        host_got = netloc[0]
        port_got = (
            netloc[1] if len(netloc) > 1 else (443 if self._protocol == "https" else 80)
        )

        return (
            ret.scheme == self._protocol
            and host_got == self._host
            and port_got == self._port
        )

    def _process_receive_saml_token(
        self, conn: SnowflakeConnection, data: list[str], socket_client: socket.socket
    ) -> None:
        if not self._process_get(data) and not self._process_post(conn, data):
            return  # error

        content = [
            "HTTP/1.1 200 OK",
            "Content-Type: text/html",
        ]
        if self._origin:
            data = {"consent": self.consent_cache_id_token}
            msg = json.dumps(data)
            content.append(f"Access-Control-Allow-Origin: {self._origin}")
            content.append("Vary: Accept-Encoding, Origin")
        else:
            msg = """
<!DOCTYPE html><html><head><meta charset="UTF-8"/>
<title>SAML Response for Snowflake</title></head>
<body>
Your identity was confirmed and propagated to Snowflake {}.
You can close this window now and go back where you started from.
</body></html>""".format(
                self._application
            )
        content.append(f"Content-Length: {len(msg)}")
        content.append("")
        content.append(msg)

        socket_client.sendall("\r\n".join(content).encode("utf-8"))

    def _check_post_requested(self, data: list[str]) -> tuple[str | None, str | None]:
        request_line = None
        header_line = None
        origin_line = None
        for line in data:
            if line.startswith("Access-Control-Request-Method:"):
                request_line = line
            elif line.startswith("Access-Control-Request-Headers:"):
                header_line = line
            elif line.startswith("Origin:"):
                origin_line = line

        if (
            not request_line
            or not header_line
            or not origin_line
            or request_line.split(":")[1].strip() != "POST"
        ):
            return None, None

        return (
            header_line.split(":")[1].strip(),
            ":".join(origin_line.split(":")[1:]).strip(),
        )

    def _process_get_url(self, url: str) -> None:
        parsed = parse_qs(urlparse(url).query)
        if "token" not in parsed or not parsed["token"][0]:
            return
        self._token = parsed["token"][0]

    def _process_get(self, data: list[str]) -> bool:
        for line in data:
            if line.startswith("GET "):
                target_line = line
                break
        else:
            return False

        self._get_user_agent(data)
        _, url, _ = target_line.split()
        self._process_get_url(url)
        return True

    def _process_post(self, conn: SnowflakeConnection, data: list[str]) -> bool:
        for line in data:
            if line.startswith("POST "):
                break
        else:
            self._handle_failure(
                conn=conn,
                ret={
                    "code": ER_IDP_CONNECTION_ERROR,
                    "message": "Invalid HTTP request from web browser. Idp "
                    "authentication could have failed.",
                },
            )
            return False

        self._get_user_agent(data)
        try:
            # parse the response as JSON
            payload = json.loads(data[-1])
            self._token = payload.get("token")
            self.consent_cache_id_token = payload.get("consent", True)
        except Exception:
            # key=value form.
            self._token = parse_qs(data[-1])["token"][0]
        return True

    def _get_user_agent(self, data: list[str]) -> None:
        for line in data:
            if line.lower().startswith("user-agent"):
                logger.debug(line)
                break
        else:
            logger.debug("No User-Agent")

    def _get_sso_url(
        self,
        conn: SnowflakeConnection,
        authenticator: str,
        service_name: str | None,
        account: str,
        callback_port: int,
        user: str,
    ) -> str:
        """Gets SSO URL from Snowflake."""
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
            conn._rest._connection.application,
            conn._rest._connection._internal_application_name,
            conn._rest._connection._internal_application_version,
            conn._rest._connection._ocsp_mode(),
            conn._rest._connection._login_timeout,
            conn._rest._connection._network_timeout,
        )

        body["data"]["AUTHENTICATOR"] = authenticator
        body["data"]["BROWSER_MODE_REDIRECT_PORT"] = str(callback_port)
        logger.debug(
            "account=%s, authenticator=%s, user=%s", account, authenticator, user
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
        sso_url = data["ssoUrl"]
        self._proof_key = data["proofKey"]
        return sso_url
