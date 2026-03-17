#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import codecs
import copy
import json
import logging
import tempfile
import time
import uuid
from datetime import datetime
from os import getenv, makedirs, mkdir, path, remove, removedirs, rmdir
from os.path import expanduser
from threading import Lock, Thread
from typing import TYPE_CHECKING, Any, Callable

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_der_private_key,
    load_pem_private_key,
)

from ..compat import IS_LINUX, IS_MACOS, IS_WINDOWS, urlencode
from ..constants import (
    DAY_IN_SECONDS,
    HTTP_HEADER_ACCEPT,
    HTTP_HEADER_CONTENT_TYPE,
    HTTP_HEADER_SERVICE_NAME,
    HTTP_HEADER_USER_AGENT,
    PARAMETER_CLIENT_REQUEST_MFA_TOKEN,
    PARAMETER_CLIENT_STORE_TEMPORARY_CREDENTIAL,
)
from ..description import (
    COMPILER,
    IMPLEMENTATION,
    OPERATING_SYSTEM,
    PLATFORM,
    PYTHON_VERSION,
)
from ..errorcode import ER_FAILED_TO_CONNECT_TO_DB
from ..errors import (
    BadGatewayError,
    DatabaseError,
    Error,
    ForbiddenError,
    ProgrammingError,
    ServiceUnavailableError,
)
from ..network import (
    ACCEPT_TYPE_APPLICATION_SNOWFLAKE,
    CONTENT_TYPE_APPLICATION_JSON,
    ID_TOKEN_INVALID_LOGIN_REQUEST_GS_CODE,
    PYTHON_CONNECTOR_USER_AGENT,
    ReauthenticationRequest,
)
from ..options import installed_keyring, keyring
from ..sqlstate import SQLSTATE_CONNECTION_WAS_NOT_ESTABLISHED
from ..version import VERSION

if TYPE_CHECKING:
    from . import AuthByPlugin

logger = logging.getLogger(__name__)


# Cache directory
CACHE_ROOT_DIR = (
    getenv("SF_TEMPORARY_CREDENTIAL_CACHE_DIR")
    or expanduser("~")
    or tempfile.gettempdir()
)
if IS_WINDOWS:
    CACHE_DIR = path.join(CACHE_ROOT_DIR, "AppData", "Local", "Snowflake", "Caches")
elif IS_MACOS:
    CACHE_DIR = path.join(CACHE_ROOT_DIR, "Library", "Caches", "Snowflake")
else:
    CACHE_DIR = path.join(CACHE_ROOT_DIR, ".cache", "snowflake")

if not path.exists(CACHE_DIR):
    try:
        makedirs(CACHE_DIR, mode=0o700)
    except Exception as ex:
        logger.debug("cannot create a cache directory: [%s], err=[%s]", CACHE_DIR, ex)
        CACHE_DIR = None
logger.debug("cache directory: %s", CACHE_DIR)

# temporary credential cache
TEMPORARY_CREDENTIAL = {}

TEMPORARY_CREDENTIAL_LOCK = Lock()

# temporary credential cache file name
TEMPORARY_CREDENTIAL_FILE = "temporary_credential.json"
TEMPORARY_CREDENTIAL_FILE = (
    path.join(CACHE_DIR, TEMPORARY_CREDENTIAL_FILE) if CACHE_DIR else ""
)

# temporary credential cache lock directory name
TEMPORARY_CREDENTIAL_FILE_LOCK = TEMPORARY_CREDENTIAL_FILE + ".lck"

# keyring
KEYRING_SERVICE_NAME = "net.snowflake.temporary_token"
KEYRING_USER = "temp_token"
KEYRING_DRIVER_NAME = "SNOWFLAKE-PYTHON-DRIVER"

ID_TOKEN = "ID_TOKEN"
MFA_TOKEN = "MFATOKEN"


class Auth:
    """Snowflake Authenticator."""

    def __init__(self, rest):
        self._rest = rest

    @staticmethod
    def base_auth_data(
        user,
        account,
        application,
        internal_application_name,
        internal_application_version,
        ocsp_mode,
        login_timeout,
        network_timeout=None,
    ):
        return {
            "data": {
                "CLIENT_APP_ID": internal_application_name,
                "CLIENT_APP_VERSION": internal_application_version,
                "SVN_REVISION": VERSION[3],
                "ACCOUNT_NAME": account,
                "LOGIN_NAME": user,
                "CLIENT_ENVIRONMENT": {
                    "APPLICATION": application,
                    "OS": OPERATING_SYSTEM,
                    "OS_VERSION": PLATFORM,
                    "PYTHON_VERSION": PYTHON_VERSION,
                    "PYTHON_RUNTIME": IMPLEMENTATION,
                    "PYTHON_COMPILER": COMPILER,
                    "OCSP_MODE": ocsp_mode.name,
                    "TRACING": logger.getEffectiveLevel(),
                    "LOGIN_TIMEOUT": login_timeout,
                    "NETWORK_TIMEOUT": network_timeout,
                },
            },
        }

    def authenticate(
        self,
        auth_instance: AuthByPlugin,
        account: str,
        user: str,
        database: str | None = None,
        schema: str | None = None,
        warehouse: str | None = None,
        role: str | None = None,
        passcode: str | None = None,
        passcode_in_password: bool = False,
        mfa_callback: Callable[[], None] | None = None,
        password_callback: Callable[[], str] | None = None,
        session_parameters: dict[Any, Any] | None = None,
        timeout: int = 120,
    ) -> dict[str, str | int | bool]:
        logger.debug("authenticate")

        if session_parameters is None:
            session_parameters = {}

        request_id = str(uuid.uuid4())
        headers = {
            HTTP_HEADER_CONTENT_TYPE: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_ACCEPT: ACCEPT_TYPE_APPLICATION_SNOWFLAKE,
            HTTP_HEADER_USER_AGENT: PYTHON_CONNECTOR_USER_AGENT,
        }
        if HTTP_HEADER_SERVICE_NAME in session_parameters:
            headers[HTTP_HEADER_SERVICE_NAME] = session_parameters[
                HTTP_HEADER_SERVICE_NAME
            ]
        url = "/session/v1/login-request"

        body_template = Auth.base_auth_data(
            user,
            account,
            self._rest._connection.application,
            self._rest._connection._internal_application_name,
            self._rest._connection._internal_application_version,
            self._rest._connection._ocsp_mode(),
            self._rest._connection._login_timeout,
            self._rest._connection._network_timeout,
        )

        body = copy.deepcopy(body_template)
        # updating request body
        logger.debug("assertion content: %s", auth_instance.assertion_content)
        auth_instance.update_body(body)

        logger.debug(
            "account=%s, user=%s, database=%s, schema=%s, "
            "warehouse=%s, role=%s, request_id=%s",
            account,
            user,
            database,
            schema,
            warehouse,
            role,
            request_id,
        )
        url_parameters = {"request_id": request_id}
        if database is not None:
            url_parameters["databaseName"] = database
        if schema is not None:
            url_parameters["schemaName"] = schema
        if warehouse is not None:
            url_parameters["warehouse"] = warehouse
        if role is not None:
            url_parameters["roleName"] = role

        url = url + "?" + urlencode(url_parameters)

        # first auth request
        if passcode_in_password:
            body["data"]["EXT_AUTHN_DUO_METHOD"] = "passcode"
        elif passcode:
            body["data"]["EXT_AUTHN_DUO_METHOD"] = "passcode"
            body["data"]["PASSCODE"] = passcode

        if session_parameters:
            body["data"]["SESSION_PARAMETERS"] = session_parameters

        logger.debug(
            "body['data']: %s",
            {k: v for (k, v) in body["data"].items() if k != "PASSWORD"},
        )

        # accommodate any authenticator specific timeout requirements here.
        # login_timeout comes from user configuration.
        # Between login timeout and auth specific
        # timeout use whichever value is smaller
        auth_timeout = min(self._rest._connection.login_timeout, auth_instance.timeout)
        logger.debug(f"Timeout set to {auth_timeout}")

        try:
            ret = self._rest._post_request(
                url,
                headers,
                json.dumps(body),
                timeout=auth_timeout,
                socket_timeout=auth_timeout,
            )
        except ForbiddenError as err:
            # HTTP 403
            raise err.__class__(
                msg=(
                    "Failed to connect to DB. "
                    "Verify the account name is correct: {host}:{port}. "
                    "{message}"
                ).format(
                    host=self._rest._host, port=self._rest._port, message=str(err)
                ),
                errno=ER_FAILED_TO_CONNECT_TO_DB,
                sqlstate=SQLSTATE_CONNECTION_WAS_NOT_ESTABLISHED,
            )
        except (ServiceUnavailableError, BadGatewayError) as err:
            # HTTP 502/504
            raise err.__class__(
                msg=(
                    "Failed to connect to DB. "
                    "Service is unavailable: {host}:{port}. "
                    "{message}"
                ).format(
                    host=self._rest._host, port=self._rest._port, message=str(err)
                ),
                errno=ER_FAILED_TO_CONNECT_TO_DB,
                sqlstate=SQLSTATE_CONNECTION_WAS_NOT_ESTABLISHED,
            )

        # waiting for MFA authentication
        if ret["data"].get("nextAction") in (
            "EXT_AUTHN_DUO_ALL",
            "EXT_AUTHN_DUO_PUSH_N_PASSCODE",
        ):
            body["inFlightCtx"] = ret["data"]["inFlightCtx"]
            body["data"]["EXT_AUTHN_DUO_METHOD"] = "push"
            self.ret = {"message": "Timeout", "data": {}}

            def post_request_wrapper(self, url, headers, body):
                # get the MFA response
                self.ret = self._rest._post_request(
                    url, headers, body, timeout=self._rest._connection.login_timeout
                )

            # send new request to wait until MFA is approved
            t = Thread(
                target=post_request_wrapper, args=[self, url, headers, json.dumps(body)]
            )
            t.daemon = True
            t.start()
            if callable(mfa_callback):
                c = mfa_callback()
                while not self.ret or self.ret.get("message") == "Timeout":
                    next(c)
            else:
                t.join(timeout=timeout)

            ret = self.ret
            if ret and ret["data"].get("nextAction") == "EXT_AUTHN_SUCCESS":
                body = copy.deepcopy(body_template)
                body["inFlightCtx"] = ret["data"]["inFlightCtx"]
                # final request to get tokens
                ret = self._rest._post_request(
                    url,
                    headers,
                    json.dumps(body),
                    timeout=self._rest._connection.login_timeout,
                    socket_timeout=self._rest._connection.login_timeout,
                )
            elif not ret or not ret["data"].get("token"):
                # not token is returned.
                Error.errorhandler_wrapper(
                    self._rest._connection,
                    None,
                    DatabaseError,
                    {
                        "msg": (
                            "Failed to connect to DB. MFA "
                            "authentication failed: {"
                            "host}:{port}. {message}"
                        ).format(
                            host=self._rest._host,
                            port=self._rest._port,
                            message=ret["message"],
                        ),
                        "errno": ER_FAILED_TO_CONNECT_TO_DB,
                        "sqlstate": SQLSTATE_CONNECTION_WAS_NOT_ESTABLISHED,
                    },
                )
                return session_parameters  # required for unit test

        elif ret["data"].get("nextAction") == "PWD_CHANGE":
            if callable(password_callback):
                body = copy.deepcopy(body_template)
                body["inFlightCtx"] = ret["data"]["inFlightCtx"]
                body["data"]["LOGIN_NAME"] = user
                body["data"]["PASSWORD"] = (
                    auth_instance.password
                    if hasattr(auth_instance, "password")
                    else None
                )
                body["data"]["CHOSEN_NEW_PASSWORD"] = password_callback()
                # New Password input
                ret = self._rest._post_request(
                    url,
                    headers,
                    json.dumps(body),
                    timeout=self._rest._connection.login_timeout,
                    socket_timeout=self._rest._connection.login_timeout,
                )

        logger.debug("completed authentication")
        if not ret["success"]:
            errno = ret.get("code", ER_FAILED_TO_CONNECT_TO_DB)
            if errno == ID_TOKEN_INVALID_LOGIN_REQUEST_GS_CODE:
                # clear stored id_token if failed to connect because of id_token
                # raise an exception for reauth without id_token
                self._rest.id_token = None
                delete_temporary_credential(self._rest._host, user, ID_TOKEN)
                raise ReauthenticationRequest(
                    ProgrammingError(
                        msg=ret["message"],
                        errno=int(errno),
                        sqlstate=SQLSTATE_CONNECTION_WAS_NOT_ESTABLISHED,
                    )
                )

            from . import AuthByKeyPair

            if isinstance(auth_instance, AuthByKeyPair):
                logger.debug(
                    "JWT Token authentication failed. "
                    "Token expires at: %s. "
                    "Current Time: %s",
                    str(auth_instance._jwt_token_exp),
                    str(datetime.utcnow()),
                )
            from . import AuthByUsrPwdMfa

            if isinstance(auth_instance, AuthByUsrPwdMfa):
                delete_temporary_credential(self._rest._host, user, MFA_TOKEN)
            Error.errorhandler_wrapper(
                self._rest._connection,
                None,
                DatabaseError,
                {
                    "msg": (
                        "Failed to connect to DB: {host}:{port}. " "{message}"
                    ).format(
                        host=self._rest._host,
                        port=self._rest._port,
                        message=ret["message"],
                    ),
                    "errno": ER_FAILED_TO_CONNECT_TO_DB,
                    "sqlstate": SQLSTATE_CONNECTION_WAS_NOT_ESTABLISHED,
                },
            )
        else:
            logger.debug(
                "token = %s", "******" if ret["data"]["token"] is not None else "NULL"
            )
            logger.debug(
                "master_token = %s",
                "******" if ret["data"]["masterToken"] is not None else "NULL",
            )
            logger.debug(
                "id_token = %s",
                "******" if ret["data"].get("idToken") is not None else "NULL",
            )
            logger.debug(
                "mfa_token = %s",
                "******" if ret["data"].get("mfaToken") is not None else "NULL",
            )
            self._rest.update_tokens(
                ret["data"]["token"],
                ret["data"]["masterToken"],
                master_validity_in_seconds=ret["data"].get("masterValidityInSeconds"),
                id_token=ret["data"].get("idToken"),
                mfa_token=ret["data"].get("mfaToken"),
            )
            self.write_temporary_credentials(
                self._rest._host, user, session_parameters, ret
            )
            if "sessionId" in ret["data"]:
                self._rest._connection._session_id = ret["data"]["sessionId"]
            if "sessionInfo" in ret["data"]:
                session_info = ret["data"]["sessionInfo"]
                self._rest._connection._database = session_info.get("databaseName")
                self._rest._connection._schema = session_info.get("schemaName")
                self._rest._connection._warehouse = session_info.get("warehouseName")
                self._rest._connection._role = session_info.get("roleName")
            if "parameters" in ret["data"]:
                session_parameters.update(
                    {p["name"]: p["value"] for p in ret["data"]["parameters"]}
                )
            self._rest._connection._update_parameters(session_parameters)
            return session_parameters

    def _read_temporary_credential(self, host, user, cred_type):
        cred = None
        if IS_MACOS or IS_WINDOWS:
            if not installed_keyring:
                logger.debug(
                    "Dependency 'keyring' is not installed, cannot cache id token. You might experience "
                    "multiple authentication pop ups while using ExternalBrowser Authenticator. To avoid "
                    "this please install keyring module using the following command : pip install "
                    "snowflake-connector-python[secure-local-storage]"
                )
                return
            try:
                cred = keyring.get_password(
                    build_temporary_credential_name(host, user, cred_type), user.upper()
                )
            except keyring.errors.KeyringError as ke:
                logger.error(
                    "Could not retrieve {} from secure storage : {}".format(
                        cred_type, str(ke)
                    )
                )
        elif IS_LINUX:
            read_temporary_credential_file()
            cred = TEMPORARY_CREDENTIAL.get(host.upper(), {}).get(
                build_temporary_credential_name(host, user, cred_type)
            )
        else:
            logger.debug("OS not supported for Local Secure Storage")
        return cred

    def read_temporary_credentials(
        self,
        host: str,
        user: str,
        session_parameters: dict[str, Any],
    ) -> None:
        if session_parameters.get(PARAMETER_CLIENT_STORE_TEMPORARY_CREDENTIAL, False):
            self._rest.id_token = self._read_temporary_credential(
                host,
                user,
                ID_TOKEN,
            )

        if session_parameters.get(PARAMETER_CLIENT_REQUEST_MFA_TOKEN, False):
            self._rest.mfa_token = self._read_temporary_credential(
                host,
                user,
                MFA_TOKEN,
            )

    def _write_temporary_credential(
        self,
        host: str,
        user: str,
        cred_type: str,
        cred: str | None,
    ) -> None:
        if not cred:
            logger.debug(
                "no credential is given when try to store temporary credential"
            )
            return
        if IS_MACOS or IS_WINDOWS:
            if not installed_keyring:
                logger.debug(
                    "Dependency 'keyring' is not installed, cannot cache id token. You might experience "
                    "multiple authentication pop ups while using ExternalBrowser Authenticator. To avoid "
                    "this please install keyring module using the following command : pip install "
                    "snowflake-connector-python[secure-local-storage]"
                )
                return
            try:
                keyring.set_password(
                    build_temporary_credential_name(host, user, cred_type),
                    user.upper(),
                    cred,
                )
            except keyring.errors.KeyringError as ke:
                logger.error("Could not store id_token to keyring, %s", str(ke))
        elif IS_LINUX:
            write_temporary_credential_file(
                host, build_temporary_credential_name(host, user, cred_type), cred
            )
        else:
            logger.debug("OS not supported for Local Secure Storage")

    def write_temporary_credentials(
        self,
        host: str,
        user: str,
        session_parameters: dict[str, Any],
        response: dict[str, Any],
    ) -> None:
        if (
            self._rest._connection.auth_class.consent_cache_id_token
            and session_parameters.get(
                PARAMETER_CLIENT_STORE_TEMPORARY_CREDENTIAL, False
            )
        ):
            self._write_temporary_credential(
                host, user, ID_TOKEN, response["data"].get("idToken")
            )

        if session_parameters.get(PARAMETER_CLIENT_REQUEST_MFA_TOKEN, False):
            self._write_temporary_credential(
                host, user, MFA_TOKEN, response["data"].get("mfaToken")
            )


def flush_temporary_credentials() -> None:
    """Flush temporary credentials in memory into disk. Need to hold TEMPORARY_CREDENTIAL_LOCK."""
    global TEMPORARY_CREDENTIAL
    global TEMPORARY_CREDENTIAL_FILE
    for _ in range(10):
        if lock_temporary_credential_file():
            break
        time.sleep(1)
    else:
        logger.debug(
            "The lock file still persists after the maximum wait time."
            "Will ignore it and write temporary credential file: %s",
            TEMPORARY_CREDENTIAL_FILE,
        )
    try:
        with open(
            TEMPORARY_CREDENTIAL_FILE, "w", encoding="utf-8", errors="ignore"
        ) as f:
            json.dump(TEMPORARY_CREDENTIAL, f)
    except Exception as ex:
        logger.debug(
            "Failed to write a credential file: " "file=[%s], err=[%s]",
            TEMPORARY_CREDENTIAL_FILE,
            ex,
        )
    finally:
        unlock_temporary_credential_file()


def write_temporary_credential_file(host, cred_name, cred) -> None:
    """Writes temporary credential file when OS is Linux."""
    if not CACHE_DIR:
        # no cache is enabled
        return
    global TEMPORARY_CREDENTIAL
    global TEMPORARY_CREDENTIAL_LOCK
    with TEMPORARY_CREDENTIAL_LOCK:
        # update the cache
        host_data = TEMPORARY_CREDENTIAL.get(host.upper(), {})
        host_data[cred_name.upper()] = cred
        TEMPORARY_CREDENTIAL[host.upper()] = host_data
        flush_temporary_credentials()


def read_temporary_credential_file() -> None:
    """Reads temporary credential file when OS is Linux."""
    if not CACHE_DIR:
        # no cache is enabled
        return

    global TEMPORARY_CREDENTIAL
    global TEMPORARY_CREDENTIAL_LOCK
    global TEMPORARY_CREDENTIAL_FILE
    with TEMPORARY_CREDENTIAL_LOCK:
        for _ in range(10):
            if lock_temporary_credential_file():
                break
            time.sleep(1)
        else:
            logger.debug(
                "The lock file still persists. Will ignore and "
                "write the temporary credential file: %s",
                TEMPORARY_CREDENTIAL_FILE,
            )
        try:
            with codecs.open(
                TEMPORARY_CREDENTIAL_FILE, "r", encoding="utf-8", errors="ignore"
            ) as f:
                TEMPORARY_CREDENTIAL = json.load(f)
            return TEMPORARY_CREDENTIAL
        except Exception as ex:
            logger.debug(
                "Failed to read a credential file. The file may not"
                "exists: file=[%s], err=[%s]",
                TEMPORARY_CREDENTIAL_FILE,
                ex,
            )
        finally:
            unlock_temporary_credential_file()


def lock_temporary_credential_file() -> bool:
    global TEMPORARY_CREDENTIAL_FILE_LOCK
    try:
        mkdir(TEMPORARY_CREDENTIAL_FILE_LOCK)
        return True
    except OSError:
        logger.debug(
            "Temporary cache file lock already exists. Other "
            "process may be updating the temporary "
        )
        return False


def unlock_temporary_credential_file() -> bool:
    global TEMPORARY_CREDENTIAL_FILE_LOCK
    try:
        rmdir(TEMPORARY_CREDENTIAL_FILE_LOCK)
        return True
    except OSError:
        logger.debug("Temporary cache file lock no longer exists.")
        return False


def delete_temporary_credential(host, user, cred_type):
    if (IS_MACOS or IS_WINDOWS) and installed_keyring:
        try:
            keyring.delete_password(
                build_temporary_credential_name(host, user, cred_type), user.upper()
            )
        except Exception as ex:
            logger.error("Failed to delete credential in the keyring: err=[%s]", ex)
    elif IS_LINUX:
        temporary_credential_file_delete_password(host, user, cred_type)


def temporary_credential_file_delete_password(host, user, cred_type):
    """Remove credential from temporary credential file when OS is Linux."""
    if not CACHE_DIR:
        # no cache is enabled
        return
    global TEMPORARY_CREDENTIAL
    global TEMPORARY_CREDENTIAL_LOCK
    with TEMPORARY_CREDENTIAL_LOCK:
        # update the cache
        host_data = TEMPORARY_CREDENTIAL.get(host.upper(), {})
        host_data.pop(build_temporary_credential_name(host, user, cred_type), None)
        if not host_data:
            TEMPORARY_CREDENTIAL.pop(host.upper(), None)
        else:
            TEMPORARY_CREDENTIAL[host.upper()] = host_data
        flush_temporary_credentials()


def delete_temporary_credential_file():
    """Deletes temporary credential file and its lock file."""
    global TEMPORARY_CREDENTIAL_FILE
    try:
        remove(TEMPORARY_CREDENTIAL_FILE)
    except Exception as ex:
        logger.debug(
            "Failed to delete a credential file: " "file=[%s], err=[%s]",
            TEMPORARY_CREDENTIAL_FILE,
            ex,
        )
    try:
        removedirs(TEMPORARY_CREDENTIAL_FILE_LOCK)
    except Exception as ex:
        logger.debug("Failed to delete credential lock file: err=[%s]", ex)


def build_temporary_credential_name(host, user, cred_type):
    return "{host}:{user}:{driver}:{cred}".format(
        host=host.upper(), user=user.upper(), driver=KEYRING_DRIVER_NAME, cred=cred_type
    )


def get_token_from_private_key(
    user: str, account: str, privatekey_path: str, key_password: str | None
) -> str:
    encoded_password = key_password.encode() if key_password is not None else None
    with open(privatekey_path, "rb") as key:
        p_key = load_pem_private_key(
            key.read(), password=encoded_password, backend=default_backend()
        )

    private_key = p_key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    from . import AuthByKeyPair

    auth_instance = AuthByKeyPair(
        private_key,
        DAY_IN_SECONDS,
    )  # token valid for 24 hours
    return auth_instance.prepare(account=account, user=user)


def get_public_key_fingerprint(private_key_file: str, password: str) -> str:
    """Helper function to generate the public key fingerprint from the private key file"""
    with open(private_key_file, "rb") as key:
        p_key = load_pem_private_key(
            key.read(), password=password.encode(), backend=default_backend()
        )
    private_key = p_key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    private_key = load_der_private_key(
        data=private_key, password=None, backend=default_backend()
    )
    from . import AuthByKeyPair

    return AuthByKeyPair.calculate_public_key_fingerprint(private_key)
