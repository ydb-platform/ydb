import logging
import uuid
from base64 import b64encode
from collections.abc import Callable
from typing import Any, cast

from urllib3 import Timeout
from urllib3.poolmanager import PoolManager
from urllib3.response import HTTPResponse

from clickhouse_connect import common
from clickhouse_connect.driver._backend.http_sync import HttpSyncBackend
from clickhouse_connect.driver._backend.httpcommon import (
    add_integration_tag,
    apply_http_server_settings,
    auth_failed_ex_code,  # noqa: F401  (compatibility re-export)
    columns_only_re,  # noqa: F401  (compatibility re-export)
    ex_header,  # noqa: F401  (compatibility re-export)
    ex_tag_header,  # noqa: F401  (compatibility re-export)
    negotiate_compression,
)
from clickhouse_connect.driver._backendclient import SyncBackendClient
from clickhouse_connect.driver.binding import (
    use_form_encoding,  # noqa: F401  (compatibility re-export)
)
from clickhouse_connect.driver.common import coerce_bool, coerce_int, dict_add, dict_copy
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.httputil import (
    ResponseSource,  # noqa: F401  (compatibility re-export)
    check_env_proxy,
    default_pool_manager,
    get_pool_manager,
    get_proxy_manager,
)
from clickhouse_connect.driver.query import TzMode, TzSource
from clickhouse_connect.driver.transform import NativeTransform

logger = logging.getLogger(__name__)


class HttpClient(SyncBackendClient):
    _backend: HttpSyncBackend
    params: dict[str, str] = {}
    valid_transport_settings = {
        "database",
        "buffer_size",
        "session_id",
        "compress",
        "decompress",
        "session_timeout",
        "session_check",
        "query_id",
        "quota_key",
        "wait_end_of_query",
        "client_protocol_version",
        "role",
    }
    optional_transport_settings = {"send_progress_in_http_headers", "http_headers_progress_interval_ms", "enable_http_compression"}
    _owns_pool_manager = False

    # R0917: too-many-positional-arguments

    def __init__(
        self,
        interface: str,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str | None,
        access_token: str | None = None,
        token_provider: Callable[[], str] | None = None,
        compress: bool | str = True,
        query_limit: int = 0,
        query_retries: int = 2,
        connect_timeout: int = 10,
        send_receive_timeout: int = 300,
        client_name: str | None = None,
        verify: bool | str = True,
        ca_cert: str | None = None,
        client_cert: str | None = None,
        client_cert_key: str | None = None,
        session_id: str | None = None,
        settings: dict[str, Any] | None = None,
        pool_mgr: PoolManager | None = None,
        http_proxy: str | None = None,
        https_proxy: str | None = None,
        server_host_name: str | None = None,
        tz_source: TzSource | None = None,
        tz_mode: str | None = None,
        show_clickhouse_errors: bool | None = None,
        autogenerate_session_id: bool | None = None,
        autogenerate_query_id: bool | None = None,
        tls_mode: str | None = None,
        proxy_path: str = "",
        form_encode_query_params: bool = False,
        rename_response_column: str | None = None,
        headers: dict[str, str] | None = None,
    ):
        """
        Create an HTTP ClickHouse Connect client
        See clickhouse_connect.get_client for parameters
        """
        proxy_path = proxy_path.lstrip("/")
        if proxy_path:
            proxy_path = "/" + proxy_path
        self.url = f"{interface}://{host}:{port}{proxy_path}"
        client_headers: dict[str, str] = {}
        self.params = dict_copy(HttpClient.params)
        ch_settings = dict_copy(settings, self.params)
        pool = pool_mgr
        if interface == "https":
            if isinstance(verify, str) and verify.lower() == "proxy":
                verify = True
                tls_mode = tls_mode or "proxy"
            if not https_proxy:
                https_proxy = check_env_proxy("https", host, port)
            verify = coerce_bool(verify)
            if client_cert and (tls_mode is None or tls_mode == "mutual"):
                if not username:
                    raise ProgrammingError("username parameter is required for Mutual TLS authentication")
                client_headers["X-ClickHouse-User"] = username
                client_headers["X-ClickHouse-SSL-Certificate-Auth"] = "on"

            if not pool and (server_host_name or ca_cert or client_cert or not verify or https_proxy):
                options: dict[str, Any] = {"verify": verify}
                dict_add(options, "ca_cert", ca_cert)
                dict_add(options, "client_cert", client_cert)
                dict_add(options, "client_cert_key", client_cert_key)
                if server_host_name:
                    if options["verify"]:
                        options["assert_hostname"] = server_host_name
                    options["server_hostname"] = server_host_name
                pool = get_pool_manager(https_proxy=https_proxy, **options)
                self._owns_pool_manager = True
        if not pool:
            if not http_proxy:
                http_proxy = check_env_proxy("http", host, port)
            if http_proxy:
                pool = get_proxy_manager(host, http_proxy)
            else:
                pool = default_pool_manager()

        if token_provider:
            access_token = token_provider()
        if access_token:
            client_headers["Authorization"] = f"Bearer {access_token}"
        elif (not client_cert or tls_mode in ("strict", "proxy")) and username:
            client_headers["Authorization"] = "Basic " + b64encode(f"{username}:{password}".encode()).decode()

        self._reported_libs: set[str] = set()
        client_headers["User-Agent"] = common.build_client_name(client_name)
        if headers:
            client_headers.update(headers)
        self._write_format = "Native"
        self._transform = NativeTransform()

        # There are use cases when the client needs to disable timeouts.
        if connect_timeout is not None:
            connect_timeout = coerce_int(connect_timeout)
        if send_receive_timeout is not None:
            send_receive_timeout = coerce_int(send_receive_timeout)
        self._rename_response_column = rename_response_column

        # allow to override the global autogenerate_session_id setting via the constructor params
        _autogenerate_session_id = (
            common.get_setting("autogenerate_session_id") if autogenerate_session_id is None else autogenerate_session_id
        )

        if session_id:
            ch_settings["session_id"] = session_id
        elif "session_id" not in ch_settings and _autogenerate_session_id:
            ch_settings["session_id"] = str(uuid.uuid4())

        compression, write_compression = negotiate_compression(compress)
        if write_compression:
            self.write_compression = write_compression

        # The backend owns transport state. The params dict is shared by
        # reference with this facade, so it is mutated in place, never rebound.
        self._backend = HttpSyncBackend(
            url=self.url,
            pool_manager=pool,
            owns_pool_manager=self._owns_pool_manager,
            headers=client_headers,
            params=self.params,
            timeout=Timeout(connect=connect_timeout, read=send_receive_timeout),
            server_host_name=server_host_name,
            token_provider=token_provider,
            # allow to override the global autogenerate_query_id setting via the constructor params
            autogenerate_query_id=(common.get_setting("autogenerate_query_id") if autogenerate_query_id is None else autogenerate_query_id),
            read_format="Native",
            form_encode_query_params=form_encode_query_params,
        )
        self._initial_settings = settings
        # Stashed for _init_common_settings, which needs the discovered server
        # settings and so runs as part of the connect step inside super().__init__
        self._ch_settings = ch_settings
        self._negotiated_compression = compression
        self._send_receive_timeout = send_receive_timeout
        super().__init__(
            database=database,
            uri=self.url,
            query_limit=query_limit,
            query_retries=query_retries,
            server_host_name=server_host_name,
            tz_source=tz_source,
            tz_mode=cast(TzMode | None, tz_mode),
            show_clickhouse_errors=show_clickhouse_errors,
            autoconnect=True,
        )

    def _init_common_settings(self, tz_source: TzSource) -> None:
        super()._init_common_settings(tz_source)
        self.params.update(self._validate_settings(self._ch_settings))
        apply_http_server_settings(self, self._backend, self._negotiated_compression, self._send_receive_timeout)

    @property
    def http(self) -> PoolManager:
        return cast(PoolManager, self._backend.http)

    @http.setter
    def http(self, pool_manager: PoolManager) -> None:
        self._backend.http = pool_manager

    @property
    def headers(self) -> dict[str, str]:
        return self._backend.headers

    @headers.setter
    def headers(self, value: dict[str, str]) -> None:
        self._backend.headers = value

    @property
    def timeout(self) -> Timeout:
        return self._backend.timeout

    @timeout.setter
    def timeout(self, value: Timeout) -> None:
        self._backend.timeout = value

    @property
    def http_retries(self) -> int:
        return self._backend.http_retries

    @http_retries.setter
    def http_retries(self, value: int) -> None:
        self._backend.http_retries = value

    @property
    def show_clickhouse_errors(self) -> bool:  # type: ignore[override]
        return self._backend.show_clickhouse_errors

    @show_clickhouse_errors.setter
    def show_clickhouse_errors(self, value: bool) -> None:
        self._backend.show_clickhouse_errors = value

    @property
    def _autogenerate_query_id(self) -> bool:
        return self._backend.autogenerate_query_id

    @_autogenerate_query_id.setter
    def _autogenerate_query_id(self, value: bool) -> None:
        self._backend.autogenerate_query_id = value

    @property
    def _token_provider(self) -> Callable[[], str] | None:
        return self._backend.token_provider

    @property
    def form_encode_query_params(self) -> bool:
        return self._backend.form_encode_query_params

    @form_encode_query_params.setter
    def form_encode_query_params(self, value: bool) -> None:
        self._backend.form_encode_query_params = value

    @property
    def _read_format(self) -> str:
        return self._backend.read_format

    @_read_format.setter
    def _read_format(self, value: str) -> None:
        self._backend.read_format = value

    @property
    def compression(self) -> str | None:  # type: ignore[override]
        return self._backend.compression

    @compression.setter
    def compression(self, value: str | None) -> None:
        self._backend.compression = value

    def set_client_setting(self, key: str, value: Any) -> None:
        str_value = self._validate_setting(key, value, common.get_setting("invalid_setting_action"))
        if str_value is not None:
            self.params[key] = str_value

    def get_client_setting(self, key: str) -> str | None:
        return self.params.get(key)

    def set_access_token(self, access_token: str) -> None:
        self._backend.set_access_token(access_token)

    def _error_handler(self, response: HTTPResponse, retried: bool = False) -> None:
        self._backend.error_handler(response, retried)

    def _raw_request(
        self,
        data,
        params: dict[str, str],
        headers: dict[str, Any] | None = None,
        method: str = "POST",
        retries: int = 0,
        stream: bool = False,
        server_wait: bool = True,
        fields: dict[str, tuple] | None = None,
        error_handler: Callable | None = None,
        retry_body: Callable[[], Any] | None = None,
    ) -> HTTPResponse:
        return self._backend.request(
            data,
            params,
            headers=headers,
            method=method,
            retries=retries,
            stream=stream,
            server_wait=server_wait,
            fields=fields,
            error_handler=error_handler,
            retry_body=retry_body,
        )

    def _add_integration_tag(self, name: str):
        """
        Dynamically adds a product (like pandas or sqlalchemy) to the User-Agent string details section.
        """
        add_integration_tag(self.headers, self._reported_libs, name)
