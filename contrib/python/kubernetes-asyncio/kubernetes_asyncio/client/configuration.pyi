from logging import FileHandler, Logger
from typing import Any, Callable, Coroutine
from kubernetes_asyncio.client.exceptions import ApiValueError as ApiValueError

JSON_SCHEMA_VALIDATION_KEYWORDS: set[str]

class Configuration:
    server_index: int
    server_operation_index: dict
    server_variables: dict
    server_operation_variables: dict
    temp_folder_path: str | None
    api_key: dict
    api_key_prefix: dict
    refresh_api_key_hook: Coroutine | Callable | None
    username: str | None
    password: str | None
    discard_unknown_keys: bool
    disabled_client_side_validations: str
    logger: dict[str, Logger]
    logger_stream_handler: Logger | None
    logger_file_handler: Logger | None
    verify_ssl: bool
    disable_strict_ssl_verification: bool
    ssl_ca_cert: str | None
    cert_file: str | None
    key_file: str | None
    assert_hostname: bool | None
    tls_server_name: str | None
    connection_pool_maxsize: int
    proxy: str | None
    proxy_headers: dict | None
    safe_chars_for_path_param: str
    retries: int | None
    client_side_validation: bool
    socket_options: dict | None
    def __init__(
        self,
        host: str | None = None,
        api_key: dict | None = None,
        api_key_prefix: dict | None = None,
        username: str | None = None,
        password: str | None = None,
        discard_unknown_keys: bool = False,
        disabled_client_side_validations: str = "",
        server_index: int | None = None,
        server_variables: dict | None = None,
        server_operation_index: dict | None = None,
        server_operation_variables: dict | None = None,
        ssl_ca_cert: str | None = None,
    ) -> None: ...
    def __deepcopy__(self, memo: Configuration): ...
    def __setattr__(self, name: str, value: Any) -> None: ...
    @classmethod
    def set_default(cls, default: Configuration | None) -> None: ...
    @classmethod
    def get_default(cls) -> Configuration: ...
    @classmethod
    def get_default_copy(cls) -> Configuration: ...
    @property
    def logger_file(self): ...
    @logger_file.setter
    def logger_file(self, value: str) -> FileHandler | None: ...
    @property
    def debug(self) -> bool: ...
    @debug.setter
    def debug(self, value: bool) -> None: ...
    @property
    def logger_format(self) -> str | None: ...
    @logger_format.setter
    def logger_format(self, value: str) -> None: ...
    async def get_api_key_with_prefix(
        self, identifier: str, alias: str | None = None
    ) -> str: ...
    def get_basic_auth_token(self) -> str: ...
    async def auth_settings(self) -> dict: ...
    def to_debug_report(self) -> str: ...
    def get_host_settings(self) -> list[dict[str, str]]: ...
    def get_host_from_settings(
        self,
        index: int,
        variables: dict | None = None,
        servers: dict | None = None,
    ) -> str | None: ...
    @property
    def host(self) -> str | None: ...
    @host.setter
    def host(self, value: str | None) -> None: ...
