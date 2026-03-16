# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A client and common configurations for the Google Ads API."""

from importlib import import_module, metadata
import logging.config

from google.api_core.gapic_v1.client_info import ClientInfo
import grpc
from proto.enums import ProtoEnumMeta
from google.auth.credentials import Credentials

from google.protobuf.message import Message as ProtobufMessageType
from proto import Message as ProtoPlusMessageType

from google.ads.googleads import config, oauth2, util
from google.ads.googleads.interceptors import (
    MetadataInterceptor,
    AsyncUnaryUnaryMetadataInterceptor,
    AsyncUnaryStreamMetadataInterceptor,
    ExceptionInterceptor,
    AsyncUnaryUnaryExceptionInterceptor,
    AsyncUnaryStreamExceptionInterceptor,
    LoggingInterceptor,
    AsyncUnaryUnaryLoggingInterceptor,
    AsyncUnaryStreamLoggingInterceptor,
)

from types import ModuleType
from typing import Any, Dict, List, Tuple, Union

_logger = logging.getLogger(__name__)

_SERVICE_CLIENT_TEMPLATE = "{}Client"
_ASYNC_SERVICE_CLIENT_TEMPLATE = "{}AsyncClient"

_VALID_API_VERSIONS = ["v23", "v22", "v21", "v20"]
_MESSAGE_TYPES = ["common", "enums", "errors", "resources", "services"]
_DEFAULT_VERSION = _VALID_API_VERSIONS[0]

# Retrieve the version of this client library to be sent in the user-agent
# information of API calls.
try:
    _CLIENT_INFO = ClientInfo(
        client_library_version=metadata.version("google-ads")
    )
except metadata.PackageNotFoundError:
    _CLIENT_INFO = ClientInfo()

# See options at grpc.github.io/grpc/core/group__grpc__arg__keys.html
_GRPC_CHANNEL_OPTIONS = [
    ("grpc.max_metadata_size", 16 * 1024 * 1024),
    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
]

unary_stream_single_threading_option = util.get_nested_attr(
    grpc, "experimental.ChannelOptions.SingleThreadedUnaryStream", None
)

if unary_stream_single_threading_option:
    _GRPC_CHANNEL_OPTIONS.append((unary_stream_single_threading_option, 1))


class _EnumGetter:
    """An intermediate getter for retrieving enums from service clients.

    Acts as the "enum" property of a service client and dynamically loads enum
    class instances when accessed.
    """

    def __init__(self, client: "GoogleAdsClient") -> None:
        """Initializer for the _EnumGetter class.

        Args:
            client: An instance of the GoogleAdsClient class.
        """
        self._client: "GoogleAdsClient" = client
        self._version: str = client.version or _DEFAULT_VERSION
        self._enums: Union[Tuple[str], None] = None
        self._use_proto_plus: bool = client.use_proto_plus

    def __dir__(self) -> Tuple[str]:
        """Overrides behavior when dir() is called on instances of this class.

        It's useful to use dir() to see a list of available attributes. Since
        this class exposes all the enums in the API it borrows the __all__
        property from the corresponding enums module.
        """
        if not self._enums:
            self._enums = import_module(
                f"google.ads.googleads.{self._version}.enums"
            ).__all__

        return self._enums

    def __getattr__(self, name: str) -> Union[ProtoPlusMessageType, ProtobufMessageType]:
        """Dynamically loads the given enum class instance.

        Args:
            name: a str of the name of the enum to load, i.e. "AdTypeEnum."

        Returns:
            An instance of the enum proto message class.
        """
        if name not in self.__dir__():
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            )
        try:
            enum_class = self._client.get_type(name)

            if self._use_proto_plus:
                for attr in dir(enum_class):
                    attr_val = getattr(enum_class, attr)
                    if isinstance(attr_val, ProtoEnumMeta):
                        return attr_val
            else:
                return enum_class
        except ValueError:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            )

    def __getstate__(self) -> Dict[str, Any]:
        """Returns self serialized as a dict.

        Since this class overrides __getattr__ we define this method to help
        with pickling, which is important to avoid recursion depth errors when
        pickling this class or the GoogleAdsClient or using multiprocessing.

        Returns:
            a dict of this object's state
        """
        return self.__dict__.copy()

    def __setstate__(self, d: Dict[str, Any]) -> None:
        """Deserializes self with the given dictionary.

        Since this class overrides __getattr__ we define this method to help
        with pickling, which is important to avoid recursion depth errors when
        pickling this class or the GoogleAdsClient or using multiprocessing.

        Args:
            d: a dict of this object's state
        """
        self.__dict__.update(d)


class GoogleAdsClient:
    """Google Ads client used to configure settings and fetch services."""

    @classmethod
    def copy_from(
            cls,
            destination: Union[ProtoPlusMessageType, ProtobufMessageType],
            origin: Union[ProtoPlusMessageType, ProtobufMessageType]
        ) -> Union[ProtoPlusMessageType, ProtobufMessageType]:
        """Copies protobuf and proto-plus messages into one-another.

        This method consolidates the CopyFrom logic of protobuf and proto-plus
        messages into a single helper method. The destination message will be
        updated with the exact state of the origin message.

        Args:
            destination: The message where changes are being copied.
            origin: The message where changes are being copied from.
        """
        return util.proto_copy_from(destination, origin)

    @classmethod
    def _get_client_kwargs(cls, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Converts configuration dict into kwargs required by the client.

        Args:
            config_data: a dict containing client configuration.

        Returns:
            A dict containing kwargs that will be provided to the
            GoogleAdsClient initializer.

        Raises:
            ValueError: If the configuration lacks a required field.
        """
        return {
            "credentials": oauth2.get_credentials(config_data),
            "developer_token": config_data.get("developer_token"),
            "endpoint": config_data.get("endpoint"),
            "login_customer_id": config_data.get("login_customer_id"),
            "logging_config": config_data.get("logging"),
            "linked_customer_id": config_data.get("linked_customer_id"),
            "http_proxy": config_data.get("http_proxy"),
            "use_proto_plus": config_data.get("use_proto_plus"),
            "use_cloud_org_for_api_access": config_data.get(
                "use_cloud_org_for_api_access"
            ),
            "gaada": config_data.get("gaada"),
        }

    @classmethod
    def _get_api_services_by_version(cls, version: str) -> ModuleType:
        """Returns a module with all services and types for a given API version.

        Args:
            version: a str indicating the API version.

        Returns:
            A module containing all services and types for the a API version.
        """
        try:
            version_module = import_module(f"google.ads.googleads.{version}")
        except ImportError:
            raise ValueError(
                f"There was an error importing the "
                f'"google.ads.googleads.{version}" module. Please check that '
                f'"{version}" is a valid API version. Here are the current '
                "API versions supported by this library: "
                f"{', '.join(_VALID_API_VERSIONS)}."
            )
        return version_module

    @classmethod
    def load_from_env(
        cls, version: Union[str, None] = None
    ) -> "GoogleAdsClient":
        """Creates a GoogleAdsClient with data stored in the env variables.

        Args:
            version: a str indicating the Google Ads API version to be used.

        Returns:
            A GoogleAdsClient initialized with the values specified in the
            env variables.

        Raises:
            ValueError: If the configuration lacks a required field.
        """
        config_data: Dict[str, Any] = config.load_from_env()
        kwargs: Dict[str, Any] = cls._get_client_kwargs(config_data)
        return cls(**dict(version=version, **kwargs))

    @classmethod
    def load_from_string(
        cls, yaml_str: str, version: Union[str, None] = None
    ) -> "GoogleAdsClient":
        """Creates a GoogleAdsClient with data stored in the YAML string.

        Args:
            yaml_str: a str containing YAML configuration data used to
              initialize a GoogleAdsClient.
            version: a str indicating the Google Ads API version to be used.

        Returns:
            A GoogleAdsClient initialized with the values specified in the
            string.

        Raises:
            ValueError: If the configuration lacks a required field.
        """
        config_data: Dict[str, Any] = config.parse_yaml_document_to_dict(yaml_str)
        kwargs: Dict[str, Any] = cls._get_client_kwargs(config_data)
        return cls(**dict(version=version, **kwargs))

    @classmethod
    def load_from_dict(
        cls, config_dict: Dict[str, Any], version: Union[str, None] = None
    ) -> "GoogleAdsClient":
        """Creates a GoogleAdsClient with data stored in the config_dict.

        Args:
            config_dict: a dict consisting of configuration data used to
              initialize a GoogleAdsClient.
            version: a str indicating the Google Ads API version to be used.

        Returns:
            A GoogleAdsClient initialized with the values specified in the
                dict.

        Raises:
            ValueError: If the configuration lacks a required field.
        """
        config_data: Dict[str, Any] = config.load_from_dict(config_dict)
        kwargs: Dict[str, Any] = cls._get_client_kwargs(config_data)
        return cls(**dict(version=version, **kwargs))

    @classmethod
    def load_from_storage(
        cls, path: Union[str, None] = None, version: Union[str, None] = None
    ) -> "GoogleAdsClient":
        """Creates a GoogleAdsClient with data stored in the specified file.

        Args:
            path: a str indicating the path to a YAML file containing
              configuration data used to initialize a GoogleAdsClient.
            version: a str indicating the Google Ads API version to be used.

        Returns:
            A GoogleAdsClient initialized with the values in the specified file.

        Raises:
            FileNotFoundError: If the specified configuration file doesn't
                exist.
            IOError: If the configuration file can't be loaded.
            ValueError: If the configuration file lacks a required field.
        """
        config_data: Dict[str, Any] = config.load_from_yaml_file(path)
        kwargs: Dict[str, Any] = cls._get_client_kwargs(config_data)
        return cls(**dict(version=version, **kwargs))

    def __init__(
        self,
        credentials: Dict[str, Any],
        developer_token: str,
        endpoint: Union[str, None] = None,
        login_customer_id: Union[str, None] = None,
        logging_config: Union[Dict[str, Any], None] = None,
        linked_customer_id: Union[str, None] = None,
        version: Union[str, None] = None,
        http_proxy: Union[str, None] = None,
        use_proto_plus: bool = False,
        use_cloud_org_for_api_access: Union[str, None] = None,
        gaada: Union[str, None] = None,
    ):
        """Initializer for the GoogleAdsClient.

        Args:
            credentials: a google.oauth2.credentials.Credentials instance.
            developer_token: a str developer token.
            endpoint: a str specifying an optional alternative API endpoint.
            login_customer_id: a str specifying a login customer ID.
            logging_config: a dict specifying logging config options.
            linked_customer_id: a str specifying a linked customer ID.
            version: a str indicating the Google Ads API version to be used.
            http_proxy: a str specifying the proxy URI through which to connect.
            use_proto_plus: a bool specifying whether or not to use proto-plus
                for protobuf message interfaces.
            use_cloud_org_for_api_access: a str specifying whether to use the
                Google Cloud Organization of your Google Cloud project instead
                of developer token to determine your Google Ads API access
                levels. Use this flag only if you are enrolled into a limited
                pilot that supports this configuration.
            gaada: a str specifying the Google Ads API Assistant version.
        """
        if logging_config:
            logging.config.dictConfig(logging_config)

        self.credentials: Credentials = credentials
        self.developer_token: str = developer_token
        self.endpoint: Union[str, None] = endpoint
        self.login_customer_id: Union[str, None] = login_customer_id
        self.linked_customer_id: Union[str, None] = linked_customer_id
        self.version: Union[str, None] = version
        self.http_proxy: Union[str, None] = http_proxy
        self.use_proto_plus: bool = use_proto_plus
        self.use_cloud_org_for_api_access: Union[str, None] = (
            use_cloud_org_for_api_access
        )
        self.enums: _EnumGetter = _EnumGetter(self)
        self.gaada: Union[str, None] = gaada

        # If given, write the http_proxy channel option for GRPC to use
        if http_proxy:
            _GRPC_CHANNEL_OPTIONS.append(("grpc.http_proxy", http_proxy))

    def get_service(
        self,
        name: str,
        version: str = _DEFAULT_VERSION,
        interceptors: Union[list, None] = None,
        is_async: bool = False,
    ) -> Any:
        """Returns a service client instance for the specified service_name.

        Args:
            name: a str indicating the name of the service for which a service
              client is being retrieved; e.g. you may specify "CampaignService"
              to retrieve a CampaignServiceClient instance.
            version: a str indicating the version of the Google Ads API to be
              used.
            interceptors: an optional list of interceptors to include in
              requests. NOTE: this parameter is not intended for non-Google use
              and is not officially supported.
            is_async: whether or not to retrieve the async version of the
              service client being requested.

        Returns:
            A service client instance associated with the given service_name.

        Raises:
            AttributeError: If the specified name doesn't exist.
        """
        # If version is specified when the instance is created,
        # override any version specified as an argument.
        version = self.version if self.version else version
        # api_module = self._get_api_services_by_version(version)
        services_path: str = (
            f"google.ads.googleads.{version}.services.services"
        )
        snaked: str = util.convert_upper_case_to_snake_case(name)
        interceptors = interceptors or []

        try:
            service_module: Any = import_module(f"{services_path}.{snaked}")

            if is_async:
                service_name = _ASYNC_SERVICE_CLIENT_TEMPLATE.format(name)
            else:
                service_name = _SERVICE_CLIENT_TEMPLATE.format(name)

            service_client_class: Any = util.get_nested_attr(
                service_module, service_name
            )
        except (AttributeError, ModuleNotFoundError):
            raise ValueError(
                'Specified service {}" does not exist in Google '
                "Ads API {}.".format(name, version)
            )

        service_transport_class: Any = service_client_class.get_transport_class(
            "grpc_asyncio" if is_async else None
        )

        endpoint: str = (
            self.endpoint
            if self.endpoint
            else service_client_class.DEFAULT_ENDPOINT
        )

        if is_async:
            # In async requests, separate UnaryUnary and UnaryStream
            # interceptors need to be added to the channel.
            interceptors: List = interceptors or []
            interceptors = interceptors + [
                AsyncUnaryUnaryMetadataInterceptor(
                    self.developer_token,
                    self.login_customer_id,
                    self.linked_customer_id,
                    self.use_cloud_org_for_api_access,
                    gaada=self.gaada,
                ),
                AsyncUnaryStreamMetadataInterceptor(
                    self.developer_token,
                    self.login_customer_id,
                    self.linked_customer_id,
                    self.use_cloud_org_for_api_access,
                    gaada=self.gaada,
                ),
                AsyncUnaryUnaryLoggingInterceptor(_logger, version, endpoint),
                AsyncUnaryStreamLoggingInterceptor(_logger, version, endpoint),
                AsyncUnaryUnaryExceptionInterceptor(
                    version, use_proto_plus=self.use_proto_plus
                ),
                AsyncUnaryStreamExceptionInterceptor(
                    version, use_proto_plus=self.use_proto_plus
                ),
            ]

            channel: grpc.aio.Channel = service_transport_class.create_channel(
                host=endpoint,
                credentials=self.credentials,
                options=_GRPC_CHANNEL_OPTIONS,
                interceptors=interceptors,
            )

            service_transport: Any = service_transport_class(
                channel=channel, client_info=_CLIENT_INFO
            )

            if name == "YouTubeVideoUploadService":
                # YouTubeVideoUploadService uses REST, so we cannot pass the credentials inside the gRPC
                # channel; we need to pass them explicitly.
                return service_client_class(
                    transport=service_transport,
                    credentials=self.credentials,
                    developer_token=self.developer_token,
                    login_customer_id=self.login_customer_id,
                    linked_customer_id=self.linked_customer_id,
                    use_cloud_org_for_api_access=self.use_cloud_org_for_api_access
                )

            return service_client_class(transport=service_transport)

        channel: grpc.Channel = service_transport_class.create_channel(
            host=endpoint,
            credentials=self.credentials,
            options=_GRPC_CHANNEL_OPTIONS,
        )

        interceptors: List[Union[grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor]] = interceptors + [
            MetadataInterceptor(
                self.developer_token,
                self.login_customer_id,
                self.linked_customer_id,
                self.use_cloud_org_for_api_access,
                gaada=self.gaada,
            ),
            LoggingInterceptor(_logger, version, endpoint),
            ExceptionInterceptor(
                version, use_proto_plus=self.use_proto_plus
            ),
        ]

        channel: grpc.Channel = grpc.intercept_channel(channel, *interceptors)

        service_transport: Any = service_transport_class(
            channel=channel, client_info=_CLIENT_INFO
        )

        if name == "YouTubeVideoUploadService":
            # YouTubeVideoUploadService uses REST, so we cannot pass the credentials inside the gRPC
            # channel; we need to pass them explicitly.
            return service_client_class(
                transport=service_transport,
                credentials=self.credentials,
                developer_token=self.developer_token,
                login_customer_id=self.login_customer_id,
                linked_customer_id=self.linked_customer_id,
                use_cloud_org_for_api_access=self.use_cloud_org_for_api_access
            )

        return service_client_class(transport=service_transport)

    def get_type(self, name: str, version: str = _DEFAULT_VERSION) -> Union[ProtoPlusMessageType, ProtobufMessageType]:
        """Returns the specified common, enum, error, or resource type.

        Args:
            name: a str indicating the name of the type that is being retrieved;
              e.g. you may specify "CampaignOperation" to retrieve a
              CampaignOperation instance.
            version: a str indicating the Google Ads API version to be used.

        Returns:
            A Message instance representing the desired type.

        Raises:
            ValueError: If the type for the specified name doesn't exist
                in the given version.
        """
        # check that the name isn't a literal pb2 file name.
        if name.lower().endswith("pb2"):
            raise ValueError(
                f"Specified type '{name}' must be a class, not a module"
            )

        # check that a service or transport class isn't being requested
        # because they are initialized differently than normal message types.
        if name.lower().endswith("serviceclient") or name.lower().endswith(
            "transport"
        ):
            raise ValueError(
                f"Specified type '{name}' must not be a service "
                "or transport class."
            )

        # If version is specified when the instance is created,
        # override any version specified as an argument.
        version: str = self.version if self.version else version
        type_classes: ModuleType = self._get_api_services_by_version(version)

        for type_name in _MESSAGE_TYPES:
            if type_name == "services":
                path: str = f"{type_name}.types.{name}"
            else:
                path: str = f"{type_name}.{name}"

            try:
                message_class: Union[ProtoPlusMessageType, ProtobufMessageType] = util.get_nested_attr(type_classes, path)  # type: ignore[no-untyped-call]

                if self.use_proto_plus:
                    return message_class()
                else:
                    return util.convert_proto_plus_to_protobuf(message_class())  # type: ignore[no-untyped-call]
            except AttributeError:
                pass

        raise ValueError(
            f"Specified type '{name}' does not exist in "
            f"Google Ads API {version}"
        )
