# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from collections import OrderedDict
from http import HTTPStatus
import json
import logging as std_logging
import os
import re
from typing import (
    Dict,
    Callable,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
)
import warnings

from google.ads.googleads.v23 import gapic_version as package_version

from google.api_core import client_options as client_options_lib
from google.api_core import exceptions as core_exceptions
from google.api_core import gapic_v1
from google.api_core import retry as retries
from google.auth import credentials as ga_credentials  # type: ignore
from google.auth.transport import mtls  # type: ignore
from google.auth.transport.grpc import SslCredentials  # type: ignore
from google.auth.exceptions import MutualTLSChannelError  # type: ignore
from google.oauth2 import service_account  # type: ignore
import google.protobuf

try:
    OptionalRetry = Union[retries.Retry, gapic_v1.method._MethodDefault, None]
except AttributeError:  # pragma: NO COVER
    OptionalRetry = Union[retries.Retry, object, None]  # type: ignore

try:
    from google.api_core import client_logging  # type: ignore

    CLIENT_LOGGING_SUPPORTED = True  # pragma: NO COVER
except ImportError:  # pragma: NO COVER
    CLIENT_LOGGING_SUPPORTED = False

_LOGGER = std_logging.getLogger(__name__)

from google.ads.googleads.v23.common.types import audience_insights_attribute
from google.ads.googleads.v23.common.types import criteria
from google.ads.googleads.v23.enums.types import audience_insights_dimension
from google.ads.googleads.v23.services.types import audience_insights_service
from .transports.base import (
    AudienceInsightsServiceTransport,
    DEFAULT_CLIENT_INFO,
)
from .transports.grpc import AudienceInsightsServiceGrpcTransport
from .transports.grpc_asyncio import AudienceInsightsServiceGrpcAsyncIOTransport


class AudienceInsightsServiceClientMeta(type):
    """Metaclass for the AudienceInsightsService client.

    This provides class-level methods for building and retrieving
    support objects (e.g. transport) without polluting the client instance
    objects.
    """

    _transport_registry = (
        OrderedDict()
    )  # type: Dict[str, Type[AudienceInsightsServiceTransport]]
    _transport_registry["grpc"] = AudienceInsightsServiceGrpcTransport
    _transport_registry["grpc_asyncio"] = (
        AudienceInsightsServiceGrpcAsyncIOTransport
    )

    def get_transport_class(
        cls,
        label: Optional[str] = None,
    ) -> Type[AudienceInsightsServiceTransport]:
        """Returns an appropriate transport class.

        Args:
            label: The name of the desired transport. If none is
                provided, then the first transport in the registry is used.

        Returns:
            The transport class to use.
        """
        # If a specific transport is requested, return that one.
        if label:
            return cls._transport_registry[label]

        # No transport is requested; return the default (that is, the first one
        # in the dictionary).
        return next(iter(cls._transport_registry.values()))


class AudienceInsightsServiceClient(
    metaclass=AudienceInsightsServiceClientMeta
):
    """Audience Insights Service helps users find information about
    groups of people and how they can be reached with Google Ads.
    Accessible to allowlisted customers only.
    """

    @staticmethod
    def _get_default_mtls_endpoint(api_endpoint):
        """Converts api endpoint to mTLS endpoint.

        Convert "*.sandbox.googleapis.com" and "*.googleapis.com" to
        "*.mtls.sandbox.googleapis.com" and "*.mtls.googleapis.com" respectively.
        Args:
            api_endpoint (Optional[str]): the api endpoint to convert.
        Returns:
            str: converted mTLS api endpoint.
        """
        if not api_endpoint:
            return api_endpoint

        mtls_endpoint_re = re.compile(
            r"(?P<name>[^.]+)(?P<mtls>\.mtls)?(?P<sandbox>\.sandbox)?(?P<googledomain>\.googleapis\.com)?"
        )

        m = mtls_endpoint_re.match(api_endpoint)
        name, mtls, sandbox, googledomain = m.groups()
        if mtls or not googledomain:
            return api_endpoint

        if sandbox:
            return api_endpoint.replace(
                "sandbox.googleapis.com", "mtls.sandbox.googleapis.com"
            )

        return api_endpoint.replace(".googleapis.com", ".mtls.googleapis.com")

    # Note: DEFAULT_ENDPOINT is deprecated. Use _DEFAULT_ENDPOINT_TEMPLATE instead.
    DEFAULT_ENDPOINT = "googleads.googleapis.com"
    DEFAULT_MTLS_ENDPOINT = _get_default_mtls_endpoint.__func__(  # type: ignore
        DEFAULT_ENDPOINT
    )

    _DEFAULT_ENDPOINT_TEMPLATE = "googleads.{UNIVERSE_DOMAIN}"
    _DEFAULT_UNIVERSE = "googleapis.com"

    @staticmethod
    def _use_client_cert_effective():
        """Returns whether client certificate should be used for mTLS if the
        google-auth version supports should_use_client_cert automatic mTLS enablement.

        Alternatively, read from the GOOGLE_API_USE_CLIENT_CERTIFICATE env var.

        Returns:
            bool: whether client certificate should be used for mTLS
        Raises:
            ValueError: (If using a version of google-auth without should_use_client_cert and
            GOOGLE_API_USE_CLIENT_CERTIFICATE is set to an unexpected value.)
        """
        # check if google-auth version supports should_use_client_cert for automatic mTLS enablement
        if hasattr(mtls, "should_use_client_cert"):  # pragma: NO COVER
            return mtls.should_use_client_cert()
        else:  # pragma: NO COVER
            # if unsupported, fallback to reading from env var
            use_client_cert_str = os.getenv(
                "GOOGLE_API_USE_CLIENT_CERTIFICATE", "false"
            ).lower()
            if use_client_cert_str not in ("true", "false"):
                raise ValueError(
                    "Environment variable `GOOGLE_API_USE_CLIENT_CERTIFICATE` must be"
                    " either `true` or `false`"
                )
            return use_client_cert_str == "true"

    @classmethod
    def from_service_account_info(cls, info: dict, *args, **kwargs):
        """Creates an instance of this client using the provided credentials
            info.

        Args:
            info (dict): The service account private key info.
            args: Additional arguments to pass to the constructor.
            kwargs: Additional arguments to pass to the constructor.

        Returns:
            AudienceInsightsServiceClient: The constructed client.
        """
        credentials = service_account.Credentials.from_service_account_info(
            info
        )
        kwargs["credentials"] = credentials
        return cls(*args, **kwargs)

    @classmethod
    def from_service_account_file(cls, filename: str, *args, **kwargs):
        """Creates an instance of this client using the provided credentials
            file.

        Args:
            filename (str): The path to the service account private key json
                file.
            args: Additional arguments to pass to the constructor.
            kwargs: Additional arguments to pass to the constructor.

        Returns:
            AudienceInsightsServiceClient: The constructed client.
        """
        credentials = service_account.Credentials.from_service_account_file(
            filename
        )
        kwargs["credentials"] = credentials
        return cls(*args, **kwargs)

    from_service_account_json = from_service_account_file

    @property
    def transport(self) -> AudienceInsightsServiceTransport:
        """Returns the transport used by the client instance.

        Returns:
            AudienceInsightsServiceTransport: The transport used by the client
                instance.
        """
        return self._transport

    @staticmethod
    def common_billing_account_path(
        billing_account: str,
    ) -> str:
        """Returns a fully-qualified billing_account string."""
        return "billingAccounts/{billing_account}".format(
            billing_account=billing_account,
        )

    @staticmethod
    def parse_common_billing_account_path(path: str) -> Dict[str, str]:
        """Parse a billing_account path into its component segments."""
        m = re.match(r"^billingAccounts/(?P<billing_account>.+?)$", path)
        return m.groupdict() if m else {}

    @staticmethod
    def common_folder_path(
        folder: str,
    ) -> str:
        """Returns a fully-qualified folder string."""
        return "folders/{folder}".format(
            folder=folder,
        )

    @staticmethod
    def parse_common_folder_path(path: str) -> Dict[str, str]:
        """Parse a folder path into its component segments."""
        m = re.match(r"^folders/(?P<folder>.+?)$", path)
        return m.groupdict() if m else {}

    @staticmethod
    def common_organization_path(
        organization: str,
    ) -> str:
        """Returns a fully-qualified organization string."""
        return "organizations/{organization}".format(
            organization=organization,
        )

    @staticmethod
    def parse_common_organization_path(path: str) -> Dict[str, str]:
        """Parse a organization path into its component segments."""
        m = re.match(r"^organizations/(?P<organization>.+?)$", path)
        return m.groupdict() if m else {}

    @staticmethod
    def common_project_path(
        project: str,
    ) -> str:
        """Returns a fully-qualified project string."""
        return "projects/{project}".format(
            project=project,
        )

    @staticmethod
    def parse_common_project_path(path: str) -> Dict[str, str]:
        """Parse a project path into its component segments."""
        m = re.match(r"^projects/(?P<project>.+?)$", path)
        return m.groupdict() if m else {}

    @staticmethod
    def common_location_path(
        project: str,
        location: str,
    ) -> str:
        """Returns a fully-qualified location string."""
        return "projects/{project}/locations/{location}".format(
            project=project,
            location=location,
        )

    @staticmethod
    def parse_common_location_path(path: str) -> Dict[str, str]:
        """Parse a location path into its component segments."""
        m = re.match(
            r"^projects/(?P<project>.+?)/locations/(?P<location>.+?)$", path
        )
        return m.groupdict() if m else {}

    @classmethod
    def get_mtls_endpoint_and_cert_source(
        cls, client_options: Optional[client_options_lib.ClientOptions] = None
    ):
        """Deprecated. Return the API endpoint and client cert source for mutual TLS.

        The client cert source is determined in the following order:
        (1) if `GOOGLE_API_USE_CLIENT_CERTIFICATE` environment variable is not "true", the
        client cert source is None.
        (2) if `client_options.client_cert_source` is provided, use the provided one; if the
        default client cert source exists, use the default one; otherwise the client cert
        source is None.

        The API endpoint is determined in the following order:
        (1) if `client_options.api_endpoint` if provided, use the provided one.
        (2) if `GOOGLE_API_USE_CLIENT_CERTIFICATE` environment variable is "always", use the
        default mTLS endpoint; if the environment variable is "never", use the default API
        endpoint; otherwise if client cert source exists, use the default mTLS endpoint, otherwise
        use the default API endpoint.

        More details can be found at https://google.aip.dev/auth/4114.

        Args:
            client_options (google.api_core.client_options.ClientOptions): Custom options for the
                client. Only the `api_endpoint` and `client_cert_source` properties may be used
                in this method.

        Returns:
            Tuple[str, Callable[[], Tuple[bytes, bytes]]]: returns the API endpoint and the
                client cert source to use.

        Raises:
            google.auth.exceptions.MutualTLSChannelError: If any errors happen.
        """

        warnings.warn(
            "get_mtls_endpoint_and_cert_source is deprecated. Use the api_endpoint property instead.",
            DeprecationWarning,
        )
        if client_options is None:
            client_options = client_options_lib.ClientOptions()
        use_client_cert = (
            AudienceInsightsServiceClient._use_client_cert_effective()
        )
        use_mtls_endpoint = os.getenv("GOOGLE_API_USE_MTLS_ENDPOINT", "auto")
        if use_mtls_endpoint not in ("auto", "never", "always"):
            raise MutualTLSChannelError(
                "Environment variable `GOOGLE_API_USE_MTLS_ENDPOINT` must be `never`, `auto` or `always`"
            )

        # Figure out the client cert source to use.
        client_cert_source = None
        if use_client_cert:
            if client_options.client_cert_source:
                client_cert_source = client_options.client_cert_source
            elif mtls.has_default_client_cert_source():
                client_cert_source = mtls.default_client_cert_source()

        # Figure out which api endpoint to use.
        if client_options.api_endpoint is not None:
            api_endpoint = client_options.api_endpoint
        elif use_mtls_endpoint == "always" or (
            use_mtls_endpoint == "auto" and client_cert_source
        ):
            api_endpoint = cls.DEFAULT_MTLS_ENDPOINT
        else:
            api_endpoint = cls.DEFAULT_ENDPOINT

        return api_endpoint, client_cert_source

    @staticmethod
    def _read_environment_variables():
        """Returns the environment variables used by the client.

        Returns:
            Tuple[bool, str, str]: returns the GOOGLE_API_USE_CLIENT_CERTIFICATE,
            GOOGLE_API_USE_MTLS_ENDPOINT, and GOOGLE_CLOUD_UNIVERSE_DOMAIN environment variables.

        Raises:
            ValueError: If GOOGLE_API_USE_CLIENT_CERTIFICATE is not
                any of ["true", "false"].
            google.auth.exceptions.MutualTLSChannelError: If GOOGLE_API_USE_MTLS_ENDPOINT
                is not any of ["auto", "never", "always"].
        """
        use_client_cert = (
            AudienceInsightsServiceClient._use_client_cert_effective()
        )
        use_mtls_endpoint = os.getenv(
            "GOOGLE_API_USE_MTLS_ENDPOINT", "auto"
        ).lower()
        universe_domain_env = os.getenv("GOOGLE_CLOUD_UNIVERSE_DOMAIN")
        if use_mtls_endpoint not in ("auto", "never", "always"):
            raise MutualTLSChannelError(
                "Environment variable `GOOGLE_API_USE_MTLS_ENDPOINT` must be `never`, `auto` or `always`"
            )
        return use_client_cert, use_mtls_endpoint, universe_domain_env

    @staticmethod
    def _get_client_cert_source(provided_cert_source, use_cert_flag):
        """Return the client cert source to be used by the client.

        Args:
            provided_cert_source (bytes): The client certificate source provided.
            use_cert_flag (bool): A flag indicating whether to use the client certificate.

        Returns:
            bytes or None: The client cert source to be used by the client.
        """
        client_cert_source = None
        if use_cert_flag:
            if provided_cert_source:
                client_cert_source = provided_cert_source
            elif mtls.has_default_client_cert_source():
                client_cert_source = mtls.default_client_cert_source()
        return client_cert_source

    @staticmethod
    def _get_api_endpoint(
        api_override, client_cert_source, universe_domain, use_mtls_endpoint
    ):
        """Return the API endpoint used by the client.

        Args:
            api_override (str): The API endpoint override. If specified, this is always
                the return value of this function and the other arguments are not used.
            client_cert_source (bytes): The client certificate source used by the client.
            universe_domain (str): The universe domain used by the client.
            use_mtls_endpoint (str): How to use the mTLS endpoint, which depends also on the other parameters.
                Possible values are "always", "auto", or "never".

        Returns:
            str: The API endpoint to be used by the client.
        """
        if api_override is not None:
            api_endpoint = api_override
        elif use_mtls_endpoint == "always" or (
            use_mtls_endpoint == "auto" and client_cert_source
        ):
            _default_universe = AudienceInsightsServiceClient._DEFAULT_UNIVERSE
            if universe_domain != _default_universe:
                raise MutualTLSChannelError(
                    f"mTLS is not supported in any universe other than {_default_universe}."
                )
            api_endpoint = AudienceInsightsServiceClient.DEFAULT_MTLS_ENDPOINT
        else:
            api_endpoint = (
                AudienceInsightsServiceClient._DEFAULT_ENDPOINT_TEMPLATE.format(
                    UNIVERSE_DOMAIN=universe_domain
                )
            )
        return api_endpoint

    @staticmethod
    def _get_universe_domain(
        client_universe_domain: Optional[str],
        universe_domain_env: Optional[str],
    ) -> str:
        """Return the universe domain used by the client.

        Args:
            client_universe_domain (Optional[str]): The universe domain configured via the client options.
            universe_domain_env (Optional[str]): The universe domain configured via the "GOOGLE_CLOUD_UNIVERSE_DOMAIN" environment variable.

        Returns:
            str: The universe domain to be used by the client.

        Raises:
            ValueError: If the universe domain is an empty string.
        """
        universe_domain = AudienceInsightsServiceClient._DEFAULT_UNIVERSE
        if client_universe_domain is not None:
            universe_domain = client_universe_domain
        elif universe_domain_env is not None:
            universe_domain = universe_domain_env
        if len(universe_domain.strip()) == 0:
            raise ValueError("Universe Domain cannot be an empty string.")
        return universe_domain

    def _validate_universe_domain(self):
        """Validates client's and credentials' universe domains are consistent.

        Returns:
            bool: True iff the configured universe domain is valid.

        Raises:
            ValueError: If the configured universe domain is not valid.
        """

        # NOTE (b/349488459): universe validation is disabled until further notice.
        return True

    def _add_cred_info_for_auth_errors(
        self, error: core_exceptions.GoogleAPICallError
    ) -> None:
        """Adds credential info string to error details for 401/403/404 errors.

        Args:
            error (google.api_core.exceptions.GoogleAPICallError): The error to add the cred info.
        """
        if error.code not in [
            HTTPStatus.UNAUTHORIZED,
            HTTPStatus.FORBIDDEN,
            HTTPStatus.NOT_FOUND,
        ]:
            return

        cred = self._transport._credentials

        # get_cred_info is only available in google-auth>=2.35.0
        if not hasattr(cred, "get_cred_info"):
            return

        # ignore the type check since pypy test fails when get_cred_info
        # is not available
        cred_info = cred.get_cred_info()  # type: ignore
        if cred_info and hasattr(error._details, "append"):
            error._details.append(json.dumps(cred_info))

    @property
    def api_endpoint(self):
        """Return the API endpoint used by the client instance.

        Returns:
            str: The API endpoint used by the client instance.
        """
        return self._api_endpoint

    @property
    def universe_domain(self) -> str:
        """Return the universe domain used by the client instance.

        Returns:
            str: The universe domain used by the client instance.
        """
        return self._universe_domain

    def __init__(
        self,
        *,
        credentials: Optional[ga_credentials.Credentials] = None,
        transport: Optional[
            Union[
                str,
                AudienceInsightsServiceTransport,
                Callable[..., AudienceInsightsServiceTransport],
            ]
        ] = None,
        client_options: Optional[
            Union[client_options_lib.ClientOptions, dict]
        ] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
    ) -> None:
        """Instantiates the audience insights service client.

        Args:
            credentials (Optional[google.auth.credentials.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify the application to the service; if none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
            transport (Optional[Union[str,AudienceInsightsServiceTransport,Callable[..., AudienceInsightsServiceTransport]]]):
                The transport to use, or a Callable that constructs and returns a new transport.
                If a Callable is given, it will be called with the same set of initialization
                arguments as used in the AudienceInsightsServiceTransport constructor.
                If set to None, a transport is chosen automatically.
            client_options (Optional[Union[google.api_core.client_options.ClientOptions, dict]]):
                Custom options for the client.

                1. The ``api_endpoint`` property can be used to override the
                default endpoint provided by the client when ``transport`` is
                not explicitly provided. Only if this property is not set and
                ``transport`` was not explicitly provided, the endpoint is
                determined by the GOOGLE_API_USE_MTLS_ENDPOINT environment
                variable, which have one of the following values:
                "always" (always use the default mTLS endpoint), "never" (always
                use the default regular endpoint) and "auto" (auto-switch to the
                default mTLS endpoint if client certificate is present; this is
                the default value).

                2. If the GOOGLE_API_USE_CLIENT_CERTIFICATE environment variable
                is "true", then the ``client_cert_source`` property can be used
                to provide a client certificate for mTLS transport. If
                not provided, the default SSL client certificate will be used if
                present. If GOOGLE_API_USE_CLIENT_CERTIFICATE is "false" or not
                set, no client certificate will be used.

                3. The ``universe_domain`` property can be used to override the
                default "googleapis.com" universe. Note that the ``api_endpoint``
                property still takes precedence; and ``universe_domain`` is
                currently not supported for mTLS.

            client_info (google.api_core.gapic_v1.client_info.ClientInfo):
                The client info used to send a user-agent string along with
                API requests. If ``None``, then default info will be used.
                Generally, you only need to set this if you're developing
                your own client library.

        Raises:
            google.auth.exceptions.MutualTLSChannelError: If mutual TLS transport
                creation failed for any reason.
        """
        self._client_options = client_options
        if isinstance(self._client_options, dict):
            self._client_options = client_options_lib.from_dict(
                self._client_options
            )
        if self._client_options is None:
            self._client_options = client_options_lib.ClientOptions()
        self._client_options = cast(
            client_options_lib.ClientOptions, self._client_options
        )

        universe_domain_opt = getattr(
            self._client_options, "universe_domain", None
        )

        (
            self._use_client_cert,
            self._use_mtls_endpoint,
            self._universe_domain_env,
        ) = AudienceInsightsServiceClient._read_environment_variables()
        self._client_cert_source = (
            AudienceInsightsServiceClient._get_client_cert_source(
                self._client_options.client_cert_source, self._use_client_cert
            )
        )
        self._universe_domain = (
            AudienceInsightsServiceClient._get_universe_domain(
                universe_domain_opt, self._universe_domain_env
            )
        )
        self._api_endpoint = None  # updated below, depending on `transport`

        # Initialize the universe domain validation.
        self._is_universe_domain_valid = False

        if CLIENT_LOGGING_SUPPORTED:  # pragma: NO COVER
            # Setup logging.
            client_logging.initialize_logging()

        api_key_value = getattr(self._client_options, "api_key", None)
        if api_key_value and credentials:
            raise ValueError(
                "client_options.api_key and credentials are mutually exclusive"
            )

        # Save or instantiate the transport.
        # Ordinarily, we provide the transport, but allowing a custom transport
        # instance provides an extensibility point for unusual situations.
        transport_provided = isinstance(
            transport, AudienceInsightsServiceTransport
        )
        if transport_provided:
            # transport is a AudienceInsightsServiceTransport instance.
            if (
                credentials
                or self._client_options.credentials_file
                or api_key_value
            ):
                raise ValueError(
                    "When providing a transport instance, "
                    "provide its credentials directly."
                )
            if self._client_options.scopes:
                raise ValueError(
                    "When providing a transport instance, provide its scopes "
                    "directly."
                )
            self._transport = cast(AudienceInsightsServiceTransport, transport)
            self._api_endpoint = self._transport.host

        self._api_endpoint = (
            self._api_endpoint
            or AudienceInsightsServiceClient._get_api_endpoint(
                self._client_options.api_endpoint,
                self._client_cert_source,
                self._universe_domain,
                self._use_mtls_endpoint,
            )
        )

        if not transport_provided:
            import google.auth._default  # type: ignore

            if api_key_value and hasattr(
                google.auth._default, "get_api_key_credentials"
            ):
                credentials = google.auth._default.get_api_key_credentials(
                    api_key_value
                )

            transport_init: Union[
                Type[AudienceInsightsServiceTransport],
                Callable[..., AudienceInsightsServiceTransport],
            ] = (
                AudienceInsightsServiceClient.get_transport_class(transport)
                if isinstance(transport, str) or transport is None
                else cast(
                    Callable[..., AudienceInsightsServiceTransport], transport
                )
            )
            # initialize with the provided callable or the passed in class
            self._transport = transport_init(
                credentials=credentials,
                credentials_file=self._client_options.credentials_file,
                host=self._api_endpoint,
                scopes=self._client_options.scopes,
                client_cert_source_for_mtls=self._client_cert_source,
                quota_project_id=self._client_options.quota_project_id,
                client_info=client_info,
                always_use_jwt_access=True,
                api_audience=self._client_options.api_audience,
            )

        if "async" not in str(self._transport):
            if CLIENT_LOGGING_SUPPORTED and _LOGGER.isEnabledFor(
                std_logging.DEBUG
            ):  # pragma: NO COVER
                _LOGGER.debug(
                    "Created client `google.ads.googleads.v23.services.AudienceInsightsServiceClient`.",
                    extra=(
                        {
                            "serviceName": "google.ads.googleads.v23.services.AudienceInsightsService",
                            "universeDomain": getattr(
                                self._transport._credentials,
                                "universe_domain",
                                "",
                            ),
                            "credentialsType": f"{type(self._transport._credentials).__module__}.{type(self._transport._credentials).__qualname__}",
                            "credentialsInfo": getattr(
                                self.transport._credentials,
                                "get_cred_info",
                                lambda: None,
                            )(),
                        }
                        if hasattr(self._transport, "_credentials")
                        else {
                            "serviceName": "google.ads.googleads.v23.services.AudienceInsightsService",
                            "credentialsType": None,
                        }
                    ),
                )

    def generate_insights_finder_report(
        self,
        request: Optional[
            Union[
                audience_insights_service.GenerateInsightsFinderReportRequest,
                dict,
            ]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        baseline_audience: Optional[
            audience_insights_service.InsightsAudience
        ] = None,
        specific_audience: Optional[
            audience_insights_service.InsightsAudience
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.GenerateInsightsFinderReportResponse:
        r"""Creates a saved report that can be viewed in the Insights Finder
        tool.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `FieldError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RangeError <>`__
        `RequestError <>`__

        Args:
            request (Union[google.ads.googleads.v23.services.types.GenerateInsightsFinderReportRequest, dict]):
                The request object. Request message for
                [AudienceInsightsService.GenerateInsightsFinderReport][google.ads.googleads.v23.services.AudienceInsightsService.GenerateInsightsFinderReport].
            customer_id (str):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            baseline_audience (google.ads.googleads.v23.services.types.InsightsAudience):
                Required. A baseline audience for
                this report, typically all people in a
                region.

                This corresponds to the ``baseline_audience`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            specific_audience (google.ads.googleads.v23.services.types.InsightsAudience):
                Required. The specific audience of
                interest for this report.  The insights
                in the report will be based on
                attributes more prevalent in this
                audience than in the report's baseline
                audience.

                This corresponds to the ``specific_audience`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.GenerateInsightsFinderReportResponse:
                The response message for
                   [AudienceInsightsService.GenerateInsightsFinderReport][google.ads.googleads.v23.services.AudienceInsightsService.GenerateInsightsFinderReport],
                   containing the shareable URL for the report.

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, baseline_audience, specific_audience]
        has_flattened_params = (
            len([param for param in flattened_params if param is not None]) > 0
        )
        if request is not None and has_flattened_params:
            raise ValueError(
                "If the `request` argument is set, then none of "
                "the individual field arguments should be set."
            )

        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request,
            audience_insights_service.GenerateInsightsFinderReportRequest,
        ):
            request = (
                audience_insights_service.GenerateInsightsFinderReportRequest(
                    request
                )
            )
            # If we have keyword arguments corresponding to fields on the
            # request, apply these.
            if customer_id is not None:
                request.customer_id = customer_id
            if baseline_audience is not None:
                request.baseline_audience = baseline_audience
            if specific_audience is not None:
                request.specific_audience = specific_audience

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._transport._wrapped_methods[
            self._transport.generate_insights_finder_report
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("customer_id", request.customer_id),)
            ),
        )

        # Validate the universe domain.
        self._validate_universe_domain()

        # Send the request.
        response = rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    def list_audience_insights_attributes(
        self,
        request: Optional[
            Union[
                audience_insights_service.ListAudienceInsightsAttributesRequest,
                dict,
            ]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        dimensions: Optional[
            MutableSequence[
                audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
            ]
        ] = None,
        query_text: Optional[str] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.ListAudienceInsightsAttributesResponse:
        r"""Searches for audience attributes that can be used to generate
        insights.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `FieldError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RangeError <>`__
        `RequestError <>`__

        Args:
            request (Union[google.ads.googleads.v23.services.types.ListAudienceInsightsAttributesRequest, dict]):
                The request object. Request message for
                [AudienceInsightsService.ListAudienceInsightsAttributes][google.ads.googleads.v23.services.AudienceInsightsService.ListAudienceInsightsAttributes].
            customer_id (str):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            dimensions (MutableSequence[google.ads.googleads.v23.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension]):
                Required. The types of attributes to be returned.
                Supported dimensions are CATEGORY, KNOWLEDGE_GRAPH,
                DEVICE, GEO_TARGET_COUNTRY, SUB_COUNTRY_LOCATION,
                YOUTUBE_LINEUP, AFFINITY_USER_INTEREST,
                IN_MARKET_USER_INTEREST, LIFE_EVENT_USER_INTEREST,
                PARENTAL_STATUS, INCOME_RANGE, AGE_RANGE, and GENDER.

                This corresponds to the ``dimensions`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            query_text (str):
                Required. A free text query. If the requested dimensions
                include Attributes CATEGORY or KNOWLEDGE_GRAPH, then the
                attributes returned for those dimensions will match or
                be related to this string. For other dimensions, this
                field is ignored and all available attributes are
                returned.

                This corresponds to the ``query_text`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.ListAudienceInsightsAttributesResponse:
                Response message for
                   [AudienceInsightsService.ListAudienceInsightsAttributes][google.ads.googleads.v23.services.AudienceInsightsService.ListAudienceInsightsAttributes].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, dimensions, query_text]
        has_flattened_params = (
            len([param for param in flattened_params if param is not None]) > 0
        )
        if request is not None and has_flattened_params:
            raise ValueError(
                "If the `request` argument is set, then none of "
                "the individual field arguments should be set."
            )

        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request,
            audience_insights_service.ListAudienceInsightsAttributesRequest,
        ):
            request = (
                audience_insights_service.ListAudienceInsightsAttributesRequest(
                    request
                )
            )
            # If we have keyword arguments corresponding to fields on the
            # request, apply these.
            if customer_id is not None:
                request.customer_id = customer_id
            if dimensions is not None:
                request.dimensions = dimensions
            if query_text is not None:
                request.query_text = query_text

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._transport._wrapped_methods[
            self._transport.list_audience_insights_attributes
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("customer_id", request.customer_id),)
            ),
        )

        # Validate the universe domain.
        self._validate_universe_domain()

        # Send the request.
        response = rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    def list_insights_eligible_dates(
        self,
        request: Optional[
            Union[
                audience_insights_service.ListInsightsEligibleDatesRequest, dict
            ]
        ] = None,
        *,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.ListInsightsEligibleDatesResponse:
        r"""Lists date ranges for which audience insights data can be
        requested.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `FieldError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RangeError <>`__
        `RequestError <>`__

        Args:
            request (Union[google.ads.googleads.v23.services.types.ListInsightsEligibleDatesRequest, dict]):
                The request object. Request message for
                [AudienceInsightsService.ListInsightsEligibleDates][google.ads.googleads.v23.services.AudienceInsightsService.ListInsightsEligibleDates].
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.ListInsightsEligibleDatesResponse:
                Response message for
                   [AudienceInsightsService.ListInsightsEligibleDates][google.ads.googleads.v23.services.AudienceInsightsService.ListInsightsEligibleDates].

        """
        # Create or coerce a protobuf request object.
        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request, audience_insights_service.ListInsightsEligibleDatesRequest
        ):
            request = (
                audience_insights_service.ListInsightsEligibleDatesRequest(
                    request
                )
            )

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._transport._wrapped_methods[
            self._transport.list_insights_eligible_dates
        ]

        # Validate the universe domain.
        self._validate_universe_domain()

        # Send the request.
        response = rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    def generate_audience_composition_insights(
        self,
        request: Optional[
            Union[
                audience_insights_service.GenerateAudienceCompositionInsightsRequest,
                dict,
            ]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        audience: Optional[audience_insights_service.InsightsAudience] = None,
        dimensions: Optional[
            MutableSequence[
                audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
            ]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.GenerateAudienceCompositionInsightsResponse:
        r"""Returns a collection of attributes that are represented in an
        audience of interest, with metrics that compare each attribute's
        share of the audience with its share of a baseline audience.

        List of thrown errors: `AudienceInsightsError <>`__
        `AuthenticationError <>`__ `AuthorizationError <>`__
        `FieldError <>`__ `HeaderError <>`__ `InternalError <>`__
        `QuotaError <>`__ `RangeError <>`__ `RequestError <>`__

        Args:
            request (Union[google.ads.googleads.v23.services.types.GenerateAudienceCompositionInsightsRequest, dict]):
                The request object. Request message for
                [AudienceInsightsService.GenerateAudienceCompositionInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceCompositionInsights].
            customer_id (str):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            audience (google.ads.googleads.v23.services.types.InsightsAudience):
                Required. The audience of interest
                for which insights are being requested.

                This corresponds to the ``audience`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            dimensions (MutableSequence[google.ads.googleads.v23.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension]):
                Required. The audience dimensions for which composition
                insights should be returned. Supported dimensions are
                KNOWLEDGE_GRAPH, GEO_TARGET_COUNTRY,
                SUB_COUNTRY_LOCATION, YOUTUBE_CHANNEL, YOUTUBE_LINEUP,
                AFFINITY_USER_INTEREST, IN_MARKET_USER_INTEREST,
                LIFE_EVENT_USER_INTEREST, PARENTAL_STATUS, INCOME_RANGE,
                AGE_RANGE, and GENDER.

                This corresponds to the ``dimensions`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.GenerateAudienceCompositionInsightsResponse:
                Response message for
                   [AudienceInsightsService.GenerateAudienceCompositionInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceCompositionInsights].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, audience, dimensions]
        has_flattened_params = (
            len([param for param in flattened_params if param is not None]) > 0
        )
        if request is not None and has_flattened_params:
            raise ValueError(
                "If the `request` argument is set, then none of "
                "the individual field arguments should be set."
            )

        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request,
            audience_insights_service.GenerateAudienceCompositionInsightsRequest,
        ):
            request = audience_insights_service.GenerateAudienceCompositionInsightsRequest(
                request
            )
            # If we have keyword arguments corresponding to fields on the
            # request, apply these.
            if customer_id is not None:
                request.customer_id = customer_id
            if audience is not None:
                request.audience = audience
            if dimensions is not None:
                request.dimensions = dimensions

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._transport._wrapped_methods[
            self._transport.generate_audience_composition_insights
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("customer_id", request.customer_id),)
            ),
        )

        # Validate the universe domain.
        self._validate_universe_domain()

        # Send the request.
        response = rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    def generate_audience_definition(
        self,
        request: Optional[
            Union[
                audience_insights_service.GenerateAudienceDefinitionRequest,
                dict,
            ]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        audience_description: Optional[
            audience_insights_service.InsightsAudienceDescription
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.GenerateAudienceDefinitionResponse:
        r"""Returns a collection of audience attributes using generative AI
        based on the provided audience description.

        List of thrown errors: `AudienceInsightsError <>`__
        `AuthenticationError <>`__ `AuthorizationError <>`__
        `FieldError <>`__ `HeaderError <>`__ `InternalError <>`__
        `QuotaError <>`__ `RequestError <>`__

        Args:
            request (Union[google.ads.googleads.v23.services.types.GenerateAudienceDefinitionRequest, dict]):
                The request object. Request message for
                [AudienceInsightsService.GenerateAudienceDefinition][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceDefinition].
            customer_id (str):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            audience_description (google.ads.googleads.v23.services.types.InsightsAudienceDescription):
                Required. Provide a text description of an audience to
                get AI-generated structured suggestions. This can take
                around 5 or more seconds to complete Supported marketing
                objectives are: AWARENESS, CONSIDERATION and RESEARCH.
                Supported dimensions are: AGE_RANGE, GENDER,
                PARENTAL_STATUS, AFFINITY_USER_INTEREST,
                IN_MARKET_USER_INTEREST, LIFE_EVENT_USER_INTEREST,
                CATEGORY and KNOWLEDGE_GRAPH.

                This corresponds to the ``audience_description`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.GenerateAudienceDefinitionResponse:
                Response message for
                   [AudienceInsightsService.GenerateAudienceDefinition][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceDefinition].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, audience_description]
        has_flattened_params = (
            len([param for param in flattened_params if param is not None]) > 0
        )
        if request is not None and has_flattened_params:
            raise ValueError(
                "If the `request` argument is set, then none of "
                "the individual field arguments should be set."
            )

        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request, audience_insights_service.GenerateAudienceDefinitionRequest
        ):
            request = (
                audience_insights_service.GenerateAudienceDefinitionRequest(
                    request
                )
            )
            # If we have keyword arguments corresponding to fields on the
            # request, apply these.
            if customer_id is not None:
                request.customer_id = customer_id
            if audience_description is not None:
                request.audience_description = audience_description

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._transport._wrapped_methods[
            self._transport.generate_audience_definition
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("customer_id", request.customer_id),)
            ),
        )

        # Validate the universe domain.
        self._validate_universe_domain()

        # Send the request.
        response = rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    def generate_suggested_targeting_insights(
        self,
        request: Optional[
            Union[
                audience_insights_service.GenerateSuggestedTargetingInsightsRequest,
                dict,
            ]
        ] = None,
        *,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.GenerateSuggestedTargetingInsightsResponse:
        r"""Returns a collection of targeting insights (e.g. targetable
        audiences) that are relevant to the requested audience.

        List of thrown errors: `AudienceInsightsError <>`__
        `AuthenticationError <>`__ `AuthorizationError <>`__
        `FieldError <>`__ `HeaderError <>`__ `InternalError <>`__
        `QuotaError <>`__ `RangeError <>`__ `RequestError <>`__

        Args:
            request (Union[google.ads.googleads.v23.services.types.GenerateSuggestedTargetingInsightsRequest, dict]):
                The request object. Request message for
                [AudienceInsightsService.GenerateSuggestedTargetingInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateSuggestedTargetingInsights].
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.GenerateSuggestedTargetingInsightsResponse:
                Response message for
                   [AudienceInsightsService.GenerateSuggestedTargetingInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateSuggestedTargetingInsights].

        """
        # Create or coerce a protobuf request object.
        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request,
            audience_insights_service.GenerateSuggestedTargetingInsightsRequest,
        ):
            request = audience_insights_service.GenerateSuggestedTargetingInsightsRequest(
                request
            )

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._transport._wrapped_methods[
            self._transport.generate_suggested_targeting_insights
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("customer_id", request.customer_id),)
            ),
        )

        # Validate the universe domain.
        self._validate_universe_domain()

        # Send the request.
        response = rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    def generate_audience_overlap_insights(
        self,
        request: Optional[
            Union[
                audience_insights_service.GenerateAudienceOverlapInsightsRequest,
                dict,
            ]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        country_location: Optional[criteria.LocationInfo] = None,
        primary_attribute: Optional[
            audience_insights_attribute.AudienceInsightsAttribute
        ] = None,
        dimensions: Optional[
            MutableSequence[
                audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
            ]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.GenerateAudienceOverlapInsightsResponse:
        r"""Returns a collection of audience attributes along with estimates
        of the overlap between their potential YouTube reach and that of
        a given input attribute.

        List of thrown errors: `AudienceInsightsError <>`__
        `AuthenticationError <>`__ `AuthorizationError <>`__
        `FieldError <>`__ `HeaderError <>`__ `InternalError <>`__
        `QuotaError <>`__ `RangeError <>`__ `RequestError <>`__

        Args:
            request (Union[google.ads.googleads.v23.services.types.GenerateAudienceOverlapInsightsRequest, dict]):
                The request object. Request message for
                [AudienceInsightsService.GenerateAudienceOverlapInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceOverlapInsights].
            customer_id (str):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            country_location (google.ads.googleads.v23.common.types.LocationInfo):
                Required. The country in which to
                calculate the sizes and overlaps of
                audiences.

                This corresponds to the ``country_location`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            primary_attribute (google.ads.googleads.v23.common.types.AudienceInsightsAttribute):
                Required. The audience attribute that
                should be intersected with all other
                eligible audiences.  This must be an
                Affinity or In-Market UserInterest, an
                AgeRange or a Gender.

                This corresponds to the ``primary_attribute`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            dimensions (MutableSequence[google.ads.googleads.v23.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension]):
                Required. The types of attributes of which to calculate
                the overlap with the primary_attribute. The values must
                be a subset of AFFINITY_USER_INTEREST,
                IN_MARKET_USER_INTEREST, AGE_RANGE and GENDER.

                This corresponds to the ``dimensions`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.GenerateAudienceOverlapInsightsResponse:
                Response message for
                   [AudienceInsightsService.GenerateAudienceOverlapInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceOverlapInsights].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [
            customer_id,
            country_location,
            primary_attribute,
            dimensions,
        ]
        has_flattened_params = (
            len([param for param in flattened_params if param is not None]) > 0
        )
        if request is not None and has_flattened_params:
            raise ValueError(
                "If the `request` argument is set, then none of "
                "the individual field arguments should be set."
            )

        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request,
            audience_insights_service.GenerateAudienceOverlapInsightsRequest,
        ):
            request = audience_insights_service.GenerateAudienceOverlapInsightsRequest(
                request
            )
            # If we have keyword arguments corresponding to fields on the
            # request, apply these.
            if customer_id is not None:
                request.customer_id = customer_id
            if country_location is not None:
                request.country_location = country_location
            if primary_attribute is not None:
                request.primary_attribute = primary_attribute
            if dimensions is not None:
                request.dimensions = dimensions

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._transport._wrapped_methods[
            self._transport.generate_audience_overlap_insights
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("customer_id", request.customer_id),)
            ),
        )

        # Validate the universe domain.
        self._validate_universe_domain()

        # Send the request.
        response = rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    def generate_targeting_suggestion_metrics(
        self,
        request: Optional[
            Union[
                audience_insights_service.GenerateTargetingSuggestionMetricsRequest,
                dict,
            ]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        audiences: Optional[
            MutableSequence[audience_insights_service.InsightsAudience]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.GenerateTargetingSuggestionMetricsResponse:
        r"""Returns potential reach metrics for targetable audiences.

        This method helps answer questions like "How many Men aged 18+
        interested in Camping can be reached on YouTube?"

        List of thrown errors: `AudienceInsightsError <>`__
        `AuthenticationError <>`__ `AuthorizationError <>`__
        `FieldError <>`__ `HeaderError <>`__ `InternalError <>`__
        `QuotaError <>`__ `RangeError <>`__ `RequestError <>`__

        Args:
            request (Union[google.ads.googleads.v23.services.types.GenerateTargetingSuggestionMetricsRequest, dict]):
                The request object. Request message for
                [AudienceInsightsService.GenerateTargetingSuggestionMetrics][google.ads.googleads.v23.services.AudienceInsightsService.GenerateTargetingSuggestionMetrics].
            customer_id (str):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            audiences (MutableSequence[google.ads.googleads.v23.services.types.InsightsAudience]):
                Required. Audiences to request
                metrics for.

                This corresponds to the ``audiences`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.GenerateTargetingSuggestionMetricsResponse:
                Response message for
                   [AudienceInsightsService.GenerateTargetingSuggestionMetrics][google.ads.googleads.v23.services.AudienceInsightsService.GenerateTargetingSuggestionMetrics].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, audiences]
        has_flattened_params = (
            len([param for param in flattened_params if param is not None]) > 0
        )
        if request is not None and has_flattened_params:
            raise ValueError(
                "If the `request` argument is set, then none of "
                "the individual field arguments should be set."
            )

        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request,
            audience_insights_service.GenerateTargetingSuggestionMetricsRequest,
        ):
            request = audience_insights_service.GenerateTargetingSuggestionMetricsRequest(
                request
            )
            # If we have keyword arguments corresponding to fields on the
            # request, apply these.
            if customer_id is not None:
                request.customer_id = customer_id
            if audiences is not None:
                request.audiences = audiences

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._transport._wrapped_methods[
            self._transport.generate_targeting_suggestion_metrics
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("customer_id", request.customer_id),)
            ),
        )

        # Validate the universe domain.
        self._validate_universe_domain()

        # Send the request.
        response = rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    def __enter__(self) -> "AudienceInsightsServiceClient":
        return self

    def __exit__(self, type, value, traceback):
        """Releases underlying transport's resources.

        .. warning::
            ONLY use as a context manager if the transport is NOT shared
            with other clients! Exiting the with block will CLOSE the transport
            and may cause errors in other clients!
        """
        self.transport.close()


DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo(
    gapic_version=package_version.__version__
)

if hasattr(DEFAULT_CLIENT_INFO, "protobuf_runtime_version"):  # pragma: NO COVER
    DEFAULT_CLIENT_INFO.protobuf_runtime_version = google.protobuf.__version__

__all__ = ("AudienceInsightsServiceClient",)
