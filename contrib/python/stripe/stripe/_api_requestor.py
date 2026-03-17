from io import BytesIO, IOBase
import json
import platform
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
    cast,
    ClassVar,
)
from typing_extensions import (
    TYPE_CHECKING,
    Literal,
    NoReturn,
    Unpack,
)
import uuid
from urllib.parse import urlsplit, urlunsplit, parse_qs

# breaking circular dependency
import stripe  # noqa: IMP101
from stripe._util import (
    log_debug,
    log_info,
    dashboard_link,
    _convert_to_stripe_object,
    get_api_mode,
)
from stripe._version import VERSION
import stripe._error as error
import stripe.oauth_error as oauth_error
from stripe._multipart_data_generator import MultipartDataGenerator
from urllib.parse import urlencode
from stripe._encode import _api_encode, _json_encode_date_callback
from stripe._stripe_response import (
    StripeResponse,
    StripeStreamResponse,
    StripeStreamResponseAsync,
)
from stripe._request_options import (
    PERSISTENT_OPTIONS_KEYS,
    RequestOptions,
    merge_options,
)
from stripe._requestor_options import (
    RequestorOptions,
    _GlobalRequestorOptions,
)
from stripe._http_client import (
    HTTPClient,
    new_default_http_client,
    new_http_client_async_fallback,
)

from stripe._base_address import BaseAddress
from stripe._api_mode import ApiMode

if TYPE_CHECKING:
    from stripe._app_info import AppInfo
    from stripe._stripe_object import StripeObject

HttpVerb = Literal["get", "post", "delete"]

# Lazily initialized
_default_proxy: Optional[str] = None


def is_v2_delete_resp(method: str, api_mode: ApiMode) -> bool:
    return method == "delete" and api_mode == "V2"


class _APIRequestor(object):
    _instance: ClassVar["_APIRequestor|None"] = None

    def __init__(
        self,
        options: Optional[RequestorOptions] = None,
        client: Optional[HTTPClient] = None,
    ):
        if options is None:
            options = RequestorOptions()
        self._options = options
        self._client = client

    # In the case of client=None, we should use the current value of stripe.default_http_client
    # or lazily initialize it. Since stripe.default_http_client can change throughout the lifetime of
    # an _APIRequestor, we shouldn't set it as stripe._client and should access it only through this
    # getter.
    def _get_http_client(self) -> HTTPClient:
        client = self._client
        if client is None:
            global _default_proxy

            if not stripe.default_http_client:
                kwargs = {
                    "verify_ssl_certs": stripe.verify_ssl_certs,
                    "proxy": stripe.proxy,
                }
                # If the stripe.default_http_client has not been set by the user
                # yet, we'll set it here. This way, we aren't creating a new
                # HttpClient for every request.
                stripe.default_http_client = new_default_http_client(
                    async_fallback_client=new_http_client_async_fallback(
                        **kwargs
                    ),
                    **kwargs,
                )
                _default_proxy = stripe.proxy
            elif stripe.proxy != _default_proxy:
                import warnings

                warnings.warn(
                    "stripe.proxy was updated after sending a "
                    "request - this is a no-op. To use a different proxy, "
                    "set stripe.default_http_client to a new client "
                    "configured with the proxy."
                )

            assert stripe.default_http_client is not None
            return stripe.default_http_client
        return client

    def _new_requestor_with_options(
        self, options: Optional[RequestOptions]
    ) -> "_APIRequestor":
        """
        Returns a new _APIRequestor instance with the same HTTP client but a (potentially) updated set of options. Useful for ensuring the original isn't modified, but any options the original had are still used.
        """
        options = options or {}
        new_options = self._options.to_dict()
        for key in PERSISTENT_OPTIONS_KEYS:
            if key in options and options[key] is not None:
                new_options[key] = options[key]
        return _APIRequestor(
            options=RequestorOptions(**new_options), client=self._client
        )

    @property
    def api_key(self):
        return self._options.api_key

    @property
    def stripe_account(self):
        return self._options.stripe_account

    @property
    def stripe_version(self):
        return self._options.stripe_version

    @property
    def base_addresses(self):
        return self._options.base_addresses

    @classmethod
    def _global_instance(cls):
        """
        Returns the singleton instance of _APIRequestor, to be used when
        calling a static method such as stripe.Customer.create(...)
        """

        # Lazily initialize.
        if cls._instance is None:
            cls._instance = cls(options=_GlobalRequestorOptions(), client=None)
        return cls._instance

    @staticmethod
    def _global_with_options(
        **params: Unpack[RequestOptions],
    ) -> "_APIRequestor":
        return _APIRequestor._global_instance()._new_requestor_with_options(
            params
        )

    @classmethod
    def _format_app_info(cls, info):
        str = info["name"]
        if info["version"]:
            str += "/%s" % (info["version"],)
        if info["url"]:
            str += " (%s)" % (info["url"],)
        return str

    def request(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        options: Optional[RequestOptions] = None,
        *,
        base_address: BaseAddress,
        usage: Optional[List[str]] = None,
    ) -> "StripeObject":
        api_mode = get_api_mode(url)
        requestor = self._new_requestor_with_options(options)
        rbody, rcode, rheaders = requestor.request_raw(
            method.lower(),
            url,
            params,
            is_streaming=False,
            api_mode=api_mode,
            base_address=base_address,
            options=options,
            usage=usage,
        )
        resp = requestor._interpret_response(rbody, rcode, rheaders, api_mode)

        obj = _convert_to_stripe_object(
            resp=resp,
            params=params,
            requestor=requestor,
            api_mode=api_mode,
            is_v2_deleted_object=is_v2_delete_resp(method, api_mode),
        )

        return obj

    async def request_async(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        options: Optional[RequestOptions] = None,
        *,
        base_address: BaseAddress,
        usage: Optional[List[str]] = None,
    ) -> "StripeObject":
        api_mode = get_api_mode(url)
        requestor = self._new_requestor_with_options(options)
        rbody, rcode, rheaders = await requestor.request_raw_async(
            method.lower(),
            url,
            params,
            is_streaming=False,
            api_mode=api_mode,
            base_address=base_address,
            options=options,
            usage=usage,
        )
        resp = requestor._interpret_response(rbody, rcode, rheaders, api_mode)

        obj = _convert_to_stripe_object(
            resp=resp,
            params=params,
            requestor=requestor,
            api_mode=api_mode,
            is_v2_deleted_object=is_v2_delete_resp(method, api_mode),
        )

        return obj

    def request_stream(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        options: Optional[RequestOptions] = None,
        *,
        base_address: BaseAddress,
        usage: Optional[List[str]] = None,
    ) -> StripeStreamResponse:
        api_mode = get_api_mode(url)
        stream, rcode, rheaders = self.request_raw(
            method.lower(),
            url,
            params,
            is_streaming=True,
            api_mode=api_mode,
            base_address=base_address,
            options=options,
            usage=usage,
        )
        resp = self._interpret_streaming_response(
            # TODO: should be able to remove this cast once self._client.request_stream_with_retries
            # returns a more specific type.
            cast(IOBase, stream),
            rcode,
            rheaders,
            api_mode,
        )
        return resp

    async def request_stream_async(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        options: Optional[RequestOptions] = None,
        *,
        base_address: BaseAddress,
        usage: Optional[List[str]] = None,
    ) -> StripeStreamResponseAsync:
        api_mode = get_api_mode(url)
        stream, rcode, rheaders = await self.request_raw_async(
            method.lower(),
            url,
            params,
            is_streaming=True,
            api_mode=api_mode,
            base_address=base_address,
            options=options,
            usage=usage,
        )
        resp = await self._interpret_streaming_response_async(
            stream,
            rcode,
            rheaders,
            api_mode,
        )
        return resp

    def handle_error_response(
        self, rbody, rcode, resp, rheaders, api_mode
    ) -> NoReturn:
        try:
            error_data = resp["error"]
        except (KeyError, TypeError):
            raise error.APIError(
                "Invalid response object from API: %r (HTTP response code "
                "was %d)" % (rbody, rcode),
                rbody,
                rcode,
                resp,
            )

        err = None

        # OAuth errors are a JSON object where `error` is a string. In
        # contrast, in API errors, `error` is a hash with sub-keys. We use
        # this property to distinguish between OAuth and API errors.
        if isinstance(error_data, str):
            err = self.specific_oauth_error(
                rbody, rcode, resp, rheaders, error_data
            )

        if err is None:
            err = (
                self.specific_v2_api_error(
                    rbody, rcode, resp, rheaders, error_data
                )
                if api_mode == "V2"
                else self.specific_v1_api_error(
                    rbody, rcode, resp, rheaders, error_data
                )
            )

        raise err

    def specific_v2_api_error(self, rbody, rcode, resp, rheaders, error_data):
        type = error_data.get("type")
        code = error_data.get("code")
        message = error_data.get("message")
        error_args = {
            "message": message,
            "http_body": rbody,
            "http_status": rcode,
            "json_body": resp,
            "headers": rheaders,
            "code": code,
        }

        log_info(
            "Stripe v2 API error received",
            error_code=code,
            error_type=error_data.get("type"),
            error_message=message,
            error_param=error_data.get("param"),
        )

        if type == "idempotency_error":
            return error.IdempotencyError(
                message,
                rbody,
                rcode,
                resp,
                rheaders,
                code,
            )
        # switchCases: The beginning of the section generated from our OpenAPI spec
        elif type == "temporary_session_expired":
            return error.TemporarySessionExpiredError(**error_args)
        # switchCases: The end of the section generated from our OpenAPI spec

        return self.specific_v1_api_error(
            rbody, rcode, resp, rheaders, error_data
        )

    def specific_v1_api_error(self, rbody, rcode, resp, rheaders, error_data):
        log_info(
            "Stripe v1 API error received",
            error_code=error_data.get("code"),
            error_type=error_data.get("type"),
            error_message=error_data.get("message"),
            error_param=error_data.get("param"),
        )

        # Rate limits were previously coded as 400's with code 'rate_limit'
        if rcode == 429 or (
            rcode == 400 and error_data.get("code") == "rate_limit"
        ):
            return error.RateLimitError(
                error_data.get("message"), rbody, rcode, resp, rheaders
            )
        elif rcode in [400, 404]:
            if error_data.get("type") == "idempotency_error":
                return error.IdempotencyError(
                    error_data.get("message"), rbody, rcode, resp, rheaders
                )
            else:
                return error.InvalidRequestError(
                    error_data.get("message"),
                    error_data.get("param"),
                    error_data.get("code"),
                    rbody,
                    rcode,
                    resp,
                    rheaders,
                )
        elif rcode == 401:
            return error.AuthenticationError(
                error_data.get("message"), rbody, rcode, resp, rheaders
            )
        elif rcode == 402:
            return error.CardError(
                error_data.get("message"),
                error_data.get("param"),
                error_data.get("code"),
                rbody,
                rcode,
                resp,
                rheaders,
            )
        elif rcode == 403:
            return error.PermissionError(
                error_data.get("message"), rbody, rcode, resp, rheaders
            )
        else:
            return error.APIError(
                error_data.get("message"), rbody, rcode, resp, rheaders
            )

    def specific_oauth_error(self, rbody, rcode, resp, rheaders, error_code):
        description = resp.get("error_description", error_code)

        log_info(
            "Stripe OAuth error received",
            error_code=error_code,
            error_description=description,
        )

        args = [error_code, description, rbody, rcode, resp, rheaders]

        if error_code == "invalid_client":
            return oauth_error.InvalidClientError(*args)
        elif error_code == "invalid_grant":
            return oauth_error.InvalidGrantError(*args)
        elif error_code == "invalid_request":
            return oauth_error.InvalidRequestError(*args)
        elif error_code == "invalid_scope":
            return oauth_error.InvalidScopeError(*args)
        elif error_code == "unsupported_grant_type":
            return oauth_error.UnsupportedGrantTypeError(*args)
        elif error_code == "unsupported_response_type":
            return oauth_error.UnsupportedResponseTypeError(*args)

        return None

    def request_headers(
        self, method: HttpVerb, api_mode: ApiMode, options: RequestOptions
    ):
        user_agent = "Stripe/%s PythonBindings/%s" % (
            api_mode.lower(),
            VERSION,
        )
        if stripe.app_info:
            user_agent += " " + self._format_app_info(stripe.app_info)

        ua: Dict[str, Union[str, "AppInfo"]] = {
            "bindings_version": VERSION,
            "lang": "python",
            "publisher": "stripe",
            "httplib": self._get_http_client().name,
        }
        for attr, func in [
            ["lang_version", platform.python_version],
            ["platform", platform.platform],
            ["uname", lambda: " ".join(platform.uname())],
        ]:
            try:
                val = func()
            except Exception:
                val = "(disabled)"
            ua[attr] = val
        if stripe.app_info:
            ua["application"] = stripe.app_info

        headers: Dict[str, str] = {
            "X-Stripe-Client-User-Agent": json.dumps(ua),
            "User-Agent": user_agent,
            "Authorization": "Bearer %s" % (options.get("api_key"),),
        }

        stripe_account = options.get("stripe_account")
        if stripe_account:
            headers["Stripe-Account"] = stripe_account

        stripe_context = options.get("stripe_context")
        if stripe_context and str(stripe_context):
            headers["Stripe-Context"] = str(stripe_context)

        idempotency_key = options.get("idempotency_key")
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key

        # IKs should be set for all POST requests and v2 delete requests
        if method == "post" or (api_mode == "V2" and method == "delete"):
            headers.setdefault("Idempotency-Key", str(uuid.uuid4()))

        if method == "post":
            if api_mode == "V2":
                headers["Content-Type"] = "application/json"
            else:
                headers["Content-Type"] = "application/x-www-form-urlencoded"

        stripe_version = options.get("stripe_version")
        if stripe_version:
            headers["Stripe-Version"] = stripe_version

        return headers

    def _args_for_request_with_retries(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        options: Optional[RequestOptions] = None,
        *,
        base_address: BaseAddress,
        api_mode: ApiMode,
        usage: Optional[List[str]] = None,
    ):
        """
        Mechanism for issuing an API call.  Used by request_raw and request_raw_async.
        """
        request_options = merge_options(self._options, options)

        # Special stripe_version handling for v2 requests:
        if (
            options
            and "stripe_version" in options
            and (options["stripe_version"] is not None)
        ):
            # If user specified an API version, honor it
            request_options["stripe_version"] = options["stripe_version"]

        if request_options.get("api_key") is None:
            raise error.AuthenticationError(
                "No API key provided. (HINT: set your API key using "
                '"stripe.api_key = <API-KEY>"). You can generate API keys '
                "from the Stripe web interface.  See https://stripe.com/api "
                "for details, or email support@stripe.com if you have any "
                "questions."
            )

        abs_url = "%s%s" % (
            self._options.base_addresses.get(base_address),
            url,
        )

        params = params or {}
        if params and (method == "get" or method == "delete"):
            # if we're sending params in the querystring, then we have to make sure we're not
            # duplicating anything we got back from the server already (like in a list iterator)
            # so, we parse the querystring the server sends back so we can merge with what we (or the user) are trying to send
            existing_params = {}
            for k, v in parse_qs(urlsplit(url).query).items():
                # note: server sends back "expand[]" but users supply "expand", so we strip the brackets from the key name
                if k.endswith("[]"):
                    existing_params[k[:-2]] = v
                else:
                    # all querystrings are pulled out as lists.
                    # We want to keep the querystrings that actually are lists, but flatten the ones that are single values
                    existing_params[k] = v[0] if len(v) == 1 else v

            # if a user is expanding something that wasn't expanded before, add (and deduplicate) it
            # this could theoretically work for other lists that we want to merge too, but that doesn't seem to be a use case
            # it never would have worked before, so I think we can start with `expand` and go from there
            if "expand" in existing_params and "expand" in params:
                params["expand"] = list(  # type:ignore - this is a dict
                    set([*existing_params["expand"], *params["expand"]])
                )

            params = {
                **existing_params,
                # user_supplied params take precedence over server params
                **params,
            }

        encoded_params = urlencode(list(_api_encode(params or {})))

        # Don't use strict form encoding by changing the square bracket control
        # characters back to their literals. This is fine by the server, and
        # makes these parameter strings easier to read.
        encoded_params = encoded_params.replace("%5B", "[").replace("%5D", "]")

        if api_mode == "V2":
            encoded_body = json.dumps(
                params or {}, default=_json_encode_date_callback
            )
        else:
            encoded_body = encoded_params

        supplied_headers = None
        if (
            "headers" in request_options
            and request_options["headers"] is not None
        ):
            supplied_headers = dict(request_options["headers"])

        headers = self.request_headers(
            # this cast is safe because the blocks below validate that `method` is one of the allowed values
            cast(HttpVerb, method),
            api_mode,
            request_options,
        )

        if method == "get" or method == "delete":
            if params:
                # if we're sending query params, we've already merged the incoming ones with the server's "url"
                # so we can overwrite the whole thing
                scheme, netloc, path, _, fragment = urlsplit(abs_url)

                abs_url = urlunsplit(
                    (scheme, netloc, path, encoded_params, fragment)
                )
            post_data = None
        elif method == "post":
            if (
                options is not None
                and options.get("content_type") == "multipart/form-data"
            ):
                generator = MultipartDataGenerator()
                generator.add_params(params or {})
                post_data = generator.get_post_data()
                headers["Content-Type"] = (
                    "multipart/form-data; boundary=%s" % (generator.boundary,)
                )
            else:
                post_data = encoded_body
        else:
            raise error.APIConnectionError(
                "Unrecognized HTTP method %r.  This may indicate a bug in the "
                "Stripe bindings.  Please contact support@stripe.com for "
                "assistance." % (method,)
            )

        if supplied_headers is not None:
            for key, value in supplied_headers.items():
                headers[key] = value

        max_network_retries = request_options.get("max_network_retries")

        return (
            # Actual args
            method,
            abs_url,
            headers,
            post_data,
            max_network_retries,
            usage,
            # For logging
            encoded_params,
            request_options.get("stripe_version"),
        )

    def request_raw(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        options: Optional[RequestOptions] = None,
        is_streaming: bool = False,
        *,
        base_address: BaseAddress,
        api_mode: ApiMode,
        usage: Optional[List[str]] = None,
    ) -> Tuple[object, int, Mapping[str, str]]:
        (
            method,
            abs_url,
            headers,
            post_data,
            max_network_retries,
            usage,
            encoded_params,
            api_version,
        ) = self._args_for_request_with_retries(
            method,
            url,
            params,
            options,
            base_address=base_address,
            api_mode=api_mode,
            usage=usage,
        )

        log_info("Request to Stripe api", method=method, url=abs_url)
        log_debug(
            "Post details", post_data=encoded_params, api_version=api_version
        )

        if is_streaming:
            (
                rcontent,
                rcode,
                rheaders,
            ) = self._get_http_client().request_stream_with_retries(
                method,
                abs_url,
                headers,
                post_data,
                max_network_retries=max_network_retries,
                _usage=usage,
            )
        else:
            (
                rcontent,
                rcode,
                rheaders,
            ) = self._get_http_client().request_with_retries(
                method,
                abs_url,
                headers,
                post_data,
                max_network_retries=max_network_retries,
                _usage=usage,
            )

        log_info("Stripe API response", path=abs_url, response_code=rcode)
        log_debug("API response body", body=rcontent)

        if "Request-Id" in rheaders:
            request_id = rheaders["Request-Id"]
            log_debug(
                "Dashboard link for request",
                link=dashboard_link(request_id),
            )

        return rcontent, rcode, rheaders

    async def request_raw_async(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        options: Optional[RequestOptions] = None,
        is_streaming: bool = False,
        *,
        base_address: BaseAddress,
        api_mode: ApiMode,
        usage: Optional[List[str]] = None,
    ) -> Tuple[AsyncIterable[bytes], int, Mapping[str, str]]:
        """
        Mechanism for issuing an API call
        """

        usage = usage or []
        usage = usage + ["async"]

        (
            method,
            abs_url,
            headers,
            post_data,
            max_network_retries,
            usage,
            encoded_params,
            api_version,
        ) = self._args_for_request_with_retries(
            method,
            url,
            params,
            options,
            base_address=base_address,
            api_mode=api_mode,
            usage=usage,
        )

        log_info("Request to Stripe api", method=method, url=abs_url)
        log_debug(
            "Post details",
            post_data=encoded_params,
            api_version=api_version,
        )

        if is_streaming:
            (
                rcontent,
                rcode,
                rheaders,
            ) = await self._get_http_client().request_stream_with_retries_async(
                method,
                abs_url,
                headers,
                post_data,
                max_network_retries=max_network_retries,
                _usage=usage,
            )
        else:
            (
                rcontent,
                rcode,
                rheaders,
            ) = await self._get_http_client().request_with_retries_async(
                method,
                abs_url,
                headers,
                post_data,
                max_network_retries=max_network_retries,
                _usage=usage,
            )

        log_info("Stripe API response", path=abs_url, response_code=rcode)
        log_debug("API response body", body=rcontent)

        if "Request-Id" in rheaders:
            request_id = rheaders["Request-Id"]
            log_debug(
                "Dashboard link for request",
                link=dashboard_link(request_id),
            )

        return rcontent, rcode, rheaders

    def _should_handle_code_as_error(self, rcode: int) -> bool:
        return not 200 <= rcode < 300

    def _interpret_response(
        self,
        rbody: object,
        rcode: int,
        rheaders: Mapping[str, str],
        api_mode: ApiMode,
    ) -> StripeResponse:
        try:
            if hasattr(rbody, "decode"):
                # TODO: should be able to remove this cast once self._client.request_with_retries
                # returns a more specific type.
                rbody = cast(bytes, rbody).decode("utf-8")
            resp = StripeResponse(
                cast(str, rbody),
                rcode,
                rheaders,
            )
        except Exception:
            raise error.APIError(
                "Invalid response body from API: %s "
                "(HTTP response code was %d)" % (rbody, rcode),
                cast(bytes, rbody),
                rcode,
                rheaders,
            )
        if self._should_handle_code_as_error(rcode):
            self.handle_error_response(
                rbody, rcode, resp.data, rheaders, api_mode
            )
        return resp

    def _interpret_streaming_response(
        self,
        stream: IOBase,
        rcode: int,
        rheaders: Mapping[str, str],
        api_mode: ApiMode,
    ) -> StripeStreamResponse:
        # Streaming response are handled with minimal processing for the success
        # case (ie. we don't want to read the content). When an error is
        # received, we need to read from the stream and parse the received JSON,
        # treating it like a standard JSON response.
        if self._should_handle_code_as_error(rcode):
            if hasattr(stream, "getvalue"):
                json_content = cast(BytesIO, stream).getvalue()
            elif hasattr(stream, "read"):
                json_content = stream.read()
            else:
                raise NotImplementedError(
                    "HTTP client %s does not return an IOBase object which "
                    "can be consumed when streaming a response."
                    % self._get_http_client().name
                )

            self._interpret_response(json_content, rcode, rheaders, api_mode)
            # _interpret_response is guaranteed to throw since we've checked self._should_handle_code_as_error
            raise RuntimeError(
                "_interpret_response should have raised an error"
            )
        else:
            return StripeStreamResponse(stream, rcode, rheaders)

    async def _interpret_streaming_response_async(
        self,
        stream: AsyncIterable[bytes],
        rcode: int,
        rheaders: Mapping[str, str],
        api_mode: ApiMode,
    ) -> StripeStreamResponseAsync:
        if self._should_handle_code_as_error(rcode):
            json_content = b"".join([chunk async for chunk in stream])
            self._interpret_response(json_content, rcode, rheaders, api_mode)
            # _interpret_response is guaranteed to throw since we've checked self._should_handle_code_as_error
            raise RuntimeError(
                "_interpret_response should have raised an error"
            )
        else:
            return StripeStreamResponseAsync(stream, rcode, rheaders)
