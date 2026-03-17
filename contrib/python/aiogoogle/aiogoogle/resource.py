__all__ = ["GoogleAPI", "Resource", "Method"]

import re
import warnings
from urllib.parse import urlencode, quote
from functools import wraps
from typing import List, Generic, TypeVar

from .excs import ValidationError
from .utils import _safe_getitem
from .models import MediaDownload, MediaUpload, ResumableUpload, Request
from .validate import validate as validate_


T = TypeVar("T")  # Generic type var

# These are the hard-coded kwargs in Method.__call__
# They're used for testing whether those names will collide with any of the url parameters that are provided by any of the discovery docs.
# If collisions were to be found, that would mean that the user won't be able to pass a url_parameter that shares the same name with any of the RESERVED_KEYWORDS.
RESERVED_KEYWORDS = [
    "validate",
    "data",
    "json",
    "upload_file",
    "pipe_from",
    "download_file",
    "pipe_to",
    "timeout",
]

# From: https://github.com/googleapis/google-api-python-client/blob/master/googleapiclient/discovery.py
# Parameters accepted by the stack, but not visible via discovery.
STACK_QUERY_PARAMETERS = frozenset(["trace", "pp", "strict"])
STACK_QUERY_PARAMETER_DEFAULT_VALUE = {"type": "string", "location": "query"}
MEDIA_SIZE_BIT_SHIFTS = {"KB": 10, "MB": 20, "GB": 30, "TB": 40}

# TODO: etagRequired: {
#    should be type: "boolean",  # noqa: F821 (weird error)
#    description: "Whether this method requires an ETag to be specified. The ETag is sent as an HTTP If-Match or If-None-Match header."
#    }
# NOTE: etagRequired is only mentioned once in all of the discovery documents available from Google. (In discovery_service-v1. So, it isn't actually being used)


def _temporarily_add_back_dashes_to_param_definitions(f):
    """
        When instantiating a Method, Method's constructor will remove all
        dashes from the names of its URI params and global params in order
        to make it possible to pass uri params through function calls
        e.g. this is viable get_videos(my_videos=True)
        this is will fail get_videos(my-videos=True)

        This function momentarily adds back '-' to url parameters and passed uri_params
        in order to be processed and validated correctly and comply with the disc doc
        Reverts back to '_' after wrapped function is done
    """

    @wraps(f)
    def wrapper(
        self,
        validate=None,
        data=None,
        json=None,
        upload_file=None,
        pipe_from=None,
        download_file=None,
        pipe_to=None,
        timeout=None,
        **uri_params,
    ):
        # unfix urls
        uri_params = self._replace_dashes_with_underscores_in_user_provided_params(
            uri_params, self.parameters
        )

        # unfix params
        self._method_specs["parameters"] = self._restore_dashes_to_params(
            self._method_specs.get("parameters")
        )
        self._global_parameters = self._restore_dashes_to_params(
            self._global_parameters
        )

        # Run function
        results = f(
            self,
            validate,
            data,
            json,
            upload_file,
            pipe_from,
            download_file,
            pipe_to,
            timeout,
            **uri_params,
        )

        # fix params again
        self._method_specs[
            "parameters"
        ] = self._replace_dashes_with_underscores_in_params(
            self._method_specs.get("parameters")
        )
        self._global_parameters = self._replace_dashes_with_underscores_in_params(
            self._global_parameters
        )

        return results

    return wrapper


class Method:
    def __init__(
        self,
        name,
        method_specs,
        global_parameters,
        schemas,
        root_url,
        service_path,
        batch_path,
        validate,
    ):
        # Replaces '-'s with '_'s and preserve old names to revert back to them after this method is called
        global_parameters = self._replace_dashes_with_underscores_in_params(
            global_parameters
        )
        method_specs["parameters"] = self._replace_dashes_with_underscores_in_params(
            method_specs.get("parameters")
        )

        self.name = name
        self._method_specs = method_specs
        self._global_parameters = global_parameters
        self._schemas = schemas

        self._root_url = root_url
        self._service_path = service_path
        self._batch_path = batch_path

        if (
            self["useMediaDownloadService"] is True
            and self["supportsMediaDownload"] is True
        ):
            self._download_base_url = self._root_url + "download/" + self._service_path
        else:
            self._download_base_url = None

        self._base_url = self._root_url + self._service_path
        self._batch_url = self._root_url + self._batch_path

        self._should_validate = validate

    # ---- Changes URL parameters with a "-" to "_" -----#
    # Depends on how you view it, but this section also changes robots with small mouths to robots with big mouths

    @staticmethod
    def _replace_dashes_with_underscores_in_params(param_set: Generic[T]) -> T:
        if param_set:
            for name, schema in list(param_set.items()):
                if "-" in name:
                    new_name = name.replace("-", "_")  # See?!
                    schema["orig_name"] = name
                    param_set[new_name] = schema
                    del param_set[name]
        return param_set

    @staticmethod
    def _restore_dashes_to_params(param_set: Generic[T]) -> T:
        if param_set:
            for name, schema in list(param_set.items()):
                if "orig_name" in schema:
                    param_set[schema["orig_name"]] = schema
                    del param_set[name]
        return param_set

    @staticmethod
    def _replace_dashes_with_underscores_in_user_provided_params(
        uri_params: Generic[T], parameters
    ) -> T:
        for k, v in list(uri_params.items()):
            if "_" in k:
                if k in parameters:
                    if "orig_name" in parameters[k]:
                        uri_params[parameters[k]["orig_name"]] = v
                        del uri_params[k]
        return uri_params

    @staticmethod
    def _del_none_params(uri_params: Generic[T]) -> T:
        for k, v in list(uri_params.items()):
            if v is None:
                del uri_params[k]
        return uri_params

    # ---- / Changes URL parameters with a "-" to "_" -----#

    @property
    def request(self) -> dict:
        """ Returns expected request body """
        body = self["request"]
        if body.get("$ref"):
            return self._schemas.get(body["$ref"])
        else:
            return body

    @property
    def response(self) -> dict:
        """ Retruns expected response body """
        body = self["response"]
        if body.get("$ref"):
            return self._schemas.get(body["$ref"])
        else:
            return body

    @property
    def parameters(self) -> dict:
        """
        Parameters property

        Returns:

            dict: All parameters that this method can take as described in the discovery document
        """
        if not self._global_parameters and not self["parameters"]:
            return {}
        elif not self._global_parameters:
            return self["parameters"]
        elif not self["parameters"]:
            return self._global_parameters
        else:
            return {**self["parameters"], **self._global_parameters}

    @property
    def optional_parameters(self) -> List[str]:
        """
        Optional Parameters

        Returns:

            list: List of the names of optional parameters this method takes
        """
        return (
            [
                parameter_name
                for parameter_name, parameter_info in self.parameters.items()
                if parameter_info.get("required") is not True
            ]
            if self.parameters
            else []
        )

    @property
    def required_parameters(self) -> List[str]:
        """
        Required Parameters

        Returns:

            list: List of the names of required parameters this method takes
        """
        return (
            [
                parameter_name
                for parameter_name, parameter_info in self.parameters.items()
                if parameter_info.get("required") is True
            ]
            if self.parameters
            else []
        )

    @property
    def path_parameters(self) -> List[str]:
        """
        Path Parameters

        Returns:

            list: List of the names of path parameters this method takes
        """
        return (
            [
                param_name
                for param_name, param_info in self.parameters.items()
                if param_info.get("location") == "path"
            ]
            if self.parameters
            else []
        )

    @property
    def query_parameters(self) -> List[str]:
        """
        Query Parameters

        Returns:

            list: List of the names of Query parameters this method takes
        """
        return (
            [
                param_name
                for param_name, param_info in self.parameters.items()
                if param_info.get("location") == "query"
            ]
            if self.parameters
            else []
        )

    @property
    def required_query_parameters(self) -> List[str]:
        """
        Required Query Parameters

        Returns:

            list: List of the names of required query parameters this method takes
        """
        return (
            [
                param_name
                for param_name in self.query_parameters
                if param_name in self.required_parameters
            ]
            if self.query_parameters
            else []
        )

    @property
    def optional_query_parameters(self) -> List[str]:
        """
        Optional Query Parameters

        Returns:

            list: List of the names of optional query parameters this method takes
        """
        return (
            [
                param_name
                for param_name in self.query_parameters
                if param_name in self.optional_parameters
            ]
            if self.query_parameters
            else []
        )

    def __getitem__(self, key):
        """
        Examples:

            ::

                >>> self['description']

                "method description"

                >>> self['scopes']

                ['returns', 'scopes', 'required', 'by', 'this', 'method', 'in', 'a', 'list']

                >>> self['supportsMediaDownload']

                False

                >>> self['supportsMediaUpload']

                True

                >>> self['httpMethod']

                'GET'

        Hint:


            Using this method with ``scopes`` as an argument can be useful for incremental authorization. (Requesting scopes when needed. As opposed to requesting them at once)

            for more: https://developers.google.com/identity/protocols/OAuth2WebServer#incrementalAuth


        """
        return self._method_specs.get(key)

    def _validate(self, instance, schema, schema_name=None):
        return validate_(instance, schema, self._schemas, schema_name)

    @_temporarily_add_back_dashes_to_param_definitions
    def __call__(
        self,
        validate=None,
        data=None,
        json=None,
        upload_file=None,
        pipe_from=None,
        download_file=None,
        pipe_to=None,
        timeout=None,
        path_params_safe_chars={},
        **uri_params,
    ) -> Request:
        """
        Builds a request from this method

        Note:

            * When passing ``datetime.datetime or datetime.date`` pass them in json format.

            * Aiogoogle won't do that as it would be a big hassle to iterate over every item in ``*uri_params``, ``json`` and ``data`` to check if there's any datetime objects.

            * Fortunately Python makes it really easy to achieve that.

            * Instead of passing say ``datetime.datetime.utcnow()``, pass: ``datetime.datetime.utcnow().jsonformat()``

        Note:

            * All ``None`` values are ommited before sending to Google apis, if you want to explicitly pass a JSON null then pass it as ``"null"`` not ``None``

        Arguments:

            validate (bool): Overrides :param: aiogoogle.Aiogoogle.validate if not None

            json (dict): Json body

            data (any): Data body (Bytes, text, www-url-form-encoded and others)

            upload_file (str): full path of file to upload

            download_file (str): full path of file to download to

            timeout (str): total timeout for this request

            path_params_safe_chars (dict): Dictionary of safe characters for each path parameter.

            **uri_params (dict): path and query, required and optional parameters

        Returns:

            aiogoogle.models.Request: An unsent request object
        """
        # If collisions are found between the 'key' of **uri_params and explicit kwargs e.g. data, json etc., then
        # priority will be given to explicit kwargs. With that said, it's not likely there will be any.
        # If you want to double check if there are any collisions,
        # you can append the API name and version you're using to tests.globals.ALL_APIS (only if they don't exist, otherwise, you shouldn't worry about collisions)
        # Then, run the unit tests and monitor: tests.test_discovery_document.test_parameters_not_colliding_with_google_api__call__ for failure

        #
        # NOTE: Use '_' instead of '-' when passing uri parameters that have a '-' in their names
        #

        # Remove params that are None
        uri_params = self._del_none_params(uri_params)

        # Assert timeout is int
        if timeout is not None:
            if not isinstance(timeout, int) or isinstance(timeout, bool):  # True isinstanceof int
                raise TypeError("Timeouts can only be ints or None")

        # Resolve validation status
        if not isinstance(validate, bool):
            validate = self._should_validate

        base_url = self._base_url

        # Build full url minus query & fragment
        url = self._build_url(
            base_url=base_url, uri_params=uri_params.copy(), validate=validate, path_params_safe_chars=path_params_safe_chars
        )

        # Filter out query parameters from all uri_params that were passed to this method
        passed_query_params = {
            param_name: param_info
            for param_name, param_info in uri_params.items()
            if param_name in self.query_parameters
        }

        # Ensure all required query parameteters were passed
        for param in self.required_query_parameters:
            if param not in passed_query_params:
                raise ValidationError(f'Missing query parameter: "{param}"')

        # Validate url query params
        if validate is True:
            if passed_query_params:
                for param_name, passed_param in passed_query_params.items():
                    schema = self.parameters[param_name]
                    if schema.get('repeated') and isinstance(passed_param, (list, set, tuple)):
                        for param in passed_param:
                            self._validate(
                                param,
                                schema,
                                schema_name=param_name
                            )
                    else:
                        self._validate(
                            passed_param,
                            schema,
                            schema_name=param_name,
                        )

        # Join query params
        if passed_query_params:
            uri = url + "?" + urlencode(passed_query_params, True)
        else:
            uri = url

        # Pop query uri_params consumed
        for param_name, _ in passed_query_params.items():
            del uri_params[param_name]

        # Pop uri_params consumed
        # for param_name in self["parameterOrder"]:
        for param_name in self.path_parameters:
            if param_name in uri_params:
                del uri_params[param_name]

        # Warn if not all uri_params were consumed/popped
        if uri_params:  # should be empty by now
            # If there's room for addtionalProperties, validate and add them to the URI
            if validate:
                if self.parameters.get("additionalProperties"):
                    for _, v in uri_params.items():
                        self._validate(
                            v,
                            self.parameters["additionalProperties"],
                            schema_name="Additional Url Parameters",
                        )
                else:
                    raise ValidationError(
                        f"Invalid (extra) parameters: {uri_params} were passed"
                    )
            else:
                if not self.parameters.get("additionalProperties"):
                    warnings.warn(
                        "Parameters {} were found and they're probably of no use."
                        " Check if they're valid parameters".format(str(uri_params))
                    )
            if "?" not in uri:
                uri = uri + "?" + urlencode(uri_params)
            else:
                uri = uri + "&" + urlencode(uri_params)

        # Ensure only one param for http req body.
        if json and data:
            raise TypeError(
                "Pass either json or data for the body of the request, not both."
                '\nThis is similar to the "body" argument in google-python-client'
                "\nThis will validate agains the $request key in this method's "
                "specs"
            )  # This raises a TypeError instead of a ValidationError because
            # it will probably make your session raise an error if it passes.
            # Better raise it early on

        # Validate body
        if validate is True:
            body = (
                json if json is not None else data if data is not None else None
            )  # json or data or None
            if body is not None:
                self._validate_body(body)

        # Validate selected options
        if download_file and pipe_to:
            raise ValueError(
                "can't have both (download_file) and (pipe_to) options"
            )

        if not download_file and not pipe_to:
            media_download = None

        # Process download_file
        if download_file:
            if validate is True:
                if self.__getitem__("supportsMediaDownload") is not True:
                    raise ValidationError(
                        "download_file was provided while method doesn't support media download"
                    )
            media_download = MediaDownload(file_path=download_file)

        # Process pipe_to
        if pipe_to:
            if validate is True:
                if self.__getitem__("supportsMediaDownload") is not True:
                    raise ValidationError(
                        "pipe_to was provided while method doesn't support media download"
                    )
            media_download = MediaDownload(pipe_to=pipe_to)

        if not upload_file and not pipe_from:
            media_upload = None

        # Process upload_file
        if upload_file:
            media_upload = self._build_upload_media(
                upload_file, uri, validate, fallback_url=url, pipe_from=None
            )

        # Process pipe_from
        if pipe_from:
            media_upload = self._build_upload_media(
                upload_file, uri, validate, fallback_url=url, pipe_from=pipe_from
            )

        return Request(
            method=self["httpMethod"],
            url=uri,
            batch_url=self._batch_url,
            data=data,
            json=json,
            timeout=timeout,
            media_download=media_download,
            media_upload=media_upload,
            callback=lambda res: res,  # TODO: get rid of this sorcery.
        )

    def _build_url(self, base_url, uri_params, validate, path_params_safe_chars):
        if self.path_parameters:
            # sort path params as sepcified in method_specs.parameterOrder
            sorted_required_path_params = (
                {}
            )  # Dict order is guaranteed (by insertion) as of Python 3.6
            for param_name in self["parameterOrder"]:
                try:
                    sorted_required_path_params[param_name] = uri_params.pop(param_name)
                except KeyError:
                    raise ValidationError(f"Missing URL path parameter: {param_name}")

            # Validate path params
            if validate is True:
                self._validate_url(sorted_required_path_params)

            for k, v in sorted_required_path_params.items():
                if path_params_safe_chars.get(k) is not None:
                    sorted_required_path_params[k] = quote(
                        str(v),
                        safe=path_params_safe_chars[k]
                    )
                else:
                    sorted_required_path_params[k] = quote(str(v))

            # Build full path
            # replace named placeholders with empty ones. e.g. {param} --> {}
            # Why? Because some endpoints have different names in their url path placeholders than in their parameter defenitions
            # e.g. path: {"v1/{+resourceName}/connections"}. e.g. param name: resourceName NOT +resourceName
            self._method_specs["path"] = re.sub(
                r"\{(.*?)\}", r"{}", self._method_specs["path"]
            )
            return base_url + self["path"].format(
                *list(sorted_required_path_params.values())
            )
        else:
            return base_url + self["path"]

    def _build_upload_media(self, upload_file, qualified_url, validate, fallback_url, pipe_from):
        if not self["supportsMediaUpload"]:
            if validate:
                raise ValidationError(
                    "upload_file was provided while method doesn't support media upload"
                )
            # This will probably not work, but will return a mediaupload object anyway
            return MediaUpload(upload_file, upload_path=fallback_url)

        # If resumable, create resumable object
        resumable = (
            self._build_resumeable_media(qualified_url)
            if _safe_getitem(
                self._method_specs, "mediaUpload", "protocols", "resumable"
            )
            else None
        )

        # Create MediaUpload object and pass it the resumable object we just created
        media_upload_url_base = self._root_url + "upload/" + self._service_path
        media_upload_url = qualified_url.replace(self._base_url, media_upload_url_base)
        max_size = self._convert_str_size_to_int(
            _safe_getitem(self._method_specs, "mediaUpload", "maxSize")
        )
        mime_range = _safe_getitem(self._method_specs, "mediaUpload", "accept")
        multipart = self["mediaUpload"]["protocols"]["simple"].get("multipart", True)

        return MediaUpload(
            upload_file,
            pipe_from=pipe_from,
            upload_path=media_upload_url,
            max_size=max_size,
            mime_range=mime_range,
            multipart=multipart,
            resumable=resumable,
            validate=validate,
        )

    def _build_resumeable_media(self, qualified_url):
        resumable_url_base = self._root_url + "resumable/upload/" + self._service_path
        resumable_url = qualified_url.replace(self._base_url, resumable_url_base)
        multipart = self["mediaUpload"]["protocols"]["resumable"].get("multipart", True)
        return ResumableUpload(multipart=multipart, upload_path=resumable_url)

    @staticmethod
    def _convert_str_size_to_int(size):
        """Convert a string media size, such as 10GB or 3TB into an integer.

        Args:
            size: (str): e.g. such as 2MB or 7GB.

        Returns:
            The size as an integer value.
        """
        if size is None:
            return None
        if len(size) < 2:
            return 0
        units = size[-2:].upper()
        bit_shift = MEDIA_SIZE_BIT_SHIFTS.get(units)
        if bit_shift is not None:
            return int(size[:-2]) << bit_shift
        else:
            return int(size)

    def _validate_url(self, sorted_required_path_params):
        for path_param_name, path_param_info in sorted_required_path_params.items():
            self._validate(
                instance=path_param_info,
                schema=self.parameters[path_param_name],
                schema_name=path_param_name,
            )

    def _validate_body(self, req):
        request_schema = self._method_specs.get("request")
        if request_schema is not None:
            schema_name = "Request Body"
            if "$ref" in request_schema:
                schema_name = request_schema["$ref"]
                request_schema = self._schemas[schema_name]
            self._validate(req, request_schema, schema_name=schema_name)
        else:
            raise ValidationError(
                "Request body should've been validated, but wasn't because the method doesn't accept a JSON body"
            )

    def __contains__(self, item):
        return item in self.parameters

    def __str__(self):
        return self["id"] + " method @ " + self._base_url

    def __repr__(self):
        return self.__str__()

    def __len__(self):
        return len(self.required_parameters) if self.required_parameters else 0


class Resource:
    def __init__(
        self,
        name,
        resource_specs,
        global_parameters,
        schemas,
        root_url,
        service_path,
        batch_path,
        validate,
    ):
        self.name = name
        self._resource_specs = resource_specs
        self._global_parameters = global_parameters
        self._schemas = schemas
        self._root_url = root_url
        self._service_path = service_path
        self._batch_path = batch_path
        self._validate = validate

    @property
    def methods_available(self) -> List[str]:
        """
        Returns the names of the methods that this resource provides
        """
        return [k for k, v in self["methods"].items()] if self["methods"] else []

    @property
    def resources_available(self) -> List[str]:
        """
        Returns the names of the nested resources in this resource
        """
        return [k for k, v in self["resources"].items()] if self["resources"] else []

    def _get_resource(self, resource_name):
        return Resource(
            name=resource_name,
            resource_specs=self["resources"][resource_name],
            global_parameters=self._global_parameters,
            schemas=self._schemas,
            root_url=self._root_url,
            service_path=self._service_path,
            batch_path=self._batch_path,
            validate=self._validate,
        )

    def _get_method(self, method_name):
        return Method(
            name=method_name,
            method_specs=self["methods"][method_name],
            global_parameters=self._global_parameters,
            schemas=self._schemas,
            root_url=self._root_url,
            service_path=self._service_path,
            batch_path=self._batch_path,
            validate=self._validate,
        )

    def __str__(self):
        return self.name + " resource @ " + self._root_url + self._service_path

    def __repr__(self):
        return self.__str__()

    def __call__(self):
        raise TypeError(
            "Only methods are callables, not resources."
            " e.g. api.resource.list() NOT api.resource().list()"
        )

    def __len__(self):
        return len(self.methods_available)

    def __contains__(self, item):
        return (item in self.methods_available) or (item in self.resources_available)

    def __getitem__(self, k):
        return self._resource_specs.get(k)

    def __getattr__(self, method_or_resource):
        """
        Returns either a method or a nested resource

        Arguments:

            method_or_resource: Name of the method or resource desired.

        Returns:

            aiogoogle.resource.Resource, aiogoogle.resource.Methods: A Resource or a Method


        Note:

            This method will first check in nested resources then will check in methods.

        Raises:

            AttributeError:
        """
        # 1. Search in nested resources
        if method_or_resource in self.resources_available:
            return self._get_resource(method_or_resource)
        # 2. Search in methods
        elif method_or_resource in self.methods_available:
            return self._get_method(method_or_resource)
        else:
            raise AttributeError(
                f"""Resource/Method {method_or_resource} doesn't exist.
                                 Check: https://developers.google.com/ for more info.
                                 \nAvailable resources are:
                                 {self.resources_available}\n
                                 Available methods are {self.methods_available}"""
            )


class GoogleAPI:
    """
    Creetes a representation of Google API given a discovery document

    Arguments:

        discovery_document (dict): A discovery document

        validate (bool): Set this to True to use this lib's built in parameter validation logic. Note that you shouldn't rely on this for critical user input validation.
    """

    def __init__(self, discovery_document, validate=False):
        self.discovery_document = self._add_extra_query_param_definitions(
            discovery_document
        )
        self._validate = validate

    def _add_extra_query_param_definitions(self, discovery_document):
        """ Adds extra parameters that aren't explicitly defined in discovery docuemnts
            i.e. "trace", "pp", "strict"
        """
        extra_params = {
            param: STACK_QUERY_PARAMETER_DEFAULT_VALUE
            for param in STACK_QUERY_PARAMETERS
        }
        if discovery_document.get("parameters"):
            discovery_document["parameters"] = {
                **discovery_document["parameters"],
                **extra_params,
            }
        else:
            discovery_document["parameters"] = extra_params
        return discovery_document

    @property
    def methods_available(self) -> List[str]:
        """
        Returns names of the methods provided by this resource
        """
        return [k for k, v in self["methods"].items()] if self["methods"] else []

    @property
    def resources_available(self) -> List[str]:
        """
        Returns names of the resources in a given API if any
        """
        return [k for k, v in self["resources"].items()] if self["resources"] else []

    def _get_resource(self, resource_name):
        return Resource(
            name=resource_name,
            resource_specs=self["resources"][resource_name],
            global_parameters=self["parameters"],
            schemas=self["schemas"]
            or {},  # josnschema validator will fail if schemas isn't a dict
            root_url=self["rootUrl"],
            service_path=self["servicePath"],
            batch_path=self["batchPath"],
            validate=self._validate,
        )

    def _get_method(self, method_name):
        return Method(
            name=method_name,
            method_specs=self["methods"][method_name],
            global_parameters=self["parameters"],
            schemas=self["schemas"]
            or {},  # josnschema validator will fail if schemas isn't a dict
            root_url=self["rootUrl"],
            service_path=self["servicePath"],
            batch_path=self["batchPath"],
            validate=self._validate,
        )

    def __getattr__(self, method_or_resource) -> Resource:
        """
        Returns resources from an API

        Note:

            This method will first check in resources then will check in methods.

        Arguments:

            method_or_resource (str): name of the top level method or resource

        Example:

            ::

                >>> google_service = GoogleAPI(google_service_discovery_doc)
                >>> google_service.user

                user resource @ google_service.com/api/v1/

        Returns:

            aiogoogle.resource.Resource, aiogoogle.resource.Method: A Resource or a Method

        Raises:

            AttributeError:
        """
        if method_or_resource in self.resources_available:
            return self._get_resource(method_or_resource)
        elif method_or_resource in self.methods_available:
            return self._get_method(method_or_resource)
        else:
            documentation_link = (
                self.discovery_document.get("documentationLink")
                or "https://developers.google.com/"
            )
            raise AttributeError(
                f"""Resource/Method {method_or_resource} doesn't exist.
                                 Check: {documentation_link} for more info.
                                 \nAvailable resources are:
                                 {self.resources_available}\n
                                 Available methods are {self.methods_available}"""
            )

    def __getitem__(self, k):
        """
        Returns items from the discovery document

        Example:

            ::

                >>> google_service = GoogleAPI(google_books_discovery_doc)
                >>> google_service['name']

                'books'

                >>> google_service['id']

                'books:v1'

                >>> google_service['version']

                'v1'

                >>> google_service['documentationLink']

                'https://developers.google.com/books/docs/v1/getting_started'

                >>> google_service['oauth2']['scopes']

                https://www.googleapis.com/auth/books: {

                    description: "Manage your books"

                }

        Returns:

            dict: Discovery Document Item
        """
        return self.discovery_document.get(k)

    def __contains__(self, name):
        return (name in self.resources_available) or (name in self.methods_available)

    def __repr__(self):
        labels = f'\nLabels:\n{self["labels"]}' if self["labels"] is not None else ""
        return (
            self.discovery_document["name"]
            + "-"
            + self.discovery_document["version"]
            + " API @ "
            + self["rootUrl"]
            + self["servicePath"]
            + labels
        )

    def __str__(self):
        return self.__repr__()

    def __len__(self):
        return len(self.resources_available) + len(self.methods_available)

    def __call__(self):
        raise TypeError(
            "Only methods are callables, not resources."
            " e.g. client.resources.user.list() NOT client.resources().user.list()"
        )
