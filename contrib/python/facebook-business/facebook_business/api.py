# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.session import FacebookSession
from facebook_business import apiconfig

from facebook_business.exceptions import (
    FacebookRequestError,
    FacebookBadObjectError,
    FacebookUnavailablePropertyException,
    FacebookBadParameterError,
)
from facebook_business.utils import api_utils
from facebook_business.utils import urls

from contextlib import contextmanager
import copy
from six.moves import http_client
import os
import json
import six
import re

try:
  # Since python 3
  from six.moves import collections_abc
except ImportError:
  # Won't work after python 3.8
  import collections as collections_abc

from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.typechecker import TypeChecker


"""
api module contains classes that make http requests to Facebook's graph API.
"""


class FacebookResponse(object):

    """Encapsulates an http response from Facebook's Graph API."""

    def __init__(self, body=None, http_status=None, headers=None, call=None):
        """Initializes the object's internal data.
        Args:
            body (optional): The response body as text.
            http_status (optional): The http status code.
            headers (optional): The http headers.
            call (optional): The original call that was made.
        """
        self._body = body
        self._http_status = http_status
        self._headers = headers or {}
        self._call = call

    def body(self):
        """Returns the response body."""
        return self._body

    def json(self):
        """Returns the response body -- in json if possible."""
        try:
            return json.loads(self._body)
        except (TypeError, ValueError):
            return self._body

    def headers(self):
        """Return the response headers."""
        return self._headers

    def etag(self):
        """Returns the ETag header value if it exists."""
        return self._headers.get('ETag')

    def status(self):
        """Returns the http status code of the response."""
        return self._http_status

    def is_success(self):
        """Returns boolean indicating if the call was successful."""

        json_body = self.json()

        if isinstance(json_body, collections_abc.Mapping) and 'error' in json_body:
            # Is a dictionary, has error in it
            return False
        elif bool(json_body):
            # Has body and no error
            if 'success' in json_body:
                return json_body['success']
            # API can return a success 200 when service unavailable occurs
            return 'Service Unavailable' not in json_body
        elif self._http_status == http_client.NOT_MODIFIED:
            # ETAG Hit
            return True
        elif self._http_status == http_client.OK:
            # HTTP Okay
            return True
        else:
            # Something else
            return False

    def is_failure(self):
        """Returns boolean indicating if the call failed."""
        return not self.is_success()

    def error(self):
        """
        Returns a FacebookRequestError (located in the exceptions module) with
        an appropriate debug message.
        """
        if self.is_failure():
            return FacebookRequestError(
                "Call was not successful",
                self._call,
                self.status(),
                self.headers(),
                self.body(),
            )
        else:
            return None


class FacebookAdsApi(object):

    """Encapsulates session attributes and methods to make API calls.
    Attributes:
        SDK_VERSION (class): indicating sdk version.
        HTTP_METHOD_GET (class): HTTP GET method name.
        HTTP_METHOD_POST (class): HTTP POST method name
        HTTP_METHOD_DELETE (class): HTTP DELETE method name
        HTTP_DEFAULT_HEADERS (class): Default HTTP headers for requests made by
            this sdk.
    """

    SDK_VERSION = apiconfig.ads_api_config['SDK_VERSION']

    API_VERSION = apiconfig.ads_api_config['API_VERSION']

    HTTP_METHOD_GET = 'GET'

    HTTP_METHOD_POST = 'POST'

    HTTP_METHOD_DELETE = 'DELETE'

    HTTP_DEFAULT_HEADERS = {
        'User-Agent': "fbbizsdk-python-%s" % SDK_VERSION,
    }

    _default_api = None
    _default_account_id = None

    def __init__(self, session, api_version=None, enable_debug_logger=False):
        """Initializes the api instance.
        Args:
            session: FacebookSession object that contains a requests interface
                and attribute GRAPH (the Facebook GRAPH API URL).
            api_version: API version
        """
        self._session = session
        self._num_requests_succeeded = 0
        self._num_requests_attempted = 0
        self._api_version = api_version or self.API_VERSION
        self._enable_debug_logger = enable_debug_logger

    def get_num_requests_attempted(self):
        """Returns the number of calls attempted."""
        return self._num_requests_attempted

    def get_num_requests_succeeded(self):
        """Returns the number of calls that succeeded."""
        return self._num_requests_succeeded

    @classmethod
    def init(
        cls,
        app_id=None,
        app_secret=None,
        access_token=None,
        account_id=None,
        api_version=None,
        proxies=None,
        timeout=None,
        debug=False,
        crash_log=True,
    ):
        session = FacebookSession(app_id, app_secret, access_token, proxies,
                                  timeout)
        api = cls(session, api_version, enable_debug_logger=debug)
        cls.set_default_api(api)

        if account_id:
            cls.set_default_account_id(account_id)

        if crash_log:
            from facebook_business.crashreporter import CrashReporter
            if debug:
                CrashReporter.enableLogging()
            CrashReporter.enable()

        return api

    @classmethod
    def set_default_api(cls, api_instance):
        """Sets the default api instance.
        When making calls to the api, objects will revert to using the default
        api if one is not specified when initializing the objects.
        Args:
            api_instance: The instance which to set as default.
        """
        cls._default_api = api_instance

    @classmethod
    def get_default_api(cls):
        """Returns the default api instance."""
        return cls._default_api

    @classmethod
    def set_default_account_id(cls, account_id):
        account_id = str(account_id)
        if account_id.find('act_') == -1:
            raise ValueError(
                "Account ID provided in FacebookAdsApi.set_default_account_id "
                "expects a string that begins with 'act_'",
            )
        cls._default_account_id = account_id

    @classmethod
    def get_default_account_id(cls):
        return cls._default_account_id

    def call(
        self,
        method,
        path,
        params=None,
        headers=None,
        files=None,
        url_override=None,
        api_version=None,
    ):
        """Makes an API call.
        Args:
            method: The HTTP method name (e.g. 'GET').
            path: A tuple of path tokens or a full URL string. A tuple will
                be translated to a url as follows:
                graph_url/tuple[0]/tuple[1]...
                It will be assumed that if the path is not a string, it will be
                iterable.
            params (optional): A mapping of request parameters where a key
                is the parameter name and its value is a string or an object
                which can be JSON-encoded.
            headers (optional): A mapping of request headers where a key is the
                header name and its value is the header value.
            files (optional): An optional mapping of file names to binary open
                file objects. These files will be attached to the request.
        Returns:
            A FacebookResponse object containing the response body, headers,
            http status, and summary of the call that was made.
        Raises:
            FacebookResponse.error() if the request failed.
        """
        if not params:
            params = {}
        if not headers:
            headers = {}
        if not files:
            files = {}

        api_version = api_version or self._api_version

        if api_version and not re.search(r'v[0-9]+\.[0-9]+', api_version):
            raise FacebookBadObjectError(
                'Please provide the API version in the following format: %s'
                % self.API_VERSION,
            )

        self._num_requests_attempted += 1

        if not isinstance(path, six.string_types):
            # Path is not a full path
            path = "/".join((
                url_override or self._session.GRAPH,
                api_version,
                '/'.join(map(str, path)),
            ))

        # Include api headers in http request
        headers = headers.copy()
        headers.update(FacebookAdsApi.HTTP_DEFAULT_HEADERS)

        if params:
            params = _top_level_param_json_encode(params)

        # Get request response and encapsulate it in a FacebookResponse
        if method in ('GET', 'DELETE'):
            response = self._session.requests.request(
                method,
                path,
                params=params,
                headers=headers,
                files=files,
                timeout=self._session.timeout
            )

        else:
            response = self._session.requests.request(
                method,
                path,
                data=params,
                headers=headers,
                files=files,
                timeout=self._session.timeout
            )
        if self._enable_debug_logger:
            import curlify
            print(curlify.to_curl(response.request))
        fb_response = FacebookResponse(
            body=response.text,
            headers=response.headers,
            http_status=response.status_code,
            call={
                'method': method,
                'path': path,
                'params': params,
                'headers': headers,
                'files': files,
            },
        )

        if fb_response.is_failure():
            raise fb_response.error()

        self._num_requests_succeeded += 1
        return fb_response

    def new_batch(self):
        """
        Returns a new FacebookAdsApiBatch, which when executed will go through
        this api.
        """
        return FacebookAdsApiBatch(api=self)


class FacebookAdsApiBatch(object):

    """
    Exposes methods to build a sequence of calls which can be executed with
    a single http request.
    Note: Individual exceptions won't be thrown for each call that fails.
        The success and failure callback functions corresponding to a call
        should handle its success or failure.
    """

    def __init__(self, api, success=None, failure=None):
        self._api = api
        self._files = []
        self._batch = []
        self._success_callbacks = []
        self._failure_callbacks = []
        if success is not None:
            self._success_callbacks.append(success)
        if failure is not None:
            self._failure_callbacks.append(failure)
        self._requests = []

    def __len__(self):
        return len(self._batch)

    def add(
        self,
        method,
        relative_path,
        params=None,
        headers=None,
        files=None,
        success=None,
        failure=None,
        request=None,
    ):
        """Adds a call to the batch.
        Args:
            method: The HTTP method name (e.g. 'GET').
            relative_path: A tuple of path tokens or a relative URL string.
                A tuple will be translated to a url as follows:
                    <graph url>/<tuple[0]>/<tuple[1]>...
                It will be assumed that if the path is not a string, it will be
                iterable.
            params (optional): A mapping of request parameters where a key
                is the parameter name and its value is a string or an object
                which can be JSON-encoded.
            headers (optional): A mapping of request headers where a key is the
                header name and its value is the header value.
            files (optional): An optional mapping of file names to binary open
                file objects. These files will be attached to the request.
            success (optional): A callback function which will be called with
                the FacebookResponse of this call if the call succeeded.
            failure (optional): A callback function which will be called with
                the FacebookResponse of this call if the call failed.
            request (optional): The APIRequest object
        Returns:
            A dictionary describing the call.
        """
        if not isinstance(relative_path, six.string_types):
            relative_url = '/'.join(relative_path)
        else:
            relative_url = relative_path

        call = {
            'method': method,
            'relative_url': relative_url,
        }

        if params:
            params = _top_level_param_json_encode(params)
            keyvals = ['%s=%s' % (key, urls.quote_with_encoding(value))
                       for key, value in params.items()]
            if method == 'GET':
                call['relative_url'] += '?' + '&'.join(keyvals)
            else:
                call['body'] = '&'.join(keyvals)

        if files:
            call['attached_files'] = ','.join(files.keys())

        if headers:
            call['headers'] = []
            for header in headers:
                batch_formatted_header = {}
                batch_formatted_header['name'] = header
                batch_formatted_header['value'] = headers[header]
                call['headers'].append(batch_formatted_header)

        self._batch.append(call)
        self._files.append(files)
        self._success_callbacks.append(success)
        self._failure_callbacks.append(failure)
        self._requests.append(request)

        return call

    def add_request(
        self,
        request,
        success=None,
        failure=None,
    ):
        """Interface to add a APIRequest to the batch.
        Args:
            request: The APIRequest object to add
            success (optional): A callback function which will be called with
                the FacebookResponse of this call if the call succeeded.
            failure (optional): A callback function which will be called with
                the FacebookResponse of this call if the call failed.
            Returns:
                A dictionary describing the call.
        """
        updated_params = copy.deepcopy(request._params)
        if request._fields:
            updated_params['fields'] = ','.join(request._fields)
        return self.add(
            method=request._method,
            relative_path=request._path,
            params=updated_params,
            files=request._file_params,
            success=success,
            failure=failure,
            request=request,
        )

    def execute(self):
        """Makes a batch call to the api associated with this object.
        For each individual call response, calls the success or failure callback
        function if they were specified.
        Note: Does not explicitly raise exceptions. Individual exceptions won't
        be thrown for each call that fails. The success and failure callback
        functions corresponding to a call should handle its success or failure.
        Returns:
            If some of the calls have failed, returns  a new FacebookAdsApiBatch
            object with those calls. Otherwise, returns None.
        """
        if not self._batch:
            return None
        method = 'POST'
        path = tuple()
        params = {'batch': self._batch}
        files = {}
        for call_files in self._files:
            if call_files:
                files.update(call_files)

        fb_response = self._api.call(
            method,
            path,
            params=params,
            files=files,
        )

        responses = fb_response.json()
        retry_indices = []

        for index, response in enumerate(responses):
            if response:
                body = response.get('body')
                code = response.get('code')
                headers = response.get('headers')

                inner_fb_response = FacebookResponse(
                    body=body,
                    headers=headers,
                    http_status=code,
                    call=self._batch[index],
                )

                if inner_fb_response.is_success():
                    if self._success_callbacks[index]:
                        self._success_callbacks[index](inner_fb_response)
                elif self._failure_callbacks[index]:
                    self._failure_callbacks[index](inner_fb_response)
            else:
                retry_indices.append(index)

        if retry_indices:
            new_batch = self.__class__(self._api)
            new_batch._files = [self._files[index] for index in retry_indices]
            new_batch._batch = [self._batch[index] for index in retry_indices]
            new_batch._success_callbacks = [self._success_callbacks[index]
                                            for index in retry_indices]
            new_batch._failure_callbacks = [self._failure_callbacks[index]
                                            for index in retry_indices]
            return new_batch
        else:
            return None


class FacebookRequest:
    """
    Represents an API request
    """

    def __init__(
        self,
        node_id,
        method,
        endpoint,
        api=None,
        param_checker=TypeChecker({}, {}),
        target_class=None,
        api_type=None,
        allow_file_upload=False,
        response_parser=None,
        include_summary=True,
        api_version=None,
    ):
        """
        Args:
            node_id: The node id to perform the api call.
            method: The HTTP method of the call.
            endpoint: The edge of the api call.
            api (optional): The FacebookAdsApi object.
            param_checker (optional): Parameter checker.
            target_class (optional): The return class of the api call.
            api_type (optional): NODE or EDGE type of the call.
            allow_file_upload (optional): Whether the call allows upload.
            response_parser (optional): An ObjectParser to parse response.
            include_summary (optional): Include "summary".
            api_version (optional): API version.
        """
        self._api = api or FacebookAdsApi.get_default_api()
        self._node_id = node_id
        self._method = method
        self._endpoint = endpoint.replace('/', '')
        self._path = (node_id, endpoint.replace('/', ''))
        self._param_checker = param_checker
        self._target_class = target_class
        self._api_type = api_type
        self._allow_file_upload = allow_file_upload
        self._response_parser = response_parser
        self._include_summary = include_summary
        self._api_version = api_version
        self._params = {}
        self._fields = []
        self._file_params = {}
        self._file_counter = 0
        self._accepted_fields = []
        if target_class is not None:
            self._accepted_fields = target_class.Field.__dict__.values()

    def add_file(self, file_path):
        if not self._allow_file_upload:
            api_utils.warning('Endpoint ' + self._endpoint + ' cannot upload files')
        file_key = 'source' + str(self._file_counter)
        if os.path.isfile(file_path):
            self._file_params[file_key] = file_path
            self._file_counter += 1
        else:
            raise FacebookBadParameterError(
                'Cannot find file ' + file_path + '!',
            )
        return self

    def add_files(self, files):
        if files is None:
            return self
        for file_path in files:
            self.add_file(file_path)
        return self

    def add_field(self, field):
        if field not in self._fields:
            self._fields.append(field)
        if field not in self._accepted_fields:
            api_utils.warning(self._endpoint + ' does not allow field ' + field)
        return self

    def add_fields(self, fields):
        if fields is None:
            return self
        for field in fields:
            self.add_field(field)
        return self

    def add_param(self, key, value):
        if not self._param_checker.is_valid_pair(key, value):
            api_utils.warning('value of ' + key + ' might not be compatible. ' +
                ' Expect ' + self._param_checker.get_type(key) + '; ' +
                ' got ' + str(type(value)))
        if self._param_checker.is_file_param(key):
            self._file_params[key] = value
        else:
            self._params[key] = self._extract_value(value)
        return self

    def add_params(self, params):
        if params is None:
            return self
        for key in params.keys():
            self.add_param(key, params[key])
        return self

    def get_fields(self):
        return list(self._fields)

    def get_params(self):
        return copy.deepcopy(self._params)

    def execute(self):
        params = copy.deepcopy(self._params)
        if self._api_type == "EDGE" and self._method == "GET":
            cursor = Cursor(
                target_objects_class=self._target_class,
                params=params,
                fields=self._fields,
                include_summary=self._include_summary,
                api=self._api,
                node_id=self._node_id,
                endpoint=self._endpoint,
                object_parser=self._response_parser,
            )
            cursor.load_next_page()
            return cursor
        if self._fields:
            params['fields'] = ','.join(self._fields)
        with open_files(self._file_params) as files:
            response = self._api.call(
                method=self._method,
                path=(self._path),
                params=params,
                files=files,
                api_version=self._api_version,
            )
            if response.error():
                raise response.error()
            if self._response_parser:
                return self._response_parser.parse_single(response.json())
            else:
                return response

    def add_to_batch(self, batch, success=None, failure=None):
        batch.add_request(self, success, failure)

    def _extract_value(self, value):
        if hasattr(value, 'export_all_data'):
            return value.export_all_data()
        elif isinstance(value, list):
            return [self._extract_value(item) for item in value]
        elif isinstance(value, dict):
            return dict((self._extract_value(k), self._extract_value(v))
                for (k, v) in value.items())
        else:
            return value


class Cursor(object):

    """Cursor is an cursor over an object's connections.
        Previously called EdgeIterator.
    Examples:
        >>> me = AdAccountUser('me')
        >>> my_accounts = [act for act in Cursor(me, AdAccount)]
        >>> my_accounts
        [<AdAccount act_abc>, <AdAccount act_xyz>]
    """

    def __init__(
        self,
        source_object=None,
        target_objects_class=None,
        fields=None,
        params=None,
        include_summary=True,
        api=None,
        node_id=None,
        endpoint=None,
        object_parser=None
    ):
        """
        Initializes an cursor over the objects to which there is an edge from
        source_object.
        To initialize, you'll need to provide either (source_object and
        target_objects_class) or (api, node_id, endpoint, and object_parser)
        Args:
            source_object: An AbstractObject instance from which to inspect an
                edge. This object should have an id.
            target_objects_class: Objects traversed over will be initialized
                with this AbstractObject class.
            fields (optional): A list of fields of target_objects_class to
                automatically read in.
            params (optional): A mapping of request parameters where a key
                is the parameter name and its value is a string or an object
                which can be JSON-encoded.
            include_summary (optional): Include summary.
            api (optional): FacebookAdsApi object.
            node_id (optional): The ID of calling node.
            endpoint (optional): The edge name.
            object_parser (optional): The ObjectParser to parse response.
        """
        self.params = dict(params or {})
        target_objects_class._assign_fields_to_params(fields, self.params)
        self._source_object = source_object
        self._target_objects_class = target_objects_class
        self._node_id = node_id or source_object.get_id_assured()
        self._endpoint = endpoint or target_objects_class.get_endpoint()
        self._api = api or source_object.get_api()
        self._path = (
            self._node_id,
            self._endpoint,
        )
        self._queue = []
        self._headers = []
        self._finished_iteration = False
        self._total_count = None
        self._summary = None
        self._include_summary = include_summary or 'default_summary' in self.params
        self._object_parser = object_parser or ObjectParser(
            api=self._api,
            target_class=self._target_objects_class,
        )

    def __repr__(self):
        return str(self._queue)

    def __len__(self):
        return len(self._queue)

    def __iter__(self):
        return self

    def __next__(self):
        # Load next page at end.
        # If load_next_page returns False, raise StopIteration exception
        if not self._queue and not self.load_next_page():
            raise StopIteration()

        return self._queue.pop(0)

    # Python 2 compatibility.
    next = __next__

    def __getitem__(self, index):
        return self._queue[index]

    def headers(self):
        return self._headers

    def total(self):
        if self._total_count is None:
            raise FacebookUnavailablePropertyException(
                "Couldn't retrieve the object total count for that type "
                "of request.",
            )
        return self._total_count

    def summary(self):
        if self._summary is None or not isinstance(self._summary, dict):
            raise FacebookUnavailablePropertyException(
                "Couldn't retrieve the object summary for that type "
                "of request.",
            )
        return "<Summary> %s" % (
            json.dumps(
                self._summary,
                sort_keys=True,
                indent=4,
                separators=(',', ': '),
            ),
        )

    def load_next_page(self):
        """Queries server for more nodes and loads them into the internal queue.
        Returns:
            True if successful, else False.
        """
        if self._finished_iteration:
            return False

        if (
            self._include_summary and
            'default_summary' not in self.params and
            'summary' not in self.params
        ):
            self.params['summary'] = True

        response_obj = self._api.call(
            'GET',
            self._path,
            params=self.params,
        )
        response = response_obj.json()
        self._headers = response_obj.headers()

        if (
            'paging' in response and
            'cursors' in response['paging'] and
            'after' in response['paging']['cursors'] and
            # 'after' will always exist even if no more pages are available
            'next' in response['paging']
        ):
            self.params['after'] = response['paging']['cursors']['after']
        else:
            # Indicate if this was the last page
            self._finished_iteration = True

        if (
            self._include_summary and
            'summary' in response and
            'total_count' in response['summary']
        ):
            self._total_count = response['summary']['total_count']

        if self._include_summary and 'summary' in response:
            self._summary = response['summary']

        self._queue = self.build_objects_from_response(response)
        return len(self._queue) > 0

    def get_one(self):
        for obj in self:
            return obj
        return None

    def build_objects_from_response(self, response):
        return self._object_parser.parse_multiple(response)


@contextmanager
def open_files(files):
    opened_files = {}
    for key, path in files.items():
        opened_files.update({key: open(path, 'rb')})
    yield opened_files
    for file in opened_files.values():
        file.close()


def _top_level_param_json_encode(params):
    params = params.copy()

    for param, value in params.items():
        if (
            isinstance(value, (collections_abc.Mapping, collections_abc.Sequence, bool))
            and not isinstance(value, six.string_types)
        ):
            params[param] = json.dumps(
                value,
                sort_keys=True,
                separators=(',', ':'),
            )
        else:
            params[param] = value

    return params
