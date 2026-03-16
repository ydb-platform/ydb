"""
(c) 2017 DigitalOcean

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import concurrent.futures as cf
import io
import os
import json

from packaging import version

# NetBox v2 token prefix (introduced in NetBox 4.5.0)
TOKEN_PREFIX = "nbt_"


def _is_v2_token(token):
    """Detect if a token is NetBox v2 format.

    V2 tokens (introduced in NetBox 4.5.0) have the format: nbt_<id>.<token>
    The nbt_ prefix is used for secrets detection.

    V1 tokens are simple strings without dots.

    Returns True if token is v2 format, False otherwise.
    """
    if not token or not token.startswith(TOKEN_PREFIX):
        return False

    # Remove nbt_ prefix
    token_body = token[len(TOKEN_PREFIX) :]

    # V2 tokens contain a dot separating the ID from the secret
    return "." in token_body


def _is_file_like(obj):
    if isinstance(obj, (str, bytes)):
        return False
    # Check if it's a standard library IO object OR has a callable read method
    return isinstance(obj, io.IOBase) or (
        hasattr(obj, "read") and callable(getattr(obj, "read"))
    )


def _extract_files(data):
    """Extract file-like objects from data dict.

    Returns a tuple of (clean_data, files) where clean_data has file objects
    removed and files is a dict suitable for requests' files parameter.
    """
    if not isinstance(data, dict):
        return data, None

    files = {}
    clean_data = {}

    for key, value in data.items():
        if _is_file_like(value):
            # Format: (filename, file_obj, content_type)
            # Try to get filename from file object, fallback to key
            filename = getattr(value, "name", None)
            if filename:
                # Extract just the filename, not the full path
                filename = os.path.basename(filename)
            else:
                filename = key
            files[key] = (filename, value)
        elif isinstance(value, tuple) and len(value) >= 2 and _is_file_like(value[1]):
            # Already in (filename, file_obj) or (filename, file_obj, content_type) format
            files[key] = value
        else:
            clean_data[key] = value

    return clean_data, files if files else None


def calc_pages(limit, count):
    """Calculate number of pages required for full results set."""
    return int(count / limit) + (limit % count > 0)


class RequestError(Exception):
    """Basic Request Exception.

    More detailed exception that returns the original requests object
    for inspection. Along with some attributes with specific details
    from the requests object. If return is json we decode and add it
    to the message.

    ## Examples

    ```python
    try:
        nb.dcim.devices.create(name="destined-for-failure")
    except pynetbox.RequestError as e:
        print(e.error)
    ```
    """

    def __init__(self, req):
        if req.status_code == 404:
            self.message = "The requested url: {} could not be found.".format(req.url)
        else:
            try:
                self.message = "The request failed with code {} {}: {}".format(
                    req.status_code, req.reason, req.json()
                )
            except ValueError:
                self.message = (
                    "The request failed with code {} {} but more specific "
                    "details were not returned in json. Check the NetBox Logs "
                    "or investigate this exception's error attribute.".format(
                        req.status_code, req.reason
                    )
                )

        super().__init__(self.message)
        self.req = req
        self.request_body = req.request.body
        self.base = req.url
        self.error = req.text

    def __str__(self):
        return self.message


class AllocationError(Exception):
    """Allocation Exception.

    Used with available-ips/available-prefixes when there is no
    room for allocation and NetBox returns 409 Conflict.
    """

    def __init__(self, req):
        super().__init__(req)
        self.req = req
        self.request_body = req.request.body
        self.base = req.url
        self.error = "The requested allocation could not be fulfilled."

    def __str__(self):
        return self.error


class ContentError(Exception):
    """Content Exception.

    If the API URL does not point to a valid NetBox API, the server may
    return a valid response code, but the content is not json. This
    exception is raised in those cases.
    """

    def __init__(self, req):
        super().__init__(req)
        self.req = req
        self.request_body = req.request.body
        self.base = req.url
        self.error = (
            "The server returned invalid (non-json) data. Maybe not a NetBox server?"
        )

    def __str__(self):
        return self.error


class ParameterValidationError(Exception):
    """API parameter validation Exception.

    Raised when filter parameters do not match Netbox OpenAPI specification.

    ## Examples

    ```python
    try:
        nb.dcim.devices.filter(field_which_does_not_exist="destined-for-failure")
    except pynetbox.ParameterValidationError as e:
        print(e.error)
    ```
    """

    def __init__(self, errors):
        super().__init__(errors)
        self.error = f"The request parameter validation returned an error: {errors}"

    def __str__(self):
        return self.error


class Request:
    """Creates requests to the Netbox API.

    Responsible for building the url and making the HTTP(S) requests to
    Netbox's API.

    ## Parameters

    * **base** (str): Base URL passed in api() instantiation.
    * **filters** (dict, optional): Contains key/value pairs that
        correlate to the filters a given endpoint accepts.
        In (e.g. /api/dcim/devices/?name='test') 'name': 'test'
        would be in the filters dict.
    """

    def __init__(
        self,
        base,
        http_session,
        filters=None,
        limit=None,
        offset=None,
        key=None,
        token=None,
        threading=False,
        expect_json=True,
    ):
        """Instantiates a new Request object.

        ## Parameters

        * **base** (string): Base URL passed in api() instantiation.
        * **filters** (dict, optional): Contains key/value pairs that
            correlate to the filters a given endpoint accepts.
            In (e.g. /api/dcim/devices/?name='test') 'name': 'test'
            would be in the filters dict.
        * **key** (int, optional): Database id of the item being queried.
        * **expect_json** (bool, optional): If True, expects JSON response
            and sets appropriate Accept header. If False, expects raw content
            (e.g., SVG, XML) and returns text. Defaults to True.

        ## Note

        The `count` attribute is not initialized here. It is set dynamically
        by the `get()` method when paginating results, or by `get_count()`
        when explicitly requesting the count. This allows `get_count()` to
        use `hasattr()` to determine if a count has already been fetched.
        """
        self.base = self.normalize_url(base)
        self.filters = filters or None
        self.key = key
        self.token = token
        self.http_session = http_session
        self.url = self.base if not key else "{}{}/".format(self.base, key)
        self.threading = threading
        self.limit = limit
        self.offset = offset
        self.expect_json = expect_json

    def get_openapi(self):
        """Gets the OpenAPI Spec."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        current_version = version.parse(self.get_version())
        if current_version >= version.parse("3.5"):
            req = self.http_session.get(
                "{}schema/".format(self.normalize_url(self.base)),
                headers=headers,
            )
        else:
            req = self.http_session.get(
                "{}docs/?format=openapi".format(self.normalize_url(self.base)),
                headers=headers,
            )

        if req.ok:
            return req.json()
        else:
            raise RequestError(req)

    def get_version(self):
        """Gets the API version of NetBox.

        Issues a GET request to the base URL to read the API version from the
        response headers.

        ## Returns
        Version number as a string. Empty string if version is not
        present in the headers.

        ## Raises
        RequestError if req.ok returns false.
        """
        headers = {"Content-Type": "application/json"}
        self._add_auth_header(headers)
        req = self.http_session.get(
            self.normalize_url(self.base),
            headers=headers,
        )
        if req.ok or req.status_code == 403:
            return req.headers.get("API-Version", "")
        else:
            raise RequestError(req)

    def get_status(self):
        """Gets the status from /api/status/ endpoint in NetBox.

        ## Returns
        Dictionary as returned by NetBox.

        ## Raises
        RequestError if request is not successful.
        """
        headers = {"Content-Type": "application/json"}
        self._add_auth_header(headers)
        req = self.http_session.get(
            "{}status/".format(self.normalize_url(self.base)),
            headers=headers,
        )
        if req.ok:
            return req.json()
        else:
            raise RequestError(req)

    def normalize_url(self, url):
        """Builds a url for POST actions."""
        if url[-1] != "/":
            return "{}/".format(url)

        return url

    def _add_auth_header(self, headers):
        """Add authorization header to headers dict if token is present.

        ## Parameters
        * **headers** (dict): Headers dictionary to update with authorization.
        """
        if self.token:
            if _is_v2_token(self.token):
                headers["authorization"] = "Bearer {}".format(self.token)
            else:
                headers["authorization"] = "Token {}".format(self.token)

    def _make_call(self, verb="get", url_override=None, add_params=None, data=None):
        # Extract any file-like objects from data
        files = None
        # Verbs that support request bodies with file uploads
        body_verbs = ("post", "put", "patch")

        # Set Accept header based on expected response type
        if self.expect_json:
            headers = {"accept": "application/json"}
        else:
            headers = {"accept": "*/*"}

        # Extract files from data for applicable verbs
        if data is not None and verb in body_verbs:
            data, files = _extract_files(data)

        # Set headers based on request type
        should_be_json_body = not files and (
            verb in body_verbs or (verb == "delete" and data)
        )

        if should_be_json_body:
            headers["Content-Type"] = "application/json"

        self._add_auth_header(headers)

        params = {}
        if not url_override:
            if self.filters:
                params.update(self.filters)
            if add_params:
                params.update(add_params)

        if files:
            # Use multipart/form-data for file uploads
            req = getattr(self.http_session, verb)(
                url_override or self.url,
                headers=headers,
                params=params,
                data=data,
                files=files,
            )
        else:
            req = getattr(self.http_session, verb)(
                url_override or self.url, headers=headers, params=params, json=data
            )

        if req.status_code == 409 and verb == "post":
            raise AllocationError(req)
        if verb == "delete":
            if req.ok:
                return True
            else:
                raise RequestError(req)
        elif req.ok:
            # Parse response based on expected type
            if self.expect_json:
                try:
                    return req.json()
                except json.JSONDecodeError:
                    raise ContentError(req)
            else:
                # Return raw text for non-JSON responses
                return req.text
        else:
            raise RequestError(req)

    def concurrent_get(self, ret, page_size, page_offsets):
        futures_to_results = []
        with cf.ThreadPoolExecutor(max_workers=4) as pool:
            for offset in page_offsets:
                new_params = {"offset": offset, "limit": page_size}
                futures_to_results.append(
                    pool.submit(self._make_call, add_params=new_params)
                )

            for future in cf.as_completed(futures_to_results):
                result = future.result()
                ret.extend(result["results"])

    def get(self, add_params=None):
        """Makes a GET request.

        Makes a GET request to NetBox's API, and automatically recurses
        any paginated results.

        ## Returns
        List of `Response` objects returned from the endpoint.

        ## Raises
        * RequestError if req.ok returns false.
        * ContentError if response is not json.
        """

        if not add_params and self.limit is not None:
            add_params = {"limit": self.limit}
            if self.limit and self.offset is not None:
                # if non-zero limit and some offset -> add offset
                add_params["offset"] = self.offset
        req = self._make_call(add_params=add_params)
        if isinstance(req, dict) and req.get("results") is not None:
            self.count = req["count"]
            if self.offset is not None:
                # only yield requested page results if paginating
                for i in req["results"]:
                    yield i
            elif self.threading:
                ret = req["results"]
                if req.get("next"):
                    page_size = len(req["results"])
                    pages = calc_pages(page_size, req["count"])
                    page_offsets = [
                        increment * page_size for increment in range(1, pages)
                    ]
                    if pages == 1:
                        req = self._make_call(url_override=req.get("next"))
                        ret.extend(req["results"])
                    else:
                        self.concurrent_get(ret, page_size, page_offsets)
                for i in ret:
                    yield i
            else:
                first_run = True
                for i in req["results"]:
                    yield i
                while req["next"]:
                    # Not worrying about making sure add_params kwargs is
                    # passed in here because results from detail routes aren't
                    # paginated, thus far.
                    if first_run:
                        req = self._make_call(
                            add_params={
                                "limit": self.limit or req["count"],
                                "offset": len(req["results"]),
                            }
                        )
                    else:
                        req = self._make_call(url_override=req["next"])
                    first_run = False
                    for i in req["results"]:
                        yield i
        elif isinstance(req, list):
            self.count = len(req)
            for i in req:
                yield i
        else:
            self.count = len(req)
            yield req

    def put(self, data):
        """Makes PUT request.

        Makes a PUT request to NetBox's API.

        ## Parameters
        * **data** (dict): Contains a dict that will be turned into a
            json object and sent to the API.

        ## Returns
        Dict containing the response from NetBox's API.

        ## Raises
        * RequestError if req.ok returns false.
        * ContentError if response is not json.
        """
        return self._make_call(verb="put", data=data)

    def post(self, data):
        """Makes POST request.

        Makes a POST request to NetBox's API.

        ## Parameters
        * **data** (dict): Contains a dict that will be turned into a
            json object and sent to the API.

        ## Returns
        Dict containing the response from NetBox's API.

        ## Raises
        * RequestError if req.ok returns false.
        * AllocationError if req.status_code is 409 (Conflict)
            as with available-ips and available-prefixes when there is
            no room for the requested allocation.
        * ContentError if response is not json.
        """
        return self._make_call(verb="post", data=data)

    def delete(self, data=None):
        """Makes DELETE request.

        Makes a DELETE request to NetBox's API.

        ## Parameters
        * **data** (list): Contains a dict that will be turned into a
            json object and sent to the API.

        ## Returns
        True if successful.

        ## Raises
        RequestError if req.ok doesn't return True.
        """
        return self._make_call(verb="delete", data=data)

    def patch(self, data):
        """Makes PATCH request.

        Makes a PATCH request to NetBox's API.

        ## Parameters
        * **data** (dict): Contains a dict that will be turned into a
            json object and sent to the API.

        ## Returns
        Dict containing the response from NetBox's API.

        ## Raises
        * RequestError if req.ok returns false.
        * ContentError if response is not json.
        """
        return self._make_call(verb="patch", data=data)

    def options(self):
        """Makes an OPTIONS request.

        Makes an OPTIONS request to NetBox's API.

        ## Returns
        Dict containing the response from NetBox's API.

        ## Raises
        * RequestError if req.ok returns false.
        * ContentError if response is not json.
        """
        return self._make_call(verb="options")

    def get_count(self, *args, **kwargs):
        """Returns object count for query.

        Makes a query to the endpoint with ``limit=1`` set and only
        returns the value of the "count" field.

        ## Returns
        Int of number of objects query returned.

        ## Raises
        * RequestError if req.ok returns false.
        * ContentError if response is not json.
        """

        if not hasattr(self, "count"):
            self.count = self._make_call(add_params={"limit": 1, "brief": 1})["count"]
        return self.count
