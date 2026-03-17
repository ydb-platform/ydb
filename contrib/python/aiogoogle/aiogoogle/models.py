from urllib.parse import urlparse, urlunparse, urlencode, parse_qs
from typing import Any, AsyncIterable, Callable, Optional
import pprint

from .excs import HTTPError, AuthError, ValidationError

DEFAULT_DOWNLOAD_CHUNK_SIZE = 1024 * 1024
DEFAULT_UPLOAD_CHUNK_SIZE = 1024 * 1024


class ResumableUpload:
    """
    Resumable Upload Object. Works in conjuction with media upload

    Arguments:

        file_path (str): Full path of the file to be uploaded

        upload_path (str): The URI path to be used for upload. Should be used in conjunction with the rootURL property at the API-level.

        multipart (bool): True if this endpoint supports upload multipart media.

        chunk_size (int): Size of a chunk of bytes that a session should read at a time when uploading in multipart.

    """

    def __init__(self, multipart=None, chunk_size=None, upload_path=None):
        self.upload_path = upload_path
        self.multipart = multipart
        self.chunk_size = chunk_size or DEFAULT_UPLOAD_CHUNK_SIZE


class MediaUpload:
    """

    Media Upload

    Arguments:

        file_path_or_bytes (str, bytes): Full path or content of the file to be uploaded

        upload_path (str): The URI path to be used for upload. Should be used in conjunction with the rootURL property at the API-level.

        mime_range (list): list of MIME Media Ranges for acceptable media uploads to this method.

        max_size (int): Maximum size of a media upload in bytes

        multipart (bool): True if this endpoint supports upload multipart media.

        chunksize (int): Size of a chunk of bytes that a session should read at a time when uploading in multipart.

        resumable (aiogoogle.models.ResumableUplaod): A ResumableUpload object

        validate (bool): Whether or not a session should validate the upload size before sending

        pipe_from (file object, AsyncIterable): class object to stream file content from
    """

    def __init__(
        self,
        file_path_or_bytes,
        upload_path=None,
        mime_range=None,
        max_size=None,
        multipart=False,
        chunk_size=None,
        resumable=None,
        validate=True,
        pipe_from=None
    ):
        if isinstance(file_path_or_bytes, bytes):
            self.file_body = file_path_or_bytes
            self.file_path = None
        else:
            self.file_body = None
            self.file_path = file_path_or_bytes
        self.upload_path = upload_path
        self.mime_range = mime_range
        self.max_size = max_size
        self.multipart = multipart
        self.chunk_size = chunk_size or DEFAULT_UPLOAD_CHUNK_SIZE
        self.resumable = resumable
        self.validate = validate
        self.pipe_from = pipe_from

    async def run_validation(self, size_func):
        if self.validate and self.max_size:
            size = await size_func(self.file_path) if self.file_path else len(self.file_body)
            if size > self.max_size:
                raise ValidationError(
                    f'"{self}" has a size of {size / 1000}KB. '
                    f'Max upload size for this endpoint is: '
                    f'{self.max_size / 1000}KB.'
                )

    async def aiter_file(self, aiter_func):
        if self.file_path:
            async for chunk in aiter_func(self.file_path, self.chunk_size):
                yield chunk
        elif self.pipe_from:
            if isinstance(self.pipe_from, AsyncIterable):
                async for chunk in self.pipe_from:
                    yield chunk
            else:
                yield self.pipe_from.read()
        else:
            async for chunk in self._aiter_body():
                yield chunk

    async def _aiter_body(self):
        for x in range(0, len(self.file_body), self.chunk_size):
            yield self.file_body[x:x + self.chunk_size]

    async def read_file(self, read_func):
        return self.file_body or await read_func(self.file_path)

    def __str__(self):
        return self.file_path or "File object"


class MediaDownload:
    """
    Media Download

    Arguments:

        file_path (str): Full path of the file to be downloaded

        chunksize (int): Size of a chunk of bytes that a session should write at a time when downloading.

        pipe_to (object): class object to stream file content to

    """

    def __init__(self, file_path=None, chunk_size=None, pipe_to=None):
        self.file_path = file_path
        self.pipe_to = pipe_to
        self.chunk_size = chunk_size or DEFAULT_DOWNLOAD_CHUNK_SIZE


class Request:
    """
    Request class for the whole library. Auth Managers, GoogleAPI and Sessions should all use this.

    .. note::

        For HTTP body, only pass one of the following params:

            - json: json as a dict
            - data: www-url-form-encoded form as a dict/ bytes/ text/


    Parameters:

        method (str): HTTP method as a string (upper case) e.g. 'GET'

        url (str): full url as a string. e.g. 'https://example.com/api/v1/resource?filter=filter#something

        batch_url (str): full url of for sending this request in a batch

        json (dict): json as a dict

        data (any): www-url-form-encoded form as a dict/ bytes/ text/

        headers (dict): headers as a dict

        media_download (aiogoogle.models.MediaDownload): MediaDownload object

        media_upload (aiogoogle.models.MediaUpload): MediaUpload object

        timeout (int): Individual timeout for this request

        callback (callable): Synchronous callback that takes the content of the response as the only argument. Should also return content.

        _verify_ssl (boolean): Defaults to True.

        upload_file_content_type (str): Optional content-type header string. In case you don't want to use the default application/octet-stream (Or whatever is auto-detected by your transport handler)
        
        """

    def __init__(
        self,
        method: Optional[str] = None,
        url: Optional[str] = None,
        batch_url: Optional[str] = None,
        headers: Optional[dict] = None,
        json: Optional[dict] = None,
        data: Any = None,
        media_upload: Optional[MediaUpload] = None,
        media_download: Optional[MediaDownload] = None,
        timeout: Optional[int] = None,
        callback: Optional[Callable] = None,
        _verify_ssl: bool = True,
        upload_file_content_type: Optional[str] = None,
    ):
        self.method = method
        self.url = url
        self.batch_url = batch_url
        self.headers = {} if headers is None else headers
        self.data = data
        self.json = json
        self.media_upload = media_upload
        self.media_download = media_download
        self.timeout = timeout
        self.callback = callback
        self._verify_ssl = _verify_ssl
        self.upload_file_content_type = upload_file_content_type

    def _add_query_param(self, query: dict):
        url = self.url
        if "?" not in url:
            if url.endswith("/"):
                url = url[:-1]
            url += "?"
        else:
            url += "&"
        query = urlencode(query)
        url += query
        self.url = url

    def _rm_query_param(self, name: str):
        u = urlparse(self.url)
        query = parse_qs(u.query)
        query.pop(name, None)
        u = u._replace(query=urlencode(query, True))
        self.url = urlunparse(u)

    @classmethod
    def batch_requests(cls, *requests):
        """
        Given many requests, will create a batch request per https://developers.google.com/discovery/v1/batch

        Arguments:

            *requests (aiogoogle.models.Request): Request objects

        Returns:

            aiogoogle.models.Request:
        """
        raise NotImplementedError

    @classmethod
    def from_response(cls, response):
        return Request(
            url=response.url,
            headers=response.headers,
            json=response.json,
            data=response.data,
        )


class Response:
    """
    Respnse Object

    Arguments:

        status_code (int): HTTP Status code

        headers (dict): HTTP response headers

        url (str): Request URL

        json (dict): Json Response if any

        data (any): data

        reason (str): reason for http error if any

        req (aiogoogle.models.Request): request that caused this response

        download_file (str): path of the download file specified in the request

        pipe_to (object): class object to stream file content to specified in the request.

        upload_file (str): path of the upload file specified in the request

        pipe_from (file object): class object to stream file content from

        session_factory (aiogoogle.sessions.abc.AbstractSession): A callable implementation of aiogoogle's session interface

        auth_manager (aiogoogle.auth.managers.ServiceAccountManager): Service account authorization manager.

        user_creds (aiogoogle.auth.creds.UserCreds): user_creds to make an api call with.
    """

    def __init__(
        self,
        status_code: int,
        headers=None,
        url=None,
        json=None,
        data=None,
        reason=None,
        req=None,
        download_file=None,
        pipe_to=None,
        upload_file=None,
        pipe_from=None,
        session_factory=None,
        auth_manager=None,
        user_creds=None
    ):
        if json and data:
            raise TypeError("Pass either json or data, not both.")

        self.status_code = status_code
        self.headers = headers
        self.url = url
        self.json = json
        self.data = data
        self.reason = reason
        self.req = req
        self.download_file = download_file
        self.pipe_to = pipe_to
        self.upload_file = upload_file
        self.pipe_from = pipe_from
        self.session_factory = session_factory
        self.auth_manager = auth_manager
        # Used for refreshing tokens for the Oauth2 authentication workflow.
        self.user_creds = user_creds

    @staticmethod
    async def _next_page_generator(
        prev_res,
        session_factory,
        req_token_name=None,
        res_token_name=None,
        json_req=False,
    ):
        from .auth.managers import ServiceAccountManager, Oauth2Manager
        prev_url = None
        while prev_res is not None:

            # Avoid infinite looping if google sent the same token twice
            if prev_url == prev_res.req.url:
                break
            prev_url = prev_res.req.url

            # yield
            yield prev_res.content

            # get request for next page
            next_req = prev_res.next_page(
                req_token_name=req_token_name,
                res_token_name=res_token_name,
                json_req=json_req,
            )
            if next_req is not None:
                async with session_factory() as sess:
                    user_creds = None

                    if isinstance(prev_res.auth_manager, (ServiceAccountManager, Oauth2Manager)):
                        is_refreshed = False
                        user_creds = None

                        if isinstance(prev_res.auth_manager, ServiceAccountManager):
                            is_refreshed = await prev_res.auth_manager.refresh()
                            if is_refreshed:
                                prev_res.auth_manager.authorize(next_req)
                        else:
                            is_refreshed, user_creds = await prev_res.auth_manager.refresh(prev_res.user_creds)
                            if is_refreshed and user_creds:
                                prev_res.auth_manager.authorize(next_req, user_creds=user_creds)

                    prev_res = await sess.send(next_req, full_res=True, auth_manager=prev_res.auth_manager, user_creds=user_creds)
            else:
                prev_res = None

    def __call__(
        self,
        session_factory=None,
        req_token_name=None,
        res_token_name=None,
        json_req=False,
    ):
        """
        Returns a generator that yields the contents of the next pages if any (and this page as well)

        Arguments:

            session_factory (aiogoogle.sessions.abc.AbstractSession): A session factory

            req_token_name (str):

                * name of the next_page token in the request

                * Default: "pageToken"

            res_token_name (str):

                * name of the next_page token in json response

                * Default: "nextPageToken"

            json_req (dict): Normally, nextPageTokens should be sent in URL query params. If you want it in A json body, set this to True

        Returns:

            async generator: self._next_page_generator (staticmethod)
        """
        if session_factory is None:
            session_factory = self.session_factory
        return self._next_page_generator(
            self, session_factory, req_token_name, res_token_name, json_req
        )

    def __aiter__(self):
        return self._next_page_generator(self, self.session_factory)

    def __iter__(self):
        raise TypeError(
            'You probably forgot to use an "async for" statement instead of just a "for" statement.'
        )

    @property
    def content(self):
        """
        Equals either ``self.json`` or ``self.data``
        """
        return self.json or self.data

    def next_page(
        self, req_token_name=None, res_token_name=None, json_req=False
    ) -> Request:
        """
        Method that returns a request object that requests the next page of a resource

        Arguments:

            req_token_name (str):

                * name of the next_page token in the request

                * Default: "pageToken"

            res_token_name (str):

                * name of the next_page token in json response

                * Default: "nextPageToken"

            json_req (dict): Normally, nextPageTokens should be sent in URL query params. If you want it in A json body, set this to True

        Returns:

            A request object (aiogoogle.models.Request):
        """
        if req_token_name is None:
            req_token_name = "pageToken"
        if res_token_name is None:
            res_token_name = "nextPageToken"
        res_token = self.json.get(res_token_name, None)
        if res_token == "":
            res_token = None
        if res_token is None:
            return None
        # request = Request.from_response(self)
        request = self.req
        if json_req:
            request.json[req_token_name] = res_token
        else:
            request._rm_query_param(req_token_name)
            request._add_query_param({req_token_name: res_token})
        return request

    @property
    def error_msg(self):
        if self.json is not None and self.json.get("error") is not None:
            return pprint.pformat(self.json["error"])

    def raise_for_status(self):
        if self.status_code >= 400:
            if self.error_msg is not None:
                self.reason = "\n\n" + self.reason + "\n\nContent:\n" + self.error_msg
            self.reason = "\n\n" + self.reason + "\n\nRequest URL:\n" + self.req.url
            if self.status_code == 401:
                raise AuthError(msg=self.reason, req=self.req, res=self)
            else:
                raise HTTPError(msg=self.reason, req=self.req, res=self)

    def __str__(self):
        return str(self.content)

    def __repr__(self):
        return f"Aiogoogle response model. Status: {str(self.status_code)}"
