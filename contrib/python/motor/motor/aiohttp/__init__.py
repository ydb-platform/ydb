# Copyright 2016 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Serve GridFS files with Motor and aiohttp.

Requires Python 3.5 or later and aiohttp 3.0 or later.

See the :doc:`/examples/aiohttp_gridfs_example`.
"""

import datetime
import mimetypes

import aiohttp.web
import gridfs
from motor.motor_asyncio import (AsyncIOMotorDatabase,
                                 AsyncIOMotorGridFSBucket)


def get_gridfs_file(bucket, filename, request):
    """Override to choose a GridFS file to serve at a URL.

    By default, if a URL pattern like ``/fs/{filename}`` is mapped to this
    :class:`AIOHTTPGridFS`, then the filename portion of the URL is used as the
    filename, so a request for "/fs/image.png" results in a call to
    :meth:`.AsyncIOMotorGridFSBucket.open_download_stream_by_name` with
    "image.png" as the ``filename`` argument. To customize the mapping of path
    to GridFS file, override ``get_gridfs_file`` and return a
    :class:`asyncio.Future` that resolves to a
    :class:`~motor.motor_asyncio.AsyncIOMotorGridOut`.

    For example, to retrieve the file by ``_id`` instead of filename::

        def get_gridfile_by_id(bucket, filename, request):
            # "filename" is interpreted as _id instead of name.
            # Return a Future AsyncIOMotorGridOut.
            return bucket.open_download_stream(file_id=filename)

        client = AsyncIOMotorClient()
        gridfs_handler = AIOHTTPGridFS(client.my_database,
                                       get_gridfs_file=get_gridfile_by_id)

    :Parameters:
      - `bucket`: An :class:`~motor.motor_asyncio.AsyncIOMotorGridFSBucket`
      - `filename`: A string, the URL portion matching {filename} in the URL
        pattern
      - `request`: An :class:`aiohttp.web.Request`
    """
    # A Future AsyncIOMotorGridOut.
    return bucket.open_download_stream_by_name(filename)


def get_cache_time(filename, modified, mime_type):
    """Override to customize cache control behavior.

    Return a positive number of seconds to trigger aggressive caching or 0
    to mark resource as cacheable, only. 0 is the default.

    For example, to allow image caching::

        def image_cache_time(filename, modified, mime_type):
            if mime_type.startswith('image/'):
                return 3600

            return 0

        client = AsyncIOMotorClient()
        gridfs_handler = AIOHTTPGridFS(client.my_database,
                                       get_cache_time=image_cache_time)

    :Parameters:
      - `filename`: A string, the URL portion matching {filename} in the URL
        pattern
      - `modified`: A datetime, when the matching GridFS file was created
      - `mime_type`: The file's type, a string like "application/octet-stream"
    """
    return 0


def set_extra_headers(response, gridout):
    """Override to modify the response before sending to client.

    For example, to allow image caching::

        def gzip_header(response, gridout):
            response.headers['Content-Encoding'] = 'gzip'

        client = AsyncIOMotorClient()
        gridfs_handler = AIOHTTPGridFS(client.my_database,
                                       set_extra_headers=gzip_header)

    :Parameters:
      - `response`: An :class:`aiohttp.web.Response`
      - `gridout`: The :class:`~motor.motor_asyncio.AsyncIOMotorGridOut` we
        will serve to the client
    """
    pass


def _config_error(request):
    try:
        formatter = request.match_info.route.resource.get_info()['formatter']
        msg = ('Bad AIOHTTPGridFS route "%s", requires a {filename} variable' %
               formatter)
    except (KeyError, AttributeError):
        # aiohttp API changed? Fall back to simpler error message.
        msg = ('Bad AIOHTTPGridFS route for request: %s' % request)

    raise aiohttp.web.HTTPInternalServerError(text=msg) from None


class AIOHTTPGridFS:
    """Serve files from `GridFS`_.

    This class is a :ref:`request handler <aiohttp-web-handler>` that serves
    GridFS files, similar to aiohttp's built-in static file server.

    .. code-block:: python

        client = AsyncIOMotorClient()
        gridfs_handler = AIOHTTPGridFS(client.my_database)

        app = aiohttp.web.Application()

        # The GridFS URL pattern must have a "{filename}" variable.
        resource = app.router.add_resource('/fs/{filename}')
        resource.add_route('GET', gridfs_handler)
        resource.add_route('HEAD', gridfs_handler)

        app_handler = app.make_handler()
        server = loop.create_server(app_handler, port=80)

    By default, requests' If-Modified-Since headers are honored, but no
    specific cache-control timeout is sent to clients. Thus each request for
    a GridFS file requires a quick check of the file's ``uploadDate`` in
    MongoDB. Pass a custom :func:`get_cache_time` to customize this.

    :Parameters:
      - `database`: An :class:`AsyncIOMotorDatabase`
      - `get_gridfs_file`: Optional override for :func:`get_gridfs_file`
      - `get_cache_time`: Optional override for :func:`get_cache_time`
      - `set_extra_headers`: Optional override for :func:`set_extra_headers`

    .. _GridFS: https://docs.mongodb.com/manual/core/gridfs/
    """

    def __init__(self,
                 database,
                 root_collection='fs',
                 get_gridfs_file=get_gridfs_file,
                 get_cache_time=get_cache_time,
                 set_extra_headers=set_extra_headers):
        if not isinstance(database, AsyncIOMotorDatabase):
            raise TypeError("First argument to AIOHTTPGridFS must be "
                            "AsyncIOMotorDatabase, not %r" % database)

        self._database = database
        self._bucket = AsyncIOMotorGridFSBucket(self._database, root_collection)
        self._get_gridfs_file = get_gridfs_file
        self._get_cache_time = get_cache_time
        self._set_extra_headers = set_extra_headers

    async def __call__(self, request):
        """Send filepath to client using request."""
        try:
            filename = request.match_info['filename']
        except KeyError:
            _config_error(request)

        if request.method not in ('GET', 'HEAD'):
            raise aiohttp.web.HTTPMethodNotAllowed(
                method=request.method, allowed_methods={'GET', 'HEAD'})

        try:
            gridout = await self._get_gridfs_file(self._bucket,
                                                  filename,
                                                  request)
        except gridfs.NoFile:
            raise aiohttp.web.HTTPNotFound(text=request.path)

        resp = aiohttp.web.StreamResponse()
        self._set_standard_headers(request.path, resp, gridout)

        # Overridable method set_extra_headers.
        self._set_extra_headers(resp, gridout)

        # Check the If-Modified-Since, and don't send the result if the
        # content has not been modified
        ims_value = request.if_modified_since
        if ims_value is not None:
            # If our MotorClient is tz-aware, assume the naive ims_value is in
            # its time zone.
            if_since = ims_value.replace(tzinfo=gridout.upload_date.tzinfo)
            modified = gridout.upload_date.replace(microsecond=0)
            if if_since >= modified:
                resp.set_status(304)
                return resp

        # Same for Etag
        etag = request.headers.get("If-None-Match")
        if etag is not None and etag.strip('"') == gridout.md5:
            resp.set_status(304)
            return resp

        resp.content_length = gridout.length
        await resp.prepare(request)

        if request.method == 'GET':
            written = 0
            while written < gridout.length:
                # Reading chunk_size at a time minimizes buffering.
                chunk = await gridout.read(gridout.chunk_size)
                await resp.write(chunk)
                written += len(chunk)
        return resp

    def _set_standard_headers(self, path, resp, gridout):
        resp.last_modified = gridout.upload_date
        content_type = gridout.content_type
        if content_type is None:
            content_type, encoding = mimetypes.guess_type(path)

        if content_type:
            resp.content_type = content_type

        # MD5 is calculated on the MongoDB server when GridFS file is created.
        resp.headers["Etag"] = '"%s"' % gridout.md5

        # Overridable method get_cache_time.
        cache_time = self._get_cache_time(path,
                                          gridout.upload_date,
                                          gridout.content_type)

        if cache_time > 0:
            resp.headers["Expires"] = (
                datetime.datetime.utcnow() +
                datetime.timedelta(seconds=cache_time)
            ).strftime("%a, %d %b %Y %H:%M:%S GMT")

            resp.headers["Cache-Control"] = "max-age=" + str(cache_time)
        else:
            resp.headers["Cache-Control"] = "public"
