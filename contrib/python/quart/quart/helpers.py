from __future__ import annotations

import mimetypes
import os
import pkgutil
import sys
from datetime import datetime, timedelta
from functools import lru_cache, wraps
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union
from urllib.parse import quote
from zlib import adler32

from werkzeug.exceptions import NotFound
from werkzeug.routing import BuildError
from werkzeug.wrappers import Response as WerkzeugResponse

from .ctx import _app_ctx_stack, _request_ctx_stack, _websocket_ctx_stack
from .globals import current_app, request, session, websocket
from .signals import message_flashed
from .typing import FilePath
from .utils import file_path_to_path
from .wrappers import Response
from .wrappers.response import ResponseBody

DEFAULT_MIMETYPE = "application/octet-stream"

locked_cached_property = property


def get_debug_flag() -> bool:
    """Reads QUART_DEBUG environment variable to determine whether to run
    the app in debug mode. If unset, and development mode has been
    configured, it will be enabled automatically.
    """
    value = os.getenv("QUART_DEBUG", None)

    if value is None:
        return "development" == get_env()

    return value.lower() not in {"0", "false", "no"}


def get_env(default: Optional[str] = "production") -> str:
    """Reads QUART_ENV environment variable to determine in which environment
    the app is running on. Defaults to 'production' when unset.
    """
    return os.getenv("QUART_ENV", default)


async def make_response(*args: Any) -> Union[Response, WerkzeugResponse]:
    """Create a response, a simple wrapper function.

    This is most useful when you want to alter a Response before
    returning it, for example

    .. code-block:: python

        response = make_response(render_template('index.html'))
        response.headers['X-Header'] = 'Something'

    """
    if not args:
        return current_app.response_class("")
    if len(args) == 1:
        args = args[0]

    return await current_app.make_response(args)


async def make_push_promise(path: str) -> None:
    """Create a push promise, a simple wrapper function.

    This takes a path that should be pushed to the client if the
    protocol is HTTP/2.

    """
    return await request.send_push_promise(path)


async def flash(message: str, category: str = "message") -> None:
    """Add a message (with optional category) to the session store.

    This is typically used to flash a message to a user that will be
    stored in the session and shown during some other request. For
    example,

    .. code-block:: python

        @app.route('/login', methods=['POST'])
        async def login():
            ...
            await flash('Login successful')
            return redirect(url_for('index'))

    allows the index route to show the flashed messages, without
    having to accept the message as an argument or otherwise.  See
    :func:`~quart.helpers.get_flashed_messages` for message retrieval.
    """
    flashes = session.get("_flashes", [])
    flashes.append((category, message))
    session["_flashes"] = flashes
    await message_flashed.send(
        current_app._get_current_object(), message=message, category=category  # type: ignore
    )


def get_flashed_messages(
    with_categories: bool = False, category_filter: Iterable[str] = ()
) -> Union[List[str], List[Tuple[str, str]]]:
    """Retrieve the flashed messages stored in the session.

    This is mostly useful in templates where it is exposed as a global
    function, for example

    .. code-block:: html+jinja

        <ul>
        {% for message in get_flashed_messages() %}
          <li>{{ message }}</li>
        {% endfor %}
        </ul>

    Note that caution is required for usage of ``category_filter`` as
    all messages will be popped, but only those matching the filter
    returned. See :func:`~quart.helpers.flash` for message creation.
    """
    flashes = _request_ctx_stack.top.flashes
    if flashes is None:
        _request_ctx_stack.top.flashes = flashes = (
            session.pop("_flashes") if "_flashes" in session else []
        )
    if category_filter:
        flashes = [flash for flash in flashes if flash[0] in category_filter]
    if not with_categories:
        flashes = [flash[1] for flash in flashes]
    return flashes


def get_template_attribute(template_name: str, attribute: str) -> Any:
    """Load a attribute from a template.

    This is useful in Python code in order to use attributes in
    templates.

    Arguments:
        template_name: To load the attribute from.
        attribute: The attribute name to load
    """
    return getattr(current_app.jinja_env.get_template(template_name).module, attribute)


def url_for(
    endpoint: str,
    *,
    _anchor: Optional[str] = None,
    _external: Optional[bool] = None,
    _method: Optional[str] = None,
    _scheme: Optional[str] = None,
    **values: Any,
) -> str:
    """Return the url for a specific endpoint.

    This is most useful in templates and redirects to create a URL
    that can be used in the browser.

    Arguments:
        endpoint: The endpoint to build a url for, if prefixed with
            ``.`` it targets endpoint's in the current blueprint.
        _anchor: Additional anchor text to append (i.e. #text).
        _external: Return an absolute url for external (to app) usage.
        _method: The method to consider alongside the endpoint.
        _scheme: A specific scheme to use.
        values: The values to build into the URL, as specified in
            the endpoint rule.
    """
    app_context = _app_ctx_stack.top
    request_context = _request_ctx_stack.top
    websocket_context = _websocket_ctx_stack.top

    if request_context is not None:
        url_adapter = request_context.url_adapter
        if endpoint.startswith("."):
            if request.blueprint is not None:
                endpoint = request.blueprint + endpoint
            else:
                endpoint = endpoint[1:]
        if _external is None:
            _external = False
    elif websocket_context is not None:
        url_adapter = websocket_context.url_adapter
        if endpoint.startswith("."):
            if websocket.blueprint is not None:
                endpoint = websocket.blueprint + endpoint
            else:
                endpoint = endpoint[1:]
        if _external is None:
            _external = False
    elif app_context is not None:
        url_adapter = app_context.url_adapter
        if _external is None:
            _external = True
    else:
        raise RuntimeError("Cannot create a url outside of an application context")

    if url_adapter is None:
        raise RuntimeError(
            "Unable to create a url adapter, try setting the SERVER_NAME config variable."
        )
    if _scheme is not None and not _external:
        raise ValueError("External must be True for scheme usage")

    app_context.app.inject_url_defaults(endpoint, values)

    old_scheme = None
    if _scheme is not None:
        old_scheme = url_adapter.url_scheme
        url_adapter.url_scheme = _scheme

    try:
        url = url_adapter.build(endpoint, values, method=_method, force_external=_external)
    except BuildError as error:
        return app_context.app.handle_url_build_error(error, endpoint, values)
    finally:
        if old_scheme is not None:
            url_adapter.url_scheme = old_scheme

    if _anchor is not None:
        quoted_anchor = quote(_anchor)
        url = f"{url}#{quoted_anchor}"
    return url


def stream_with_context(func: Callable) -> Callable:
    """Share the current request context with a generator.

    This allows the request context to be accessed within a streaming
    generator, for example,

    .. code-block:: python

        @app.route('/')
        def index() -> AsyncGenerator[bytes, None]:
            @stream_with_context
            async def generator() -> bytes:
                yield request.method.encode()
                yield b' '
                yield request.path.encode()

            return generator()

    """
    request_context = _request_ctx_stack.top.copy()

    @wraps(func)
    async def generator(*args: Any, **kwargs: Any) -> Any:
        async with request_context:
            async for data in func(*args, **kwargs):
                yield data

    return generator


def find_package(name: str) -> Tuple[Optional[Path], Path]:
    """Finds packages install prefix (or None) and it's containing Folder"""
    module = name.split(".")[0]
    loader = pkgutil.get_loader(module)
    if name == "__main__" or loader is None:
        package_path = Path.cwd()
    else:
        if hasattr(loader, "get_filename"):
            filename = loader.get_filename(module)  # type: ignore
        else:
            __import__(name)
            filename = sys.modules[name].__file__
        package_path = Path(filename).resolve().parent
        if hasattr(loader, "is_package"):
            is_package = loader.is_package(module)  # type: ignore
            if is_package:
                package_path = Path(package_path).resolve().parent
    sys_prefix = Path(sys.prefix).resolve()
    try:
        package_path.relative_to(sys_prefix)
    except ValueError:
        return None, package_path
    else:
        return sys_prefix, package_path


def safe_join(directory: FilePath, *paths: FilePath) -> Path:
    """Safely join the paths to the known directory to return a full path.

    Raises:
        NotFound: if the full path does not share a commonprefix with
        the directory.
    """
    try:
        safe_path = file_path_to_path(directory).resolve(strict=True)
        full_path = file_path_to_path(directory, *paths).resolve(strict=True)
    except FileNotFoundError:
        raise NotFound()
    try:
        full_path.relative_to(safe_path)
    except ValueError:
        raise NotFound()
    return full_path


async def send_from_directory(
    directory: FilePath,
    file_name: str,
    *,
    mimetype: Optional[str] = None,
    as_attachment: bool = False,
    attachment_filename: Optional[str] = None,
    add_etags: bool = True,
    cache_timeout: Optional[int] = None,
    conditional: bool = True,
    last_modified: Optional[datetime] = None,
) -> Response:
    """Send a file from a given directory.

    Arguments:
       directory: Directory that when combined with file_name gives
           the file path.
       file_name: File name that when combined with directory gives
           the file path.

    See :func:`send_file` for the other arguments.
    """
    file_path = safe_join(directory, file_name)
    if not file_path.is_file():
        raise NotFound()
    return await send_file(
        file_path,
        mimetype=mimetype,
        as_attachment=as_attachment,
        attachment_filename=attachment_filename,
        add_etags=add_etags,
        cache_timeout=cache_timeout,
        conditional=conditional,
        last_modified=last_modified,
    )


async def send_file(
    filename_or_io: Union[FilePath, BytesIO],
    mimetype: Optional[str] = None,
    as_attachment: bool = False,
    attachment_filename: Optional[str] = None,
    add_etags: bool = True,
    cache_timeout: Optional[int] = None,
    conditional: bool = False,
    last_modified: Optional[datetime] = None,
) -> Response:
    """Return a Response to send the filename given.

    Arguments:
        filename_or_io: The filename (path) to send, remember to use
            :func:`safe_join`.
        mimetype: Mimetype to use, by default it will be guessed or
            revert to the DEFAULT_MIMETYPE.
        as_attachment: If true use the attachment filename in a
            Content-Disposition attachment header.
        attachment_filename: Name for the filename, if it differs
        add_etags: Set etags based on the filename, size and
            modification time.
        last_modified: Used to override the last modified value.
        cache_timeout: Time in seconds for the response to be cached.

    """
    file_body: ResponseBody
    file_size: int
    etag: Optional[str] = None
    if isinstance(filename_or_io, BytesIO):
        file_body = current_app.response_class.io_body_class(filename_or_io)
        file_size = filename_or_io.getbuffer().nbytes
    else:
        file_path = file_path_to_path(filename_or_io)
        file_size = file_path.stat().st_size
        if attachment_filename is None:
            attachment_filename = file_path.name
        file_body = current_app.response_class.file_body_class(file_path)
        if last_modified is None:
            last_modified = file_path.stat().st_mtime  # type: ignore
        if cache_timeout is None:
            cache_timeout = current_app.get_send_file_max_age(str(file_path))
        etag = "{}-{}-{}".format(
            file_path.stat().st_mtime, file_path.stat().st_size, adler32(bytes(file_path))
        )

    if mimetype is None and attachment_filename is not None:
        mimetype = mimetypes.guess_type(attachment_filename)[0] or DEFAULT_MIMETYPE
    if mimetype is None:
        raise ValueError(
            "The mime type cannot be inferred, please set it manually via the mimetype argument."
        )

    response = current_app.response_class(file_body, mimetype=mimetype)
    response.content_length = file_size

    if as_attachment:
        response.headers.add("Content-Disposition", "attachment", filename=attachment_filename)

    if last_modified is not None:
        response.last_modified = last_modified

    response.cache_control.public = True
    if cache_timeout is not None:
        response.cache_control.max_age = cache_timeout
        response.expires = datetime.utcnow() + timedelta(seconds=cache_timeout)

    if add_etags and etag is not None:
        response.set_etag(etag)

    if conditional:
        await response.make_conditional(request.range)
    return response


@lru_cache(maxsize=None)
def _split_blueprint_path(name: str) -> List[str]:
    bps = [name]
    while "." in bps[-1]:
        bps.append(bps[-1].rpartition(".")[0])
    return bps
