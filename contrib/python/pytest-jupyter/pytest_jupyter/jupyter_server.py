"""Fixtures for use with jupyter server and downstream."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import importlib
import inspect
import io
import logging
import os
import re
import shutil
import urllib.parse
from binascii import hexlify
from pathlib import Path

import jupyter_core.paths
import pytest

# The try block is needed so that the documentation can
# still build without needed to install all the dependencies.
try:
    import nbformat
    import tornado
    import tornado.testing
    from jupyter_server._version import version_info
    from jupyter_server.auth import Authorizer
    from jupyter_server.extension import serverextension
    from jupyter_server.serverapp import JUPYTER_SERVICE_HANDLERS, ServerApp
    from jupyter_server.utils import url_path_join
    from tornado.escape import url_escape
    from tornado.httpclient import HTTPClientError
    from tornado.websocket import WebSocketHandler
    from traitlets.config import Config

    is_v2 = version_info[0] == 2

except ImportError:
    Authorizer = object  # type:ignore[assignment, misc]
    import warnings

    warnings.warn(
        "The server plugin has not been installed. "
        "If you're trying to use this plugin and you've installed "
        "`pytest-jupyter`, there is likely one more step "
        "you need. Try: `pip install 'pytest-jupyter[server]'`",
        stacklevel=2,
    )


# Bring in local plugins.
from pytest_jupyter.jupyter_core import *  # noqa: F403
from pytest_jupyter.pytest_tornasync import *  # noqa: F403
from pytest_jupyter.utils import mkdir


@pytest.fixture
def jp_server_config():
    """Allows tests to setup their specific configuration values."""
    if is_v2:
        config = {
            "ServerApp": {
                "jpserver_extensions": {"jupyter_server_terminals": True},
            }
        }
    else:
        config = {}
    return Config(config)


@pytest.fixture
def jp_root_dir(tmp_path):
    """Provides a temporary Jupyter root directory value."""
    return mkdir(tmp_path, "root_dir")


@pytest.fixture
def jp_template_dir(tmp_path):
    """Provides a temporary Jupyter templates directory value."""
    return mkdir(tmp_path, "templates")


@pytest.fixture
def jp_argv():
    """Allows tests to setup specific argv values."""
    return []


@pytest.fixture
def jp_http_port(http_server_port):
    """Returns the port value from the http_server_port fixture."""
    yield http_server_port[-1]
    http_server_port[0].close()


@pytest.fixture
def jp_extension_environ(jp_env_config_path, monkeypatch):
    """Monkeypatch a Jupyter Extension's config path into each test's environment variable"""
    monkeypatch.setattr(serverextension, "ENV_CONFIG_PATH", [str(jp_env_config_path)])


@pytest.fixture
def jp_nbconvert_templates(jp_data_dir):
    """Setups up a temporary directory consisting of the nbconvert templates."""

    # Get path to nbconvert template directory *before*
    # monkeypatching the paths env variable via the jp_environ fixture.
    possible_paths = jupyter_core.paths.jupyter_path("nbconvert", "templates")
    nbconvert_path = None
    for path in possible_paths:
        if Path(path).exists():
            nbconvert_path = path
            break

    nbconvert_target = jp_data_dir / "nbconvert" / "templates"

    # copy nbconvert templates to new tmp data_dir.
    if nbconvert_path:
        shutil.copytree(nbconvert_path, str(nbconvert_target))


@pytest.fixture
def jp_logging_stream():
    """StringIO stream intended to be used by the core
    Jupyter ServerApp logger's default StreamHandler. This
    helps avoid collision with stdout which is hijacked
    by Pytest.
    """
    logging_stream = io.StringIO()
    yield logging_stream
    output = logging_stream.getvalue()
    # If output exists, print it.
    if output:
        print(output)  # noqa: T201
    return output


@pytest.fixture
def jp_configurable_serverapp(
    jp_nbconvert_templates,  # this fixture must precede jp_environ
    jp_environ,
    jp_server_config,
    jp_argv,
    jp_http_port,
    jp_base_url,
    tmp_path,
    jp_root_dir,
    jp_logging_stream,
    jp_asyncio_loop,
):
    """Starts a Jupyter Server instance based on
    the provided configuration values.
    The fixture is a factory; it can be called like
    a function inside a unit test. Here's a basic
    example of how use this fixture:

    .. code-block:: python

      def my_test(jp_configurable_serverapp):
          app = jp_configurable_serverapp(...)
          ...
    """
    ServerApp.clear_instance()

    # Inject jupyter_server_terminals into config unless it was
    # explicitly put in config.
    serverapp_config = jp_server_config.setdefault("ServerApp", {})
    exts = serverapp_config.setdefault("jpserver_extensions", {})
    if "jupyter_server_terminals" not in exts and is_v2:
        exts["jupyter_server_terminals"] = True

    def _configurable_serverapp(
        config=jp_server_config,
        base_url=jp_base_url,
        argv=jp_argv,
        environ=jp_environ,
        http_port=jp_http_port,
        tmp_path=tmp_path,
        root_dir=jp_root_dir,
        **kwargs,
    ):
        c = Config(config)
        c.NotebookNotary.db_file = ":memory:"

        default_token = hexlify(os.urandom(4)).decode("ascii")
        if not is_v2:
            kwargs["token"] = default_token

        elif "token" not in c.ServerApp and not c.IdentityProvider.token:
            c.IdentityProvider.token = default_token

        # Allow tests to configure root_dir via a file, argv, or its
        # default (cwd) by specifying a value of None.
        if root_dir is not None:
            kwargs["root_dir"] = str(root_dir)

        app = ServerApp.instance(
            # Set the log level to debug for testing purposes
            log_level="DEBUG",
            port=http_port,
            port_retries=0,
            open_browser=False,
            base_url=base_url,
            config=c,
            allow_root=True,
            **kwargs,
        )
        app.init_signal = lambda: None  # type:ignore[method-assign]
        app.log.propagate = True
        app.log.handlers = []
        # Initialize app without httpserver
        if jp_asyncio_loop.is_running():
            app.initialize(argv=argv, new_httpserver=False)
        else:

            async def initialize_app():
                app.initialize(argv=argv, new_httpserver=False)

            jp_asyncio_loop.run_until_complete(initialize_app())  # type:ignore[no-untyped-call]
        # Reroute all logging StreamHandlers away from stdin/stdout since pytest hijacks
        # these streams and closes them at unfortunate times.
        stream_handlers = [h for h in app.log.handlers if isinstance(h, logging.StreamHandler)]
        for handler in stream_handlers:
            handler.setStream(jp_logging_stream)
        app.log.propagate = True
        app.log.handlers = []
        app.start_app()
        return app

    return _configurable_serverapp


@pytest.fixture
def jp_serverapp(jp_server_config, jp_argv, jp_configurable_serverapp):
    """Starts a Jupyter Server instance based on the established configuration values."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*WindowsProactorEventLoopPolicy.*",
            category=DeprecationWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message=".*WindowsSelectorEventLoopPolicy.*",
            category=DeprecationWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message=".*get_event_loop_policy.*",
            category=DeprecationWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message=".*set_event_loop_policy.*",
            category=DeprecationWarning,
        )

        return jp_configurable_serverapp(config=jp_server_config, argv=jp_argv)


@pytest.fixture
def jp_web_app(jp_serverapp):
    """app fixture is needed by pytest_tornasync plugin"""
    return jp_serverapp.web_app


@pytest.fixture
def jp_auth_header(jp_serverapp):
    """Configures an authorization header using the token from the serverapp fixture."""
    if not is_v2:
        return {"Authorization": f"token {jp_serverapp.token}"}
    return {"Authorization": f"token {jp_serverapp.identity_provider.token}"}


@pytest.fixture
def jp_base_url():
    """Returns the base url to use for the test."""
    return "/a%40b/"


@pytest.fixture
def jp_fetch(jp_serverapp, http_server_client, jp_auth_header, jp_base_url):
    """Sends an (asynchronous) HTTP request to a test server.
    The fixture is a factory; it can be called like
    a function inside a unit test. Here's a basic
    example of how use this fixture:

    .. code-block:: python

        async def my_test(jp_fetch):
            response = await jp_fetch("api", "spec.yaml")
            ...

    With query parameters:

    .. code-block:: python

        async def my_test(jp_fetch):
            response = await jp_fetch("api", "spec.yaml", params={"parameter": "value"})
            ...

    A POST request with data:

    .. code-block:: python

        async def my_test(jp_fetch):
            response = await jp_fetch(
                "api", "spec.yaml", method="POST", body=json.dumps({"parameter": "value"})
            )
            ...

    """

    def client_fetch(*parts, headers=None, params=None, **kwargs):
        if not headers:
            headers = {}
        if not params:
            params = {}
        # Handle URL strings
        path_url = url_escape(url_path_join(*parts), plus=False)
        base_path_url = url_path_join(jp_base_url, path_url)
        params_url = urllib.parse.urlencode(params)
        url = base_path_url + "?" + params_url
        # Add auth keys to header, if not overridden
        for key, value in jp_auth_header.items():
            headers.setdefault(key, value)
        # Handle timeout
        request_timeout = kwargs.pop("request_timeout", 20)
        # Make request.
        return http_server_client.fetch(
            url, headers=headers, request_timeout=request_timeout, **kwargs
        )

    return client_fetch


@pytest.fixture
def jp_ws_fetch(jp_serverapp, http_server_client, jp_auth_header, jp_http_port, jp_base_url):
    """Sends a websocket request to a test server.
    The fixture is a factory; it can be called like
    a function inside a unit test. Here's a basic
    example of how use this fixture:

    .. code-block:: python

        async def my_test(jp_fetch, jp_ws_fetch):
            # Start a kernel
            r = await jp_fetch(
                "api", "kernels", method="POST", body=json.dumps({"name": "python3"})
            )
            kid = json.loads(r.body.decode())["id"]
            # Open a websocket connection.
            ws = await jp_ws_fetch("api", "kernels", kid, "channels")
            ...
    """

    def client_fetch(*parts, headers=None, params=None, **kwargs):
        if not headers:
            headers = {}
        if not params:
            params = {}
        # Handle URL strings
        path_url = url_escape(url_path_join(*parts), plus=False)
        base_path_url = url_path_join(jp_base_url, path_url)
        urlparts = urllib.parse.urlparse(f"ws://localhost:{jp_http_port}")
        urlparts = urlparts._replace(path=base_path_url, query=urllib.parse.urlencode(params))
        url = urlparts.geturl()
        # Add auth keys to header, if not overridden
        for key, value in jp_auth_header.items():
            headers.setdefault(key, value)
        # Make request.
        req = tornado.httpclient.HTTPRequest(url, headers=headers, connect_timeout=120)
        allowed = list(inspect.signature(tornado.websocket.websocket_connect).parameters)
        for name in list(kwargs):
            if name not in allowed:
                del kwargs[name]
        return tornado.websocket.websocket_connect(req, **kwargs)

    return client_fetch


@pytest.fixture
def jp_create_notebook(jp_root_dir):
    """Creates a notebook in the test's home directory."""

    def inner(nbpath):
        nbpath = jp_root_dir.joinpath(nbpath)
        # Check that the notebook has the correct file extension.
        if nbpath.suffix != ".ipynb":
            msg = "File extension for notebook must be .ipynb"
            raise Exception(msg)
        # If the notebook path has a parent directory, make sure it's created.
        parent = nbpath.parent
        parent.mkdir(parents=True, exist_ok=True)
        # Create a notebook string and write to file.
        nb = nbformat.v4.new_notebook()  # type:ignore[no-untyped-call]
        nbtext = nbformat.writes(nb, version=4)  # type:ignore[no-untyped-call]
        nbpath.write_text(nbtext)
        return nb

    return inner


@pytest.fixture(autouse=True)
def jp_server_cleanup(jp_asyncio_loop):
    """Automatically cleans up server resources."""
    yield
    app: ServerApp = ServerApp.instance()
    try:
        jp_asyncio_loop.run_until_complete(app._cleanup())
    except (RuntimeError, SystemExit) as e:
        print("ignoring cleanup error", e)  # noqa: T201
    if hasattr(app, "kernel_manager"):
        app.kernel_manager.context.destroy()
    ServerApp.clear_instance()


@pytest.fixture
def send_request(jp_fetch, jp_ws_fetch):
    """Send to Jupyter Server and return response code."""

    async def _(url, **fetch_kwargs):
        fetch = jp_ws_fetch if url.endswith("channels") or "/websocket/" in url else jp_fetch

        try:
            r = await fetch(url, **fetch_kwargs, allow_nonstandard_methods=True)
            code = r.code
        except HTTPClientError as err:
            code = err.code
            print(f"HTTPClientError ({err.code}): {err}")  # noqa: T201
        else:
            if fetch is jp_ws_fetch:
                r.close()

        return code

    return _


@pytest.fixture
def jp_server_auth_core_resources():
    """The core auth resources for use with a server."""
    modules = []
    for mod_name in JUPYTER_SERVICE_HANDLERS.values():
        if mod_name:
            modules.extend(mod_name)
    resource_map = {}
    for handler_module in modules:
        mod = importlib.import_module(handler_module)
        name = mod.AUTH_RESOURCE
        for handler in mod.default_handlers:
            url_regex = handler[0]
            resource_map[url_regex] = name
    return resource_map


@pytest.fixture
def jp_server_auth_resources(jp_server_auth_core_resources):
    """The auth resources used by the server."""
    return jp_server_auth_core_resources


class _Authorizer(Authorizer):
    """A custom authorizer class for testing."""

    # Set these class attributes from within a test
    # to verify that they match the arguments passed
    # by the REST API.
    permissions: dict[str, str] = {}
    _default_regex_mapping: dict[str, str] = {}

    HTTP_METHOD_TO_AUTH_ACTION = {
        "GET": "read",
        "HEAD": "read",
        "OPTIONS": "read",
        "POST": "write",
        "PUT": "write",
        "PATCH": "write",
        "DELETE": "write",
        "WEBSOCKET": "execute",
    }

    def match_url_to_resource(
        self, url: str, regex_mapping: dict[str, str] | None = None
    ) -> str | None:
        """Finds the JupyterHandler regex pattern that would
        match the given URL and returns the resource name (str)
        of that handler.
        e.g.
        /api/contents/... returns "contents"
        """
        regex_mapping = regex_mapping or self._default_regex_mapping
        for regex, auth_resource in regex_mapping.items():
            pattern = re.compile(regex)
            if pattern.fullmatch(url):
                return auth_resource
        return None

    def normalize_url(self, path: str) -> str:
        """Drop the base URL and make sure path leads with a /"""
        if not self.parent:
            msg = "Cannot normalize the url without a parent object"
            raise ValueError(msg)
        base_url = self.parent.base_url
        # Remove base_url
        if path.startswith(base_url):
            path = path[len(base_url) :]
        # Make sure path starts with /
        if not path.startswith("/"):
            path = "/" + path
        return path

    def is_authorized(self, handler, user, action, resource):  # noqa: ARG002
        """Test if a request is authorized."""
        # Parse Request
        method = "WEBSOCKET" if isinstance(handler, WebSocketHandler) else handler.request.method
        url = self.normalize_url(handler.request.path)

        # Map request parts to expected action and resource.
        expected_action = self.HTTP_METHOD_TO_AUTH_ACTION[method]
        expected_resource = self.match_url_to_resource(url)

        # Assert that authorization layer returns the
        # correct action + resource.
        if action != expected_action or resource != expected_resource:
            raise AssertionError

        # Now, actually apply the authorization layer.
        return all(
            [
                action in self.permissions.get("actions", []),
                resource in self.permissions.get("resources", []),
            ]
        )


@pytest.fixture
def jp_server_authorizer(jp_server_auth_resources):
    """An authorizer for the server."""
    auth_klass = _Authorizer
    auth_klass._default_regex_mapping = jp_server_auth_resources
    return auth_klass
