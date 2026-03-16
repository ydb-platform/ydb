import uuid
import logging as _logging

from flask import request, g, current_app

from .parser import auto_parser
from .ctx_fetcher import MultiContextRequestIdFetcher, ExecutedOutsideContext


logger = _logging.getLogger(__name__)


def flask_ctx_get_request_id():
    """
    Get request id from flask's G object
    :return: The id or None if not found.
    """
    from flask import _app_ctx_stack as stack  # We do not support < Flask 0.9

    if stack.top is None:
        raise ExecutedOutsideContext()

    g_object_attr = stack.top.app.config['LOG_REQUEST_ID_G_OBJECT_ATTRIBUTE']
    return g.get(g_object_attr, None)


current_request_id = MultiContextRequestIdFetcher()
current_request_id.register_fetcher(flask_ctx_get_request_id)


class RequestID(object):
    """
    Flask extension to parse or generate the id of each request
    """

    def __init__(self, app=None, request_id_parser=None, request_id_generator=None):
        """
        Initialize extension
        :param flask.Application | None app: The flask application or None if you want to initialize later
        :param None | () -> str request_id_parser: The parser to extract request-id from request headers. If None
        the default auto_parser() will be used that will try all known parsers.
        :param ()->str request_id_generator: A callable to use in case of missing request-id.
        """
        self.app = app
        self._request_id = None

        self._request_id_parser = request_id_parser
        if self._request_id_parser is None:
            self._request_id_parser = auto_parser

        self._request_id_generator = request_id_generator
        if self._request_id_generator is None:
            self._request_id_generator = lambda: str(uuid.uuid4())

        self._generate_id_if_not_found = True

        if app is not None:
            self.init_app(app)

    def init_app(self, app):

        # Default configuration
        app.config.setdefault('LOG_REQUEST_ID_GENERATE_IF_NOT_FOUND', True)
        app.config.setdefault('LOG_REQUEST_ID_LOG_ALL_REQUESTS', False)
        app.config.setdefault('LOG_REQUEST_ID_G_OBJECT_ATTRIBUTE', 'log_request_id')

        # Register before request callback
        @app.before_request
        def _persist_request_id():
            """
            It will parse and persist the RequestID from the HTTP request. If not
            found it will generate a new one if requestsed.

            To be used as a consumer of Flask.before_request event.
            """
            g_object_attr = current_app.config['LOG_REQUEST_ID_G_OBJECT_ATTRIBUTE']

            setattr(g, g_object_attr, self._request_id_parser())
            if g.get(g_object_attr) is None:
                if app.config['LOG_REQUEST_ID_GENERATE_IF_NOT_FOUND']:
                    setattr(g, g_object_attr, self._request_id_generator())

        # Register after request
        if self.app.config['LOG_REQUEST_ID_LOG_ALL_REQUESTS']:
            app.after_request(self._log_http_event)

    @staticmethod
    def _log_http_event(response):
        """
        It will create a log event as werkzeug but at the end of request holding the request-id

        Intended usage is a handler of Flask.after_request
        :return: The same response object
        """
        logger.info(
            '{ip} - - "{method} {path} {status_code}"'.format(
                ip=request.remote_addr,
                method=request.method,
                path=request.path,
                status_code=response.status_code)
        )
        return response
