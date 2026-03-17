"""OpenAPI core contrib flask handlers module"""
from flask.globals import current_app
from flask.json import dumps

from openapi_core.templating.media_types.exceptions import MediaTypeNotFound
from openapi_core.templating.paths.exceptions import (
    ServerNotFound, OperationNotFound, PathNotFound,
)


class FlaskOpenAPIErrorsHandler(object):

    OPENAPI_ERROR_STATUS = {
        ServerNotFound: 400,
        OperationNotFound: 405,
        PathNotFound: 404,
        MediaTypeNotFound: 415,
    }

    @classmethod
    def handle(cls, errors):
        data_errors = [
            cls.format_openapi_error(err)
            for err in errors
        ]
        data = {
            'errors': data_errors,
        }
        data_error_max = max(data_errors, key=lambda x: x['status'])
        status = data_error_max['status']
        return current_app.response_class(
            dumps(data),
            status=status,
            mimetype='application/json'
        )

    @classmethod
    def format_openapi_error(cls, error):
        return {
            'title': str(error),
            'status': cls.OPENAPI_ERROR_STATUS.get(error.__class__, 400),
            'class': str(type(error)),
        }
