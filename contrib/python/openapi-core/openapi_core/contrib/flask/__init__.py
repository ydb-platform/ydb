from openapi_core.contrib.flask.requests import FlaskOpenAPIRequestFactory
from openapi_core.contrib.flask.responses import FlaskOpenAPIResponseFactory

# backward compatibility
FlaskOpenAPIRequest = FlaskOpenAPIRequestFactory.create
FlaskOpenAPIResponse = FlaskOpenAPIResponseFactory.create

__all__ = [
    'FlaskOpenAPIRequestFactory', 'FlaskOpenAPIResponseFactory',
    'FlaskOpenAPIRequest', 'FlaskOpenAPIResponse',
]
