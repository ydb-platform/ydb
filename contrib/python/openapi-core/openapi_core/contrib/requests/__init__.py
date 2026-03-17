from openapi_core.contrib.requests.requests import (
    RequestsOpenAPIRequestFactory,
)
from openapi_core.contrib.requests.responses import (
    RequestsOpenAPIResponseFactory,
)

# backward compatibility
RequestsOpenAPIRequest = RequestsOpenAPIRequestFactory.create
RequestsOpenAPIResponse = RequestsOpenAPIResponseFactory.create

__all__ = [
    'RequestsOpenAPIRequestFactory', 'RequestsOpenAPIResponseFactory',
    'RequestsOpenAPIRequest', 'RequestsOpenAPIResponse',
]
