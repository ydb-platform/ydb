from openapi_core.contrib.django.requests import DjangoOpenAPIRequestFactory
from openapi_core.contrib.django.responses import DjangoOpenAPIResponseFactory

# backward compatibility
DjangoOpenAPIRequest = DjangoOpenAPIRequestFactory.create
DjangoOpenAPIResponse = DjangoOpenAPIResponseFactory.create

__all__ = [
    'DjangoOpenAPIRequestFactory', 'DjangoOpenAPIResponseFactory',
    'DjangoOpenAPIRequest', 'DjangoOpenAPIResponse',
]
