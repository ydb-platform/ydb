"""OpenAPI core testing module"""
from openapi_core.testing.mock import MockRequestFactory, MockResponseFactory

# backward compatibility
MockRequest = MockRequestFactory.create
MockResponse = MockResponseFactory.create

__all__ = [
    'MockRequestFactory', 'MockResponseFactory', 'MockRequest', 'MockResponse',
]
