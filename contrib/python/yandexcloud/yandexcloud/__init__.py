"""Main package for Yandex.Cloud SDK."""

# flake8: noqa
from yandexcloud._auth_fabric import set_up_yc_api_endpoint
from yandexcloud._backoff import (
    backoff_exponential_with_jitter,
    backoff_linear_with_jitter,
    default_backoff,
)
from yandexcloud._retry_interceptor import RetryInterceptor
from yandexcloud._sdk import SDK

__version__ = "0.0.2"
