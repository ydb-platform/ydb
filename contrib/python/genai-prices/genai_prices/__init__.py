from __future__ import annotations as _annotations

from datetime import datetime
from importlib.metadata import version as _metadata_version
from typing import Any, overload

from . import data_snapshot, types
from .types import Usage
from .update_prices import UpdatePrices, wait_prices_updated_async, wait_prices_updated_sync

__version__ = _metadata_version('genai_prices')
__all__ = 'Usage', 'calc_price', 'UpdatePrices', 'wait_prices_updated_sync', 'wait_prices_updated_async', '__version__'


@overload
def calc_price(
    usage: types.AbstractUsage,
    model_ref: str,
    *,
    provider_id: types.ProviderID | str | None = None,
    genai_request_timestamp: datetime | None = None,
) -> types.PriceCalculation: ...


@overload
def calc_price(
    usage: types.AbstractUsage,
    model_ref: str,
    *,
    provider_api_url: str | None = None,
    genai_request_timestamp: datetime | None = None,
) -> types.PriceCalculation: ...


def calc_price(
    usage: types.AbstractUsage,
    model_ref: str,
    *,
    provider_id: types.ProviderID | str | None = None,
    provider_api_url: str | None = None,
    genai_request_timestamp: datetime | None = None,
) -> types.PriceCalculation:
    """Calculate the price of an LLM API call.

    Either `provider_id` or `provider_api_url` should be provided, but not both. If neither are provided,
    we try to find the most suitable provider based on the model reference.

    Args:
        usage: The usage to calculate the price for.
        model_ref: A reference to the model used, this method will try to match this to a specific model.
        provider_id: The ID of the provider to calculate the price for.
        provider_api_url: The API URL of the provider to calculate the price for.
        genai_request_timestamp: The timestamp of the request to the GenAI service, use `None` to use the current time.

    Returns:
        The price calculation details.
    """
    return data_snapshot.get_snapshot().calc(usage, model_ref, provider_id, provider_api_url, genai_request_timestamp)


@overload
def extract_usage(
    response_data: Any, *, provider_id: types.ProviderID | str, api_flavor: str = 'default'
) -> types.ExtractedUsage: ...


@overload
def extract_usage(
    response_data: Any, *, provider_api_url: str, api_flavor: str = 'default'
) -> types.ExtractedUsage: ...


def extract_usage(
    response_data: Any,
    *,
    provider_id: types.ProviderID | str | None = None,
    provider_api_url: str | None = None,
    api_flavor: str = 'default',
) -> types.ExtractedUsage:
    """Extract usage information from a response.

    One of `provider_id` or `provider_api_url` is required.

    Args:
        response_data: The response data to extract usage information from.
        provider: The provider to extract usage information for.
        provider_id: The ID of the provider to extract usage information for.
        provider_api_url: The API URL of the provider to extract usage information for.
        api_flavor: The API flavor of the provider to extract usage information for.

    Returns:
        The extracted usage information, model ref and provider used.
    """
    return data_snapshot.get_snapshot().extract_usage(response_data, provider_id, provider_api_url, api_flavor)
