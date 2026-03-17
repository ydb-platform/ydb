from __future__ import annotations as _annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import cache
from typing import Any

from . import types

__all__ = 'DataSnapshot', 'set_custom_snapshot'

# snapshot set by UpdatePrices, or manually by the user
_custom_snapshot: DataSnapshot | None = None


def get_snapshot() -> DataSnapshot:
    if _custom_snapshot is not None:
        return _custom_snapshot
    return _bundled_snapshot()


@cache
def _bundled_snapshot() -> DataSnapshot:
    from .data import providers

    return DataSnapshot(providers=providers, from_auto_update=False)


def set_custom_snapshot(snapshot: DataSnapshot | None):
    global _custom_snapshot
    _custom_snapshot = snapshot


@dataclass
class DataSnapshot:
    providers: list[types.Provider]
    from_auto_update: bool
    _lookup_cache: dict[tuple[str | None, str | None, str], tuple[types.Provider, types.ModelInfo]] = field(
        default_factory=lambda: {}
    )
    timestamp: datetime = field(default_factory=datetime.now)

    def active(self, ttl: timedelta) -> bool:
        """Check if the snapshot is "active" (e.g. hasn't expired) based on a time to live."""
        return self.timestamp + ttl > datetime.now()

    def calc(
        self,
        usage: types.AbstractUsage,
        model_ref: str,
        provider_id: str | None,
        provider_api_url: str | None,
        genai_request_timestamp: datetime | None,
    ) -> types.PriceCalculation:
        """Calculate the price for the given usage."""
        genai_request_timestamp = genai_request_timestamp or datetime.now(tz=timezone.utc)

        provider, model = self.find_provider_model(model_ref, None, provider_id, provider_api_url)
        return model.calc_price(
            usage,
            provider,
            genai_request_timestamp=genai_request_timestamp,
            auto_update_timestamp=self.timestamp if self.from_auto_update else None,
        )

    def extract_usage(
        self,
        response_data: Any,
        provider_id: types.ProviderID | str | None = None,
        provider_api_url: str | None = None,
        api_flavor: str = 'default',
    ) -> types.ExtractedUsage:
        provider = self.find_provider(None, provider_id, provider_api_url)
        model_ref, usage = provider.extract_usage(response_data, api_flavor=api_flavor)
        if model_ref is not None:
            _, model = self.find_provider_model(model_ref, provider, None, None)
        else:
            model = None
        return types.ExtractedUsage(usage, model, provider, self.timestamp if self.from_auto_update else None)

    def find_provider_model(
        self,
        model_ref: str,
        provider: types.Provider | None,
        provider_id: str | None,
        provider_api_url: str | None,
    ) -> tuple[types.Provider, types.ModelInfo]:
        """Find the provider and model for the given model reference and optional provider identifier."""
        model_ref = model_ref.lower()

        # Handle litellm provider_id by extracting actual provider from model name prefix
        if provider_id and provider_id.lower() == 'litellm' and '/' in model_ref:
            actual_provider_id, actual_model_ref = model_ref.split('/', 1)
            # Only use the extracted provider if it exists
            if actual_provider_id and find_provider_by_id(self.providers, actual_provider_id):
                provider_id = actual_provider_id
                model_ref = actual_model_ref

        if provider:
            if provider_model := self._lookup_cache.get((provider.id, None, model_ref)):
                return provider_model
        else:
            if provider_model := self._lookup_cache.get((provider_id, provider_api_url, model_ref)):
                return provider_model

            provider = self.find_provider(model_ref, provider_id, provider_api_url)

        if model := provider.find_model(model_ref, all_providers=self.providers):
            self._lookup_cache[(provider_id, provider_api_url, model_ref)] = ret = provider, model
            return ret
        else:
            raise LookupError(f'Unable to find model with {model_ref=!r} in {provider.id}')

    def find_provider(
        self,
        model_ref: str | None,
        provider_id: str | None,
        provider_api_url: str | None,
    ) -> types.Provider:
        if provider_id is not None:
            if provider := find_provider_by_id(self.providers, provider_id):
                return provider
            # Special case for litellm: fall back to model matching if provider not found
            if provider_id.lower() != 'litellm':
                raise LookupError(f'Unable to find provider {provider_id=!r}')

        if provider_api_url is not None:
            for provider in self.providers:
                if re.match(provider.api_pattern, provider_api_url):
                    return provider
            raise LookupError(f'Unable to find provider {provider_api_url=!r}')

        if model_ref:
            for provider in self.providers:
                if provider.model_match is not None and provider.model_match.is_match(model_ref):
                    return provider

        raise LookupError(f'Unable to find provider with model matching {model_ref!r}')


def find_provider_by_id(providers: list[types.Provider], provider_id: str) -> types.Provider | None:
    """Find a provider by matching against provider_match logic.

    Args:
        providers: List of available providers
        provider_id: The provider ID to match

    Returns:
        The matching provider or None
    """
    normalized_provider_id = provider_id.lower().strip()

    for provider in providers:
        if provider.id == normalized_provider_id:
            return provider

    for provider in providers:
        if provider.provider_match and provider.provider_match.is_match(normalized_provider_id):
            return provider

    return None
