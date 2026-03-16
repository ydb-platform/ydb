"""Token usage utilities for Claude Agent SDK."""

from typing import Any


def extract_usage_metadata(usage: Any) -> dict[str, Any]:
    """Extract and normalize usage metrics from a Claude usage object or dict."""
    if not usage:
        return {}

    get = usage.get if isinstance(usage, dict) else lambda k: getattr(usage, k, None)

    def to_int(value):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def to_float(value):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    meta: dict[str, Any] = {}
    if (v := to_int(get("input_tokens"))) is not None:
        meta["input_tokens"] = v
    if (v := to_int(get("output_tokens"))) is not None:
        meta["output_tokens"] = v

    cache_read = to_float(get("cache_read_input_tokens"))
    cache_create = to_float(get("cache_creation_input_tokens"))
    if cache_read is not None or cache_create is not None:
        meta["input_token_details"] = {}
        if cache_read is not None:
            meta["input_token_details"]["cache_read"] = cache_read
        if cache_create is not None:
            meta["input_token_details"]["cache_creation"] = cache_create

    return meta


def sum_anthropic_tokens(usage_metadata: dict[str, Any]) -> dict[str, int]:
    """Sum Anthropic cache tokens into input_tokens and add total_tokens."""
    details = usage_metadata.get("input_token_details") or {}
    cache_read = details.get(
        "cache_read", usage_metadata.get("cache_read_input_tokens")
    )
    cache_create = details.get(
        "cache_creation", usage_metadata.get("cache_creation_input_tokens")
    )

    input_tokens = usage_metadata.get("input_tokens") or 0
    cache_read_val = cache_read or 0
    cache_create_val = cache_create or 0
    total_prompt = input_tokens + cache_read_val + cache_create_val

    output_tokens = usage_metadata.get("output_tokens") or 0
    return {
        **usage_metadata,
        "input_tokens": total_prompt,
        "total_tokens": total_prompt + output_tokens,
    }
