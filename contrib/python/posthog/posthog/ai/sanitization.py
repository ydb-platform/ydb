import os
import re
from typing import Any
from urllib.parse import urlparse

REDACTED_IMAGE_PLACEHOLDER = "[base64 image redacted]"


def _is_multimodal_enabled() -> bool:
    """Check if multimodal capture is enabled via environment variable."""
    return os.environ.get("_INTERNAL_LLMA_MULTIMODAL", "").lower() in (
        "true",
        "1",
        "yes",
    )


def is_base64_data_url(text: str) -> bool:
    return re.match(r"^data:([^;]+);base64,", text) is not None


def is_valid_url(text: str) -> bool:
    try:
        result = urlparse(text)
        return bool(result.scheme and result.netloc)
    except Exception:
        pass

    return text.startswith(("/", "./", "../"))


def is_raw_base64(text: str) -> bool:
    if is_valid_url(text):
        return False

    return len(text) > 20 and re.match(r"^[A-Za-z0-9+/]+=*$", text) is not None


def redact_base64_data_url(value: Any) -> Any:
    if _is_multimodal_enabled():
        return value

    if not isinstance(value, str):
        return value

    if is_base64_data_url(value):
        return REDACTED_IMAGE_PLACEHOLDER

    if is_raw_base64(value):
        return REDACTED_IMAGE_PLACEHOLDER

    return value


def process_messages(messages: Any, transform_content_func) -> Any:
    if not messages:
        return messages

    def process_content(content: Any) -> Any:
        if isinstance(content, str):
            return content

        if not content:
            return content

        if isinstance(content, list):
            return [transform_content_func(item) for item in content]

        return transform_content_func(content)

    def process_message(msg: Any) -> Any:
        if not isinstance(msg, dict) or "content" not in msg:
            return msg
        return {**msg, "content": process_content(msg["content"])}

    if isinstance(messages, list):
        return [process_message(msg) for msg in messages]

    return process_message(messages)


def sanitize_openai_image(item: Any) -> Any:
    if not isinstance(item, dict):
        return item

    if item.get("type") == "input_image" and isinstance(item.get("image_url"), str):
        return {
            **item,
            "image_url": redact_base64_data_url(item["image_url"]),
        }

    if (
        item.get("type") == "image_url"
        and isinstance(item.get("image_url"), dict)
        and "url" in item["image_url"]
    ):
        return {
            **item,
            "image_url": {
                **item["image_url"],
                "url": redact_base64_data_url(item["image_url"]["url"]),
            },
        }

    if item.get("type") == "audio" and "data" in item:
        if _is_multimodal_enabled():
            return item
        return {**item, "data": REDACTED_IMAGE_PLACEHOLDER}

    return item


def sanitize_openai_response_image(item: Any) -> Any:
    if not isinstance(item, dict):
        return item

    if item.get("type") == "input_image" and "image_url" in item:
        return {
            **item,
            "image_url": redact_base64_data_url(item["image_url"]),
        }

    return item


def sanitize_anthropic_image(item: Any) -> Any:
    if _is_multimodal_enabled():
        return item

    if not isinstance(item, dict):
        return item

    if (
        item.get("type") == "image"
        and isinstance(item.get("source"), dict)
        and item["source"].get("type") == "base64"
        and "data" in item["source"]
    ):
        return {
            **item,
            "source": {
                **item["source"],
                "data": REDACTED_IMAGE_PLACEHOLDER,
            },
        }

    return item


def sanitize_gemini_part(part: Any) -> Any:
    if _is_multimodal_enabled():
        return part

    if not isinstance(part, dict):
        return part

    if (
        "inline_data" in part
        and isinstance(part["inline_data"], dict)
        and "data" in part["inline_data"]
    ):
        return {
            **part,
            "inline_data": {
                **part["inline_data"],
                "data": REDACTED_IMAGE_PLACEHOLDER,
            },
        }

    return part


def process_gemini_item(item: Any) -> Any:
    if not isinstance(item, dict):
        return item

    if "parts" in item and item["parts"]:
        parts = item["parts"]
        if isinstance(parts, list):
            parts = [sanitize_gemini_part(part) for part in parts]
        else:
            parts = sanitize_gemini_part(parts)

        return {**item, "parts": parts}

    return item


def sanitize_langchain_image(item: Any) -> Any:
    if not isinstance(item, dict):
        return item

    if (
        item.get("type") == "image_url"
        and isinstance(item.get("image_url"), dict)
        and "url" in item["image_url"]
    ):
        return {
            **item,
            "image_url": {
                **item["image_url"],
                "url": redact_base64_data_url(item["image_url"]["url"]),
            },
        }

    if item.get("type") == "image" and "data" in item:
        return {**item, "data": redact_base64_data_url(item["data"])}

    if (
        item.get("type") == "image"
        and isinstance(item.get("source"), dict)
        and "data" in item["source"]
    ):
        if _is_multimodal_enabled():
            return item

        return {
            **item,
            "source": {
                **item["source"],
                "data": REDACTED_IMAGE_PLACEHOLDER,
            },
        }

    if item.get("type") == "media" and "data" in item:
        return {**item, "data": redact_base64_data_url(item["data"])}

    return item


def sanitize_openai(data: Any) -> Any:
    return process_messages(data, sanitize_openai_image)


def sanitize_openai_response(data: Any) -> Any:
    return process_messages(data, sanitize_openai_response_image)


def sanitize_anthropic(data: Any) -> Any:
    return process_messages(data, sanitize_anthropic_image)


def sanitize_gemini(data: Any) -> Any:
    if not data:
        return data

    if isinstance(data, list):
        return [process_gemini_item(item) for item in data]

    return process_gemini_item(data)


def sanitize_langchain(data: Any) -> Any:
    return process_messages(data, sanitize_langchain_image)
