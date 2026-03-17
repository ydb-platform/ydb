"""General utils for the fake_useragent package."""

import json
import sys
from typing import TypedDict, Union

# We need files() from Python 3.10 or higher
if sys.version_info >= (3, 10):
    import importlib.resources as ilr
else:
    import importlib_resources as ilr  # noqa: F401

from pathlib import Path

from fake_useragent.errors import FakeUserAgentError
from fake_useragent.log import logger


class BrowserUserAgentData(TypedDict):
    """The schema for the browser user agent data that the `browsers.jsonl` file must follow."""

    useragent: str
    """The user agent string."""
    percent: float
    """The usage percentage of the user agent."""
    type: str
    """The device type for this user agent (eg. mobile or desktop)."""
    device_brand: Union[str, None]
    """Brand name for the device (eg. Generic_Android)."""
    browser: Union[str, None]
    """Browser name for the user agent (eg. Chrome Mobile)."""
    browser_version: str
    """Version of the browser (eg. "100.0.4896.60")."""
    browser_version_major_minor: float
    """Major and minor version of the browser (eg. 100.0)."""
    os: Union[str, None]
    """OS name for the user agent (eg. Android)."""
    os_version: Union[str, None]
    """OS version (eg. 10)."""
    platform: str
    """Platform for the user agent (eg. Linux armv81)."""


def find_browser_json_path() -> Path:
    """Find the path to the browsers.json file.

    Returns:
        Path: Path to the browsers.json file.

    Raises:
        FakeUserAgentError: If unable to find the file.
    """
    try:
        file_path = ilr.files("fake_useragent.data").joinpath("browsers.jsonl")
        return Path(str(file_path))
    except Exception as exc:
        logger.warning(
            "Unable to find local data/jsonl file using importlib-resources.",
            exc_info=exc,
        )
        raise FakeUserAgentError("Could not locate browsers.jsonl file") from exc


def load() -> list[BrowserUserAgentData]:
    """Load the included `browser.json` file into memory.

    Raises:
        FakeUserAgentError: If unable to load or parse the data.

    Returns:
        list[BrowserUserAgentData]: The list of browser user agent data, following the
            `BrowserUserAgentData` schema.
    """
    data = []
    try:
        json_path = find_browser_json_path()
        for line in json_path.read_text().splitlines():
            data.append(json.loads(line))
    except Exception as exc:
        raise FakeUserAgentError("Failed to load or parse browsers.json") from exc

    if not data:
        raise FakeUserAgentError("Data list is empty", data)

    if not isinstance(data, list):
        raise FakeUserAgentError("Data is not a list", data)
    return data
