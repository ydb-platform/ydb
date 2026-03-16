"""Up-to-date simple useragent faker with real world database."""

from fake_useragent.errors import FakeUserAgentError, UserAgentError
from fake_useragent.fake import FakeUserAgent, UserAgent
from fake_useragent.get_version import __version__

__all__ = [
    "FakeUserAgent",
    "UserAgent",
    "FakeUserAgentError",
    "UserAgentError",
    "__version__",
]
