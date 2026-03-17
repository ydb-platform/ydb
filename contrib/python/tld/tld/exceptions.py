__author__ = "Artur Barseghyan"
__copyright__ = "2013-2025 Artur Barseghyan"
__license__ = "MPL-1.1 OR GPL-2.0-only OR LGPL-2.1-or-later"
__all__ = (
    "TldBadUrl",
    "TldDomainNotFound",
    "TldImproperlyConfigured",
    "TldIOError",
)


class TldIOError(IOError):
    """TldIOError.

    Supposed to be thrown when problems with reading/writing occur.
    """


class TldDomainNotFound(ValueError):
    """TldDomainNotFound.

    Supposed to be thrown when domain name is not found (didn't match) the
    local TLD policy.
    """

    def __init__(self, domain_name):
        super(TldDomainNotFound, self).__init__(
            "Domain %s didn't match any existing TLD name!" % domain_name
        )


class TldBadUrl(ValueError):
    """TldBadUrl.

    Supposed to be thrown when bad URL is given.
    """

    def __init__(self, url):
        super(TldBadUrl, self).__init__("Is not a valid URL %s!" % url)


class TldImproperlyConfigured(Exception):
    """TldImproperlyConfigured.

    Supposed to be thrown when code is improperly configured. Typical use-case
    is when user tries to use `get_tld` function with both `search_public` and
    `search_private` set to False.
    """
