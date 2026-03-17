from typing import Any, Dict
from urllib.parse import SplitResult

__author__ = "Artur Barseghyan"
__copyright__ = "2013-2025 Artur Barseghyan"
__license__ = "MPL-1.1 OR GPL-2.0-only OR LGPL-2.1-or-later"
__all__ = ("Result",)


class Result(object):
    """Container."""

    __slots__ = ("subdomain", "domain", "tld", "__fld", "parsed_url")

    def __init__(
        self, tld: str, domain: str, subdomain: str, parsed_url: SplitResult
    ):
        self.tld = tld
        self.domain = domain if domain != "" else tld
        self.subdomain = subdomain
        self.parsed_url = parsed_url

        if domain:
            self.__fld = f"{self.domain}.{self.tld}"
        else:
            self.__fld = self.tld

    @property
    def extension(self) -> str:
        """Alias of ``tld``.

        :return str:
        """
        return self.tld

    suffix = extension

    @property
    def fld(self) -> str:
        """First level domain.

        :return:
        :rtype: str
        """
        return self.__fld

    def __str__(self) -> str:
        return self.tld

    __repr__ = __str__

    @property
    def __dict__(self) -> Dict[str, Any]:  # type: ignore
        """Mimic __dict__ functionality.

        :return:
        :rtype: dict
        """
        return {
            "tld": self.tld,
            "domain": self.domain,
            "subdomain": self.subdomain,
            "fld": self.fld,
            "parsed_url": self.parsed_url,
        }
