from typing import Any

from .parse import TLDBaseKeys
from .errors import NotFoundError
from . import tldparsers


class DomainParser:
    _no_match_checks = [
        "no match",
        "not found",
        "no entries found",
        "invalid query",
        "domain name not known",
        "no object found",
        "available for re-registration",
        "object does not exist",
        "domain you requested is not known",
    ]

    def __init__(self, ignore_not_found: bool = False) -> None:
        self.ignore_not_found = ignore_not_found

    def parse(self, blob: str, tld: str) -> dict[TLDBaseKeys, Any]:
        low_blob = blob.lower()
        if not self.ignore_not_found and any(
            n in low_blob for n in self._no_match_checks
        ):
            raise NotFoundError("Domain not found!")
        parser = self._init_parser(tld)
        return parser.parse(blob)

    @staticmethod
    def _init_parser(tld: str) -> tldparsers.TLDParser:
        """
        Retrieves the parser instance which can most accurately extract
        key/value pairs from the whois server output for the given `tld`.

        :param tld: the top level domain
        :return: instance of TLDParser or a TLDParser subclass
        """

        try:
            cls_ = getattr(tldparsers, f"Regex{tld.upper()}")
        except AttributeError:
            # The TLDParser can handle all "Generic" and some "Country-Code"
            # TLDs.  If the parsed output of lookup is not what you expect or
            # even incorrect, check and modify the existing Regex subclass or
            # create a new one.
            cls_ = tldparsers.TLDParser
        return cls_()
