from __future__ import unicode_literals

import argparse
import sys
from codecs import open as codecs_open
from functools import lru_cache
from os.path import isabs
from typing import Dict, List, Optional, Tuple, Type, Union
from urllib.parse import SplitResult, urlsplit

from .base import BaseTLDSourceParser, Registry
from .exceptions import (
    TldBadUrl,
    TldDomainNotFound,
    TldImproperlyConfigured,
    TldIOError,
)
from .helpers import project_dir
from .result import Result
from .trie import Trie

# codecs_open = open


__author__ = "Artur Barseghyan"
__copyright__ = "2013-2025 Artur Barseghyan"
__license__ = "MPL-1.1 OR GPL-2.0-only OR LGPL-2.1-or-later"
__all__ = (
    "BaseMozillaTLDSourceParser",
    "get_fld",
    "get_tld",
    "get_tld_names",
    "get_tld_names_container",
    "is_tld",
    "MozillaTLDSourceParser",
    "MozillaPublicOnlyTLDSourceParser",
    "parse_tld",
    "pop_tld_names_container",
    "process_url",
    "reset_tld_names",
    "Result",
    "tld_names",
    "update_tld_names",
    "update_tld_names_cli",
    "update_tld_names_container",
)

tld_names: Dict[str, Trie] = {}


def get_tld_names_container() -> Dict[str, Trie]:
    """Get container of all tld names.

    :return:
    :rtype dict:
    """
    global tld_names
    return tld_names


def update_tld_names_container(
    tld_names_local_path: str, trie_obj: Trie
) -> None:
    """Update TLD Names container item.

    :param tld_names_local_path:
    :param trie_obj:
    :return:
    """
    global tld_names
    # tld_names.update({tld_names_local_path: trie_obj})
    tld_names[tld_names_local_path] = trie_obj


def pop_tld_names_container(tld_names_local_path: str) -> None:
    """Remove TLD names container item.

    :param tld_names_local_path:
    :return:
    """
    global tld_names
    tld_names.pop(tld_names_local_path, None)


@lru_cache(maxsize=128, typed=True)
def update_tld_names(
    fail_silently: bool = False, parser_uid: str = None
) -> bool:
    """Update TLD names.

    :param fail_silently:
    :param parser_uid:
    :return:
    """
    results: List[bool] = []
    results_append = results.append
    if parser_uid:
        parser_cls = Registry.get(parser_uid, None)
        if parser_cls and parser_cls.source_url:
            results_append(
                parser_cls.update_tld_names(fail_silently=fail_silently)
            )
    else:
        for parser_uid, parser_cls in Registry.items():
            if parser_cls and parser_cls.source_url:
                results_append(
                    parser_cls.update_tld_names(fail_silently=fail_silently)
                )

    return all(results)


def update_tld_names_cli() -> int:
    """CLI wrapper for update_tld_names.

    Since update_tld_names returns True on success, we need to negate the
    result to match CLI semantics.
    """
    parser = argparse.ArgumentParser(description="Update TLD names")
    parser.add_argument(
        "parser_uid",
        nargs="?",
        default=None,
        help="UID of the parser to update TLD names for.",
    )
    parser.add_argument(
        "--fail-silently",
        dest="fail_silently",
        default=False,
        action="store_true",
        help="Fail silently",
    )
    args = parser.parse_args(sys.argv[1:])
    parser_uid = args.parser_uid
    fail_silently = args.fail_silently
    return int(
        not update_tld_names(parser_uid=parser_uid, fail_silently=fail_silently)
    )


def get_tld_names(
    fail_silently: bool = False,
    retry_count: int = 0,
    parser_class: Type[BaseTLDSourceParser] = None,
) -> Dict[str, Trie]:
    """Build the ``tlds`` list if empty. Recursive.

    :param fail_silently: If set to True, no exceptions are raised and None
        is returned on failure.
    :param retry_count: If greater than 1, we raise an exception in order
        to avoid infinite loops.
    :param parser_class:
    :type fail_silently: bool
    :type retry_count: int
    :type parser_class: BaseTLDSourceParser
    :return: List of TLD names
    :rtype: obj:`tld.utils.Trie`
    """
    if not parser_class:
        parser_class = MozillaTLDSourceParser

    return parser_class.get_tld_names(
        fail_silently=fail_silently, retry_count=retry_count
    )


# **************************************************************************
# **************************** Parser classes ******************************
# **************************************************************************


class BaseMozillaTLDSourceParser(BaseTLDSourceParser):
    @classmethod
    def get_tld_names(
        cls, fail_silently: bool = False, retry_count: int = 0
    ) -> Optional[Dict[str, Trie]]:
        """Parse.

        :param fail_silently:
        :param retry_count:
        :return:
        """
        if retry_count > 1:
            if fail_silently:
                return None
            else:
                raise TldIOError

        global tld_names
        _tld_names = tld_names
        # _tld_names = get_tld_names_container()

        # If already loaded, return
        if (
            cls.local_path in _tld_names
            and _tld_names[cls.local_path] is not None
        ):
            return _tld_names

        try:
            # Load the TLD names file
            if isabs(cls.local_path):
                local_path = cls.local_path
                local_file = codecs_open(local_path, "r", encoding="utf8")
            else:
                local_path = project_dir(cls.local_path)
                local_file = local_path.open("r", encoding="utf8")

            trie = Trie()
            trie_add = trie.add  # Performance opt
            # Make a list of it all, strip all garbage
            private_section = False
            include_private = cls.include_private

            for line in local_file:
                if "===BEGIN PRIVATE DOMAINS===" in line:
                    private_section = True

                if private_section and not include_private:
                    break

                # Puny code TLD names
                if "// xn--" in line:
                    line = line.split()[1]

                if line[0] in ("/", "\n"):
                    continue

                trie_add(f"{line.strip()}", private=private_section)

            update_tld_names_container(cls.local_path, trie)

            local_file.close()
        except IOError:
            # Grab the file
            cls.update_tld_names(fail_silently=fail_silently)
            # Increment ``retry_count`` in order to avoid infinite loops
            retry_count += 1
            # Run again
            return cls.get_tld_names(
                fail_silently=fail_silently, retry_count=retry_count
            )
        except Exception as err:
            if fail_silently:
                return None
            else:
                raise err
        finally:
            try:
                local_file.close()
            except Exception:
                pass

        return _tld_names


class MozillaTLDSourceParser(BaseMozillaTLDSourceParser):
    """Mozilla TLD source."""

    uid: str = "mozilla"
    source_url: str = "https://publicsuffix.org/list/public_suffix_list.dat"
    local_path: str = "res/effective_tld_names.dat.txt"


class MozillaPublicOnlyTLDSourceParser(BaseMozillaTLDSourceParser):
    """Mozilla TLD source."""

    uid: str = "mozilla_public_only"
    source_url: str = (
        "https://publicsuffix.org/list/public_suffix_list.dat?publiconly"
    )
    local_path: str = "res/effective_tld_names_public_only.dat.txt"
    include_private: bool = False


# **************************************************************************
# **************************** Core functions ******************************
# **************************************************************************


def process_url(
    url: Union[str, SplitResult],
    fail_silently: bool = False,
    fix_protocol: bool = False,
    search_public: bool = True,
    search_private: bool = True,
    parser_class: Type[BaseTLDSourceParser] = MozillaTLDSourceParser,
) -> Union[Tuple[List[str], int, SplitResult], Tuple[None, None, SplitResult]]:
    """Process URL.

    :param parser_class:
    :param url:
    :param fail_silently:
    :param fix_protocol:
    :param search_public:
    :param search_private:
    :return:
    """
    if not (search_public or search_private):
        raise TldImproperlyConfigured(
            "Either `search_public` or `search_private` (or both) shall be "
            "set to True."
        )

    # Init
    _tld_names = get_tld_names(
        fail_silently=fail_silently, parser_class=parser_class
    )

    if not isinstance(url, SplitResult):
        if fix_protocol and not url.startswith(("//", "http://", "https://")):
            url = f"https://{url}"

        # Get parsed URL as we might need it later
        try:
            parsed_url = urlsplit(url)
        except ValueError as e:
            if fail_silently:
                return None, None, url
            else:
                raise e
    else:
        parsed_url = url

    # Get (sub) domain name
    domain_name = parsed_url.hostname

    if not domain_name:
        if fail_silently:
            return None, None, parsed_url
        else:
            raise TldBadUrl(url=url)

    domain_name = domain_name.lower()

    # This will correctly handle dots at the end of domain name in URLs like
    # https://github.com............/barseghyanartur/tld/
    if domain_name.endswith("."):
        domain_name = domain_name.rstrip(".")

    domain_parts = domain_name.split(".")
    tld_names_local_path = parser_class.local_path

    # Now we query our Trie iterating on the domain parts in reverse order
    node = _tld_names[tld_names_local_path].root
    current_length = 0
    tld_length = 0
    match = None
    len_domain_parts = len(domain_parts)
    for i in range(len_domain_parts - 1, -1, -1):
        part = domain_parts[i]

        # Cannot go deeper
        if node.children is None:
            break

        # Exception
        if part == node.exception:
            break

        child = node.children.get(part)

        # Wildcards
        if child is None:
            child = node.children.get("*")

        # If the current part is not in current node's children, we can stop
        if child is None:
            break

        # Else we move deeper and increment our tld offset
        current_length += 1
        node = child

        if node.leaf:
            tld_length = current_length
            match = node

    # Checking the node we finished on is a leaf and is one we allow
    if (
        (match is None)
        or (not match.leaf)
        or (not search_public and not match.private)
        or (not search_private and match.private)
    ):
        if fail_silently:
            return None, None, parsed_url
        else:
            raise TldDomainNotFound(domain_name=domain_name)

    if len_domain_parts == tld_length:
        non_zero_i = -1  # hostname = tld
    else:
        non_zero_i = max(1, len_domain_parts - tld_length)

    return domain_parts, non_zero_i, parsed_url


def get_fld(
    url: Union[str, SplitResult],
    fail_silently: bool = False,
    fix_protocol: bool = False,
    search_public: bool = True,
    search_private: bool = True,
    parser_class: Type[BaseTLDSourceParser] = None,
    **kwargs,
) -> Optional[str]:
    """Extract the first level domain.

    Extract the top level domain based on the mozilla's effective TLD names
    dat file. Returns a string. May throw ``TldBadUrl`` or
    ``TldDomainNotFound`` exceptions if there's bad URL provided or no TLD
    match found respectively.

    :param url: URL to get top level domain from.
    :param fail_silently: If set to True, no exceptions are raised and None
        is returned on failure.
    :param fix_protocol: If set to True, missing or wrong protocol is
        ignored (https is appended instead).
    :param search_public: If set to True, search in public domains.
    :param search_private: If set to True, search in private domains.
    :param parser_class:
    :type url: str | SplitResult
    :type fail_silently: bool
    :type fix_protocol: bool
    :type search_public: bool
    :type search_private: bool
    :return: String with top level domain (if ``as_object`` argument
        is set to False) or a ``tld.utils.Result`` object (if ``as_object``
        argument is set to True); returns None on failure.
    :rtype: str
    """
    if "as_object" in kwargs:
        raise TldImproperlyConfigured(
            "`as_object` argument is deprecated for `get_fld`. Use `get_tld` "
            "instead."
        )

    if not parser_class:
        parser_class = (
            MozillaTLDSourceParser
            if search_private
            else MozillaPublicOnlyTLDSourceParser
        )

    domain_parts, non_zero_i, parsed_url = process_url(
        url=url,
        fail_silently=fail_silently,
        fix_protocol=fix_protocol,
        search_public=search_public,
        search_private=search_private,
        parser_class=parser_class,
    )

    if domain_parts is None:
        return None

    # This should be None when domain_parts is None
    # but mypy isn't quite smart enough to figure that out yet
    assert non_zero_i is not None
    if non_zero_i < 0:
        # hostname = tld
        return parsed_url.hostname

    return ".".join(domain_parts[non_zero_i - 1 :])


def get_tld(
    url: Union[str, SplitResult],
    fail_silently: bool = False,
    as_object: bool = False,
    fix_protocol: bool = False,
    search_public: bool = True,
    search_private: bool = True,
    parser_class: Type[BaseTLDSourceParser] = None,
) -> Optional[Union[str, Result]]:
    """Extract the top level domain.

    Extract the top level domain based on the mozilla's effective TLD names
    dat file. Returns a string. May throw ``TldBadUrl`` or
    ``TldDomainNotFound`` exceptions if there's bad URL provided or no TLD
    match found respectively.

    :param url: URL to get top level domain from.
    :param fail_silently: If set to True, no exceptions are raised and None
        is returned on failure.
    :param as_object: If set to True, ``tld.utils.Result`` object is returned,
        ``domain``, ``suffix`` and ``tld`` properties.
    :param fix_protocol: If set to True, missing or wrong protocol is
        ignored (https is appended instead).
    :param search_public: If set to True, search in public domains.
    :param search_private: If set to True, search in private domains.
    :param parser_class:
    :type url: str | SplitResult
    :type fail_silently: bool
    :type as_object: bool
    :type fix_protocol: bool
    :type search_public: bool
    :type search_private: bool
    :return: String with top level domain (if ``as_object`` argument
        is set to False) or a ``tld.utils.Result`` object (if ``as_object``
        argument is set to True); returns None on failure.
    :rtype: str
    """
    if not parser_class:
        parser_class = (
            MozillaTLDSourceParser
            if search_private
            else MozillaPublicOnlyTLDSourceParser
        )

    domain_parts, non_zero_i, parsed_url = process_url(
        url=url,
        fail_silently=fail_silently,
        fix_protocol=fix_protocol,
        search_public=search_public,
        search_private=search_private,
        parser_class=parser_class,
    )

    if domain_parts is None:
        return None

    # This should be None when domain_parts is None
    # but mypy isn't quite smart enough to figure that out yet
    assert non_zero_i is not None

    if not as_object:
        if non_zero_i < 0:
            # hostname = tld
            return parsed_url.hostname
        return ".".join(domain_parts[non_zero_i:])

    if non_zero_i < 0:
        # hostname = tld
        subdomain = ""
        domain = ""
        # This is checked in `process_url`, but the type is
        # ambiguous (Optional[str]) so this assertion is just to satisfy mypy
        assert parsed_url.hostname is not None, "No hostname in URL"
        _tld = parsed_url.hostname
    else:
        subdomain = ".".join(domain_parts[: non_zero_i - 1])
        domain = ".".join(domain_parts[non_zero_i - 1 : non_zero_i])
        _tld = ".".join(domain_parts[non_zero_i:])

    return Result(
        subdomain=subdomain, domain=domain, tld=_tld, parsed_url=parsed_url
    )


def parse_tld(
    url: Union[str, SplitResult],
    fail_silently: bool = False,
    fix_protocol: bool = False,
    search_public: bool = True,
    search_private: bool = True,
    parser_class: Type[BaseTLDSourceParser] = None,
) -> Union[Tuple[None, None, None], Tuple[str, str, str]]:
    """Parse TLD into parts.

    :param url:
    :param fail_silently:
    :param fix_protocol:
    :param search_public:
    :param search_private:
    :param parser_class:
    :return: Tuple (tld, domain, subdomain)
    :rtype: tuple
    """
    if not parser_class:
        parser_class = (
            MozillaTLDSourceParser
            if search_private
            else MozillaPublicOnlyTLDSourceParser
        )

    try:
        obj = get_tld(
            url,
            fail_silently=fail_silently,
            as_object=True,
            fix_protocol=fix_protocol,
            search_public=search_public,
            search_private=search_private,
            parser_class=parser_class,
        )
        if obj is None:
            return None, None, None

        return obj.tld, obj.domain, obj.subdomain  # type: ignore

    except (TldBadUrl, TldDomainNotFound, TldImproperlyConfigured, TldIOError):
        pass

    return None, None, None


def is_tld(
    value: Union[str, SplitResult],
    search_public: bool = True,
    search_private: bool = True,
    parser_class: Type[BaseTLDSourceParser] = None,
) -> bool:
    """Check if given URL is tld.

    :param value: URL to get top level domain from.
    :param search_public: If set to True, search in public domains.
    :param search_private: If set to True, search in private domains.
    :param parser_class:
    :type value: str
    :type search_public: bool
    :type search_private: bool
    :return:
    :rtype: bool
    """
    if not parser_class:
        parser_class = (
            MozillaTLDSourceParser
            if search_private
            else MozillaPublicOnlyTLDSourceParser
        )

    _tld = get_tld(
        url=value,
        fail_silently=True,
        fix_protocol=True,
        search_public=search_public,
        search_private=search_private,
        parser_class=parser_class,
    )
    return value == _tld


def reset_tld_names(tld_names_local_path: str = None) -> None:
    """Reset the ``tld_names`` to empty value.

    If ``tld_names_local_path`` is given, removes specified
    entry from ``tld_names`` instead.

    :param tld_names_local_path:
    :type tld_names_local_path: str
    :return:
    """

    if tld_names_local_path:
        pop_tld_names_container(tld_names_local_path)
    else:
        global tld_names
        tld_names = {}
