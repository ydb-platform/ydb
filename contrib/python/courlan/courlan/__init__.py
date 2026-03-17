"""
coURLan: Clean, filter, normalize, and sample URLs
"""

# meta
__title__ = "courlan"
__author__ = "Adrien Barbaresi"
__license__ = "Apache-2.0"
__copyright__ = "Copyright 2020-present, Adrien Barbaresi"
__version__ = "1.3.2"


# imports
from .clean import clean_url, normalize_url, scrub_url
from .core import check_url, extract_links
from .filters import (
    is_navigation_page,
    is_not_crawlable,
    is_valid_url,
    lang_filter,
    validate_url,
)
from .sampling import sample_urls
from .urlstore import UrlStore
from .urlutils import (
    extract_domain,
    filter_urls,
    fix_relative_urls,
    get_base_url,
    get_host_and_path,
    get_hostinfo,
    is_external,
)

__all__ = [
    "clean_url",
    "normalize_url",
    "scrub_url",
    "check_url",
    "extract_links",
    "is_navigation_page",
    "is_not_crawlable",
    "is_valid_url",
    "lang_filter",
    "validate_url",
    "sample_urls",
    "UrlStore",
    "extract_domain",
    "filter_urls",
    "fix_relative_urls",
    "get_base_url",
    "get_host_and_path",
    "get_hostinfo",
    "is_external",
]
