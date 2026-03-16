#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#
import os
import pickle

from functools import partial
from hashlib import md5

import attr

from commoncode import ignore
from commoncode.datautils import attribute
from commoncode.fileutils import resource_iter
from commoncode.fileutils import create_dir

from scancode_config import licensedcode_cache_dir
from scancode_config import scancode_cache_dir
from scancode_config import scancode_src_dir
from scancode_config import SCANCODE_DEV_MODE

"""
An on-disk persistent cache of LicenseIndex and related data structures such as
the licenses database. The data are  pickled and invalidated if there are any
changes in the code or licenses text or rules. Loading and dumping the cached
pickle is safe to use across multiple processes using lock files.
"""

# This is the Pickle protocol we use, which was added in Python 3.4.
PICKLE_PROTOCOL = 4

# global in-memory cache of the LicenseCache
_LICENSE_CACHE = None

LICENSE_INDEX_LOCK_TIMEOUT = 60 * 4
LICENSE_INDEX_DIR = 'license_index'
LICENSE_INDEX_FILENAME = 'index_cache'
LICENSE_LOCKFILE_NAME = 'scancode_license_index_lockfile'
LICENSE_CHECKSUM_FILE = 'scancode_license_index_tree_checksums'


@attr.s(slots=True)
class LicenseCache:
    """
    Represent cachable/pickable LicenseIndex and index-related objects.
    """
    db = attribute(help='mapping of License objects by key')
    index = attribute(help='LicenseIndex object')
    licensing = attribute(help='Licensing object')
    spdx_symbols = attribute(help='mapping of LicenseSymbol objects by SPDX key')
    unknown_spdx_symbol = attribute(help='LicenseSymbol object')

    @staticmethod
    def load_or_build(
        licensedcode_cache_dir=licensedcode_cache_dir,
        scancode_cache_dir=scancode_cache_dir,
        check_consistency=SCANCODE_DEV_MODE,
        # used for testing only
        timeout=LICENSE_INDEX_LOCK_TIMEOUT,
        tree_base_dir=scancode_src_dir,
        licenses_data_dir=None,
        rules_data_dir=None,
    ):
        """
        Load or build and save and return a LicenseCache object.

        We either load a cached LicenseIndex or build and cache the index.
        On the side, we load cached or build license db, SPDX symbols and other
        license-related data structures.

        - If the cache does not exist, a new index is built and cached.
        - If `check_consistency` is True, the cache is checked for consistency and
          rebuilt if inconsistent or stale.
        - If `check_consistency` is False, the cache is NOT checked for consistency and
          if the cache files exist but ARE stale, the cache WILL NOT be rebuilt
        """
        idx_cache_dir = os.path.join(licensedcode_cache_dir, LICENSE_INDEX_DIR)
        create_dir(idx_cache_dir)
        cache_file = os.path.join(idx_cache_dir, LICENSE_INDEX_FILENAME)

        has_cache = os.path.exists(cache_file) and os.path.getsize(cache_file)

        # bypass check if no consistency check is needed
        if has_cache and not check_consistency:
            try:
                return load_cache_file(cache_file)
            except Exception as e:
                # work around some rare Windows quirks
                import traceback
                print('Inconsistent License cache: checking and rebuilding index.')
                print(str(e))
                print(traceback.format_exc())

        from licensedcode.models import licenses_data_dir as ldd
        from licensedcode.models import rules_data_dir as rdd
        from licensedcode.models import load_licenses
        from scancode import lockfile

        licenses_data_dir = licenses_data_dir or ldd
        rules_data_dir = rules_data_dir or rdd

        lock_file = os.path.join(scancode_cache_dir, LICENSE_LOCKFILE_NAME)
        checksum_file = os.path.join(scancode_cache_dir, LICENSE_CHECKSUM_FILE)

        has_tree_checksum = os.path.exists(checksum_file)

        # here, we have no cache or we want a validity check: lock, check
        # and build or rebuild as needed
        try:
            # acquire lock and wait until timeout to get a lock or die
            with lockfile.FileLock(lock_file).locked(timeout=timeout):
                current_checksum = None
                # is the current cache consistent or stale?
                if has_cache and has_tree_checksum:
                    # if we have a saved cached index
                    # load saved tree_checksum and compare with current tree_checksum
                    with open(checksum_file) as etcs:
                        existing_checksum = etcs.read()

                    current_checksum = tree_checksum(tree_base_dir=tree_base_dir)
                    if current_checksum == existing_checksum:
                        # The cache is consistent with the latest code and data
                        # load and return
                        return load_cache_file(cache_file)

                # Here, the cache is not consistent with the latest code and
                # data: It is either stale or non-existing: we need to
                # rebuild all cached data (e.g. mostly the index) and cache it

                licenses_db = load_licenses(licenses_data_dir=licenses_data_dir)
                index = build_index(
                    licenses_db=licenses_db,
                    licenses_data_dir=licenses_data_dir,
                    rules_data_dir=rules_data_dir,
                )
                spdx_symbols = build_spdx_symbols(licenses_db=licenses_db)
                unknown_spdx_symbol = build_unknown_spdx_symbol(licenses_db=licenses_db)
                licensing = build_licensing(licenses_db=licenses_db)

                license_cache = LicenseCache(
                    db=licenses_db,
                    index=index,
                    licensing=licensing,
                    spdx_symbols=spdx_symbols,
                    unknown_spdx_symbol=unknown_spdx_symbol,
                )

                # save the cache as pickle new tree checksum
                with open(cache_file, 'wb') as fn:
                    pickle.dump(license_cache, fn, protocol=PICKLE_PROTOCOL)

                current_checksum = tree_checksum(tree_base_dir=tree_base_dir)
                with open(checksum_file, 'w') as ctcs:
                    ctcs.write(current_checksum)

                return license_cache

        except lockfile.LockTimeout:
            # TODO: handle unable to lock in a nicer way
            raise


def build_index(licenses_db=None, licenses_data_dir=None, rules_data_dir=None):
    """
    Return an index built from rules and licenses directories
    """
    from licensedcode.index import LicenseIndex
    from licensedcode.models import get_rules
    from licensedcode.models import get_all_spdx_key_tokens
    from licensedcode.models import licenses_data_dir as ldd
    from licensedcode.models import rules_data_dir as rdd
    from licensedcode.models import load_licenses

    licenses_data_dir = licenses_data_dir or ldd
    rules_data_dir = rules_data_dir or rdd

    licenses_db = licenses_db or load_licenses(licenses_data_dir=licenses_data_dir)
    spdx_tokens = set(get_all_spdx_key_tokens(licenses_db))
    rules = get_rules(licenses_db=licenses_db, rules_data_dir=rules_data_dir)
    return LicenseIndex(rules, _spdx_tokens=spdx_tokens)


def build_licensing(licenses_db=None):
    """
    Return a `license_expression.Licensing` objet built from a `licenses_db`
    mapping of {key: License} or the standard license db.
    """
    from license_expression import LicenseSymbolLike
    from license_expression import Licensing
    from licensedcode.models import load_licenses

    licenses_db = licenses_db or load_licenses()
    return Licensing((LicenseSymbolLike(lic) for lic in licenses_db.values()))


def build_spdx_symbols(licenses_db=None):
    """
    Return a mapping of {lowercased SPDX license key: LicenseSymbolLike} where
    LicenseSymbolLike wraps a License object loaded from a `licenses_db` mapping
    of {key: License} or the standard license db.
    """
    from license_expression import LicenseSymbolLike
    from licensedcode.models import load_licenses

    licenses_db = licenses_db or load_licenses()
    symbols_by_spdx_key = {}

    for lic in licenses_db.values():
        if not (lic.spdx_license_key or lic.other_spdx_license_keys):
            continue

        symbol = LicenseSymbolLike(lic)
        if lic.spdx_license_key:
            slk = lic.spdx_license_key.lower()
            existing = symbols_by_spdx_key.get(slk)

            if existing:
                raise ValueError(
                    'Duplicated SPDX license key: %(slk)r defined in '
                    '%(lic)r and %(existing)r' % locals())

            symbols_by_spdx_key[slk] = symbol

        for other_spdx in lic.other_spdx_license_keys:
            if not (other_spdx and other_spdx.strip()):
                continue
            slk = other_spdx.lower()
            existing = symbols_by_spdx_key.get(slk)

            if existing:
                raise ValueError(
                    'Duplicated "other" SPDX license key: %(slk)r defined '
                    'in %(lic)r and %(existing)r' % locals())
            symbols_by_spdx_key[slk] = symbol

    return symbols_by_spdx_key


def build_unknown_spdx_symbol(licenses_db=None):
    """
    Return the unknown SPDX license symbol given a `licenses_db` mapping of
    {key: License} or the standard license db.
    """
    from license_expression import LicenseSymbolLike
    from licensedcode.models import load_licenses
    licenses_db = licenses_db or load_licenses()
    return LicenseSymbolLike(licenses_db['unknown-spdx'])


def get_cache(check_consistency=SCANCODE_DEV_MODE):
    """
    Optionally return and either load or build and cache a LicenseCache.
    """
    populate_cache(check_consistency=check_consistency)
    global _LICENSE_CACHE
    return _LICENSE_CACHE


def populate_cache(check_consistency=SCANCODE_DEV_MODE):
    """
    Load or build and cache a LicenseCache. Return None.
    """
    global _LICENSE_CACHE
    if not _LICENSE_CACHE:
        _LICENSE_CACHE = LicenseCache.load_or_build(
            licensedcode_cache_dir=licensedcode_cache_dir,
            scancode_cache_dir=scancode_cache_dir,
            check_consistency=check_consistency,
            # used for testing only
            timeout=LICENSE_INDEX_LOCK_TIMEOUT,
            tree_base_dir=scancode_src_dir,
        )


def load_cache_file(cache_file):
    """
    Return a LicenseCache loaded from ``cache_file``.
    """
    with open(cache_file, 'rb') as lfc:
        # Note: weird but read() + loads() is much (twice++???) faster than load()
        try:
            return pickle.load(lfc)
        except Exception as e:
            msg = (
                'ERROR: Failed to load license cache (the file may be corrupted ?).\n'
                f'Please delete "{cache_file}" and retry.\n'
                'If the problem persists, copy this error message '
                'and submit a bug report at https://github.com/nexB/scancode-toolkit/issues/'
            )
            raise Exception(msg) from e


_ignored_from_hash = partial(
    ignore.is_ignored,
    ignores={
        '*.pyc': 'pyc files',
        '*~': 'temp gedit files',
        '*.swp': 'vi swap files',
    },
    unignores={}
)

licensedcode_dir = os.path.join(scancode_src_dir, 'licensedcode')


def tree_checksum(tree_base_dir=licensedcode_dir, _ignored=_ignored_from_hash):
    """
    Return a checksum computed from a file tree using the file paths, size and
    last modified time stamps. The purpose is to detect is there has been any
    modification to source code or data files and use this as a proxy to verify
    the cache consistency. This includes the actual cached index file.

    NOTE: this is not 100% fool proof but good enough in practice.
    """
    resources = resource_iter(tree_base_dir, ignored=_ignored, with_dirs=False)
    hashable = (pth + str(os.path.getmtime(pth)) + str(os.path.getsize(pth)) for pth in resources)
    hashable = ''.join(sorted(hashable))
    hashable = hashable.encode('utf-8')
    return md5(hashable).hexdigest()


def get_index(check_consistency=SCANCODE_DEV_MODE):
    """
    Return and eventually build and cache a LicenseIndex.
    """
    return get_cache(check_consistency=check_consistency).index


get_cached_index = get_index


def get_licenses_db(check_consistency=SCANCODE_DEV_MODE):
    """
    Return a mapping of license key -> license object.
    """
    return get_cache(check_consistency=check_consistency).db


def get_licensing(check_consistency=SCANCODE_DEV_MODE):
    """
    Return a license_expression.Licensing objet built from the all the licenses.
    """
    return get_cache(check_consistency=check_consistency).licensing


def get_unknown_spdx_symbol(check_consistency=SCANCODE_DEV_MODE):
    """
    Return the unknown SPDX license symbol.
    """
    return get_cache(check_consistency=check_consistency).unknown_spdx_symbol


def get_spdx_symbols(licenses_db=None, check_consistency=SCANCODE_DEV_MODE):
    """
    Return a mapping of {lowercased SPDX license key: LicenseSymbolLike} where
    LicenseSymbolLike wraps a License object
    """
    if licenses_db:
        return build_spdx_symbols(licenses_db)
    return get_cache(check_consistency=check_consistency).spdx_symbols
