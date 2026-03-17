"""
This module contains version checking helpers.

"""

import functools
from importlib.metadata import version as get_version

from packaging.version import Version

try:
    import ssl
except ImportError:
    ssl = None


PYMONGO = Version(get_version("pymongo"))


@functools.lru_cache(maxsize=16)
def _lt(version: str) -> bool:
    """Return ``True`` if ``pymongo.version`` is less than `version`.

    :param str version: Version string

    """
    return PYMONGO < Version(version)


@functools.lru_cache(maxsize=16)
def _gte(version: str) -> bool:
    """Return ``True`` if ``pymongo.version`` is greater than or equal to
    `version`.

    :param str version: Version string

    """
    return PYMONGO >= Version(version)


def _clean(kwargs: dict) -> None:
    """Mutate `kwargs` to handle backwards incompatible version discrepancies.

    Currently only changes the `safe` param into `w`. If ``safe=False`` is
    passed it is transformed into ``w=0``.

    Otherwise `safe` is removed from the args.
    """
    if _lt("3.0"):
        return

    if "safe" not in kwargs:
        return

    if kwargs["safe"] is False:
        del kwargs["safe"]
        kwargs["w"] = 0
        return

    del kwargs["safe"]


def _clean_create_index(kwargs: dict) -> None:
    """Mutate `kwargs` to handle backwards incompatible version discrepancies.

    Versions older than 4.1:

    - Removes `comment` keyword argument

    Versions older than 3.11:

    - Removes `hidden` keyword argument

    Versions older than 3.6:

    - Removes `session` keyword argument

    Versions newer than 3.0:

    - Removes `cache_for` keyword argument
    - Changes `drop_dups` to `dropDups`
    - Changes `bucket_size` to `bucketSize`
    - Changes `key_or_list` to `keys`

    """
    if _gte("3.0"):
        kwargs.pop("cache_for", None)

        if "drop_dups" in kwargs:
            kwargs["dropDups"] = kwargs.pop("drop_dups")

        if "bucket_size" in kwargs:
            kwargs["bucketSize"] = kwargs.pop("bucket_size")

        if "key_or_list" in kwargs:
            kwargs["keys"] = kwargs.pop("key_or_list")

    if _lt("2.3") and "cache_for" in kwargs:
        kwargs["ttl"] = kwargs.pop("cache_for")

    if _lt("3.6"):
        kwargs.pop("session", None)
        return

    if _lt("3.11"):
        kwargs.pop("hidden", None)
        return

    if _lt("4.1"):
        kwargs.pop("comment", None)


def _clean_connection_kwargs(kwargs: dict) -> None:
    """Mutate `kwargs` to handle backwards incompatible version discrepancies.

    Currently only changes the `safe` param into `w`. If ``safe=False`` is
    passed it is transformed into ``w=0``.

    Otherwise `safe` is removed from the args.
    """
    if _gte("2.1.0") and _lt("2.2.0"):
        # This causes an error for the 2.1.x versions of Pymongo, so we
        # remove it
        kwargs.pop("auto_start_request")
        kwargs.pop("use_greenlets")

    if _gte("3.0.0"):
        # Handle removed keywords
        kwargs.pop("use_greenlets")
        kwargs.pop("auto_start_request")

        # Handle changed keywords
        if "max_pool_size" in kwargs:
            kwargs["maxPoolSize"] = kwargs.pop("max_pool_size")

        # Handle other 3.0 stuff
        if kwargs.get("ssl"):
            kwargs.setdefault("ssl_cert_reqs", ssl.CERT_NONE)

    if _gte("4.0.0"):
        # SSL related keywords that have changed
        if "ssl_pem_passphrase" in kwargs:
            kwargs["tlsCertificateKeyFilePassword"] = kwargs.pop("ssl_pem_passphrase")

        if "ssl_ca_certs" in kwargs:
            kwargs["tlsCAFile"] = kwargs.pop("ssl_ca_certs")

        if "ssl_crl_file" in kwargs:
            kwargs["tlsCRLFile"] = kwargs.pop("ssl_crl_file")

        if "ssl_match_hostname" in kwargs:
            kwargs["tlsAllowInvalidHostnames"] = kwargs.pop("ssl_match_hostname")

        if "ssl_certfile" in kwargs or "ssl_keyfile" in kwargs:
            raise ValueError(
                "ssl_certfile and ssl_keyfile are no longer supported. "
                "Use tlsCertificateKeyFile instead (https://pymongo.readthedocs.io/en/stable/migrate-to-pymongo4.html#renamed-uri-options)."
            )

        if "ssl_cert_reqs" in kwargs:
            ssl_cert_reqs = kwargs.pop("ssl_cert_reqs")
            if ssl_cert_reqs == ssl.CERT_NONE or ssl_cert_reqs == ssl.CERT_OPTIONAL:
                kwargs["tlsAllowInvalidCertificates"] = True
            else:
                kwargs["tlsCertificateKeyFilePassword"] = False

        # Other changed keywords
        if "j" in kwargs:
            kwargs["journal"] = kwargs.pop("j")

        if "wtimeout" in kwargs:
            kwargs["wtimeoutMS"] = kwargs.pop("wtimeout")
