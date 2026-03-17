"""This submodule contains utility code for Prance."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ("iterators", "fs", "formats", "resolver", "url", "path", "exceptions")


def stringify_keys(data):
    """
    Recursively stringify keys in a dict-like object.

    :param dict-like data: A dict-like object to stringify keys in.
    :return: A new dict-like object of the same type with stringified keys,
        but the same values.
    """
    from collections.abc import Mapping

    assert isinstance(data, Mapping)

    ret = type(data)()
    for key, value in data.items():
        if not isinstance(key, str):
            key = str(key)
        if isinstance(value, Mapping):
            value = stringify_keys(value)
        ret[key] = value
    return ret


def validation_backends():
    """Return a list of validation backends supported by the environment."""
    ret = []

    try:
        import flex  # noqa: F401

        ret.append("flex")  # pragma: nocover
    except (ImportError, SyntaxError):  # pragma: nocover
        pass

    try:
        import openapi_spec_validator  # noqa: F401

        ret.append("openapi-spec-validator")  # pragma: nocover
    except (ImportError, SyntaxError):  # pragma: nocover
        pass

    try:
        import swagger_spec_validator  # noqa: F401

        ret.append("swagger-spec-validator")  # pragma: nocover
    except (ImportError, SyntaxError):  # pragma: nocover
        pass

    return tuple(ret)


def default_validation_backend():
    """Return the default validation backend, or raise an error."""
    backends = validation_backends()
    if len(backends) <= 0:  # pragma: nocover
        raise RuntimeError(
            "No validation backend available! Install one of "
            '"flex", "openapi-spec-validator" or "swagger-spec-validator".'
        )
    return backends[0]
