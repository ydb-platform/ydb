class FastAPIPaginationError(Exception):
    pass


class UninitializedConfigurationError(FastAPIPaginationError, RuntimeError):
    pass


class UnsupportedFeatureError(FastAPIPaginationError):
    pass


__all__ = [
    "FastAPIPaginationError",
    "UninitializedConfigurationError",
    "UnsupportedFeatureError",
]
