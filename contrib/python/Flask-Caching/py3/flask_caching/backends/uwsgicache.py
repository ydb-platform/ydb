import warnings

from flask_caching.contrib.uwsgicache import UWSGICache as _UWSGICache


class UWSGICache(_UWSGICache):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "Importing UWSGICache from flask_caching.backends is deprecated, "
            "use flask_caching.contrib.uwsgicache.UWSGICache instead",
            category=DeprecationWarning,
            stacklevel=2,
        )

        super().__init__(*args, **kwargs)
