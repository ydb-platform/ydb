"""[DEPRECATED] Use 'hijridate' package instead."""

import warnings

# Issue deprecation warning
warnings.warn(
    "hijri-converter is deprecated. Use 'hijridate' instead: "
    "pip install hijridate==2.3.0",
    DeprecationWarning,
    stacklevel=2
)

__version__ = "2.3.2.post1"

from hijri_converter.convert import Gregorian, Hijri
