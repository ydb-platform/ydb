# name space package to host third party extensions

from __future__ import annotations

import pkgutil

__path__ = pkgutil.extend_path(__path__, __name__)
