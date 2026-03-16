# © Copyright 2020-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

__title__ = "aiocsv"
__description__ = "Asynchronous CSV reading/writing"
__version__ = "1.4.0"

__url__ = "https://github.com/MKuranowski/aiocsv"
__author__ = "Mikołaj Kuranowski"
__email__ = "mkuranowski+pypackages@gmail.com"

__copyright__ = "© Copyright 2020-2024 Mikołaj Kuranowski"
__license__ = "MIT"

from .readers import AsyncDictReader, AsyncReader
from .writers import AsyncDictWriter, AsyncWriter

__all__ = ["AsyncReader", "AsyncDictReader", "AsyncWriter", "AsyncDictWriter"]
