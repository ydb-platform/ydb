# SPDX-License-Identifier: MIT
# Copyright (c) 2019 Martijn Pieters
# Licensed under the MIT license as detailed in LICENSE.txt

from importlib.metadata import version  # type: ignore

from .leakybucket import AsyncLimiter

__version__ = version("aiolimiter")
__all__ = ["AsyncLimiter"]
