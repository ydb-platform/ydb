#!/usr/bin/env python3
# tifffile/__main__.py

"""Display image and metadata in TIFF file."""

from __future__ import annotations

import sys

from .tifffile import main

sys.exit(main())
