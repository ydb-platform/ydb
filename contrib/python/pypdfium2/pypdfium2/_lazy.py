# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

# see https://gist.github.com/mara004/6915e904797916b961e9c53b4fc874ec for alternative approaches to deferred imports

import sys
import logging
import functools

logger = logging.getLogger(__name__)

if sys.version_info < (3, 8):  # pragma: no cover
    # NOTE alternatively, we could write our own cached property backport with python's descriptor protocol
    def cached_property(func):
        return property( functools.lru_cache(maxsize=1)(func) )
else:
    cached_property = functools.cached_property


class _LazyClass:
    
    @cached_property
    def PIL_Image(self):
        logger.debug("Evaluating lazy import 'PIL.Image' ...")
        import PIL.Image; return PIL.Image
    
    @cached_property
    def numpy(self):
        logger.debug("Evaluating lazy import 'numpy' ...")
        import numpy; return numpy

Lazy = _LazyClass()
