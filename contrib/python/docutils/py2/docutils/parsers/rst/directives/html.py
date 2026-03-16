# $Id: html.py 8896 2021-11-18 19:48:58Z grubert $
# Author: David Goodger <goodger@python.org>
# Copyright: This module has been placed in the public domain.

"""
Dummy module for backwards compatibility.

This module is provisional: it will be removed in Docutils version 1.2.
"""

__docformat__ = 'reStructuredText'

import warnings

from docutils.parsers.rst.directives.misc import MetaBody, Meta

warnings.warn('The `docutils.parsers.rst.directive.html` module'
              ' will be removed in Docutils 1.2.'
              ' Since Docutils 0.18, the "Meta" node is defined in'
              ' `docutils.parsers.rst.directives.misc`.',
              DeprecationWarning, stacklevel=2)
