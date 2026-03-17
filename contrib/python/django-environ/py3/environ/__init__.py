# This file is part of the django-environ.
#
# Copyright (c) 2024-present, Daniele Faraglia <daniele.faraglia@gmail.com>
# Copyright (c) 2021-2024, Serghei Iakovlev <oss@serghei.pl>
# Copyright (c) 2013-2021, Daniele Faraglia <daniele.faraglia@gmail.com>
#
# For the full copyright and license information, please view
# the LICENSE.txt file that was distributed with this source code.

"""The top-level module for django-environ package.

This module tracks the version of the package as well as the base
package info used by various functions within django-environ.

Refer to the `documentation <https://django-environ.readthedocs.io/en/latest/>`_
for details on the use of this package.
"""  # noqa: E501

from .environ import *


__copyright__ = 'Copyright (C) 2013-2026 Daniele Faraglia'
"""The copyright notice of the package."""

__version__ = '0.13.0'
"""The version of the package."""

__license__ = 'MIT'
"""The license of the package."""

__author__ = 'Daniele Faraglia'
"""The author of the package."""

__author_email__ = 'daniele.faraglia@gmail.com'
"""The email of the author of the package."""

__maintainer__ = 'Daniele Faraglia'
"""The maintainer of the package."""

__maintainer_email__ = 'daniele.faraglia@gmail.com'
"""The email of the maintainer of the package."""

__url__ = 'https://django-environ.readthedocs.org'
"""The URL of the package."""

# pylint: disable=line-too-long
__description__ = 'A package that allows you to utilize 12factor inspired environment variables to configure your Django application.'  # noqa: E501
"""The description of the package."""
