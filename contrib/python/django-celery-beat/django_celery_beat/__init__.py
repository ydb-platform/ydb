"""Database-backed Periodic Tasks."""
# :copyright: (c) 2016, Ask Solem.
#             All rights reserved.
# :license:   BSD (3 Clause), see LICENSE for more details.
import re

from collections import namedtuple

import django

__version__ = '2.3.0'
__author__ = 'Asif Saif Uddin, Ask Solem'
__contact__ = 'auvipy@gmail.com, ask@celeryproject.org'
__homepage__ = 'https://github.com/celery/django-celery-beat'
__docformat__ = 'restructuredtext'

# -eof meta-

version_info_t = namedtuple('version_info_t', (
    'major', 'minor', 'micro', 'releaselevel', 'serial',
))

# bumpversion can only search for {current_version}
# so we have to parse the version here.
_temp = re.match(
    r'(\d+)\.(\d+).(\d+)(.+)?', __version__).groups()
VERSION = version_info = version_info_t(
    int(_temp[0]), int(_temp[1]), int(_temp[2]), _temp[3] or '', '')
del(_temp)
del(re)

__all__ = []

if django.VERSION < (3, 2):
    default_app_config = 'django_celery_beat.apps.BeatConfig'
