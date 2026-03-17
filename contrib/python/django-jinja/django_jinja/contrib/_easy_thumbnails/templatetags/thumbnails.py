import logging

from easy_thumbnails.conf import settings
from easy_thumbnails.templatetags import thumbnail as _thumbnail
from django_jinja import library
from functools import wraps


logger = logging.getLogger(__name__)


def debug_silence(error_output=''):
    def inner(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                if settings.THUMBNAIL_DEBUG:
                    raise
                logger.error('Error: %s', exc)
                return error_output
        return wrapper
    return inner


@library.filter
@debug_silence(error_output='')
def thumbnail_url(source, alias):
    return _thumbnail.thumbnail_url(source, alias)


@library.global_function
@debug_silence(error_output=None)
def thumbnailer_passive(obj):
    return _thumbnail.thumbnailer_passive(obj)


@library.global_function
@debug_silence(error_output=None)
def thumbnailer(obj):
    return _thumbnail.thumbnailer(obj)


@library.global_function
@debug_silence(error_output='')
def thumbnail(source, **kwargs):
    thumbnail =  _thumbnail.get_thumbnailer(source).get_thumbnail(kwargs)
    return thumbnail.url
