from typing import Optional
from warnings import warn

from django.http.request import HttpRequest
from django.template import Library
from django.utils.safestring import mark_safe

from .. import utils

register = Library()
_WARNING_MESSAGE = (
    'You have specified skip_common_chunks=True but the passed context '
    'doesn\'t have a request. django_webpack_loader needs a request object to '
    'filter out duplicate chunks. Please see https://github.com/django-webpack'
    '/django-webpack-loader#use-skip_common_chunks-on-render_bundle')


@register.simple_tag(takes_context=True)
def render_bundle(
        context, bundle_name, extension=None, config='DEFAULT', suffix='',
        attrs='', is_preload=False, skip_common_chunks=None):
    if skip_common_chunks is None:
        skip_common_chunks = utils.get_skip_common_chunks(config)

    request: Optional[HttpRequest] = context.get('request')
    tags = utils.get_as_url_to_tag_dict(
        bundle_name, request=request, extension=extension, config=config,
        suffix=suffix, attrs=attrs, is_preload=is_preload)

    if request is None:
        if skip_common_chunks:
            warn(message=_WARNING_MESSAGE, category=RuntimeWarning)
        return mark_safe('\n'.join(tags.values()))

    used_urls = getattr(request, '_webpack_loader_used_urls', None)
    if used_urls is None:
        used_urls = set()
        setattr(request, '_webpack_loader_used_urls', used_urls)
    if skip_common_chunks:
        tags = {url: tag for url, tag in tags.items() if url not in used_urls}
    used_urls.update(tags)
    return mark_safe('\n'.join(tags.values()))


@register.simple_tag
def webpack_static(asset_name, config='DEFAULT'):
    return utils.get_static(asset_name, config=config)


@register.simple_tag
def webpack_asset(asset_name, config='DEFAULT'):
    return utils.get_asset(asset_name, config=config)


@register.simple_tag(takes_context=True)
def get_files(
        context, bundle_name, extension=None, config='DEFAULT',
        skip_common_chunks=None):
    """
    Returns all chunks in the given bundle.
    Example usage::

        {% get_files 'editor' 'css' as editor_css_chunks %}
        CKEDITOR.config.contentsCss = '{{ editor_css_chunks.0.url }}';

    :param context: The request, if you want to use `skip_common_chunks`
    :param bundle_name: The name of the bundle
    :param extension: (optional) filter by extension
    :param config: (optional) the name of the configuration
    :param skip_common_chunks: (optional) `True` if you want to skip returning already rendered common chunks
    :return: a list of matching chunks
    """
    if skip_common_chunks is None:
        skip_common_chunks = utils.get_skip_common_chunks(config)

    result = utils.get_files(bundle_name, extension=extension, config=config)

    request = context.get('request')
    if request is None:
        if skip_common_chunks:
            warn(message=_WARNING_MESSAGE, category=RuntimeWarning)
        return result

    used_urls = getattr(request, '_webpack_loader_used_urls', None)
    if not used_urls:
        used_urls = set()
    if skip_common_chunks:
        result = [chunk for chunk in result if chunk['url'] not in used_urls]
    return result
