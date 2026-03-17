from functools import lru_cache
from importlib import import_module
from typing import Optional, OrderedDict

from django.conf import settings
from django.http.request import HttpRequest

from .config import load_config
from .loaders import WebpackLoader


def import_string(dotted_path):
    '''
    This is a rough copy of django's import_string, which wasn't introduced until Django 1.7

    Once this package's support for Django 1.6 has been removed, this can be safely replaced with
    `from django.utils.module_loading import import_string`
    '''
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
        module = import_module(module_path)
        return getattr(module, class_name)
    except (ValueError, AttributeError, ImportError):
        raise ImportError('%s doesn\'t look like a valid module path' % dotted_path)


@lru_cache(maxsize=None)
def get_loader(config_name) -> WebpackLoader:
    config = load_config(config_name)
    loader_class = import_string(config['LOADER_CLASS'])
    return loader_class(config_name, config)


def get_skip_common_chunks(config_name):
    loader = get_loader(config_name)
    # The global default is currently False, whenever that is changed, change
    # this fallback value as well which is present to provide backwards
    # compatibility.
    return loader.config.get('SKIP_COMMON_CHUNKS', False)


def _filter_by_extension(bundle, extension):
    '''Return only files with the given extension'''
    for chunk in bundle:
        if chunk['name'].endswith('.{0}'.format(extension)):
            yield chunk


def _get_bundle(loader, bundle_name, extension):
    bundle = loader.get_bundle(bundle_name)
    if extension:
        bundle = _filter_by_extension(bundle, extension)
    return bundle


def get_files(bundle_name, extension=None, config='DEFAULT'):
    '''Returns list of chunks from named bundle'''
    loader = get_loader(config)
    return list(_get_bundle(loader, bundle_name, extension))


def get_as_url_to_tag_dict(
    bundle_name, request: Optional[HttpRequest] = None, extension=None,
    config='DEFAULT', suffix='', attrs='', is_preload=False
) -> OrderedDict[str, str]:
    '''
    Get a dict of URLs to formatted <script> & <link> tags for the assets in the
    named bundle.

    :param bundle_name: The name of the bundle
    :param extension: (optional) filter by extension, eg. 'js' or 'css'
    :param config: (optional) the name of the configuration
    :return: a dict of URLs to formatted tags as strings
    '''

    loader = get_loader(config)
    bundle = _get_bundle(loader, bundle_name, extension)
    result = OrderedDict[str, str]()
    attrs_l = attrs.lower()

    for chunk in bundle:
        if chunk['name'].endswith(('.js', '.js.gz')):
            if is_preload:
                result[chunk['url']] = (
                    '<link rel="preload" as="script" href="{0}" {1}/>'
                ).format(''.join([chunk['url'], suffix]), attrs)
            else:
                result[chunk['url']] = (
                    '<script src="{0}"{2}{3}{1}></script>'
                ).format(
                    ''.join([chunk['url'], suffix]),
                    attrs,
                    loader.get_integrity_attr(chunk, request, attrs_l),
                    loader.get_nonce_attr(chunk, request, attrs_l),
                )
        elif chunk['name'].endswith(('.css', '.css.gz')):
            result[chunk['url']] = (
                '<link href="{0}" rel={2}{3}{4}{1}/>'
            ).format(
                ''.join([chunk['url'], suffix]),
                attrs,
                '"stylesheet"' if not is_preload else '"preload" as="style"',
                loader.get_integrity_attr(chunk, request, attrs_l),
                loader.get_nonce_attr(chunk, request, attrs_l),
            )
    return result


def get_as_tags(
        bundle_name, request=None, extension=None, config='DEFAULT', suffix='',
        attrs='', is_preload=False):
    '''
    Get a list of formatted <script> & <link> tags for the assets in the
    named bundle.

    :param bundle_name: The name of the bundle
    :param extension: (optional) filter by extension, eg. 'js' or 'css'
    :param config: (optional) the name of the configuration
    :return: a list of formatted tags as strings
    '''
    return list(get_as_url_to_tag_dict(bundle_name, request, extension, config, suffix, attrs, is_preload).values())


def get_static(asset_name, config='DEFAULT'):
    '''
    Equivalent to Django's 'static' look up but for webpack assets.

    :param asset_name: the name of the asset
    :param config: (optional) the name of the configuration
    :return: path to webpack asset as a string
    '''
    public_path = get_loader(config).get_assets().get('publicPath')
    if not public_path or public_path == 'auto':
        public_path = getattr(settings, 'STATIC_URL')

    return '{0}{1}'.format(public_path, asset_name)


def get_asset(source_filename, config='DEFAULT'):
    '''
    Equivalent to Django's 'static' look up but for webpack assets, given its original filename.
    Allow handling files whose path has been modified by Webpack processing, such as including content hash to filename.

    :param source_filename: the source filename of the asset
    :param config: (optional) the name of the configuration
    :return: path to webpack asset as a string
    '''
    loader = get_loader(config)
    asset = loader.get_asset_by_source_filename(source_filename)
    if not asset:
        return None

    return get_static(asset['name'], config)
