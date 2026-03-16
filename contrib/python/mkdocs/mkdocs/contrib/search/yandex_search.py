import logging
import os
import tempfile

import mkdocs.contrib.search
from mkdocs.utils import yandex as utils


log = logging.getLogger(__name__)

BASE_KEY_PREFIX = utils.package_prefix(__package__)


class LangOption(mkdocs.contrib.search.LangOption):
    def get_lunr_supported_lang(self, lang):
        for lang_part in lang.split("_"):
            lang_part = lang_part.lower()
            r = utils.load_content(utils.resource_key(BASE_KEY_PREFIX, 'lunr-language', f'lunr.{lang_part}.js'))
            log.debug('HERE %s %s', lang_part, r is not None)
            if r is not None:
                return lang_part


class SearchPlugin(mkdocs.contrib.search.SearchPlugin):
    @staticmethod
    def load():
        log.debug('loading search plugin')
        return SearchPlugin

    # use base class options except Lang which uses resource files
    config_scheme = tuple(
        ('lang', LangOption()) if sch_entry[0] == 'lang' else sch_entry
        for sch_entry in mkdocs.contrib.search.SearchPlugin.config_scheme
    )

    def get_theme_media_path(self):
        path = tempfile.mkdtemp(dir=os.environ.get('TEMP'))
        prefix = BASE_KEY_PREFIX, 'templates'

        utils.unpack_resource_files(path, True, *prefix)
        return path

    def copy_files_on_post_build(self, files, output_base_path):
        log.debug('copy_files_on_post_build %s to %s', files,  output_base_path)
        for filename in files:
            key = utils.resource_key(BASE_KEY_PREFIX, 'lunr-language', filename)
            if not utils.unpack_resource_file(os.path.join(output_base_path, filename), key):
                raise Exception('Resource file {} required by {} not found'.format(
                    key, self.__class__.__module__ + '.' + self.__class__.__name__))
