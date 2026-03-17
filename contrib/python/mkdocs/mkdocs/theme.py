import os
import jinja2
import logging

from mkdocs import utils
from mkdocs.utils import filters
from mkdocs.utils import yandex
from mkdocs.config.base import ValidationError
from mkdocs import localization

log = logging.getLogger(__name__)


class Theme:
    """
    A Theme object.

    Keywords:

        name: The name of the theme as defined by its entrypoint.

        custom_dir: User defined directory for custom templates.

        static_templates: A list of templates to render as static pages.

    All other keywords are passed as-is and made available as a key/value mapping.

    """

    def __init__(self, name=None, **user_config):
        self.name = name
        self._vars = {'locale': 'en'}

        # MkDocs provided static templates are always included
        mkdocs_templates = self.get_mkdocs_templates_dir()
        self.static_templates = set(os.listdir(mkdocs_templates))

        # Build self.dirs from various sources in order of precedence
        self.dirs = []

        if 'custom_dir' in user_config:
            self.dirs.append(user_config.pop('custom_dir'))

        if self.name:
            self._load_theme_config(name)

        # Include templates provided directly by MkDocs (outside any theme)
        self.dirs.append(mkdocs_templates)

        # Handle remaining user configs. Override theme configs (if set)
        self.static_templates.update(user_config.pop('static_templates', []))
        self._vars.update(user_config)

        # Validate locale and convert to Locale object
        self._vars['locale'] = localization.parse_locale(self._vars['locale'])

    def get_mkdocs_templates_dir(self):
        package_dir = os.path.abspath(os.path.dirname(__file__))
        return os.path.join(package_dir, 'templates')

    def __repr__(self):
        return "{}(name='{}', dirs={}, static_templates={}, {})".format(
            self.__class__.__name__, self.name, self.dirs, list(self.static_templates),
            ', '.join(f'{k}={v!r}' for k, v in self._vars.items())
        )

    def __getitem__(self, key):
        return self._vars[key]

    def __setitem__(self, key, value):
        self._vars[key] = value

    def __contains__(self, item):
        return item in self._vars

    def __iter__(self):
        return iter(self._vars)

    def _load_theme_config(self, name):
        """ Recursively load theme and any parent themes. """

        theme_dir = utils.get_theme_dir(name)
        self.dirs.append(theme_dir)

        try:
            file_path = os.path.join(theme_dir, 'mkdocs_theme.yml')
            with open(file_path, 'rb') as f:
                theme_config = utils.yaml_load(f)
                if theme_config is None:
                    theme_config = {}
        except OSError as e:
            log.debug(e)
            raise ValidationError(
                f"The theme '{name}' does not appear to have a configuration file. "
                f"Please upgrade to a current version of the theme."
            )

        log.debug(f"Loaded theme configuration for '{name}' from '{file_path}': {theme_config}")

        parent_theme = theme_config.pop('extends', None)
        if parent_theme:
            themes = utils.get_theme_names()
            if parent_theme not in themes:
                raise ValidationError(
                    f"The theme '{name}' inherits from '{parent_theme}', which does not appear to be installed. "
                    f"The available installed themes are: {', '.join(themes)}"
                )
            self._load_theme_config(parent_theme)

        self.static_templates.update(theme_config.pop('static_templates', []))
        self._vars.update(theme_config)

    def get_env(self):
        """ Return a Jinja environment for the theme. """

        loader = jinja2.FileSystemLoader(self.dirs)
        # No autoreload because editing a template in the middle of a build is not useful.
        env = jinja2.Environment(loader=loader, auto_reload=False)
        env.filters['url'] = filters.url_filter
        localization.install_translations(env, self._vars['locale'], self.dirs)
        return env


DEFAULT_PREFIX = 'mkdocs/templates'


class YandexTheme(Theme):

    def get_mkdocs_templates_dir(self):
        default_dir = os.path.join(utils.themes_dir_root, 'mkdocs', 'templates')
        if not os.path.exists(default_dir):
            os.makedirs(default_dir)
            static_templates, errors = yandex.unpack_resource_files(default_dir, False, DEFAULT_PREFIX)

            if static_templates:
                log.debug('Theme %s provided Mkdocs templates to dir %s', self.name, default_dir)
            if errors:
                raise ValidationError('Theme {} requires resource files {} but they were not found'.format(
                    self.name, ', '.join(errors)
                ))

        return default_dir

    def _load_theme_config(self, name):
        """ Recursively load theme and any parent themes. """
        self.dir = theme_dir = utils.get_theme_dir(name)
        if not os.path.exists(theme_dir):
            theme_prefix = os.path.relpath(theme_dir, utils.themes_dir_root).replace(os.path.sep, '/')
            theme_files, errors = yandex.unpack_resource_files(theme_dir, True, theme_prefix)

            if theme_files:
                log.debug('Theme %s filled its dir %s', name, theme_dir)
            if errors:
                raise ValidationError('Theme {} requires resource files {} but they were not found'.format(
                    self.name, ', '.join(errors)
                ))

        super(YandexTheme, self)._load_theme_config(name)
