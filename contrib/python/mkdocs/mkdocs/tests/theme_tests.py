import os
import tempfile
import unittest
from unittest import mock

import mkdocs
from mkdocs.theme import YandexTheme as Theme
from mkdocs.tests.base import PathAssertionMixin
from mkdocs.localization import parse_locale

abs_path = os.path.abspath(os.path.dirname(__file__))
mkdocs_dir = os.path.abspath(os.path.dirname(mkdocs.__file__))
mkdocs_templates_dir = os.path.join(mkdocs_dir, 'mkdocs', 'templates')
theme_dir = os.path.abspath(os.path.join(mkdocs_dir, 'mkdocs', 'themes'))
mkdocs.utils.themes_dir_root = mkdocs_dir


def get_vars(theme):
    """ Return dict of theme vars. """
    return {k: theme[k] for k in iter(theme)}


class ThemeTests(PathAssertionMixin, unittest.TestCase):

    def test_simple_theme(self):
        theme = Theme(name='mkdocs')
        self.assertEqual(
            theme.dirs,
            [os.path.join(theme_dir, 'mkdocs'), mkdocs_templates_dir]
        )
        self.assertEqual(theme.static_templates, {'404.html', 'sitemap.xml'})
        self.assertPathIsFile(theme_dir, 'mkdocs', '404.html')
        self.assertPathIsFile(mkdocs_templates_dir, 'sitemap.xml')
        self.assertEqual(get_vars(theme), {
            'locale': parse_locale('en'),
            'include_search_page': False,
            'search_index_only': False,
            'analytics': {'gtag': None},
            'highlightjs': True,
            'hljs_style': 'github',
            'hljs_languages': [],
            'navigation_depth': 2,
            'nav_style': 'primary',
            'shortcuts': {'help': 191, 'next': 78, 'previous': 80, 'search': 83}
        })

    def test_custom_dir(self):
        custom = tempfile.mkdtemp()
        theme = Theme(name='mkdocs', custom_dir=custom)
        self.assertEqual(
            theme.dirs,
            [
                custom,
                os.path.join(theme_dir, 'mkdocs'),
                mkdocs_templates_dir
            ]
        )
        self.assertPathIsDir(theme_dir, 'mkdocs')
        self.assertPathIsDir(mkdocs_templates_dir)

    def test_custom_dir_only(self):
        custom = tempfile.mkdtemp()
        theme = Theme(name=None, custom_dir=custom)
        self.assertEqual(
            theme.dirs,
            [custom, mkdocs_templates_dir]
        )
        self.assertPathIsDir(mkdocs_templates_dir)

    def static_templates(self):
        theme = Theme(name='mkdocs', static_templates='foo.html')
        self.assertEqual(
            theme.static_templates,
            {'404.html', 'sitemap.xml', 'foo.html'}
        )
        self.assertPathIsDir(theme_dir, 'mkdocs')
        self.assertPathNotExists(theme_dir, 'mkdocs', 'foo.html')

    def test_vars(self):
        theme = Theme(name='mkdocs', foo='bar', baz=True)
        self.assertEqual(theme['foo'], 'bar')
        self.assertEqual(theme['baz'], True)
        self.assertTrue('new' not in theme)
        with self.assertRaises(KeyError):
            theme['new']
        theme['new'] = 42
        self.assertTrue('new' in theme)
        self.assertEqual(theme['new'], 42)

    @mock.patch('mkdocs.utils.yaml_load', return_value=None)
    def test_no_theme_config(self, m):
        theme = Theme(name='mkdocs')
        self.assertEqual(m.call_count, 1)
        self.assertEqual(theme.static_templates, {'sitemap.xml'})
        self.assertPathIsDir(theme_dir, 'mkdocs')

    def test_inherited_theme(self):
        m = mock.Mock(side_effect=[
            {'extends': 'readthedocs', 'static_templates': ['child.html']},
            {'static_templates': ['parent.html']}
        ])
        with mock.patch('mkdocs.utils.yaml_load', m) as m:
            theme = Theme(name='mkdocs')
            self.assertEqual(m.call_count, 2)
            self.assertEqual(
                theme.dirs,
                [
                    os.path.join(theme_dir, 'mkdocs'),
                    os.path.join(theme_dir, 'readthedocs'),
                    mkdocs_templates_dir
                ]
            )
            self.assertPathIsDir(theme_dir, 'mkdocs')
            self.assertPathIsDir(theme_dir, 'readthedocs')
            self.assertPathIsDir(mkdocs_templates_dir)
            self.assertEqual(
                theme.static_templates, {'sitemap.xml', 'child.html', 'parent.html'}
            )
            self.assertPathIsFile(mkdocs_templates_dir, 'sitemap.xml')
            self.assertPathNotExists(theme_dir, 'mkdocs', 'child.html')
            self.assertPathNotExists(theme_dir, 'readthedocs', 'parent.html')
