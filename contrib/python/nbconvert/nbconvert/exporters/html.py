# -*- coding: utf-8 -*-
"""HTML Exporter class"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

import os
import mimetypes
import base64

from traitlets import default, Unicode, Bool
from traitlets.config import Config
from jupyter_core.paths import jupyter_path
import jinja2
import markupsafe

if tuple(int(x) for x in jinja2.__version__.split(".")[:3]) < (3, 0, 0):
    from jinja2 import contextfilter
else:
    from jinja2 import pass_context as contextfilter

from jinja2.loaders import split_template_path

from nbconvert.filters.highlight import Highlight2HTML
from nbconvert.filters.widgetsdatatypefilter import WidgetsDataTypeFilter
from nbconvert.filters.markdown_mistune import IPythonRenderer, MarkdownWithMath

from .templateexporter import TemplateExporter


class HTMLExporter(TemplateExporter):
    """
    Exports a basic HTML document.  This exporter assists with the export of
    HTML.  Inherit from it if you are writing your own HTML template and need
    custom preprocessors/filters.  If you don't need custom preprocessors/
    filters, just change the 'template_file' config option.
    """
    export_from_notebook = "HTML"

    anchor_link_text = Unicode(u'¶',
        help="The text used as the text for anchor links.").tag(config=True)

    exclude_anchor_links = Bool(False,
        help="If anchor links should be included or not.").tag(config=True)

    require_js_url = Unicode(
        "https://cdnjs.cloudflare.com/ajax/libs/require.js/2.1.10/require.min.js",
        help="""
        URL to load require.js from.

        Defaults to loading from cdnjs.
        """
    ).tag(config=True)

    mathjax_url = Unicode(
        "https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.7/latest.js?config=TeX-AMS_CHTML-full,Safe",
        help="""
        URL to load Mathjax from.

        Defaults to loading from cdnjs.
        """
    ).tag(config=True)

    jquery_url = Unicode(
        "https://cdnjs.cloudflare.com/ajax/libs/jquery/2.0.3/jquery.min.js",
        help="""
        URL to load jQuery from.

        Defaults to loading from cdnjs.
        """
    ).tag(config=True)

    jupyter_widgets_base_url = Unicode(
        "https://unpkg.com/",
        help="URL base for Jupyter widgets"
    ).tag(config=True)

    widget_renderer_url = Unicode("", help="Full URL for Jupyter widgets").tag(config=True)

    html_manager_semver_range = Unicode(
        "*",
        help="Semver range for Jupyter widgets HTML manager"
    ).tag(config=True)

    @default('file_extension')
    def _file_extension_default(self):
        return '.html'

    @default('template_name')
    def _template_name_default(self):
        return 'lab'

    theme = Unicode('light',
                    help='Template specific theme(e.g. the JupyterLab CSS theme for the lab template)'
    ).tag(config=True)

    output_mimetype = 'text/html'

    @property
    def default_config(self):
        c = Config({
            'NbConvertBase': {
                'display_data_priority': [
                    'application/vnd.jupyter.widget-view+json',
                    'application/javascript',
                    'text/html',
                    'text/markdown',
                    'image/svg+xml',
                    'text/latex',
                    'image/png',
                    'image/jpeg',
                    'text/plain'
                ]
            },
            'HighlightMagicsPreprocessor': {
                'enabled': True
            }
        })
        c.merge(super().default_config)
        return c

    @contextfilter
    def markdown2html(self, context, source):
        """Markdown to HTML filter respecting the anchor_link_text setting"""
        cell = context.get('cell', {})
        attachments = cell.get('attachments', {})
        renderer = IPythonRenderer(escape=False, attachments=attachments,
                                   anchor_link_text=self.anchor_link_text,
                                   exclude_anchor_links=self.exclude_anchor_links)
        return MarkdownWithMath(renderer=renderer).render(source)

    def default_filters(self):
        for pair in super().default_filters():
            yield pair
        yield ('markdown2html', self.markdown2html)

    def from_notebook_node(self, nb, resources=None, **kw):
        langinfo = nb.metadata.get('language_info', {})
        lexer = langinfo.get('pygments_lexer', langinfo.get('name', None))
        highlight_code = self.filters.get('highlight_code', Highlight2HTML(pygments_lexer=lexer, parent=self))

        filter_data_type = WidgetsDataTypeFilter(notebook_metadata=self._nb_metadata, parent=self, resources=resources)

        self.register_filter('highlight_code', highlight_code)
        self.register_filter('filter_data_type', filter_data_type)
        return super().from_notebook_node(nb, resources, **kw)

    def _init_resources(self, resources):
        def resources_include_css(name):
            env = self.environment
            code = """<style type="text/css">\n%s</style>""" % (env.loader.get_source(env, name)[0])
            return markupsafe.Markup(code)

        def resources_include_js(name):
            env = self.environment
            code = """<script>\n%s</script>""" % (env.loader.get_source(env, name)[0])
            return markupsafe.Markup(code)

        def resources_include_url(name):
            env = self.environment
            mime_type, encoding = mimetypes.guess_type(name)
            try:
                # we try to load via the jinja loader, but that tries to load
                # as (encoded) text
                data = env.loader.get_source(env, name)[0].encode('utf8')
            except UnicodeDecodeError:
                # if that fails (for instance a binary file, png or ttf)
                # we mimic jinja2
                pieces = split_template_path(name)
                for searchpath in self.template_paths:
                    filename = os.path.join(searchpath, *pieces)
                    if os.path.exists(filename):
                        with open(filename, "rb") as f:
                            data = f.read()
                            break
                else:
                    raise ValueError("No file %r found in %r" % (name, searchpaths))
            data = base64.b64encode(data)
            data = data.replace(b'\n', b'').decode('ascii')
            src = 'data:{mime_type};base64,{data}'.format(mime_type=mime_type, data=data)
            return markupsafe.Markup(src)
        resources = super()._init_resources(resources)
        resources['theme'] = self.theme
        resources['include_css'] = resources_include_css
        resources['include_js'] = resources_include_js
        resources['include_url'] = resources_include_url
        resources['require_js_url'] = self.require_js_url
        resources['mathjax_url'] = self.mathjax_url
        resources['jquery_url'] = self.jquery_url
        resources['jupyter_widgets_base_url'] = self.jupyter_widgets_base_url
        resources['widget_renderer_url'] = self.widget_renderer_url
        resources['html_manager_semver_range'] = self.html_manager_semver_range
        return resources
