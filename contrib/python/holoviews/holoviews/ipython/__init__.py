import os

import param
from bokeh.settings import settings as bk_settings
from IPython.core.completer import IPCompleter
from IPython.display import HTML, publish_display_data
from param import ipython as param_ext

import holoviews as hv

from ..core.dimension import LabelledData
from ..core.options import Store
from ..core.tree import AttrTree
from ..util import extension
from .display_hooks import display, png_display, pprint_display, svg_display
from .magics import load_magics

AttrTree._disabled_prefixes = ['_repr_','_ipython_canary_method_should_not_exist']

def show_traceback():
    """Display the full traceback after an abbreviated traceback has occurred.

    """
    from .display_hooks import FULL_TRACEBACK
    print(FULL_TRACEBACK)


def __getattr__(attr):
    if attr == "IPTestCase":
        from ..element.comparison import IPTestCase
        from ..util.warnings import deprecated
        deprecated("1.23.0", old="holoviews.ipython.IPTestCase", new="holoviews.element.comparison.IPTestCase")
        return IPTestCase
    raise AttributeError(f"module {__name__!r} has no attribute {attr!r}")


class notebook_extension(extension):
    """Notebook specific extension to hv.extension that offers options for
    controlling the notebook environment.

    """

    css = param.String(default='', doc="Optional CSS rule set to apply to the notebook.")

    logo = param.ClassSelector(default=True, class_=(bool, dict), doc="""
        Controls logo display. Dictionary option must include the keys
        `logo_link`, `logo_src`, and `logo_title`.""")

    inline = param.Boolean(default=False, doc="""
        Whether to inline JS and CSS resources.
        If disabled, resources are loaded from CDN if one is available.""")

    width = param.Number(default=None, bounds=(0, 100), doc="""
        Width of the notebook as a percentage of the browser screen window width.""")

    display_formats = param.List(default=['html'], doc="""
        A list of formats that are rendered to the notebook where
        multiple formats may be selected at once (although only one
        format will be displayed).

        Although the 'html' format is supported across backends, other
        formats supported by the current backend (e.g. 'png' and 'svg'
        using the matplotlib backend) may be used. This may be useful to
        export figures to other formats such as PDF with nbconvert.""")

    allow_jedi_completion = param.Boolean(default=True, doc="""
       Whether to allow jedi tab-completion to be enabled in IPython.""")

    case_sensitive_completion = param.Boolean(default=False, doc="""
       Whether to monkey patch IPython to use the correct tab-completion
       behavior. """)

    enable_mathjax = param.Boolean(default=False, doc="""
        Whether to load bokeh-mathjax bundle in the notebook.""")

    _loaded = False

    def __call__(self, *args, **params):
        comms = params.pop('comms', None)
        super().__call__(*args, **params)
        # Abort if IPython not found
        try:
            ip = params.pop('ip', None) or get_ipython() # noqa (get_ipython)
        except Exception:
            return

        # Notebook archive relies on display hooks being set to work.
        try:
            import nbformat  # noqa: F401

            try:
                from .archive import notebook_archive
                hv.archive = notebook_archive
            except AttributeError as e:
                if str(e) != "module 'tornado.web' has no attribute 'asynchronous'":
                    raise

        except ImportError:
            pass

        # Not quite right, should be set when switching backends
        if 'matplotlib' in Store.renderers and not notebook_extension._loaded:
            svg_exporter = Store.renderers['matplotlib'].instance(holomap=None,fig='svg')
            hv.archive.exporters = [svg_exporter, *hv.archive.exporters]

        p = param.ParamOverrides(self, {k:v for k,v in params.items() if k!='config'})
        if p.case_sensitive_completion:
            from IPython.core import completer
            completer.completions_sorting_key = self.completions_sorting_key
        if not p.allow_jedi_completion and hasattr(IPCompleter, 'use_jedi'):
            ip.run_line_magic('config', 'IPCompleter.use_jedi = False')

        resources = self._get_resources(args, params)

        Store.display_formats = p.display_formats
        if 'html' not in p.display_formats and len(p.display_formats) > 1:
            msg = ('Output magic unable to control displayed format '
                   'as IPython notebook uses fixed precedence '
                   f'between {p.display_formats!r}')
            display(HTML(f'<b>Warning</b>: {msg}'))

        loaded = notebook_extension._loaded
        if loaded == False:
            param_ext.load_ipython_extension(ip, verbose=False)
            load_magics(ip)
            Store.output_settings.initialize(list(Store.renderers.keys()))
            Store.set_display_hook('html+js', LabelledData, pprint_display)
            Store.set_display_hook('png', LabelledData, png_display)
            Store.set_display_hook('svg', LabelledData, svg_display)
            bk_settings.simple_ids.set_value(False)
            notebook_extension._loaded = True

        css = ''
        if p.width is not None:
            css += f'<style>div.container {{ width: {p.width}% }}</style>'
        if p.css:
            css += f'<style>{p.css}</style>'

        if css:
            display(HTML(css))

        resources = list(resources)
        if len(resources) == 0: return

        from panel import config, extension as panel_extension
        if hasattr(config, 'comms') and comms:
            config.comms = comms

        same_cell_execution = published = getattr(self, '_repeat_execution_in_cell', False)
        for r in [r for r in resources if r != 'holoviews']:
            Store.renderers[r].load_nb(inline=p.inline)

        from ..plotting.renderer import Renderer
        Renderer.load_nb(inline=p.inline, reloading=same_cell_execution, enable_mathjax=p.enable_mathjax)

        if not published and hasattr(panel_extension, "_display_globals"):
            panel_extension._display_globals()

        if hasattr(ip, 'kernel') and not loaded:
            Renderer.comm_manager.get_client_comm(notebook_extension._process_comm_msg,
                                                  "hv-extension-comm")

        # Create a message for the logo (if shown)
        if not same_cell_execution and p.logo:
            self.load_logo(logo=p.logo,
                           bokeh_logo=  p.logo and ('bokeh' in resources),
                           mpl_logo=    p.logo and (('matplotlib' in resources)
                                                    or resources==['holoviews']),
                           plotly_logo= p.logo and ('plotly' in resources))

    @classmethod
    def completions_sorting_key(cls, word):
        """Fixed version of IPython.completer.completions_sorting_key

        """
        prio1, prio2 = 0, 0
        if word.startswith('__'):  prio1 = 2
        elif word.startswith('_'): prio1 = 1
        if word.endswith('='):     prio1 = -1
        if word.startswith('%%'):
            if '%' not in word[2:]:
                word, prio2 = word[2:], 2
        elif word.startswith('%'):
            if '%' not in word[1:]:
                word, prio2 = word[1:], 1
        return prio1, word, prio2


    def _get_resources(self, args, params):
        """Finds the list of resources from the keyword parameters and pops
        them out of the params dictionary.

        """
        resources = []
        disabled = []
        for resource in ['holoviews', *Store.renderers]:
            if resource in args:
                resources.append(resource)

            if resource in params:
                setting = params.pop(resource)
                if setting is True and resource != 'matplotlib':
                    if resource not in resources:
                        resources.append(resource)
                if setting is False:
                    disabled.append(resource)

        unmatched_args = set(args) - set(resources)
        if unmatched_args:
            display(HTML("<b>Warning:</b> Unrecognized resources '{}'".format("', '".join(unmatched_args))))

        resources = [r for r in resources if r not in disabled]
        if ('holoviews' not in disabled) and ('holoviews' not in resources):
            resources = ['holoviews', *resources]
        return resources

    @classmethod
    def load_logo(cls, logo: dict | bool = False, bokeh_logo=False, mpl_logo=False, plotly_logo=False):
        """Allow to display Holoviews' logo and the plotting extensions' logo.

        """
        import jinja2

        templateLoader = jinja2.PackageLoader(package_name='holoviews', package_path='ipython')
        jinjaEnv = jinja2.Environment(loader=templateLoader)
        template = jinjaEnv.get_template('load_notebook.html')
        if isinstance(logo, dict):
            logo_src = logo['logo_src']
            logo_link = logo['logo_link']
            logo_title = logo['logo_title']
        elif not logo:
            logo_src = logo_link = logo_title = ''
        else:
            from .. import __version__

            logo_src = None  # holoviews logo available in the template
            logo_link = 'https://holoviews.org'
            logo_title = f'HoloViews {__version__}'

        bokeh_version = mpl_version = plotly_version = ''
        # Backends are already imported at this stage.
        if bokeh_logo:
            import bokeh
            bokeh_version = bokeh.__version__
        if mpl_logo:
            import matplotlib as mpl
            mpl_version = mpl.__version__
        if plotly_logo:
            import plotly
            plotly_version = plotly.__version__

        # Hide tooltip first by checking if HV_HIDE_TOOLTIP_LOGO is set to true,
        # and then for CI, default is to not hide.
        hide_tooltip = os.getenv("HV_HIDE_TOOLTIP_LOGO", os.getenv("CI", "0")).lower() in ("1", "true")
        html = template.render({
            'logo':        logo,
            'logo_src':    logo_src,
            'logo_link':   logo_link,
            'logo_title':  logo_title,
            'bokeh_logo':  bokeh_logo,
            'mpl_logo':    mpl_logo,
            'plotly_logo': plotly_logo,
            'bokeh_version':  bokeh_version,
            'mpl_version':    mpl_version,
            'plotly_version': plotly_version,
            'show_tooltip': not hide_tooltip,
        })
        publish_display_data(data={'text/html': html})


def _delete_plot(plot_id):
    from ..plotting.renderer import Renderer
    return Renderer._delete_plot(plot_id)

notebook_extension.add_delete_action(_delete_plot)


def load_ipython_extension(ip):
    notebook_extension("matplotlib", ip=ip)

def unload_ipython_extension(ip):
    notebook_extension._loaded = False
