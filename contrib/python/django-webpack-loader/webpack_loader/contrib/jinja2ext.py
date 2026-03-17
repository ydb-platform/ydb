from jinja2.ext import Extension
from jinja2.runtime import Context
from jinja2.utils import pass_context

from ..templatetags.webpack_loader import get_files, render_bundle


@pass_context
def _render_bundle(context: Context, *args, **kwargs):
    return render_bundle(context, *args, **kwargs)


@pass_context
def _get_files(context: Context, *args, **kwargs):
    return get_files(context, *args, **kwargs)


class WebpackExtension(Extension):
    def __init__(self, environment):
        super(WebpackExtension, self).__init__(environment)
        environment.globals["render_bundle"] = _render_bundle
        environment.globals["webpack_get_files"] = _get_files
