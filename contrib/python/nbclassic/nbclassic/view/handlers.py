#encoding: utf-8
"""Tornado handlers for viewing HTML files."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from tornado import web, gen


from jupyter_server.base.handlers import JupyterHandler, path_regex
from jupyter_server.utils import url_escape, ensure_async, url_path_join

from nbclassic import nbclassic_path


class CustomViewHandler(JupyterHandler):
    """Render HTML files within an iframe."""

    @web.authenticated
    @gen.coroutine
    def get(self, path):
        """Get a view on a given path."""

        path = path.strip('/')
        exists = yield ensure_async(self.contents_manager.file_exists(path))
        if not exists:
            raise web.HTTPError(404, u'File does not exist: %s' % path)

        basename = path.rsplit('/', 1)[-1]
        file_url = url_path_join(self.base_url, "files", url_escape(path))
        self.write(self.render_template('view.html',
            file_url=file_url,
            page_title=basename,
            )
        )


default_handlers = [
    (r"{}/view{}".format(nbclassic_path(), path_regex), CustomViewHandler),
]
