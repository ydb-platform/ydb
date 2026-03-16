from ..base import OptionsGroup
from ..utils import listify


class Statics(OptionsGroup):
    """Statics.

    Unfortunately you cannot live without serving static files via some protocol (HTTP, SPDY or something else).
    Fortunately uWSGI has a wide series of options and micro-optimizations for serving static files.

    .. note:: This subsystem automatically honours the ``If-Modified-Since`` HTTP request header.

    * http://uwsgi.readthedocs.io/en/latest/StaticFiles.html

    """

    DIR_DOCUMENT_ROOT = 'docroot'
    """Used to check for static files in the requested DOCUMENT_ROOT. Pass into ``static_dir``."""

    class expiration_criteria:
        """Expiration criteria (subjects) to use with ``.add_expiration_rule()``."""

        FILENAME = 'filename'
        """Allows setting the Expires header for the specified file name pattern."""

        MIME_TYPE = 'type'
        """Allows setting the Expires header for the specified MIME type."""

        PATH_INFO = 'path-info'
        """Allows setting the Expires header for the specified ``PATH_INFO`` pattern."""

        REQUEST_URI = 'uri'
        """Allows setting the Expires header for the specified ``REQUEST_URI`` pattern."""

    class transfer_modes:
        """File transfer (serving) modes.

        With this, uWSGI will only generate response headers and
        the web server will be delegated to transferring the physical file.

        * http://uwsgi.readthedocs.io/en/latest/StaticFiles.html#transfer-modes

        """

        SENDFILE = 'x-sendfile'
        """Use ``X-Sendfile`` mode. Apache."""

        ACCEL_REDIRECT = 'x-accel-redirect'
        """Use ``X-Accel-Redirect`` mode. Nginx."""

    def set_basic_params(
            self, *, static_dir=None, index_file=None, mime_file=None, skip_ext=None, transfer_mode=None):
        """

        :param str|list[str] static_dir: Check for static files in the specified directory.

            .. note:: Use ``DIR_DOCUMENT_ROOT`` constant to serve files under ``DOCUMENT_ROOT``.

        :param str|list[str] index_file: Search for specified file if a directory is requested.

            Example: ``index.html``

        :param str|list[str] mime_file: Set mime types file path to extend uWSGI builtin list.

            Default: ``/etc/mime.types`` or ``/etc/apache2/mime.types``.

        :param str|list[str] skip_ext: Skip specified extension from static file checks.

            Example: add ``.php`` to not serve it as static.

        :param str transfer_mode: Set static file serving (transfer) mode.

            See ``.transfer_modes``.

            .. note:: Another option is to specify ``count_offload`` in ``.workers.set_thread_params()``.

        """

        if static_dir == self.DIR_DOCUMENT_ROOT:
            self._set('check-static-docroot', True, cast=bool)

        else:
            self._set('static-check', static_dir, multi=True)

        self._set('static-index', index_file, multi=True)
        self._set('mimefile', mime_file, multi=True)
        self._set('static-skip-ext', skip_ext, multi=True)
        self._set('fileserve-mode', skip_ext, multi=True)

        return self._section

    def register_static_map(self, mountpoint, target, *, retain_resource_path=False, safe_target=False):
        """Allows mapping mountpoint to a static directory (or file).

        * http://uwsgi.readthedocs.io/en/latest/StaticFiles.html#mode-3-using-static-file-mount-points

        :param str mountpoint:

        :param str target:

        :param bool retain_resource_path: Append the requested resource to the docroot.

            Example: if ``/images`` maps to ``/var/www/img`` requested ``/images/logo.png`` will be served from:

            * ``True``: ``/var/www/img/images/logo.png``

            * ``False``: ``/var/www/img/logo.png``

        :param bool safe_target: Skip security checks if the file is under the specified path.

            Whether to consider resolved (real) target a safe one to serve from.

            * http://uwsgi.readthedocs.io/en/latest/StaticFiles.html#security

        """
        command = 'static-map'

        if retain_resource_path:

            command += '2'

        self._set(command, f'{mountpoint}={target}', multi=True)

        if safe_target:
            self._set('static-safe', target, multi=True)

        return self._section

    def add_expiration_rule(self, criterion, value, *, timeout, use_mod_time=False):
        """Adds statics expiration rule based on a criterion.

        :param str criterion: Criterion (subject) to base expiration on.

            See ``.expiration_criteria``.

        :param str|list[str] value: Value to test criteria upon.

            .. note:: Usually a regular expression.

        :param int timeout: Number of seconds to expire after.

        :param bool use_mod_time: Base on file modification time instead of the current time.

        """
        command = 'static-expires'
        separator = ' '

        if criterion != self.expiration_criteria.FILENAME:

            command += f'-{criterion}'

        if criterion == self.expiration_criteria.MIME_TYPE:

            separator = '='

        if use_mod_time:

            command += '-mtime'

        for value in listify(value):
            self._set(command, f'{value}{separator}{timeout}', multi=True)

        return self._section

    # todo consider adding:
    # static-gzip*

    def set_paths_caching_params(self, *, timeout=None, cache_name=None):
        """Use the uWSGI caching subsystem to store mappings from URI to filesystem paths.

        * http://uwsgi.readthedocs.io/en/latest/StaticFiles.html#caching-paths-mappings-resolutions

        :param int timeout: Amount of seconds to put resolved paths in the uWSGI cache.

        :param str cache_name: Cache name to use for static paths.

        """
        self._set('static-cache-paths', timeout)
        self._set('static-cache-paths-name', cache_name)

        return self._section
