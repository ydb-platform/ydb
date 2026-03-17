from pathlib import Path
from typing import Union, Callable, Optional

from ..config import Section as _Section
from ..settings import ENV_MAINTENANCE, ENV_MAINTENANCE_INPLACE
from ..typehints import Strlist


class Section(_Section):
    """Basic nice configuration."""

    def __init__(
            self,
            name: str = None,
            *,
            touch_reload: Strlist =None,
            workers: int = None,
            threads: Union[int, bool] = None,
            mules: int = None,
            owner: str = None,
            log_into: str = None,
            log_dedicated: bool = None,
            process_prefix: str = None,
            ignore_write_errors: bool = None,
            **kwargs):
        """

        :param name: Section name.

        :param touch_reload: Reload uWSGI if the specified file or directory is modified/touched.

        :param workers: Spawn the specified number of workers (processes).
            Default: workers number equals to CPU count.

        :param threads: Number of threads per worker or ``True`` to enable user-made threads support.

        :param mules: Number of mules to spawn.

        :param owner: Set process owner user and group.

        :param log_into: Filepath or UDP address to send logs into.

        :param log_dedicated: If ``True`` all logging will be handled with a separate
            thread in master process.

        :param process_prefix: Add prefix to process names.

        :param ignore_write_errors: If ``True`` no annoying SIGPIPE/write/writev errors
            will be logged, and no related exceptions will be raised.

            .. note:: Usually such errors could be seen on client connection cancelling
               and are safe to ignore.

        :param kwargs:

        """
        super().__init__(strict_config=True, name=name, **kwargs)

        # Fix possible problems with non-ASCII.
        self.env('LANG', 'en_US.UTF-8')

        if touch_reload:
            self.main_process.set_basic_params(touch_reload=touch_reload)

        if workers:
            self.workers.set_basic_params(count=workers)
        else:
            self.workers.set_count_auto()

        set_threads = self.workers.set_thread_params

        if isinstance(threads, bool):
            set_threads(enable=threads)

        else:
            set_threads(count=threads)

        if log_dedicated:
            self.logging.set_master_logging_params(enable=True, dedicate_thread=True)

        self.workers.set_mules_params(mules=mules)
        self.workers.set_harakiri_params(verbose=True)
        self.main_process.set_basic_params(vacuum=True)
        self.main_process.set_naming_params(
            autonaming=True,
            prefix=f'{process_prefix} ' if process_prefix else None,
        )
        self.master_process.set_basic_params(enable=True)
        self.master_process.set_exit_events(sig_term=True)  # Respect the convention. Make Upstart and Co happy.
        self.locks.set_basic_params(thunder_lock=True)
        self.configure_owner(owner=owner)
        self.logging.log_into(target=log_into)

        if ignore_write_errors:
            self.master_process.set_exception_handling_params(no_write_exception=True)
            self.logging.set_filters(write_errors=False, sigpipe=False)

    def get_log_format_default(self) -> str:
        """Returns default log message format.

        .. note:: Some params may be missing.

        """
        vars = self.logging.vars

        app_id = app_req_count = worker_req_count = tsize = sendfile_route_offload = '-'

        format_default = (
            f"[pid: {vars.WORKER_PID}|app: {app_id}|req: {app_req_count}/{worker_req_count}] "
            f"{vars.REQ_REMOTE_ADDR} ({vars.REQ_REMOTE_USER}) "
            f"{{{vars.REQ_COUNT_VARS_CGI} vars in {vars.SIZE_PACKET_UWSGI} bytes}} "
            f"[{vars.REQ_START_CTIME}] {vars.REQ_METHOD} {vars.REQ_URI} => "
            f"generated {vars.RESP_SIZE_BODY} bytes in {vars.RESP_TIME_MS} "
            f"{tsize}{sendfile_route_offload}({vars.REQ_SERVER_PROTOCOL} {vars.RESP_STATUS}) "
            f"{vars.RESP_COUNT_HEADERS} headers in {vars.RESP_SIZE_HEADERS} bytes "
            f"({vars.ASYNC_SWITCHES} switches on core {vars.CORE})"
        )
        return format_default

    @classmethod
    def get_bundled_static_path(cls, filename: str) -> str:
        """Returns a full path to a static HTML page file bundled with uwsgiconf.

        :param filename: File name to return path to.

            Examples:
                * 403.html
                * 404.html
                * 500.html
                * 503.html

        """
        return str(Path(__file__).parent.parent / 'contrib/django/uwsgify/static/uwsgify' / filename)

    def configure_maintenance_mode(self, trigger: Union[str, Path], response: str):
        """Allows maintenance mode when a certain response
        is given for every request if a trigger is set.

        :param trigger: This triggers maintenance mode responses.
            Should be a path to a file: if file exists, maintenance mode is on.

        :param response: Response to to give in maintenance mode.

            Supported:
                1. File path - this file will be served in response.

                2. URLs starting with ``http`` - requests will be redirected there using 302.
                  This is often discouraged, because it may have search ranking implications.

                3. Prefix ``app`` will replace your entire app with a maintenance one.
                  Using this also prevents background tasks registration and execution
                  (including scheduler, timers, signals).

                  * If the value is `app` - the default maintenance application bundled with
                    uwsgiconf would be used.

                  * Format ``app::<your-module>:<your-app-function>`` instructs uwsgiconf to
                    load your function as a maintenance app. E.g.: app::my_pack.my_module:my_func

        """
        if response.startswith('app'):

            if response == 'app':
                response = 'uwsgiconf.maintenance:app_maintenance'

            else:
                _, _, response = response.partition('::')

            filepath = Path(trigger)

            # We reload uwsgi to swap our app for maintenance app.
            self.main_process.set_basic_params(touch_reload=f'{filepath}')

            # This will also make available Maintenance page in Django admin.
            self.env(ENV_MAINTENANCE, f'{filepath}', update_local=True)

            if filepath.exists():
                self.env(ENV_MAINTENANCE_INPLACE, 1, update_local=True)
                self.python.set_wsgi_params(module=response)

            return self

        routing = self.routing

        rule = routing.route_rule

        if response.startswith('http'):
            action = rule.actions.redirect(response)

        else:
            action = rule.actions.serve_static(Path(response).absolute())

        routing.register_route(
            routing.route_rule(
                subject=rule.subjects.custom(trigger).exists(),
                action=action,
            ))

        return self

    def configure_owner(self, owner: str = 'www-data'):
        """Shortcut to set process owner data.

        :param owner: Sets user and group. Default: ``www-data``.

        """
        if owner is not None:
            self.main_process.set_owner_params(uid=owner, gid=owner)

        return self

    def configure_https_redirect(self):
        """Enables HTTP to HTTPS redirect."""
        rule = self.routing.route_rule

        self.routing.register_route([
            rule(
                rule.actions.redirect('https://${HTTP_HOST}${REQUEST_URI}', permanent=True),
                rule.subjects.custom('${HTTPS}', negate=True).eq('on')
            ),
        ])
        return self

    def configure_certbot_https(
            self,
            domain: str,
            webroot: str,
            *,
            address: str = None,
            allow_shared_sockets: bool = None,
            http_redirect: bool = False,
    ):
        """Enables HTTPS using certificates from Certbot https://certbot.eff.org.

        .. note:: This relies on ``webroot`` mechanism of Certbot - https://certbot.eff.org/docs/using.html#webroot

            Sample command to get a certificate: ``certbot certonly --webroot -w /webroot/path/ -d mydomain.org``

        :param domain: Domain name certificates issued for
            (the same as in ``-d`` option in the above command).

        :param webroot: Directory to store challenge files to get and renew the certificate
            (the same as in ``-w`` option in the above command).

        :param address: Address to bind socket to.

        :param allow_shared_sockets: Allows using shared sockets to bind
            to privileged ports. If not provided automatic mode is enabled:
            shared are allowed if current user is not root.

        :param http_redirect: Redirect HTTP requests to HTTPS
            if certificates exist.

        """
        address = address or ':443'

        networking = self.networking

        path_cert_chain, path_cert_private = networking.sockets.https.get_certbot_paths(domain)

        if path_cert_chain:

            networking.register_socket(
                networking.sockets.from_dsn(
                    f'https://{address}?cert={path_cert_chain}&key={path_cert_private}',
                    allow_shared_sockets=allow_shared_sockets))

            if http_redirect:
                self.configure_https_redirect()

        self.statics.register_static_map(
            '/.well-known/', webroot, retain_resource_path=True)

        return self

    def configure_logging_json(self):
        """Configures uWSGI output to be json-formatted."""

        logging = self.logging

        vars_enc = logging.encoders.format.vars
        vars_req = logging.vars

        # Allow request encoding.
        logging.add_logger(logging.loggers.stdio(), requests_only=True)

        # Log essential request data to place into "msg".
        logging.set_basic_params(template=(
            f'{vars_req.REQ_METHOD} {vars_req.REQ_URI} -> {vars_req.RESP_STATUS}'
        ))

        new_line = logging.encoders.newline()

        log_template = (
            '{'
            '"dt": "' + str(vars_enc.TIME_FORMAT('iso')) + '", '
            '"src": "__src__", '
            f'"msg": "{vars_enc.MESSAGE}", '
            '"ctx": {}, '
            f'"ms": {vars_enc.TIME_MS}'
            '}'
        )

        logging.add_logger_encoder(
            [logging.encoders.json(log_template.replace('__src__', 'uwsgi.req')), new_line],
            requests_only=True)

        logging.add_logger_encoder(
            [logging.encoders.json(log_template.replace('__src__', 'uwsgi.out')), new_line])

        return self


class PythonSection(Section):
    """Basic nice configuration using Python plugin."""

    def __init__(
            self,
            name: str = None,
            *,
            params_python: dict = None,
            wsgi_module: str = None,
            wsgi_callable: Union[str, Callable] = None,
            embedded_plugins: Optional[bool] = True,
            require_app: bool = True,
            threads: Union[bool, int] = True,
            **kwargs
    ):
        """

        :param name: Section name.

        :param params_python: See Python plugin basic params.

        :param wsgi_module: WSGI application module path or filepath.

            Example:
                mypackage.my_wsgi_module -- read from `application` attr of mypackage/my_wsgi_module.py
                mypackage.my_wsgi_module:my_app -- read from `my_app` attr of mypackage/my_wsgi_module.py

        :param wsgi_callable: WSGI application callable name. Default: application.

        :param embedded_plugins: This indicates whether plugins were embedded into uWSGI,
            which is the case if you have uWSGI from PyPI.

        :param require_app: Exit if no app can be loaded.

        :param threads: Number of threads per worker or ``True`` to enable user-made threads support.

        :param kwargs:

        """
        if embedded_plugins is True:
            embedded_plugins = self.embedded_plugins_presets.BASIC + ['python', 'python2', 'python3']

        super().__init__(
            name=name, embedded_plugins=embedded_plugins, threads=threads,
            **kwargs)

        self.python.set_basic_params(**(params_python or {}))

        if callable(wsgi_callable):
            wsgi_callable = wsgi_callable.__name__

        self.python.set_wsgi_params(module=wsgi_module, callable_name=wsgi_callable)

        self.applications.set_basic_params(exit_if_none=require_app)
