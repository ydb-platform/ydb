from ..base import OptionsGroup


class Applications(OptionsGroup):
    """Applications.

    """

    def set_basic_params(
            self,
            *,
            exit_if_none: bool = None,
            max_per_worker: int = None,
            single_interpreter: bool = None,
            no_default: bool = None,
            manage_script_name: bool = None
    ):
        """

        :param exit_if_none: Exit if no app can be loaded.

        :param max_per_worker: Set the maximum number of per-worker applications.

        :param single_interpreter: Do not use multiple interpreters (where available).
            Some of the supported languages (such as Python) have the concept of "multiple interpreters".
            By default every app is loaded in a new python interpreter (that means a pretty-well isolated
            namespace for each app). If you want all of the app to be loaded in the same python vm,
            use the this option.

        :param no_default: Do not automatically fallback to default app. By default, the first loaded app
            is mounted as the "default one". That app will be served when no mountpoint matches.

        :param manage_script_name: You can to instruct uWSGI to map specific apps in the so called "mountpoint"
            and rewrite SCRIPT_NAME and PATH_INFO automatically. See .mount().
            The WSGI standard dictates that SCRIPT_NAME is the variable used to select a specific application.

        """
        self._set('need-app', exit_if_none, cast=bool)
        self._set('max-apps', max_per_worker)
        self._set('single-interpreter', single_interpreter, cast=bool)
        self._set('no-default-app', no_default, cast=bool)
        self._set('manage-script-name', manage_script_name, cast=bool)

        return self._section

    def mount(self, mountpoint: str, app: str, *, into_worker: bool = False):
        """Load application under mountpoint.

        Example:
            * .mount('', 'app0.py') -- Root URL part
            * .mount('/app1', 'app1.py') -- URL part
            * .mount('/pinax/here', '/var/www/pinax/deploy/pinax.wsgi')
            * .mount('the_app3', 'app3.py')  -- Variable value: application alias (can be set by ``UWSGI_APPID``)
            * .mount('example.com', 'app2.py')  -- Variable value: Hostname (variable set in nginx)

        * http://uwsgi-docs.readthedocs.io/en/latest/Nginx.html#hosting-multiple-apps-in-the-same-process-aka-managing-script-name-and-path-info

        :param mountpoint: URL part, or variable value.

            .. note:: In case of URL part you may also want to set ``manage_script_name`` basic param to ``True``.

            .. warning:: In case of URL part a trailing slash may case problems in some cases
                (e.g. with Django based projects).

        :param app: App module/file.

        :param into_worker: Load application under mountpoint
            in the specified worker or after workers spawn.

        """
        # todo check worker mount -- uwsgi_init_worker_mount_app() expects worker://
        self._set('worker-mount' if into_worker else 'mount', f'{mountpoint}={app}', multi=True)

        return self._section

    def switch_into_lazy_mode(self, *, affect_master: bool = None):
        """Load apps in workers instead of master.

        This option may have memory usage implications
        as Copy-on-Write semantics can not be used.

        .. note:: Consider using ``touch_chain_reload`` option in ``workers`` basic params
            for lazy apps reloading.

        :param affect_master: If **True** only workers will be
          reloaded by uWSGI's reload signals; the master will remain alive.

          .. warning:: uWSGI configuration changes are not picked up on reload by the master.


        """
        self._set('lazy' if affect_master else 'lazy-apps', True, cast=bool)

        return self._section
