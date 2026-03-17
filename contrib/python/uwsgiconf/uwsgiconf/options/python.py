import sys
from pathlib import Path
from typing import Union

from ..base import OptionsGroup
from ..exceptions import ConfigurationError
from ..typehints import Strint, Strlist

AUTO = -1


class Python(OptionsGroup):
    """Python plugin options.

    .. note:: By default the plugin does not initialize the GIL.
        This means your app-generated threads will not run.
        If you need threads, remember to enable them with ``enable_threads``.

    """
    plugin = True

    def set_basic_params(
            self,
            *,
            version: Strint = AUTO,
            python_home: str = None,
            enable_threads: bool = None,
            search_path: str = None,
            python_binary: str = None,
            tracebacker_path: str = None,
            plugin_dir: str = None,
            os_env_reload: bool = None,
            optimization_level: int = None
    ):
        """

        :param version: Python version plugin supports.

            Example:
                * 3 - version 3
                * <empty> - version 2
                * <default> - version deduced by uwsgiconf

        :param python_home: Set python executable directory - PYTHONHOME/virtualenv.

        :param bool enable_threads: Enable threads in the embedded languages.
            This will allow to spawn threads in your app.

            .. warning:: Threads will simply *not work* if this option is not enabled.
                         There will likely be no error, just no execution of your thread code.

        :param search_path: Add directory (or an .egg or a glob) to the Python search path.

            .. note:: This can be specified up to 64 times.

        :param python_binary: Set python program name.

        :param tracebacker_path: Enable the uWSGI Python tracebacker.
            http://uwsgi-docs.readthedocs.io/en/latest/Tracebacker.html

        :param plugin_dir: Directory to search for plugin.

        :param os_env_reload: Force ``os.environ`` reloading for every request.
            Used to allow setting of ``UWSGI_SETENV`` for Python applications.

        :param optimization_level: Python optimization level (see ``-O`` argument).
            .. warning:: This may be dangerous for some apps.

        """
        self._set_name(version)

        self._section.workers.set_thread_params(enable=enable_threads)
        self._set('py-tracebacker', tracebacker_path)
        self._set('py-program-name', python_binary)
        self._set('pyhome', python_home)
        self._set('pythonpath', search_path, multi=True)
        self._set('reload-os-env', os_env_reload, cast=bool)
        self._set('optimize', optimization_level)

        self._section.set_plugins_params(search_dirs=plugin_dir)

        return self._section

    def _set_name(self, version: Strint = AUTO):
        """Returns plugin name."""

        name = 'python'

        if version:
            if version is AUTO:
                version = sys.version_info[0]

                if version == 2:
                    version = ''

            name = f'{name}{version}'

        self.name = name

    def _get_name(self, *args, **kwargs):
        name = self.name

        if not name:
            self._set_name()

        return self.name

    def set_app_args(self, *args):
        """Sets ``sys.argv`` for python apps.

        Examples:
            * pyargv="one two three" will set ``sys.argv`` to ``('one', 'two', 'three')``.

        :param args:

        """
        if args:
            self._set('pyargv', ' '.join(args))

        return self._section

    def set_wsgi_params(self, *, module: Union[str, Path] = None, callable_name: str = None, env_strategy: str = None):
        """Set wsgi related parameters.

        :param module:
            * load .wsgi file as the Python application
            * load a WSGI module as the application.

            .. note:: The module (sans ``.py``) must be importable, ie. be in ``PYTHONPATH``.

            Examples:
                * mypackage.my_wsgi_module -- read from `application` attr of mypackage/my_wsgi_module.py
                * mypackage.my_wsgi_module:my_app -- read from `my_app` attr of mypackage/my_wsgi_module.py

        :param callable_name: Set WSGI callable name. Default: application.

        :param env_strategy: Strategy for allocating/deallocating
            the WSGI env, can be:

            * ``cheat`` - preallocates the env dictionary on uWSGI startup and clears it
                after each request. Default behaviour for uWSGI <= 2.0.x

            * ``holy`` - creates and destroys the environ dictionary at each request.
                Default behaviour for uWSGI >= 2.1

        """
        module = str(module or '')

        if '/' in module:
            self._set('wsgi-file', module, condition=module)

        else:
            self._set('wsgi', module, condition=module)

        self._set('callable', callable_name)
        self._set('wsgi-env-behaviour', env_strategy)

        return self._section

    def eval_wsgi_entrypoint(self, code: str):
        """Evaluates Python code as WSGI entry point.

        :param code:

        """
        self._set('eval', code)

        return self._section

    def set_autoreload_params(self, *, scan_interval: int = None, ignore_modules: Strlist = None):
        """Sets autoreload related parameters.

        :param scan_interval: Seconds. Monitor Python modules' modification times to trigger reload.

            .. warning:: Use only in development.

        :param ignore_modules: Ignore the specified module during auto-reload scan.

        """
        self._set('py-auto-reload', scan_interval)
        self._set('py-auto-reload-ignore', ignore_modules, multi=True)

        return self._section

    def register_module_alias(self, alias: str, module_path: str, *, after_init: bool = False):
        """Adds an alias for a module.

        http://uwsgi-docs.readthedocs.io/en/latest/PythonModuleAlias.html

        :param alias:
        :param module_path:
        :param after_init: add a python module alias after uwsgi module initialization

        """
        command = 'post-pymodule-alias' if after_init else 'pymodule-alias'
        self._set(command, f'{alias}={module_path}', multi=True)

        return self._section

    def import_module(self, modules: Strint, *, shared: bool = False, into_spooler: bool = False):
        """Imports a python module.

        :param modules:

        :param shared: If shared import is done once in master process.
            Otherwise import a python module in all of the processes.
            This is done after fork but before request processing.

        :param into_spooler: Import a python module in the spooler.
            http://uwsgi-docs.readthedocs.io/en/latest/Spooler.html

        """
        if all((shared, into_spooler)):
            raise ConfigurationError('Unable to set both `shared` and `into_spooler` flags')

        if into_spooler:
            command = 'spooler-python-import'
        else:
            command = 'shared-python-import' if shared else 'python-import'

        self._set(command, modules, multi=True)

        return self._section

    def run_module(self, module: str):
        """Runs a Python script in the uWSGI environment.

        :param module:

        """
        self._set('pyrun', module)

        return self._section
