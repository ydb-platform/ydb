import importlib.util
import inspect
import os
from importlib import import_module
from typing import Optional

from uwsgiconf.presets.nice import PythonSection
from uwsgiconf.settings import CONFIGS_MODULE_ATTR
from uwsgiconf.utils import ConfModule, UwsgiRunner

if False:  # pragma: nocover
    from uwsgiconf.base import Section  # noqa


def find_project_dir() -> str:
    """Runs up the stack to find the location of manage.py
    which will be considered a project base path.

    """
    frame = inspect.currentframe()

    while True:
        frame = frame.f_back
        filename = frame.f_globals['__file__']

        if os.path.basename(filename).startswith('manage.py'):  # support py, pyc, etc.
            break

    return os.path.dirname(filename)


def get_project_name(project_dir: str) -> str:
    """Return project name from project directory.

    :param project_dir:

    """
    return os.path.basename(project_dir)


class SectionMutator:
    """Configuration file section mutator."""

    def __init__(self, section: 'Section', dir_base: str, project_name: str, options: dict):
        from django.conf import settings

        self.section = section
        self.dir_base = dir_base
        self.project_name = project_name
        self.settings = settings
        self.options = options

    @property
    def runtime_dir(self) -> str:
        """Project runtime directory."""
        return self.section.replace_placeholders('{project_runtime_dir}')

    def get_pid_filepath(self) -> str:
        """Return pidfile path for the given project."""
        return os.path.join(self.runtime_dir, 'uwsgi.pid')

    def get_fifo_filepath(self) -> str:
        """Return master FIFO path for the given project."""
        return os.path.join(self.runtime_dir, 'uwsgi.fifo')

    @classmethod
    def spawn(cls, options: dict = None, dir_base: str = None) -> 'SectionMutator':
        """Alternative constructor. Creates a mutator and returns section object.

        :param options:
        :param dir_base:

        """
        options_all = {
            'compile': True,
            'embedded': False,

            'contribute_static': False,
            'contribute_runtimes': False,
            'contribute_errpages': False,
        }
        options_all.update(options or {})

        dir_base = os.path.abspath(dir_base or find_project_dir())

        name_module = ConfModule.default_name
        name_project = get_project_name(dir_base)
        path_conf = os.path.join(dir_base, name_module)
        embedded = options_all['embedded']

        # Read an existing config for further modification of first section.
        section = cls._get_section_existing(
            path_conf, name_module, name_project,
            embedded=embedded)

        if not section:
            # Create section on-fly.
            section = cls._get_section_new(dir_base)

        mutator = cls(
            section=section,
            dir_base=dir_base,
            project_name=name_project,
            options=options_all)

        mutator.mutate(embedded=embedded)

        return mutator

    @classmethod
    def _get_section_existing(
            self,
            path_conf: str,
            name_module: str,
            name_project: str,
            embedded: bool = False
    ) -> Optional['Section']:
        """Loads config section from existing configuration file (aka uwsgicfg.py)

        :param path_conf: Path containing configuration module.

        :param name_module: Configuration module name.

        :param name_project: Project (package) name.

        :param embedded: Flag. Do not try to load module file from file system manually,
            but try to import the module.

        """
        def load():
            module_fake_name = f'{name_project}.{os.path.splitext(name_module)[0]}'

            spec = importlib.util.spec_from_file_location(module_fake_name, path_conf)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            return module

        if embedded:
            try:
                module = import_module(f"{name_project}.{name_module.rstrip('.py')}")

            except ModuleNotFoundError:
                return None

        else:
            if os.path.exists(path_conf):
                module = load()
            else:
                return None

        config = getattr(module, CONFIGS_MODULE_ATTR)[0]
        section = config.sections[0]

        return section

    @classmethod
    def _get_section_new(cls, dir_base: str) -> PythonSection:
        """Creates a new section with default settings.

        :param dir_base:

        """
        from django.conf import settings

        wsgi_app = settings.WSGI_APPLICATION
        name_package, name_module, name_func = wsgi_app.split('.')

        section = PythonSection.bootstrap(
            'http://127.0.0.1:8000',
            wsgi_module=f'{name_package}.{name_module}',
        )

        if os.path.exists(dir_base):
            section.main_process.change_dir(dir_base)

        return section

    def contribute_static(self):
        """Contributes static and media file serving settings to an existing section."""
        settings = self.settings
        statics = self.section.statics

        static_tuples = (
            (settings.STATIC_URL, settings.STATIC_ROOT),
            (settings.MEDIA_URL, settings.MEDIA_ROOT),
        )
        for url, path in static_tuples:
            path and statics.register_static_map(url, path)

        if self.options['compile']:
            return

        from django.core.management import call_command
        call_command('collectstatic', clear=True, interactive=False)

    def contribute_error_pages(self):
        """Contributes generic static error massage pages to an existing section."""

        static_dir = self.settings.STATIC_ROOT

        if not static_dir:
            # Source static directory is not configured. Use temporary.
            import tempfile
            static_dir = os.path.join(tempfile.gettempdir(), self.project_name)
            self.settings.STATIC_ROOT = static_dir

        self.section.routing.set_error_pages(
            common_prefix=os.path.join(static_dir, 'uwsgify'))

    def contribute_runtime_dir(self):
        section = self.section

        if not section.get_runtime_dir(default=False):
            # If runtime directory is not set by user, let's try use system default.
            section.set_runtime_dir(section.get_runtime_dir())

            if not self.options['compile']:
                os.makedirs(self.runtime_dir, 0o755, True)

    def mutate(self, embedded: bool = False):
        """Mutates current section."""
        section = self.section
        project_name = self.project_name

        section.project_name = project_name

        main = section.main_process

        if embedded:

            # The following should prevent possible segfaults in uwsgi_set_processname()'s memsets
            # while embedded.
            main.set_naming_params(autonaming=False)

            # Applications registry is ready by now,
            # we import base uwsgiinit in master process to bootstrap.
            section.python.import_module(
                'uwsgiconf.contrib.django.uwsgify.uwsgiinit',
                shared=True,
            )

        else:
            main.set_naming_params(prefix=f'[{project_name}] ')

        section.print_out(
            f"Embedded mode: {'yes' if embedded else 'no'}",
            format_options='blue')

        # todo maybe autoreload in debug

        apps = section.applications
        apps.set_basic_params(
            manage_script_name=True,
        )

        options = self.options

        if options['contribute_runtimes']:

            self.contribute_runtime_dir()

            main.set_pid_file(
                self.get_pid_filepath(),
                before_priv_drop=False,  # For vacuum to cleanup properly.
                safe=True,
            )

            section.master_process.set_basic_params(
                fifo_file=self.get_fifo_filepath(),
            )

        if options['contribute_static']:
            self.contribute_static()

        if options['contribute_errpages']:
            self.contribute_error_pages()


def run_uwsgi(config_section: 'Section', compile_only: bool = False, embedded: bool = False):
    """Runs uWSGI using the given section configuration.

    :param config_section:

    :param compile_only: Do not run, only compile and output configuration file for run.

    :param embedded: Do not create temporary config files and try to use resource files for configuration.

    """
    config = config_section.as_configuration()

    if compile_only:
        config.print_ini()
        return

    runner = UwsgiRunner()
    runner.spawn(
        config=config,
        replace=True,
        embedded=embedded,
    )
