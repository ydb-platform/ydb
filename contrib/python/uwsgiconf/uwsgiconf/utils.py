import logging
import os
import sys
from collections import namedtuple
from contextlib import contextmanager
from importlib import import_module
from io import StringIO
from types import ModuleType
from typing import Optional, Any, List, Tuple, Dict

from .exceptions import UwsgiconfException
from .settings import CONFIGS_MODULE_ATTR
from .typehints import Strlist

if False:  # pragma: nocover
    from .config import Configuration  # noqa


EmbeddedPlugins = namedtuple('EmbeddedPlugins', ['generic', 'request'])


def get_logger(name: str):
    # Here to mitigate module name clashing.
    return logging.getLogger(name)


def encode(value: str) -> bytes:
    """Encodes str into bytes."""
    return value.encode('utf-8')


def decode(value: Optional[bytes]) -> Optional[str]:
    """Decodes bytes into str."""
    if value is None:
        return value
    return value.decode('utf-8')


def decode_deep(value: Any) -> Any:
    """Decodes object deep if required.

    :param value:

    """
    if isinstance(value, dict):
        out = {}
        for key, val in value.items():
            out[decode_deep(key)] = decode_deep(val)

    elif isinstance(value, tuple):
        out = tuple(decode_deep(item) for item in value)

    elif isinstance(value, list):
        out = [decode_deep(item) for item in value]

    elif isinstance(value, bytes):
        out = decode(value)

    else:
        out = value

    return out


@contextmanager
def output_capturing():
    """Temporarily captures/redirects stdout."""
    out = sys.stdout

    sys.stdout = StringIO()

    try:
        yield

    finally:
        sys.stdout = out


class ConfModule:
    """Represents a uwsgiconf configuration module.

    Allows reading configurations from .py files.

    """
    default_name: str = 'uwsgicfg.py'

    def __init__(self, fpath: str):
        """Module filepath.

        :param fpath:

        """
        fpath = os.path.abspath(fpath)
        self.fpath = fpath
        self._confs = None

    def spawn_uwsgi(self, *, only: str = None) -> List[Tuple[str, int]]:
        """Spawns uWSGI process(es) which will use configuration(s) from the module.

        Returns list of tuples:
            (configuration_alias, uwsgi_process_id)

        If only one configuration found current process (uwsgiconf) is replaced with a new one (uWSGI),
        otherwise a number of new detached processes is spawned.

        :param only: Configuration alias to run from the module.
            If not set uWSGI will be spawned for every configuration found in the module.

        """
        spawned = []
        configs = self.configurations

        if len(configs) == 1:

            alias = configs[0].alias
            UwsgiRunner().spawn(configs[0], replace=True, filepath=self.fpath)
            spawned.append((alias, os.getpid()))

        else:
            for config in configs:
                alias = config.alias

                if only is None or alias == only:
                    pid = UwsgiRunner().spawn(config, filepath=self.fpath)
                    spawned.append((alias, pid))

        return spawned

    @property
    def configurations(self) -> List['Configuration']:
        """Configurations from uwsgiconf module."""

        if self._confs is not None:
            return self._confs

        with output_capturing():
            module = self.load(self.fpath)
            confs = getattr(module, CONFIGS_MODULE_ATTR)
            confs = listify(confs)

        self._confs = confs

        return confs

    @classmethod
    def load(cls, fpath: str) -> ModuleType:
        """Loads a module and returns its object.

        :param fpath:

        """
        module_name = os.path.splitext(os.path.basename(fpath))[0]

        sys.path.insert(0, os.path.dirname(fpath))
        try:
            module = import_module(module_name)

        finally:
            sys.path = sys.path[1:]

        return module


def listify(src: Any) -> List:
    """Make a list with source object if not already a list.

    :param src:

    """
    if not isinstance(src, list):
        src = [src]

    return src


def filter_locals(
        locals_dict: Dict[str, Any],
        *,
        drop: List[str] = None,
        include: List[str] = None
) -> Dict[str, Any]:
    """Filters a dictionary produced by locals().

    :param locals_dict:

    :param drop: Keys to drop from dict.

    :param include: Keys to include into dict.

    """
    drop = drop or []
    drop.extend([
        'self',
        '__class__',  # py3
    ])

    include = include or locals_dict.keys()

    relevant_keys = [key for key in include if key not in drop]

    locals_dict = {k: v for k, v in locals_dict.items() if k in relevant_keys}

    return locals_dict


class KeyValue:
    """Allows lazy flattening the given dictionary into a key-value string."""

    def __init__(
            self,
            locals_dict: Dict[str, Any],
            *,
            keys: List[str] = None,
            aliases: Dict[str, str] = None,
            bool_keys: List[str] = None,
            list_keys: List[str] = None,
            items_separator: str = ','
    ):
        """

        :param locals_dict: Dictionary produced by locals().

        :param keys: Relevant keys from dictionary.
            If not defined - all keys are relevant.
            If defined keys will flattened into string using given order.

        :param aliases: Mapping key names from locals_dict into names
            they should be replaced with.

        :param bool_keys: Keys to consider their values bool.

        :param list_keys: Keys expecting lists.

        :param items_separator: String to use as items (chunks) separator.

        """
        self.locals_dict = dict(locals_dict)
        self.keys = keys or sorted(filter_locals(locals_dict).keys())
        self.aliases = aliases or {}
        self.bool_keys = bool_keys or []
        self.list_keys = list_keys or []
        self.items_separator = items_separator

    def __str__(self):
        value_chunks = []

        for key in self.keys:
            val = self.locals_dict[key]

            if val is not None:

                if key in self.bool_keys:
                    val = 1

                elif key in self.list_keys:

                    val = ';'.join(listify(val))

                value_chunks.append(f'{self.aliases.get(key, key)}={val}')

        return self.items_separator.join(value_chunks).strip()


def get_output(cmd: str, *, args: Strlist) -> str:
    """Runs a command and returns its output (stdout + stderr).

    :param cmd:
    :param args:

    """
    from subprocess import Popen, STDOUT, PIPE

    command = [cmd]
    command.extend(listify(args))

    process = Popen(command, stdout=PIPE, stderr=STDOUT)
    out, _ = process.communicate()

    return out.decode('utf-8')


class Finder:
    """Finds various entities."""

    @classmethod
    def uwsgiconf(cls) -> str:
        """Finds uwsgiconf executable location."""
        return get_output('which', args=['uwsgiconf']).strip()

    @classmethod
    def python(cls) -> str:
        """Finds Python executable location."""
        return sys.executable


class Fifo:
    """uWSGI Master FIFO interface."""

    def __init__(self, fifo_filepath: str):
        """
        :param fifo_filepath: Path to uWSGI Master FIFO file.

        """
        self.fifo = fifo_filepath

    def cmd_log(self, *, reopen: bool = False, rotate: bool = False):
        """Allows managing of uWSGI log related stuff

        :param reopen: Reopen log file. Could be required after third party rotation.
        :param rotate: Trigger built-in log rotation.

        """
        cmd = b''

        if reopen:
            cmd += b'l'

        if rotate:
            cmd += b'L'

        return self.send_command(cmd)

    def cmd_stats(self):
        """Dump uWSGI configuration and current stats into the log."""
        return self.send_command(b's')

    def cmd_stop(self, *, force: bool = False):
        """Shutdown uWSGI instance.

        :param force: Use forced (brutal) shutdown instead of a graceful one.

        """
        return self.send_command(b'Q' if force else b'q')

    def cmd_reload(self, *, force: bool = False, workers_only: bool = False, workers_chain: bool = False):
        """Reloads uWSGI master process, workers.

        :param force: Use forced (brutal) reload instead of a graceful one.
        :param workers_only: Reload only workers.
        :param workers_chain: Run chained workers reload (one after another,
            instead of destroying all of them in bulk).

        """
        if workers_chain:
            return self.send_command(b'c')

        if workers_only:
            return self.send_command(b'R' if force else b'r')

        return self.send_command(b'R' if force else b'r')

    def send_command(self, cmd: bytes):
        """Sends a generic command into FIFO.

        :param cmd: Command chars to send into FIFO.

        """
        if not cmd:
            return

        with open(self.fifo, 'wb') as f:
            f.write(cmd)


class UwsgiRunner:
    """Exposes methods to run uWSGI."""

    def __init__(self, binary_path: str = None):
        self.binary_uwsgi = binary_path or 'uwsgi'
        self.binary_python = self.prepare_env()

    def get_output(self, command_args: Strlist) -> str:
        """Runs a command and returns its output (stdout + stderr).

        :param command_args:

        """
        return get_output(self.binary_uwsgi, args=command_args)

    def get_plugins(self) -> EmbeddedPlugins:
        """Returns ``EmbeddedPlugins`` object with."""
        out = self.get_output('--plugin-list')
        return parse_command_plugins_output(out)

    @classmethod
    def get_env_path(cls) -> str:
        """Returns PATH environment variable updated
        to run uwsgiconf in (e.g. for virtualenv).

        """
        return os.path.dirname(Finder.python()) + os.pathsep + os.environ['PATH']

    @classmethod
    def prepare_env(cls) -> str:
        """Prepares current environment and returns Python binary name.

        This adds some virtualenv friendliness so that we try use uwsgi from it.

        """
        os.environ['PATH'] = cls.get_env_path()
        return os.path.basename(Finder.python())

    def spawn(
            self,
            config: 'Configuration',
            *,
            replace: bool = False,
            filepath: str = None,
            embedded: bool = False
    ):
        """Spawns uWSGI using the given configuration module.

        .. note::
            uWSGI loader schemas:
                * From a symbol -- sym://uwsgi_funny_function
                * From binary appended data -- data://0
                * From http -- http://example.com/hello
                * From a file descriptor -- fd://3
                * From a process stdout -- exec://foo.pl
                * From a function call returning a char * -- call://uwsgi_func

            Loading in config:
                foo = @(sym://uwsgi_funny_function)

        :param config: Configuration object to spawn uWSGI with.

        :param filepath: Override configuration file path.

        :param replace: Whether a new process should replace current one.

        :param embedded: Flag. Do not create a configuration file even if required,
            translate all config parameters into command line arguments and
            pass it to ``pyuwsgi``.

        """
        args = ['uwsgi']

        if embedded:
            import pyuwsgi  # noqa
            args.extend(config.format(formatter='args'))
            pyuwsgi.run(args[1:])

        else:
            args.append('--ini')

            filepath = filepath or config.tofile()

            if os.path.splitext(os.path.basename(filepath))[1] == '.ini':
                args.append(filepath)

            else:
                # Consider it to be a python script (uwsgicfg.py).
                # Pass --conf as an argument to have a chance to use
                # touch reloading form .py configuration file change.
                args.append(f'exec://{self.binary_python} {filepath} --conf {config.alias}')

        if replace:

            try:
                return os.execvp('uwsgi', args)

            except FileNotFoundError:

                raise UwsgiconfException(
                    'uWSGI executable not found. '
                    'Please make sure it is installed and available.')

        return os.spawnvp(os.P_NOWAIT, 'uwsgi', args)


def parse_command_plugins_output(out: str) -> EmbeddedPlugins:
    """Parses ``plugin-list`` command output from uWSGI
    and returns object containing lists of embedded plugin names.

    :param out:

    """
    out = out.split('--- end of plugins list ---')[0]
    out = out.partition('plugins ***')[2]
    out = out.splitlines()

    current_slot = 0

    plugins = EmbeddedPlugins([], [])

    for line in out:
        line = line.strip()

        if not line:
            continue

        if line.startswith('***'):
            current_slot += 1
            continue

        if current_slot is not None:
            plugins[current_slot].append(line)

    plugins = plugins._replace(request=[plugin.partition(' ')[2] for plugin in plugins.request])

    return plugins


def get_uwsgi_stub_attrs_diff() -> Tuple[List[str], List[str]]:
    """Returns attributes difference two elements tuple between
    real uwsgi module and its stub.

    Might be of use while describing in stub new uwsgi functions.

    Returns (uwsgi_only_attrs, stub_only_attrs)

    """
    try:
        import uwsgi

    except ImportError:
        from uwsgiconf.exceptions import UwsgiconfException

        raise UwsgiconfException(
            '`uwsgi` module is unavailable. Calling `get_attrs_diff` in such environment makes no sense.')

    from . import uwsgi_stub

    def get_attrs(src):
        return set(attr for attr in dir(src) if not attr.startswith('_'))

    attrs_uwsgi = get_attrs(uwsgi)
    attrs_stub = get_attrs(uwsgi_stub)

    from_uwsgi = sorted(attrs_uwsgi.difference(attrs_stub))
    from_stub = sorted(attrs_stub.difference(attrs_uwsgi))

    return from_uwsgi, from_stub
