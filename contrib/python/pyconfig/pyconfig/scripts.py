import argparse
import ast
import os
import re
import sys

import pyconfig

# Pygments is optional but allows for colorization of output
try:
    import importlib

    pygments = importlib.import_module("pygments")
    importlib.import_module("pygments.lexers")
    importlib.import_module("pygments.formatters")
    pygments  # Make Pyflakes stop bitching about this being unused
except ImportError:
    pygments = None


def _create_parser(pygments_available):
    """Creates the argument parser for the script."""
    parser = argparse.ArgumentParser(
        description="Helper for working with pyconfigs", prog="pyconfig"
    )
    target_group = parser.add_mutually_exclusive_group()
    target_group.add_argument(
        "-f", "--filename", help="parse an individual file or directory", metavar="F"
    )
    target_group.add_argument(
        "-m",
        "--module",
        help="parse a package or module, recursively looking inside it",
        metavar="M",
    )
    parser.add_argument(
        "-v",
        "--view-call",
        help="show the actual pyconfig call made (default: show namespace)",
        action="store_true",
    )
    parser.add_argument(
        "-l",
        "--load-configs",
        help="query the currently set value for each key found",
        action="store_true",
    )
    key_group = parser.add_mutually_exclusive_group()
    key_group.add_argument(
        "-a",
        "--all",
        help="show keys which don't have defaults set",
        action="store_true",
    )
    key_group.add_argument(
        "-k",
        "--only-keys",
        help="show a list of discovered keys without values",
        action="store_true",
    )
    parser.add_argument(
        "-n",
        "--natural-sort",
        help="sort by filename and line (default: alphabetical by key)",
        action="store_true",
    )
    parser.add_argument(
        "-s",
        "--source",
        help="show source annotations (implies --natural-sort)",
        action="store_true",
    )
    parser.add_argument(
        "-c",
        "--color",
        help="toggle output colors (default: %s)" % pygments_available,
        action="store_const",
        default=pygments_available,
        const=(not pygments_available),
    )
    return parser


def main(argv=None):
    """
    Main script for `pyconfig` command.

    """
    parser = _create_parser(bool(pygments))
    args = parser.parse_args(argv)

    if args.color and not pygments:
        _error("Pygments is required for color output.\n    pip install pygments")

    if args.module:
        _handle_module(args)

    if args.filename:
        _handle_file(args)


class Unparseable(object):
    """
    This class represents an argument to a pyconfig setting that couldn't be
    easily parsed - e.g. was not a basic type.

    """

    def __repr__(self):
        return "<unparsed>"


class NotSet(object):
    """
    This class represents a default value which is not set.

    """

    def __repr__(self):
        return "<not set>"


class _PyconfigCall(object):
    """
    This class represents a pyconfig call along with its metadata.

    :param method: The method used, one of ``set``, ``get`` or ``setting``
    :param key: The pyconfig key
    :param default: The default value for the key if specified
    :param source: A 4-tuple of (filename, source line, line number, column)
    :type method: str
    :type key: str
    :type default: list
    :type source: tuple

    """

    def __init__(self, method, key, default, source):
        self.method = method
        self.key = key
        self.default = default
        self.filename, self.source, self.lineno, self.col_offset = source

    def as_namespace(self, namespace=None):
        """
        Return this call as if it were being assigned in a pyconfig namespace.

        If `namespace` is specified and matches the top level of this call's
        :attr:`key`, then that section of the key will be removed.

        """
        key = self.get_key()
        if namespace and isinstance(key, str) and key.startswith(namespace + "."):
            key = key[len(namespace) + 1 :]

        return "%s = %s" % (key, self._default() or NotSet())

    def as_live(self):
        """
        Return this call as if it were being assigned in a pyconfig namespace,
        but load the actual value currently available in pyconfig.

        """
        key = self.get_key()
        default = pyconfig.get(key)
        if default:
            default = repr(default)
        else:
            default = self._default() or NotSet()
        return "%s = %s" % (key, default)

    def as_call(self):
        """
        Return this call as it is called in its source.

        """
        default = self._default()
        default = ", " + default if default else ""
        return "pyconfig.%s(%r%s)" % (self.method, self.get_key(), default)

    def annotation(self):
        """
        Return this call's source annotation.

        """
        if not self.source:
            return "# Loaded config"
        return "# %s, line %s" % (self.filename, self.lineno)

    def get_key(self):
        """
        Return the call key, even if it has to be parsed from the source.

        """
        if not isinstance(self.key, Unparseable):
            return self.key

        line = self.source[self.col_offset :]
        regex = re.compile(r"""pyconfig\.[eginst]+\(([^,]+).*?\)""")
        match = regex.match(line)
        if not match:
            return Unparseable()

        return "<%s>" % match.group(1)

    def _source_call_only(self):
        """
        Return the source line stripped down to just the pyconfig call.

        """
        line = self.source[self.col_offset :]
        regex = re.compile(r"""(pyconfig\.[eginst]+\(['"][^)]+?['"].*?\))""")
        match = regex.match(line)
        if not match:
            # Fuck it, return the whole line
            return self.source

        return match.group(1)

    def _default_value_only(self):
        """
        Return only the default value, if there is one.

        """
        line = self.source[self.col_offset :]
        regex = re.compile(r"""pyconfig\.[eginst]+\(['"][^)]+?['"], ?(.*?)\)""")
        match = regex.match(line)
        if not match:
            return ""

        return match.group(1)

    def _default(self):
        """
        Return the default argument, formatted nicely.

        """
        try:
            # Check if it's iterable
            iter(self.default)
        except TypeError:
            if self.default is None:
                return ""
            return repr(self.default)

        # This is to look for unparsable values, and if we find one, we try to
        # directly parse the string
        for v in self.default:
            if isinstance(v, Unparseable):
                default = self._default_value_only()
                if default:
                    return default
        # Otherwise just make it a string and go
        return ", ".join(repr(v) for v in self.default)

    def __repr__(self):
        return self.as_call()


def _handle_module(args):
    """
    Handles the -m argument.

    """
    module = _get_module_filename(args.module)
    if not module:
        _error("Could not load module or package: %r", args.module)
    elif isinstance(module, Unparseable):
        _error("Could not determine module source: %r", args.module)

    _parse_and_output(module, args)


def _handle_file(args):
    """
    Handle the --file argument.

    """
    filename = args.filename
    _parse_and_output(filename, args)


def _error(msg, *args):
    """
    Print an error message and exit.

    :param msg: A message to print
    :type msg: str

    """
    print(msg % args, file=sys.stderr)
    sys.exit(1)


def _get_module_filename(module):
    """
    Return the filename of `module` if it can be imported.

    If `module` is a package, its directory will be returned.

    If it cannot be imported ``None`` is returned.

    If the ``__file__`` attribute is missing, or the module or package is a
    compiled egg, then an :class:`Unparseable` instance is returned, since the
    source can't be retrieved.

    :param module: A module name, such as ``'test.test_config'``
    :type module: str

    """
    # Split up the module and its containing package, if it has one
    module = module.split(".")
    package = ".".join(module[:-1])
    module = module[-1]

    try:
        if not package:
            # We aren't accessing a module within a package, but rather a top
            # level package, so it's a straight up import
            module = __import__(module)
        else:
            # Import the package containing our desired module
            package = __import__(package, fromlist=[module])
            # Get the module from that package
            module = getattr(package, module, None)

        filename = getattr(module, "__file__", None)
        if not filename:
            # No filename? Nothing to do here
            return Unparseable()

        # If we get a .pyc, strip the c to get .py so we can parse the source
        if filename.endswith(".pyc"):
            filename = filename[:-1]
            if not os.path.exists(filename) and os.path.isfile(filename):
                # If there's only a .pyc and no .py it's a compile package or
                # egg and we can't get at the source for parsing
                return Unparseable()
        # If we have a package, we want the directory not the init file
        if filename.endswith("__init__.py"):
            filename = filename[:-11]

        # Yey, we found it
        return filename
    except ImportError:
        # Definitely not a valid module or package
        return


def _parse_and_output(filename, args):
    """
    Parse `filename` appropriately and then output calls according to the
    `args` specified.

    :param filename: A file or directory
    :param args: Command arguments
    :type filename: str

    """
    relpath = os.path.dirname(filename)
    if os.path.isfile(filename):
        calls = _parse_file(filename, relpath)
    elif os.path.isdir(filename):
        calls = _parse_dir(filename, relpath)
    else:
        # XXX(shakefu): This is an error of some sort, maybe symlinks?
        # Probably need some thorough testing
        _error("Could not determine file type: %r", filename)

    if not calls:
        # XXX(shakefu): Probably want to change this to not be an error and
        # just be a normal fail (e.g. command runs, no output).
        _error("No pyconfig calls.")

    if args.load_configs:
        # We want to iterate over the configs and add any keys which haven't
        # already been found
        keys = set()
        for call in calls:
            keys.add(call.key)

        # Iterate the loaded keys and make _PyconfigCall instances
        conf = pyconfig.Config()
        for key, value in conf.settings.items():
            if key in keys:
                continue
            calls.append(_PyconfigCall("set", key, value, [None] * 4))

    _output(calls, args)


def _output(calls, args):
    """
    Outputs `calls`.

    :param calls: List of :class:`_PyconfigCall` instances
    :param args: :class:`~argparse.ArgumentParser` instance
    :type calls: list
    :type args: argparse.ArgumentParser

    """
    # Sort the keys appropriately
    if args.natural_sort or args.source:
        calls = sorted(calls, key=lambda c: (c.filename, c.lineno))
    else:
        calls = sorted(calls, key=lambda c: c.key)

    out = []

    # Handle displaying only the list of keys
    if args.only_keys:
        keys = set()
        for call in calls:
            if call.key in keys:
                continue
            out.append(_format_call(call, args))
            keys.add(call.key)

        out = "\n".join(out)
        if args.color:
            out = _colorize(out)
        print(out, end=" ")

        # We're done here
        return

    # Build a list of keys which have default values available, so that we can
    # toggle between displaying only those keys with defaults and all keys
    keys = set()
    for call in calls:
        if call.default:
            keys.add(call.key)

    for call in calls:
        if not args.all and not call.default and call.key in keys:
            continue
        out.append(_format_call(call, args))

    out = "\n".join(out)
    if args.color:
        out = _colorize(out)
    print(out, end=" ")


def _format_call(call, args):
    """
    Return `call` formatted appropriately for `args`.

    :param call: A pyconfig call object
    :param args: Arguments from the command
    :type call: :class:`_PyconfigCall`

    """
    out = ""
    if args.source:
        out += call.annotation() + "\n"

    if args.only_keys:
        out += call.get_key()
        return out

    if args.view_call:
        out += call.as_call()
    elif args.load_configs:
        out += call.as_live()
    else:
        out += call.as_namespace()

    return out


def _colorize(output):
    """
    Return `output` colorized with Pygments, if available.

    """
    if not pygments:
        return output
    # Available styles
    # ['monokai', 'manni', 'rrt', 'perldoc', 'borland', 'colorful', 'default',
    # 'murphy', 'vs', 'trac', 'tango', 'fruity', 'autumn', 'bw', 'emacs',
    # 'vim', 'pastie', 'friendly', 'native']
    return pygments.highlight(
        output,
        pygments.lexers.PythonLexer(),
        pygments.formatters.Terminal256Formatter(style="monokai"),
    )


def _parse_dir(directory, relpath):
    """
    Return a list of :class:`_PyconfigCall` from recursively parsing
    `directory`.

    :param directory: Directory to walk looking for python files
    :param relpath: Path to make filenames relative to
    :type directory: str
    :type relpath: str

    """
    relpath = os.path.dirname(relpath)
    pyconfig_calls = []
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if not filename.endswith(".py"):
                continue
            filename = os.path.join(root, filename)
            pyconfig_calls.extend(_parse_file(filename, relpath))

    return pyconfig_calls


def _parse_file(filename, relpath=None):
    """
    Return a list of :class:`_PyconfigCall` from parsing `filename`.

    :param filename: A file to parse
    :param relpath: Relative directory to strip (optional)
    :type filename: str
    :type relpath: str

    """
    with open(filename, "r") as source:
        source = source.read()

    pyconfig_calls = []
    try:
        nodes = ast.parse(source, filename=filename)
    except SyntaxError:
        # XXX(Jake): We might want to handle this differently
        return []

    # Look for UTF-8 encoding
    first_lines = source[0:200]
    match = re.match("^#.*coding[:=].?([a-zA-Z0-9-_]+).*", first_lines)
    if match:
        try:
            coding = match.group(1)
            source = source.decode(coding)
        except Exception:
            print("# Error decoding file, may not parse correctly:", filename)

    try:
        # Split the source into lines so we can reference it easily
        source = source.split("\n")
    except Exception:
        print("# Error parsing file, ignoring:", filename)
        return []

    # Make the filename relative to the given path, if needed
    if relpath:
        filename = os.path.relpath(filename, relpath)

    for call in ast.walk(nodes):
        if not isinstance(call, ast.Call):
            # Skip any node that isn't a Call
            continue

        func = call.func
        if not isinstance(call.func, ast.Attribute):
            # We're looking for calls to pyconfig.*, so the function has to be
            # an Attribute node, otherwise skip it
            continue

        if getattr(func.value, "id", None) != "pyconfig":
            # If the Attribute value isn't a Name (doesn't have an `id`) or it
            # isn't 'pyconfig', then we skip
            continue

        if func.attr not in ["get", "set", "setting"]:
            # If the Attribute attr isn't one of the pyconfig API methods, then
            # we skip
            continue

        # Now we parse the call arguments as best we can
        args = []
        if call.args:
            arg = call.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                args.append(arg.value)
            else:
                args.append(_map_arg(arg))

        for arg in call.args[1:]:
            args.append(_map_arg(arg))

        line = (filename, source[call.lineno - 1], call.lineno, call.col_offset)
        call = _PyconfigCall(func.attr, args[0], args[1:], line)
        pyconfig_calls.append(call)

    return pyconfig_calls


def _map_arg(arg):
    """
    Return `arg` appropriately parsed or mapped to a usable value.

    """
    # Python 3.8+ uses ast.Constant for literals
    if isinstance(arg, ast.Constant):
        return arg.value
    elif isinstance(arg, ast.Name):
        name = arg.id
        if name == "True":
            return True
        elif name == "False":
            return False
        elif name == "None":
            return None
        return name
    else:
        # Everything else we don't bother with
        return Unparseable()
