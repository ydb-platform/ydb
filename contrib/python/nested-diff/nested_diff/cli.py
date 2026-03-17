# Copyright 2019-2024 Michael Samoglyadov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Common stuff for cli tools."""

import argparse
import os
import sys

import nested_diff
import nested_diff.handlers

HELP_EPILOG = """\
examples:
  Print version:
    %(prog)s --version
"""


class App:
    """Base class for command line tools."""

    default_ifmt = 'auto'
    default_ofmt = 'auto'

    supported_ifmts = (
        'auto',
        'ini',
        'json',
        'plaintext',
        'toml',
        'yaml',
    )
    supported_ofmts = supported_ifmts

    version = nested_diff.__version__

    def __init__(self, args=None):
        """Initialize application.

        Args:
            args: Command line arguments; sys.argv used by default.

        """
        self.override_excepthook()  # ASAP, but overridable by descendants

        self.argparser = self.get_argparser(
            description=self.__doc__,
            epilog=getattr(sys.modules[self.__module__], 'HELP_EPILOG', None),
        )
        self.args = self.argparser.parse_args(args=args)

    @staticmethod
    def _decode_fmt_opts(opts):
        if opts is None:
            return {}

        import json  # noqa: PLC0415

        return json.loads(opts)

    @classmethod
    def cli(cls):
        """Cli tool entry point."""
        try:
            return cls().run()
        except KeyboardInterrupt:
            return 2  # pragma nocover

    @property
    def dumper(self):
        """Return appropriate data dumper."""
        try:
            return self.__dumper
        except AttributeError:
            self.__dumper = self.get_dumper(
                self.args.ofmt,
                **self._decode_fmt_opts(self.args.ofmt_opts),
            )

            return self.__dumper

    def get_argparser(self, description=None, epilog=None):
        """Return complete CLI argument parser."""
        return argparse.ArgumentParser(
            description=description,
            parents=(
                self.get_optional_args_parser(),
                self.get_positional_args_parser(),
            ),
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=epilog,
        )

    def get_optional_args_parser(self):
        """Return parser for optional part (dash prefixed) of CLI args."""
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument(
            '--version',
            action='version',
            version=f'%(prog)s {self.version}',
            help='print version and exit',
        )
        parser.add_argument(
            '--ifmt',
            type=str,
            default=self.default_ifmt,
            choices=sorted(self.supported_ifmts),
            help='input files format; "%(default)s" is used by default',
        )
        parser.add_argument(
            '--ifmt-opts',
            metavar='JSON',
            type=str,
            help='input files format options (JSON string)',
        )
        parser.add_argument(
            '--ofmt',
            type=str,
            default=self.default_ofmt,
            choices=sorted(self.supported_ofmts),
            help='output files format; "%(default)s" is used by default',
        )
        parser.add_argument(
            '--ofmt-opts',
            metavar='JSON',
            type=str,
            help='output files format options (JSON string)',
        )

        return parser

    @staticmethod
    def get_positional_args_parser():
        """Return parser for positional part (files etc) of CLI args."""
        return argparse.ArgumentParser(add_help=False)

    @staticmethod
    def get_dumper(fmt, **kwargs):
        """Create dumper object according to passed format.

        Args:
            fmt: Dumper format.
            kwargs: Passed to dumper's constructor as is.

        Returns:
            Dumper object.

        Raises:
            RuntimeError: Unsupported format passed.

        """
        if fmt == 'json':
            return JsonDumper(**kwargs)
        if fmt == 'yaml':
            return YamlDumper(**kwargs)
        if fmt == 'ini':
            return IniDumper(**kwargs)
        if fmt == 'toml':
            return TomlDumper(**kwargs)
        if fmt == 'plaintext':
            return Dumper(**kwargs)

        raise RuntimeError(f'Unsupported output format: {fmt}')

    def guess_fmt(self, fp, default):
        """Guess format of a file object according it's extension."""
        try:
            fmt = os.path.splitext(fp.name)[-1].split('.')[-1].lower()
        except Exception:  # noqa: BLE001
            fmt = default

        if fmt == 'yml':
            fmt = 'yaml'

        return fmt if fmt in self.supported_ifmts else default

    @staticmethod
    def get_loader(fmt, **kwargs):
        """Create data loader object according to passed format.

        Args:
            fmt: Loader format.
            kwargs: Passed to loader's constructor as is.

        Returns:
            Loader object.

        Raises:
            RuntimeError: Unsupported format passed.

        """
        if fmt == 'json':
            return JsonLoader(**kwargs)
        if fmt == 'yaml':
            return YamlLoader(**kwargs)
        if fmt == 'ini':
            return IniLoader(**kwargs)
        if fmt == 'toml':
            return TomlLoader(**kwargs)
        if fmt == 'plaintext':
            return Loader(**kwargs)

        raise RuntimeError(f'Unsupported input format: {fmt}')

    def load(self, file_):
        """Load data from file using appropriate loader.

        Args:
            file_: File object to load from.

        Returns:
            Python object.

        """
        if self.args.ifmt == 'auto':
            fmt = self.guess_fmt(file_, 'plaintext')
        else:
            fmt = self.args.ifmt

        fmt_opts = self._decode_fmt_opts(self.args.ifmt_opts)

        return self.get_loader(fmt, **fmt_opts).load(file_)

    @staticmethod
    def override_excepthook():
        """Change default exit code for unhandled exceptions from 1 to 127.

        Mainly for diff tool (version control systems treat 1 as difference
        in files).

        """

        def overridden(*args, **kwargs):
            sys.__excepthook__(*args, **kwargs)  # do all the same
            raise SystemExit(127)  # but change exit code

        sys.excepthook = overridden

    def run(self):
        """App object entry point.

        Raises:
            NotImplementedError: Must be implemented in derivatives.

        """
        raise NotImplementedError


class Dumper:
    """Base class for data dumpers."""

    tty_final_new_line = False

    def __init__(self, header='', footer=''):
        """Initialize dumper.

        Args:
            header: Optional header to dump.
            footer: Optional footer to dump.

        """
        self.header = header
        self.footer = footer

    def encode(self, data):
        """Encode data.

        Args:
            data: Data to encode.

        Returns:
            Encoded data.

        """
        return data

    @staticmethod
    def get_opts(opts):
        """Return dumper options."""
        return opts

    def dump(self, file_, data):
        """Encode data and write to file.

        Args:
            file_: File object.
            data: Data to dump.

        """
        file_.write(self.encode(data))

        if self.tty_final_new_line and file_.isatty():
            file_.write('\n')

        file_.flush()


class Loader:
    """Base class for data loaders."""

    def decode(self, data):
        """Decode data.

        Args:
            data: Data to decode.

        Returns:
            Decoded data structure.

        """
        return data

    @staticmethod
    def get_opts(opts):
        """Return loader options."""
        return opts

    def load(self, file_):
        """Decode data loaded from file.

        Args:
            file_: File object.

        Returns:
            Python object.

        """
        return self.decode(file_.read())


class JsonDumper(Dumper):
    """JSON dumper."""

    tty_final_new_line = True

    def __init__(self, **kwargs):
        """Initialize dumper.

        Args:
            kwargs: Options for json.JSONEncoder.

        """
        super().__init__()

        import json  # noqa: PLC0415

        self.encoder = json.JSONEncoder(**self.get_opts(kwargs))

    def encode(self, data):
        """Encode data as JSON string."""
        return self.encoder.encode(data)

    @staticmethod
    def get_opts(opts):
        """Extend options by default values.

        indent opt is set to 3 and sort_keys opt to True if absent in opts.

        Args:
            opts: Initial options (dict).

        Returns:
            Options extended by default values.

        """
        opts.setdefault('indent', 3)
        opts.setdefault('sort_keys', True)
        return opts


class JsonLoader(Loader):
    """JSON loader."""

    def __init__(self, **kwargs):
        """Initialize loader.

        Args:
            kwargs: Options for json.JSONDecoder.

        """
        super().__init__()

        import json  # noqa: PLC0415

        self.decoder = json.JSONDecoder(**self.get_opts(kwargs))

    def decode(self, data):
        """Parse JSON string."""
        return self.decoder.decode(data)


class IniDumper(Dumper):
    """INI dumper."""

    def __init__(self, **kwargs):
        """Initialize dumper.

        Args:
            kwargs: Options for configparser.ConfigParser.

        """
        super().__init__()

        import configparser  # noqa: PLC0415
        import io  # noqa: PLC0415

        self.encoder = configparser.ConfigParser(**self.get_opts(kwargs))
        self.stringio = io.StringIO()

    def encode(self, data):
        """Encode data as INI string."""
        self.encoder.read_dict(data)
        self.encoder.write(self.stringio)

        return self.stringio.getvalue()


class IniLoader(Loader):
    """INI loader."""

    def __init__(self, **kwargs):
        """Initialize loader.

        Args:
            kwargs: Options for configparser.ConfigParser.

        """
        super().__init__()

        import configparser  # noqa: PLC0415

        self.decoder = configparser.ConfigParser(**kwargs)

    def decode(self, data):
        """Parse INI string."""
        self.decoder.read_string(data)

        out = {}
        for section in self.decoder.sections():
            out[section] = {}
            for option in self.decoder.options(section):
                out[section][option] = self.decoder.get(section, option)

            # cleanup (parser accumulates all confs)
            self.decoder.remove_section(section)

        return out


class PprintDumper(Dumper):
    """Pprint dumper."""

    def __init__(self, **kwargs):
        """Initialize dumper."""
        super().__init__()

        import pprint  # noqa: PLC0415

        self.codec = pprint.PrettyPrinter(**self.get_opts(kwargs))

    def encode(self, data):
        """Encode data as pprint does."""
        return self.codec.pformat(data)

    @staticmethod
    def get_opts(opts):
        """Extend options by default values.

        Args:
            opts: Kwargs for pprint.PrettyPrinter as dict.

        Returns:
            Options extended by default values.

        """
        defaults = {
            'indent': 1,
            'width': 80,
            'depth': None,
            'compact': True,
            'sort_dicts': True,
        }
        defaults.update(opts)

        return defaults


class TomlDumper(Dumper):
    """TOML dumper."""

    def __init__(self):
        """Initialize dumper."""
        super().__init__()

        import tomli_w  # noqa: PLC0415

        self.codec = tomli_w

    def encode(self, data):
        """Encode data as TOML string."""
        return self.codec.dumps(data)


class TomlLoader(Loader):
    """TOML loader."""

    def __init__(self):
        """Initialize loader."""
        super().__init__()

        if sys.version_info >= (3, 11):
            import tomllib  # pragma nocover # noqa: PLC0415
        else:
            import tomli as tomllib  # pragma nocover # noqa: PLC0415

        self.codec = tomllib

    def decode(self, data):
        """Parse TOML string."""
        return self.codec.loads(data)


class YamlDumper(Dumper):
    """YAML dumper."""

    def __init__(self, **kwargs):
        """Initialize dumper.

        Args:
            kwargs: Options for yaml.dump.

        """
        super().__init__()

        import yaml  # noqa: PLC0415

        try:
            from yaml import CSafeDumper as ImportedYamlDumper  # noqa: PLC0415
        except ImportError:
            from yaml import SafeDumper as ImportedYamlDumper  # noqa: PLC0415

        class _YamlDumper(ImportedYamlDumper):
            def represent_scalar(self, tag, value, style=None):
                if isinstance(value, str) and '\n' in value:
                    return super().represent_scalar(tag, value, style='|')

                return super().represent_scalar(tag, value, style=style)

        self.yaml = yaml
        self.yaml_dumper = _YamlDumper
        self.opts = self.get_opts(kwargs)

    def encode(self, data):
        """Encode data as YAML string."""
        return self.yaml.dump(data, Dumper=self.yaml_dumper, **self.opts)

    @staticmethod
    def get_opts(opts):
        """Extend options by default values.

        default_flow_style opt is set to `False` if absent in passed opts.

        Args:
            opts: Initial options (dict).

        Returns:
            Options extended by default values.

        """
        opts.setdefault('default_flow_style', False)
        opts.setdefault('explicit_start', True)

        return opts


class YamlLoader(Loader):
    """YAML loader."""

    def __init__(self, **kwargs):
        """Initialize loader.

        Args:
            kwargs: options for yaml.safe_load.

        """
        super().__init__()

        import yaml  # noqa: PLC0415

        try:
            from yaml import CSafeLoader as YamlLoader  # noqa: PLC0415
        except ImportError:
            from yaml import SafeLoader as YamlLoader  # noqa: PLC0415

        from yaml.nodes import (  # noqa: PLC0415
            MappingNode as YamlMappingNode,
            ScalarNode as YamlScalarNode,
            SequenceNode as YamlSequenceNode,
        )

        self.opts = self.get_opts(kwargs)
        self.yaml = yaml
        self.yaml_loader = YamlLoader

        def _default_constructor(loader, tag_suffix, node):  # noqa: ARG001
            tag = node.tag

            if isinstance(node, YamlScalarNode):
                value = node.value
            elif isinstance(node, YamlSequenceNode):
                node.tag = 'tag:yaml.org,2002:seq'
                value = loader.construct_sequence(node, deep=True)
            elif isinstance(node, YamlMappingNode):
                node.tag = 'tag:yaml.org,2002:map'
                value = loader.construct_mapping(node, deep=True)

            return YamlNode(tag, value)

        self.yaml_loader.add_multi_constructor(None, _default_constructor)

    def decode(self, data):
        """Parse YAML string."""
        items = list(
            self.yaml.load_all(data, Loader=self.yaml_loader, **self.opts),
        )

        if len(items) == 1:
            return items[0]

        return ListOfDocuments(items)


class ListOfDocuments:
    """Wrapper to represent bunch of documents like YAML stream."""

    def __init__(self, items):
        """Initialize wrapper.

        Args:
            items: list of documents.

        """
        self.items = items

    def __repr__(self):
        """Repr for wrapper."""
        return f'ListOfDocuments({self.items})'


class ListOfDocumentsHandler(nested_diff.handlers.ListHandler):
    """ListOfDocuments handler."""

    extension_id = 'nested_diff.ListOfDocuments'
    handled_type = ListOfDocuments

    def diff(self, differ, a, b):
        """Calculate diff for two ListOfDocuments objects.

        Args:
            differ: nested_diff.Differ object.
            a: First object to diff.
            b: Second object to diff.

        Returns:
            Tuple: equality flag and nested diff.

        """
        equal, diff = differ.diff(a.items, b.items)

        if diff:
            diff['E'] = self.extension_id

        return equal, diff


class YamlNode:
    """Wrapper to represent YAML node."""

    def __init__(self, tag, value):
        """Initialize wrapper.

        Args:
            tag: YAML node tag.
            value: YAML node value.

        """
        self.tag = tag
        self.value = value

    def __repr__(self):
        """Repr for YAML node wrapper."""
        return f"YamlNode(tag='{self.tag}', value={self.value!r})"


class YamlNodeHandler(nested_diff.handlers.TypeHandler):
    """YamlNode handler."""

    extension_id = 'nested_diff.YamlNode'
    handled_type = YamlNode

    def diff(self, differ, a, b):
        """Calculate diff for two YamlNode objects.

        Args:
            differ: nested_diff.Differ object.
            a: First node to diff.
            b: Second node to diff.

        Returns:
            Tuple: equality flag and nested diff.

        """
        equal, _ = differ.diff(a.tag, b.tag)

        if not equal:
            return equal, {'N': b, 'O': a, 'E': self.extension_id}

        equal, diff = differ.diff(a.value, b.value)

        if diff:
            diff = {
                'D': {'value': diff},
                'E': self.extension_id,
                'tag': a.tag,
            }

        return equal, diff

    def generate_formatted_diff(self, formatter, diff, depth):
        """Generate formatted YamlNode diff."""
        if 'D' in diff:
            yield from formatter.generate_string(diff['tag'], 'D', depth)
            yield from formatter.generate_diff(diff['D']['value'], depth + 1)

            return

        yield from formatter.generate_string(diff['O'].tag, 'O', depth)
        yield from formatter.generate_value(diff['O'].value, 'O', depth + 1)

        yield from formatter.generate_string(diff['N'].tag, 'N', depth)
        yield from formatter.generate_value(diff['N'].value, 'N', depth + 1)
