import sys
import copy
import os
import shutil
import textwrap
import itertools


def halt(*args):
    print(*args, file=sys.stderr)
    sys.exit(1)


def internal_error(*args):
    halt('INTERNAL ERROR:', *args)


def get_parser(element):
    if isinstance(element, ArgumentParser):
        return element
    elif isinstance(element, (Parser, Subparsers, ArgumentMetaInfo, ValueMetaInfo)):
        if element.parser is None:
            raise ValueError(f'Element with type {type(element).__name__} with None parser')
        return element.parser
    elif isinstance(element, Argument):
        return get_parser(element.metainfo)
    elif isinstance(element, ValueHolder):
        return get_parser(element.metainfo)
    elif isinstance(element, MutuallyExclusiveGroup):
        return get_parser(element._parser)
    raise TypeError(f'Unknown argument type for get_parser, got {type(element)}')
    internal_error()


def print_with_word_wrapping(msg='', initial_indent='', subsequent_indent='', file=sys.stdout):
    width, _ = shutil.get_terminal_size()
    if width <= len(initial_indent):
        raise ValueError(f'initial_indent ({len(initial_indent)}) more than width ({width}) in terminal')
    if width <= len(subsequent_indent):
        raise ValueError(f'subsequent_indent ({len(subsequent_indent)}) more than width ({width}) in terminal')
    if isinstance(msg, str):
        lines = msg.splitlines()
    else:
        lines = list(itertools.chain((line.splitlines() for line in msg)))
    for line in lines:
        if line:
            text = textwrap.fill(
                line,
                width=width,
                initial_indent=initial_indent,
                subsequent_indent=subsequent_indent,
                drop_whitespace=False,
                replace_whitespace=False)
            print(text, file=file)
        else:
            print(initial_indent)
        initial_indent = subsequent_indent


def print_error_with_usage(element, msg):
    parser = get_parser(element)
    usage = parser._generate_usage()
    print_with_word_wrapping(usage, file=sys.stderr)
    print_with_word_wrapping(msg, file=sys.stderr)
    print_with_word_wrapping(f"To see more info run '{parser.generate_help_command()}'")
    sys.exit(1)


class ValueMetaInfo:
    def __init__(self, name, type, choices, parser):
        self.name = name
        self.type = type
        self.choices = choices
        self.owners = []
        self.parser = parser

    def apply_value(self, value):
        error_msg = None
        if self.choices is not None and value is not None and value not in self.choices:
            error_msg = 'Unexpected value {0} for {1}, it should be one of the next values: {2}'.format(
                value, self.name, ', '.join(self.choices))
        if self.type is not None:
            try:
                value = self.type(value)
            except Exception:
                error_msg = 'Can\'t convert value {0} to type {1} for {2}'.format(
                    value, self.type, self.name)
        if error_msg is not None:
            print_error_with_usage(self, error_msg)
        return value

    def add_owner(self, owner):
        self.owners.append(owner)

    def _additional_eq(self, other):
        return True

    def __eq__(self, other):
        return all((
            type(self) is type(other),
            self.name == other.name,
            self.type == other.type,
            self.choices == other.choices,
            self._additional_eq(other)
        ))


class SingleValueMetaInfo(ValueMetaInfo):
    def __init__(self, name, type, choices, default, required, parser=None):
        ValueMetaInfo.__init__(self, name, type, choices, parser)
        self.default = default
        self.required = required

    def _additional_eq(self, other):
        return self.default == other.default and self.required == other.required


class ListValueMetaInfo(ValueMetaInfo):
    def __init__(self, name, type, choices, required, min_count, max_count, parser=None):
        ValueMetaInfo.__init__(self, name, type, choices, parser)
        self.default = None
        self.min_count = min_count
        self.max_count = max_count
        self.required = required

    def _additional_eq(self, other):
        return self.min_count == other.min_count and self.max_count == other.max_count


class ValueHolder:
    def __init__(self, metainfo):
        self.metainfo = metainfo
        self._value = None
        self._referencing_args = []
        self.clear()

    def clear(self):
        self._value = copy.copy(self.metainfo.default)

    def value(self):
        return self._value

    def set_field(self, args):
        setattr(args, self.metainfo.name, self._value)

    def apply_value(self, arg):
        internal_error('Used method in base class without implementation')

    def check(self, arg):
        internal_error('Used method in base class without implementation')


class SingleValueHolder(ValueHolder):
    def __init__(self, metainfo):
        if not isinstance(metainfo, SingleValueMetaInfo):
            internal_error('Uncorrect metainfo for class SingleValueHolder')
        ValueHolder.__init__(self, metainfo)
        self._has_value = False

    def clear(self):
        ValueHolder.clear(self)
        self._has_value = False

    def apply_value(self, value):
        if self._has_value:
            print_error_with_usage(self, 'Value {0} already assigned by {1}, unexpected new value {2}'.format(
                self.metainfo.name, self._value, value))
        self._value = self.metainfo.apply_value(value)
        self._has_value = True

    def check(self):
        if self.metainfo.required and not self._has_value:
            print_error_with_usage(self, 'Value {0} is required'.format(self.metainfo.name))


class ListValueHolder(ValueHolder):
    def __init__(self, metainfo):
        if not isinstance(metainfo, ListValueMetaInfo):
            internal_error('Uncorrect metainfo for class ListValueHolder')
        ValueHolder.__init__(self, metainfo)
        self._has_value = False

    def clear(self):
        ValueHolder.clear(self)

    def apply_value(self, value):
        if self._value is None:
            self._value = list()
        applyed_values = len(self._value)
        if self.metainfo.max_count is not None and applyed_values >= self.metainfo.max_count:
            print_error_with_usage(self, 'Value {0} already riched max item count'.format(self.metainfo.name))
        self._value.append(self.metainfo.apply_value(value))
        self._has_value = True

    def check(self):
        if self.metainfo.required and not self._has_value:
            print_error_with_usage(self, 'Value {0} is required'.format(self.metainfo.name))
        if self._value is not None and len(self._value) < self.metainfo.min_count and self._has_value:
            print_error_with_usage(self, 'Value {0} is expected minimum {1} items, but gotten only {2}'.format(
                self.metainfo.name, self.metainfo.min_count, len(self._value)))


class ArgumentMetaInfo:
    def __init__(self, name, aliases, help, description=None, metavar=None, multivalue=False, is_expecting_value=True, parser=None):
        self.name = name
        self.aliases = aliases
        self.help = help
        self.description = description
        self.metavar = metavar
        self.multivalue = multivalue
        self.is_expecting_value = is_expecting_value
        self.parser = parser
        if parser is None:
            raise ValueError("parser mustn't be None")

    def make_lines_for_help(self, with_metavar):
        if len(self.aliases) > 1:
            res = '{{{0}}}'.format('|'.join(self.aliases))
        else:
            res = self.aliases[0]
        if self.metavar:
            res += ' ' + self.metavar
        elif with_metavar:
            res += ' ' + self.name.upper().replace('-', '_')
        return (res, self.help)


class Argument:
    def __init__(self, metainfo: ArgumentMetaInfo, value: ValueHolder, action):
        self.metainfo = metainfo
        self._value = value
        self._action = action
        self._is_presented = False
        if self._value is not None:
            self._value.metainfo.add_owner(self)

    def clear(self):
        if self._value is not None:
            self._value.clear()
        self._is_presented = False

    def apply(self, *args):
        self._action(self, *args)
        self._is_presented = True

    def is_expecting_value(self):
        return self.metainfo.is_expecting_value

    def make_help_lines(self):
        return self.metainfo.make_lines_for_help(self.metainfo.is_expecting_value)

    def check(self):
        if self._value is not None:
            self._value.check()


class ArgumentActions:
    @staticmethod
    def print_help(arg: Argument):
        arg.metainfo.parser._print_help()
        sys.exit(0)

    @staticmethod
    def make_store_const(const, expected_args=False):
        def set_const(arg: Argument, *args):
            if len(args) == 0:
                arg._value.apply_value(const)
            elif len(args) == 1 and expected_args:
                arg._value.apply_value(args[0])
            else:
                internal_error('Unexpected arguments')
        return set_const

    @staticmethod
    def default_action(arg, value):
        arg._value.apply_value(value)


class Subparsers:
    def __init__(self, value, parser):
        self._value = value
        self.parser = parser
        self._subparsers = []
        self._subparsers_names = []
        self._value.metainfo.choices = self._subparsers_names
        self._subparser_dict = dict()
        if self._value is not None:
            self._value.metainfo.add_owner(self)

    def add_parser(self, command, aliases=None, help=None, description=None):
        if aliases is None:
            aliases = [command]
        else:
            aliases = list(aliases)
            aliases.append(command)
        res = ArgumentParser(prog=command, description=description, help=help, aliases=aliases, parent=self.parser)
        self._subparsers.append(res)
        for alias in aliases:
            self._subparser_dict[alias] = res
            self._subparsers_names.append(alias)
        return res

    def check(self):
        self._value.check()

    def __getitem__(self, name):
        if name not in self._subparser_dict:
            print_error_with_usage(f"Unexpected command '{name}'")
        return self._subparser_dict[name]


class MutuallyExclusiveGroup:
    def __init__(self, group, parser, required=False):
        self._options = []
        self._is_required = required
        self._group = group
        self._parser = parser

    def add_argument(self, name, *args, **kwargs):
        self._options.append(name)
        self._group.add_argument(name, *args, **kwargs)

    def _get_option(self, name):
        return self._group._get_option(name)

    def check(self):
        presented = []
        for opt_name in self._options:
            opt = self._get_option(opt_name)
            if opt._is_presented:
                presented.append(opt_name)
        if self._is_required and not presented:
            print_error_with_usage(self, 'Expected one of the next options: ' + ','.join(self._options))
        if len(presented) > 1:
            print_error_with_usage(self, 'Presented mutually exclusive options: ' + ','.join(presented))

    def option_count(self):
        return len(self._options)


class OptionGroup:
    def __init__(self, title: str, description: str, parser):
        self._title = title
        self.description = description
        self.parser = parser
        self._options = []
        self._mutually_exclusive_groups = []

    def add_argument(self, name, *args, **kwargs):
        self._options.append(name)
        self.parser._add_argument(name, *args, **kwargs)

    def _get_option(self, name):
        return self.parser._option_dict[name]

    def add_mutually_exclusive_group(self, required=False):
        self._mutually_exclusive_groups.append(MutuallyExclusiveGroup(required=required, group=self, parser=self.parser))
        return self._mutually_exclusive_groups[-1]

    def check(self):
        for opt_name in self._options:
            self._get_option(opt_name).check()
        for group in self._mutually_exclusive_groups:
            group.check()

    def option_count(self):
        count = len(self._options)
        for group in self._mutually_exclusive_groups:
            count += group.option_count()
        return count

    def generate_lines_for_help(self):
        lines = [self._title + ':']
        if self._options:
            opt_lines = map(Argument.make_help_lines, map(self._get_option, self._options))
            for opt, help in opt_lines:
                lines.append((opt, '', help))
        return lines

    def generate_short_version_for_help(self):
        names = []
        if self._options:
            opt_lines = map(Argument.make_help_lines, map(self._get_option, self._options))
            for opt, _ in opt_lines:
                names.append(opt)
        return ', '.join(names)


class ArgumentParser:
    def __init__(self, prog=None, description=None, parent=None, aliases=None, help=None):
        self._parent = parent
        self.metainfo = ArgumentMetaInfo(
            sys.argv[0] if prog is None else prog,
            aliases,
            help=help,
            description=description,
            parser=self)
        self._values = dict()
        self._option_groups = []
        self._option_dict = dict()
        self._free_arguments = []
        self._subparsers = None
        self.add_argument_group(title='Options')
        self.add_argument('--help', '-?', '-h', action='print_help', help='Print usage')
        self._terminal_size = None

    def get_terminal_size(self):
        if self._parent is not None:
            return self._parent.get_terminal_size()
        if self._terminal_size is not None:
            return self._terminal_size
        return os.terminal_size(80, 24)

    def _has_additional_options(self):
        count = 0
        for group in self._option_groups:
            count += group.option_count()
        return count > 1

    def _get_options_name(self):
        if self._parent is None:
            return 'global options'
        return self.metainfo.name + ' options'

    def _generate_call_example(self):
        example = self.metainfo.name
        if self._parent:
            example = self._parent._generate_call_example() + ' ' + example
        return example

    def _generate_usage(self, with_subparser=True, with_options=True):
        usage = self.metainfo.name
        if self._parent:
            usage = self._parent._generate_usage(with_subparser=False, with_options=with_options) + ' ' + usage
        if self._has_additional_options() and with_options:
            usage += ' [{0} ...]'.format(self._get_options_name())
        if self._subparsers and with_subparser:
            usage += ' <subcommand>' if self._parent else ' <global_command>'
        return usage

    def generate_help_command(self):
        return f'{self._generate_usage(with_subparser=False, with_options=False)} --help'

    def _generate_description(self):
        return self.metainfo.description if self.metainfo.description else ''

    def _generate_help_text(self):
        return self.metainfo.help if self.metainfo.help else ''

    def _generate_subcommands(self):
        descr = self._generate_help_text()
        lines = [(self.metainfo.name, '', descr)]
        if self._subparsers is None or not self._subparsers._subparsers:
            return lines
        for idx, parser in enumerate(self._subparsers._subparsers):
            is_last = (idx + 1 == len(self._subparsers._subparsers))
            subcommands_lines = parser._generate_subcommands()
            prefixes = ['└─ ', '   '] if is_last else ['├─ ', '│  ']
            for line_idx, line_tuple in enumerate(subcommands_lines):
                prefix_idx = 1 if line_idx else 0
                initial = prefixes[prefix_idx] + line_tuple[0]
                subsequent = prefixes[1] + line_tuple[1]
                lines.append((initial, subsequent, line_tuple[2]))
        return lines

    def _generate_short_options(self):
        lines = []
        if self._parent is not None:
            lines = self._parent._generate_short_options()
        if self._has_additional_options():
            lines.append('')
            name = self._get_options_name()
            lines.append(name[:1].upper() + name[1:] + ':')
            from_groups = []
            for group in self._option_groups:
                from_groups.append(group.generate_short_version_for_help())
            lines.append('  ' + ', '.join(from_groups))
            lines.append("To get full description of these options run '{0} --help'".format(
                self._generate_call_example()))
        return lines

    def _generate_options(self):
        lines = []
        for idx, group in enumerate(self._option_groups):
            lines += group.generate_lines_for_help()
            lines.append('')
        return lines

    def _print_help(self):
        # USAGE
        print_with_word_wrapping('Usage: ' + self._generate_usage())
        # DESCRIPTION
        desc = self._generate_description()
        if desc:
            print()
            print_with_word_wrapping(self._generate_description())

        # SUBCOMMANDS
        subcommands_lines = self._generate_subcommands()
        max_length = 20
        if self._subparsers is not None and self._subparsers._subparsers:
            print()
            print_with_word_wrapping('Subcommands:')
            cmds = (x[0] for x in subcommands_lines)
            max_length = max(max_length, max(map(len, cmds)))
            for command, subsequent, help in subcommands_lines:
                if help:
                    optional_empty_line = '' if len(command) < max_length else '\n'
                    print_with_word_wrapping(
                        f'{optional_empty_line}{help}',
                        initial_indent=f'{command:<{max_length}}',
                        subsequent_indent=f'{subsequent:<{max_length}}')
                else:
                    print_with_word_wrapping(command)
        # SHORT OPTIONS
        if self._parent is not None:
            print()
            short_options_lines = self._parent._generate_short_options()
            if short_options_lines:
                for line in short_options_lines:
                    print_with_word_wrapping(
                        line, subsequent_indent=f'{"":<{4}}')
        # OPTIONS
        options_lines = self._generate_options()
        if options_lines:
            print()
            opts = (x[0] for x in options_lines if isinstance(x, tuple))
            max_length = min(max_length, max(10, max(map(len, opts)) + 2)) - 2
            for line in options_lines:
                if isinstance(line, str):
                    print_with_word_wrapping(line)
                else:
                    opt, empty, help = line
                    optional_empty_line = '' if len(opt) < max_length else '\n'
                    print_with_word_wrapping(
                        f'{optional_empty_line}{help}',
                        initial_indent=f'  {opt:<{max_length-2}}',
                        subsequent_indent=f'{empty:<{max_length}}')
        # FREE ARGS
        free_args_count = len(self._free_arguments) + int(bool(self._subparsers))
        if free_args_count:
            min_args = str(free_args_count)
            max_args = free_args_count if not self._subparsers else 'unlimited'
            print()
            print_with_word_wrapping('Free args: min: {0} max: {1}'.format(min_args, max_args))

            if self._free_arguments:
                for free_argument in self._free_arguments:
                    opt, help = free_argument.make_help_lines()
                    optional_empty_line = '' if len(opt) < max_length else '\n'
                    print_with_word_wrapping(
                        f'{optional_empty_line}{help}',
                        initial_indent=f'  {opt:<{max_length-2}}',
                        subsequent_indent=f'{"":<{max_length}}')
            if self._subparsers:
                if self._free_arguments:
                    print()
                line = '  <subcommand>  {0}'.format(', '.join((x.metainfo.name for x in self._subparsers._subparsers)))
                print_with_word_wrapping(line, subsequent_indent='  ')

    def get_option(self, opt_name):
        if opt_name not in self._option_dict:
            print_error_with_usage(self, f'Unknown options {opt_name}')
        return self._option_dict[opt_name]

    def _add_argument(self, name, *aliases, type=None, action=None, default=None, metavar=None, required=False, help='', choices=None, nargs=None, const=None, dest=None):
        realname = name
        if realname.startswith('--'):
            realname = realname[2:]
        if realname.startswith('-'):
            realname = realname[1:]

        if dest is None:
            dest = realname.replace('-', '_')

        multivalue = nargs is not None

        is_expecting_value = True
        if action == 'append' or (nargs is not None and nargs != '?'):
            min_count = 0
            max_count = None
            if nargs == '+':
                min_count = 1
            if isinstance(nargs, int):
                min_count = max_count = nargs
            valuemetainfo = ListValueMetaInfo(dest, type, choices, required, min_count, max_count, parser=self)
        elif action == 'print_help':
            valuemetainfo = None
            is_expecting_value = False
        else:
            if action == 'store_true':
                default = False
            if action == 'store_false':
                default = True
            valuemetainfo = SingleValueMetaInfo(dest, type, choices, default, required, parser=self)
            is_expecting_value = action not in ['store_const', 'store_true', 'store_false']

        value = None
        if valuemetainfo is not None:
            if dest in self._values:
                if valuemetainfo != self._values[dest].metainfo:
                    internal_error('Destination {0} was described twice with different parameters'.format(dest))
                value = self._values[dest]
            else:
                if isinstance(valuemetainfo, SingleValueMetaInfo):
                    value = SingleValueHolder(valuemetainfo)
                else:
                    value = ListValueHolder(valuemetainfo)
                self._values[dest] = value

        arg_action = ArgumentActions.default_action
        if action == 'store_const':
            arg_action = ArgumentActions.make_store_const(const)
        elif action == 'store_true':
            arg_action = ArgumentActions.make_store_const(True)
        elif action == 'store_false':
            arg_action = ArgumentActions.make_store_const(False)
        elif action == 'print_help':
            arg_action = ArgumentActions.print_help
        elif nargs == '?':
            arg_action = ArgumentActions.make_store_const(const, expected_args=True)

        if choices is not None and metavar is None:
            metavar = '{{{0}}}'.format(','.join(map(str, choices)))

        all_aliases = list(aliases)
        all_aliases.append(name)
        metainfo = ArgumentMetaInfo(
            realname, all_aliases, help, metavar=metavar,
            multivalue=multivalue, is_expecting_value=is_expecting_value, parser=self
        )

        arg = Argument(metainfo, value, arg_action)
        if value is not None:
            value._referencing_args.append(arg)

        if name.startswith('-'):
            self._option_dict[name] = arg
            for alias in aliases:
                self._option_dict[alias] = arg
        else:
            self._free_arguments.append(arg)

    def add_argument(self, name, *args, **kvargs):
        if name.startswith('-'):
            self._option_groups[0].add_argument(name, *args, **kvargs)
        else:
            self._add_argument(name, *args, **kvargs)

    def add_argument_group(self, title=None, description=None):
        if title is None:
            title = 'Options'
        if description is None:
            description = ''
        self._option_groups.append(OptionGroup(title, description, self))
        return self._option_groups[-1]

    def add_mutually_exclusive_group(self, *args, **kvargs):
        return self._option_groups[0].add_mutually_exclusive_group(*args, **kvargs)

    def add_subparsers(self, help=None, dest=None, required=False):
        if dest not in self._values:
            metainfo = SingleValueMetaInfo(dest, default=None, type=str, choices=None, required=required, parser=self)
            value = SingleValueHolder(metainfo)
            self._values[dest] = value
        else:
            value = self._values[dest]
        if self._subparsers is None:
            self._subparsers = Subparsers(value, self)
        return self._subparsers

    def parse_args(self, args=None):
        if args is None:
            args = sys.argv[1:]
        real_parser = Parser(args, self)
        real_parser.parse()
        return real_parser.parsed_args


class ParsedArgs:
    def __init__(self):
        pass


class Parser:
    def __init__(self, args, parser, parsed_args=None):
        self.parser = parser
        self._args = args
        self._current_idx = 0
        self._current_parser = parser
        self.parsed_args = parsed_args
        self._free_arg_idx = 0
        self._free_args_limit = len(self.parser._free_arguments) + int(bool(self.parser._subparsers))

    def _shift(self):
        self._current_idx += 1
        if self._current_idx >= len(self._args):
            return False
        return True

    def _value(self):
        return self._args[self._current_idx]

    def _is_end(self):
        return self._current_idx >= len(self._args)

    def _end(self):
        self._current_idx = len(self._args)

    def _apply_value(self, arg):
        arg.apply(self._value())
        self._shift()

    def _apply_multivalue(self, arg):
        while not self._is_end() and not self._value().startswith('-'):
            self._apply_value(arg)

    def _apply_option(self, option):
        if option[0:2] != '--':
            internal_error('Start parsing option without lead \'--\'')
        words = option.split('=')
        value = None
        if len(words) == 1:
            opt_name = words[0]
        elif len(words) == 2:
            [opt_name, value] = words
        else:
            opt_name = words[0]
            value = '='.join(words[1:])
        arg = self.parser.get_option(opt_name)
        if not arg.is_expecting_value() and value is not None:
            print_error_with_usage(self, f'Unexpected value {value} for option {opt_name}')
        if value is not None:
            arg.apply(value)
            self._shift()
        elif not self._shift() or not arg.is_expecting_value():
            arg.apply()
        elif arg.metainfo.multivalue:
            self._apply_multivalue(arg)
        else:
            self._apply_value(arg)

    def _apply_short_options(self, options):
        if options[0] != '-':
            internal_error('Start parsing short options without lead \'-\'')
        options = options[1:]
        for local_idx, opt_char in enumerate(options):
            opt_name = '-' + opt_char
            arg = self.parser.get_option(opt_name)
            if arg.is_expecting_value() and local_idx + 1 < len(options):
                arg.apply(options[local_idx+1:])
                self._shift()
                break
            elif not arg.is_expecting_value():
                arg.apply()
                self._shift()
            elif not self._shift():
                arg.apply()
            elif arg.metainfo.multivalue:
                self._apply_multivalue(arg)
            else:
                self._apply_value(arg)

    def _apply_free_argument(self, value):
        if self._free_arg_idx >= self._free_args_limit:
            print_error_with_usage(self, f'Unexpected free arg {value}')
        if self._free_arg_idx < len(self.parser._free_arguments):
            self.parser._free_arguments[self._free_arg_idx].apply(value)
            self._shift()
        else:
            self.parser._subparsers._value.apply_value(value)
            subparser = self.parser._subparsers[value]
            Parser(self._args[self._current_idx+1:], subparser, parsed_args=self.parsed_args).parse()
            self._end()
        self._free_arg_idx += 1

    def parse(self):
        if self.parsed_args is None:
            self.parsed_args = ParsedArgs()
        while not self._is_end():
            value = self._value()
            if value.startswith('--'):
                self._apply_option(value)
            elif value.startswith('-'):
                self._apply_short_options(value)
            else:
                self._apply_free_argument(value)
        for value in self.parser._values.values():
            value.set_field(self.parsed_args)
        for group in self.parser._option_groups:
            group.check()
        if self.parser._subparsers:
            self.parser._subparsers.check()
