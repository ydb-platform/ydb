from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass


CACHE_RELATIVE_PATH = os.path.join('.mnc', 'cache', 'command_options')
CACHE_DIRECTORY_RELATIVE_PATH = os.path.dirname(CACHE_RELATIVE_PATH)
EXCLUDED_DESTS = {
    '__breaker__',
    'help',
    'quiet',
    'tui',
    'verbose',
}


@dataclass
class OptionSpec:
    aliases: list[str]
    dest: str
    expects_value: bool
    multivalue: bool

    @property
    def canonical_alias(self):
        for alias in self.aliases:
            if alias.startswith('--'):
                return alias
        return self.aliases[-1]


def cache_path():
    home = os.environ.get('HOME')
    if not home:
        return None
    return os.path.join(home, CACHE_RELATIVE_PATH)


def cache_dir_path():
    home = os.environ.get('HOME')
    if not home:
        return None
    return os.path.join(home, CACHE_DIRECTORY_RELATIVE_PATH)


def load_cache(path: str = None):
    path = path or cache_path()
    if path is None:
        return {}
    try:
        with open(path) as cache_file:
            data = json.load(cache_file)
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def save_cache(data: dict, path: str = None):
    path = path or cache_path()
    if path is None:
        return
    ensure_private_cache_dir(os.path.dirname(path))
    fd, tmp_path = tempfile.mkstemp(prefix='.command_options.', dir=os.path.dirname(path), text=True)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'w') as tmp_file:
            json.dump(data, tmp_file, indent=2, sort_keys=True)
            tmp_file.write('\n')
        os.replace(tmp_path, path)
        os.chmod(path, 0o600)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def ensure_private_cache_dir(path: str):
    os.makedirs(path, mode=0o700, exist_ok=True)
    home = os.environ.get('HOME')
    default_cache_dir = cache_dir_path()
    if home and default_cache_dir and os.path.abspath(path) == os.path.abspath(default_cache_dir):
        current = home
        for part in CACHE_DIRECTORY_RELATIVE_PATH.split(os.sep):
            current = os.path.join(current, part)
            os.chmod(current, 0o700)
        return
    os.chmod(path, 0o700)


def command_key(path: list[str]):
    return '/'.join(path)


def apply_cached_options(parser, argv: list[str]):
    path, leaf_parser, command_end = command_from_argv(parser, argv)
    if not path:
        return argv

    cache_entry = load_cache().get(command_key(path), {})
    cached_tokens = cache_entry.get('tokens', [])
    if not isinstance(cached_tokens, list) or not cached_tokens:
        return argv

    user_seen_dests = seen_dests_from_argv(parser, argv)
    tokens = filter_cached_tokens(leaf_parser, cached_tokens, user_seen_dests)
    if not tokens:
        return argv
    return argv[:command_end] + tokens + argv[command_end:]


def save_command_options(parser, args):
    path, leaf_parser = command_from_namespace(parser, args)
    if not path:
        return

    tokens = tokens_from_namespace(leaf_parser, args)
    data = load_cache()
    key = command_key(path)
    if tokens:
        data[key] = {'tokens': tokens}
    else:
        data.pop(key, None)
    save_cache(data)


def command_from_namespace(parser, args):
    path = []
    current = parser
    while current._subparsers is not None:
        dest = current._subparsers._value.metainfo.name
        name = getattr(args, dest, None)
        if not name:
            break
        path.append(name)
        current = current._subparsers[name]
    return path, current


def command_from_argv(parser, argv: list[str]):
    current = parser
    path = []
    idx = 0
    while idx < len(argv):
        token = argv[idx]
        option = find_option(current, token)
        if option is not None:
            idx = skip_option(argv, idx, option)
            continue
        if current._subparsers is not None and token in current._subparsers._subparser_dict:
            path.append(token)
            current = current._subparsers[token]
            idx += 1
            continue
        if token.startswith('-'):
            idx += 1
            continue
        break
    return path, current, idx


def seen_dests_from_argv(parser, argv: list[str]):
    current = parser
    seen = set()
    free_arg_idx = 0
    idx = 0
    while idx < len(argv):
        token = argv[idx]
        option = find_option(current, token)
        if option is not None:
            if option.dest:
                seen.add(option.dest)
            idx = skip_option(argv, idx, option)
            continue
        if current._subparsers is not None and token in current._subparsers._subparser_dict:
            current = current._subparsers[token]
            free_arg_idx = 0
            idx += 1
            continue
        if token.startswith('-'):
            idx += 1
            continue
        if free_arg_idx < len(current._free_arguments):
            dest = option_dest(current._free_arguments[free_arg_idx])
            if dest:
                seen.add(dest)
            free_arg_idx += 1
        idx += 1
    return seen


def filter_cached_tokens(parser, tokens: list[str], skipped_dests: set[str]):
    result = []
    free_arg_idx = 0
    idx = 0
    while idx < len(tokens):
        token = tokens[idx]
        option = find_option(parser, token)
        if option is not None:
            group, next_idx = option_group(tokens, idx, option)
            if option.dest not in skipped_dests:
                result.extend(group)
            idx = next_idx
            continue
        if free_arg_idx < len(parser._free_arguments):
            dest = option_dest(parser._free_arguments[free_arg_idx])
            if dest not in skipped_dests:
                result.append(token)
            free_arg_idx += 1
        idx += 1
    return result


def tokens_from_namespace(parser, args):
    tokens = []
    seen_dests = set()

    for free_argument in parser._free_arguments:
        dest = option_dest(free_argument)
        if dest is None:
            continue
        value = getattr(args, dest, None)
        if should_store_dest(dest, value):
            append_value_tokens(tokens, None, value)
            seen_dests.add(dest)

    for option in unique_options(parser):
        if option.dest in seen_dests:
            continue
        value = getattr(args, option.dest, None)
        if not should_store_dest(option.dest, value):
            continue
        if option.expects_value:
            append_value_tokens(tokens, option.canonical_alias, value)
        elif value:
            tokens.append(option.canonical_alias)
        seen_dests.add(option.dest)
    return tokens


def should_store_dest(dest, value):
    if dest in EXCLUDED_DESTS:
        return False
    if value is None:
        return False
    if value is False:
        return False
    if value == []:
        return False
    return True


def append_value_tokens(tokens: list[str], alias: str, value):
    if alias is not None:
        tokens.append(alias)
    if isinstance(value, (list, tuple)):
        tokens.extend(str(item) for item in value)
    else:
        tokens.append(str(value))


def unique_options(parser):
    seen_args = set()
    seen_dests = set()
    result = []
    for alias, argument in parser._option_dict.items():
        if id(argument) in seen_args:
            continue
        seen_args.add(id(argument))
        spec = option_spec(argument)
        if spec.dest is None:
            continue
        if spec.dest in EXCLUDED_DESTS or spec.dest in seen_dests:
            continue
        seen_dests.add(spec.dest)
        result.append(spec)
    return result


def find_option(parser, token: str):
    option_name = token.split('=', 1)[0]
    if option_name in parser._option_dict:
        return option_spec(parser._option_dict[option_name])
    return None


def option_spec(argument):
    return OptionSpec(
        aliases=list(argument.metainfo.aliases),
        dest=option_dest(argument),
        expects_value=argument.is_expecting_value(),
        multivalue=argument.metainfo.multivalue,
    )


def option_dest(argument):
    if argument._value is not None:
        return argument._value.metainfo.name
    return argument.metainfo.name


def skip_option(tokens: list[str], idx: int, option: OptionSpec):
    _, next_idx = option_group(tokens, idx, option)
    return next_idx


def option_group(tokens: list[str], idx: int, option: OptionSpec):
    group = [tokens[idx]]
    if '=' in tokens[idx] or not option.expects_value:
        return group, idx + 1
    idx += 1
    if option.multivalue:
        while idx < len(tokens) and not tokens[idx].startswith('-'):
            group.append(tokens[idx])
            idx += 1
        return group, idx
    if idx < len(tokens):
        group.append(tokens[idx])
        idx += 1
    return group, idx
