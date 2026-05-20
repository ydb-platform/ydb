import os
import os.path

import yaml
import logging
from functools import lru_cache

import ydb.tools.mnc.scheme as scheme
from ydb.tools.mnc.lib.exceptions import CliError, ConfigError


logger = logging.getLogger(__name__)

multinode_configure_config_dir = '.mnc'

user = os.environ.get('USER', 'ydb')
home_path = os.environ.get('HOME', '/')
default_path_to_git_ydb = os.path.join(home_path, 'ydbwork', 'ydb')

local_config_dir_path = os.path.join(home_path, multinode_configure_config_dir)
mnc_config_path = os.path.join(local_config_dir_path, 'mnc.yaml')


@lru_cache
def read_config(config_scheme, name, path):
    try:
        config = scheme.parse(path)
        logger.debug(f"read config: scheme='{scheme.__name__}' name='{name}' path='{path}'")
        return config
    except Exception as ex:
        logger.debug(f"can't read config: scheme='{scheme.__name__}' name='{name}' path='{path}' reason='{ex}'")
    return None


def find_config(config_scheme, name, paths=None):
    if paths is None:
        paths = [local_config_dir_path]
    for dir_path in paths:
        for suffix in ['.yaml', '']:
            path = os.path.join(dir_path, f'{name}{suffix}')
            config = read_config(scheme, name, path)
            if config is not None:
                return config
    return None


class QuestionError(Exception):
    def __init__(self, message):
        super().__init__(message)


def oneof(elems):
    return lambda x: x in elems


def is_directory(path):
    is_dir = os.path.isdir(path)
    if not is_dir:
        print(f'Not a directory: {path}')
    return is_dir


def repeated_question(repeat_count, var, question, default, response_checker, lower=False):
    for idx in range(10):
        if question:
            print(f'{question} (default: {default})')
        val = input(f'{var}: ')
        if lower:
            val = val.lower()
        if val and not response_checker(val):
            continue
        if not val:
            val = default
        return val
    raise QuestionError(f'Failed to get {var}')


def verify_mnc_config(cfg):
    if not cfg.get('git_ydb_root'):
        raise ConfigError(mnc_config_path, 'git_ydb_root is required')
    if not os.path.isdir(cfg['git_ydb_root']):
        raise ConfigError(mnc_config_path, f"git_ydb_root is not a directory: {cfg['git_ydb_root']}")


def make_mnc_config_interctively():
    questions = [
        ('git_ydb_root', 'Write path to git ydb root.', default_path_to_git_ydb, is_directory),
    ]

    config = {}
    try:
        for var, question, default, expected in questions:
            config[var] = repeated_question(10, var, question, default, expected)
        verify_mnc_config(config)
        need_to_save = repeated_question(10, 'Save config? [Y/n]', None, 'y', oneof(['y', 'yes', 'n', 'no']), lower=True)
    except ConfigError as error:
        error.config_path = 'interactive mnc'
        raise error
    except QuestionError as error:
        raise ConfigError('interactive mnc', str(error))

    if need_to_save[0] == 'y':
        os.makedirs(os.path.dirname(mnc_config_path), exist_ok=True)
        with open(mnc_config_path, 'w') as file:
            print(yaml.safe_dump(config), file=file)
    return config


@lru_cache
def get_mnc_config():
    config = read_config(scheme.mnc, 'mnc', mnc_config_path)
    if config is not None:
        verify_mnc_config(config)
        return config
    return make_mnc_config_interctively()


def get_config_by_args(config_scheme, args):
    mnc_config = get_mnc_config()
    if args.config_path is None and args.config_name is None:
        raise CliError('Config must be specified by --config_path or --config')
    if args.config_path is not None:
        return read_config(config_scheme, 'config_from_cli', args.config_path)
    paths = [
        local_config_dir_path,
        os.path.join(mnc_config['git_ydb_root'], 'junk', user, multinode_configure_config_dir)
    ]
    config = find_config(config_scheme, args.config_name, paths)
    if config is not None:
        return config
    raise CliError('Failed to find config')


def get_config(config_scheme, args):
    d = get_config_by_args(config_scheme, args)

    if d is None:
        raise CliError('Failed to find config')

    if d is not None and 'arc_token' in d:
        if 'get_from_system' in d['arc_token']:
            with open(f'{os.environ.get("HOME")}/.arc/token', 'r') as file:
                d['arc_token']['token'] = file.read()
                del d['arc_token']['get_from_system']

    required_cli_args = d.get('required_cli_args', set())
    rejected_cli_args = d.get('rejected_cli_args', set())

    errors = []
    for name in sorted(required_cli_args):
        v = getattr(args, name, None)
        if v is None:
            errors.append(f'required cli argument: {name}')
        d[name] = v
    for name in sorted(rejected_cli_args):
        v = getattr(args, name, None)
        if v is not None:
            errors.append(f'rejected cli argument: {name}')

    if errors:
        raise CliError('\n'.join(errors))
    return d
