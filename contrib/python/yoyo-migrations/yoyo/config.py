# Copyright 2015 Oliver Cope
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

"""
Handle config file and argument parsing
"""
from collections import deque
from configparser import ConfigParser
from pathlib import Path
import configparser
import functools
import itertools
import os
import typing as t

CONFIG_FILENAME = "yoyo.ini"
CONFIG_EDITOR_KEY = "editor"
CONFIG_NEW_MIGRATION_COMMAND_KEY = "post_create_command"

INHERIT = "%inherit"
INCLUDE = "%include"


class CircularReferenceError(configparser.Error):
    """
    %include or %inherit directive has created a circular reference.
    """


class CustomInterpolation(configparser.BasicInterpolation):
    defaults: t.Dict[str, str] = {}

    def __init__(self, defaults):
        self.defaults = defaults or {}

    def before_get(self, parser, section, option, value, defaults):
        merged_defaults = self.defaults.copy()
        merged_defaults.update(defaults)
        return super(CustomInterpolation, self).before_get(
            parser, section, option, value, merged_defaults
        )


def get_interpolation_defaults(path: t.Optional[str] = None):
    parser = configparser.ConfigParser()
    defaults = {
        parser.optionxform(k): v.replace("%", "%%") for k, v in os.environ.items()
    }
    if path:
        defaults["here"] = os.path.dirname(os.path.abspath(path))
    return defaults


def get_configparser(defaults=None) -> ConfigParser:
    return ConfigParser(interpolation=CustomInterpolation(defaults))


def update_argparser_defaults(parser, defaults):
    """
    Update an ArgumentParser's defaults.

    Unlike ArgumentParser.set_defaults this will only set defaults for
    arguments the parser has configured.
    """
    known_args = {action.dest for action in parser._actions}
    parser.set_defaults(**{k: v for k, v in defaults.items() if k in known_args})


def read_config(src: t.Optional[str]) -> ConfigParser:
    """
    Read the configuration file at ``src`` and construct a ConfigParser instance.

    If ``src`` is None a new, empty ConfigParse object will be created.
    """
    if src is None:
        return get_configparser(get_interpolation_defaults())

    path = _make_path(src)
    config = _read_config(path)
    config_files = {path: config}
    merge_paths = deque([path])
    to_process: t.List[t.Tuple[t.Union[t.List[Path]], Path, ConfigParser]] = [
        ([], path, config)
    ]
    while to_process:
        ancestors, path, config = to_process.pop()
        inherits, includes = find_includes(path, config)
        for p in itertools.chain(inherits, includes):
            if p in ancestors:
                raise CircularReferenceError(
                    "{path!r} contains circular references".format(path=path)
                )
            config = _read_config(p)
            config_files[p] = config
            to_process.append((ancestors + [path], p, config))

        merge_paths.extendleft(inherits)
        merge_paths.extend(includes)

    merged = merge_configs([config_files[p] for p in merge_paths])
    merged.remove_option("DEFAULT", INCLUDE)
    merged.remove_option("DEFAULT", INHERIT)

    return merged


def _make_path(s: str, basepath: t.Optional[Path] = None) -> Path:
    """
    Return a fully resolved Path. Raises FileNotFoundError if the path does not
    exist.

    """
    if basepath:
        p = basepath.parent / s
    else:
        p = Path(s)

    # Path.resolve has a backward incompatible change in Python 3.6
    #
    # py35: will raise FileNotFoundError if the path doesn't exist.
    # py36: does not raise FileNotFoundError, unless you pass strict=True
    #
    # Passing strict=True would cause a TypeError in Python 3.5, we
    # have to do an explicit exists() check to support both versions.
    p = p.resolve()
    if not p.exists():
        raise FileNotFoundError(p)
    return p.resolve()


def _read_config(path: Path) -> ConfigParser:
    config = get_configparser(get_interpolation_defaults(str(path)))
    config.read([str(path)])
    return config


def find_includes(
    basepath: Path, config: ConfigParser
) -> t.Tuple[t.List[Path], t.List[Path]]:
    result: t.Dict[str, t.List[Path]] = {INCLUDE: [], INHERIT: []}
    for key in [INHERIT, INCLUDE]:
        try:
            paths = config["DEFAULT"][key].split()
        except KeyError:
            continue

        for p in paths:
            strict = True
            if p.startswith("?"):
                strict = False
                p = p[1:]
            try:
                path = _make_path(p, basepath)
            except FileNotFoundError:
                if strict:
                    raise
                continue

            result[key].append(path)
    return result[INHERIT], result[INCLUDE]


def merge_configs(configs: t.List[ConfigParser]) -> ConfigParser:
    def merge(c1, c2):
        c1.read_dict(c2)
        return c1

    return functools.reduce(merge, configs[1:], configs[0])


def save_config(config, path):
    """
    Write the configuration file to ``path``.
    """
    os.umask(0o77)
    f = open(path, "w")
    try:
        return config.write(f)
    finally:
        f.close()


def find_config():
    """Find the closest config file in the cwd or a parent directory"""
    d = os.getcwd()
    while d != os.path.dirname(d):
        path = os.path.join(d, CONFIG_FILENAME)
        if os.path.isfile(path):
            return path
        d = os.path.dirname(d)
    return None


def config_changed(config: ConfigParser, path: str) -> bool:
    def to_dict(config: ConfigParser):
        return {k: dict(section.items()) for k, section in config.items()}

    if Path(path).exists():
        return to_dict(read_config(path)) != to_dict(config)
    return True
