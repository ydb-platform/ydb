#!/usr/bin/env python

import json
import os


def from_envvars(conf, prefix=None, envvars=None, as_json=True):
    """Load environment variables as Flask configuration settings.

    Values are parsed as JSON. If parsing fails with a ValueError,
    values are instead used as verbatim strings.

    :param app: App, whose configuration should be loaded from ENVVARs.
    :param prefix: If ``None`` is passed as envvars, all variables from
                   ``environ`` starting with this prefix are imported. The
                   prefix is stripped upon import.
    :param envvars: A dictionary of mappings of environment-variable-names
                    to Flask configuration names. If a list is passed
                    instead, names are mapped 1:1. If ``None``, see prefix
                    argument.
    :param as_json: If False, values will not be parsed as JSON first.
    """
    if prefix is None and envvars is None:
        raise RuntimeError('Must either give prefix or envvars argument')

    # if it's a list, convert to dict
    if isinstance(envvars, list):
        envvars = {k: None for k in envvars}

    if not envvars:
        envvars = {k: k[len(prefix):] for k in os.environ.keys()
                   if k.startswith(prefix)}

    for env_name, name in envvars.items():
        if name is None:
            name = env_name

        if not env_name in os.environ:
            continue

        if as_json:
            try:
                conf[name] = json.loads(os.environ[env_name])
            except ValueError:
                conf[name] = os.environ[env_name]
        else:
            conf[name] = os.environ[env_name]
