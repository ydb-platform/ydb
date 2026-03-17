"""
Utility functions
"""

import os
import platform
import sys

from .cmdstan import (
    EXTENSION,
    cmdstan_path,
    cmdstan_version,
    cmdstan_version_before,
    cxx_toolchain_path,
    get_latest_cmdstan,
    install_cmdstan,
    set_cmdstan_path,
    set_make_env,
    validate_cmdstan_path,
    validate_dir,
    wrap_url_progress_hook,
)
from .command import do_command
from .data_munging import build_xarray_data, flatten_chains
from .filesystem import (
    SanitizedOrTmpFilePath,
    create_named_text_file,
    pushd,
    windows_short_path,
)
from .json import write_stan_json
from .logging import disable_logging, enable_logging, get_logger
from .stancsv import check_sampler_csv, parse_rdump_value, read_metric, rload


def show_versions(output: bool = True) -> str:
    """Prints out system and dependency information for debugging"""

    import importlib
    import locale
    import struct

    deps_info = []
    try:
        (sysname, _, release, _, machine, processor) = platform.uname()
        deps_info.extend(
            [
                ("python", sys.version),
                ("python-bits", struct.calcsize("P") * 8),
                ("OS", f"{sysname}"),
                ("OS-release", f"{release}"),
                ("machine", f"{machine}"),
                ("processor", f"{processor}"),
                ("byteorder", f"{sys.byteorder}"),
                ("LC_ALL", f'{os.environ.get("LC_ALL", "None")}'),
                ("LANG", f'{os.environ.get("LANG", "None")}'),
                ("LOCALE", f"{locale.getlocale()}"),
            ]
        )
    # pylint: disable=broad-except
    except Exception:
        pass

    try:
        deps_info.append(('cmdstan_folder', cmdstan_path()))
        deps_info.append(('cmdstan', str(cmdstan_version())))
    # pylint: disable=broad-except
    except Exception:
        deps_info.append(('cmdstan', 'NOT FOUND'))

    deps = ['cmdstanpy', 'pandas', 'xarray', 'tqdm', 'numpy']
    for module in deps:
        try:
            if module in sys.modules:
                mod = sys.modules[module]
            else:
                mod = importlib.import_module(module)
        # pylint: disable=broad-except
        except Exception:
            deps_info.append((module, None))
        else:
            try:
                ver = mod.__version__  # type: ignore
                deps_info.append((module, ver))
            # pylint: disable=broad-except
            except Exception:
                deps_info.append((module, "installed"))

    out = 'INSTALLED VERSIONS\n---------------------\n'
    for k, info in deps_info:
        out += f'{k}: {info}\n'
    if output:
        print(out)
        return " "
    else:
        return out


__all__ = [
    'EXTENSION',
    'SanitizedOrTmpFilePath',
    'build_xarray_data',
    'check_sampler_csv',
    'cmdstan_path',
    'cmdstan_version',
    'cmdstan_version_before',
    'create_named_text_file',
    'cxx_toolchain_path',
    'do_command',
    'flatten_chains',
    'get_latest_cmdstan',
    'get_logger',
    'install_cmdstan',
    'parse_rdump_value',
    'pushd',
    'read_metric',
    'rload',
    'set_cmdstan_path',
    'set_make_env',
    'show_versions',
    'validate_cmdstan_path',
    'validate_dir',
    'windows_short_path',
    'wrap_url_progress_hook',
    'write_stan_json',
    'enable_logging',
    'disable_logging',
]
