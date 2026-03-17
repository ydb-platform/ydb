"""
Makefile options for stanc and C++ compilers
"""

import io
import json
import os
import platform
import shutil
import subprocess
from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional, Union

from cmdstanpy.utils import get_logger
from cmdstanpy.utils.cmdstan import (
    EXTENSION,
    cmdstan_path,
    cmdstan_version,
    cmdstan_version_before,
    stanc_path,
)
from cmdstanpy.utils.command import do_command
from cmdstanpy.utils.filesystem import SanitizedOrTmpFilePath

STANC_OPTS = [
    'O',
    'O0',
    'O1',
    'Oexperimental',
    'allow-undefined',
    'use-opencl',
    'warn-uninitialized',
    'include-paths',
    'name',
    'warn-pedantic',
]

# TODO(2.0): remove
STANC_DEPRECATED_OPTS = {
    'allow_undefined': 'allow-undefined',
    'include_paths': 'include-paths',
}

STANC_IGNORE_OPTS = [
    'debug-lex',
    'debug-parse',
    'debug-ast',
    'debug-decorated-ast',
    'debug-generate-data',
    'debug-mir',
    'debug-mir-pretty',
    'debug-optimized-mir',
    'debug-optimized-mir-pretty',
    'debug-transformed-mir',
    'debug-transformed-mir-pretty',
    'dump-stan-math-signatures',
    'auto-format',
    'print-canonical',
    'print-cpp',
    'o',
    'help',
    'version',
]

OptionalPath = Union[str, os.PathLike, None]


# TODO(2.0): can remove add function and other logic
class CompilerOptions:
    """
    User-specified flags for stanc and C++ compiler.

    Attributes:
        stanc_options - stanc compiler flags, options
        cpp_options - makefile options (NAME=value)
        user_header - path to a user .hpp file to include during compilation
    """

    def __init__(
        self,
        *,
        stanc_options: Optional[dict[str, Any]] = None,
        cpp_options: Optional[dict[str, Any]] = None,
        user_header: OptionalPath = None,
    ) -> None:
        """Initialize object."""
        self._stanc_options = stanc_options if stanc_options is not None else {}
        self._cpp_options = cpp_options if cpp_options is not None else {}
        self._user_header = str(user_header) if user_header is not None else ''

    def __repr__(self) -> str:
        return 'stanc_options={}, cpp_options={}'.format(
            self._stanc_options, self._cpp_options
        )

    def __eq__(self, other: Any) -> bool:
        """Overrides the default implementation"""
        if self.is_empty() and other is None:  # equiv w/r/t compiler
            return True
        if not isinstance(other, CompilerOptions):
            return False
        return (
            self._stanc_options == other.stanc_options
            and self._cpp_options == other.cpp_options
            and self._user_header == other.user_header
        )

    def is_empty(self) -> bool:
        """True if no options specified."""
        return (
            self._stanc_options == {}
            and self._cpp_options == {}
            and self._user_header == ''
        )

    @property
    def stanc_options(self) -> dict[str, Union[bool, int, str, Iterable[str]]]:
        """Stanc compiler options."""
        return self._stanc_options

    @property
    def cpp_options(self) -> dict[str, Union[bool, int]]:
        """C++ compiler options."""
        return self._cpp_options

    @property
    def user_header(self) -> str:
        """user header."""
        return self._user_header

    def validate(self) -> None:
        """
        Check compiler args.
        Raise ValueError if invalid options are found.
        """
        self.validate_stanc_opts()
        self.validate_cpp_opts()
        self.validate_user_header()

    def validate_stanc_opts(self) -> None:
        """
        Check stanc compiler args and consistency between stanc and C++ options.
        Raise ValueError if bad config is found.
        """
        # pylint: disable=no-member
        if self._stanc_options is None:
            return
        ignore = []
        paths = None
        has_o_flag = False

        for deprecated, replacement in STANC_DEPRECATED_OPTS.items():
            if deprecated in self._stanc_options:
                if replacement:
                    get_logger().warning(
                        'compiler option "%s" is deprecated, use "%s" instead',
                        deprecated,
                        replacement,
                    )
                    self._stanc_options[replacement] = copy(
                        self._stanc_options[deprecated]
                    )
                    del self._stanc_options[deprecated]
                else:
                    get_logger().warning(
                        'compiler option "%s" is deprecated and should '
                        'not be used',
                        deprecated,
                    )
        for key, val in self._stanc_options.items():
            if key in STANC_IGNORE_OPTS:
                get_logger().info('ignoring compiler option: %s', key)
                ignore.append(key)
            elif key not in STANC_OPTS:
                raise ValueError(f'unknown stanc compiler option: {key}')
            elif key == 'include-paths':
                paths = val
                if isinstance(val, str):
                    paths = val.split(',')
                elif not isinstance(val, list):
                    raise ValueError(
                        'Invalid include-paths, expecting list or '
                        f'string, found type: {type(val)}.'
                    )
            elif key == 'use-opencl':
                if self._cpp_options is None:
                    self._cpp_options = {'STAN_OPENCL': 'TRUE'}
                else:
                    self._cpp_options['STAN_OPENCL'] = 'TRUE'
            elif key.startswith('O'):
                if has_o_flag:
                    get_logger().warning(
                        'More than one of (O, O1, O2, Oexperimental)'
                        'optimizations passed. Only the last one will'
                        'be used'
                    )
                else:
                    has_o_flag = True

        for opt in ignore:
            del self._stanc_options[opt]
        if paths is not None:
            bad_paths = [dir for dir in paths if not os.path.exists(dir)]
            if any(bad_paths):
                raise ValueError(
                    'invalid include paths: {}'.format(', '.join(bad_paths))
                )

            self._stanc_options['include-paths'] = [
                os.path.abspath(os.path.expanduser(path)) for path in paths
            ]

    def validate_cpp_opts(self) -> None:
        """
        Check cpp compiler args.
        Raise ValueError if bad config is found.
        """
        if self._cpp_options is None:
            return
        for key in ['OPENCL_DEVICE_ID', 'OPENCL_PLATFORM_ID']:
            if key in self._cpp_options:
                self._cpp_options['STAN_OPENCL'] = 'TRUE'
                val = self._cpp_options[key]
                if not isinstance(val, int) or val < 0:
                    raise ValueError(
                        f'{key} must be a non-negative integer '
                        f'value, found {val}.'
                    )

    def validate_user_header(self) -> None:
        """
        User header exists.
        Raise ValueError if bad config is found.
        """
        if self._user_header != "":
            if not (
                os.path.exists(self._user_header)
                and os.path.isfile(self._user_header)
            ):
                raise ValueError(
                    f"User header file {self._user_header} cannot be found"
                )
            if self._user_header[-4:] != '.hpp':
                raise ValueError(
                    f"Header file must end in .hpp, got {self._user_header}"
                )
            if "allow-undefined" not in self._stanc_options:
                self._stanc_options["allow-undefined"] = True
            # set full path
            self._user_header = os.path.abspath(self._user_header)

            if ' ' in self._user_header:
                raise ValueError(
                    "User header must be in a location with no spaces in path!"
                )

            if (
                'USER_HEADER' in self._cpp_options
                and self._user_header != self._cpp_options['USER_HEADER']
            ):
                raise ValueError(
                    "Disagreement in user_header C++ options found!\n"
                    f"{self._user_header}, {self._cpp_options['USER_HEADER']}"
                )

            self._cpp_options['USER_HEADER'] = self._user_header

    def add(self, new_opts: "CompilerOptions") -> None:  # noqa: disable=Q000
        """Adds options to existing set of compiler options."""
        if new_opts.stanc_options is not None:
            if self._stanc_options is None:
                self._stanc_options = new_opts.stanc_options
            else:
                for key, val in new_opts.stanc_options.items():
                    if key == 'include-paths':
                        if isinstance(val, Iterable) and not isinstance(
                            val, str
                        ):
                            for path in val:
                                self.add_include_path(str(path))
                        else:
                            self.add_include_path(str(val))
                    else:
                        self._stanc_options[key] = val
        if new_opts.cpp_options is not None:
            for key, val in new_opts.cpp_options.items():
                self._cpp_options[key] = val
        if new_opts._user_header != '' and self._user_header == '':
            self._user_header = new_opts._user_header

    def add_include_path(self, path: str) -> None:
        """Adds include path to existing set of compiler options."""
        path = os.path.abspath(os.path.expanduser(path))
        if 'include-paths' not in self._stanc_options:
            self._stanc_options['include-paths'] = [path]
        elif path not in self._stanc_options['include-paths']:
            self._stanc_options['include-paths'].append(path)

    def compose_stanc(self, filename_in_msg: Optional[str]) -> list[str]:
        opts = []

        if filename_in_msg is not None:
            opts.append(f'--filename-in-msg={filename_in_msg}')

        if self._stanc_options is not None and len(self._stanc_options) > 0:
            for key, val in self._stanc_options.items():
                if key == 'include-paths':
                    opts.append(
                        '--include-paths='
                        + ','.join(
                            (
                                Path(p).as_posix()
                                for p in self._stanc_options['include-paths']
                            )
                        )
                    )
                elif key == 'name':
                    opts.append(f'--name={val}')
                else:
                    opts.append(f'--{key}')
        return opts

    def compose(self, filename_in_msg: Optional[str] = None) -> list[str]:
        """
        Format makefile options as list of strings.

        Parameters
        ----------
        filename_in_msg : str, optional
            filename to be displayed in stanc3 error messages
            (if different from actual filename on disk), by default None
        """
        opts = [
            'STANCFLAGS+=' + flag.replace(" ", "\\ ")
            for flag in self.compose_stanc(filename_in_msg)
        ]
        if self._cpp_options is not None and len(self._cpp_options) > 0:
            for key, val in self._cpp_options.items():
                opts.append(f'{key}={val}')
        return opts


def src_info(
    stan_file: str, compiler_options: CompilerOptions
) -> dict[str, Any]:
    """
    Get source info for Stan program file.

    This function is used in the implementation of
    :meth:`CmdStanModel.src_info`, and should not be called directly.
    """
    cmd = (
        [stanc_path()]
        # handle include-paths, allow-undefined etc
        + compiler_options.compose_stanc(None)
        + ['--info', str(stan_file)]
    )
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode:
        raise ValueError(
            f"Failed to get source info for Stan model "
            f"'{stan_file}'. Console:\n{proc.stderr}"
        )
    result: dict[str, Any] = json.loads(proc.stdout)
    return result


def compile_stan_file(
    src: Union[str, Path],
    force: bool = False,
    stanc_options: Optional[dict[str, Any]] = None,
    cpp_options: Optional[dict[str, Any]] = None,
    user_header: OptionalPath = None,
) -> str:
    """
    Compile the given Stan program file.  Translates the Stan code to
    C++, then calls the C++ compiler.

    By default, this function compares the timestamps on the source and
    executable files; if the executable is newer than the source file, it
    will not recompile the file, unless argument ``force`` is ``True``
    or unless the compiler options have been changed.

    :param src: Path to Stan program file.

    :param force: When ``True``, always compile, even if the executable file
        is newer than the source file.  Used for Stan models which have
        ``#include`` directives in order to force recompilation when changes
        are made to the included files.

    :param stanc_options: Options for stanc compiler.
    :param cpp_options: Options for C++ compiler.
    :param user_header: A path to a header file to include during C++
        compilation.
    """

    src = Path(src).resolve()
    if not src.exists():
        raise ValueError(f'stan file does not exist: {src}')

    compiler_options = CompilerOptions(
        stanc_options=stanc_options,
        cpp_options=cpp_options,
        user_header=user_header,
    )
    compiler_options.validate()

    exe_target = src.with_suffix(EXTENSION)
    if exe_target.exists():
        exe_time = os.path.getmtime(exe_target)
        included_files = [src]
        included_files.extend(
            src_info(str(src), compiler_options).get('included_files', [])
        )
        out_of_date = any(
            os.path.getmtime(included_file) > exe_time
            for included_file in included_files
        )
        if not out_of_date and not force:
            get_logger().debug('found newer exe file, not recompiling')
            return str(exe_target)

    compilation_failed = False
    # if target path has spaces or special characters, use a copy in a
    # temporary directory (GNU-Make constraint)
    with SanitizedOrTmpFilePath(str(src)) as (stan_file, is_copied):
        exe_file = os.path.splitext(stan_file)[0] + EXTENSION

        hpp_file = os.path.splitext(exe_file)[0] + '.hpp'
        if os.path.exists(hpp_file):
            os.remove(hpp_file)
        if os.path.exists(exe_file):
            get_logger().debug('Removing %s', exe_file)
            os.remove(exe_file)

        get_logger().info(
            'compiling stan file %s to exe file %s',
            stan_file,
            exe_target,
        )

        make = os.getenv(
            'MAKE',
            'make' if platform.system() != 'Windows' else 'mingw32-make',
        )
        cmd = [make]
        cmd.extend(compiler_options.compose(filename_in_msg=src.name))
        cmd.append(Path(exe_file).as_posix())

        sout = io.StringIO()
        try:
            do_command(cmd=cmd, cwd=cmdstan_path(), fd_out=sout)
        except RuntimeError as e:
            sout.write(f'\n{str(e)}\n')
            compilation_failed = True
        finally:
            console = sout.getvalue()

        get_logger().debug('Console output:\n%s', console)
        if not compilation_failed:
            if is_copied:
                shutil.copy(exe_file, exe_target)
            get_logger().info('compiled model executable: %s', exe_target)
        if 'Warning' in console:
            lines = console.split('\n')
            warnings = [x for x in lines if x.startswith('Warning')]
            get_logger().warning(
                'Stan compiler has produced %d warnings:',
                len(warnings),
            )
            get_logger().warning(console)
        if compilation_failed:
            if 'PCH' in console or 'precompiled header' in console:
                get_logger().warning(
                    "CmdStan's precompiled header (PCH) files "
                    "may need to be rebuilt."
                    "Please run cmdstanpy.rebuild_cmdstan().\n"
                    "If the issue persists please open a bug report"
                )
            raise ValueError(
                f"Failed to compile Stan model '{src}'. Console:\n{console}"
            )
        return str(exe_target)


def format_stan_file(
    stan_file: Union[str, os.PathLike],
    *,
    overwrite_file: bool = False,
    canonicalize: Union[bool, str, Iterable[str]] = False,
    max_line_length: int = 78,
    backup: bool = True,
    stanc_options: Optional[dict[str, Any]] = None,
) -> None:
    """
    Run stanc's auto-formatter on the model code. Either saves directly
    back to the file or prints for inspection

    :param stan_file: Path to Stan program file.
    :param overwrite_file: If True, save the updated code to disk, rather
        than printing it. By default False
    :param canonicalize: Whether or not the compiler should 'canonicalize'
        the Stan model, removing things like deprecated syntax. Default is
        False. If True, all canonicalizations are run. If it is a list of
        strings, those options are passed to stanc (new in Stan 2.29)
    :param max_line_length: Set the wrapping point for the formatter. The
        default value is 78, which wraps most lines by the 80th character.
    :param backup: If True, create a stanfile.bak backup before
        writing to the file. Only disable this if you're sure you have other
        copies of the file or are using a version control system like Git.
    :param stanc_options: Additional options to pass to the stanc compiler.
    """
    stan_file = Path(stan_file).resolve()

    if not stan_file.exists():
        raise ValueError(f'File does not exist: {stan_file}')

    try:
        cmd = (
            [stanc_path()]
            # handle include-paths, allow-undefined etc
            + CompilerOptions(stanc_options=stanc_options).compose_stanc(None)
            + [str(stan_file)]
        )

        if canonicalize:
            if cmdstan_version_before(2, 29):
                if isinstance(canonicalize, bool):
                    cmd.append('--print-canonical')
                else:
                    raise ValueError(
                        "Invalid arguments passed for current CmdStan"
                        + " version({})\n".format(
                            cmdstan_version() or "Unknown"
                        )
                        + "--canonicalize requires 2.29 or higher"
                    )
            else:
                if isinstance(canonicalize, str):
                    cmd.append('--canonicalize=' + canonicalize)
                elif isinstance(canonicalize, Iterable):
                    cmd.append('--canonicalize=' + ','.join(canonicalize))
                else:
                    cmd.append('--print-canonical')

        # before 2.29, having both --print-canonical
        # and --auto-format printed twice
        if not (cmdstan_version_before(2, 29) and canonicalize):
            cmd.append('--auto-format')

        if not cmdstan_version_before(2, 29):
            cmd.append(f'--max-line-length={max_line_length}')
        elif max_line_length != 78:
            raise ValueError(
                "Invalid arguments passed for current CmdStan version"
                + " ({})\n".format(cmdstan_version() or "Unknown")
                + "--max-line-length requires 2.29 or higher"
            )

        out = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if out.stderr:
            get_logger().warning(out.stderr)
        result = out.stdout
        if overwrite_file:
            if result:
                if backup:
                    shutil.copyfile(
                        stan_file,
                        str(stan_file)
                        + '.bak-'
                        + datetime.now().strftime("%Y%m%d%H%M%S"),
                    )
                stan_file.write_text(result)
        else:
            print(result)

    except (ValueError, RuntimeError) as e:
        raise RuntimeError("Stanc formatting failed") from e
