"""
Utilities for finding and installing CmdStan
"""

import os
import platform
import subprocess
import sys
from collections import OrderedDict
from typing import Callable, Optional, Union

from tqdm.auto import tqdm

from cmdstanpy import _DOT_CMDSTAN

from .. import progress as progbar
from .logging import get_logger

EXTENSION = '.exe' if platform.system() == 'Windows' else ''


def determine_linux_arch() -> str:
    machine = platform.machine()
    arch = ""
    if machine == "aarch64":
        arch = "arm64"
    elif machine == "armv7l":
        # Telling armel and armhf apart is nontrivial
        # c.f. https://forums.raspberrypi.com/viewtopic.php?t=20873
        readelf = subprocess.run(
            ["readelf", "-A", "/proc/self/exe"],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        )
        if "Tag_ABI_VFP_args" in readelf.stdout:
            arch = "armel"
        else:
            arch = "armhf"
    elif machine == "mips64":
        arch = "mips64el"
    elif machine == "ppc64el" or machine == "ppc64le":
        arch = "ppc64el"
    elif machine == "s390x":
        arch = "s390x"
    return arch


def get_download_url(version: str) -> str:
    arch = os.environ.get("CMDSTAN_ARCH", "")
    if not arch and platform.system() == "Linux":
        arch = determine_linux_arch()

    if arch and arch.lower() != "false":
        url_end = f'v{version}/cmdstan-{version}-linux-{arch}.tar.gz'
    else:
        url_end = f'v{version}/cmdstan-{version}.tar.gz'

    return f'https://github.com/stan-dev/cmdstan/releases/download/{url_end}'


def validate_dir(install_dir: str) -> None:
    """Check that specified install directory exists, can write."""
    if not os.path.exists(install_dir):
        try:
            os.makedirs(install_dir)
        except (IOError, OSError, PermissionError) as e:
            raise ValueError(
                'Cannot create directory: {}'.format(install_dir)
            ) from e
    else:
        if not os.path.isdir(install_dir):
            raise ValueError(
                'File exists, should be a directory: {}'.format(install_dir)
            )
        try:
            with open('tmp_test_w', 'w'):
                pass
            os.remove('tmp_test_w')  # cleanup
        except OSError as e:
            raise ValueError(
                'Cannot write files to directory {}'.format(install_dir)
            ) from e


def get_latest_cmdstan(cmdstan_dir: str) -> Optional[str]:
    """
    Given a valid directory path, find all installed CmdStan versions
    and return highest (i.e., latest) version number.

    Assumes directory consists of CmdStan releases, created by
    function `install_cmdstan`, and therefore dirnames have format
    "cmdstan-<maj>.<min>.<patch>" or "cmdstan-<maj>.<min>.<patch>-rc<num>",
    which is CmdStan release practice as of v 2.24.
    """
    versions = [
        name[8:]
        for name in os.listdir(cmdstan_dir)
        if os.path.isdir(os.path.join(cmdstan_dir, name))
        and name.startswith('cmdstan-')
    ]
    if len(versions) == 0:
        return None
    if len(versions) == 1:
        return 'cmdstan-' + versions[0]
    # we can only compare numeric versions
    versions = [v for v in versions if v[0].isdigit() and v.count('.') == 2]
    # munge rc for sort, e.g. 2.25.0-rc1 -> 2.25.-99
    for i in range(len(versions)):  # # pylint: disable=C0200
        if '-rc' in versions[i]:
            comps = versions[i].split('-rc')
            mmp = comps[0].split('.')
            rc_num = comps[1]
            patch = str(int(rc_num) - 100)
            versions[i] = '.'.join([mmp[0], mmp[1], patch])

    versions.sort(key=lambda s: list(map(int, s.split('.'))))
    latest = versions[len(versions) - 1]

    # unmunge as needed
    mmp = latest.split('.')
    if int(mmp[2]) < 0:
        rc_num = str(int(mmp[2]) + 100)
        mmp[2] = "0-rc" + rc_num
        latest = '.'.join(mmp)

    return 'cmdstan-' + latest


def validate_cmdstan_path(path: str) -> None:
    """
    Validate that CmdStan directory exists and binaries have been built.
    Throws exception if specified path is invalid.
    """
    if not os.path.isdir(path):
        raise ValueError(f'No CmdStan directory, path {path} does not exist.')
    if not os.path.exists(os.path.join(path, 'makefile')):
        raise ValueError(
            f'CmdStan installataion missing makefile, path {path} is invalid.'
            ' You may wish to re-install cmdstan by running command '
            '"install_cmdstan --overwrite", or Python code '
            '"import cmdstanpy; cmdstanpy.install_cmdstan(overwrite=True)"'
        )


def stanc_path() -> str:
    """
    Returns the path to the stanc executable in the CmdStan installation.
    """
    cmdstan = cmdstan_path()
    stanc_exe = os.path.join(cmdstan, 'bin', 'stanc' + EXTENSION)
    if not os.path.exists(stanc_exe):
        raise ValueError(
            f'stanc executable not found in CmdStan installation: {cmdstan}.\n'
            'You may need to re-install or re-build CmdStan.',
        )
    return stanc_exe


def set_cmdstan_path(path: str) -> None:
    """
    Validate, then set CmdStan directory path.
    """
    validate_cmdstan_path(path)
    os.environ['CMDSTAN'] = path


def set_make_env(make: str) -> None:
    """
    set MAKE environmental variable.
    """
    os.environ['MAKE'] = make


def cmdstan_path() -> str:
    """
    Validate, then return CmdStan directory path.
    """
    cmdstan = ''
    if 'CMDSTAN' in os.environ and len(os.environ['CMDSTAN']) > 0:
        cmdstan = os.environ['CMDSTAN']
    else:
        cmdstan_dir = os.path.expanduser(os.path.join('~', _DOT_CMDSTAN))
        if not os.path.exists(cmdstan_dir):
            raise ValueError(
                'No CmdStan installation found, run command "install_cmdstan"'
                'or (re)activate your conda environment!'
            )
        latest_cmdstan = get_latest_cmdstan(cmdstan_dir)
        if latest_cmdstan is None:
            raise ValueError(
                'No CmdStan installation found, run command "install_cmdstan"'
                'or (re)activate your conda environment!'
            )
        cmdstan = os.path.join(cmdstan_dir, latest_cmdstan)
        os.environ['CMDSTAN'] = cmdstan
    validate_cmdstan_path(cmdstan)
    return os.path.normpath(cmdstan)


def cmdstan_version() -> Optional[tuple[int, ...]]:
    """
    Parses version string out of CmdStan makefile variable CMDSTAN_VERSION,
    returns Tuple(Major, minor).

    If CmdStan installation is not found or cannot parse version from makefile
    logs warning and returns None.  Lenient behavoir required for CI tests,
    per comment:
    https://github.com/stan-dev/cmdstanpy/pull/321#issuecomment-733817554
    """
    try:
        makefile = os.path.join(cmdstan_path(), 'makefile')
    except ValueError as e:
        get_logger().info('No CmdStan installation found.')
        get_logger().debug("%s", e)
        return None

    with open(makefile, 'r') as fd:
        contents = fd.read()

    start_idx = contents.find('CMDSTAN_VERSION := ')
    if start_idx < 0:
        get_logger().info(
            'Cannot parse version from makefile: %s.',
            makefile,
        )
        return None

    start_idx += len('CMDSTAN_VERSION := ')
    end_idx = contents.find('\n', start_idx)

    version = contents[start_idx:end_idx]
    splits = version.split('.')
    if len(splits) != 3:
        get_logger().info(
            'Cannot parse version, expected "<major>.<minor>.<patch>", '
            'found: "%s".',
            version,
        )
        return None
    return tuple(int(x) for x in splits[0:2])


def cmdstan_version_before(
    major: int, minor: int, info: Optional[dict[str, str]] = None
) -> bool:
    """
    Check that CmdStan version is less than Major.minor version.

    :param major: Major version number
    :param minor: Minor version number

    :return: True if version at or above major.minor, else False.
    """
    cur_version = None
    if info is None or 'stan_version_major' not in info:
        cur_version = cmdstan_version()
    else:
        cur_version = (
            int(info['stan_version_major']),
            int(info['stan_version_minor']),
        )
    if cur_version is None:
        get_logger().info(
            'Cannot determine whether version is before %d.%d.', major, minor
        )
        return False
    if cur_version[0] < major or (
        cur_version[0] == major and cur_version[1] < minor
    ):
        return True
    return False


def cxx_toolchain_path(
    version: Optional[str] = None, install_dir: Optional[str] = None
) -> tuple[str, ...]:
    """
    Validate, then activate C++ toolchain directory path.
    """
    if platform.system() != 'Windows':
        raise RuntimeError(
            'Functionality is currently only supported on Windows'
        )
    if version is not None and not isinstance(version, str):
        raise TypeError('Format version number as a string')
    logger = get_logger()
    if 'CMDSTAN_TOOLCHAIN' in os.environ:
        toolchain_root = os.environ['CMDSTAN_TOOLCHAIN']
        if os.path.exists(os.path.join(toolchain_root, 'mingw64')):
            compiler_path = os.path.join(
                toolchain_root,
                'mingw64' if (sys.maxsize > 2**32) else 'mingw32',
                'bin',
            )
            if os.path.exists(compiler_path):
                tool_path = os.path.join(toolchain_root, 'usr', 'bin')
                if not os.path.exists(tool_path):
                    tool_path = ''
                    compiler_path = ''
                    logger.warning(
                        'Found invalid installion for RTools40 on %s',
                        toolchain_root,
                    )
                    toolchain_root = ''
            else:
                compiler_path = ''
                logger.warning(
                    'Found invalid installion for RTools40 on %s',
                    toolchain_root,
                )
                toolchain_root = ''

        elif os.path.exists(os.path.join(toolchain_root, 'mingw_64')):
            compiler_path = os.path.join(
                toolchain_root,
                'mingw_64' if (sys.maxsize > 2**32) else 'mingw_32',
                'bin',
            )
            if os.path.exists(compiler_path):
                tool_path = os.path.join(toolchain_root, 'bin')
                if not os.path.exists(tool_path):
                    tool_path = ''
                    compiler_path = ''
                    logger.warning(
                        'Found invalid installion for RTools35 on %s',
                        toolchain_root,
                    )
                    toolchain_root = ''
            else:
                compiler_path = ''
                logger.warning(
                    'Found invalid installion for RTools35 on %s',
                    toolchain_root,
                )
                toolchain_root = ''
    else:
        rtools40_home = os.environ.get('RTOOLS40_HOME')
        cmdstan_dir = os.path.expanduser(os.path.join('~', _DOT_CMDSTAN))
        for toolchain_root in (
            ([rtools40_home] if rtools40_home is not None else [])
            + (
                [
                    os.path.join(install_dir, 'RTools40'),
                    os.path.join(install_dir, 'RTools35'),
                    os.path.join(install_dir, 'RTools30'),
                    os.path.join(install_dir, 'RTools'),
                ]
                if install_dir is not None
                else []
            )
            + [
                os.path.join(cmdstan_dir, 'RTools40'),
                os.path.join(os.path.abspath("/"), "RTools40"),
                os.path.join(cmdstan_dir, 'RTools35'),
                os.path.join(os.path.abspath("/"), "RTools35"),
                os.path.join(cmdstan_dir, 'RTools'),
                os.path.join(os.path.abspath("/"), "RTools"),
                os.path.join(os.path.abspath("/"), "RBuildTools"),
            ]
        ):
            compiler_path = ''
            tool_path = ''

            if os.path.exists(toolchain_root):
                if version not in ('35', '3.5', '3'):
                    compiler_path = os.path.join(
                        toolchain_root,
                        'mingw64' if (sys.maxsize > 2**32) else 'mingw32',
                        'bin',
                    )
                    if os.path.exists(compiler_path):
                        tool_path = os.path.join(toolchain_root, 'usr', 'bin')
                        if not os.path.exists(tool_path):
                            tool_path = ''
                            compiler_path = ''
                            logger.warning(
                                'Found invalid installation for RTools40 on %s',
                                toolchain_root,
                            )
                            toolchain_root = ''
                        else:
                            break
                    else:
                        compiler_path = ''
                        logger.warning(
                            'Found invalid installation for RTools40 on %s',
                            toolchain_root,
                        )
                        toolchain_root = ''
                else:
                    compiler_path = os.path.join(
                        toolchain_root,
                        'mingw_64' if (sys.maxsize > 2**32) else 'mingw_32',
                        'bin',
                    )
                    if os.path.exists(compiler_path):
                        tool_path = os.path.join(toolchain_root, 'bin')
                        if not os.path.exists(tool_path):
                            tool_path = ''
                            compiler_path = ''
                            logger.warning(
                                'Found invalid installation for RTools35 on %s',
                                toolchain_root,
                            )
                            toolchain_root = ''
                        else:
                            break
                    else:
                        compiler_path = ''
                        logger.warning(
                            'Found invalid installation for RTools35 on %s',
                            toolchain_root,
                        )
                        toolchain_root = ''
            else:
                toolchain_root = ''

    if not toolchain_root:
        raise ValueError(
            'no RTools toolchain installation found, '
            'run command line script '
            '"python -m cmdstanpy.install_cxx_toolchain"'
        )
    logger.info('Add C++ toolchain to $PATH: %s', toolchain_root)
    os.environ['PATH'] = ';'.join(
        list(
            OrderedDict.fromkeys(
                [compiler_path, tool_path] + os.getenv('PATH', '').split(';')
            )
        )
    )
    return compiler_path, tool_path


def install_cmdstan(
    version: Optional[str] = None,
    dir: Optional[str] = None,
    overwrite: bool = False,
    compiler: bool = False,
    progress: bool = False,
    verbose: bool = False,
    cores: int = 1,
    *,
    interactive: bool = False,
) -> bool:
    """
    Download and install a CmdStan release from GitHub. Downloads the release
    tar.gz file to temporary storage.  Retries GitHub requests in order
    to allow for transient network outages. Builds CmdStan executables
    and tests the compiler by building example model ``bernoulli.stan``.

    :param version: CmdStan version string, e.g. "2.29.2".
        Defaults to latest CmdStan release.
        If ``git`` is installed, a git tag or branch of stan-dev/cmdstan
        can be specified, e.g. "git:develop".

    :param dir: Path to install directory.  Defaults to hidden directory
        ``$HOME/.cmdstan``.
        If no directory is specified and the above directory does not
        exist, directory ``$HOME/.cmdstan`` will be created and populated.

    :param overwrite:  Boolean value; when ``True``, will overwrite and
        rebuild an existing CmdStan installation.  Default is ``False``.

    :param compiler: Boolean value; when ``True`` on WINDOWS ONLY, use the
        C++ compiler from the ``install_cxx_toolchain`` command or install
        one if none is found.

    :param progress: Boolean value; when ``True``, show a progress bar for
        downloading and unpacking CmdStan.  Default is ``False``.

    :param verbose: Boolean value; when ``True``, show console output from all
        intallation steps, i.e., download, build, and test CmdStan release.
        Default is ``False``.
    :param cores: Integer, number of cores to use in the ``make`` command.
        Default is 1 core.

    :param interactive: Boolean value; if true, ignore all other arguments
        to this function and run in an interactive mode, prompting the user
        to provide the other information manually through the standard input.

        This flag should only be used in interactive environments,
        e.g. on the command line.

    :return: Boolean value; ``True`` for success.
    """
    logger = get_logger()
    try:
        from ..install_cmdstan import (
            InstallationSettings,
            InteractiveSettings,
            run_install,
        )

        args: Union[InstallationSettings, InteractiveSettings]

        if interactive:
            if any(
                [
                    version,
                    dir,
                    overwrite,
                    compiler,
                    progress,
                    verbose,
                    cores != 1,
                ]
            ):
                logger.warning(
                    "Interactive installation requested but other arguments"
                    " were used.\n\tThese values will be ignored!"
                )
            args = InteractiveSettings()
        else:
            args = InstallationSettings(
                version=version,
                overwrite=overwrite,
                verbose=verbose,
                compiler=compiler,
                progress=progress,
                dir=dir,
                cores=cores,
            )
        run_install(args)
    # pylint: disable=broad-except
    except Exception as e:
        logger.warning('CmdStan installation failed.\n%s', str(e))
        return False

    if 'git:' in args.version:
        folder = f"cmdstan-{args.version.replace(':', '-').replace('/', '_')}"
    else:
        folder = f"cmdstan-{args.version}"
    set_cmdstan_path(os.path.join(args.dir, folder))

    return True


@progbar.wrap_callback
def wrap_url_progress_hook() -> Optional[Callable[[int, int, int], None]]:
    """Sets up tqdm callback for url downloads."""
    pbar: tqdm = tqdm(
        unit='B',
        unit_scale=True,
        unit_divisor=1024,
        colour='blue',
        leave=False,
    )

    def download_progress_hook(
        count: int, block_size: int, total_size: int
    ) -> None:
        if pbar.total is None:
            pbar.total = total_size
            pbar.reset()
        downloaded_size = count * block_size
        pbar.update(downloaded_size - pbar.n)
        if pbar.n >= total_size:
            pbar.close()

    return download_progress_hook
