#!/usr/bin/env python
"""
Download and install a C++ toolchain.
Currently implemented platforms (platform.system)
    Windows: RTools 3.5, 4.0 (default)
    Darwin (macOS): Not implemented
    Linux: Not implemented
Optional command line arguments:
   -v, --version : version, defaults to latest
   -d, --dir : install directory, defaults to '~/.cmdstan
   -s (--silent) : install with /VERYSILENT instead of /SILENT for RTools
   -m --no-make : don't install mingw32-make (Windows RTools 4.0 only)
   --progress : flag, when specified show progress bar for RTools download
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
import urllib.request
from collections import OrderedDict
from time import sleep
from typing import Any

from cmdstanpy import _DOT_CMDSTAN
from cmdstanpy.utils import pushd, validate_dir, wrap_url_progress_hook

EXTENSION = '.exe' if platform.system() == 'Windows' else ''
IS_64BITS = sys.maxsize > 2**32


def usage() -> None:
    """Print usage."""
    print(
        """Arguments:
        -v (--version) :CmdStan version
        -d (--dir) : install directory
        -s (--silent) : install with /VERYSILENT instead of /SILENT for RTools
        -m (--no-make) : don't install mingw32-make (Windows RTools 4.0 only)
        --progress : flag, when specified show progress bar for RTools download
        -h (--help) : this message
        """
    )


def get_config(dir: str, silent: bool) -> list[str]:
    """Assemble config info."""
    config = []
    if platform.system() == 'Windows':
        _, dir = os.path.splitdrive(os.path.abspath(dir))
        if dir.startswith('\\'):
            dir = dir[1:]
        config = [
            '/SP-',
            '/VERYSILENT' if silent else '/SILENT',
            '/SUPPRESSMSGBOXES',
            '/CURRENTUSER',
            'LANG="English"',
            '/DIR="{}"'.format(dir),
            '/NOICONS',
            '/NORESTART',
        ]
    return config


def install_version(
    installation_dir: str,
    installation_file: str,
    version: str,
    silent: bool,
    verbose: bool = False,
) -> None:
    """Install specified toolchain version."""
    with pushd('.'):
        print(
            'Installing the C++ toolchain: {}'.format(
                os.path.splitext(installation_file)[0]
            )
        )
        cmd = [installation_file]
        cmd.extend(get_config(installation_dir, silent))
        print(' '.join(cmd))
        proc = subprocess.Popen(
            cmd,
            cwd=None,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=os.environ,
        )
        while proc.poll() is None:
            if proc.stdout:
                output = proc.stdout.readline().decode('utf-8').strip()
                if output and verbose:
                    print(output, flush=True)
        _, stderr = proc.communicate()
        if proc.returncode:
            print('Installation failed: returncode={}'.format(proc.returncode))
            if stderr:
                print(stderr.decode('utf-8').strip())
            if is_installed(installation_dir, version):
                print('Installation files found at the installation location.')
            sys.exit(3)
    # check installation
    if is_installed(installation_dir, version):
        os.remove(installation_file)
    print('Installed {}'.format(os.path.splitext(installation_file)[0]))


def install_mingw32_make(toolchain_loc: str, verbose: bool = False) -> None:
    """Install mingw32-make for Windows RTools 4.0."""
    os.environ['PATH'] = ';'.join(
        list(
            OrderedDict.fromkeys(
                [
                    os.path.join(
                        toolchain_loc,
                        'mingw_64' if IS_64BITS else 'mingw_32',
                        'bin',
                    ),
                    os.path.join(toolchain_loc, 'usr', 'bin'),
                ]
                + os.environ.get('PATH', '').split(';')
            )
        )
    )
    cmd = [
        'pacman',
        '-Sy',
        'mingw-w64-x86_64-make' if IS_64BITS else 'mingw-w64-i686-make',
        '--noconfirm',
    ]
    with pushd('.'):
        print(' '.join(cmd))
        proc = subprocess.Popen(
            cmd,
            cwd=None,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=os.environ,
        )
        while proc.poll() is None:
            if proc.stdout:
                output = proc.stdout.readline().decode('utf-8').strip()
                if output and verbose:
                    print(output, flush=True)
        _, stderr = proc.communicate()
        if proc.returncode:
            print(
                'mingw32-make installation failed: returncode={}'.format(
                    proc.returncode
                )
            )
            if stderr:
                print(stderr.decode('utf-8').strip())
            sys.exit(3)
    print('Installed mingw32-make.exe')


def is_installed(toolchain_loc: str, version: str) -> bool:
    """Returns True is toolchain is installed."""
    if platform.system() == 'Windows':
        if version in ['35', '3.5']:
            if not os.path.exists(os.path.join(toolchain_loc, 'bin')):
                return False
            return os.path.exists(
                os.path.join(
                    toolchain_loc,
                    'mingw_64' if IS_64BITS else 'mingw_32',
                    'bin',
                    'g++' + EXTENSION,
                )
            )
        elif version in ['40', '4.0', '4']:
            return os.path.exists(
                os.path.join(
                    toolchain_loc,
                    'mingw64' if IS_64BITS else 'mingw32',
                    'bin',
                    'g++' + EXTENSION,
                )
            )
        else:
            return False
    return False


def latest_version() -> str:
    """Windows version hardcoded to 4.0."""
    if platform.system() == 'Windows':
        return '4.0'
    return ''


def retrieve_toolchain(filename: str, url: str, progress: bool = True) -> None:
    """Download toolchain from URL."""
    print('Downloading C++ toolchain: {}'.format(filename))
    for i in range(6):
        try:
            if progress:
                progress_hook = wrap_url_progress_hook()
            else:
                progress_hook = None
            _ = urllib.request.urlretrieve(
                url, filename=filename, reporthook=progress_hook
            )
            break
        except urllib.error.URLError as err:
            print('Failed to download C++ toolchain')
            print(err)
            if i < 5:
                print('retry ({}/5)'.format(i + 1))
                sleep(1)
                continue
            sys.exit(3)
    print('Download successful, file: {}'.format(filename))


def normalize_version(version: str) -> str:
    """Return maj.min part of version string."""
    if platform.system() == 'Windows':
        if version in ['4', '40']:
            version = '4.0'
        elif version == '35':
            version = '3.5'
    return version


def get_toolchain_name() -> str:
    """Return toolchain name."""
    if platform.system() == 'Windows':
        return 'RTools'
    return ''


# TODO(2.0): drop 3.5 support
def get_url(version: str) -> str:
    """Return URL for toolchain."""
    url = ''
    if platform.system() == 'Windows':
        if version == '4.0':
            # pylint: disable=line-too-long
            if IS_64BITS:
                url = 'https://cran.r-project.org/bin/windows/Rtools/rtools40-x86_64.exe'  # noqa: disable=E501
            else:
                url = 'https://cran.r-project.org/bin/windows/Rtools/rtools40-i686.exe'  # noqa: disable=E501
        elif version == '3.5':
            url = 'https://cran.r-project.org/bin/windows/Rtools/Rtools35.exe'
    return url


def get_toolchain_version(name: str, version: str) -> str:
    """Toolchain version."""
    toolchain_folder = ''
    if platform.system() == 'Windows':
        toolchain_folder = '{}{}'.format(name, version.replace('.', ''))

    return toolchain_folder


def run_rtools_install(args: dict[str, Any]) -> None:
    """Main."""
    if platform.system() not in {'Windows'}:
        raise NotImplementedError(
            'Download for the C++ toolchain '
            'on the current platform has not '
            f'been implemented: {platform.system()}'
        )
    toolchain = get_toolchain_name()
    version = args['version']
    if version is None:
        version = latest_version()
    version = normalize_version(version)
    print("C++ toolchain '{}' version: {}".format(toolchain, version))

    url = get_url(version)

    if 'verbose' in args:
        verbose = args['verbose']
    else:
        verbose = False

    install_dir = args['dir']
    if install_dir is None:
        install_dir = os.path.expanduser(os.path.join('~', _DOT_CMDSTAN))
    validate_dir(install_dir)
    print('Install directory: {}'.format(install_dir))

    if 'progress' in args:
        progress = args['progress']
    else:
        progress = False

    if platform.system() == 'Windows':
        silent = 'silent' in args
        # force silent == False for 4.0 version
        if 'silent' not in args and version in ('4.0', '4', '40'):
            silent = False
    else:
        silent = False

    toolchain_folder = get_toolchain_version(toolchain, version)
    with pushd(install_dir):
        if is_installed(toolchain_folder, version):
            print('C++ toolchain {} already installed'.format(toolchain_folder))
        else:
            if os.path.exists(toolchain_folder):
                shutil.rmtree(toolchain_folder, ignore_errors=False)
            retrieve_toolchain(
                toolchain_folder + EXTENSION, url, progress=progress
            )
            install_version(
                toolchain_folder,
                toolchain_folder + EXTENSION,
                version,
                silent,
                verbose,
            )
        if (
            'no-make' not in args
            and (platform.system() == 'Windows')
            and (version in ('4.0', '4', '40'))
        ):
            if os.path.exists(
                os.path.join(
                    toolchain_folder, 'mingw64', 'bin', 'mingw32-make.exe'
                )
            ):
                print('mingw32-make.exe already installed')
            else:
                install_mingw32_make(toolchain_folder, verbose)


def parse_cmdline_args() -> dict[str, Any]:
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', '-v', help="version, defaults to latest")
    parser.add_argument(
        '--dir', '-d', help="install directory, defaults to '~/.cmdstan"
    )
    parser.add_argument(
        '--silent',
        '-s',
        action='store_true',
        help="install with /VERYSILENT instead of /SILENT for RTools",
    )
    parser.add_argument(
        '--no-make',
        '-m',
        action='store_false',
        help="don't install mingw32-make (Windows RTools 4.0 only)",
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help="flag, when specified prints output from RTools build process",
    )
    parser.add_argument(
        '--progress',
        action='store_true',
        help="flag, when specified show progress bar for CmdStan download",
    )
    return vars(parser.parse_args(sys.argv[1:]))


def __main__() -> None:
    run_rtools_install(parse_cmdline_args())


if __name__ == '__main__':
    __main__()
