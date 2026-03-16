"""
Utilities for interacting with the filesystem on multiple platforms
"""

import contextlib
import os
import platform
import re
import shutil
import tempfile
from typing import Any, Iterator, Mapping, Optional, Union

import numpy as np

from cmdstanpy import _TMPDIR

from .json import write_stan_json
from .logging import get_logger

EXTENSION = '.exe' if platform.system() == 'Windows' else ''


def windows_short_path(path: str) -> str:
    """
    Gets the short path name of a given long path.
    http://stackoverflow.com/a/23598461/200291

    On non-Windows platforms, returns the path

    If (base)path does not exist, function raises RuntimeError
    """
    if platform.system() != 'Windows':
        return path

    if os.path.isfile(path) or (
        not os.path.isdir(path) and os.path.splitext(path)[1] != ''
    ):
        base_path, file_name = os.path.split(path)
    else:
        base_path, file_name = path, ''

    if not os.path.exists(base_path):
        raise RuntimeError(
            'Windows short path function needs a valid directory. '
            'Base directory does not exist: "{}"'.format(base_path)
        )

    import ctypes
    from ctypes import wintypes

    # pylint: disable=invalid-name
    _GetShortPathNameW = (
        ctypes.windll.kernel32.GetShortPathNameW  # type: ignore
    )

    _GetShortPathNameW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.LPWSTR,
        wintypes.DWORD,
    ]
    _GetShortPathNameW.restype = wintypes.DWORD

    output_buf_size = 0
    while True:
        output_buf = ctypes.create_unicode_buffer(output_buf_size)
        needed = _GetShortPathNameW(base_path, output_buf, output_buf_size)
        if output_buf_size >= needed:
            short_base_path = output_buf.value
            break
        else:
            output_buf_size = needed

    short_path = (
        os.path.join(short_base_path, file_name)
        if file_name
        else short_base_path
    )
    return short_path


def create_named_text_file(
    dir: str, prefix: str, suffix: str, name_only: bool = False
) -> str:
    """
    Create a named unique file, return filename.
    Flag 'name_only' will create then delete the tmp file;
    this lets us create filename args for commands which
    disallow overwriting existing files (e.g., 'stansummary').
    """
    fd = tempfile.NamedTemporaryFile(
        mode='w+', prefix=prefix, suffix=suffix, dir=dir, delete=name_only
    )
    path = fd.name
    fd.close()
    return path


@contextlib.contextmanager
def pushd(new_dir: str) -> Iterator[None]:
    """Acts like pushd/popd."""
    previous_dir = os.getcwd()
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(previous_dir)


def _temp_single_json(
    data: Union[str, os.PathLike, Mapping[str, Any], None],
) -> Iterator[Optional[str]]:
    """Context manager for json files."""
    if data is None:
        yield None
        return
    if isinstance(data, (str, os.PathLike)):
        yield str(data)
        return

    data_file = create_named_text_file(dir=_TMPDIR, prefix='', suffix='.json')
    get_logger().debug('input tempfile: %s', data_file)
    write_stan_json(data_file, data)
    try:
        yield data_file
    finally:
        with contextlib.suppress(PermissionError):
            os.remove(data_file)


temp_single_json = contextlib.contextmanager(_temp_single_json)


def _temp_multiinput(
    input: Union[str, os.PathLike, Mapping[str, Any], list[Any], None],
    base: int = 1,
) -> Iterator[Optional[str]]:
    if isinstance(input, list):
        # most complicated case: list of inits
        # for multiple chains, we need to create multiple files
        # which look like somename_{i}.json and then pass somename.json
        # to CmdStan

        mother_file = create_named_text_file(
            dir=_TMPDIR, prefix='', suffix='.json', name_only=True
        )
        new_files = [
            os.path.splitext(mother_file)[0] + f'_{i + base}.json'
            for i in range(len(input))
        ]
        for init, file in zip(input, new_files):
            if isinstance(init, dict):
                write_stan_json(file, init)
            elif isinstance(init, str):
                shutil.copy(init, file)
            else:
                raise ValueError(
                    'A list of inits must contain dicts or strings, not'
                    + str(type(init))
                )
        try:
            yield mother_file
        finally:
            for file in new_files:
                with contextlib.suppress(PermissionError):
                    os.remove(file)
    else:
        yield from _temp_single_json(input)


@contextlib.contextmanager
def temp_metrics(
    metrics: Union[
        str, os.PathLike, Mapping[str, Any], np.ndarray, list[Any], None
    ],
    *,
    id: int = 1,
) -> Iterator[Union[str, None]]:
    if isinstance(metrics, dict):
        if 'inv_metric' not in metrics:
            raise ValueError('Entry "inv_metric" not found in metric dict.')
    if isinstance(metrics, np.ndarray):
        metrics = {"inv_metric": metrics}

    if isinstance(metrics, list):
        metrics_processed = []
        for init in metrics:
            if isinstance(init, np.ndarray):
                metrics_processed.append({"inv_metric": init})
            else:
                metrics_processed.append(init)
                if isinstance(metrics_processed, dict):
                    if 'inv_metric' not in metrics_processed:
                        raise ValueError(
                            'Entry "inv_metric" not found in metric dict.'
                        )
        metrics = metrics_processed
    yield from _temp_multiinput(metrics, base=id)


@contextlib.contextmanager
def temp_inits(
    inits: Union[
        str, os.PathLike, Mapping[str, Any], float, int, list[Any], None
    ],
    *,
    allow_multiple: bool = True,
    id: int = 1,
) -> Iterator[Union[str, float, int, None]]:
    if isinstance(inits, (float, int)):
        yield inits
        return
    if allow_multiple:
        yield from _temp_multiinput(inits, base=id)
    else:
        if isinstance(inits, list):
            raise ValueError('Expected single initialization, got list')
        yield from _temp_single_json(inits)


class SanitizedOrTmpFilePath:
    """
    Context manager for tmpfiles, handles special characters in filepath.
    """

    UNIXISH_PATTERN = re.compile(r"[\s~]")
    WINDOWS_PATTERN = re.compile(r"\s")

    @classmethod
    def _has_special_chars(cls, file_path: str) -> bool:
        if platform.system() == "Windows":
            return bool(cls.WINDOWS_PATTERN.search(file_path))
        return bool(cls.UNIXISH_PATTERN.search(file_path))

    def __init__(self, file_path: str):
        self._tmpdir = None

        if self._has_special_chars(os.path.abspath(file_path)):
            base_path, file_name = os.path.split(os.path.abspath(file_path))
            os.makedirs(base_path, exist_ok=True)
            try:
                short_base_path = windows_short_path(base_path)
                if os.path.exists(short_base_path):
                    file_path = os.path.join(short_base_path, file_name)
            except RuntimeError:
                pass

        if self._has_special_chars(os.path.abspath(file_path)):
            tmpdir = tempfile.mkdtemp()
            if self._has_special_chars(tmpdir):
                raise RuntimeError(
                    'Unable to generate temporary path without spaces or '
                    'special characters! \n Please move your stan file to a '
                    'location without spaces or special characters.'
                )

            _, path = tempfile.mkstemp(suffix='.stan', dir=tmpdir)

            shutil.copy(file_path, path)
            self._path = path
            self._tmpdir = tmpdir
        else:
            self._path = file_path

    def __enter__(self) -> tuple[str, bool]:
        return self._path, self._tmpdir is not None

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore
        if self._tmpdir:
            shutil.rmtree(self._tmpdir, ignore_errors=True)
