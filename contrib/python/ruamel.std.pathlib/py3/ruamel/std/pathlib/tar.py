
from __future__ import annotations

import tarfile
import io

import zstandard

from ruamel.std.pathlib import Path


if not hasattr(Path, 'tar'):

    class TarFile:
        def __init__(self, file_name):
            self._file_name = file_name
            self._stream = None
            self._decoded_stream = None
            self._tar_file = None

        def __enter__(self):
            if self._file_name.suffix == '.zst':
                self._stream = self._file_name.open('rb')
                self._decoded_stream = io.BytesIO(zstandard.ZstdDecompressor().stream_reader(self._stream).read())
                self._tar_file = tarfile.open('r|', fileobj=self._decoded_stream)
            else:
                self._tar_file = tarfile.open(self._file_name)
            return self._tar_file

        def __exit__(self, exc_type, exc_inst, exc_tb):
            if self._tar_file is not None:
                self._tar_file.close()
            if self._decoded_stream is not None:
                # self._decoded_stream.close()
                self._decoded_stream = None
            if self._stream is not None:
                self._stream.close()


    class Tar:
        @property
        def open(self):
            return TarFile(self)

    Path.tar = Tar.open

