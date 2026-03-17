# Copyright (c) 2014-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import BinaryIO, cast, TextIO, Optional, Iterator
import zipfile
from contextlib import contextmanager

from ezdxf.lldxf.validator import is_dxf_stream, dxf_info

CRLF = b"\r\n"
LF = b"\n"


class ZipReader:
    def __init__(self, zip_archive_name: str, errors="surrogateescape"):
        if not zipfile.is_zipfile(zip_archive_name):
            raise IOError(f"'{zip_archive_name}' is not a zip archive.")
        self.zip_archive_name = zip_archive_name
        self.zip_archive: Optional[zipfile.ZipFile] = None
        self.dxf_file_name: Optional[str] = None
        self.dxf_file: Optional[BinaryIO] = None
        self.encoding = "cp1252"
        self.errors = errors
        self.dxfversion = "AC1009"

    def open(self, dxf_file_name: Optional[str] = None) -> None:
        def open_dxf_file() -> BinaryIO:
            # Open always in binary mode:
            return cast(BinaryIO, self.zip_archive.open(self.dxf_file_name))  # type: ignore

        self.zip_archive = zipfile.ZipFile(self.zip_archive_name)
        self.dxf_file_name = (
            dxf_file_name
            if dxf_file_name is not None
            else self.get_first_dxf_file_name()
        )
        self.dxf_file = open_dxf_file()

        # Reading with standard encoding 'cp1252' - readline() fails if leading
        # comments contain none ASCII characters.
        if not is_dxf_stream(cast(TextIO, self)):
            raise IOError(f"'{self.dxf_file_name}' is not a DXF file.")
        self.dxf_file = open_dxf_file()  # restart
        self.get_dxf_info()
        self.dxf_file = open_dxf_file()  # restart

    def get_first_dxf_file_name(self) -> str:
        dxf_file_names = self.get_dxf_file_names()
        if len(dxf_file_names) > 0:
            return dxf_file_names[0]
        else:
            raise IOError("No DXF files found.")

    def get_dxf_file_names(self) -> list[str]:
        assert self.zip_archive is not None
        return [
            name
            for name in self.zip_archive.namelist()
            if name.lower().endswith(".dxf")
        ]

    def get_dxf_info(self) -> None:
        info = dxf_info(cast(TextIO, self))
        # Since DXF R2007 (AC1021) file encoding is always 'utf-8'
        self.encoding = info.encoding if info.version < "AC1021" else "utf-8"
        self.dxfversion = info.version

    def readline(self) -> str:
        assert self.dxf_file is not None
        next_line = self.dxf_file.readline().replace(CRLF, LF)
        return str(next_line, self.encoding, self.errors)

    def close(self) -> None:
        assert self.zip_archive is not None
        self.zip_archive.close()


@contextmanager
def ctxZipReader(
    zipfilename: str,
    filename: Optional[str] = None,
    errors: str = "surrogateescape",
) -> Iterator[ZipReader]:
    zip_reader = ZipReader(zipfilename, errors=errors)
    zip_reader.open(filename)
    yield zip_reader
    zip_reader.close()
