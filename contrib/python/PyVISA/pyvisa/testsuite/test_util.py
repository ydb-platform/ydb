# -*- coding: utf-8 -*-
"""Test pyvisa utility functions."""

import array
import contextlib
import logging
import os
import struct
import subprocess
import sys
import tempfile
import unittest
from configparser import ConfigParser
from functools import partial
from io import StringIO
from pathlib import Path
from types import ModuleType
from typing import Optional

import pytest
from pytest import LogCaptureFixture, MonkeyPatch

from pyvisa import highlevel, util
from pyvisa.ctwrapper import IVIVisaLibrary
from pyvisa.testsuite import BaseTestCase

np: Optional[ModuleType]
try:
    import numpy

    np = numpy
except ImportError:
    np = None


class TestConfigFile(BaseTestCase):
    """Test reading information from a user configuration file."""

    def setup_method(self):
        # Skip if a real config file exists
        if any(
            os.path.isfile(p)
            for p in [
                os.path.join(sys.prefix, "share", "pyvisa", ".pyvisarc"),
                os.path.join(os.path.expanduser("~"), ".pyvisarc"),
            ]
        ):
            raise unittest.SkipTest(
                ".pyvisarc file exists; cannot properly test in this case"
            )
        self.temp_dir = tempfile.TemporaryDirectory()
        os.makedirs(os.path.join(self.temp_dir.name, "share", "pyvisa"))
        self.config_path = os.path.join(
            self.temp_dir.name, "share", "pyvisa", ".pyvisarc"
        )
        self._prefix = sys.prefix
        sys.prefix = self.temp_dir.name
        self._platform = sys.platform
        self._version_info = sys.version_info
        util._ADDED_DLL_PATHS = set()

    def teardown_method(self):
        self.temp_dir.cleanup()
        sys.prefix = self._prefix
        sys.platform = self._platform
        sys.version_info = self._version_info

    def test_reading_config_file(self):
        config = ConfigParser()
        config["Paths"] = {}
        config["Paths"]["visa library"] = "test"
        with open(self.config_path, "w") as f:
            config.write(f)
        assert util.read_user_library_path() == "test"

    def test_no_section(self, caplog):
        config = ConfigParser()
        with open(self.config_path, "w") as f:
            config.write(f)
        with caplog.at_level(level=logging.DEBUG):
            assert util.read_user_library_path() is None
        assert "NoOptionError or NoSectionError" in caplog.records[1].message

    def test_no_key(self, caplog):
        config = ConfigParser()
        config["Paths"] = {}
        with open(self.config_path, "w") as f:
            config.write(f)
        with caplog.at_level(level=logging.DEBUG):
            assert util.read_user_library_path() is None
        assert "NoOptionError or NoSectionError" in caplog.records[1].message

    def test_no_config_file(self, caplog):
        with caplog.at_level(level=logging.DEBUG):
            assert util.read_user_library_path() is None
        assert "No user defined" in caplog.records[0].message

    # --- Test reading dll_extra_paths.

    def test_reading_config_file_not_windows(self, caplog: LogCaptureFixture):
        sys.platform = "darwin"
        sys.version_info = (3, 12, 1)  # type: ignore[assignment]

        with caplog.at_level(level=logging.DEBUG):
            assert util.add_user_dll_extra_paths() is None
        assert "Not loading dll_extra_paths" in caplog.records[0].message

    def test_reading_config_file_for_dll_extra_paths(self, monkeypatch: MonkeyPatch):
        sys.platform = "win32"
        extra_paths = [
            r"C:\Program Files",
            r"C:\Program Files (x86)",
        ]
        added_dll_directories: list[str] = []
        monkeypatch.setattr(
            os,
            "add_dll_directory",
            lambda path: added_dll_directories.append(path),
            raising=False,
        )
        config = ConfigParser()
        config["Paths"] = {}
        config["Paths"]["dll_extra_paths"] = r"C:\Program Files;C:\Program Files (x86)"

        with open(self.config_path, "w") as f:
            config.write(f)

        assert util.add_user_dll_extra_paths() == extra_paths
        assert added_dll_directories == extra_paths

    def test_reading_config_file_for_dll_extra_paths_multiple_times(
        self, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ):
        sys.platform = "win32"
        sys.version_info = (3, 8, 1)  # type: ignore[assignment]

        iter_count = 5
        extra_paths = [
            r"C:\Program Files",
            r"C:\Program Files (x86)",
        ]
        added_dll_directories: list[str] = []
        monkeypatch.setattr(
            os,
            "add_dll_directory",
            lambda path: added_dll_directories.append(path),
            raising=False,
        )
        config = ConfigParser()
        config["Paths"] = {}
        config["Paths"]["dll_extra_paths"] = r"C:\Program Files;C:\Program Files (x86)"

        with open(self.config_path, "w") as f:
            config.write(f)

        with caplog.at_level(level=logging.DEBUG):
            for _ in range(iter_count):
                assert util.add_user_dll_extra_paths() == extra_paths
        skipping_log_messages = [
            rec.message
            for rec in caplog.records
            if "already been added; skipping" in rec.message
        ]
        assert added_dll_directories == extra_paths
        # one log message per path per iteration, except initial add
        assert (iter_count - 1) * len(extra_paths) == len(skipping_log_messages)

    def test_no_section_for_dll_extra_paths(
        self, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ):
        sys.platform = "win32"
        sys.version_info = (3, 8, 1)  # type: ignore[assignment]

        added_dll_directories: list[str] = []
        monkeypatch.setattr(
            os,
            "add_dll_directory",
            lambda path: added_dll_directories.append(path),
            raising=False,
        )
        config = ConfigParser()

        with open(self.config_path, "w") as f:
            config.write(f)

        with caplog.at_level(level=logging.DEBUG):
            assert util.add_user_dll_extra_paths() is None
        assert "NoOptionError or NoSectionError" in caplog.records[1].message
        assert added_dll_directories == []

    def test_no_key_for_dll_extra_paths(
        self, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ):
        sys.platform = "win32"
        added_dll_directories: list[str] = []
        monkeypatch.setattr(
            os,
            "add_dll_directory",
            lambda path: added_dll_directories.append(path),
            raising=False,
        )
        config = ConfigParser()
        config["Paths"] = {}

        with open(self.config_path, "w") as f:
            config.write(f)

        with caplog.at_level(level=logging.DEBUG):
            assert util.add_user_dll_extra_paths() is None
        assert "NoOptionError or NoSectionError" in caplog.records[1].message
        assert added_dll_directories == []

    def test_no_config_file_for_dll_extra_paths(
        self, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ):
        sys.platform = "win32"
        sys.version_info = (3, 8, 1)  # type: ignore[assignment]

        added_dll_directories: list[str] = []
        monkeypatch.setattr(
            os,
            "add_dll_directory",
            lambda path: added_dll_directories.append(path),
            raising=False,
        )

        with caplog.at_level(level=logging.DEBUG):
            assert util.add_user_dll_extra_paths() is None
        assert "No user defined" in caplog.records[0].message
        assert added_dll_directories == []


class TestParser(BaseTestCase):
    def test_parse_binary(self):
        s = (
            b"#0@\xe2\x8b<@\xe2\x8b<@\xe2\x8b<@\xe2\x8b<@\xde\x8b<@\xde\x8b<@"
            b"\xde\x8b<@\xde\x8b<@\xe0\x8b<@\xe0\x8b<@\xdc\x8b<@\xde\x8b<@"
            b"\xe2\x8b<@\xe0\x8b<"
        )
        e = [
            0.01707566,
            0.01707566,
            0.01707566,
            0.01707566,
            0.01707375,
            0.01707375,
            0.01707375,
            0.01707375,
            0.01707470,
            0.01707470,
            0.01707280,
            0.01707375,
            0.01707566,
            0.01707470,
        ]

        # Test handling indefinite length block
        p = util.from_ieee_block(
            s,
            datatype="f",
            is_big_endian=False,
        )
        for a, b in zip(p, e):
            assert a == pytest.approx(b)

        # Test handling definite length block
        p = util.from_ieee_block(
            b"#256" + s[2:],
            datatype="f",
            is_big_endian=False,
        )
        for a, b in zip(p, e):
            assert a == pytest.approx(b)

        # Test handling zero length block
        p = util.from_ieee_block(
            b"#10" + s[2:],
            datatype="f",
            is_big_endian=False,
        )
        assert not p

        # Test handling definite length block with HP header format
        p = util.from_hp_block(
            b"#A\x38\x00" + s[2:],
            datatype="f",
            is_big_endian=False,
            container=partial(array.array, "f"),
        )
        for a, b in zip(p, e):
            assert a == pytest.approx(b)

        # Test handling definite length block with R&S header format
        p = util.from_rs_block(
            b"#(56)" + s[2:],
            datatype="f",
            is_big_endian=False,
        )
        for a, b in zip(p, e):
            assert a == pytest.approx(b)

        # Test handling definite length block with IEEE header format with
        # the automatic format determination
        p = util.from_ieee_or_rs_block(
            b"#256" + s[2:],
            datatype="f",
            is_big_endian=False,
        )
        for a, b in zip(p, e):
            assert a == pytest.approx(b)

        # Test handling definite length block with R&S header format with
        # the automatic format determination
        p = util.from_ieee_or_rs_block(
            b"#(56)" + s[2:],
            datatype="f",
            is_big_endian=False,
        )
        for a, b in zip(p, e):
            assert a == pytest.approx(b)

        # Test handling indefinite length block with
        # the automatic format determination
        p = util.from_ieee_or_rs_block(
            s,
            datatype="f",
            is_big_endian=False,
        )
        for a, b in zip(p, e):
            assert a == pytest.approx(b)

        # Test handling definite length block with HP header format with
        # automatic format determination
        with pytest.raises(ValueError):
            p = util.from_ieee_or_rs_block(
                b"#A\x38\x00" + s[2:],
                datatype="f",
                is_big_endian=False,
            )

    def test_integer_ascii_block(self):
        values = list(range(99))
        for fmt in "d":
            msg = "block=%s, fmt=%s"
            msg = msg % ("ascii", fmt)

            # Test handling the case of a trailing comma
            def tb(values):
                return util.to_ascii_block(values, fmt, ",") + ","

            def fb(block, cont):
                return util.from_ascii_block(block, fmt, ",", cont)

            self.round_trip_block_conversion(values, tb, fb, msg)

    def test_non_integer_ascii_block(self):
        values = [val + 0.5 for val in range(99)]
        values = list(range(99))
        for fmt in "fFeEgG":
            msg = "block=%s, fmt=%s"
            msg = msg % ("ascii", fmt)

            def tb(values):
                return util.to_ascii_block(values, fmt, ",")

            def fb(block, cont):
                return util.from_ascii_block(block, fmt, ",", cont)

            self.round_trip_block_conversion(values, tb, fb, msg)

    def test_invalid_string_converter(self):
        with pytest.raises(ValueError) as ex:
            util.to_ascii_block([1, 2], "m")
        assert "unsupported format character" in ex.exconly()
        with pytest.raises(ValueError) as ex:
            util.from_ascii_block("1,2,3", "m")
        assert "Invalid code for converter" in ex.exconly()

    def test_function_separator(self):
        values = list(range(99))
        fmt = "d"
        msg = "block=ascii, fmt=%s" % fmt

        def tb(values):
            return util.to_ascii_block(values, fmt, ":".join)

        def fb(block, cont):
            return util.from_ascii_block(block, fmt, lambda s: s.split(":"), cont)

        self.round_trip_block_conversion(values, tb, fb, msg)

    def test_function_converter(self):
        values = list(range(99))
        msg = "block=ascii"

        def tb(values):
            return util.to_ascii_block(values, str, ":".join)

        def fb(block, cont):
            return util.from_ascii_block(block, int, lambda s: s.split(":"), cont)

        self.round_trip_block_conversion(values, tb, fb, msg)

    def test_integer_binary_block(self):
        values = list(range(99))
        for block, tb, fb in zip(
            ("ieee", "hp", "rs"),
            (util.to_ieee_block, util.to_hp_block, util.to_rs_block),
            (util.from_ieee_block, util.from_hp_block, util.from_ieee_or_rs_block),
        ):
            for fmt in "bBhHiIfd":
                for endi in (True, False):
                    msg = "block=%s, fmt=%s, endianness=%s"
                    msg = msg % (block, fmt, endi)

                    def tblock(values):
                        return tb(values, fmt, endi)

                    def fblock(block, cont):
                        return fb(block, fmt, endi, cont)

                    self.round_trip_block_conversion(values, tblock, fblock, msg)

    def test_noninteger_binary_block(self):
        values = [val + 0.5 for val in range(99)]
        for block, tb, fb in zip(
            ("ieee", "hp", "rs"),
            (util.to_ieee_block, util.to_hp_block, util.to_rs_block),
            (util.from_ieee_block, util.from_hp_block, util.from_ieee_or_rs_block),
        ):
            for fmt in "fd":
                for endi in (True, False):
                    msg = "block=%s, fmt=%s, endianness=%s"
                    msg = msg % (block, fmt, endi)

                    def tblock(values):
                        return bytearray(tb(values, fmt, endi))

                    def fblock(block, cont):
                        return fb(block, fmt, endi, cont)

                    self.round_trip_block_conversion(values, tblock, fblock, msg)

    def test_bytes_binary_block(self):
        values = b"dbslbw cj saj \x00\x76"
        for block, tb, fb in zip(
            ("ieee", "hp", "rs", "rs"),
            (util.to_ieee_block, util.to_hp_block, util.to_rs_block, util.to_rs_block),
            (
                util.from_ieee_block,
                util.from_hp_block,
                util.from_ieee_or_rs_block,
                util.from_rs_block,
            ),
        ):
            for fmt in "sbB":
                block = tb(values, datatype=fmt)
                print(fmt, block)
                rt = fb(block, datatype=fmt, container=bytes)
                assert values == rt

    def test_no_start_of_block_indicator_binary_block_header(self):
        values = list(range(10))
        for header, tb, fb in zip(
            ("ieee", "hp", "rs", "rs"),
            (util.to_ieee_block, util.to_hp_block, util.to_rs_block, util.to_rs_block),
            (
                util.from_ieee_block,
                util.from_hp_block,
                util.from_ieee_or_rs_block,
                util.from_rs_block,
            ),
        ):
            block = tb(values, "h", False)
            bad_block = block[1:]
            with pytest.raises(ValueError) as e:
                fb(bad_block, "h", False, list)

            assert '"#' in e.exconly()

    def test_malformed_rs_binary_block_header_no_closing_parenthesis(self):
        values = list(range(10))
        for header, tb, fb in zip(
            ("rs",),
            (util.to_rs_block,),
            (util.from_ieee_or_rs_block,),
        ):
            block = tb(values, "h", False)
            index = block.find(b")")
            bad_block = block[:index] + block[index + 1 :]
            with pytest.raises(RuntimeError):
                fb(bad_block, "h", False, list)

    def test_malformed_rs_binary_block_header_late_closing_parenthesis(self):
        values = list(range(100))
        for header, tb, fb in zip(
            ("rs",),
            (util.to_rs_block,),
            (util.from_ieee_or_rs_block,),
        ):
            block = tb(values, "h", False)
            index = block.find(b")")
            # Another closing parenthesis will appear late in the datastream
            # as part of the sequence from 0 to 100
            bad_block = block[:index] + block[index + 1 :]
            with pytest.raises(RuntimeError):
                fb(bad_block, "h", False, list)

    def test_late_binary_block_header(self):
        values = list(range(100))
        for header, tb, fb in zip(
            ("ieee", "hp", "rs"),
            (util.to_ieee_block, util.to_hp_block, util.to_rs_block),
            (util.from_ieee_block, util.from_hp_block, util.from_ieee_or_rs_block),
        ):
            block = tb(values, "h", False)
            bad_block = block[1:]

            if header == "hp":
                index = bad_block.find(b"#")
                bad_block = bad_block[:index] + b"#A" + bad_block[index + 2 :]
            elif header == "rs":
                index = bad_block.find(b"#")
                bad_block = bad_block[:index] + b"#(123)" + bad_block[index + 2 :]

            with pytest.warns(UserWarning):
                fb(bad_block, "h", False, list)

    def test_late_binary_block_header_raise(self):
        values = list(range(100))
        for header, tb, fb in zip(
            ("ieee", "hp", "rs"),
            (util.to_ieee_block, util.to_hp_block, util.to_rs_block),
            (util.from_ieee_block, util.from_hp_block, util.from_ieee_or_rs_block),
        ):
            block = tb(values, "h", False)
            bad_block = block[1:]

            if header == "hp":
                index = bad_block.find(b"#")
                bad_block = bad_block[:index] + b"#A" + bad_block[index + 2 :]
            elif header == "rs":
                index = bad_block.find(b"#")
                bad_block = bad_block[:index] + b"#(123)" + bad_block[index + 2 :]

            if header == "ieee":
                parse = util.parse_ieee_block_header
            elif header == "hp":
                parse = partial(util.parse_hp_block_header, is_big_endian=False)
            elif header == "rs":
                parse = util.parse_ieee_or_rs_block_header

            with pytest.raises(RuntimeError):
                parse(bad_block, raise_on_late_block=True)

            parse(bad_block, length_before_block=1000)

    def test_binary_block_shorter_than_advertized(self):
        values = list(range(99))
        for header, tb, fb in zip(
            ("ieee", "hp", "rs", "rs"),
            (util.to_ieee_block, util.to_hp_block, util.to_rs_block, util.to_rs_block),
            (
                util.from_ieee_block,
                util.from_hp_block,
                util.from_ieee_or_rs_block,
                util.from_rs_block,
            ),
        ):
            block = tb(values, "h", False)
            if header == "ieee":
                header_byte_number = int(block[1])
                block = (
                    block[:2]
                    + b"9" * header_byte_number
                    + block[2 + header_byte_number :]
                )
            elif header == "rs":
                block = b"#(9999" + block[block.find(b")") :]
            else:
                block = block[:2] + b"\xff\xff\xff\xff" + block[2 + 4 :]
            with pytest.raises(ValueError) as e:
                fb(block, "h", False, list)

            assert "Binary data is incomplete" in e.exconly()

    def test_guessing_block_length(self):
        values = list(range(99))
        for header, tb, fb in zip(
            ("ieee", "hp", "rs"),
            (util.to_ieee_block, util.to_hp_block, util.to_rs_block),
            (util.from_ieee_block, util.from_hp_block, util.from_rs_block),
        ):
            block = tb(values, "h", False) + b"\n"
            if header == "ieee":
                header_length = int(block[1:2].decode())
                block = block[:2] + b"0" * header_length + block[2 + header_length :]
            elif header == "rs":
                header_length = block.find(b")") - 2
                block = block[:2] + b"0" * header_length + block[2 + header_length :]
            else:
                block = block[:2] + b"\x00\x00\x00\x00" + block[2 + 4 :]
            assert not fb(block, "h", False, list)

    def test_block_length_too_large_for_header(self):
        values = range(1_000_000_000_000_000)
        for tb in (util.to_ieee_block, util.to_hp_block):
            with pytest.raises(OverflowError):
                tb(values, datatype="q")

    def test_handling_malformed_binary(self):
        containers = (list, tuple) + ((np.array, np.ndarray) if np else ())

        # Use this to generate malformed data which should in theory be
        # impossible
        class DumbBytes(bytes):
            def __len__(self):
                return 10

        for container in containers:
            with pytest.raises(ValueError) as e:
                util.from_binary_block(DumbBytes(b"\x00\x00\x00"), container=container)
            assert (
                "malformed" if container in (list, tuple) else "buffer" in e.exconly()
            )

    def round_trip_block_conversion(self, values, to_block, from_block, msg):
        """Test that block conversion round trip as expected."""
        containers = (list, tuple) + ((np.array,) if np else ())
        for cont in containers:
            conv = cont(values)
            msg += ", container=%s"
            msg = msg % cont.__name__
            try:
                block = to_block(conv)
                parsed = from_block(block, cont)
            except Exception as e:
                raise Exception(msg + "\n" + repr(e))

            if np and cont in (np.array,):
                np.testing.assert_array_equal(conv, parsed, msg)
            else:
                assert conv == parsed, msg


class TestSystemDetailsAnalysis(BaseTestCase):
    """Test getting the system details."""

    def setup_method(self):
        self._unicode_size = sys.maxunicode

    def teardown_method(self):
        sys.maxunicode = self._unicode_size

    def test_getting_system_details(self):
        sys.maxunicode = 65535
        path = os.path.join(os.path.dirname(__file__), "fake-extensions")
        sys.path.append(path)
        try:
            details = util.get_system_details(True)
        finally:
            sys.path.remove(path)
        assert details["backends"]
        assert details["unicode"] == "UCS2"

        sys.maxunicode = 1114111
        details = util.get_system_details(False)
        assert not details["backends"]
        assert details["unicode"] == "UCS4"

    def test_get_debug_info(self):
        details = util.system_details_to_str(util.get_system_details())
        assert util.get_debug_info(False) == details
        temp_stdout = StringIO()
        with contextlib.redirect_stdout(temp_stdout):
            util.get_debug_info()
        output = temp_stdout.getvalue()
        assert output.strip() == details.strip()

    def test_system_details_for_plugins(self):
        """Test reporting on plugins."""

        def dummy_list_backends():
            return ["test1", "test2", "test3", "test4"]

        def dummy_get_wrapper_class(backend):
            if backend == "test1":
                return IVIVisaLibrary

            elif backend == "test2":

                class BrokenBackend:
                    @classmethod
                    def get_debug_info(cls):
                        raise Exception()

                return BrokenBackend

            elif backend == "test4":

                class WeirdBackend:
                    @classmethod
                    def get_debug_info(cls):
                        return {"": {"": [object()]}}

                return WeirdBackend

            else:
                raise Exception()

        old_lb = highlevel.list_backends
        old_gwc = highlevel.get_wrapper_class
        highlevel.list_backends = dummy_list_backends
        highlevel.get_wrapper_class = dummy_get_wrapper_class

        try:
            details = util.get_system_details()
        finally:
            highlevel.list_backends = old_lb
            highlevel.get_wrapper_class = old_gwc

        assert "Could not instantiate" in details["backends"]["test3"][0]
        assert "Could not obtain" in details["backends"]["test2"][0]
        assert "Version" in details["backends"]["test1"]
        assert "" in details["backends"]["test4"]

        # Test converting the details to string
        util.system_details_to_str(details)


def generate_fakelibs(dirname: Path):
    libs = {
        "fakelib_bad_magic.dll": struct.pack(
            "=6sH52sl", b"MAPE\x00\x00", 0x014C, 52 * b"\0", 2
        ),
        "fakelib_good_x86_32.dll": struct.pack(
            "=6sH52sl", b"MZPE\x00\x00", 0x014C, 52 * b"\0", 2
        ),
        "fakelib_good_x86_64.dll": struct.pack(
            "=6sH52sl", b"MZPE\x00\x00", 0x8664, 52 * b"\0", 2
        ),
        "fakelib_good_arm_64.dll": struct.pack(
            "=6sH52sl", b"MZPE\x00\x00", 0xAA64, 52 * b"\0", 2
        ),
        "fakelib_good_unknown.dll": struct.pack(
            "=6sH52sl", b"MZPE\x00\x00", 0xFFFF, 52 * b"\0", 2
        ),
        "fakelib_not_pe.dll": struct.pack(
            "=6sH52sl", b"MZDE\x00\x00", 0x014C, 52 * b"\0", 2
        ),
    }
    for name, blob in libs.items():
        with open(dirname / name, "wb") as f:
            f.write(blob)
            print("Written %s" % name)


class TestLibraryAnalysis(BaseTestCase):
    """Test (through monkey patching) the analysis of binary libraries."""

    def test_get_shared_library_arch(self, tmp_path: Path):
        """Test analysing a library on Windows."""
        generate_fakelibs(tmp_path)

        for f, a in zip(
            ["x86_32", "x86_64", "arm_64"],
            [
                util.PEMachineType.I386,
                util.PEMachineType.AMD64,
                util.PEMachineType.AARCH64,
            ],
        ):
            arch = util.get_shared_library_arch(tmp_path / f"fakelib_good_{f}.dll")
            assert arch == a

        arch = util.get_shared_library_arch(tmp_path / "fakelib_good_unknown.dll")
        assert arch == util.PEMachineType.UNKNOWN

        with pytest.raises(Exception) as e:
            util.get_shared_library_arch(tmp_path / "fakelib_bad_magic.dll")
        assert "Not an executable" in e.exconly()

        with pytest.raises(Exception) as e:
            util.get_shared_library_arch(tmp_path / "fakelib_not_pe.dll")
        assert "Not a PE executable" in e.exconly()

    def test_get_arch_windows(self, tmp_path: Path):
        """Test identifying the computer architecture on windows."""
        generate_fakelibs(tmp_path)

        platform = sys.platform
        sys.platform = "win32"
        try:
            for f, a in zip(
                ["x86_32", "x86_64", "arm_64", "_unknown"],
                [
                    [util.ArchitectureType.I386],
                    [util.ArchitectureType.X86_64],
                    [util.ArchitectureType.AARCH64],
                    [],
                ],
            ):
                print(f, a)
                path = tmp_path / f"fakelib_good_{f}.dll"
                lib = util.LibraryPath(str(path))
                assert lib.arch == a
        finally:
            sys.platform = platform

    @pytest.mark.skipif(sys.version_info < (3, 7), reason="Fails weirdly on Python 3.6")
    def test_get_arch_unix(self):
        """Test identifying the computer architecture on linux and Mac."""
        platform = sys.platform
        run = subprocess.run
        try:

            def alt_run(*args, **kwargs):
                if platform.startswith("win"):
                    kwargs["shell"] = True
                return run(["echo", args[0][1]], *args[1:], **kwargs)

            subprocess.run = alt_run

            for p, f, a in [
                ("linux", "80386", [(util.ArchitectureType.I386)]),
                ("linux", "x86-64", [(util.ArchitectureType.X86_64)]),
                ("linux", "aarch64", [(util.ArchitectureType.AARCH64)]),
                ("darwin", "executable i386", [(util.ArchitectureType.I386)]),
                ("darwin", "executable x86_64", [(util.ArchitectureType.X86_64)]),
                ("darwin", "executable arm64", [(util.ArchitectureType.AARCH64)]),
            ]:
                sys.platform = p
                lib = util.LibraryPath(f)
                assert lib.arch == a
        finally:
            sys.platform = platform
            subprocess.run = run

    def test_get_arch_unix_unreported(self):
        """Test identifying the computer architecture on an unknown platform."""
        platform = sys.platform
        run = subprocess.run
        try:
            sys.platform = "darwin"
            lib = util.LibraryPath("")
            assert lib.arch == []
        finally:
            sys.platform = platform
            subprocess.run = run

    def test_get_arch_unknown(self):
        """Test identifying the computer architecture on an unknown platform."""
        platform = sys.platform
        run = subprocess.run
        try:
            sys.platform = "test"
            lib = util.LibraryPath("")
            assert lib.arch == []
        finally:
            sys.platform = platform
            subprocess.run = run


class TestMessageSize(BaseTestCase):
    """Test the message_size() function."""

    @pytest.mark.parametrize(
        "parameters, expected",
        [
            ((10, "h", "empty"), 21),
            ((10, "h", "ieee"), 25),
            ((10, "f", "hp"), 45),
            ((10, "f", "empty"), 41),
        ],
    )
    def test_message_size(self, parameters, expected):
        """Test the functionality of the message_size() function."""
        assert util.message_size(*parameters) == expected

    def test_message_size_error(self):
        """Verify that a bad header format value raises a ValueError"""
        with pytest.raises(ValueError):
            util.message_size(1, "f", "BAD VALUE")

    def test_message_defaults(self):
        """Verify that message_size() uses the expected point_size and header_format
        default values."""
        expected = util.message_size(num_points=100, datatype="f", header_format="ieee")
        assert util.message_size(num_points=100) == expected
