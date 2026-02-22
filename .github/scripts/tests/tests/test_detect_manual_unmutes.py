"""Tests for detect_manual_unmutes."""
import os
import tempfile

import pytest

# Import the module - it has parse_mute_file and main
import importlib.util
_spec = importlib.util.spec_from_file_location(
    "detect_manual_unmutes",
    os.path.join(os.path.dirname(__file__), '..', 'detect_manual_unmutes.py')
)
_dmu = importlib.util.module_from_spec(_spec)


def test_parse_mute_file():
    _spec.loader.exec_module(_dmu)
    parse = _dmu.parse_mute_file
    assert parse(None) == set()
    assert parse('') == set()
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write("a b\n# comment\nc d\n")
        path = f.name
    try:
        assert parse(path) == {'a b', 'c d'}
    finally:
        os.unlink(path)


def test_manual_unmute_detection():
    _spec.loader.exec_module(_dmu)
    parse = _dmu.parse_mute_file
    previous = {'suite1 test1', 'suite2 test2', 'suite3 test3'}
    current = {'suite1 test1', 'suite3 test3'}  # suite2 removed
    our_to_unmute = {'suite3 test3'}  # we unmuted suite3
    removed = previous - current  # suite2 test2
    manual = removed - our_to_unmute  # suite2 (we didn't unmute it)
    assert manual == {'suite2 test2'}


def test_auto_unmute_not_manual():
    _spec.loader.exec_module(_dmu)
    previous = {'suite1 test1', 'suite2 test2'}
    current = {'suite1 test1'}
    our_to_unmute = {'suite2 test2'}
    removed = previous - current
    manual = removed - our_to_unmute
    assert manual == set()


def test_main_integration(tmp_path):
    _spec.loader.exec_module(_dmu)
    prev = tmp_path / "prev.txt"
    curr = tmp_path / "curr.txt"
    to_unmute = tmp_path / "to_unmute.txt"
    quarantine = tmp_path / "quarantine.txt"
    prev.write_text("a b\nc d\n")
    curr.write_text("a b\n")  # c d removed
    to_unmute.write_text("")  # we didn't unmute c d
    quarantine.write_text("")
    import sys
    old_argv = sys.argv
    sys.argv = ['detect_manual_unmutes', '--previous_base', str(prev), '--current_base', str(curr),
                '--to_unmute', str(to_unmute), '--quarantine_file', str(quarantine)]
    try:
        rc = _dmu.main()
        assert rc == 0
        assert quarantine.read_text().strip() == "c d"
    finally:
        sys.argv = old_argv


def test_main_no_manual_unmutes(tmp_path):
    _spec.loader.exec_module(_dmu)
    prev = tmp_path / "prev.txt"
    curr = tmp_path / "curr.txt"
    to_unmute = tmp_path / "to_unmute.txt"
    quarantine = tmp_path / "quarantine.txt"
    prev.write_text("a b\nc d\n")
    curr.write_text("a b\n")
    to_unmute.write_text("c d\n")  # we unmuted c d
    quarantine.write_text("")
    import sys
    old_argv = sys.argv
    sys.argv = ['detect_manual_unmutes', '--previous_base', str(prev), '--current_base', str(curr),
                '--to_unmute', str(to_unmute), '--quarantine_file', str(quarantine)]
    try:
        rc = _dmu.main()
        assert rc == 0
        assert quarantine.read_text().strip() == ""
    finally:
        sys.argv = old_argv
