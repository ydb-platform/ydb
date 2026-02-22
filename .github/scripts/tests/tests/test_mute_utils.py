"""Tests for mute_utils."""
import os
import tempfile

import pytest

from mute_utils import _parse_mute_file, apply_quarantine, pattern_to_re


def test_parse_mute_file_empty():
    assert _parse_mute_file(None) == set()
    assert _parse_mute_file('') == set()


def test_parse_mute_file_nonexistent():
    assert _parse_mute_file('/nonexistent/path/12345') == set()


def test_parse_mute_file_skips_comments_and_empty():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write("# comment\n")
        f.write("\n")
        f.write("suite1 test1\n")
        f.write("  \n")
        f.write("# another\n")
        f.write("suite2 test2\n")
        path = f.name
    try:
        result = _parse_mute_file(path)
        assert result == {'suite1 test1', 'suite2 test2'}
    finally:
        os.unlink(path)


def test_apply_quarantine_muted_minus_quarantine():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as m:
        m.write("suite1 test1\nsuite2 test2\nsuite3 test3\n")
        muted = m.name
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as q:
        q.write("suite2 test2\n")
        quarantine = q.name
    try:
        out = apply_quarantine(muted, quarantine)
        assert out == "suite1 test1\nsuite3 test3\n"
    finally:
        os.unlink(muted)
        os.unlink(quarantine)


def test_apply_quarantine_to_file():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as m:
        m.write("a b\nc d\n")
        muted = m.name
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as q:
        q.write("c d\n")
        quarantine = q.name
    out_path = tempfile.mktemp(suffix='.txt')
    try:
        result = apply_quarantine(muted, quarantine, out_path)
        assert result == out_path
        with open(out_path) as f:
            assert f.read() == "a b\n"
    finally:
        for p in [muted, quarantine, out_path]:
            if os.path.exists(p):
                os.unlink(p)


def test_apply_quarantine_empty_quarantine():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as m:
        m.write("suite1 test1\n")
        muted = m.name
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as q:
        q.write("")
        quarantine = q.name
    try:
        out = apply_quarantine(muted, quarantine)
        assert "suite1 test1" in out
    finally:
        os.unlink(muted)
        os.unlink(quarantine)


def test_apply_quarantine_all_quarantined():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as m:
        m.write("suite1 test1\n")
        muted = m.name
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as q:
        q.write("suite1 test1\n")
        quarantine = q.name
    try:
        out = apply_quarantine(muted, quarantine)
        assert out == ""
    finally:
        os.unlink(muted)
        os.unlink(quarantine)


def test_pattern_to_re_wildcard():
    p = pattern_to_re("suite*")
    assert p.startswith("(?:^")
    assert p.endswith("$)")
    assert ".*" in p


def test_pattern_to_re_literal():
    p = pattern_to_re("suite")
    assert "suite" in p
    assert p.startswith("(?:^")
