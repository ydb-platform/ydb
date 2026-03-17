import traceback

import pytest

from wasabi.traceback_printer import TracebackPrinter


@pytest.fixture
def tb():
    return traceback.extract_stack()


def test_traceback_printer(tb):
    tbp = TracebackPrinter(tb_base="wasabi")
    msg = tbp("Hello world", "This is a test", tb=tb)
    print(msg)


def test_traceback_printer_highlight(tb):
    tbp = TracebackPrinter(tb_base="wasabi")
    msg = tbp("Hello world", "This is a test", tb=tb, highlight="kwargs")
    print(msg)


def test_traceback_printer_custom_colors(tb):
    tbp = TracebackPrinter(
        tb_base="wasabi", color_error="blue", color_highlight="green", color_tb="yellow"
    )
    msg = tbp("Hello world", "This is a test", tb=tb, highlight="kwargs")
    print(msg)


def test_traceback_printer_only_title(tb):
    tbp = TracebackPrinter(tb_base="wasabi")
    msg = tbp("Hello world", tb=tb)
    print(msg)


def test_traceback_dot_relative_path_tb_base(tb):
    tbp = TracebackPrinter(tb_base=".")
    msg = tbp("Hello world", tb=tb)
    print(msg)


def test_traceback_tb_base_none(tb):
    tbp = TracebackPrinter()
    msg = tbp("Hello world", tb=tb)
    print(msg)


def test_traceback_printer_no_tb():
    tbp = TracebackPrinter(tb_base="wasabi")
    msg = tbp("Hello world", "This is a test")
    print(msg)


def test_traceback_printer_custom_tb_range():
    tbp = TracebackPrinter(tb_range_start=-10, tb_range_end=-3)
    msg = tbp("Hello world", "This is a test")
    print(msg)


def test_traceback_printer_custom_tb_range_start():
    tbp = TracebackPrinter(tb_range_start=-1)
    msg = tbp("Hello world", "This is a test")
    print(msg)
