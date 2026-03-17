import os
import re
import time

import pytest

from wasabi.printer import Printer
from wasabi.util import MESSAGES, NO_UTF8, supports_ansi

SUPPORTS_ANSI = supports_ansi()


def test_printer():
    p = Printer(no_print=True)
    text = "This is a test."
    good = p.good(text)
    fail = p.fail(text)
    warn = p.warn(text)
    info = p.info(text)
    assert p.text(text) == text
    if SUPPORTS_ANSI and not NO_UTF8:
        assert good == "\x1b[38;5;2m\u2714 {}\x1b[0m".format(text)
        assert fail == "\x1b[38;5;1m\u2718 {}\x1b[0m".format(text)
        assert warn == "\x1b[38;5;3m\u26a0 {}\x1b[0m".format(text)
        assert info == "\x1b[38;5;4m\u2139 {}\x1b[0m".format(text)
    if SUPPORTS_ANSI and NO_UTF8:
        assert good == "\x1b[38;5;2m[+] {}\x1b[0m".format(text)
        assert fail == "\x1b[38;5;1m[x] {}\x1b[0m".format(text)
        assert warn == "\x1b[38;5;3m[!] {}\x1b[0m".format(text)
        assert info == "\x1b[38;5;4m[i] {}\x1b[0m".format(text)
    if not SUPPORTS_ANSI and not NO_UTF8:
        assert good == "\u2714 {}".format(text)
        assert fail == "\u2718 {}".format(text)
        assert warn == "\u26a0 {}".format(text)
        assert info == "\u2139 {}".format(text)
    if not SUPPORTS_ANSI and NO_UTF8:
        assert good == "[+] {}".format(text)
        assert fail == "[x] {}".format(text)
        assert warn == "[!] {}".format(text)
        assert info == "[i] {}".format(text)


def test_printer_print():
    p = Printer()
    text = "This is a test."
    p.good(text)
    p.fail(text)
    p.info(text)
    p.text(text)


def test_printer_print_timestamp():
    p = Printer(no_print=True, timestamp=True)
    result = p.info("Hello world")
    matches = re.match("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}", result)
    assert matches


def test_printer_no_pretty():
    p = Printer(no_print=True, pretty=False)
    text = "This is a test."
    assert p.good(text) == text
    assert p.fail(text) == text
    assert p.warn(text) == text
    assert p.info(text) == text
    assert p.text(text) == text


def test_printer_custom():
    colors = {"yellow": 220, "purple": 99}
    icons = {"warn": "\u26a0\ufe0f", "question": "?"}
    p = Printer(no_print=True, colors=colors, icons=icons)
    text = "This is a test."
    purple_question = p.text(text, color="purple", icon="question")
    warning = p.warn(text)
    if SUPPORTS_ANSI and not NO_UTF8:
        assert purple_question == "\x1b[38;5;99m? {}\x1b[0m".format(text)
        assert warning == "\x1b[38;5;3m\u26a0\ufe0f {}\x1b[0m".format(text)
    if SUPPORTS_ANSI and NO_UTF8:
        assert purple_question == "\x1b[38;5;99m? {}\x1b[0m".format(text)
        assert warning == "\x1b[38;5;3m?? {}\x1b[0m".format(text)
    if not SUPPORTS_ANSI and not NO_UTF8:
        assert purple_question == "? {}".format(text)
        assert warning == "\u26a0\ufe0f {}".format(text)
    if not SUPPORTS_ANSI and NO_UTF8:
        assert purple_question == "? {}".format(text)
        assert warning == "?? {}".format(text)


def test_color_as_int():
    p = Printer(no_print=True)
    text = "This is a text."
    result = p.text(text, color=220)
    if SUPPORTS_ANSI:
        assert result == "\x1b[38;5;220mThis is a text.\x1b[0m"
    else:
        assert result == "This is a text."


def test_bg_color():
    p = Printer(no_print=True)
    text = "This is a text."
    result = p.text(text, bg_color="red")
    print(result)
    if SUPPORTS_ANSI:
        assert result == "\x1b[48;5;1mThis is a text.\x1b[0m"
    else:
        assert result == "This is a text."


def test_bg_color_as_int():
    p = Printer(no_print=True)
    text = "This is a text."
    result = p.text(text, bg_color=220)
    print(result)
    if SUPPORTS_ANSI:
        assert result == "\x1b[48;5;220mThis is a text.\x1b[0m"
    else:
        assert result == "This is a text."


def test_color_and_bc_color():
    p = Printer(no_print=True)
    text = "This is a text."
    result = p.text(text, color="green", bg_color="yellow")
    print(result)
    if SUPPORTS_ANSI:
        assert result == "\x1b[38;5;2;48;5;3mThis is a text.\x1b[0m"
    else:
        assert result == "This is a text."


def test_printer_counts():
    p = Printer()
    text = "This is a test."
    for i in range(2):
        p.good(text)
    for i in range(1):
        p.fail(text)
    for i in range(4):
        p.warn(text)
    assert p.counts[MESSAGES.GOOD] == 2
    assert p.counts[MESSAGES.FAIL] == 1
    assert p.counts[MESSAGES.WARN] == 4


def test_printer_spaced():
    p = Printer(no_print=True, pretty=False)
    text = "This is a test."
    assert p.good(text) == text
    assert p.good(text, spaced=True) == "\n{}\n".format(text)


def test_printer_divider():
    p = Printer(line_max=20, no_print=True)
    p.divider() == "\x1b[1m\n================\x1b[0m"
    p.divider("test") == "\x1b[1m\n====== test ======\x1b[0m"
    p.divider("test", char="*") == "\x1b[1m\n****** test ******\x1b[0m"
    assert (
        p.divider("This is a very long text, it is very long")
        == "\x1b[1m\n This is a very long text, it is very long \x1b[0m"
    )
    with pytest.raises(ValueError):
        p.divider("test", char="~.")


@pytest.mark.parametrize("hide_animation", [False, True])
def test_printer_loading(hide_animation):
    p = Printer(hide_animation=hide_animation)
    print("\n")
    with p.loading("Loading..."):
        time.sleep(1)
    p.good("Success!")

    with p.loading("Something else..."):
        time.sleep(2)
    p.good("Yo!")

    with p.loading("Loading..."):
        time.sleep(1)
    p.good("Success!")


def test_printer_loading_raises_exception():
    def loading_with_exception():
        p = Printer()
        print("\n")
        with p.loading():
            raise Exception("This is an error.")

    with pytest.raises(Exception):
        loading_with_exception()


def test_printer_loading_no_print():
    p = Printer(no_print=True)
    with p.loading("Loading..."):
        time.sleep(1)
    p.good("Success!")


def test_printer_log_friendly():
    text = "This is a test."
    ENV_LOG_FRIENDLY = "WASABI_LOG_FRIENDLY"
    os.environ[ENV_LOG_FRIENDLY] = "True"
    p = Printer(no_print=True)
    assert p.good(text) in ("\u2714 This is a test.", "[+] This is a test.")
    del os.environ[ENV_LOG_FRIENDLY]


def test_printer_log_friendly_prefix():
    text = "This is a test."
    ENV_LOG_FRIENDLY = "CUSTOM_LOG_FRIENDLY"
    os.environ[ENV_LOG_FRIENDLY] = "True"
    p = Printer(no_print=True, env_prefix="CUSTOM")
    assert p.good(text) in ("\u2714 This is a test.", "[+] This is a test.")
    print(p.good(text))
    del os.environ[ENV_LOG_FRIENDLY]


@pytest.mark.skip(reason="Now seems to raise TypeError: readonly attribute?")
def test_printer_none_encoding(monkeypatch):
    """Test that printer works even if sys.stdout.encoding is set to None. This
    previously caused a very confusing error."""
    monkeypatch.setattr("sys.stdout.encoding", None)
    p = Printer()  # noqa: F841


def test_printer_no_print_raise_on_exit():
    """Test that the printer raises if a non-zero exit code is provided, even
    if no_print is set to True."""
    err = "This is an error."
    p = Printer(no_print=True, pretty=False)
    with pytest.raises(SystemExit) as e:
        p.fail(err, exits=True)
    assert str(e.value).strip()[-len(err) :] == err
