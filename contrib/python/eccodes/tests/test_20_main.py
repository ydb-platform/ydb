import pytest

from eccodes import __main__


def test_main(capsys):
    __main__.main(argv=["selfcheck"])
    stdout, _ = capsys.readouterr()

    assert "Your system is ready." in stdout

    with pytest.raises(RuntimeError):
        __main__.main(argv=["non-existent-command"])
