from __future__ import annotations

from jupyter_core.troubleshoot import main


def test_troubleshoot(capsys):
    """Smoke test the troubleshoot function"""
    main()
    out = capsys.readouterr().out
    assert "pip list" in out
