import pathlib

import pytest

from babel.messages import frontend

jinja2 = pytest.importorskip("jinja2")

import yatest.common as yc
jinja2_data_path = pathlib.Path(yc.source_path(__file__)).parent / "jinja2_data"


def test_jinja2_interop(monkeypatch, tmp_path):
    """
    Test that babel can extract messages from Jinja2 templates.
    """
    monkeypatch.chdir(jinja2_data_path)
    cli = frontend.CommandLineInterface()
    pot_file = tmp_path / "messages.pot"
    cli.run(['pybabel', 'extract', '--mapping', 'mapping.cfg', '-o', str(pot_file), '.'])
    assert '"Hello, %(name)s!"' in pot_file.read_text()
