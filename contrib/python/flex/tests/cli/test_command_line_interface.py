import os

import tempfile
import click
from click.testing import CliRunner
from flex.cli import main
from library.python import resource


DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_flex_cli_schema_validation():
    runner = CliRunner()
    with tempfile.NamedTemporaryFile() as f:
        f.write(resource.find('schemas/cli-test-valid-schema.yaml'))
        f.flush()
        result = runner.invoke(main, ['-s', f.name])

    assert result.exit_code == 0, result.output
    assert result.output == 'Validation passed\n'


def test_flex_cli_schema_validation_with_verbose_argument_name():
    runner = CliRunner()
    with tempfile.NamedTemporaryFile() as f:
        f.write(resource.find('schemas/cli-test-valid-schema.yaml'))
        f.flush()
        result = runner.invoke(main, ['--source', f.name])

    assert result.exit_code == 0
    assert result.output == 'Validation passed\n'


def test_flex_cli_schema_validation_with_invalid_schema():
    runner = CliRunner()
    with tempfile.NamedTemporaryFile() as f:
        f.write(resource.find('schemas/cli-test-invalid-schema.yaml'))
        f.flush()
        result = runner.invoke(main, ['--source', f.name])

    assert result.exit_code == 1
    assert "Invalid value. 2.1 is not one of the available options (['2.0'])" in result.output
