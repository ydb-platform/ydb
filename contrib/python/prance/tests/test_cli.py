"""Test suite for prance.cli ."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()

import pytest

from . import none_of

import yatest.common as yc


@pytest.fixture
def runner():
    from click.testing import CliRunner

    return CliRunner()


@pytest.mark.skipif(none_of("click"), reason="Click does not exist")
def test_validate_defaults(runner):
    from prance import cli

    # Good example
    result = runner.invoke(cli.validate, [yc.test_source_path("specs/petstore.yaml")])
    assert result.exit_code == 0
    expected = """Processing "{}/specs/petstore.yaml"...
 -> Resolving external references.
Validates OK as Swagger/OpenAPI 2.0!
""".format(yc.test_source_path())
    assert result.output == expected

    # Bad example
    result = runner.invoke(cli.validate, [yc.test_source_path("specs/definitions.yaml")])
    assert result.exit_code == 1
    assert "ValidationError" in result.output


@pytest.mark.skipif(none_of("click"), reason="Click does not exist")
def test_validate_multiple(runner):
    from prance import cli

    result = runner.invoke(
        cli.validate, [yc.test_source_path("specs/petstore.yaml"), yc.test_source_path("specs/petstore.yaml")]
    )
    assert result.exit_code == 0
    expected = """Processing "{0}/specs/petstore.yaml"...
 -> Resolving external references.
Validates OK as Swagger/OpenAPI 2.0!
Processing "{0}/specs/petstore.yaml"...
 -> Resolving external references.
Validates OK as Swagger/OpenAPI 2.0!
""".format(yc.test_source_path())
    assert result.output == expected


@pytest.mark.skipif(none_of("click"), reason="Click does not exist")
def test_validate_no_resolve(runner):
    from prance import cli

    # Good example
    result = runner.invoke(cli.validate, ["--no-resolve", yc.test_source_path("specs/petstore.yaml")])
    assert result.exit_code == 0
    expected = """Processing "{}/specs/petstore.yaml"...
 -> Not resolving external references.
Validates OK as Swagger/OpenAPI 2.0!
""".format(yc.test_source_path())
    assert result.output == expected


@pytest.mark.skipif(none_of("click"), reason="Click does not exist")
def test_validate_output_too_many_inputs(runner):
    from prance import cli

    result = runner.invoke(
        cli.validate,
        ["-o", "foo", yc.test_source_path("specs/petstore.yaml"), yc.test_source_path("specs/petstore.yaml")],
    )
    assert result.exit_code == 2
    assert "If --output-file is given," in result.output


@pytest.mark.skipif(none_of("click"), reason="Click does not exist")
def test_validate_output(runner):
    from prance import cli

    import os, os.path

    curdir = os.getcwd()

    outnames = ["foo.json", "foo.yaml"]
    for outname in outnames:
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli.validate,
                ["-o", outname, os.path.join(curdir, yc.test_source_path("specs/petstore.yaml"))],
            )
            assert result.exit_code == 0

            # There also must be a 'foo' file now.
            files = [f for f in os.listdir(".") if os.path.isfile(f)]
            assert outname in files

            # Ensure a UTF-8 file encoding.
            from prance.util import fs

            assert "utf-8" in fs.detect_encoding(
                outname, default_to_utf8=False, read_all=True
            )

            # Now do a full encoding detection, too

            # The 'foo' file must be a valid swagger spec.
            result = runner.invoke(cli.validate, [outname])
            assert result.exit_code == 0


@pytest.mark.skipif(none_of("click"), reason="Click does not exist")
def test_compile_defaults(runner):
    from prance import cli

    # Good example
    result = runner.invoke(cli.compile, [yc.test_source_path("specs/petstore.yaml")])
    assert result.exit_code == 0
    expected = """Processing "{}/specs/petstore.yaml"...
 -> Resolving external references.
Validates OK as Swagger/OpenAPI 2.0!
""".format(yc.test_source_path())
    assert result.output.startswith(expected)

    # Bad example
    result = runner.invoke(cli.validate, [yc.test_source_path("specs/definitions.yaml")])
    assert result.exit_code == 1
    assert "ValidationError" in result.output


@pytest.mark.skipif(none_of("click"), reason="Click does not exist")
def test_compile_output(runner):
    from prance import cli

    import os, os.path

    curdir = os.getcwd()

    outnames = ["foo.json", "foo.yaml"]
    for outname in outnames:
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli.compile,
                [os.path.join(curdir, yc.test_source_path("specs/petstore.yaml")), outname],
            )
            assert result.exit_code == 0

            # There also must be a 'foo' file now.
            files = [f for f in os.listdir(".") if os.path.isfile(f)]
            assert outname in files

            # Ensure a UTF-8 file encoding.
            from prance.util import fs

            assert "utf-8" in fs.detect_encoding(
                outname, default_to_utf8=False, read_all=True
            )

            # Now do a full encoding detection, too

            # The 'foo' file must be a valid swagger spec.
            result = runner.invoke(cli.validate, [outname])
            assert result.exit_code == 0


@pytest.mark.skip(reason="This test uses 3rd party resource 'mermade.org.uk'")
@pytest.mark.skipif(none_of("click"), reason="Click does not exist")
@pytest.mark.requires_network()
def test_convert_defaults(runner):
    from prance import cli

    # Good example
    result = runner.invoke(cli.convert, [yc.test_source_path("specs/petstore.yaml")])
    assert result.exit_code == 0
    assert "openapi" in result.output
    assert "3." in result.output

    # Bad example
    result = runner.invoke(cli.validate, [yc.test_source_path("specs/definitions.yaml")])
    assert result.exit_code == 1
    assert "ValidationError" in result.output


@pytest.mark.skip(reason="This test uses 3rd party resource 'mermade.org.uk'")
@pytest.mark.skipif(none_of("click"), reason="Click does not exist")
@pytest.mark.requires_network()
def test_convert_output(runner):
    from prance import cli

    import os, os.path

    curdir = os.getcwd()

    outnames = ["foo.json", "foo.yaml"]
    for outname in outnames:
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli.convert,
                [os.path.join(curdir, yc.test_source_path("specs/petstore.yaml")), outname],
            )
            assert result.exit_code == 0

            # There also must be a 'foo' file now.
            files = [f for f in os.listdir(".") if os.path.isfile(f)]
            assert outname in files

            # Ensure a UTF-8 file encoding.
            from prance.util import fs

            assert "utf-8" in fs.detect_encoding(
                outname, default_to_utf8=False, read_all=True
            )

            # Now do a full encoding detection, too
            contents = fs.read_file(outname)
            assert "openapi" in contents
            assert "3." in contents
