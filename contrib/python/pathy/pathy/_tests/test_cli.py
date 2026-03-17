import pathlib
import tempfile

import pytest
from typer.testing import CliRunner

from pathy import BlobStat, BucketClientFS, Pathy
from pathy.cli import app

from .conftest import ENV_ID, TEST_ADAPTERS

runner = CliRunner()

# TODO: add support for wildcard cp/mv/rm/ls paths (e.g. "pathy cp gs://my/*.file ./")
# TODO: add support for streaming in/out sources (e.g. "pathy cp - gs://my/my.file")


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_cp_invalid_from_path(with_adapter: str, bucket: str) -> None:
    source = f"{with_adapter}://{bucket}/{ENV_ID}/cli_cp_file_invalid/file.txt"
    destination = f"{with_adapter}://{bucket}/{ENV_ID}/cli_cp_file_invalid/dest.txt"
    assert runner.invoke(app, ["cp", source, destination]).exit_code == 1
    assert not Pathy(destination).is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_cp_file(with_adapter: str, bucket: str) -> None:
    source = f"{with_adapter}://{bucket}/{ENV_ID}/cli_cp_file/file.txt"
    destination = f"{with_adapter}://{bucket}/{ENV_ID}/cli_cp_file/other.txt"
    Pathy(source).write_text("---")
    assert runner.invoke(app, ["cp", source, destination]).exit_code == 0
    assert Pathy(source).exists()
    assert Pathy(destination).is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_cp_file_name_from_source(with_adapter: str, bucket: str) -> None:
    source = pathlib.Path("./file.txt")
    source.touch()
    destination = f"{with_adapter}://{bucket}/{ENV_ID}/cli_cp_file/"
    assert runner.invoke(app, ["cp", str(source), destination]).exit_code == 0
    assert Pathy(f"{destination}file.txt").is_file()
    source.unlink()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_cp_folder(with_adapter: str, bucket: str) -> None:
    root = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/")
    source = root / "cli_cp_folder"
    destination = root / "cli_cp_folder_other"
    for i in range(2):
        for j in range(2):
            (source / f"{i}" / f"{j}").write_text("---")
    assert runner.invoke(app, ["cp", str(source), str(destination)]).exit_code == 0
    assert Pathy(source).exists()
    assert Pathy(destination).is_dir()
    for i in range(2):
        for j in range(2):
            assert (destination / f"{i}" / f"{j}").is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_mv_folder(with_adapter: str, bucket: str) -> None:
    root = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/")
    source = root / "cli_mv_folder"
    destination = root / "cli_mv_folder_other"
    for i in range(2):
        for j in range(2):
            (source / f"{i}" / f"{j}").write_text("---")
    assert runner.invoke(app, ["mv", str(source), str(destination)]).exit_code == 0
    assert not Pathy(source).exists()
    assert Pathy(destination).is_dir()
    # Ensure source files are gone
    for i in range(2):
        for j in range(2):
            assert not (source / f"{i}" / f"{j}").is_file()
    # And dest files exist
    for i in range(2):
        for j in range(2):
            assert (destination / f"{i}" / f"{j}").is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_mv_file_copy_from_name(with_adapter: str, bucket: str) -> None:
    source = pathlib.Path("./file.txt")
    source.touch()
    destination = f"{with_adapter}://{bucket}/{ENV_ID}/cli_cp_file/"
    assert runner.invoke(app, ["mv", str(source), destination]).exit_code == 0
    assert Pathy(f"{destination}file.txt").is_file()
    # unlink should happen from the operation
    assert not source.exists()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_mv_file(with_adapter: str, bucket: str) -> None:
    source = f"{with_adapter}://{bucket}/{ENV_ID}/cli_mv_file/file.txt"
    destination = f"{with_adapter}://{bucket}/{ENV_ID}/cli_mv_file/other.txt"
    Pathy(source).write_text("---")
    assert Pathy(source).exists()
    assert runner.invoke(app, ["mv", source, destination]).exit_code == 0
    assert not Pathy(source).exists()
    assert Pathy(destination).is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_mv_file_across_buckets(
    with_adapter: str, bucket: str, other_bucket: str
) -> None:
    source = f"{with_adapter}://{bucket}/{ENV_ID}/cli_mv_file_across_buckets/file.txt"
    destination = (
        f"{with_adapter}://{other_bucket}/{ENV_ID}/cli_mv_file_across_buckets/other.txt"
    )
    Pathy(source).write_text("---")
    assert Pathy(source).exists()
    assert runner.invoke(app, ["mv", source, destination]).exit_code == 0
    assert not Pathy(source).exists()
    assert Pathy(destination).is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_mv_folder_across_buckets(
    with_adapter: str, bucket: str, other_bucket: str
) -> None:
    source = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/cli_mv_folder_across_buckets")
    destination = Pathy(
        f"{with_adapter}://{other_bucket}/{ENV_ID}/cli_mv_folder_across_buckets"
    )
    for i in range(2):
        for j in range(2):
            (source / f"{i}" / f"{j}").write_text("---")
    assert runner.invoke(app, ["mv", str(source), str(destination)]).exit_code == 0
    assert not Pathy(source).exists()
    assert Pathy(destination).is_dir()
    # Ensure source files are gone
    for i in range(2):
        for j in range(2):
            assert not (source / f"{i}" / f"{j}").is_file()
    # And dest files exist
    for i in range(2):
        for j in range(2):
            assert (destination / f"{i}" / f"{j}").is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_rm_invalid_file(with_adapter: str, bucket: str) -> None:
    source = f"{with_adapter}://{bucket}/{ENV_ID}/cli_rm_file_invalid/file.txt"
    path = Pathy(source)
    assert not path.exists()
    assert runner.invoke(app, ["rm", source]).exit_code == 1


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_rm_file(with_adapter: str, bucket: str) -> None:
    source = f"{with_adapter}://{bucket}/{ENV_ID}/cli_rm_file/file.txt"
    path = Pathy(source)
    path.write_text("---")
    assert path.exists()
    assert runner.invoke(app, ["rm", source]).exit_code == 0
    assert not path.exists()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_rm_verbose(with_adapter: str, bucket: str) -> None:
    root = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/") / "cli_rm_folder"
    source = str(root / "file.txt")
    other = str(root / "folder/other")
    Pathy(source).write_text("---")
    Pathy(other).write_text("---")
    result = runner.invoke(app, ["rm", "-v", source])
    assert result.exit_code == 0
    assert source in result.output
    assert other not in result.output

    Pathy(source).write_text("---")
    result = runner.invoke(app, ["rm", "-rv", str(root)])
    assert result.exit_code == 0
    assert source in result.output
    assert other in result.output


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_rm_folder(with_adapter: str, bucket: str) -> None:
    root = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/")
    source = root / "cli_rm_folder"
    for i in range(2):
        for j in range(2):
            (source / f"{i}" / f"{j}").write_text("---")

    # Returns exit code 1 without recursive flag when given a folder
    assert runner.invoke(app, ["rm", str(source)]).exit_code == 1
    assert runner.invoke(app, ["rm", "-r", str(source)]).exit_code == 0
    assert not Pathy(source).exists()
    # Ensure source files are gone
    for i in range(2):
        for j in range(2):
            assert not (source / f"{i}" / f"{j}").is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_ls_invalid_source(with_adapter: str, bucket: str) -> None:
    root = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/") / "cli_ls_invalid"
    three = str(root / "folder/file.txt")

    result = runner.invoke(app, ["ls", str(three)])
    assert result.exit_code == 1
    assert "No such file or directory" in result.output


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_ls(with_adapter: str, bucket: str) -> None:
    root = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/") / "cli_ls"
    one = str(root / "file.txt")
    two = str(root / "other.txt")
    three = str(root / "folder/file.txt")
    Pathy(one).write_text("---")
    Pathy(two).write_text("---")
    Pathy(three).write_text("---")

    result = runner.invoke(app, ["ls", str(root)])
    assert result.exit_code == 0
    assert one in result.output
    assert two in result.output
    assert str(root / "folder") in result.output

    result = runner.invoke(app, ["ls", "-l", str(root)])
    assert result.exit_code == 0
    assert one in result.output
    assert two in result.output
    assert str(root / "folder") in result.output


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_cli_ls_local_files(with_adapter: str, bucket: str) -> None:
    root = Pathy.fluid(tempfile.mkdtemp()) / ENV_ID / "ls"
    root.mkdir(parents=True, exist_ok=True)
    for i in range(3):
        (root / f"file_{i}").write_text("NICE")
    files = list(root.ls())
    assert len(files) == 3
    valid_names = [f"file_{i}" for i in range(len(files))]
    for i, blob_stat in enumerate(files):
        assert blob_stat.name in valid_names
        assert blob_stat.size == 4
        assert blob_stat.last_modified is not None

    root = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/") / "cli_ls"
    one = str(root / "file.txt")
    two = str(root / "other.txt")
    three = str(root / "folder/file.txt")
    Pathy(one).write_text("---")
    Pathy(two).write_text("---")
    Pathy(three).write_text("---")

    result = runner.invoke(app, ["ls", str(root)])
    assert result.exit_code == 0
    assert one in result.output
    assert two in result.output
    assert str(root / "folder") in result.output

    result = runner.invoke(app, ["ls", "-l", str(root)])
    assert result.exit_code == 0
    assert one in result.output
    assert two in result.output
    assert str(root / "folder") in result.output


@pytest.mark.parametrize("adapter", ["fs"])
def test_cli_ls_diff_years_modified(with_adapter: str, bucket: str) -> None:
    import os

    root = Pathy(f"{with_adapter}://{bucket}") / ENV_ID / "ls_diff_year"
    root.mkdir(parents=True, exist_ok=True)

    client: BucketClientFS = root.client(root)  # type:ignore

    # Create one file right now
    new_path = root / "new_file.txt"
    new_path.write_text("new")

    # Create another and set its modified time to one year before now
    old_path = root / "old_file.txt"
    old_path.write_text("old")
    old_stat = old_path.stat()
    assert isinstance(old_stat, BlobStat)
    one_year = 31556926  # seconds
    assert old_stat.last_modified is not None

    local_file = str(client.full_path(old_path))
    os.utime(
        local_file,
        (old_stat.last_modified - one_year, old_stat.last_modified - one_year),
    )
    new_old_stat = old_path.stat()
    assert new_old_stat.last_modified is not None
    assert isinstance(new_old_stat, BlobStat)
    assert int(old_stat.last_modified) == int(new_old_stat.last_modified + one_year)

    result = runner.invoke(app, ["ls", "-l", str(root)])
    assert result.exit_code == 0
    assert str(old_path) in result.output
    assert str(new_path) in result.output

    root.rmdir()
