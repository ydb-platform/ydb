import pathlib
from pathlib import Path
from uuid import uuid4

import pytest

try:
    import spacy  # type:ignore

    has_spacy = bool(spacy)
except ModuleNotFoundError:
    has_spacy = False

from pathy import (
    BasePath,
    BlobStat,
    BucketClientFS,
    FluidPath,
    Pathy,
    clear_fs_cache,
    use_fs,
    use_fs_cache,
)
from .conftest import ENV_ID, TEST_ADAPTERS


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_is_path_instance(with_adapter: str) -> None:
    blob = Pathy(f"{with_adapter}://fake/blob")
    assert isinstance(blob, BasePath)


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_fluid(with_adapter: str, bucket: str) -> None:
    path: FluidPath = Pathy.fluid(f"{with_adapter}://{bucket}/{ENV_ID}/fake-key")
    assert isinstance(path, Pathy)
    path = Pathy.fluid("foo/bar.txt")
    assert isinstance(path, pathlib.Path)
    path = Pathy.fluid("/dev/null")
    assert isinstance(path, pathlib.Path)
    path = Pathy.fluid("C:\\Windows\\Path")
    assert isinstance(path, pathlib.Path)


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_path_to_local(with_adapter: str, bucket: str) -> None:
    root: Pathy = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/to_local")
    foo_blob: Pathy = root / "foo"
    foo_blob.write_text("---")
    assert isinstance(foo_blob, Pathy)
    use_fs_cache()

    # Cache a blob
    cached: Path = Pathy.to_local(foo_blob)
    second_cached: Path = Pathy.to_local(foo_blob)
    assert isinstance(cached, Path)
    assert cached.exists() and cached.is_file(), "local file should exist"
    assert second_cached == cached, "must be the same path"
    assert second_cached.stat() == cached.stat(), "must have the same stat"

    # Cache a folder hierarchy with blobs
    complex_folder = root / "complex"
    for i in range(3):
        folder = f"folder_{i}"
        for j in range(2):
            gcs_blob: Pathy = complex_folder / folder / f"file_{j}.txt"
            gcs_blob.write_text("---")

    cached_folder: Path = Pathy.to_local(complex_folder)
    assert isinstance(cached_folder, Path)
    assert cached_folder.exists() and cached_folder.is_dir()

    # Verify all the files exist in the file-system cache folder
    for i in range(3):
        folder = f"folder_{i}"
        for j in range(2):
            iter_blob: Path = cached_folder / folder / f"file_{j}.txt"
            assert iter_blob.exists()
            assert iter_blob.read_text() == "---"

    clear_fs_cache()
    assert not cached.exists(), "cache clear should delete file"


def test_pathy_buckets_rename_replace(temp_folder: Path) -> None:
    use_fs(temp_folder)
    Pathy.from_bucket("foo").mkdir()
    from_path = Pathy("gs://foo/bar")
    to_path = Pathy("gs://foo/baz")
    # Source foo/bar does not exist
    with pytest.raises(FileNotFoundError):
        from_path.rename(to_path)
    with pytest.raises(FileNotFoundError):
        from_path.replace(to_path)


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_stat(with_adapter: str, bucket: str) -> None:
    path = Pathy("fake-bucket-1234-0987/fake-key")
    with pytest.raises(ValueError):
        path.stat()
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/stat/foo.txt")
    path.write_text("a-a-a-a-a-a-a")
    stat = path.stat()
    assert isinstance(stat, BlobStat)
    assert stat.size is not None and stat.size > 0
    assert stat.last_modified is not None and stat.last_modified > 0
    with pytest.raises(ValueError):
        assert Pathy(f"{with_adapter}://{bucket}").stat()
    with pytest.raises(FileNotFoundError):
        assert Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/stat/nope.txt").stat()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_resolve(with_adapter: str, bucket: str) -> None:
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/fake-key")
    assert path.resolve() == path
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/dir/../fake-key")
    assert path.resolve() == Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/fake-key")


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_exists(with_adapter: str, bucket: str) -> None:
    path = Pathy("./fake-key")
    with pytest.raises(ValueError):
        path.exists()

    # invalid bucket name
    invalid_bucket = "unknown-bucket-name-123987519875419"
    assert Pathy(f"{with_adapter}://{invalid_bucket}").exists() is False
    assert Pathy(f"{with_adapter}://{invalid_bucket}/foo.blob").exists() is False
    # valid bucket with invalid object
    assert Pathy(f"{with_adapter}://{bucket}/not_found_lol_nice.txt").exists() is False
    assert Pathy(f"{with_adapter}://{bucket}/@^@^#$%@#$.txt").exists() is False

    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/directory/foo.txt")
    path.write_text("---")
    assert path.exists()
    for parent in path.parents:
        assert parent.exists()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_glob(with_adapter: str, bucket: str) -> None:
    base = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/")
    root = base / "glob/"
    for i in range(3):
        path = root / f"{i}.file"
        path.write_text("---")
    for i in range(2):
        path = root / f"{i}/dir/file.txt"
        path.write_text("---")

    assert list(root.glob("*.test")) == []
    blobs = sorted(list(root.glob("*.file")))
    assert blobs == [
        root / "0.file",
        root / "1.file",
        root / "2.file",
    ]
    assert list((root / "0").glob("*/*.txt")) == [root / "0/dir/file.txt"]
    assert sorted(base.glob("*lob/")) == [root]
    # Recursive matches
    assert sorted(list(root.glob("**/*.txt"))) == [
        root / "0/dir/file.txt",
        root / "1/dir/file.txt",
    ]
    # rglob adds the **/ for you
    assert sorted(list(root.rglob("*.txt"))) == [
        root / "0/dir/file.txt",
        root / "1/dir/file.txt",
    ]

    bad_root = Pathy(f"{with_adapter}://{bucket}-bad-name")
    assert list(bad_root.glob("*.txt")) == []

    bad_name = root / "@#$%@^@%@#@#$.glob"
    assert list(bad_name.glob("*.glob")) == []


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_unlink_path(with_adapter: str, bucket: str) -> None:
    root = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/unlink/")
    path = root / "404.txt"
    with pytest.raises(FileNotFoundError):
        path.unlink()
    path = root / "foo.txt"
    path.write_text("---")
    assert path.exists()
    path.unlink()
    assert not path.exists()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_is_dir(with_adapter: str, bucket: str) -> None:
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/is_dir/subfolder/another/my.file")
    path.write_text("---")
    assert path.is_dir() is False
    for parent in path.parents:
        assert parent.is_dir() is True


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_is_dir_bucket(with_adapter: str, bucket: str) -> None:
    path = Pathy(f"{with_adapter}://{bucket}")
    assert path.is_dir()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_is_dir_bucket_not_found(with_adapter: str) -> None:
    invalid_bucket = f"unknown-bucket-name-{uuid4().hex[:16]}"
    path = Pathy(f"{with_adapter}://{invalid_bucket}")
    assert path.is_dir() is False


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_is_file(with_adapter: str, bucket: str) -> None:
    path = Pathy(
        f"{with_adapter}://{bucket}/{ENV_ID}/is_file/subfolder/another/my.file"
    )
    path.write_text("---")
    # The full file is a file
    assert path.is_file() is True
    # Each parent node in the path is only a directory
    for parent in path.parents:
        assert parent.is_file() is False


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_iterdir(with_adapter: str, bucket: str) -> None:
    root = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/iterdir/")
    # (n) files in a folder
    for i in range(2):
        path = root / f"{i}.file"
        path.write_text("---")

    # 2 files in a subfolder
    (root / "sub/file.txt").write_text("---")
    (root / "sub/file2.txt").write_text("---")

    check = sorted(root.iterdir())
    assert check == [
        root / "0.file",
        root / "1.file",
        root / "sub",
    ]


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_iterdir_pipstore(with_adapter: str, bucket: str) -> None:
    path = Pathy(
        f"{with_adapter}://{bucket}/{ENV_ID}/iterdir_pipstore/prodigy/prodigy.whl"
    )
    path.write_bytes(b"---")
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/iterdir_pipstore")
    res = [e.name for e in sorted(path.iterdir())]
    assert res == ["prodigy"]


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_open_errors(with_adapter: str, bucket: str) -> None:
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/open_errors/file.txt")
    # Invalid open mode
    with pytest.raises(ValueError):
        path.open(mode="t")

    # Invalid buffering value
    with pytest.raises(ValueError):
        path.open(buffering=0)

    # Binary mode with encoding value
    with pytest.raises(ValueError):
        path.open(mode="rb", encoding="utf8")


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_open_for_read(with_adapter: str, bucket: str) -> None:
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/read/file.txt")
    path.write_text("---")
    with path.open() as file_obj:
        assert file_obj.read() == "---"
    assert path.read_text() == "---"


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_open_for_write(with_adapter: str, bucket: str) -> None:
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/write/file.txt")
    with path.open(mode="w") as file_obj:
        file_obj.write("---")
        file_obj.writelines(["---"])
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/write/file.txt")
    with path.open() as file_obj:
        assert file_obj.read() == "------"


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_open_binary_read(with_adapter: str, bucket: str) -> None:
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/read_binary/file.txt")
    path.write_bytes(b"---")
    with path.open(mode="rb") as file_obj:
        assert file_obj.readlines() == [b"---"]
    with path.open(mode="rb") as file_obj:
        assert file_obj.readline() == b"---"
        assert file_obj.readline() == b""


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_readwrite_text(with_adapter: str, bucket: str) -> None:
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/write_text/file.txt")
    path.write_text("---")
    with path.open() as file_obj:
        assert file_obj.read() == "---"
    assert path.read_text() == "---"


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_readwrite_bytes(with_adapter: str, bucket: str) -> None:
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/write_bytes/file.txt")
    path.write_bytes(b"---")
    assert path.read_bytes() == b"---"


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_readwrite_lines(with_adapter: str, bucket: str) -> None:
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/write_text/file.txt")
    with path.open("w") as file_obj:
        file_obj.writelines(["---"])
    with path.open("r") as file_obj:
        assert file_obj.readlines() == ["---"]
    with path.open("rt") as file_obj:
        assert file_obj.readline() == "---"


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_ls_blobs_with_stat(with_adapter: str, bucket: str) -> None:
    root = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/ls")
    for i in range(3):
        (root / f"file_{i}").write_text("NICE")
    files = list(root.ls())
    assert len(files) == 3
    valid_names = [f"file_{i}" for i in range(len(files))]
    for i, blob_stat in enumerate(files):
        assert blob_stat.name in valid_names
        assert blob_stat.size == 4
        assert blob_stat.last_modified is not None


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_owner(with_adapter: str, bucket: str) -> None:
    # Raises for invalid file
    with pytest.raises(FileNotFoundError):
        Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/write_text/not_a_valid_blob").owner()

    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/write_text/file.txt")
    path.write_text("---")
    # TODO: How to set file owner to non-None in GCS? Then assert here.
    #
    # NOTE: The owner is always set when using the filesystem adapter, so
    #       we can't assert the same behavior here until we fix the above
    #       todo comment.
    path.owner()
    # dumb assert means we didn't raise
    assert True


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_rename_files_in_bucket(with_adapter: str, bucket: str) -> None:
    # Rename a single file
    from_blob = f"{with_adapter}://{bucket}/{ENV_ID}/rename/file.txt"
    to_blob = f"{with_adapter}://{bucket}/{ENV_ID}/rename/other.txt"
    Pathy(from_blob).write_text("---")
    Pathy(from_blob).rename(to_blob)
    assert not Pathy(from_blob).exists()
    assert Pathy(to_blob).is_file()

    with pytest.raises(FileNotFoundError):
        Pathy(f"{with_adapter}://invlid_bkt_nme_18$%^@57582397/foo").rename(to_blob)


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_rename_files_across_buckets(
    with_adapter: str, bucket: str, other_bucket: str
) -> None:
    # Rename a single file across buckets
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rename/file.txt").write_text("---")
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rename/file.txt").rename(
        f"{with_adapter}://{other_bucket}/{ENV_ID}/rename/other.txt"
    )
    assert not Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rename/file.txt").exists()
    assert Pathy(f"{with_adapter}://{other_bucket}/{ENV_ID}/rename/other.txt").is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_rename_folders_in_bucket(with_adapter: str, bucket: str) -> None:
    # Rename a folder in the same bucket
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rename/folder/one.txt").write_text("---")
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rename/folder/two.txt").write_text("---")
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rename/folder/")
    new_path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rename/other/")
    path.rename(new_path)
    assert not path.exists()
    assert new_path.exists()
    assert Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rename/other/one.txt").is_file()
    assert Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rename/other/two.txt").is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_rename_folders_across_buckets(
    with_adapter: str, bucket: str, other_bucket: str
) -> None:
    # Rename a folder across buckets
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rename/folder/one.txt").write_text("---")
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rename/folder/two.txt").write_text("---")
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rename/folder/")
    new_path = Pathy(f"{with_adapter}://{other_bucket}/{ENV_ID}/rename/other/")
    path.rename(new_path)
    assert not path.exists()
    assert new_path.exists()
    assert Pathy(
        f"{with_adapter}://{other_bucket}/{ENV_ID}/rename/other/one.txt"
    ).is_file()
    assert Pathy(
        f"{with_adapter}://{other_bucket}/{ENV_ID}/rename/other/two.txt"
    ).is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_replace_files_in_bucket(with_adapter: str, bucket: str) -> None:
    # replace a single file
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/file.txt").write_text("---")
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/file.txt").replace(
        f"{with_adapter}://{bucket}/{ENV_ID}/replace/other.txt"
    )
    assert not Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/file.txt").exists()
    assert Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/other.txt").is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_replace_files_across_buckets(
    with_adapter: str, bucket: str, other_bucket: str
) -> None:
    # Rename a single file across buckets
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/file.txt").write_text("---")
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/file.txt").replace(
        f"{with_adapter}://{other_bucket}/{ENV_ID}/replace/other.txt"
    )
    assert not Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/file.txt").exists()
    assert Pathy(
        f"{with_adapter}://{other_bucket}/{ENV_ID}/replace/other.txt"
    ).is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_replace_folders_in_bucket(with_adapter: str, bucket: str) -> None:
    # Rename a folder in the same bucket
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/folder/one.txt").write_text(
        "---"
    )
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/folder/two.txt").write_text(
        "---"
    )
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/folder/")
    new_path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/other/")
    path.replace(new_path)
    assert not path.exists()
    assert new_path.exists()
    assert Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/other/one.txt").is_file()
    assert Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/other/two.txt").is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_replace_folders_across_buckets(
    with_adapter: str, bucket: str, other_bucket: str
) -> None:
    # Rename a folder across buckets
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/folder/one.txt").write_text(
        "---"
    )
    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/folder/two.txt").write_text(
        "---"
    )
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/replace/folder/")
    new_path = Pathy(f"{with_adapter}://{other_bucket}/{ENV_ID}/replace/other/")
    path.replace(new_path)
    assert not path.exists()
    assert new_path.exists()
    assert Pathy(
        f"{with_adapter}://{other_bucket}/{ENV_ID}/replace/other/one.txt"
    ).is_file()
    assert Pathy(
        f"{with_adapter}://{other_bucket}/{ENV_ID}/replace/other/two.txt"
    ).is_file()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_rmdir(with_adapter: str, bucket: str) -> None:
    blob = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rmdir/one.txt")
    blob.write_text("---")

    # Cannot rmdir a blob
    with pytest.raises(NotADirectoryError):
        blob.rmdir()

    Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rmdir/folder/two.txt").write_text("---")
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rmdir/")
    path.rmdir()
    assert not Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rmdir/one.txt").is_file()
    assert not Pathy(
        f"{with_adapter}://{bucket}/{ENV_ID}/rmdir/other/two.txt"
    ).is_file()
    assert not path.exists()

    # Cannot rmdir an invalid folder
    with pytest.raises(FileNotFoundError):
        path.rmdir()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_samefile(with_adapter: str, bucket: str) -> None:
    blob_str = f"{with_adapter}://{bucket}/{ENV_ID}/samefile/one.txt"
    blob_one = Pathy(blob_str)
    blob_two = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/samefile/two.txt")
    blob_one.touch()
    blob_two.touch()
    assert blob_one.samefile(blob_two) is False

    # accepts a Path-like object
    assert blob_one.samefile(blob_one) is True
    # accepts a str
    assert blob_one.samefile(blob_str) is True


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_touch(with_adapter: str, bucket: str) -> None:
    blob = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/touch/one.txt")
    if blob.is_file():
        blob.unlink()
    # The blob doesn't exist
    assert blob.is_file() is False
    # Touch creates an empty text blob
    blob.touch()
    # Now it exists
    assert blob.is_file() is True

    # Can't touch an existing blob if exist_ok=False
    with pytest.raises(FileExistsError):
        blob.touch(exist_ok=False)

    # Can touch an existing blob by default (no-op)
    blob.touch()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_rglob_unlink(with_adapter: str, bucket: str) -> None:
    files = [
        f"{with_adapter}://{bucket}/{ENV_ID}/rglob_and_unlink/{i}.file.txt"
        for i in range(3)
    ]
    for file in files:
        Pathy(file).write_text("---")
    path = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/rglob_and_unlink/")
    for blob in path.rglob("*"):
        blob.unlink()
    # All the files are gone
    for file in files:
        assert Pathy(file).exists() is False
    # The folder is gone
    assert not path.exists()


@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_mkdir(with_adapter: str) -> None:
    bucket_name = f"pathy-e2e-test-{uuid4().hex}"
    # Create a bucket
    path = Pathy(f"{with_adapter}://{bucket_name}/")
    path.mkdir()
    assert path.exists()
    # Does not assert if it already exists
    path.mkdir(exist_ok=True)
    with pytest.raises(FileExistsError):
        path.mkdir(exist_ok=False)
    assert path.exists()
    path.rmdir()
    assert not path.exists()


@pytest.mark.skip
@pytest.mark.parametrize("adapter", TEST_ADAPTERS)
def test_pathy_ignore_extension(with_adapter: str, bucket: str) -> None:
    """The smart_open library does automatic decompression based
    on the filename. We disable that to avoid errors, e.g. if you
    have a .tar.gz file that isn't gzipped."""
    import yatest.common
    not_targz = Pathy(f"{with_adapter}://{bucket}/{ENV_ID}/ignore_ext/one.tar.gz")
    fixture_tar = Path(yatest.common.source_path("contrib/python/pathy/pathy/_tests")) / "fixtures" / "tar_but_not_gzipped.tar.gz"
    not_targz.write_bytes(fixture_tar.read_bytes())
    again = not_targz.read_bytes()
    assert again is not None


def test_pathy_raises_with_no_known_bucket_clients_for_a_scheme(
    temp_folder: Path,
) -> None:
    path = Pathy("foo://foo")
    with pytest.raises(ValueError):
        path.client(path)
    # Setting a fallback FS adapter fixes the problem
    use_fs(str(temp_folder))
    assert isinstance(path.client(path), BucketClientFS)
