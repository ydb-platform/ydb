"""
Build a subset of Python's documentation using Sphinx
"""

import io
import os
from pathlib import Path
import shutil

import pyperf
from sphinx.cmd.build import main as sphinx_main


# Sphinx performs a lot of filesystem I/O when it operates. This can cause the
# results to be highly variable. Instead, we pre-load all of the source files
# and then monkeypatch "open" so that Sphinx is reading from in-memory
# `io.BytesIO` and `io.StringIO` objects.


DOC_ROOT = (Path(__file__).parent / "data" / "Doc").resolve()


_orig_open = open


preloaded_files = {}


def read_all_files():
    for filename in DOC_ROOT.glob("**/*"):
        if filename.is_file():
            preloaded_files[str(filename)] = filename.read_bytes()


def open(
    file,
    mode="r",
    buffering=-1,
    encoding=None,
    errors=None,
    newline=None,
    closefd=True,
    opener=None,
):
    if isinstance(file, Path):
        file = str(file)

    if isinstance(file, str):
        if "r" in mode and file in preloaded_files:
            if "b" in mode:
                return io.BytesIO(preloaded_files[file])
            else:
                return io.StringIO(preloaded_files[file].decode(encoding or "utf-8"))
        elif "w" in mode and DOC_ROOT in Path(file).parents:
            if "b" in mode:
                newfile = io.BytesIO()
            else:
                newfile = io.StringIO()
            preloaded_files[file] = newfile
            return newfile

    return _orig_open(
        file,
        mode=mode,
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
        closefd=closefd,
        opener=opener,
    )


__builtins__.open = open


def replace(src, dst):
    pass


os.replace = replace


def build_doc(doc_root):
    # Make sure there is no caching going on
    t0 = pyperf.perf_counter()
    sphinx_main(
        [
            "--builder",
            "dummy",
            "--doctree-dir",
            str(doc_root / "build" / "doctrees"),
            "--jobs",
            "1",
            "--silent",
            "--fresh-env",
            "--write-all",
            str(doc_root),
            str(doc_root / "build" / "html"),
        ]
    )
    return pyperf.perf_counter() - t0


def bench_sphinx(loops, doc_root):
    if (DOC_ROOT / "build").is_dir():
        shutil.rmtree(DOC_ROOT / "build")
    read_all_files()

    runs_total = 0
    for _ in range(loops):
        runs_total += build_doc(doc_root)
        if (DOC_ROOT / "build").is_dir():
            shutil.rmtree(DOC_ROOT / "build")

    return runs_total


if __name__ == "__main__":
    runner = pyperf.Runner()

    runner.metadata["description"] = (
        "Render documentation with Sphinx, like the CPython docs"
    )
    args = runner.parse_args()

    runner.bench_time_func("sphinx", bench_sphinx, DOC_ROOT)
