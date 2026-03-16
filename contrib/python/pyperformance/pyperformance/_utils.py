__all__ = [  # noqa: RUF022
    # filesystem
    "check_dir",
    "check_file",
    "temporary_file",
    # platform
    "MS_WINDOWS",
    # misc
    "check_name",
    "iter_clean_lines",
    "parse_name_pattern",
    "parse_selections",
    "parse_tag_pattern",
]


#######################################
# filesystem utils

import contextlib
import errno
import os
import os.path
import shlex
import shutil
import subprocess
import sys
import tempfile
import urllib.request


@contextlib.contextmanager
def temporary_file():
    tmp_filename = tempfile.mktemp()
    try:
        yield tmp_filename
    finally:
        try:
            os.unlink(tmp_filename)
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise


def check_file(filename):
    if not os.path.isabs(filename):
        raise ValueError(f"expected absolute path, got {filename!r}")
    if not os.path.isfile(filename):
        raise ValueError(f"file missing ({filename})")


def check_dir(dirname):
    if not os.path.isabs(dirname):
        raise ValueError(f"expected absolute path, got {dirname!r}")
    if not os.path.isdir(dirname):
        raise ValueError(f"directory missing ({dirname})")


def resolve_file(filename, relroot=None):
    resolved = os.path.normpath(filename)
    resolved = os.path.expanduser(resolved)
    # resolved = os.path.expandvars(filename)
    if not os.path.isabs(resolved):
        if not relroot:
            relroot = os.getcwd()
        elif not os.path.isabs(relroot):
            raise NotImplementedError(relroot)
        resolved = os.path.join(relroot, resolved)
    return resolved


def safe_rmtree(path):
    if not os.path.exists(path):
        return False

    print("Remove directory %s" % path)
    # XXX Pass onerror to report on any files that could not be deleted?
    shutil.rmtree(path)
    return True


#######################################
# platform utils


MS_WINDOWS = sys.platform == "win32"


def run_cmd(argv, *, env=None, capture=None, verbose=True):
    try:
        cmdstr = " ".join(shlex.quote(a) for a in argv)
    except TypeError:
        print(argv)
        raise  # re-raise

    if capture is True:
        capture = "both"
    kw = dict(
        env=env,
    )
    if capture == "both":
        kw.update(
            dict(
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        )
    elif capture == "combined":
        kw.update(
            dict(
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        )
    elif capture == "stdout":
        kw.update(
            dict(
                stdout=subprocess.PIPE,
            )
        )
    elif capture == "stderr":
        kw.update(
            dict(
                stderr=subprocess.PIPE,
            )
        )
    elif capture:
        raise NotImplementedError(repr(capture))
    if capture:
        kw.update(
            dict(
                encoding="utf-8",
            )
        )

    # XXX Use a logger.
    if verbose:
        print("#", cmdstr)

    # Explicitly flush standard streams, required if streams are buffered
    # (not TTY) to write lines in the expected order
    sys.stdout.flush()
    sys.stderr.flush()

    try:
        proc = subprocess.run(argv, **kw)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            if verbose:
                print("command failed (not found)")
            return 127, None, None
        raise
    if proc.returncode != 0 and verbose:
        print(f"Command failed with exit code {proc.returncode}")
    return proc.returncode, proc.stdout, proc.stderr


def run_python(*args, python=sys.executable, **kwargs):
    if not isinstance(python, str) and python is not None:
        try:
            # See _pythoninfo.get_info().
            python = python.sys.executable
        except AttributeError:
            raise TypeError(f"expected python str, got {python!r}")
    return run_cmd([python, *args], **kwargs)


#######################################
# network utils


def download(url, filename):
    response = urllib.request.urlopen(url)
    with response:
        content = response.read()

    with open(filename, "wb") as fp:
        fp.write(content)
        fp.flush()


#######################################
# misc utils


def check_name(name, *, loose=False, allownumeric=False):
    if not name or not isinstance(name, str):
        raise ValueError(f"bad name {name!r}")
    if allownumeric:
        name = f"_{name}"
    if not loose:
        if name.startswith("-"):
            raise ValueError(name)
        if not name.replace("-", "_").isidentifier():
            raise ValueError(name)


def parse_name_pattern(text, *, fail=True):
    name = text
    # XXX Support globs and/or regexes?  (return a callable)
    try:
        check_name("_" + name)
    except Exception:
        if fail:
            raise  # re-raise
        return None
    return name


def parse_tag_pattern(text):
    if not text.startswith("<"):
        return None
    if not text.endswith(">"):
        return None
    tag = text[1:-1]
    # XXX Support globs and/or regexes?  (return a callable)
    check_name(tag)
    return tag


def parse_selections(selections, parse_entry=None):
    if isinstance(selections, str):
        selections = selections.split(",")
    if parse_entry is None:

        def parse_entry(o, e):
            return (o, e, None, e)

    for entry in selections:
        entry = entry.strip()
        if not entry:
            continue

        op = "+"
        if entry.startswith("-"):
            op = "-"
            entry = entry[1:]

        yield parse_entry(op, entry)


def iter_clean_lines(filename):
    with open(filename, encoding="utf-8") as reqsfile:
        for line in reqsfile:
            # strip comment
            line = line.partition("#")[0]
            line = line.rstrip()
            if not line:
                continue
            yield line
