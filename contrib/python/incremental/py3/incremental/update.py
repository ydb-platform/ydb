# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

from __future__ import absolute_import, division, print_function

import click
import os
import datetime
from typing import Dict, Optional, Callable

from incremental import Version, _findPath, _existing_version

_VERSIONPY_TEMPLATE = '''"""
Provides {package} version information.
"""

# This file is auto-generated! Do not edit!
# Use `python -m incremental.update {package}` to change this file.

from incremental import Version

__version__ = {version_repr}
__all__ = ["__version__"]
'''

_YEAR_START = 2000


def _run(
    package,  # type: str
    path,  # type: Optional[str]
    newversion,  # type: Optional[str]
    patch,  # type: bool
    rc,  # type: bool
    post,  # type: bool
    dev,  # type: bool
    create,  # type: bool
    _date=None,  # type: Optional[datetime.date]
    _getcwd=None,  # type: Optional[Callable[[], str]]
    _print=print,  # type: Callable[[object], object]
):  # type: (...) -> None
    if not _getcwd:
        _getcwd = os.getcwd

    if not _date:
        _date = datetime.date.today()

    if not path:
        path = _findPath(_getcwd(), package)

    if (
        newversion
        and patch
        or newversion
        and dev
        or newversion
        and rc
        or newversion
        and post
    ):
        raise ValueError("Only give --newversion")

    if dev and patch or dev and rc or dev and post:
        raise ValueError("Only give --dev")

    if (
        create
        and dev
        or create
        and patch
        or create
        and rc
        or create
        and post
        or create
        and newversion
    ):
        raise ValueError("Only give --create")

    versionpath = os.path.join(path, "_version.py")
    if newversion:
        from pkg_resources import parse_version

        existing = _existing_version(versionpath)
        st_version = parse_version(newversion)._version  # type: ignore[attr-defined]

        release = list(st_version.release)

        minor = 0
        micro = 0
        if len(release) == 1:
            (major,) = release
        elif len(release) == 2:
            major, minor = release
        else:
            major, minor, micro = release

        v = Version(
            package,
            major,
            minor,
            micro,
            release_candidate=st_version.pre[1] if st_version.pre else None,
            post=st_version.post[1] if st_version.post else None,
            dev=st_version.dev[1] if st_version.dev else None,
        )

    elif create:
        v = Version(package, _date.year - _YEAR_START, _date.month, 0)
        existing = v

    elif rc and not patch:
        existing = _existing_version(versionpath)

        if existing.release_candidate:
            v = Version(
                package,
                existing.major,
                existing.minor,
                existing.micro,
                existing.release_candidate + 1,
            )
        else:
            v = Version(package, _date.year - _YEAR_START, _date.month, 0, 1)

    elif patch:
        existing = _existing_version(versionpath)
        v = Version(
            package,
            existing.major,
            existing.minor,
            existing.micro + 1,
            1 if rc else None,
        )

    elif post:
        existing = _existing_version(versionpath)

        if existing.post is None:
            _post = 0
        else:
            _post = existing.post + 1

        v = Version(package, existing.major, existing.minor, existing.micro, post=_post)

    elif dev:
        existing = _existing_version(versionpath)

        if existing.dev is None:
            _dev = 0
        else:
            _dev = existing.dev + 1

        v = Version(
            package,
            existing.major,
            existing.minor,
            existing.micro,
            existing.release_candidate,
            dev=_dev,
        )

    else:
        existing = _existing_version(versionpath)

        if existing.release_candidate:
            v = Version(package, existing.major, existing.minor, existing.micro)
        else:
            raise ValueError("You need to issue a rc before updating the major/minor")

    NEXT_repr = repr(Version(package, "NEXT", 0, 0)).split("#")[0].replace("'", '"')
    NEXT_repr_bytes = NEXT_repr.encode("utf8")

    version_repr = repr(v).split("#")[0].replace("'", '"')
    version_repr_bytes = version_repr.encode("utf8")

    existing_version_repr = repr(existing).split("#")[0].replace("'", '"')
    existing_version_repr_bytes = existing_version_repr.encode("utf8")

    _print("Updating codebase to %s" % (v.public()))

    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            with open(filepath, "rb") as f:
                original_content = f.read()
            content = original_content

            # Replace previous release_candidate calls to the new one
            if existing.release_candidate:
                content = content.replace(
                    existing_version_repr_bytes, version_repr_bytes
                )
                content = content.replace(
                    (package.encode("utf8") + b" " + existing.public().encode("utf8")),
                    (package.encode("utf8") + b" " + v.public().encode("utf8")),
                )

            # Replace NEXT Version calls with the new one
            content = content.replace(NEXT_repr_bytes, version_repr_bytes)
            content = content.replace(
                NEXT_repr_bytes.replace(b"'", b'"'), version_repr_bytes
            )

            # Replace <package> NEXT with <package> <public>
            content = content.replace(
                package.encode("utf8") + b" NEXT",
                (package.encode("utf8") + b" " + v.public().encode("utf8")),
            )

            if content != original_content:
                _print("Updating %s" % (filepath,))
                with open(filepath, "wb") as f:
                    f.write(content)

    _print("Updating %s" % (versionpath,))
    with open(versionpath, "wb") as f:
        f.write(
            (
                _VERSIONPY_TEMPLATE.format(package=package, version_repr=version_repr)
            ).encode("utf8")
        )


@click.command()
@click.argument("package")
@click.option("--path", default=None)
@click.option("--newversion", default=None)
@click.option("--patch", is_flag=True)
@click.option("--rc", is_flag=True)
@click.option("--post", is_flag=True)
@click.option("--dev", is_flag=True)
@click.option("--create", is_flag=True)
def run(
    package,  # type: str
    path,  # type: Optional[str]
    newversion,  # type: Optional[str]
    patch,  # type: bool
    rc,  # type: bool
    post,  # type: bool
    dev,  # type: bool
    create,  # type: bool
):  # type: (...) -> None
    return _run(
        package=package,
        path=path,
        newversion=newversion,
        patch=patch,
        rc=rc,
        post=post,
        dev=dev,
        create=create,
    )


if __name__ == "__main__":  # pragma: no cover
    run()
