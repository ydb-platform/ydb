# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

# TODO(future) add bindings info (e.g. ctypesgen version, reference/generated, runtime libpaths)

__all__ = ("PYPDFIUM_INFO", "PDFIUM_INFO")

import json
import pkgutil
from pathlib import Path
import pypdfium2_raw


class _version_class:

    def __init__(self):
        try:
            with open(self._FILE, "r") as buf:
                data = json.load(buf)
        except (FileNotFoundError, OSError):
            raw = pkgutil.get_data(self._PKG, "version.json")
            if raw is None:
                raise
            data = json.loads(raw)
        for k, v in data.items():
            setattr(self, k, v)
        self.api_tag = tuple(data[k] for k in self._TAG_FIELDS)
        self._hook()
        self.version = self.tag + self.desc

    def __repr__(self):
        return self.version

    def _craft_tag(self):
        return ".".join(str(v) for v in self.api_tag)

    def _craft_desc(self, *suffixes):

        local_ver = []
        if self.n_commits > 0:
            local_ver += [str(self.n_commits), str(self.hash)]
        local_ver += suffixes

        desc = ""
        if local_ver:
            desc += "+" + ".".join(local_ver)
        return desc


class _version_pypdfium2 (_version_class):

    _FILE = Path(__file__).parent / "version.json"
    _PKG = "pypdfium2"
    _TAG_FIELDS = ("major", "minor", "patch")

    def _hook(self):

        self.tag = self._craft_tag()
        if self.beta is not None:
            self.tag += f"b{self.beta}"

        suffixes = ["dirty"] if self.dirty else []
        self.desc = self._craft_desc(*suffixes)
        if self.data_source != "git":
            self.desc += f":{self.data_source}"
        if self.is_editable:
            self.desc += "@editable"


class _version_pdfium (_version_class):

    _FILE = Path(pypdfium2_raw.__file__).parent / "version.json"
    _PKG = "pypdfium2_raw"
    _TAG_FIELDS = ("major", "minor", "build", "patch")

    def _hook(self):

        self.flags = tuple(self.flags)
        self.tag = self._craft_tag()

        self.desc = self._craft_desc()
        if self.flags:
            self.desc += f":{','.join(self.flags)}"
        if self.origin != "pdfium-binaries":
            self.desc += f"@{self.origin}"


PYPDFIUM_INFO = _version_pypdfium2()
"""
pypdfium2 helpers version.

It is suggesed to compare against *api_tag* and possibly also *beta* (see below).

Parameters:
    version (str):
        Joined tag and desc, forming the full version.
    tag (str):
        Version ciphers joined as str, including possible beta. Corresponds to the latest release tag at install time.
    desc (str):
        Non-cipher descriptors represented as str.
    api_tag (tuple[int]):
        Version ciphers joined as tuple, excluding possible beta.
    major (int):
        Major cipher.
    minor (int):
        Minor cipher.
    patch (int):
        Patch cipher.
    beta (int | None):
        Beta cipher, or None if not a beta version.
    n_commits (int):
        Number of commits after tag at install time. 0 for release.
    hash (str | None):
        Hash of head commit (prefixed with 'g') if n_commits > 0, None otherwise.
    dirty (bool):
        True if there were uncommitted changes at install time, False otherwise.
    data_source (str):
        Source of this version info. Possible values:\n
        - ``git``: Parsed from git describe. Always used if available. Highest accuracy.
        - ``given``: Pre-supplied version file (e.g. packaged with sdist, or else created by caller).
        - ``record``: Parsed from autorelease record. Implies that possible changes after tag are unknown.
    is_editable (bool | None):
        True for editable install, False otherwise. None if unknown.\n
        If True, the version info is the one captured at install time. An arbitrary number of forward or reverse changes may have happened since.
"""


PDFIUM_INFO = _version_pdfium()
"""
PDFium version.

It is suggesed to compare against *build* (see below).

Parameters:
    version (str):
        Joined tag and desc, forming the full version.
    tag (str):
        Version ciphers joined as string.
    desc (str):
        Descriptors (origin, flags) as string.
    api_tag (tuple[int]):
        Version ciphers grouped as tuple.
    major (int):
        Chromium major cipher.
    minor (int):
        Chromium minor cipher.
    build (int):
        Chromium/pdfium build cipher.
        This value uniquely identifies the pdfium version.
    patch (int):
        Chromium patch cipher.
    n_commits (int):
        Number of commits after tag at install time. 0 for tagged build commit.
    hash (str | None):
        Hash of head commit (prefixed with 'g') if n_commits > 0, None otherwise.
    origin (str):
        The pdfium binary's origin.
    flags (tuple[str]):
        Tuple of pdfium feature flags. Empty for default build. (V8, XFA) for pdfium-binaries V8 build.
"""

# Freeze the base class after we have constructed the instance objects
def _frozen_setattr(self, name, value):
    raise AttributeError(f"Version class is read-only - assignment '{name} = {value}' not allowed")
_version_class.__setattr__ = _frozen_setattr
