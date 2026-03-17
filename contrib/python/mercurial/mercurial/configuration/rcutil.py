# rcutil.py - utilities about config paths, special config sections etc.
#
#  Copyright Mercurial Contributors
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import os

from .. import (
    configuration as conf_mod,
    encoding,
    localrepo,
    pycompat,
    requirements as requirementsmod,
    util,
    vfs,
)

from ..utils import resourceutil

if pycompat.iswindows:
    from .. import scmwindows as scmplatform
else:
    from .. import scmposix as scmplatform

fallbackpager = scmplatform.fallbackpager
systemrcpath = scmplatform.systemrcpath
userrcpath = scmplatform.userrcpath

ComponentT = conf_mod.ComponentT
ConfigItemT = conf_mod.ConfigItemT
FileRCT = conf_mod.FileRCT
ResourceIDT = conf_mod.ResourceIDT


def _expandrcpath(path: bytes) -> list[FileRCT]:
    '''path could be a file or a directory. return a list of file paths'''
    p = util.expandpath(path)
    if os.path.isdir(p):
        join = os.path.join
        return sorted(
            join(p, f) for f, k in util.listdir(p) if f.endswith(b'.rc')
        )
    return [p]


def envrcitems(env: dict[bytes, bytes] | None = None) -> list[ConfigItemT]:
    """Return [(section, name, value, source)] config items.

    The config items are extracted from environment variables specified by env,
    used to override systemrc, but not userrc.

    If env is not provided, encoding.environ will be used.
    """
    if env is None:
        env = encoding.environ
    checklist = [
        (b'EDITOR', b'ui', b'editor'),
        (b'VISUAL', b'ui', b'editor'),
        (b'PAGER', b'pager', b'pager'),
    ]
    result = []
    for envname, section, configname in checklist:
        if envname not in env:
            continue
        result.append((section, configname, env[envname], b'$%s' % envname))
    return result


def default_rc_resources() -> list[ResourceIDT]:
    """return rc resource IDs in defaultrc"""
    rsrcs = resourceutil.contents(b'mercurial.defaultrc')
    return [
        (b'mercurial.defaultrc', r)
        for r in sorted(rsrcs)
        if resourceutil.is_resource(b'mercurial.defaultrc', r)
        and r.endswith(b'.rc')
    ]


def rccomponents(use_hgrcpath=True) -> list[ComponentT]:
    """return an ordered [(type, obj)] about where to load configs.

    respect $HGRCPATH. if $HGRCPATH is empty, only .hg/hgrc of current repo is
    used. if $HGRCPATH is not set, the platform default will be used. If
    `use_hgrcpath` is False, it is never used.

    if a directory is provided, *.rc files under it will be used.

    type could be either 'path', 'items' or 'resource'. If type is 'path',
    obj is a string, and is the config file path. if type is 'items', obj is a
    list of (section, name, value, source) that should fill the config directly.
    If type is 'resource', obj is a tuple of (package name, resource name).
    """
    envrc = (conf_mod.LEVEL_ENV_OVERWRITE, b'items', envrcitems())

    _rccomponents = []
    comp = _rccomponents.append

    if b'HGRCPATH' in encoding.environ and use_hgrcpath:
        # assume HGRCPATH is all about user configs so environments can be
        # overridden.
        comp(envrc)
        for p in encoding.environ[b'HGRCPATH'].split(pycompat.ospathsep):
            if not p:
                continue
            for p in _expandrcpath(p):
                comp((conf_mod.LEVEL_ENV_OVERWRITE, b'path', p))
    else:
        for r in default_rc_resources():
            comp((conf_mod.LEVEL_BUNDLED_RESOURCE, b'resource', r))

        for p in systemrcpath():
            comp((conf_mod.LEVEL_GLOBAL, b'path', os.path.normpath(p)))
        comp(envrc)
        for p in userrcpath():
            comp((conf_mod.LEVEL_USER, b'path', os.path.normpath(p)))
    return _rccomponents


def _shared_source_component(path: bytes) -> list[FileRCT]:
    """if the current repository is shared one, this tries to read
    .hg/hgrc of shared source if we are in share-safe mode

    This should be called before reading .hg/hgrc or the main repo
    as that overrides config set in shared source"""
    try:
        with open(os.path.join(path, b".hg", b"requires"), "rb") as fp:
            requirements = set(fp.read().splitlines())
            if not (
                requirementsmod.SHARESAFE_REQUIREMENT in requirements
                and requirementsmod.SHARED_REQUIREMENT in requirements
            ):
                return []
            hgvfs = vfs.vfs(os.path.join(path, b".hg"))
            sharedvfs = localrepo._getsharedvfs(hgvfs, requirements)
            return [sharedvfs.join(b"hgrc")]
    except OSError:
        pass
    return []


def repo_components(repo_path: bytes) -> list[ComponentT]:
    """return the list of config file to read for a repository"""
    components = []
    comp = components.append
    for p in _shared_source_component(repo_path):
        comp((conf_mod.LEVEL_SHARED, b'path', p))
    comp(
        (
            conf_mod.LEVEL_LOCAL,
            b'path',
            os.path.join(repo_path, b".hg", b"hgrc"),
        )
    )
    comp(
        (
            conf_mod.LEVEL_NON_SHARED,
            b'path',
            os.path.join(repo_path, b".hg", b"hgrc-not-shared"),
        )
    )
    return components


def all_rc_components(repo_path: bytes | None):
    components = []
    components.extend(rccomponents(use_hgrcpath=False))
    if repo_path is not None:
        components.extend(repo_components(repo_path))
    return components


def defaultpagerenv() -> dict[bytes, bytes]:
    """return a dict of default environment variables and their values,
    intended to be set before starting a pager.
    """
    return {b'LESS': b'FRX', b'LV': b'-c'}


def use_repo_hgrc() -> bool:
    """True if repositories `.hg/hgrc` config should be read"""
    return b'HGRCSKIPREPO' not in encoding.environ
