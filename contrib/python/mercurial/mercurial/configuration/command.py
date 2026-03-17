# Gather code related to command dealing with configuration.

from __future__ import annotations

import os

from typing import Any, Collection

from ..i18n import _

from .. import (
    cmdutil,
    error,
    formatter,
    pycompat,
    requirements,
    ui as uimod,
    util,
)

from . import (
    ConfigLevelT,
    EDIT_LEVELS,
    LEVEL_SHARED,
    NO_REPO_EDIT_LEVELS,
    rcutil,
)

EDIT_FLAG = 'edit'


def find_edit_level(
    ui: uimod.ui,
    repo,
    opts: dict[str, Any],
) -> ConfigLevelT | None:
    """return the level we should edit, if any.

    Parse the command option to detect when an edit is requested, and if so the
    configuration level we should edit.
    """
    if opts.get(EDIT_FLAG) or any(opts.get(o) for o in EDIT_LEVELS):
        cmdutil.check_at_most_one_arg(opts, *EDIT_LEVELS)
        for level in EDIT_LEVELS:
            if opts.get(level):
                return level
        return EDIT_LEVELS[0]
    return None


def edit_config(ui: uimod.ui, repo, level: ConfigLevelT) -> None:
    """let the user edit configuration file for the given level"""

    # validate input
    if repo is None and level not in NO_REPO_EDIT_LEVELS:
        msg = b"can't use --%s outside a repository" % pycompat.bytestr(level)
        raise error.InputError(_(msg))
    if level == LEVEL_SHARED:
        if not repo.shared():
            msg = _(b"repository is not shared; can't use --shared")
            raise error.InputError(msg)
        if requirements.SHARESAFE_REQUIREMENT not in repo.requirements:
            raise error.InputError(
                _(
                    b"share safe feature not enabled; "
                    b"unable to edit shared source repository config"
                )
            )

    # find rc files paths
    repo_path = None
    if repo is not None:
        repo_path = repo.root
    all_rcs = rcutil.all_rc_components(repo_path)
    rc_by_level = {}
    for lvl, rc_type, values in all_rcs:
        if rc_type != b'path':
            continue
        rc_by_level.setdefault(lvl, []).append(values)

    if level not in rc_by_level:
        msg = 'unknown config level: %s' % level
        raise error.ProgrammingError(msg)

    paths = rc_by_level[level]
    for f in paths:
        if os.path.exists(f):
            break
    else:
        samplehgrc = uimod.samplehgrcs.get(level)

        f = paths[0]
        if samplehgrc is not None:
            util.writefile(f, util.tonativeeol(samplehgrc))

    editor = ui.geteditor()
    ui.system(
        b"%s \"%s\"" % (editor, f),
        onerr=error.InputError,
        errprefix=_(b"edit failed"),
        blockedtag=b'config_edit',
    )


def show_component(ui: uimod.ui, repo) -> None:
    """show the component used to build the config

    XXX this skip over various source and ignore the repository config, so it
    XXX is probably useless old code.
    """
    for _lvl, t, f in rcutil.rccomponents():
        if t == b'path':
            ui.debug(b'read config from: %s\n' % f)
        elif t == b'resource':
            ui.debug(b'read config from: resource:%s.%s\n' % (f[0], f[1]))
        elif t == b'items':
            # Don't print anything for 'items'.
            pass
        else:
            raise error.ProgrammingError(b'unknown rctype: %s' % t)


def show_config(
    ui: uimod.ui,
    repo,
    value_filters: Collection[bytes],
    formatter_options: dict,
    untrusted: bool = False,
    all_known: bool = False,
    show_source: bool = False,
) -> bool:
    """Display config value to the user

    The display is done using a dedicated `formatter` object.


    :value_filters:
        if non-empty filter the display value according to these filters. If
        the filter does not match any value, the function return False. True
        otherwise.

    :formatter_option:
        options passed to the formatter

    :untrusted:
        When set, use untrusted value instead of ignoring them

    :all_known:
        Display all known config item, not just the one with an explicit value.

    :show_source:
        Show where each value has been defined.
    """
    fm = ui.formatter(b'config', formatter_options)
    selsections = selentries = []
    filtered = False
    if value_filters:
        selsections = [v for v in value_filters if b'.' not in v]
        selentries = [v for v in value_filters if b'.' in v]
        filtered = True
    uniquesel = len(selentries) == 1 and not selsections
    selsections = set(selsections)
    selentries = set(selentries)

    matched = False
    entries = ui.walkconfig(untrusted=untrusted, all_known=all_known)
    for section, name, value in entries:
        source = ui.configsource(section, name, untrusted)
        value = pycompat.bytestr(value)
        defaultvalue = ui.configdefault(section, name)
        if fm.isplain():
            source = source or b'none'
            value = value.replace(b'\n', b'\\n')
        entryname = section + b'.' + name
        if filtered and not (section in selsections or entryname in selentries):
            continue
        fm.startitem()
        fm.condwrite(show_source, b'source', b'%s: ', source)
        if uniquesel:
            fm.data(name=entryname)
            fm.write(b'value', b'%s\n', value)
        else:
            fm.write(b'name value', b'%s=%s\n', entryname, value)
        if formatter.isprintable(defaultvalue):
            fm.data(defaultvalue=defaultvalue)
        elif isinstance(defaultvalue, list) and all(
            formatter.isprintable(e) for e in defaultvalue
        ):
            fm.data(defaultvalue=fm.formatlist(defaultvalue, name=b'value'))
        # TODO: no idea how to process unsupported defaultvalue types
        matched = True
    fm.end()
    return matched
