#
# Copyright (C) 2007-2011 Edgewall Software, 2013-2025 the Babel team
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution. The terms
# are also available at https://github.com/python-babel/babel/blob/master/LICENSE.
#
# This software consists of voluntary contributions made by many
# individuals. For the exact contribution history, see the revision
# history and logs, available at https://github.com/python-babel/babel/commits/master/.

from __future__ import annotations

import re
import shlex
from functools import partial
from io import BytesIO, StringIO

import pytest

from babel.messages import Catalog, extract, frontend
from babel.messages.frontend import (
    CommandLineInterface,
    ExtractMessages,
    UpdateCatalog,
)
from babel.messages.pofile import write_po
from tests.messages.consts import project_dir
from tests.messages.utils import CUSTOM_EXTRACTOR_COOKIE

mapping_cfg = """
[extractors]
custom = tests.messages.utils:custom_extractor

# Special extractor for a given Python file
[custom: special.py]
treat = delicious

# Python source files
[python: **.py]

# Genshi templates
[genshi: **/templates/**.html]
include_attrs =

[genshi: **/templates/**.txt]
template_class = genshi.template:TextTemplate
encoding = latin-1

# Some custom extractor
[custom: **/custom/*.*]
"""

mapping_toml = """
[extractors]
custom = "tests.messages.utils:custom_extractor"

# Special extractor for a given Python file
[[mappings]]
method = "custom"
pattern = "special.py"
treat = "delightful"

# Python source files
[[mappings]]
method = "python"
pattern = "**.py"

# Genshi templates
[[mappings]]
method = "genshi"
pattern = "**/templates/**.html"
include_attrs = ""

[[mappings]]
method = "genshi"
pattern = "**/templates/**.txt"
template_class = "genshi.template:TextTemplate"
encoding = "latin-1"

# Some custom extractor
[[mappings]]
method = "custom"
pattern = "**/custom/*.*"
"""


@pytest.mark.parametrize(
    ("data", "parser", "preprocess", "is_toml"),
    [
        (
            mapping_cfg,
            frontend.parse_mapping_cfg,
            None,
            False,
        ),
        (
            mapping_toml,
            frontend._parse_mapping_toml,
            None,
            True,
        ),
        (
            mapping_toml,
            partial(frontend._parse_mapping_toml, style="pyproject.toml"),
            lambda s: re.sub(r"^(\[+)", r"\1tool.babel.", s, flags=re.MULTILINE),
            True,
        ),
    ],
    ids=("cfg", "toml", "pyproject-toml"),
)
def test_parse_mapping(data: str, parser, preprocess, is_toml):
    if preprocess:
        data = preprocess(data)
    if is_toml:
        buf = BytesIO(data.encode())
    else:
        buf = StringIO(data)

    method_map, options_map = parser(buf)
    assert len(method_map) == 5

    assert method_map[1] == ('**.py', 'python')
    assert options_map['**.py'] == {}
    assert method_map[2] == ('**/templates/**.html', 'genshi')
    assert options_map['**/templates/**.html']['include_attrs'] == ''
    assert method_map[3] == ('**/templates/**.txt', 'genshi')
    assert (options_map['**/templates/**.txt']['template_class']
            == 'genshi.template:TextTemplate')
    assert options_map['**/templates/**.txt']['encoding'] == 'latin-1'
    assert method_map[4] == ('**/custom/*.*', 'tests.messages.utils:custom_extractor')
    assert options_map['**/custom/*.*'] == {}


def test_parse_keywords():
    kw = frontend.parse_keywords(['_', 'dgettext:2', 'dngettext:2,3', 'pgettext:1c,2'])
    assert kw == {
        '_': None,
        'dgettext': (2,),
        'dngettext': (2, 3),
        'pgettext': ((1, 'c'), 2),
    }


def test_parse_keywords_with_t():
    kw = frontend.parse_keywords(['_:1', '_:2,2t', '_:2c,3,3t'])

    assert kw == {
        '_': {
            None: (1,),
            2: (2,),
            3: ((2, 'c'), 3),
        },
    }


def test_extract_messages_with_t():
    content = rb"""
_("1 arg, arg 1")
_("2 args, arg 1", "2 args, arg 2")
_("3 args, arg 1", "3 args, arg 2", "3 args, arg 3")
_("4 args, arg 1", "4 args, arg 2", "4 args, arg 3", "4 args, arg 4")
"""
    kw = frontend.parse_keywords(['_:1', '_:2,2t', '_:2c,3,3t'])
    result = list(extract.extract("python", BytesIO(content), kw))
    expected = [(2, '1 arg, arg 1', [], None),
                (3, '2 args, arg 1', [], None),
                (3, '2 args, arg 2', [], None),
                (4, '3 args, arg 1', [], None),
                (4, '3 args, arg 3', [], '3 args, arg 2'),
                (5, '4 args, arg 1', [], None)]
    assert result == expected


def configure_cli_command(cmdline: str | list[str]):
    """
    Helper to configure a command class, but not run it just yet.

    :param cmdline: The command line (sans the executable name)
    :return: Command instance
    """
    args = shlex.split(cmdline) if isinstance(cmdline, str) else list(cmdline)
    cli = CommandLineInterface()
    cmdinst = cli._configure_command(cmdname=args[0], argv=args[1:])
    return cmdinst


@pytest.mark.parametrize("split", (False, True))
@pytest.mark.parametrize("arg_name", ("-k", "--keyword", "--keywords"))
def test_extract_keyword_args_384(split, arg_name):
    # This is a regression test for https://github.com/python-babel/babel/issues/384
    # and it also tests that the rest of the forgotten aliases/shorthands implied by
    # https://github.com/python-babel/babel/issues/390 are re-remembered (or rather
    # that the mechanism for remembering them again works).

    kwarg_specs = [
        "gettext_noop",
        "gettext_lazy",
        "ngettext_lazy:1,2",
        "ugettext_noop",
        "ugettext_lazy",
        "ungettext_lazy:1,2",
        "pgettext_lazy:1c,2",
        "npgettext_lazy:1c,2,3",
    ]

    if split:  # Generate a command line with multiple -ks
        kwarg_text = " ".join(f"{arg_name} {kwarg_spec}" for kwarg_spec in kwarg_specs)
    else:  # Generate a single space-separated -k
        specs = ' '.join(kwarg_specs)
        kwarg_text = f'{arg_name} "{specs}"'

    # (Both of those invocation styles should be equivalent, so there is no parametrization from here on out)

    cmdinst = configure_cli_command(
        f"extract -F babel-django.cfg --add-comments Translators: -o django232.pot {kwarg_text} .",
    )
    assert isinstance(cmdinst, ExtractMessages)
    assert set(cmdinst.keywords.keys()) == {'_', 'dgettext', 'dngettext',
                                            'dnpgettext', 'dpgettext',
                                            'gettext', 'gettext_lazy',
                                            'gettext_noop', 'N_', 'ngettext',
                                            'ngettext_lazy', 'npgettext',
                                            'npgettext_lazy', 'pgettext',
                                            'pgettext_lazy', 'ugettext',
                                            'ugettext_lazy', 'ugettext_noop',
                                            'ungettext', 'ungettext_lazy'}


def test_update_catalog_boolean_args():
    cmdinst = configure_cli_command(
        "update --init-missing --no-wrap -N --ignore-obsolete --previous -i foo -o foo -l en")
    assert isinstance(cmdinst, UpdateCatalog)
    assert cmdinst.init_missing is True
    assert cmdinst.no_wrap is True
    assert cmdinst.no_fuzzy_matching is True
    assert cmdinst.ignore_obsolete is True
    assert cmdinst.previous is False  # Mutually exclusive with no_fuzzy_matching


def test_compile_catalog_dir(tmp_path):
    """
    Test that `compile` can compile all locales in a directory.
    """
    locales = ("fi_FI", "sv_SE")
    for locale in locales:
        l_dir = tmp_path / locale / "LC_MESSAGES"
        l_dir.mkdir(parents=True)
        po_file = l_dir / 'messages.po'
        po_file.write_text('msgid "foo"\nmsgstr "bar"\n')
    cmdinst = configure_cli_command([  # fmt: skip
        'compile',
        '--statistics',
        '--use-fuzzy',
        '-d', str(tmp_path),
    ])
    assert not cmdinst.run()
    for locale in locales:
        assert (tmp_path / locale / "LC_MESSAGES" / "messages.mo").exists()


def test_compile_catalog_explicit(tmp_path):
    """
    Test that `compile` can explicitly compile a single catalog.
    """
    po_file = tmp_path / 'temp.po'
    po_file.write_text('msgid "foo"\nmsgstr "bar"\n')
    mo_file = tmp_path / 'temp.mo'
    cmdinst = configure_cli_command([  # fmt: skip
        'compile',
        '--statistics',
        '--use-fuzzy',
        '-i', str(po_file),
        '-o', str(mo_file),
        '-l', 'fi_FI',
    ])
    assert not cmdinst.run()
    assert mo_file.exists()


@pytest.mark.parametrize("explicit_locale", (None, 'fi_FI'), ids=("implicit", "explicit"))
def test_update_dir(tmp_path, explicit_locale: bool):
    """
    Test that `update` can deal with directories too.
    """
    template = Catalog()
    template.add("1")
    template.add("2")
    template.add("3")
    tmpl_file = tmp_path / 'temp-template.pot'
    with tmpl_file.open("wb") as outfp:
        write_po(outfp, template)
    locales = ("fi_FI", "sv_SE")
    for locale in locales:
        l_dir = tmp_path / locale / "LC_MESSAGES"
        l_dir.mkdir(parents=True)
        po_file = l_dir / 'messages.po'
        po_file.touch()
    cmdinst = configure_cli_command([  # fmt: skip
        'update',
        '-i', str(tmpl_file),
        '-d', str(tmp_path),
        *(['-l', explicit_locale] if explicit_locale else []),
    ])
    assert not cmdinst.run()
    for locale in locales:
        if explicit_locale and locale != explicit_locale:
            continue
        assert (tmp_path / locale / "LC_MESSAGES" / "messages.po").stat().st_size > 0


def _test_extract_cli_knows_dash_s():
    # This is a regression test for https://github.com/python-babel/babel/issues/390
    cmdinst = configure_cli_command("extract -s -o foo babel")
    assert isinstance(cmdinst, ExtractMessages)
    assert cmdinst.strip_comments


def _test_extract_cli_knows_dash_dash_last_dash_translator():
    cmdinst = configure_cli_command('extract --last-translator "FULL NAME EMAIL@ADDRESS" -o foo babel')
    assert isinstance(cmdinst, ExtractMessages)
    assert cmdinst.last_translator == "FULL NAME EMAIL@ADDRESS"


def _test_extract_add_location():
    cmdinst = configure_cli_command("extract -o foo babel --add-location full")
    assert isinstance(cmdinst, ExtractMessages)
    assert cmdinst.add_location == 'full'
    assert not cmdinst.no_location
    assert cmdinst.include_lineno

    cmdinst = configure_cli_command("extract -o foo babel --add-location file")
    assert isinstance(cmdinst, ExtractMessages)
    assert cmdinst.add_location == 'file'
    assert not cmdinst.no_location
    assert not cmdinst.include_lineno

    cmdinst = configure_cli_command("extract -o foo babel --add-location never")
    assert isinstance(cmdinst, ExtractMessages)
    assert cmdinst.add_location == 'never'
    assert cmdinst.no_location


def _test_extract_error_code(monkeypatch, capsys):
    monkeypatch.chdir(project_dir)
    cmdinst = configure_cli_command("compile --domain=messages --directory i18n --locale fi_BUGGY")
    assert cmdinst.run() == 1
    out, err = capsys.readouterr()
    if err:
        assert "unknown named placeholder 'merkki'" in err


@pytest.mark.parametrize("with_underscore_ignore", (False, True))
def _test_extract_ignore_dirs(monkeypatch, capsys, tmp_path, with_underscore_ignore):
    pot_file = tmp_path / 'temp.pot'
    monkeypatch.chdir(project_dir)
    cmd = f"extract . -o '{pot_file}' --ignore-dirs '*ignored* .*' "
    if with_underscore_ignore:
        # This also tests that multiple arguments are supported.
        cmd += "--ignore-dirs '_*'"
    cmdinst = configure_cli_command(cmd)
    assert isinstance(cmdinst, ExtractMessages)
    assert cmdinst.directory_filter
    cmdinst.run()
    pot_content = pot_file.read_text()

    # The `ignored` directory is now actually ignored:
    assert 'this_wont_normally_be_here' not in pot_content

    # Since we manually set a filter, the otherwise `_hidden` directory is walked into,
    # unless we opt in to ignore it again
    assert ('ssshhh....' in pot_content) != with_underscore_ignore
    assert ('_hidden_by_default' in pot_content) != with_underscore_ignore


def _test_extract_header_comment(monkeypatch, tmp_path):
    pot_file = tmp_path / 'temp.pot'
    monkeypatch.chdir(project_dir)
    cmdinst = configure_cli_command(f"extract . -o '{pot_file}' --header-comment 'Boing' ")
    cmdinst.run()
    pot_content = pot_file.read_text()
    assert 'Boing' in pot_content


@pytest.mark.parametrize("mapping_format", ("toml", "cfg"))
def _test_pr_1121(tmp_path, monkeypatch, caplog, mapping_format):
    """
    Test that extraction uses the first matching method and options,
    instead of the first matching method and last matching options.

    Without the fix in PR #1121, this test would fail,
    since the `custom_extractor` isn't passed a delicious treat via
    the configuration.
    """
    if mapping_format == "cfg":
        mapping_file = (tmp_path / "mapping.cfg")
        mapping_file.write_text(mapping_cfg)
    else:
        mapping_file = (tmp_path / "mapping.toml")
        mapping_file.write_text(mapping_toml)
    (tmp_path / "special.py").write_text("# this file is special")
    pot_path = (tmp_path / "output.pot")
    monkeypatch.chdir(tmp_path)
    cmdinst = configure_cli_command(f"extract . -o {shlex.quote(str(pot_path))} --mapping {shlex.quote(mapping_file.name)}")
    assert isinstance(cmdinst, ExtractMessages)
    cmdinst.run()
    # If the custom extractor didn't run, we wouldn't see the cookie in there.
    assert CUSTOM_EXTRACTOR_COOKIE in pot_path.read_text()
