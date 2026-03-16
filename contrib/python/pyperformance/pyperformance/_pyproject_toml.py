# This module should be replaced with the equivalent functionality
# in the PyPI "packaging" package (once it's added there).

__all__ = [
    "load_pyproject_toml",
    "parse_classifier",
    "parse_entry_point",
    "parse_person",
    "parse_pyproject_toml",
]


import os.path
import re
import urllib.parse

import packaging.requirements
import packaging.specifiers
import packaging.utils
import packaging.version

try:
    import tomllib  # type: ignore[import-not-found] # tomllib doesn't exist on 3.7-3.10
except ImportError:
    import tomli as tomllib

from ._utils import check_name

NAME_RE = re.compile("^([A-Z0-9]|[A-Z0-9][A-Z0-9._-]*[A-Z0-9])$", re.IGNORECASE)


def parse_person(text):
    # XXX
    return text


def parse_classifier(text):
    # XXX Use https://pypi.org/project/packaging-classifiers.
    return text


def parse_entry_point(text):
    # See:
    #  * https://packaging.python.org/specifications/entry-points/#data-model
    #  * https://www.python.org/dev/peps/pep-0517/#source-trees
    module, sep, qualname = text.partition(":")
    if all(p.isidentifier() for p in module.split(".")):
        if not sep or all(p.isidentifier() for p in qualname.split(".")):
            return module, qualname

    raise ValueError(f"invalid entry point {text!r}")


def parse_pyproject_toml(
    text,
    rootdir,
    name=None,
    *,
    tools=None,
    requirefiles=True,
):
    data = tomllib.loads(text)
    unused = list(data)

    for section, normalize in SECTIONS.items():
        try:
            secdata = data[section]
        except KeyError:
            data[section] = None
        else:
            data[section] = normalize(
                secdata,
                name=name,
                tools=tools,
                rootdir=rootdir,
                requirefiles=requirefiles,
            )
            unused.remove(section)

    if unused:
        raise ValueError(f"unsupported sections ({', '.join(sorted(unused))})")

    return data


def load_pyproject_toml(filename, *, name=None, tools=None, requirefiles=True):
    if os.path.isdir(filename):
        rootdir = filename
        filename = os.path.join(rootdir, "pyproject.toml")
    else:
        rootdir = os.path.dirname(filename)

    with open(filename, encoding="utf-8") as infile:
        text = infile.read()
    data = parse_pyproject_toml(
        text,
        rootdir,
        name,
        tools=tools,
        requirefiles=requirefiles,
    )
    return data, filename


#######################################
# internal implementation


def _check_relfile(relname, rootdir, kind):
    if os.path.isabs(relname):
        raise ValueError(f"{relname!r} is absolute, expected relative")
    actual = os.path.join(rootdir, relname)
    if kind == "dir":
        if not os.path.isdir(actual):
            raise ValueError(f"directory {actual!r} does not exist")
    elif kind == "file":
        if not os.path.isfile(actual):
            raise ValueError(f"file {actual!r} does not exist")
    elif kind == "any":
        if not os.path.exists(actual):
            raise ValueError(f"{actual!r} does not exist")
    elif kind:
        raise NotImplementedError(kind)


def _check_file_or_text(table, rootdir, requirefiles, extra=None):
    unsupported = set(table) - set(["file", "text"]) - set(extra or ())
    if unsupported:
        raise ValueError(f"unsupported license data {table!r}")

    if "file" in table:
        if "text" in table:
            raise ValueError('"file" and "text" are mutually exclusive')
        kind = "file" if requirefiles else None
        _check_relfile(table["file"], rootdir, kind)


def _normalize_project(data, rootdir, name, requirefiles, **_ignored):
    # See PEP 621.
    unused = set(data)

    ##########
    # First handle the required fields.

    name = data.get("name", name)
    if name:
        if not NAME_RE.match(name):
            raise ValueError(f"invalid name {name!r}")
        name = packaging.utils.canonicalize_name(name)
        data["name"] = name
        if "name" in unused:
            unused.remove("name")
    else:
        if "name" not in data.get("dynamic", []):
            raise ValueError('missing required "name" field')

    try:
        version = data["version"]
    except KeyError:
        if "version" not in data.get("dynamic", []):
            raise ValueError('missing required "version" field')
    else:
        # We keep the full version string rather than
        # the canonicalized form.  However, we still validate and
        # (effectively) normalize it.
        version = packaging.version.parse(version)
        data["version"] = str(version)
        unused.remove("version")

    ##########
    # Now we handle the optional fields.

    # We leave "description" as-is.

    key = "readme"
    if key in data:
        readme = data[key]
        if isinstance(readme, str):
            readme = data[key] = {"file": readme}
        # XXX Check the suffix.
        # XXX Handle 'content-type'.
        # XXX Handle "charset" parameter.
        _check_file_or_text(
            data[key], rootdir, requirefiles, ["content-type", "charset"]
        )
        unused.remove(key)

    key = "requires-python"
    if key in data:
        # We keep it as a string.
        data[key] = str(packaging.specifiers.SpecifierSet(data[key]))
        unused.remove(key)

    key = "license"
    if key in data:
        _check_file_or_text(data[key], rootdir, requirefiles)
        unused.remove(key)

    key = "keywords"
    if key in data:
        for keyword in data[key]:
            # XXX Is this the right check?
            check_name(name, loose=True)
        unused.remove(key)

    key = "authors"
    if key in data:
        for person in data[key]:
            # We only make sure it is valid.
            parse_person(person)
        unused.remove(key)

    key = "maintainers"
    if key in data:
        for person in data[key]:
            # We only make sure it is valid.
            parse_person(person)
        unused.remove(key)

    key = "classifiers"
    if key in data:
        for classifier in data[key]:
            # We only make sure it is valid.
            parse_classifier(classifier)
        unused.remove(key)

    key = "dependencies"
    if key in data:
        for dep in data[key]:
            # We only make sure it is valid.
            packaging.requirements.Requirement(dep)
        unused.remove(key)

    key = "optional-dependencies"
    if key in data:
        # XXX
        unused.remove(key)

    key = "urls"
    if key in data:
        for name, url in data[key].items():
            # XXX Is there a stricter check?
            check_name(name, loose=True)
            # We only make sure it is valid.
            urllib.parse.urlparse(url)
        unused.remove(key)

    key = "scripts"
    if key in data:
        for name, value in data[key].items():
            # XXX Is there a stricter check?
            check_name(name, loose=True)
            # We only make sure it is valid.
            parse_entry_point(value)
        unused.remove(key)

    key = "gui-scripts"
    if key in data:
        for _, value in data[key].items():
            # XXX Is there a stricter check?
            check_name(name, loose=True)
            # We only make sure it is valid.
            parse_entry_point(value)
        unused.remove(key)

    key = "entry-points"
    if key in data:
        for groupname, group in data[key].items():
            # XXX Is there a stricter check?
            check_name(groupname, loose=True)
            for epname, value in group.items():
                # XXX Is there a stricter check?
                check_name(epname, loose=True)
                # We only make sure it is valid.
                parse_entry_point(value)
        unused.remove(key)

    key = "dynamic"
    if key in data:
        for field in data[key]:
            check_name(field, loose=True)
            # XXX Fail it isn't one of the supported fields.
        unused.remove(key)

    return data


def _normalize_build_system(data, rootdir, requirefiles, **_ignored):
    # See PEP 518 and 517.
    unused = set(data)

    key = "requires"
    if key in data:
        reqs = data[key]
        for i, raw in enumerate(reqs):
            # We only make sure it is valid.
            packaging.requirements.Requirement(raw)
        unused.remove(key)
    else:
        raise ValueError('missing "requires" field')

    key = "build-backend"
    if key in data:
        # We only make sure it is valid.
        parse_entry_point(data[key])
        unused.remove(key)

    key = "backend-path"
    if key in data:
        if "build-backend" not in data:
            raise ValueError('missing "build-backend" field')
        kind = "dir" if requirefiles else None
        for dirname in data[key]:
            _check_relfile(dirname, rootdir, kind=kind)
        unused.remove(key)

    if unused:
        raise ValueError(f"unsupported keys ({', '.join(sorted(unused))})")

    return data


def _normalize_tool(data, tools, rootdir, **_ignored):
    # See PEP 518.
    tools = tools or {}
    for name, tooldata in list(data.items()):
        if name in tools:
            normalize = tools[name]
            data[name] = normalize(name, tooldata, rootdir=rootdir)
            if data[name] is None:
                del data[name]
    return data


SECTIONS = {
    "project": _normalize_project,
    "build-system": _normalize_build_system,
    "tool": _normalize_tool,
}
