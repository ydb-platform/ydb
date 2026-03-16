__all__ = [
    "BenchmarksManifest",
    "load_manifest",
    "parse_manifest",
]


import os.path

from . import DATA_DIR, __version__, _benchmark, _utils

DEFAULTS_DIR = os.path.join(DATA_DIR, "benchmarks")
DEFAULT_MANIFEST = os.path.join(DEFAULTS_DIR, "MANIFEST")

BENCH_COLUMNS = ("name", "metafile")
BENCH_HEADER = "\t".join(BENCH_COLUMNS)


def load_manifest(filename, *, resolve=None):
    if not filename:
        filename = DEFAULT_MANIFEST
    filename = _utils.resolve_file(filename)
    sections = _parse_manifest_file(filename)
    return BenchmarksManifest._from_sections(sections, resolve, filename)


def parse_manifest(lines, *, resolve=None, filename=None):
    if isinstance(lines, str):
        lines = lines.splitlines()
    else:
        if not filename:
            # Try getting the filename from a file.
            filename = getattr(lines, "name", None)
    sections = _parse_manifest(lines, filename)
    return BenchmarksManifest._from_sections(sections, resolve, filename)


def resolve_default_benchmark(bench):
    if isinstance(bench, _benchmark.Benchmark):
        spec = bench.spec
    else:
        spec = bench
        bench = _benchmark.Benchmark(spec, "<bogus>")
        bench.metafile = None

    if not spec.version:
        spec = spec._replace(version=__version__)
    if not spec.origin:
        spec = spec._replace(origin="<default>")
    bench.spec = spec

    if not bench.metafile:
        metafile = os.path.join(DEFAULTS_DIR, f"bm_{bench.name}", "pyproject.toml")
        bench.metafile = metafile
    return bench


class BenchmarksManifest:
    @classmethod
    def _from_sections(cls, sections, resolve=None, filename=None):
        self = cls(filename=filename)
        self._add_sections(sections, resolve)
        return self

    def __init__(self, benchmarks=None, groups=None, filename=None):
        self._raw_benchmarks = []
        # XXX Support disabling all groups (except all and default)?
        self._raw_groups = {}
        self._raw_filename = filename
        self._byname = {}
        self._groups = None
        self._tags = None

        if benchmarks:
            self._add_benchmarks(benchmarks)
        if groups:
            self._add_groups(groups)

    def __repr__(self):
        args = (
            f"{n}={getattr(self, '_raw_' + n)}"
            for n in ("benchmarks", "groups", "filename")
        )
        return f"{type(self).__name__}({', '.join(args)})"

    @property
    def benchmarks(self):
        return list(self._byname.values())

    @property
    def groups(self):
        names = self._custom_groups()
        return names | {"all", "default"}

    @property
    def tags(self):
        return set(self._get_tags())

    @property
    def filename(self):
        return self._raw_filename

    def _add_sections(self, sections, resolve):
        seen_by_file = {}
        for filename, section, data in sections:
            try:
                seen = seen_by_file[filename]
            except KeyError:
                seen = seen_by_file[filename] = set()
            self._add_section_for_file(filename, section, data, resolve, seen)

    def _add_section_for_file(self, filename, section, data, resolve, seen):
        if resolve is None and filename == DEFAULT_MANIFEST:
            resolve = resolve_default_benchmark

        if section == "group":
            name, entries = data
            self._add_group(name, entries)
        else:
            # All sections with an identifier have already been handled.
            if section in seen:
                # For now each section_key can only show up once.
                raise NotImplementedError((section, data))
            seen.add(section)

            if section == "includes":
                pass
            elif section == "benchmarks":
                entries = ((s, m, filename) for s, m in data)
                self._add_benchmarks(entries, resolve)
            elif section == "groups":
                for name in data:
                    self._add_group(name, None)
            else:
                raise NotImplementedError((section, data))

    def _add_benchmarks(self, entries, resolve):
        for spec, metafile, filename in entries:
            # XXX Ignore duplicates?
            self._add_benchmark(spec, metafile, resolve, filename)

    def _add_benchmark(self, spec, metafile, resolve, filename):
        if spec.name in self._raw_groups:
            raise ValueError(
                f"a group and a benchmark have the same name ({spec.name})"
            )
        if spec.name == "all":
            raise ValueError(
                'a benchmark named "all" is not allowed ("all" is reserved for selecting the full set of declared benchmarks)'
            )
        if metafile:
            if filename:
                localdir = os.path.dirname(filename)
                metafile = os.path.join(localdir, metafile)
            bench = _benchmark.Benchmark(spec, metafile)
        else:
            metafile = None
            bench = spec
        self._raw_benchmarks.append((spec, metafile, filename))
        if resolve is not None:
            bench = resolve(bench)
        if bench.name in self._byname:
            raise ValueError(f"a benchmark named {bench.name} was already declared")
        self._byname[bench.name] = bench
        self._groups = None  # Force re-resolution.
        self._tags = None  # Force re-resolution.

    def _add_group(self, name, entries):
        if name in self._byname:
            raise ValueError(f"a group and a benchmark have the same name ({name})")
        if name == "all":
            raise ValueError(
                'a group named "all" is not allowed ("all" is reserved for selecting the full set of declared benchmarks)'
            )
        if entries is None:
            if name in self._raw_groups:
                return
            self._raw_groups[name] = None
        elif name in self._raw_groups and self._raw_groups[name] is not None:
            raise ValueError(f"a group named {name} was already defined")
        else:
            self._raw_groups[name] = list(entries) if entries else []
        self._groups = None  # Force re-resolution.

    def _custom_groups(self):
        return set(self._raw_groups) - {"all", "default"}

    def _get_tags(self):
        if self._tags is None:
            self._tags = _get_tags(self._byname.values())
            self._tags.pop("all", None)  # It is manifest-specific.
            self._tags.pop("default", None)  # It is manifest-specific.
        return self._tags

    def _resolve_groups(self):
        if self._groups is not None:
            return self._groups

        raw = {}
        for name, entries in self._raw_groups.items():
            if entries and entries[0][0] == "-":
                entries = list(entries)
                entries.insert(0, ("+", "<all>"))
            raw[name] = entries
        self._groups = _resolve_groups(raw, self._byname)
        return self._groups

    def resolve_group(self, name, *, fail=True):
        if name == "all":
            benchmarks = self._byname.values()
        elif name == "default":
            if "default" not in self._raw_groups:
                benchmarks = self._byname.values()
            else:
                groups = self._resolve_groups()
                benchmarks = groups.get(name)
        elif not self._custom_groups():
            benchmarks = self._get_tags().get(name)
            if benchmarks is None and fail:
                raise KeyError(name)
        else:
            groups = self._resolve_groups()
            benchmarks = groups.get(name)
            if not benchmarks:
                if name not in self._raw_groups:
                    benchmarks = self._get_tags().get(name, ())
                elif fail:
                    raise KeyError(name)
        yield from benchmarks or ()

    def show(self, *, raw=True, resolved=True):
        yield self.filename
        yield "groups:"
        if raw:
            yield f" {self._raw_groups}"
        if resolved:
            yield f" {self.groups}"
        yield "default:"
        if resolved:
            for i, bench in enumerate(self.resolve_group("default")):
                yield f" {i:>2} {bench}"
        if raw:
            yield "benchmarks (raw):"
            for i, bench in enumerate(self._raw_benchmarks):
                yield f" {i:>2} {bench}"
        if resolved:
            yield "benchmarks:"
            for i, bench in enumerate(self.benchmarks):
                yield f" {i:>2} {bench}"


#######################################
# internal implementation


def _iter_sections(lines):
    lines = (line.split("#")[0].strip() for line in lines)

    name = None
    section = None
    for line in lines:
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            if name:
                yield name, section
            name = line[1:-1].strip()
            section = []
        else:
            if not name:
                raise ValueError(f"expected new section, got {line!r}")
            section.append(line)
    if name:
        yield name, section
    else:
        raise ValueError("invalid manifest file, no sections found")


def _parse_manifest_file(filename):
    with open(filename, encoding="utf-8") as infile:
        yield from _parse_manifest(infile, filename)


def _parse_manifest(lines, filename):
    relroot = os.path.dirname(filename)
    for section, seclines in _iter_sections(lines):
        if section == "includes":
            yield filename, section, list(seclines)
            for line in seclines:
                if line == "<default>":
                    line = DEFAULT_MANIFEST
                else:
                    line = _utils.resolve_file(line, relroot)
                yield from _parse_manifest_file(line)
        elif section == "benchmarks":
            yield filename, section, list(_parse_benchmarks_section(seclines))
        elif section == "groups":
            yield filename, section, list(_parse_groups_section(seclines))
        elif section.startswith("group "):
            section, _, group = section.partition(" ")
            entries = list(_parse_group_section(seclines))
            yield filename, section, (group, entries)
        else:
            raise ValueError(f"unsupported section {section!r}")


def _parse_benchmarks_section(lines):
    if not lines:
        lines = ["<empty>"]
    lines = iter(lines)
    if next(lines) != BENCH_HEADER:
        raise ValueError("invalid manifest file, expected benchmarks table header")

    version = origin = None
    for line in lines:
        try:
            name, metafile = (
                None if field == "-" else field for field in line.split("\t")
            )
        except ValueError:
            raise ValueError(f"bad benchmark line {line!r}")
        spec = _benchmark.BenchmarkSpec(name or None, version, origin)
        metafile = _parse_metafile(metafile, name)
        yield spec, metafile


def _parse_metafile(metafile, name):
    if not metafile:
        return None
    elif metafile.startswith("<") and metafile.endswith(">"):
        directive, _, extra = metafile[1:-1].partition(":")
        if directive == "local":
            if extra:
                rootdir = f"bm_{extra}"
                basename = f"bm_{name}.toml"
            else:
                rootdir = f"bm_{name}"
                basename = "pyproject.toml"
            # A relative path will be resolved against the manifset file.
            return os.path.join(rootdir, basename)
        else:
            raise ValueError(f"unsupported metafile directive {metafile!r}")
    else:
        return os.path.abspath(metafile)


def _parse_groups_section(lines):
    for name in lines:
        _utils.check_name(name)
        yield name


def _parse_group_section(lines):
    for line in lines:
        if line.startswith("-"):
            # Exclude a benchmark or group.
            op = "-"
            name = line[1:]
        elif line.startswith("+"):
            op = "+"
            name = line[1:]
        else:
            op = "+"
            name = line
        _benchmark.check_name(name)
        yield op, name


def _get_tags(benchmarks):
    # Fill in groups from benchmark tags.
    tags = {}
    for bench in benchmarks:
        for tag in getattr(bench, "tags", ()):
            if tag in tags:
                tags[tag].append(bench)
            else:
                tags[tag] = [bench]
    return tags


def _resolve_groups(rawgroups, byname):
    benchmarks = set(byname.values())
    tags = None
    groups = {
        "all": list(benchmarks),
    }
    unresolved = {}
    for groupname, entries in rawgroups.items():
        if groupname == "all":
            continue
        if not entries:
            if groupname == "default":
                groups[groupname] = list(benchmarks)
            else:
                if tags is None:
                    tags = _get_tags(benchmarks)
                groups[groupname] = tags.get(groupname, ())
            continue
        assert entries[0][0] == "+", (groupname, entries)
        unresolved[groupname] = names = set()
        for op, name in entries:
            if op == "+":
                if name == "<all>":
                    names.update(byname)
                elif name in byname or name in rawgroups:
                    names.add(name)
            elif op == "-":
                if name == "<all>":
                    raise NotImplementedError((groupname, op, name))
                elif name in byname or name in rawgroups:
                    if name in names:
                        names.remove(name)
            else:
                raise NotImplementedError((groupname, op, name))
    while unresolved:
        for groupname, names in list(unresolved.items()):
            benchmarks = set()

            q = list(names)
            while q:
                name = q.pop()

                if name in byname:
                    benchmarks.add(byname[name])
                elif name in groups:
                    benchmarks.update(groups[name])
                elif name == groupname:
                    pass
                else:  # name in unresolved
                    q.extend(unresolved[name])

            groups[groupname] = benchmarks
            del unresolved[groupname]
    return groups
