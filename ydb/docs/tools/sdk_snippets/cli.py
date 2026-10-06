"""Download only allowlisted SDK files and validate documentation regions."""

import argparse
import fnmatch
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path, PurePosixPath

import yaml


MAX_FILE_SIZE = 2 * 1024 * 1024
MAX_SOURCE_SIZE = 32 * 1024 * 1024
MAX_FILES = 1000
STAGING_ROOT = ".generated/sdk-snippets"
REPOSITORY = re.compile(r"https://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
DIGEST = re.compile(r"[0-9a-f]{64}\Z")
MARKER = re.compile(r"\[(BEGIN|END) ([A-Za-z0-9_]+)\]")
DIRECTIVE = re.compile(r"\{%\s*code\s+([\"'])(.*?)\1(.*?)%\}")
ATTRIBUTE = re.compile(r"([a-z-]+)\s*=\s*([\"'])(.*?)\2")


class SnippetError(Exception):
    pass


def checked_path(value):
    if not isinstance(value, str) or not value or "\\" in value or any(ord(c) < 32 for c in value):
        raise SnippetError("invalid source path: {!r}".format(value))
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in ("", ".", "..") for part in value.split("/")):
        raise SnippetError("source path must be relative and normalized: {!r}".format(value))
    return path


def allowed(path, patterns):
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def read_yaml(path):
    try:
        with path.open(encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except (OSError, yaml.YAMLError) as error:
        raise SnippetError("{}: {}".format(path, error)) from error
    if not isinstance(value, dict):
        raise SnippetError("{}: expected a YAML mapping".format(path))
    return value


def write_yaml(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False, encoding="utf-8") as stream:
        yaml.safe_dump(value, stream, sort_keys=False, allow_unicode=True)
        temporary = Path(stream.name)
    temporary.replace(path)


def manifest_sources(manifest):
    if type(manifest.get("version")) is not int or manifest["version"] != 1 or manifest.get("staging-root") != STAGING_ROOT:
        raise SnippetError("manifest requires version: 1 and staging-root: " + STAGING_ROOT)
    sources = manifest.get("sources")
    if not isinstance(sources, dict) or not sources:
        raise SnippetError("manifest sources must be a nonempty mapping")
    for sdk, source in sources.items():
        if not isinstance(sdk, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", sdk):
            raise SnippetError("invalid SDK identifier: {!r}".format(sdk))
        if not isinstance(source, dict) or not REPOSITORY.fullmatch(source.get("repository", "")):
            raise SnippetError("{}: expected a public github.com repository URL".format(sdk))
        ref = source.get("ref", {})
        if not isinstance(ref, dict) or ref.get("kind") not in ("branch", "tag", "commit"):
            raise SnippetError("{}: ref.kind must be branch, tag or commit".format(sdk))
        name = ref.get("name")
        if not isinstance(name, str) or not name or any(ord(c) < 32 for c in name):
            raise SnippetError("{}: ref.name is required".format(sdk))
        if ref["kind"] == "commit" and not SHA.fullmatch(name):
            raise SnippetError("{}: commit refs require a full SHA".format(sdk))
        patterns = source.get("include")
        if not isinstance(patterns, list) or not patterns:
            raise SnippetError("{}: include must be a nonempty list".format(sdk))
        for pattern in patterns:
            checked_path(pattern)
            if not pattern.startswith(("examples/ydb_tech/", "ydb/examples/ydb_tech/")):
                raise SnippetError("{}: include paths must stay inside examples/ydb_tech/".format(sdk))
    return sources


def checked_lock(manifest, lock):
    sources = manifest_sources(manifest)
    if type(lock.get("version")) is not int or lock["version"] != 1 or not isinstance(lock.get("sources"), dict):
        raise SnippetError("lock requires version: 1 and a sources mapping")
    if sources.keys() != lock["sources"].keys():
        raise SnippetError("manifest and lock have different SDK sources; run update")
    for sdk, source in sources.items():
        entry = lock["sources"][sdk]
        if entry.get("repository") != source["repository"] or entry.get("requested-ref") != source["ref"]:
            raise SnippetError("{}: manifest differs from lock; run update".format(sdk))
        if not SHA.fullmatch(entry.get("resolved-commit", "")):
            raise SnippetError("{}: lock requires a full resolved-commit SHA".format(sdk))
        if source["ref"]["kind"] == "commit" and entry["resolved-commit"] != source["ref"]["name"]:
            raise SnippetError("{}: commit ref differs from resolved-commit".format(sdk))
        if entry.get("include") != source["include"]:
            raise SnippetError("{}: include differs from lock; run update".format(sdk))
        files = entry.get("files")
        if not isinstance(files, dict) or not files or len(files) > MAX_FILES:
            raise SnippetError("{}: lock requires between 1 and {} files".format(sdk, MAX_FILES))
        size = 0
        for path, file in files.items():
            checked_path(path)
            if not allowed(path, source["include"]):
                raise SnippetError("{}: {} is outside include paths".format(sdk, path))
            if not SHA.fullmatch(file.get("git-blob", "")) or not DIGEST.fullmatch(file.get("sha256", "")):
                raise SnippetError("{}: {} has an invalid digest".format(sdk, path))
            length = file.get("size")
            if type(length) is not int or not 0 < length <= MAX_FILE_SIZE:
                raise SnippetError("{}: {} has an invalid file size".format(sdk, path))
            size += length
        if size > MAX_SOURCE_SIZE:
            raise SnippetError("{}: source exceeds the size limit".format(sdk))
    return sources


def verify_file(data, file, label):
    if len(data) != file["size"] or hashlib.sha256(data).hexdigest() != file["sha256"]:
        raise SnippetError("{}: file size or SHA-256 differs from lock".format(label))
    blob = b"blob " + str(len(data)).encode("ascii") + b"\0" + data
    if hashlib.sha1(blob).hexdigest() != file["git-blob"]:
        raise SnippetError("{}: Git blob differs from lock".format(label))


class GitHub:
    def __init__(self):
        self.token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")

    def request(self, url, limit):
        headers = {"User-Agent": "ydb-sdk-snippets", "Accept": "application/vnd.github+json"}
        if self.token and urllib.parse.urlsplit(url).hostname == "api.github.com":
            headers["Authorization"] = "Bearer " + self.token
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30) as stream:
                data = stream.read(limit + 1)
        except (OSError, urllib.error.HTTPError) as error:
            raise SnippetError("cannot download {}: {}".format(url, error)) from error
        if len(data) > limit:
            raise SnippetError("download exceeds size limit: " + url)
        return data

    def api(self, repository, suffix):
        owner, repo = REPOSITORY.fullmatch(repository).groups()
        data = self.request("https://api.github.com/repos/{}/{}/{}".format(owner, repo, suffix), 8 * 1024 * 1024)
        try:
            return json.loads(data)
        except ValueError as error:
            raise SnippetError("invalid JSON response from GitHub") from error

    def resolve(self, source):
        ref = source["ref"]
        repository = source["repository"]
        if ref["kind"] == "commit":
            return self.api(repository, "git/commits/" + ref["name"])["sha"]
        namespace = "heads" if ref["kind"] == "branch" else "tags"
        obj = self.api(repository, "git/ref/{}/{}".format(namespace, urllib.parse.quote(ref["name"], safe="/")))["object"]
        seen = set()
        while obj["type"] == "tag":
            if obj["sha"] in seen or len(seen) >= 10:
                raise SnippetError("invalid annotated tag chain")
            seen.add(obj["sha"])
            obj = self.api(repository, "git/tags/" + obj["sha"])["object"]
        if obj["type"] != "commit" or not SHA.fullmatch(obj["sha"]):
            raise SnippetError("ref does not resolve to a commit")
        return obj["sha"]

    def files(self, source, commit):
        repository = source["repository"]
        root = self.api(repository, "git/commits/" + commit)["tree"]["sha"]
        tree_cache = {}

        def tree(sha, recursive=False):
            key = (sha, recursive)
            if key not in tree_cache:
                result = self.api(repository, "git/trees/" + sha + ("?recursive=1" if recursive else ""))
                if result.get("truncated"):
                    raise SnippetError("GitHub truncated the requested source tree")
                tree_cache[key] = result["tree"]
            return tree_cache[key]

        files = {}
        for pattern in source["include"]:
            parts = pattern.split("/")
            prefix = []
            for part in parts:
                if any(c in part for c in "*?["):
                    break
                prefix.append(part)
            if len(prefix) == len(parts):
                prefix = prefix[:-1]
            sha = root
            for part in prefix:
                matches = [item for item in tree(sha) if item["path"] == part and item["type"] == "tree"]
                if len(matches) != 1:
                    raise SnippetError("include directory is missing: " + "/".join(prefix))
                sha = matches[0]["sha"]
            matched = 0
            for item in tree(sha, recursive=True):
                path = "/".join(prefix + [item["path"]])
                if not fnmatch.fnmatchcase(path, pattern):
                    continue
                checked_path(path)
                if item["type"] == "tree":
                    continue
                if item["type"] != "blob" or item["mode"] not in ("100644", "100755"):
                    raise SnippetError("symlinks and submodules are not allowed: " + path)
                size = item.get("size", 0)
                if not 0 < size <= MAX_FILE_SIZE:
                    raise SnippetError("source file exceeds size limit or is empty: " + path)
                files[path] = {"git-blob": item["sha"], "size": size}
                matched += 1
            if not matched:
                raise SnippetError("include pattern matches no files: " + pattern)
        if len(files) > MAX_FILES or sum(file["size"] for file in files.values()) > MAX_SOURCE_SIZE:
            raise SnippetError("source exceeds the file count or size limit")
        return dict(sorted(files.items()))

    def download(self, source, commit, path):
        owner, repo = REPOSITORY.fullmatch(source["repository"]).groups()
        url = "https://raw.githubusercontent.com/{}/{}/{}/{}".format(owner, repo, commit, urllib.parse.quote(path, safe="/"))
        return self.request(url, MAX_FILE_SIZE)


def cache_file(cache, digest):
    return cache / "sha256" / digest[:2] / digest


def cached_data(cache, file, label):
    path = cache_file(cache, file["sha256"])
    if not path.is_file() or path.is_symlink():
        return None
    data = path.read_bytes()
    verify_file(data, file, label)
    return data


def store_file(cache, data, file):
    path = cache_file(cache, file["sha256"])
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        stream.write(data)
        temporary = Path(stream.name)
    temporary.replace(path)


def update(manifest_path, lock_path, cache, sdk=None, ref=None, kind=None, github=None):
    github = github or GitHub()
    manifest = read_yaml(manifest_path)
    sources = manifest_sources(manifest)
    selected = [sdk] if sdk else list(sources)
    if sdk and sdk not in sources:
        raise SnippetError("unknown SDK: " + sdk)
    if ref and not sdk:
        raise SnippetError("--ref requires one SDK identifier")
    if ref:
        sources[sdk]["ref"] = {"kind": kind or sources[sdk]["ref"]["kind"], "name": ref}
    elif kind:
        raise SnippetError("--kind requires --ref")
    manifest_sources(manifest)
    lock = read_yaml(lock_path) if lock_path.exists() else {"version": 1, "sources": {}}
    for name in selected:
        source = sources[name]
        commit = github.resolve(source)
        previous = lock["sources"].get(name, {})
        if (source["ref"]["kind"] == "tag" and previous.get("repository") == source["repository"]
                and previous.get("requested-ref") == source["ref"]
                and previous.get("resolved-commit") != commit):
            raise SnippetError("{}: tag moved from its locked commit; choose a new tag or an explicit commit".format(name))
        files = github.files(source, commit)
        for path, file in files.items():
            data = github.download(source, commit, path)
            file["sha256"] = hashlib.sha256(data).hexdigest()
            verify_file(data, file, name + "/" + path)
            store_file(cache, data, file)
        lock["sources"][name] = {
            "repository": source["repository"], "requested-ref": dict(source["ref"]),
            "resolved-commit": commit, "include": list(source["include"]), "files": files,
        }
        print("{}: {} files at {}".format(name, len(files), commit))
    lock["sources"] = {name: lock["sources"][name] for name in sources if name in lock["sources"]}
    checked_lock(manifest, lock)
    write_yaml(lock_path, lock)
    if ref:
        write_yaml(manifest_path, manifest)
    return lock


def regions(text, label):
    found = {}
    opened = None
    for index, line in enumerate(text.splitlines()):
        markers = list(MARKER.finditer(line))
        if ("[BEGIN " in line or "[END " in line) and len(markers) != 1:
            raise SnippetError("{}:{}: invalid region marker".format(label, index + 1))
        if not markers:
            continue
        marker = markers[0]
        action, name = marker.groups()
        if action == "BEGIN":
            if opened or name in found:
                raise SnippetError("{}:{}: nested or duplicate region {}".format(label, index + 1, name))
            opened = (name, index)
        else:
            if not opened or opened[0] != name:
                raise SnippetError("{}:{}: unmatched region {}".format(label, index + 1, name))
            content = "\n".join(text.splitlines()[opened[1] + 1:index])
            if not content.strip():
                raise SnippetError("{}:{}: empty region {}".format(label, index + 1, name))
            found[name] = content
            opened = None
    if opened:
        raise SnippetError("{}: unclosed region {}".format(label, opened[0]))
    return found


def directives(root):
    # Code directives in Markdown fences are explanatory text, not dependencies.
    for page in sorted(root.rglob("*.md")):
        if any(part.startswith(".") for part in page.relative_to(root).parts):
            continue
        fenced = None
        for number, line in enumerate(page.read_text(encoding="utf-8").splitlines(), 1):
            fence = re.match(r"\s*(`{3,}|~{3,})", line)
            if fence:
                if not fenced:
                    fenced = fence[1]
                elif fence[1][0] == fenced[0] and len(fence[1]) >= len(fenced):
                    fenced = None
                continue
            if fenced:
                continue
            if STAGING_ROOT not in line or not re.search(r"\{%\s*code\b", line):
                continue
            matches = list(DIRECTIVE.finditer(line))
            if not matches:
                raise SnippetError("{}:{}: malformed SDK code directive".format(page, number))
            for match in matches:
                path = match[2]
                if not path.startswith("/" + STAGING_ROOT + "/"):
                    raise SnippetError("{}:{}: SDK snippets require a root-relative staging path".format(page, number))
                attributes = ATTRIBUTE.findall(match[3])
                attrs = {}
                remainder = ATTRIBUTE.sub("", match[3]).strip()
                if remainder:
                    raise SnippetError("{}:{}: unsupported SDK code attributes".format(page, number))
                for key, _, value in attributes:
                    if key in attrs or key not in ("lang", "lines", "keep-indents"):
                        raise SnippetError("{}:{}: duplicate or unsupported attribute {}".format(page, number, key))
                    attrs[key] = value
                if not attrs.get("lang"):
                    raise SnippetError("{}:{}: SDK code requires lang".format(page, number))
                yield page, number, path[len("/" + STAGING_ROOT + "/"):], attrs


def validate(root, manifest, lock, staging=None):
    sources = checked_lock(manifest, lock)
    staging = staging or root / STAGING_ROOT
    expected = set()
    text_by_path = {}
    for sdk, entry in lock["sources"].items():
        for path, file in entry["files"].items():
            key = sdk + "/" + path
            expected.add(key)
            target = staging / key
            if not target.is_file() or target.is_symlink() or not target.resolve().is_relative_to(staging.resolve()):
                raise SnippetError("staged file is missing or unsafe: " + key)
            data = target.read_bytes()
            verify_file(data, file, key)
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError as error:
                raise SnippetError("source file must be UTF-8: " + key) from error
            text_by_path[key] = (text, regions(text, key))
    actual = set()
    if staging.exists():
        for path in staging.rglob("*"):
            if path.is_symlink():
                raise SnippetError("symlinks are not allowed in staging: " + str(path))
            if path.is_file():
                actual.add(path.relative_to(staging).as_posix())
    if actual != expected:
        raise SnippetError("staging differs from locked files: " + ", ".join(sorted(actual ^ expected)))
    count = 0
    for page, number, key, attrs in directives(root):
        checked_path(key)
        sdk, _, path = key.partition("/")
        label = "{}:{}".format(page.relative_to(root), number)
        if sdk not in sources or key not in text_by_path or not allowed(path, sources[sdk]["include"]):
            raise SnippetError(label + ": referenced file is not in the lock allowlist: " + key)
        if "lines" in attrs:
            selected = re.fullmatch(r"\[BEGIN ([A-Za-z0-9_]+)\]-\[END \1\]", attrs["lines"])
            if not selected or selected[1] not in text_by_path[key][1]:
                raise SnippetError(label + ": referenced region is missing or invalid: " + attrs["lines"])
        count += 1
    if not count:
        raise SnippetError("documentation contains no SDK code references")
    return count


def prepare(root, manifest, lock, cache, offline=False, local=None, github=None):
    github = github or GitHub()
    sources = checked_lock(manifest, lock)
    staging = root / STAGING_ROOT
    if staging.is_symlink() or staging.parent.is_symlink():
        raise SnippetError("staging directory must not be a symlink")
    staging.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix="sdk-snippets-", dir=staging.parent))
    try:
        for sdk, source in sources.items():
            entry = lock["sources"][sdk]
            for path, file in entry["files"].items():
                label = sdk + "/" + path
                if local and sdk in local:
                    source_root = local[sdk].resolve()
                    source_file = source_root / path
                    if source_file.is_symlink() or not source_file.resolve().is_relative_to(source_root):
                        raise SnippetError("local source is a symlink or escapes its root: " + label)
                    data = source_file.read_bytes()
                    verify_file(data, file, label)
                else:
                    data = cached_data(cache, file, label)
                    if data is None:
                        if offline:
                            raise SnippetError("offline cache is missing: " + label)
                        data = github.download(source, entry["resolved-commit"], path)
                        verify_file(data, file, label)
                        store_file(cache, data, file)
                target = temporary / label
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
        count = validate(root, manifest, lock, temporary)
        backup = temporary.with_name(temporary.name + "-previous")
        if staging.exists():
            staging.replace(backup)
        try:
            temporary.replace(staging)
        except OSError:
            if backup.exists():
                backup.replace(staging)
            raise
        if backup.exists():
            shutil.rmtree(backup)
        print("Prepared {} sources; validated {} SDK code references".format(len(sources), count))
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def selected_snippets(root, lock, cache):
    values = {}
    for page, number, key, attrs in directives(root):
        sdk, _, path = key.partition("/")
        entry = lock["sources"].get(sdk, {})
        file = entry.get("files", {}).get(path)
        label = "{}:{}".format(page.relative_to(root), number)
        if file is None:
            values[label + " " + key] = None
            continue
        data = cached_data(cache, file, key)
        if data is None:
            raise SnippetError("diff requires cached source file: " + key)
        text = data.decode("utf-8")
        name = attrs.get("lines", "")
        if name:
            selected = re.fullmatch(r"\[BEGIN ([A-Za-z0-9_]+)\]-\[END \1\]", name)
            text = regions(text, key).get(selected[1]) if selected else None
        values[page.relative_to(root).as_posix() + " " + key + " " + name] = text
    return values


def snippet_diff(root, previous, current, cache):
    import difflib

    github = GitHub()
    for lock in (previous, current):
        manifest = {"version": 1, "staging-root": STAGING_ROOT, "sources": {
            sdk: {"repository": entry["repository"], "ref": entry["requested-ref"], "include": entry["include"]}
            for sdk, entry in lock["sources"].items()
        }}
        sources = checked_lock(manifest, lock)
        for sdk, entry in lock["sources"].items():
            for path, file in entry["files"].items():
                if cached_data(cache, file, sdk + "/" + path) is None:
                    data = github.download(sources[sdk], entry["resolved-commit"], path)
                    verify_file(data, file, sdk + "/" + path)
                    store_file(cache, data, file)
    before = selected_snippets(root, previous, cache)
    after = selected_snippets(root, current, cache)
    for label in sorted(before.keys() | after.keys()):
        old, new = before.get(label), after.get(label)
        if old != new:
            print("".join(difflib.unified_diff((old or "").splitlines(True), (new or "").splitlines(True),
                                             fromfile=label + " (before)", tofile=label + " (after)")), end="")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).resolve().parents[2] / "sdk-snippets.yaml")
    parser.add_argument("--cache", type=Path, default=Path(os.environ.get("SDK_SNIPPETS_CACHE", "~/.cache/ydb/sdk-snippets")).expanduser())
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare", help="prepare locked files and validate references")
    prepare_parser.add_argument("--offline", action="store_true")
    prepare_parser.add_argument("--local-source", action="append", default=[], metavar="SDK=PATH")
    commands.add_parser("validate", help="validate manifest, lock, staging and all references")
    update_parser = commands.add_parser("update", help="resolve refs, download allowed files and refresh lock")
    update_parser.add_argument("sdk", nargs="?")
    update_parser.add_argument("--ref")
    update_parser.add_argument("--kind", choices=("branch", "tag", "commit"))
    diff_parser = commands.add_parser("diff", help="compare selected snippet contents with an earlier lock")
    diff_parser.add_argument("--before", required=True, type=Path)
    args = parser.parse_args(argv)
    root = args.manifest.resolve().parent
    lock_path = root / "sdk-snippets.lock.yaml"
    try:
        if args.command == "update":
            update(args.manifest, lock_path, args.cache, args.sdk, args.ref, args.kind)
            return 0
        manifest, lock = read_yaml(args.manifest), read_yaml(lock_path)
        if args.command == "prepare":
            local = {}
            for override in args.local_source:
                sdk, separator, path = override.partition("=")
                if not separator or sdk not in manifest_sources(manifest):
                    raise SnippetError("--local-source requires a known SDK=PATH")
                local[sdk] = Path(path)
            prepare(root, manifest, lock, args.cache, args.offline, local)
        elif args.command == "validate":
            print("Validated {} SDK code references".format(validate(root, manifest, lock)))
        else:
            snippet_diff(root, read_yaml(args.before), lock, args.cache)
    except (SnippetError, OSError, KeyError, TypeError) as error:
        print("sdk-snippets: " + str(error), file=sys.stderr)
        return 1
    return 0
