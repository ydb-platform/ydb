#!/usr/bin/env python3
"""Preparation-only network fetch of SHA256-locked browser bundle inputs.

Tests never invoke this file. No npm installation, resource upload or host
package installation occurs. All content is staged under the explicit output.
"""
import argparse
import concurrent.futures
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import posixpath
import shutil
import subprocess
import tarfile
import zipfile


HERE = Path(__file__).resolve().parent
REPOSITORY = HERE.parents[4]


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def fetch(item, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.is_file() or sha256(target) != item["sha256"]:
        subprocess.run(["curl", "--fail", "--location", "--silent", "--show-error",
                        "--max-time", "120", "--output", str(target), item["url"]], check=True)
    if sha256(target) != item["sha256"]:
        raise ValueError("download SHA256 mismatch: %s" % item["url"])
    return target


def extract_browser(archive, target):
    target.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as zipped:
        for info in zipped.infolist():
            name = PurePosixPath(info.filename)
            if name.is_absolute() or ".." in name.parts:
                raise ValueError("unsafe browser zip path: %s" % name)
        if zipped.testzip() is not None:
            raise ValueError("browser zip CRC failure")
        zipped.extractall(target)
        for info in zipped.infolist():
            if not info.is_dir():
                (target / info.filename).chmod((info.external_attr >> 16) & 0o777 or 0o644)


def ar_data(archive):
    data = archive.read_bytes()
    if data[:8] != b"!<arch>\n":
        raise ValueError("not a Debian ar package: %s" % archive)
    offset = 8
    while offset < len(data):
        header = data[offset:offset + 60]
        size = int(header[48:58])
        name = header[:16].decode().strip().rstrip("/")
        payload = data[offset + 60:offset + 60 + size]
        offset += 60 + size + (size % 2)
        if name.startswith("data.tar"):
            return payload
    raise ValueError("Debian package has no data archive: %s" % archive)


def extract_libraries(archive, libraries, licenses):
    # Flatten shared-library aliases to actual ELF payloads. No absolute or
    # external symlinks from the Debian filesystem escape into the bundle.
    with tarfile.open(fileobj=io.BytesIO(ar_data(archive)), mode="r:*") as unpacked:
        members = unpacked.getmembers()
        by_name = {member.name.removeprefix("./"): member for member in members}
        for member in members:
            name = member.name.removeprefix("./")
            if "/lib/" in "/" + name and ".so" in Path(name).name and (member.isfile() or member.issym() or member.islnk()):
                source = member
                seen = set()
                while source.issym() or source.islnk():
                    if source.name in seen:
                        raise ValueError("cyclic native symlink: %s" % source.name)
                    seen.add(source.name)
                    link = source.linkname
                    destination = link[1:] if link.startswith("/") else posixpath.normpath(
                        posixpath.join(posixpath.dirname(source.name.removeprefix("./")), link))
                    source = by_name.get(destination) or by_name.get(destination.removeprefix("usr/")) or by_name.get("usr/" + destination)
                    if source is None:
                        break
                if source is not None and source.isfile():
                    payload = unpacked.extractfile(source).read()
                    if payload[:4] == b"\x7fELF":
                        target = libraries / Path(name).name
                        if target.exists() and target.read_bytes() != payload:
                            raise ValueError("native library name collision: %s" % target.name)
                        target.write_bytes(payload)
            if name.endswith("/copyright") and member.isfile():
                (licenses / (archive.name + ".copyright")).write_bytes(unpacked.extractfile(member).read())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staging", required=True, type=Path)
    parser.add_argument("--platform", choices=("darwin-arm64", "linux-x86_64", "all"), default="all")
    args = parser.parse_args()
    root = args.staging.resolve()
    root.mkdir(parents=True, exist_ok=True)
    lock = json.loads((HERE / "resources/upstream.json").read_text())
    package = fetch(lock["playwright"], root / "playwright-core-1.64.1.tgz")
    with tarfile.open(package) as packed:
        packed.extractall(root, filter="data")
    metadata = json.loads((root / "package/package.json").read_text())
    if metadata["version"] != "1.64.1":
        raise ValueError("wrong Playwright version")
    shell = next(item for item in json.loads((root / "package/browsers.json").read_text())["browsers"]
                 if item["name"] == "chromium-headless-shell")
    if shell["revision"] != "1248" or shell["browserVersion"] != "156.0.8078.4":
        raise ValueError("wrong Playwright browser mapping")
    fonts = root / "fonts"
    fonts.mkdir(exist_ok=True)
    for item in lock["fonts"]:
        source = REPOSITORY / item["repo_relative_path"]
        if sha256(source) != item["sha256"]:
            raise ValueError("pinned repository font/license changed: %s" % source)
        shutil.copy2(source, fonts / item["name"])
    platforms = lock["browsers"] if args.platform == "all" else {args.platform: lock["browsers"][args.platform]}
    for name, browser in platforms.items():
        archive = fetch(browser, root / Path(browser["url"]).name)
        extract_browser(archive, root / name)
        provenance = {"playwright": lock["playwright"],
                      "fonts": [{key: value for key, value in item.items() if key != "repo_relative_path"}
                                for item in lock["fonts"]],
                      "source_mapping": lock["source_mapping"],
                      "uploads": "not performed; repository-owner approval required",
                      "browser": browser}
        if name == "linux-x86_64":
            native = json.loads((HERE / "resources/linux-native-lock.json").read_text())
            libraries, licenses = root / "native-libs", root / "native-licenses"
            libraries.mkdir(exist_ok=True)
            licenses.mkdir(exist_ok=True)
            def download(item):
                return fetch(item, root / "debs" / Path(item["url"]).name)
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as workers:
                archives = list(workers.map(download, native))
            for dependency in archives:
                extract_libraries(dependency, libraries, licenses)
            provenance["native_dependencies"] = native
            provenance["debian_package_index"] = lock["debian_package_index"]
            provenance["native_validation"] = lock["native_validation"]
        (root / ("PROVENANCE-" + name + ".json")).write_text(json.dumps(provenance, indent=2) + "\n")
        print("Staged verified inputs for", name, "at", root)


if __name__ == "__main__":
    main()
