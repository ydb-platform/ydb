#!/usr/bin/env python3
"""Verify and pack already downloaded, owner-reviewed browser test dependencies.

This command performs no network access or upload. Retain the emitted manifest
and SHA256 beside the archive for repository-owner approval before registration.
"""
import argparse
import hashlib
import gzip
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import zipfile


PLAYWRIGHT = "1.64.1"
REVISION = "1248"
PLATFORMS = ("linux-x86_64", "darwin-arm64")
UPSTREAM_PATH = Path(__file__).with_name("resources") / "upstream.json"
# Everything else reported by ldd must be in the supplied native closure.
HOST_GLIBC = re.compile(r"^(?:ld-linux[^/]*|lib(?:c|m|pthread|dl|rt|resolv|util)\.so(?:\..*)?)$")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def regular_tree(root):
    root = Path(root)
    if root.is_symlink() or not stat.S_ISDIR(root.lstat().st_mode):
        raise ValueError("bundle source root must be a regular directory: %s" % root)
    files, directories = {}, set()
    for path in root.rglob("*"):
        relative = str(path.relative_to(root))
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            directories.add(relative)
        elif stat.S_ISREG(mode):
            files[relative] = path
        else:
            raise ValueError("bundle source contains a symlink/nonregular node: %s" % path)
    return files, directories


def copy_tree(source, target):
    regular_tree(source)
    shutil.copytree(source, target)


def archive_relative(name, kind="browser"):
    path = PurePosixPath(name)
    if not name or path.is_absolute() or ".." in path.parts or "\\" in name or str(path) != name or name == ".":
        raise ValueError("unsafe %s archive path: %s" % (kind, name))
    return path


def verify_staged_browser(browser, archive, platform):
    if archive is None:
        raise ValueError("the staged pinned Chromium archive is required (--browser-archive)")
    archive = Path(archive)
    if not stat.S_ISREG(archive.lstat().st_mode):
        raise ValueError("pinned Chromium archive must be a regular file")
    locked = json.loads(UPSTREAM_PATH.read_text())["browsers"][platform]
    if sha256(archive) != locked["sha256"]:
        raise ValueError("pinned Chromium archive SHA256 mismatch")
    expected_files, expected_directories, seen = {}, set(), set()
    with zipfile.ZipFile(archive) as packed:
        for info in packed.infolist():
            relative = info.filename[:-1] if info.is_dir() else info.filename
            path = archive_relative(relative)
            if relative in seen:
                raise ValueError("duplicate browser archive path: %s" % relative)
            seen.add(relative)
            kind = stat.S_IFMT(info.external_attr >> 16)
            if kind not in (0, stat.S_IFDIR if info.is_dir() else stat.S_IFREG):
                raise ValueError("browser archive contains a symlink/nonregular node: %s" % relative)
            expected_directories.update(str(parent) for parent in path.parents if str(parent) != ".")
            if info.is_dir():
                expected_directories.add(relative)
            else:
                checksum = hashlib.sha256()
                with packed.open(info) as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        checksum.update(chunk)
                expected_files[relative] = checksum.hexdigest()
    actual_files, actual_directories = regular_tree(browser)
    if set(actual_files) != set(expected_files) or actual_directories != expected_directories:
        raise ValueError("staged browser inventory differs from the pinned Chromium archive")
    for relative, checksum in expected_files.items():
        if sha256(actual_files[relative]) != checksum:
            raise ValueError("staged browser file differs from the pinned Chromium archive: %s" % relative)


def verify_staged_playwright(playwright, archive):
    if archive is None:
        raise ValueError("the staged pinned Playwright archive is required (--playwright-archive)")
    archive = Path(archive)
    if not stat.S_ISREG(archive.lstat().st_mode):
        raise ValueError("pinned Playwright archive must be a regular file")
    locked = json.loads(UPSTREAM_PATH.read_text())["playwright"]
    if sha256(archive) != locked["sha256"]:
        raise ValueError("pinned Playwright archive SHA256 mismatch")
    expected_files, expected_directories, seen = {}, set(), set()
    with tarfile.open(archive, "r:*") as packed:
        for info in packed:
            name = info.name[:-1] if info.isdir() and info.name.endswith("/") else info.name
            path = archive_relative(name, "Playwright")
            if path.parts[0] != "package":
                raise ValueError("Playwright archive path must be under package/: %s" % name)
            if name in seen:
                raise ValueError("duplicate Playwright archive path: %s" % name)
            seen.add(name)
            if not info.isdir() and not info.isfile():
                raise ValueError("Playwright archive contains a symlink/nonregular node: %s" % name)
            if str(path) == "package":
                if not info.isdir():
                    raise ValueError("Playwright archive package root must be a directory")
                continue
            path = path.relative_to("package")
            relative = str(path)
            expected_directories.update(str(parent) for parent in path.parents if str(parent) != ".")
            if info.isdir():
                expected_directories.add(relative)
            else:
                checksum = hashlib.sha256()
                with packed.extractfile(info) as content:
                    for chunk in iter(lambda: content.read(1024 * 1024), b""):
                        checksum.update(chunk)
                expected_files[relative] = checksum.hexdigest()
    actual_files, actual_directories = regular_tree(playwright)
    if set(actual_files) != set(expected_files) or actual_directories != expected_directories:
        raise ValueError("staged Playwright inventory differs from the pinned archive")
    for relative, checksum in expected_files.items():
        if sha256(actual_files[relative]) != checksum:
            raise ValueError("staged Playwright file differs from the pinned archive: %s" % relative)


def verify_staged_fonts(fonts, font_license):
    entries = json.loads(UPSTREAM_PATH.read_text())["fonts"]
    font_pins = {item["name"]: item["sha256"] for item in entries
                 if Path(item["name"]).suffix.lower() in (".ttf", ".otf")}
    license_pins = {item["name"]: item["sha256"] for item in entries if item["name"] not in font_pins}
    seen = set()
    for path, kind, locked in [(font, "font", font_pins) for font in fonts] + [(font_license, "font license", license_pins)]:
        if kind == "font" and path.suffix.lower() not in (".ttf", ".otf"):
            raise ValueError("font must be an explicit TTF/OTF: %s" % path)
        if path.name not in locked:
            raise ValueError("%s is not locked by upstream.json: %s" % (kind, path.name))
        if path.name in seen:
            raise ValueError("duplicate font/license name: %s" % path.name)
        seen.add(path.name)
        if not stat.S_ISREG(path.lstat().st_mode):
            raise ValueError("pinned %s must be a regular file: %s" % (kind, path.name))
        if sha256(path) != locked[path.name]:
            raise ValueError("pinned %s SHA256 mismatch: %s" % (kind, path.name))


def linux_closure(browser, native, readelf=None):
    """Check DT_NEEDED dependencies recursively; host glibc/loader stay outside."""
    env = dict(os.environ, LD_LIBRARY_PATH=str(native))

    def is_elf(path):
        with path.open("rb") as source:
            return source.read(4) == b"\x7fELF"

    files = [path for directory in (browser.parent, native) for path in directory.rglob("*")
             if path.is_file() and is_elf(path)]
    available = {path.name for directory in (browser.parent, native) for path in directory.rglob("*") if path.is_file()}
    if readelf:
        definitions = {}
        requirements = {}
        highest_glibc = (0, 0)
        browser_glibc = False
        readelf_env = dict(os.environ, LC_ALL="C")
        for binary in files:
            dynamic = subprocess.run([str(readelf), "--dynamic", str(binary)], text=True,
                                     capture_output=True, check=True, env=readelf_env).stdout
            for name in re.findall(r"Shared library: \[([^\]]+)\]", dynamic):
                if not HOST_GLIBC.fullmatch(name) and name not in available:
                    raise ValueError("missing native DT_NEEDED dependency of %s: %s" % (binary, name))
            versions = subprocess.run([str(readelf), "--version-info", str(binary)], text=True,
                                      capture_output=True, check=True, env=readelf_env).stdout
            defined = versions.split("Version definition section", 1)
            definitions[binary.name] = set(re.findall(r"Name: (\S+)", defined[1].split("Version needs section", 1)[0])) if len(defined) > 1 else set()
            needed = versions.split("Version needs section", 1)
            requirements[binary.name] = []
            library = None
            if len(needed) > 1:
                for line in needed[1].splitlines():
                    match = re.search(r"File: (\S+)", line)
                    if match:
                        library = match[1]
                    match = re.search(r"Name: (\S+)", line)
                    if match:
                        version = match[1]
                        requirements[binary.name].append((library, version))
                        glibc = re.fullmatch(r"GLIBC_(\d+)\.(\d+)(?:\.\d+)?", version)
                        if glibc:
                            browser_glibc |= binary == browser
                            highest_glibc = max(highest_glibc, tuple(map(int, glibc.groups())))
        if not browser_glibc:
            raise ValueError("cannot verify browser GLIBC requirements from readelf version output")
        if highest_glibc > (2, 31):
            raise ValueError("native closure exceeds the supported glibc2.31 host baseline: %s" % (highest_glibc,))
        for binary, needed in requirements.items():
            for library, version in needed:
                if version.startswith("GLIBC_"):
                    continue
                if version not in definitions.get(library, set()):
                    raise ValueError("missing native symbol version for %s: %s/%s" % (binary, library, version))
        print(json.dumps({"elf_names": len(definitions), "highest_required_glibc": ".".join(map(str, highest_glibc)),
                          "supported_host_glibc_baseline": "2.31", "non_glibc_version_closure": "passed"}))
        return
    for binary in files:
        result = subprocess.run(["ldd", str(binary)], env=env, text=True,
                                capture_output=True, check=False)
        if result.returncode:
            raise ValueError("ldd failed for %s: %s" % (binary, result.stderr))
        for line in result.stdout.splitlines():
            if "not found" in line:
                raise ValueError("missing native dependency of %s: %s" % (binary, line.strip()))
            name = line.strip().split(" ", 1)[0]
            if not name or name.startswith("linux-vdso") or HOST_GLIBC.fullmatch(Path(name).name):
                continue
            match = re.search(r"(?:=>\s*)?(/[^ ]+)", line)
            if match and not Path(match[1]).resolve().is_relative_to(native.resolve()):
                raise ValueError("unbundled native dependency of %s: %s" % (binary, line.strip()))


def prepare(args):
    verify_staged_playwright(args.playwright, getattr(args, "playwright_archive", None))
    package = args.playwright / "package.json"
    metadata = json.loads(package.read_text())
    if metadata.get("name") != "playwright-core" or metadata.get("version") != PLAYWRIGHT:
        raise ValueError("required playwright-core %s, got %r" % (PLAYWRIGHT, metadata))
    browsers = json.loads((args.playwright / "browsers.json").read_text())["browsers"]
    shell = next(item for item in browsers if item["name"] == "chromium-headless-shell")
    if shell["revision"] != REVISION:
        raise ValueError("playwright-core does not pin chromium-headless-shell r%s" % REVISION)
    if not args.fonts or not args.provenance.is_file():
        raise ValueError("pinned fonts and source/checksum/license provenance are required")
    verify_staged_browser(args.browser, getattr(args, "browser_archive", None), args.platform)
    verify_staged_fonts(args.fonts, args.font_license)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="skipper-browser-") as temporary:
        root = Path(temporary) / "bundle"
        root.mkdir()
        copy_tree(args.playwright, root / "node_modules" / "playwright-core")
        copy_tree(args.browser, root / "browser")
        font_dir = root / "fonts"
        font_dir.mkdir()
        for font in args.fonts:
            shutil.copy2(font, font_dir / font.name)
        shutil.copy2(args.provenance, root / "PROVENANCE.json")
        shutil.copy2(args.font_license, font_dir / "LICENSE")
        if args.native_licenses:
            copy_tree(args.native_licenses, root / "native" / "licenses")
        if args.platform == "linux-x86_64":
            if args.native is None:
                raise ValueError("Linux requires the native dependency closure including fontconfig")
            if any(HOST_GLIBC.fullmatch(path.name) for path in args.native.iterdir()):
                raise ValueError("native closure must omit host glibc and ELF loader")
            copy_tree(args.native, root / "native" / "lib")
            browser = root / "browser" / "chrome-headless-shell-linux64" / "chrome-headless-shell"
            if not browser.is_file():
                raise ValueError("Linux browser directory must contain chrome-headless-shell-linux64/chrome-headless-shell")
            linux_closure(browser, root / "native" / "lib", args.readelf)
        else:
            browser = root / "browser" / "chrome-headless-shell-mac-arm64" / "chrome-headless-shell"
            if not browser.is_file():
                raise ValueError("macOS browser directory must contain chrome-headless-shell-mac-arm64/chrome-headless-shell")
        (root / "fonts.conf").write_text(
            '<?xml version="1.0"?><!DOCTYPE fontconfig SYSTEM "fonts.dtd">'
            '<fontconfig><dir prefix="relative">fonts</dir><cachedir prefix="xdg">'
            'fontconfig</cachedir><config><rescan><int>0</int></rescan></config></fontconfig>\n')
        manifest = {"schema": 1, "platform": args.platform, "playwright_version": PLAYWRIGHT,
                    "chromium_headless_shell_revision": REVISION, "files": {}}
        for path in sorted(root.rglob("*")):
            if path.is_file():
                manifest["files"][str(path.relative_to(root))] = sha256(path)
        (root / "bundle.json").write_text(json.dumps(manifest, indent=2) + "\n")
        prepared = output / args.platform
        if prepared.exists():
            raise ValueError("prepared output directory already exists: %s" % prepared)
        shutil.copytree(root, prepared, symlinks=True)
        archive = output / ("skipper-browser-%s-pw%s-r%s.tar.gz" % (args.platform, PLAYWRIGHT, REVISION))
        # Normalize ownership/timestamps so approval hashes reproduce.
        def normalize(info):
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = 0
            return info
        with archive.open("wb") as output_file:
            with gzip.GzipFile(filename="", mode="wb", fileobj=output_file, mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w") as packed:
                    for path in sorted(root.iterdir()):
                        packed.add(path, arcname=path.name, filter=normalize)
        manifest["archive_sha256"] = sha256(archive)
        (output / (archive.name + ".manifest.json")).write_text(json.dumps(manifest, indent=2) + "\n")
        print(json.dumps({"archive": str(archive), "sha256": manifest["archive_sha256"],
                          "upload": "not performed; repository-owner approval required"}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", choices=PLATFORMS, required=True)
    parser.add_argument("--playwright", type=Path, required=True, help="unpacked playwright-core package")
    parser.add_argument("--playwright-archive", type=Path, required=True, help="original staged Playwright tarball pinned by resources/upstream.json")
    parser.add_argument("--browser", type=Path, required=True, help="unpacked r1248 headless-shell archive")
    parser.add_argument("--browser-archive", type=Path, required=True, help="original staged Chromium zip pinned by resources/upstream.json")
    parser.add_argument("--font", dest="fonts", type=Path, action="append", required=True)
    parser.add_argument("--font-license", type=Path, required=True, help="license text distributed with the selected fonts")
    parser.add_argument("--native-licenses", type=Path, help="copyright/license notices for the native closure")
    parser.add_argument("--native", type=Path, help="Linux shared library closure, without glibc/loader")
    parser.add_argument("--readelf", type=Path, help="cross-platform llvm-readelf for static Linux dependency-closure verification")
    parser.add_argument("--provenance", type=Path, required=True, help="sources, SHA256 and licenses")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        prepare(args)
    except (ValueError, OSError, KeyError, StopIteration, tarfile.TarError, zipfile.BadZipFile) as error:
        parser.exit(1, "browser bundle preparation failed: %s\n" % error)


if __name__ == "__main__":
    main()
