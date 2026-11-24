"""Offline provenance regressions; synthetic pins isolate the verification contract."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
from types import SimpleNamespace
import tarfile
import tempfile
import unittest
from unittest import mock
import zipfile

import yatest.common


SOURCE = "ydb/apps/bridge_skipper/acceptance/browser"
PLATFORM = "darwin-arm64"
BROWSER = "browser/chrome-headless-shell-mac-arm64/chrome-headless-shell"
PACKAGE = "node_modules/playwright-core/package.json"


def load_source(name, relative):
    spec = importlib.util.spec_from_file_location(name, yatest.common.source_path(SOURCE + "/" + relative))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class BundleProvenanceTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="skipper-provenance-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.browser_module = load_source("skipper_browser_provenance_target", "test_browser.py")
        self.preparer = load_source("skipper_bundle_preparer_target", "prepare_bundle.py")
        self.bundle = self.root / "bundle"
        for relative, payload in ((BROWSER, b"pinned-browser"), (PACKAGE, b'{"name":"playwright-core","version":"1.64.1"}')):
            path = self.bundle / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        self.manifest = {"schema": 1, "platform": PLATFORM, "playwright_version": "1.64.1",
                         "chromium_headless_shell_revision": "1248", "files": {}}
        self.registration = self.root / "registration.json"
        self.node = self.root / "node-resource"
        self.node.mkdir()
        (self.node / "node").write_bytes(b"test-node")
        self.write_manifest(trust=True)

    def write_manifest(self, trust=False):
        self.manifest["files"] = {str(path.relative_to(self.bundle)): digest(path)
                                  for path in sorted(self.bundle.rglob("*"))
                                  if path.is_file() and path.name != "bundle.json"}
        (self.bundle / "bundle.json").write_text(json.dumps(self.manifest, indent=2) + "\n")
        if trust:
            self.registration.write_text(json.dumps({"schema": 1, "platforms": {PLATFORM: {
                "browser_relative_path": BROWSER,
                "bundle_manifest_sha256": digest(self.bundle / "bundle.json")
            }}}))

    def resolve(self, registered=False):
        resources = {"NODEJS_24_22_1_RESOURCE_GLOBAL": str(self.node)}
        if registered:
            resources["SKIPPER_BROWSER_RESOURCE_GLOBAL"] = str(self.bundle)
        # Default cases explicitly exercise the local override described by D3,
        # independently of the caller's real prepared-bundle environment.
        with mock.patch.dict(os.environ, {"SKIPPER_PREPARED_BROWSER_BUNDLE": str(self.bundle)}), \
                mock.patch.object(self.browser_module.platform, "system", return_value="Darwin"), \
                mock.patch.object(self.browser_module.platform, "machine", return_value="arm64"), \
                mock.patch.object(self.browser_module.yatest.common, "source_path", return_value=str(self.registration)), \
                mock.patch.object(self.browser_module.yatest.common, "global_resources", return_value=resources):
            return self.browser_module.resource_paths()

    def rejected(self, reason):
        return self.assertRaisesRegex(
            AssertionError, "^Invalid pinned browser bundle: " + re.escape(reason) + "$")

    def test_valid_trusted_bundle_is_accepted(self):
        node, bundle, browser = self.resolve()
        self.assertEqual(node, self.node / "node")
        self.assertEqual(bundle, self.bundle)
        self.assertEqual(browser, self.bundle / BROWSER)

    def test_valid_registered_bundle_is_accepted(self):
        node, bundle, browser = self.resolve(registered=True)
        self.assertEqual(node, self.node / "node")
        self.assertEqual(bundle, self.bundle)
        self.assertEqual(browser, self.bundle / BROWSER)

    def test_forged_self_consistent_manifest_cannot_replace_browser(self):
        (self.bundle / BROWSER).write_bytes(b"different-chromium-with-forged-version-label")
        self.write_manifest()  # Self-consistent attacker manifest, trusted pin unchanged.
        with self.rejected("bundle manifest digest differs from registration pin"):
            self.resolve()

    def test_unlisted_regular_file_is_rejected(self):
        (self.bundle / "unlisted-loader.js").write_text("untrusted executable code")
        with self.rejected("unlisted file unlisted-loader.js"):
            self.resolve()

    def test_listed_internal_symlink_is_rejected(self):
        package = self.bundle / PACKAGE
        duplicate = self.bundle / "duplicate-package.json"
        duplicate.write_bytes(package.read_bytes())
        package.unlink()
        package.symlink_to("../../duplicate-package.json")
        self.write_manifest(trust=True)
        with self.rejected("symlink/nonregular node node_modules/playwright-core/package.json"):
            self.resolve()

    def test_unlisted_symlink_is_rejected(self):
        (self.bundle / "unlisted-link").symlink_to(BROWSER)
        with self.rejected("symlink/nonregular node unlisted-link"):
            self.resolve()

    def test_nonregular_node_is_rejected_without_opening_it(self):
        os.mkfifo(self.bundle / "unlisted-fifo")
        with self.rejected("symlink/nonregular node unlisted-fifo"):
            self.resolve()

    def test_parent_traversal_is_rejected_even_if_it_resolves_inside_bundle(self):
        self.manifest["files"].pop(PACKAGE)
        self.manifest["files"]["browser/../" + PACKAGE] = digest(self.bundle / PACKAGE)
        (self.bundle / "bundle.json").write_text(json.dumps(self.manifest) + "\n")
        registration = json.loads(self.registration.read_text())
        registration["platforms"][PLATFORM]["bundle_manifest_sha256"] = digest(self.bundle / "bundle.json")
        self.registration.write_text(json.dumps(registration))
        with self.rejected("noncanonical/traversing manifest path browser/../node_modules/playwright-core/package.json"):
            self.resolve()

    def test_bundle_relative_rejects_parent_traversal_without_inventory_checks(self):
        relative = "browser/../" + PACKAGE
        with self.rejected("noncanonical/traversing manifest path " + relative):
            self.browser_module.bundle_relative(relative)

    def preparation(self):
        package = self.root / "playwright"
        package.mkdir()
        (package / "package.json").write_text('{"name":"playwright-core","version":"1.64.1"}')
        (package / "browsers.json").write_text(json.dumps({
            "browsers": [{
                "name": "chromium-headless-shell",
                "revision": "1248",
                "browserVersion": "156.0.8078.4",
            }],
        }))
        (package / "index.js").write_bytes(b"synthetic-pinned-playwright-code")
        playwright_archive = self.root / "pinned-playwright.tgz"
        with tarfile.open(playwright_archive, "w:gz") as packed:
            packed.add(package, arcname="package")
        staged = self.root / "staged-browser"
        executable = staged / "chrome-headless-shell-mac-arm64/chrome-headless-shell"
        executable.parent.mkdir(parents=True)
        executable.write_bytes(b"authentic-pinned-chromium-archive-content")
        executable.chmod(0o755)
        archive = self.root / "pinned-chromium.zip"
        with zipfile.ZipFile(archive, "w") as packed:
            packed.write(executable, str(executable.relative_to(staged)))
        font, license_file, provenance = self.root / "pinned.ttf", self.root / "LICENSE", self.root / "PROVENANCE.json"
        font.write_bytes(b"synthetic-pinned-font")
        license_file.write_text("synthetic fixture license")
        provenance.write_text('{"fixture":"synthetic provenance"}')
        upstream = self.root / "upstream.json"
        upstream.write_text(json.dumps({
            "playwright": {
                "name": "playwright-core", "version": "1.64.1",
                "sha256": digest(playwright_archive),
            },
            "fonts": [{"name": path.name, "sha256": digest(path)}
                      for path in (font, license_file)],
            "browsers": {PLATFORM: {
                "sha256": digest(archive),
                "revision": "1248",
                "version": "156.0.8078.4",
            }},
        }))
        return SimpleNamespace(
            platform=PLATFORM, playwright=package, playwright_archive=playwright_archive,
            browser=staged, browser_archive=archive,
            fonts=[font], font_license=license_file, provenance=provenance, native=None,
            native_licenses=None, readelf=None, output=self.root / "prepared"), upstream

    def prepare(self, args, upstream):
        # Interface for the repository-owned trusted platform archive lock.
        with mock.patch.object(self.preparer, "UPSTREAM_PATH", upstream, create=True):
            return self.preparer.prepare(args)

    def test_valid_pinned_archive_and_matching_staged_directory_are_accepted(self):
        args, upstream = self.preparation()
        self.prepare(args, upstream)
        manifest = json.loads((args.output / PLATFORM / "bundle.json").read_text())
        self.assertEqual(manifest["platform"], PLATFORM)
        self.assertIn(BROWSER, manifest["files"])

    def test_preparation_rejects_tampered_pinned_archive(self):
        args, upstream = self.preparation()
        args.browser_archive.write_bytes(args.browser_archive.read_bytes() + b"tampered-archive")
        with self.assertRaisesRegex(ValueError, "^pinned Chromium archive SHA256 mismatch$"):
            self.prepare(args, upstream)

    def test_preparation_rejects_other_chromium_in_staged_directory(self):
        args, upstream = self.preparation()
        (args.browser / "chrome-headless-shell-mac-arm64/chrome-headless-shell").write_bytes(b"another browser")
        with self.assertRaisesRegex(
                ValueError, "^staged browser file differs from the pinned Chromium archive: "
                "chrome-headless-shell-mac-arm64/chrome-headless-shell$"):
            self.prepare(args, upstream)

    def test_preparation_rejects_extra_staged_browser_file(self):
        args, upstream = self.preparation()
        (args.browser / "extra-browser-loader").write_bytes(b"unlisted code")
        with self.assertRaisesRegex(
                ValueError, "^staged browser inventory differs from the pinned Chromium archive$"):
            self.prepare(args, upstream)

    def test_preparation_requires_pinned_playwright_archive(self):
        args, upstream = self.preparation()
        args.playwright_archive = None
        with self.assertRaisesRegex(ValueError, "the staged pinned Playwright archive is required"):
            self.prepare(args, upstream)

    def test_preparation_rejects_tampered_playwright_archive(self):
        args, upstream = self.preparation()
        args.playwright_archive.write_bytes(args.playwright_archive.read_bytes() + b"tampered-archive")
        with self.assertRaisesRegex(ValueError, "^pinned Playwright archive SHA256 mismatch$"):
            self.prepare(args, upstream)

    def test_preparation_rejects_playwright_code_with_unchanged_version(self):
        args, upstream = self.preparation()
        (args.playwright / "index.js").write_bytes(b"modified-playwright-code-same-version")
        with self.assertRaisesRegex(ValueError, r"^staged Playwright file differs from the pinned archive: index\.js$"):
            self.prepare(args, upstream)

    def test_preparation_rejects_extra_playwright_code(self):
        args, upstream = self.preparation()
        (args.playwright / "extra-loader.js").write_bytes(b"extra executable code")
        with self.assertRaisesRegex(ValueError, "^staged Playwright inventory differs from the pinned archive$"):
            self.prepare(args, upstream)

    def test_preparation_rejects_missing_playwright_code(self):
        args, upstream = self.preparation()
        (args.playwright / "index.js").unlink()
        with self.assertRaisesRegex(ValueError, "^staged Playwright inventory differs from the pinned archive$"):
            self.prepare(args, upstream)

    def test_preparation_rejects_extra_empty_playwright_directory(self):
        args, upstream = self.preparation()
        (args.playwright / "unlisted-empty-directory").mkdir()
        with self.assertRaisesRegex(ValueError, "^staged Playwright inventory differs from the pinned archive$"):
            self.prepare(args, upstream)

    def add_locked_playwright_archive_member(self, args, upstream, member):
        with tarfile.open(args.playwright_archive, "w:gz") as packed:
            packed.add(args.playwright, arcname="package")
            packed.addfile(member)
        lock = json.loads(upstream.read_text())
        lock["playwright"]["sha256"] = digest(args.playwright_archive)
        upstream.write_text(json.dumps(lock))

    def test_preparation_rejects_traversing_playwright_archive_member(self):
        args, upstream = self.preparation()
        member = tarfile.TarInfo("package/../outside.js")
        self.add_locked_playwright_archive_member(args, upstream, member)
        with self.assertRaisesRegex(
                ValueError, "^" + re.escape("unsafe Playwright archive path: package/../outside.js") + "$"):
            self.prepare(args, upstream)

    def test_preparation_rejects_playwright_archive_symlink(self):
        args, upstream = self.preparation()
        member = tarfile.TarInfo("package/redirect.js")
        member.type = tarfile.SYMTYPE
        member.linkname = "../../outside.js"
        self.add_locked_playwright_archive_member(args, upstream, member)
        with self.assertRaisesRegex(
                ValueError, "^" + re.escape("Playwright archive contains a symlink/nonregular node: package/redirect.js") + "$"):
            self.prepare(args, upstream)

    def test_preparation_rejects_modified_locked_font(self):
        args, upstream = self.preparation()
        args.fonts[0].write_bytes(b"another-font-same-filename-and-suffix")
        with self.assertRaisesRegex(ValueError, r"^pinned font SHA256 mismatch: pinned\.ttf$"):
            self.prepare(args, upstream)

    def test_preparation_rejects_unlocked_ttf_font(self):
        args, upstream = self.preparation()
        other = self.root / "other.ttf"
        other.write_bytes(b"unlocked-font-with-valid-suffix")
        args.fonts = [other]
        with self.assertRaisesRegex(ValueError, r"^font is not locked by upstream\.json: other\.ttf$"):
            self.prepare(args, upstream)

    def test_preparation_rejects_modified_locked_font_license(self):
        args, upstream = self.preparation()
        args.font_license.write_text("another license")
        with self.assertRaisesRegex(ValueError, "^pinned font license SHA256 mismatch: LICENSE$"):
            self.prepare(args, upstream)

    def linux_closure_fixture(self):
        browser = self.root / "linux-browser/chrome-headless-shell"
        browser.parent.mkdir()
        browser.write_bytes(b"\x7fELFsynthetic-browser")
        native = self.root / "native"
        native.mkdir()
        return browser, native

    def test_linux_closure_accepts_recognized_glibc_requirements(self):
        browser, native = self.linux_closure_fixture()

        def readelf(command, **kwargs):
            if "--dynamic" in command:
                return SimpleNamespace(stdout="Shared library: [libc.so.6]\n")
            return SimpleNamespace(stdout=(
                "Version needs section '.gnu.version_r' contains 1 entry:\n"
                "  File: libc.so.6 Cnt: 1\n"
                "  Name: GLIBC_2.17 Flags: none Version: 2\n"))

        with mock.patch.object(self.preparer.subprocess, "run", side_effect=readelf):
            self.preparer.linux_closure(browser, native, readelf=Path("readelf"))

    def test_linux_closure_rejects_unrecognized_version_output(self):
        browser, native = self.linux_closure_fixture()
        for output in ("", "Unsupported symbol version output\n"):
            with self.subTest(output=output), \
                    mock.patch.object(self.preparer.subprocess, "run", return_value=SimpleNamespace(stdout=output)):
                with self.assertRaises(ValueError):
                    self.preparer.linux_closure(browser, native, readelf=Path("readelf"))
