import hashlib
import tempfile
import unittest
from pathlib import Path

from . import cli
from .clean_output import clean


def file_info(data):
    return {
        "size": len(data), "sha256": hashlib.sha256(data).hexdigest(),
        "git-blob": hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest(),
    }


class FakeGitHub:
    def __init__(self, data):
        self.data = data
        self.calls = []

    def download(self, source, commit, path):
        self.calls.append((commit, path))
        return self.data


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.cache = self.root / "cache"
        self.data = b"// [BEGIN topic_create]\ncreateTopic();\n// [END topic_create]\n"
        self.path = "ydb_tech/topic/main.ts"
        self.source = {
            "repository": "https://github.com/ydb-platform/ydb-js-sdk",
            "ref": {"kind": "branch", "name": "docs/topic-snippets"},
            "include": ["ydb_tech/topic/*.ts"],
        }
        self.manifest = {"version": 1, "staging-root": cli.STAGING_ROOT, "sources": {"javascript": self.source}}
        self.lock = {"version": 1, "sources": {"javascript": {
            "repository": self.source["repository"], "requested-ref": dict(self.source["ref"]),
            "resolved-commit": "a" * 40, "include": list(self.source["include"]),
            "files": {self.path: file_info(self.data)},
        }}}
        self.page = self.root / "ru" / "topic.md"
        self.page.parent.mkdir()
        self.page.write_text("{% code \"/.generated/sdk-snippets/javascript/ydb_tech/topic/main.ts\" "
                             "lang=\"typescript\" lines=\"[BEGIN topic_create]-[END topic_create]\" %}\n")

    def prepare(self, **kwargs):
        cli.prepare(self.root, self.manifest, self.lock, self.cache, **kwargs)

    def test_downloads_only_locked_files_at_locked_commit_and_reuses_offline_cache(self):
        github = FakeGitHub(self.data)
        self.prepare(github=github)
        self.assertEqual(github.calls, [("a" * 40, self.path)])
        self.prepare(offline=True, github=github)
        self.assertEqual(len(github.calls), 1)
        self.assertEqual(cli.validate(self.root, self.manifest, self.lock), 1)

    def test_missing_cache_fails_offline_without_network(self):
        github = FakeGitHub(self.data)
        with self.assertRaisesRegex(cli.SnippetError, "offline cache"):
            self.prepare(offline=True, github=github)
        self.assertEqual(github.calls, [])

    def test_download_checksum_failure_preserves_previous_staging(self):
        self.prepare(github=FakeGitHub(self.data))
        target = self.root / cli.STAGING_ROOT / "javascript" / self.path
        target.write_bytes(b"previous staging")
        cli.cache_file(self.cache, file_info(self.data)["sha256"]).unlink()
        with self.assertRaisesRegex(cli.SnippetError, "SHA-256"):
            self.prepare(github=FakeGitHub(b"tampered content"))
        self.assertEqual(target.read_bytes(), b"previous staging")

    def test_corrupted_cache_fails_instead_of_silently_redownloading(self):
        self.prepare(github=FakeGitHub(self.data))
        cli.cache_file(self.cache, file_info(self.data)["sha256"]).write_bytes(b"bad")
        github = FakeGitHub(self.data)
        with self.assertRaises(cli.SnippetError):
            self.prepare(github=github)
        self.assertEqual(github.calls, [])

    def test_stale_ref_and_allowlist_are_rejected(self):
        self.source["ref"]["name"] = "another-branch"
        with self.assertRaisesRegex(cli.SnippetError, "manifest differs"):
            self.prepare(github=FakeGitHub(self.data))
        self.source["ref"]["name"] = "docs/topic-snippets"
        self.source["include"] = ["ydb_tech/other/*.ts"]
        with self.assertRaisesRegex(cli.SnippetError, "include differs"):
            self.prepare(github=FakeGitHub(self.data))

    def test_lock_rejects_traversal_and_nonallowlisted_paths(self):
        files = self.lock["sources"]["javascript"]["files"]
        for path in ("../main.ts", "/main.ts", "ydb_tech/../main.ts", "ydb_tech/other/main.ts"):
            with self.subTest(path=path):
                files.clear()
                files[path] = file_info(self.data)
                with self.assertRaises(cli.SnippetError):
                    self.prepare(github=FakeGitHub(self.data))

    def test_symlink_in_staging_is_rejected(self):
        staging = self.root / cli.STAGING_ROOT
        staging.parent.mkdir(parents=True)
        staging.symlink_to(self.root / "ru", target_is_directory=True)
        with self.assertRaisesRegex(cli.SnippetError, "symlink"):
            self.prepare(github=FakeGitHub(self.data))

    def test_missing_marker_fails_before_replacing_staging(self):
        self.page.write_text(self.page.read_text().replace("topic_create", "deleted_region"))
        with self.assertRaisesRegex(cli.SnippetError, "referenced region"):
            self.prepare(github=FakeGitHub(self.data))
        self.assertFalse((self.root / cli.STAGING_ROOT).exists())

    def test_lang_and_duplicate_attributes_are_required(self):
        original = self.page.read_text()
        for text in (original.replace('lang="typescript"', ''), original.replace('lang="typescript"', 'lang="typescript" lang="ts"')):
            with self.subTest(text=text):
                self.page.write_text(text)
                with self.assertRaises(cli.SnippetError):
                    self.prepare(github=FakeGitHub(self.data))

    def test_directives_in_code_fences_are_ignored_and_locales_share_source(self):
        reference = self.page.read_text()
        self.page.write_text(reference + "```markdown\n" + reference.replace("topic_create", "missing") + "```\n")
        english = self.root / "en" / "topic.md"
        english.parent.mkdir()
        english.write_text(reference)
        self.prepare(github=FakeGitHub(self.data))
        self.assertEqual(cli.validate(self.root, self.manifest, self.lock), 2)

    def test_plain_staging_path_mentions_are_not_code_references(self):
        self.page.write_text(self.page.read_text() + "The .generated/sdk-snippets directory is ignored.\n")
        self.prepare(github=FakeGitHub(self.data))
        self.assertEqual(cli.validate(self.root, self.manifest, self.lock), 1)

    def test_staging_must_contain_exactly_locked_files(self):
        self.prepare(github=FakeGitHub(self.data))
        (self.root / cli.STAGING_ROOT / "unexpected.txt").write_text("extra")
        with self.assertRaisesRegex(cli.SnippetError, "staging differs"):
            cli.validate(self.root, self.manifest, self.lock)

    def test_git_blob_is_checked_independently_of_sha256(self):
        self.lock["sources"]["javascript"]["files"][self.path]["git-blob"] = "b" * 40
        with self.assertRaisesRegex(cli.SnippetError, "Git blob"):
            self.prepare(github=FakeGitHub(self.data))


class RegionTests(unittest.TestCase):
    def test_invalid_regions_are_rejected(self):
        invalid = [
            "// [BEGIN a]\n// [END a]",
            "// [END a]\nx\n// [BEGIN a]",
            "// [BEGIN a]\n// [BEGIN b]\nx\n// [END b]\n// [END a]",
            "// [BEGIN a]\nx\n// [END b]",
            "// [BEGIN a]\nx",
            "// [BEGIN auth-static]\nx\n// [END auth-static]",
            "// [BEGIN a]\nx\n// [END a]\n// [BEGIN a]\nx\n// [END a]",
        ]
        for source in invalid:
            with self.subTest(source=source), self.assertRaises(cli.SnippetError):
                cli.regions(source, "main.ts")


class OutputTests(unittest.TestCase):
    def test_cleanup_removes_only_sdk_dependencies_and_preserves_rendered_content(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / cli.STAGING_ROOT / "go" / "ydb_tech" / "main.go"
            raw.parent.mkdir(parents=True)
            raw.write_text("SDK source")
            page = root / "index.html"
            page.write_text("Rendered code")
            clean(root)
            self.assertFalse((root / cli.STAGING_ROOT).exists())
            self.assertEqual(page.read_text(), "Rendered code")

    def test_cleanup_rejects_a_staging_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "source"
            target.mkdir()
            staging = root / cli.STAGING_ROOT
            staging.parent.mkdir()
            staging.symlink_to(target, target_is_directory=True)
            with self.assertRaises(ValueError):
                clean(root)
            self.assertTrue(target.exists())


class SourceSelectionTests(unittest.TestCase):
    def test_only_requested_subtree_is_walked(self):
        class TreeGitHub(cli.GitHub):
            def __init__(self):
                self.calls = []

            def api(self, repository, suffix):
                self.calls.append(suffix)
                fixtures = {
                    "git/commits/" + "a" * 40: {"tree": {"sha": "root"}},
                    "git/trees/root": {"tree": [{"path": "ydb_tech", "type": "tree", "sha": "docs"},
                                                   {"path": "src", "type": "tree", "sha": "sdk-code"}]},
                    "git/trees/docs": {"tree": [{"path": "topic", "type": "tree", "sha": "topic"}]},
                    "git/trees/topic?recursive=1": {"tree": [
                        {"path": "main.ts", "type": "blob", "mode": "100644", "sha": "b" * 40, "size": 10},
                        {"path": "README.md", "type": "blob", "mode": "100644", "sha": "c" * 40, "size": 10},
                    ]},
                }
                return fixtures[suffix]
        github = TreeGitHub()
        source = {"repository": "https://github.com/ydb-platform/ydb-js-sdk", "include": ["ydb_tech/topic/*.ts"]}
        self.assertEqual(list(github.files(source, "a" * 40)), ["ydb_tech/topic/main.ts"])
        self.assertNotIn("git/trees/sdk-code", github.calls)

    def test_annotated_tag_is_resolved_to_commit(self):
        class TagGitHub(cli.GitHub):
            def api(self, repository, suffix):
                if suffix == "git/ref/tags/v1":
                    return {"object": {"type": "tag", "sha": "b" * 40}}
                return {"object": {"type": "commit", "sha": "a" * 40}}
        self.assertEqual(TagGitHub().resolve({"repository": "https://github.com/o/r", "ref": {"kind": "tag", "name": "v1"}}), "a" * 40)


if __name__ == "__main__":
    unittest.main()
