#!/usr/bin/env python3
"""Tests for check.py. Run: python3.8 -m unittest discover -s <this dir>"""
import contextlib
import io
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import check  # noqa: E402

GITIGNORE = "*\n!*.*\n!*/\n.claude/\n"


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)


def skill_text(name, description="Demo skill. Not for real use.", body="# Demo\n\nText.\n"):
    return "---\nname: %s\ndescription: \"%s\"\n---\n\n%s" % (name, description, body)


class Fixture(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="check-test-")
        write(os.path.join(self.root, ".gitignore"), GITIGNORE)
        write(os.path.join(self.root, "ydb", "agents", "GUIDE.md"), "# guide\n")
        self.has_git = shutil.which("git") is not None
        if self.has_git:
            subprocess.run(["git", "init", "-q", self.root], check=True)

    def tearDown(self):
        shutil.rmtree(self.root)

    def make_skill(self, rel_dir="ydb/foo", name="ydb-demo-skill", with_claude_md=True, with_symlink=True, agents_text=None, body=None):
        owner = os.path.join(self.root, rel_dir)
        skill_md = os.path.join(owner, ".agents", "skills", name, "SKILL.md")
        write(skill_md, skill_text(name, body=body) if body else skill_text(name))
        link = ".agents/skills/%s/SKILL.md" % name
        write(os.path.join(owner, "AGENTS.md"), agents_text if agents_text is not None else "# Foo\n\nRead the [skill](%s).\n" % link)
        if with_claude_md:
            write(os.path.join(owner, "CLAUDE.md"), "@./AGENTS.md\n")
        if with_symlink:
            os.symlink(".agents", os.path.join(owner, ".claude"))
        return owner, skill_md

    def run_check(self, *paths):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = check.main(list(paths) + ["--root", self.root])
        return code, out.getvalue()


class CheckSkillTests(Fixture):
    def test_clean_skill_has_no_findings(self):
        owner, _ = self.make_skill()
        code, out = self.run_check(owner)
        self.assertEqual(code, 0, out)
        self.assertIn("0 errors, 0 warnings", out)

    def test_name_must_match_folder(self):
        owner, skill_md = self.make_skill()
        write(skill_md, skill_text("other-name"))
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("must equal the folder name", out)

    def test_name_without_prefix_is_error(self):
        owner = os.path.join(self.root, "ydb", "foo")
        write(os.path.join(owner, ".agents", "skills", "demo-skill", "SKILL.md"), skill_text("demo-skill"))
        write(os.path.join(owner, "AGENTS.md"), "# Foo\n\n[s](.agents/skills/demo-skill/SKILL.md)\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("must start with ydb-", out)

    def test_duplicate_name_in_repo_is_error(self):
        if not self.has_git:
            self.skipTest("git not installed")
        owner, _ = self.make_skill()
        self.make_skill(rel_dir="ydb/other", with_symlink=True)
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("already used by", out)

    def test_similar_name_is_warning(self):
        if not self.has_git:
            self.skipTest("git not installed")
        owner, _ = self.make_skill()
        self.make_skill(rel_dir="ydb/other", name="ydb-demo-skills")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0, out)
        self.assertIn("looks like ydb-demo-skills", out)

    def test_missing_frontmatter_is_error(self):
        owner, skill_md = self.make_skill()
        write(skill_md, "# No frontmatter\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("frontmatter must start with ---", out)

    def test_unclosed_frontmatter_is_error(self):
        owner, skill_md = self.make_skill()
        write(skill_md, "---\nname: ydb-demo-skill\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("not closed", out)

    def test_long_description_is_error(self):
        owner, skill_md = self.make_skill()
        write(skill_md, skill_text("ydb-demo-skill", description="x" * (check.DESCRIPTION_MAX_CHARS + 1)))
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("characters; limit is", out)

    def test_extra_frontmatter_key_is_warning(self):
        owner, skill_md = self.make_skill()
        write(skill_md, "---\nname: ydb-demo-skill\ndescription: \"d\"\nmodel: opus\n---\n\n# D\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0)
        self.assertIn("not portable", out)

    def test_block_scalar_description_is_read(self):
        owner, skill_md = self.make_skill()
        write(skill_md, "---\nname: ydb-demo-skill\ndescription: >\n  first line\n  second line\n---\n\n# D\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0, out)

    def test_missing_sibling_agents_md_is_error(self):
        owner, _ = self.make_skill()
        os.remove(os.path.join(owner, "AGENTS.md"))
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("every skill needs a sibling AGENTS.md", out)

    def test_agents_md_without_pointer_is_warning(self):
        owner, _ = self.make_skill(agents_text="# Foo\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0)
        self.assertIn("does not link to", out)

    def test_claude_md_with_other_content_is_error(self):
        owner, _ = self.make_skill()
        write(os.path.join(owner, "CLAUDE.md"), "Some rules\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("must contain the single line", out)

    def test_missing_claude_md_and_symlink_are_warnings(self):
        owner, _ = self.make_skill(with_claude_md=False, with_symlink=False)
        code, out = self.run_check(owner)
        self.assertEqual(code, 0)
        self.assertIn("CLAUDE.md:0: missing", out)
        self.assertIn(".claude:0: missing", out)

    def test_real_claude_dir_is_error(self):
        owner, _ = self.make_skill(with_symlink=False)
        os.mkdir(os.path.join(owner, ".claude"))
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("is a real directory", out)

    def test_wrong_symlink_target_is_error(self):
        owner, _ = self.make_skill(with_symlink=False)
        os.symlink("elsewhere", os.path.join(owner, ".claude"))
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("must point to .agents", out)

    def test_broken_link_is_error_and_code_span_is_ignored(self):
        owner, _ = self.make_skill(body="# D\n\nSee [x](missing.md) and `[y](also-missing.md)`.\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("missing.md", out)
        self.assertNotIn("also-missing.md", out)

    def test_long_sentence_is_warning(self):
        owner, _ = self.make_skill(body="# D\n\n" + " ".join(["word"] * (check.SENTENCE_MAX_WORDS + 1)) + ".\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0)
        self.assertIn("sentence longer than", out)

    def test_ignored_symlink_is_error(self):
        if not self.has_git:
            self.skipTest("git not installed")
        owner, _ = self.make_skill()
        write(os.path.join(self.root, ".gitignore"), "*\n!*.*\n!*/\n.claude\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("ignored by git", out)


    def test_todo_line_is_warning(self):
        owner, _ = self.make_skill(body="# D\n\nTODO: fill me.\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0)
        self.assertIn("TODO line left", out)

    def test_link_with_title_is_checked(self):
        owner, _ = self.make_skill(body="# D\n\nSee [x](missing.md \"title\").\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("missing.md", out)

    def test_ignored_skill_md_is_reported_once(self):
        if not self.has_git:
            self.skipTest("git not installed")
        owner, _ = self.make_skill()
        write(os.path.join(self.root, ".gitignore"), GITIGNORE + "skills/\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertEqual(out.count("SKILL.md:0: is ignored by git"), 1)

    def test_multiline_quoted_description_is_read(self):
        owner, skill_md = self.make_skill()
        write(skill_md, "---\nname: ydb-demo-skill\ndescription: \"first line\n  second line\"\n---\n\n# D\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0, out)

    def test_link_in_fenced_block_is_ignored(self):
        owner, _ = self.make_skill(body="# D\n\n```markdown\n[x](missing.md)\n```\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0, out)

    def test_broken_link_in_reference_is_error(self):
        owner, skill_md = self.make_skill()
        write(os.path.join(os.path.dirname(skill_md), "references", "more.md"), "See [x](gone.md).\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("references/more.md", out)

    def test_claude_md_with_include_and_extra_text_is_warning(self):
        owner, _ = self.make_skill()
        write(os.path.join(owner, "CLAUDE.md"), "@./AGENTS.md\n\nExtra.\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0)
        self.assertIn("has extra content", out)

    def test_warnings_as_errors(self):
        owner, _ = self.make_skill(with_claude_md=False)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = check.main([owner, "--root", self.root, "--warnings-as-errors"])
        self.assertEqual(code, 1)

    def test_ignored_reference_file_is_error(self):
        if not self.has_git:
            self.skipTest("git not installed")
        owner, skill_md = self.make_skill()
        write(os.path.join(os.path.dirname(skill_md), "references", "more.md"), "text\n")
        write(os.path.join(self.root, ".gitignore"), GITIGNORE + "references/\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("references/more.md:0: is ignored by git", out)


class CheckScriptTests(Fixture):
    def script_path(self, owner, name="tool.py"):
        return os.path.join(owner, ".agents", "skills", "ydb-demo-skill", "scripts", name)

    def test_script_without_tests_is_error(self):
        owner, _ = self.make_skill()
        write(self.script_path(owner), "#!/usr/bin/env python3\nimport os\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("has no tests", out)

    def test_script_with_tests_is_clean(self):
        owner, _ = self.make_skill()
        write(self.script_path(owner), "#!/usr/bin/env python3\nimport os\n")
        write(self.script_path(owner, "tests/test_tool.py"), "#!/usr/bin/env python3\nimport tool\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0, out)

    def test_new_grammar_is_error(self):
        owner, _ = self.make_skill()
        write(self.script_path(owner), "#!/usr/bin/env python3\nmatch 1:\n    case 1: pass\n")
        write(self.script_path(owner, "tests/test_tool.py"), "#!/usr/bin/env python3\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("not valid Python 3.8 syntax", out)

    def test_stdlib_imports_are_accepted(self):
        owner, _ = self.make_skill()
        write(self.script_path(owner), "#!/usr/bin/env python3\nimport os\nimport json\nimport sys\nimport subprocess\nfrom collections import OrderedDict\n")
        write(self.script_path(owner, "tests/test_tool.py"), "#!/usr/bin/env python3\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0, out)

    def test_third_party_import_is_error(self):
        owner, _ = self.make_skill()
        write(self.script_path(owner), "#!/usr/bin/env python3\nimport yaml\n")
        write(self.script_path(owner, "tests/test_tool.py"), "#!/usr/bin/env python3\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("not in the standard library", out)

    def test_unknown_module_is_error(self):
        owner, _ = self.make_skill()
        write(self.script_path(owner), "#!/usr/bin/env python3\nimport notarealmodule_xyz\n")
        write(self.script_path(owner, "tests/test_tool.py"), "#!/usr/bin/env python3\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("notarealmodule_xyz", out)

    def test_init_py_needs_no_tests(self):
        owner, _ = self.make_skill()
        write(self.script_path(owner, "__init__.py"), "#!/usr/bin/env python3\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0, out)

    def test_scripts_are_checked_even_with_broken_frontmatter(self):
        owner, skill_md = self.make_skill()
        write(skill_md, "# no frontmatter\n")
        write(self.script_path(owner), "#!/usr/bin/env python3\n")
        code, out = self.run_check(owner)
        self.assertIn("has no tests", out)

    def test_missing_shebang_is_warning(self):
        owner, _ = self.make_skill()
        write(self.script_path(owner), "import os\n")
        write(self.script_path(owner, "tests/test_tool.py"), "#!/usr/bin/env python3\n")
        code, out = self.run_check(owner)
        self.assertIn("first line should be", out)


class CheckAgentsAndClaudeTests(Fixture):
    def test_long_agents_is_warning(self):
        for rel in ("AGENTS.md", os.path.join("ydb", "bar", "AGENTS.md")):
            path = os.path.join(self.root, rel)
            write(path, "# Title\n" + "- line\n" * check.AGENTS_MAX_LINES)
            code, out = self.run_check(path)
            self.assertEqual(code, 0)
            self.assertIn("budget is %d" % check.AGENTS_MAX_LINES, out)

    def test_other_file_is_not_checked(self):
        path = os.path.join(self.root, "ydb", "bar", "README.md")
        write(path, "# Bar\n")
        code, out = self.run_check(path)
        self.assertEqual(code, 0)
        self.assertIn("not an instruction file", out)

    def test_agents_chain_over_limit_is_error(self):
        write(os.path.join(self.root, "AGENTS.md"), "x" * (check.AGENTS_CHAIN_MAX_BYTES + 1))
        path = os.path.join(self.root, "ydb", "bar", "AGENTS.md")
        write(path, "# Bar\n")
        code, out = self.run_check(path)
        self.assertEqual(code, 1)
        self.assertIn("Codex stops reading", out)

    def test_claude_include_needs_agents_md(self):
        path = os.path.join(self.root, "ydb", "bar", "CLAUDE.md")
        write(path, "@./AGENTS.md\n")
        code, out = self.run_check(path)
        self.assertEqual(code, 1)
        self.assertIn("AGENTS.md does not exist", out)

    def test_missing_path_is_error_and_usage_without_path_is_2(self):
        code, out = self.run_check(os.path.join(self.root, "nope"))
        self.assertEqual(code, 1)
        with self.assertRaises(SystemExit) as raised:
            check.main([])
        self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
