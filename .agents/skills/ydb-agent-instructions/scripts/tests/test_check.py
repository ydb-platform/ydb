#!/usr/bin/env python3
"""Tests for check.py. Run: python3.9 -m unittest discover -s <this dir>"""
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


def write(path, text, encoding="utf-8"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding=encoding) as handle:
        handle.write(text)


def skill_text(name, description="Demo skill. Not for real use.", body="# Demo\n\nText.\n"):
    return "---\nname: %s\ndescription: \"%s\"\n---\n\n%s" % (name, description, body)


class Fixture(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="check-test-")
        write(os.path.join(self.root, ".gitignore"), GITIGNORE)
        write(os.path.join(self.root, "ydb", "core", "base", "README.md"), "# base\n")
        self.has_git = shutil.which("git") is not None
        if self.has_git:
            subprocess.run(["git", "init", "-q", self.root], check=True)

    def tearDown(self):
        shutil.rmtree(self.root)

    def make_skill(self, rel_dir="ydb/foo", name="ydb-demo-skill", with_claude_md=True, agents_text=None, body=None):
        owner = os.path.join(self.root, rel_dir)
        skill_md = os.path.join(owner, ".agents", "skills", name, "SKILL.md")
        write(skill_md, skill_text(name, body=body) if body else skill_text(name))
        write(os.path.join(owner, "AGENTS.md"), agents_text if agents_text is not None else "# Foo\n\nA rule.\n")
        if with_claude_md:
            write(os.path.join(owner, "CLAUDE.md"), "%s\n\n%s\n- .agents/skills/%s/SKILL.md: Demo skill. Not for real use.\n" % (check.CLAUDE_INCLUDE, check.CLAUDE_SKILLS_HEADER, name))
        return owner, skill_md

    def run_check(self, *paths, **kwargs):
        out = io.StringIO()
        err = io.StringIO()
        extra = ["--warnings-as-errors"] if kwargs.get("strict") else []
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = check.main(list(paths) + ["--root", self.root] + extra)
        return code, out.getvalue() + err.getvalue()


class SkillTests(Fixture):
    def test_clean_skill_has_no_findings(self):
        owner, _ = self.make_skill()
        code, out = self.run_check(owner, strict=True)
        self.assertEqual(code, 0, out)
        self.assertIn("0 errors, 0 warnings", out)

    def test_name_must_match_folder(self):
        owner, skill_md = self.make_skill()
        write(skill_md, skill_text("ydb-other-name"))
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("must equal the folder name", out)

    def test_name_rules(self):
        for bad in ("demo-skill", "ydb-", "ydb-a--b", "ydb-a-", "ydb-A", "ydb-" + "x" * 70):
            owner = os.path.join(self.root, "ydb", "bad")
            shutil.rmtree(owner, ignore_errors=True)
            write(os.path.join(owner, ".agents", "skills", bad, "SKILL.md"), skill_text(bad))
            write(os.path.join(owner, "AGENTS.md"), "# Bad\n\nA rule.\n")
            code, out = self.run_check(owner)
            self.assertEqual(code, 1, bad)
            self.assertTrue("name must be ydb-" in out or "limit is 64" in out, (bad, out))

    def test_duplicate_name_in_repo_is_error(self):
        if not self.has_git:
            self.skipTest("git not installed")
        owner, _ = self.make_skill()
        self.make_skill(rel_dir="ydb/other")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("already used by", out)

    def test_missing_and_unclosed_frontmatter_are_errors(self):
        owner, skill_md = self.make_skill()
        write(skill_md, "# No frontmatter\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("frontmatter must start with ---", out)
        write(skill_md, "---\nname: ydb-demo-skill\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("not closed", out)

    def test_bom_is_accepted(self):
        owner, skill_md = self.make_skill()
        write(skill_md, "﻿" + skill_text("ydb-demo-skill"))
        code, out = self.run_check(owner, strict=True)
        self.assertEqual(code, 0, out)

    def test_non_utf8_is_error_not_a_crash(self):
        owner, skill_md = self.make_skill()
        with open(skill_md, "wb") as handle:
            handle.write(b"---\nname: ydb-demo-skill\ndescription: \"caf\xe9\"\n---\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("not valid UTF-8", out)

    def test_description_rules(self):
        owner, skill_md = self.make_skill()
        write(skill_md, skill_text("ydb-demo-skill", description="x" * (check.DESCRIPTION_MAX_CHARS + 1)))
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("characters; limit is", out)
        write(skill_md, skill_text("ydb-demo-skill", description="TODO: describe"))
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("description still holds TODO", out)
        # a changed description is checked on the skill file alone; CLAUDE.md would need the new text
        write(skill_md, "---\nname: ydb-demo-skill\ndescription: \"first line\n  second line\"\n---\n\n# D\n")
        code, out = self.run_check(skill_md, strict=True)
        self.assertEqual(code, 0, out)
        write(skill_md, "---\nname: ydb-demo-skill\ndescription: >\n  first\n  second\n---\n\n# D\n")
        code, out = self.run_check(skill_md, strict=True)
        self.assertEqual(code, 0, out)

    def test_extra_frontmatter_keys_are_allowed(self):
        owner, skill_md = self.make_skill()
        write(skill_md, "---\nname: ydb-demo-skill\ndescription: \"Demo skill. Not for real use.\"\nallowed-tools: Read Grep\npaths:\n  - \"**/*.md\"\nmetadata:\n  owner: x\n---\n\n# D\n")
        code, out = self.run_check(owner, strict=True)
        self.assertEqual(code, 0, out)

    def test_claude_md_must_follow_a_changed_description(self):
        owner, skill_md = self.make_skill()
        write(skill_md, skill_text("ydb-demo-skill", description="New text."))
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("must list the skill ydb-demo-skill as: - .agents/skills/ydb-demo-skill/SKILL.md: New text.", out)

    def test_todo_line_is_warning(self):
        owner, _ = self.make_skill(body="# D\n\nTODO: fill me.\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0)
        self.assertIn("TODO line left", out)

    def test_sibling_files(self):
        owner, _ = self.make_skill()
        os.remove(os.path.join(owner, "AGENTS.md"))
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("every skill needs a sibling AGENTS.md", out)
        owner, _ = self.make_skill(rel_dir="ydb/bar", name="ydb-bar-skill", with_claude_md=False)
        code, out = self.run_check(owner)
        self.assertEqual(code, 0)
        self.assertIn("CLAUDE.md:0: missing", out)

    def test_claude_md_content(self):
        owner, _ = self.make_skill()
        claude_md = os.path.join(owner, "CLAUDE.md")
        write(claude_md, "Some rules\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("must contain the line @./AGENTS.md", out)
        write(claude_md, "@./AGENTS.md\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("must contain the line: Skills, read the one", out)
        self.assertIn("must list the skill ydb-demo-skill as: - .agents/skills/ydb-demo-skill/SKILL.md: Demo skill. Not for real use.", out)
        write(claude_md, "%s\n%s\n- .agents/skills/ydb-demo-skill/SKILL.md: Demo skill. Not for real use.\nExtra.\n" % (check.CLAUDE_INCLUDE, check.CLAUDE_SKILLS_HEADER))
        code, out = self.run_check(owner)
        self.assertEqual(code, 0)
        self.assertIn("has extra content", out)
        path = os.path.join(self.root, "ydb", "plain", "CLAUDE.md")
        write(path, "@./AGENTS.md\n")
        write(os.path.join(self.root, "ydb", "plain", "AGENTS.md"), "# Plain\n")
        code, out = self.run_check(path, strict=True)
        self.assertEqual(code, 0, out)

    def test_claude_symlink_and_claude_skills_are_errors(self):
        owner, _ = self.make_skill()
        os.symlink(".agents", os.path.join(owner, ".claude"))
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("is a symlink", out)
        os.remove(os.path.join(owner, ".claude"))
        write(os.path.join(owner, ".claude", "skills", "x", "SKILL.md"), "x\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("skills live only in .agents/skills", out)

    def test_paths_are_checked_outside_code(self):
        body = "# D\n\nRead ydb/core/base/README.md, [x](missing.md \"t\") and ydb/nope/missing.md, not `ydb/nope/in-code.md` or ydb/nope/<name>.md.\n\n```bash\ncat ydb/nope/example.md\n```\n"
        owner, _ = self.make_skill(body=body)
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("path does not exist: missing.md", out)
        self.assertIn("path does not exist: ydb/nope/missing.md", out)
        for absent in ("README.md", "in-code.md", "<name>", "example.md"):
            self.assertNotIn(absent, out)

    def test_broken_path_in_reference_is_error(self):
        owner, skill_md = self.make_skill()
        write(os.path.join(os.path.dirname(skill_md), "references", "more.md"), "See gone/file.md.\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("references/more.md", out)

    def test_long_sentence_is_warning(self):
        owner, _ = self.make_skill(body="# D\n\n" + " ".join(["word"] * (check.SENTENCE_MAX_WORDS + 1)) + ".\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 0)
        self.assertIn("sentence longer than", out)

    def test_ignored_files_are_errors(self):
        if not self.has_git:
            self.skipTest("git not installed")
        owner, skill_md = self.make_skill()
        write(os.path.join(os.path.dirname(skill_md), "references", "more.md"), "text\n")
        write(os.path.join(self.root, ".gitignore"), GITIGNORE + "references/\nskills/\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("references/more.md:0: is ignored by git", out)
        self.assertEqual(out.count("SKILL.md:0: is ignored by git"), 1)


class ScriptTests(Fixture):
    def script_path(self, owner, name="tool.py"):
        return os.path.join(owner, ".agents", "skills", "ydb-demo-skill", "scripts", name)

    def with_test(self, owner):
        write(self.script_path(owner, "tests/test_tool.py"), "#!/usr/bin/env python3\nimport tool\n")

    def test_script_without_tests_is_error(self):
        owner, _ = self.make_skill()
        write(self.script_path(owner), "#!/usr/bin/env python3\nimport os\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("has no tests", out)

    def test_script_with_tests_and_stdlib_imports_is_clean(self):
        owner, _ = self.make_skill()
        write(self.script_path(owner), "#!/usr/bin/env python3\nimport os\nimport json\nimport sys\nimport subprocess\nfrom collections import OrderedDict\n")
        self.with_test(owner)
        code, out = self.run_check(owner, strict=True)
        self.assertEqual(code, 0, out)

    def test_init_and_helpers_need_no_tests_but_are_checked(self):
        owner, _ = self.make_skill()
        write(self.script_path(owner, "__init__.py"), "#!/usr/bin/env python3\n")
        write(self.script_path(owner, "lib/helper.py"), "#!/usr/bin/env python3\nimport notarealmodule_xyz\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertNotIn("test___init__", out)
        self.assertNotIn("test_helper", out)
        self.assertIn("lib/helper.py:2: import 'notarealmodule_xyz'", out)

    def test_new_grammar_and_third_party_import_are_errors(self):
        owner, _ = self.make_skill()
        self.with_test(owner)
        write(self.script_path(owner), "#!/usr/bin/env python3\nmatch 1:\n    case 1: pass\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("not valid Python 3.9 syntax", out)
        write(self.script_path(owner), "#!/usr/bin/env python3\nimport yaml\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("not in the standard library", out)

    def test_scripts_are_checked_even_with_broken_frontmatter(self):
        owner, skill_md = self.make_skill()
        write(skill_md, "# no frontmatter\n")
        write(self.script_path(owner), "#!/usr/bin/env python3\n")
        code, out = self.run_check(owner)
        self.assertEqual(code, 1)
        self.assertIn("has no tests", out)

    def test_missing_shebang_is_warning(self):
        owner, _ = self.make_skill()
        write(self.script_path(owner), "import os\n")
        self.with_test(owner)
        code, out = self.run_check(owner)
        self.assertEqual(code, 0)
        self.assertIn("first line should be", out)


class AgentsAndClaudeTests(Fixture):
    def test_long_agents_is_warning(self):
        for rel in ("AGENTS.md", os.path.join("ydb", "bar", "AGENTS.md")):
            path = os.path.join(self.root, rel)
            write(path, "# Title\n" + "- line\n" * check.AGENTS_MAX_LINES)
            code, out = self.run_check(path)
            self.assertEqual(code, 0)
            self.assertIn("budget is %d" % check.AGENTS_MAX_LINES, out)

    def test_todo_in_agents_is_warning(self):
        path = os.path.join(self.root, "ydb", "bar", "AGENTS.md")
        write(path, "# Bar\n\nTODO: rules.\n")
        code, out = self.run_check(path)
        self.assertEqual(code, 0)
        self.assertIn("TODO line left", out)

    def test_agents_chain_over_limit_is_error(self):
        write(os.path.join(self.root, "AGENTS.md"), "x" * (check.AGENTS_CHAIN_MAX_BYTES + 1))
        path = os.path.join(self.root, "ydb", "bar", "AGENTS.md")
        write(path, "# Bar\n")
        code, out = self.run_check(path)
        self.assertEqual(code, 1)
        self.assertIn("Codex drops files", out)

    def test_claude_include_needs_agents_md(self):
        path = os.path.join(self.root, "ydb", "bar", "CLAUDE.md")
        write(path, "@./AGENTS.md\n")
        code, out = self.run_check(path)
        self.assertEqual(code, 1)
        self.assertIn("AGENTS.md does not exist", out)

    def test_other_file_is_a_warning(self):
        path = os.path.join(self.root, "ydb", "bar", "README.md")
        write(path, "# Bar\n")
        code, out = self.run_check(path)
        self.assertEqual(code, 0)
        self.assertIn("not an instruction file", out)

    def test_usage_errors_exit_2(self):
        code, out = self.run_check(os.path.join(self.root, "nope"))
        self.assertEqual(code, 2)
        self.assertIn("path does not exist", out)
        with self.assertRaises(SystemExit) as raised, contextlib.redirect_stderr(io.StringIO()):
            check.main([])
        self.assertEqual(raised.exception.code, 2)

    def test_without_git_checks_are_skipped_with_a_warning(self):
        owner, _ = self.make_skill()
        original = check.run_git
        check.run_git = lambda *args, **kwargs: None
        try:
            code, out = self.run_check(owner)
        finally:
            check.run_git = original
        self.assertEqual(code, 0, out)
        self.assertIn("git is not installed", out)


if __name__ == "__main__":
    unittest.main()
