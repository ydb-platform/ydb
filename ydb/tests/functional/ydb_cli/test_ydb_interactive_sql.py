# -*- coding: utf-8 -*-
"""
Functional tests for YDB CLI YQL interactive mode.
"""

import pexpect
import time
import logging
import uuid
import yaml

from pathlib import Path
from typing import List, Optional
from ydb.tests.functional.ydb_cli.ydb_cli_interactive_helpers import BaseInteractiveTest

logger = logging.getLogger(__name__)


class BaseSqlInteractiveTest(BaseInteractiveTest):
    @classmethod
    def run_interactive_session(cls, queries: List[str], tmp_path: Path, extra_args: List[str] = None, endpoint: Optional[str] = None) -> BaseInteractiveTest.ExecutionResult:
        extra_args = cls._with_profile_isolation(extra_args)

        stdin_content = "\n".join(queries) + "\n"
        stdin_path = str(tmp_path / f"interactive_stdin_{uuid.uuid4().hex}.txt")
        with open(stdin_path, "w") as f:
            f.write(stdin_content)

        with open(stdin_path, "r") as stdin_file:
            result = cls.execute_ydb_cli_command(extra_args, endpoint=endpoint, stdin=stdin_file, check_exit_code=False)

        logger.debug("stdin:\n%s", stdin_content)
        return result

    def _wait_for_prompt(self, child):
        """Wait for the YQL interactive prompt to appear."""
        child.expect("YQL\033\\[22;39m>", timeout=self.PROMPT_TIMEOUT)

    @classmethod
    def create_profile(cls, profile_name: str, profile_file: str, active: bool = False):
        profile_data = {
            "profiles": {
                profile_name: {
                    "endpoint": cls.grpc_endpoint(),
                    "database": cls.root_dir,
                }
            }
        }

        if active:
            profile_data["active_profile"] = profile_name

        with open(profile_file, "w") as f:
            yaml.dump(profile_data, f)


# =============================================================================
# Welcome message
# =============================================================================


class TestInteractiveWelcomeMessage(BaseSqlInteractiveTest):
    """Verify the welcome banner shown when entering interactive mode."""

    def test_welcome_message_contains_ydb_cli(self):
        result = self.run_interactive_session(["exit"], self.tmp_path)
        assert result.exit_code == 0
        assert "Welcome to YDB CLI" in result.stdout
        assert "YQL" in result.stdout
        assert "localhost:{}".format(self.grpc_port()) in result.stdout
        assert self.root_dir in result.stdout
        assert "YDB Server" in result.stdout
        assert "/help" in result.stdout
        assert "Bye!" in result.stdout

    def test_welcome_message_shows_connection_profile(self):
        profile_name = "my_test_profile"
        profile_file = str(self.tmp_path / "profiles.yaml")
        self.create_profile(profile_name, profile_file)

        result = self.run_interactive_session(["exit"], self.tmp_path, extra_args=["--profile-file", profile_file, "--profile", profile_name])
        assert result.exit_code == 0
        assert f"Connection profile: {profile_name}" in result.stdout

    def test_welcome_message_shows_active_profile(self):
        profile_name = "my_active_profile"
        profile_file = str(self.tmp_path / "profiles_active.yaml")
        self.create_profile(profile_name, profile_file, active=True)

        result = self.run_interactive_session(["exit"], self.tmp_path, extra_args=["--profile-file", profile_file])
        assert result.exit_code == 0
        assert f"Connection profile: {profile_name}" in result.stdout

    def test_connection_failure_prints_error(self):
        result = self.run_interactive_session(["exit"], self.tmp_path, endpoint="grpc://localhost:1")
        assert result.exit_code != 0

        combined = result.stdout + result.stderr
        assert "Status: TRANSPORT_UNAVAILABLE" in combined
        assert "Issues:" in combined
        assert "Grpc error response on endpoint localhost:1" in combined


# =============================================================================
# Exit / quit
# =============================================================================


class TestInteractiveExitCommands(BaseSqlInteractiveTest):
    """Verify that 'exit' and 'quit' terminate the session cleanly."""

    def test_exit_command_lowercase(self):
        result = self.run_interactive_session(["exit"], self.tmp_path)
        assert result.exit_code == 0
        assert "Bye!" in result.stdout

    def test_exit_command_uppercase(self):
        result = self.run_interactive_session(["EXIT"], self.tmp_path)
        assert result.exit_code == 0
        assert "Bye!" in result.stdout

    def test_exit_command_mixed_case(self):
        result = self.run_interactive_session(["Exit"], self.tmp_path)
        assert result.exit_code == 0
        assert "Bye!" in result.stdout

    def test_quit_command_lowercase(self):
        result = self.run_interactive_session(["quit"], self.tmp_path)
        assert result.exit_code == 0
        assert "Bye!" in result.stdout

    def test_quit_command_uppercase(self):
        result = self.run_interactive_session(["QUIT"], self.tmp_path)
        assert result.exit_code == 0
        assert "Bye!" in result.stdout

    def test_quit_command_mixed_case(self):
        result = self.run_interactive_session(["Quit"], self.tmp_path)
        assert result.exit_code == 0
        assert "Bye!" in result.stdout

    def test_eof_exits_cleanly(self):
        """EOF on stdin (Ctrl+D equivalent) should terminate the session."""
        result = self.run_interactive_session([], self.tmp_path)
        assert result.exit_code == 0
        assert "Bye!" in result.stdout

    def test_single_ctrl_c_keeps_session_alive(self):
        """A single Ctrl+C cancels the current input but does not exit the session."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.sendcontrol("c")
            self._wait_for_prompt(child)
            # Sleep past the double-Ctrl+C window (5s in line_reader.cpp) so the next
            # Ctrl+C is treated as a cancel rather than an exit signal.
            time.sleep(5)
            child.sendcontrol("c")
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
            child.expect(pexpect.EOF, timeout=5)
        finally:
            child.close()

    def test_double_ctrl_c_exits(self):
        """Two Ctrl+Cs sent within the double-Ctrl+C window terminate the session."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.sendcontrol("c")
            self._wait_for_prompt(child)
            child.sendcontrol("c")
            child.expect("Bye!", timeout=10)
            child.expect(pexpect.EOF, timeout=5)
        finally:
            child.close()

    def test_consequence_ctrl_c_without_prompt(self):
        """A burst of Ctrl+Cs sent without waiting for prompt redraw must terminate the session cleanly."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            # Send all four Ctrl+Cs in a single write so they land in the child's stdin
            # buffer atomically. This avoids pexpect's per-call delaybeforesend inflating
            # the gap between consecutive cancels beyond the double-Ctrl+C window.
            child.send("\x03\x03\x03\x03")
            child.expect("Bye!", timeout=10)
            child.expect(pexpect.EOF, timeout=5)
        finally:
            child.close()


# =============================================================================
# SQL query execution
# =============================================================================


class TestInteractiveSqlExecution(BaseSqlInteractiveTest):
    """Verify that SQL queries are executed and results are printed."""

    def test_simple_select_1(self):
        result = self.run_interactive_session(["SELECT 1;", "exit"], self.tmp_path)
        assert result.exit_code == 0
        assert "1" in result.stdout

    def test_select_unicode_string_literal(self):
        result = self.run_interactive_session(['SELECT "Строка с другой кодировкой"u;', "exit"], self.tmp_path)
        assert result.exit_code == 0
        assert "Строка с другой кодировкой" in result.stdout

    def test_select_multiple_columns(self):
        result = self.run_interactive_session(
            ["SELECT 1 AS a, 2 AS b, 3 AS c;", "SELECT 42 AS x, 43 AS y;", "SELECT 'abcd' AS str_col, -1 AS numeric_col;", "exit"], self.tmp_path
        )
        assert result.exit_code == 0

        # Columns
        assert "a" in result.stdout
        assert "b" in result.stdout
        assert "c" in result.stdout
        assert "x" in result.stdout
        assert "y" in result.stdout
        assert "str_col" in result.stdout
        assert "numeric_col" in result.stdout

        # Results
        assert "1" in result.stdout
        assert "2" in result.stdout
        assert "3" in result.stdout
        assert "42" in result.stdout
        assert "43" in result.stdout
        assert "abcd" in result.stdout
        assert "-1" in result.stdout

    def test_empty_lines_are_ignored(self):
        result = self.run_interactive_session(
            ["", "", "SELECT 42;", "   ", "\t", "", "exit"], self.tmp_path
        )
        assert result.exit_code == 0
        assert "42" in result.stdout

    def test_select_from_empty_table(self):
        result = self.run_interactive_session(
            ["SELECT * FROM `{}`;".format(self.table_path), "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0

    def test_upsert_and_select(self):
        result = self.run_interactive_session(
            [
                'UPSERT INTO `{path}` (id, name) VALUES (1, "Alice");'.format(
                    path=self.table_path
                ),
                "SELECT id, name FROM `{}`;".format(self.table_path),
                "exit",
            ],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "Alice" in result.stdout

    def test_multiple_upserts_and_select(self):
        result = self.run_interactive_session(
            [
                'UPSERT INTO `{path}` (id, name) VALUES (1, "Alice"), (2, "Bob");'.format(
                    path=self.table_path
                ),
                "SELECT id, name FROM `{}` ORDER BY id;".format(self.table_path),
                "exit",
            ],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "Alice" in result.stdout
        assert "Bob" in result.stdout

    def test_invalid_sql_produces_error(self):
        result = self.run_interactive_session(
            ["THIS IS NOT VALID SQL @@@@;", "SELECT 433;", "exit"], self.tmp_path
        )
        assert result.exit_code == 0
        assert "GENERIC_ERROR" in result.stderr + result.stdout
        assert "Error:" in result.stderr + result.stdout
        assert "433" in result.stdout
        assert "Bye!" in result.stdout


# =============================================================================
# /help command
# =============================================================================


class TestInteractiveHelpCommand(BaseSqlInteractiveTest):
    """Verify that /help prints usage information."""

    def test_help_command(self):
        result = self.run_interactive_session(["/help", "exit"], self.tmp_path)
        assert result.exit_code == 0
        assert "YQL" in result.stdout

        # Key commands
        assert "TAB" in result.stdout
        assert "Arrows" in result.stdout
        assert "Ctrl+↑" in result.stdout
        assert "Ctrl+↓" in result.stdout
        assert "Ctrl+R" in result.stdout
        assert "Ctrl+Enter" in result.stdout
        assert "Ctrl+D" in result.stdout

        # Special commands
        assert "SET stats" in result.stdout
        assert "EXPLAIN" in result.stdout
        assert "AST" in result.stdout

        # Interactive commands
        assert "/config" in result.stdout
        assert "/help" in result.stdout

    def test_help_does_not_exit(self):
        """After /help, the session should still accept further input."""
        result = self.run_interactive_session(
            ["/help", "SELECT 1;", "exit"], self.tmp_path
        )
        assert result.exit_code == 0
        assert "1" in result.stdout
        assert "Bye!" in result.stdout


# =============================================================================
# SET stats command
# =============================================================================


class TestInteractiveSetStats(BaseSqlInteractiveTest):
    """Verify the 'SET stats = <mode>' special command."""

    def _run(self, command):
        return self.run_interactive_session([command, "SELECT 987;", "exit"], self.tmp_path)

    def test_set_stats_none(self):
        result = self._run("SET stats = none")
        assert result.exit_code == 0
        assert "Unknown stats" not in result.stderr
        assert "987" in result.stdout
        assert "Statistics:" not in result.stdout

    def test_set_stats_basic(self):
        result = self._run("SET stats = basic")
        assert result.exit_code == 0
        assert "Unknown stats" not in result.stderr
        assert "987" in result.stdout
        assert "Statistics:" in result.stdout

    def test_set_stats_full(self):
        result = self._run("SET stats = full")
        assert result.exit_code == 0
        assert "Unknown stats" not in result.stderr
        assert "987" in result.stdout
        assert "Statistics:" in result.stdout
        assert "Execution plan:" in result.stdout

    def test_set_stats_profile(self):
        result = self._run("SET stats = profile")
        assert result.exit_code == 0
        assert "Unknown stats" not in result.stderr
        assert "987" in result.stdout
        assert "Statistics:" in result.stdout
        assert "Execution plan:" in result.stdout

    def test_set_stats_invalid_mode(self):
        """An unknown stats mode should print an error but not crash the session."""
        result = self._run("SET stats = garbage")
        assert result.exit_code == 0
        assert "Unknown stats collection mode" in result.stderr
        assert "987" in result.stdout
        assert "Bye!" in result.stdout

    def test_set_missing_variable_name(self):
        """'SET' with no variable name should print an error."""
        result = self._run("SET")
        assert result.exit_code == 0
        assert "Missing variable name" in result.stderr
        assert "987" in result.stdout

    def test_set_missing_equals(self):
        """'SET stats' without '=' should print an error."""
        result = self._run("SET stats")
        assert result.exit_code == 0
        assert 'Missing "="' in result.stderr
        assert "987" in result.stdout

    def test_set_missing_value(self):
        """'SET stats =' without a value should print an error."""
        result = self._run("SET stats =")
        assert result.exit_code == 0
        assert "Missing variable value" in result.stderr
        assert "987" in result.stdout

    def test_set_unknown_variable(self):
        result = self._run("SET unknown_var = value")
        assert result.exit_code == 0
        assert "Unknown variable name" in result.stderr
        assert "987" in result.stdout

    def test_set_stats_extra_token(self):
        """Extra tokens after the value should produce an error."""
        result = self._run("SET stats = none extra")
        assert result.exit_code == 0
        assert "exactly one token" in result.stderr
        assert "987" in result.stdout


# =============================================================================
# Pexpect-based interactive tests (pty mode — captures both stdout and stderr)
# =============================================================================


class TestInteractivePexpect(BaseSqlInteractiveTest):
    """
    Use pexpect to interact with the CLI in real time.
    pexpect uses a pty so it captures output from both stdout and stderr,
    which is required for commands like EXPLAIN that are unreliable over a
    plain pipe.
    """

    def test_welcome_message_and_prompt_appear(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            child.expect("YQL", timeout=10)
            child.expect("YQL.*>.*Enter to execute, Ctrl\\+Enter for newline.*Ctrl\\+D to exit", timeout=self.PROMPT_TIMEOUT)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
            child.expect(pexpect.EOF, timeout=5)
        finally:
            child.close()

    def test_sql_query_produces_output(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "SELECT 10;")
            child.expect("10", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "SELECT 20;")
            child.expect("20", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_help_command_produces_output(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "/help")
            child.expect("EXPLAIN", timeout=10)
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_explain_shows_query_plan(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "EXPLAIN SELECT 1;")
            child.expect("Query Plan", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_explain_ast_shows_query_ast(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "EXPLAIN AST SELECT 1;")
            child.expect("Query AST", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_explain_followed_by_regular_query(self):
        """After EXPLAIN, normal queries should still execute correctly."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "EXPLAIN SELECT 1;")
            child.expect("Query Plan", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "SELECT 55;")
            child.expect("55", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_set_stats_invalid_shows_error(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "SET stats = bad_mode")
            child.expect("Unknown stats collection mode", timeout=10)
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_set_stats_valid_no_error(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "SET stats = basic")
            self._wait_for_prompt(child)
            self._send_query(child, "SELECT 987;")
            child.expect("987", timeout=15)
            child.expect("Statistics:", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_invalid_sql_shows_error_and_continues(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "THIS IS NOT VALID SQL;")
            child.expect("Failed", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "SELECT 1;")
            child.expect("1", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_quit_exits_session(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._send_query(child, "quit")
            child.expect("Bye!", timeout=10)
            child.expect(pexpect.EOF, timeout=5)
        finally:
            child.close()


# =============================================================================
# /config command (pexpect, FTXUI TUI)
# =============================================================================


class TestInteractiveConfigCommand(BaseSqlInteractiveTest):
    # Time so FTXUI can render between key events.
    FTXUI_SETTLE = 0.2

    # ------------------------------------------------------------------
    # FTXUI navigation helpers
    # ------------------------------------------------------------------

    def _open_config(self, child):
        """Send /config without an extra \\r so FTXUI starts with clean stdin."""
        self._send_query(child, "/config")
        child.expect("Interactive Mode Settings", timeout=10)
        time.sleep(self.FTXUI_SETTLE)

    def _send_down(self, child):
        child.send('\x1b[B')
        time.sleep(self.FTXUI_SETTLE)

    def _send_up(self, child):
        child.send('\x1b[A')
        time.sleep(self.FTXUI_SETTLE)

    def _send_enter(self, child):
        child.send('\r')
        time.sleep(self.FTXUI_SETTLE)

    def _send_escape(self, child):
        child.send('\x1b')
        time.sleep(self.FTXUI_SETTLE)

    # ------------------------------------------------------------------
    # Main menu
    # ------------------------------------------------------------------

    def test_config_main_menu_shows_all_options(self):
        """All four main menu options appear in the /config panel."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._open_config(child)
            child.expect("Enable/Disable hints", timeout=5)
            child.expect("Enable/Disable colors", timeout=5)
            child.expect("Choose color theme", timeout=5)
            child.expect("Clone color theme", timeout=5)
            self._send_escape(child)
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_config_esc_on_main_menu_returns_to_prompt(self):
        """Pressing Escape on the main /config menu returns to the YQL prompt."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._open_config(child)
            self._send_escape(child)
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    # ------------------------------------------------------------------
    # Enable/Disable hints  (item 0)
    # ------------------------------------------------------------------

    def test_config_hints_submenu_shows_options(self):
        """The hints sub-menu exposes 'Enable hints' and 'Disable hints'."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._open_config(child)
            self._send_enter(child)           # select item 0 — hints
            child.expect("Enable hints", timeout=10)
            child.expect("Disable hints", timeout=5)
            self._send_escape(child)          # Esc in sub-menu exits /config
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_config_hints_esc_in_submenu_closes_config(self):
        """Pressing Escape inside the hints sub-menu closes /config entirely."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._open_config(child)
            self._send_enter(child)           # open hints sub-menu
            child.expect("Enable hints", timeout=10)
            self._send_escape(child)          # exits /config
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    # ------------------------------------------------------------------
    # Enable/Disable colors  (item 1)
    # ------------------------------------------------------------------

    def test_config_colors_submenu_shows_all_modes(self):
        """The colors sub-menu lists auto, never, and always options."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._open_config(child)
            self._send_down(child)            # item 1 — colors
            self._send_enter(child)
            child.expect("Choose colors mode", timeout=10)
            child.expect("auto", timeout=5)
            child.expect("never", timeout=5)
            child.expect("always", timeout=5)
            self._send_escape(child)          # Esc exits /config
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_config_colors_esc_in_submenu_closes_config(self):
        """Pressing Escape inside the colors sub-menu closes /config entirely."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._open_config(child)
            self._send_down(child)
            self._send_enter(child)
            child.expect("Choose colors mode", timeout=10)
            self._send_escape(child)
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    # ------------------------------------------------------------------
    # Choose color theme  (item 2)
    # ------------------------------------------------------------------

    def test_config_choose_theme_shows_current_marker(self):
        """The active theme is marked '(current)' in the theme selector."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._open_config(child)
            self._send_down(child)
            self._send_down(child)            # item 2 — theme
            self._send_enter(child)
            child.expect("Choose color theme", timeout=10)
            child.expect("current", timeout=5)
            self._send_escape(child)
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    # ------------------------------------------------------------------
    # Clone color theme  (item 3)
    # ------------------------------------------------------------------

    def test_config_clone_theme_shows_source_picker(self):
        """'Clone color theme...' opens a source-theme picker."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._open_config(child)
            self._send_down(child)            # item 1
            self._send_down(child)            # item 2
            self._send_down(child)            # item 3 — clone
            self._send_enter(child)
            child.expect("Select theme to clone", timeout=10)
            self._send_escape(child)          # Esc exits /config
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_config_clone_theme_navigate_source_list(self):
        """Arrow keys navigate the clone source picker without crashing."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._open_config(child)
            self._send_down(child)
            self._send_down(child)
            self._send_down(child)            # item 3 — clone
            self._send_enter(child)
            child.expect("Select theme to clone", timeout=10)
            self._send_down(child)
            self._send_up(child)
            self._send_escape(child)
            self._wait_for_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()


# =============================================================================
# Syntax highlighting and tab-completion (pexpect, pty mode)
# =============================================================================


class TestInteractiveHighlightAndCompletion(BaseSqlInteractiveTest):
    """
    Verify syntax highlighting and tab-completion via pexpect (pty mode).
    """

    # Time for schema listing.
    COMPLETION_TIMEOUT = 10

    @staticmethod
    def _send_ctrl_down(child):
        child.send("\x1b[1;5B")

    def _discard_and_exit(self, child):
        child.sendcontrol('c')
        child.sendcontrol('c')
        child.sendcontrol('c')
        child.sendcontrol('c')

    # ------------------------------------------------------------------
    # 1. Syntax highlighting
    # ------------------------------------------------------------------

    def test_select_keyword_is_highlighted(self):
        """
        Syntax highlighting is enabled by default.
        """
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.send("SELECT")
            child.expect(" \x1b.*\\[.*SELECT\x1b", timeout=5)
        finally:
            self._discard_and_exit(child)
            child.close()

    # ------------------------------------------------------------------
    # 2. Keyword completion
    # ------------------------------------------------------------------

    def test_tab_completes_sel_to_select(self):
        """
        Tab completion is enabled by default.
        """
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.send("SEL")
            child.send('\t')              # trigger heavy completion
            child.expect("SELECT", timeout=self.COMPLETION_TIMEOUT)
        finally:
            self._discard_and_exit(child)
            child.close()

    def test_tab_shows_candidate_list_for_ambiguous_prefix(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.send("S")
            child.send("\t")              # show full candidate list
            child.expect("S.*ELECT.*S.*HOW CREATE", timeout=self.COMPLETION_TIMEOUT)
            self._wait_for_prompt(child)
        finally:
            self._discard_and_exit(child)
            child.close()

    def test_hints_showed_for_ambiguous_prefix(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.send("S")
            child.expect("SELECT", timeout=self.COMPLETION_TIMEOUT)
            child.expect("SHOW CREATE", timeout=self.COMPLETION_TIMEOUT)
        finally:
            self._discard_and_exit(child)
            child.close()

    def test_interactive_commands_completion(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.send("/")
            child.send("\t")              # show full candidate list
            child.expect("/.*help.*/.*config", timeout=self.COMPLETION_TIMEOUT)
            self._wait_for_prompt(child)
        finally:
            self._discard_and_exit(child)
            child.close()

    def test_interactive_commands_hints_listing(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.send("/")
            child.expect("/help", timeout=self.COMPLETION_TIMEOUT)
            child.expect("/config", timeout=self.COMPLETION_TIMEOUT)
            self._send_ctrl_down(child)
            self._send_ctrl_down(child)
            child.expect("/config", timeout=self.COMPLETION_TIMEOUT)
            child.expect("/help", timeout=self.COMPLETION_TIMEOUT)
            child.send("\t")
            child.expect("/.*config", timeout=self.COMPLETION_TIMEOUT)
        finally:
            self._discard_and_exit(child)
            child.close()

    # ------------------------------------------------------------------
    # 3. Schema-aware completion
    # ------------------------------------------------------------------

    def test_tab_after_backtick_lists_schema_tables(self):
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.send("SELECT 42 FROM `")
            child.send('\t')
            child.expect(self.tmp_path.name, timeout=self.COMPLETION_TIMEOUT)
            self._wait_for_prompt(child)
        finally:
            self._discard_and_exit(child)
            child.close()
