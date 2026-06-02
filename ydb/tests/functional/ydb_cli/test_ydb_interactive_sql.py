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
from ydb.tests.functional.ydb_cli.ydb_cli_helpers import ydb_bin

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
            # Sleep past the double-Ctrl+C window (3s in line_reader.cpp) so the next
            # Ctrl+C is treated as a cancel rather than an exit signal.
            time.sleep(4)
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

    def test_consecutive_ctrl_c_without_prompt(self):
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


# =============================================================================
# Interactive transactions (BEGIN / COMMIT / ROLLBACK)
# =============================================================================


class TestInteractiveTransactions(BaseSqlInteractiveTest):
    """Verify interactive transaction control via Query service session (ydb_int only)."""

    CLI_BINARY_ENV = "YDB_CLI_INT_BINARY"

    @classmethod
    def run_interactive_session(cls, queries: List[str], tmp_path: Path, extra_args: List[str] = None, endpoint: Optional[str] = None) -> BaseInteractiveTest.ExecutionResult:
        import yatest.common

        extra_args = cls._with_profile_isolation(extra_args)

        stdin_content = "\n".join(queries) + "\n"
        stdin_path = str(tmp_path / f"interactive_stdin_{uuid.uuid4().hex}.txt")
        with open(stdin_path, "w") as f:
            f.write(stdin_content)

        with open(stdin_path, "r") as stdin_file:
            execution = yatest.common.execute(
                [
                    ydb_bin(cls.CLI_BINARY_ENV),
                    "--endpoint", endpoint or cls.grpc_endpoint(),
                    "--database", cls.root_dir,
                ] + (extra_args or []),
                stdin=stdin_file,
                check_exit_code=False,
            )

        logger.debug("stdin:\n%s", stdin_content)
        return cls.ExecutionResult(
            stdout=execution.std_out.decode("utf-8") if execution.std_out else "",
            stderr=execution.std_err.decode("utf-8") if execution.std_err else "",
            exit_code=execution.exit_code,
        )

    @staticmethod
    def _combined_output(result: "BaseInteractiveTest.ExecutionResult") -> str:
        return result.stdout + result.stderr

    @classmethod
    def _combined_lower(cls, result: "BaseInteractiveTest.ExecutionResult") -> str:
        return cls._combined_output(result).lower()

    def _assert_show_create_table_ran(self, result: "BaseInteractiveTest.ExecutionResult") -> None:
        """SHOW CREATE TABLE returned DDL for the fixture table (see create_table)."""
        combined = self._combined_lower(result)
        assert "cannot be executed inside an interactive transaction" not in combined
        assert "create table" in combined
        assert "`id`" in combined
        assert "`name`" in combined
        assert "primary key" in combined
        assert self.tmp_path.name.lower() in combined

    @staticmethod
    def _assert_stdout_contains(result: "BaseInteractiveTest.ExecutionResult", *values: str) -> None:
        for value in values:
            assert value in result.stdout

    @staticmethod
    def _assert_combined_has_error(combined_lower: str) -> None:
        """YDB/CLI errors are wording-dependent; match a small stable set."""
        assert any(
            marker in combined_lower
            for marker in (
                "error",
                "failed",
                "not found",
                "does not exist",
                "path not found",
                "unknown",
            )
        )

    # ------------------------------------------------------------------
    # Happy path: BEGIN / COMMIT / ROLLBACK forms
    # ------------------------------------------------------------------

    def test_bare_begin_commit_select(self):
        result = self.run_interactive_session(
            ["BEGIN", "SELECT 1;", "COMMIT", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "BEGIN" in result.stdout
        assert "COMMIT" in result.stdout
        assert "1" in result.stdout

    def test_begin_with_ydb_tx_mode(self):
        # snapshot-ro is used here (and in similar tests below) instead of
        # read-committed-rw because read-committed-rw is gated by server-side
        # configuration and is not enabled in the default CI YDB setup.
        result = self.run_interactive_session(
            ["BEGIN snapshot-ro", "SELECT 3;", "COMMIT", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "BEGIN" in result.stdout
        assert "COMMIT" in result.stdout
        assert "3" in result.stdout

    def test_begin_transaction_with_ydb_tx_mode(self):
        result = self.run_interactive_session(
            ["BEGIN TRANSACTION serializable-rw", "SELECT 11;", "COMMIT", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "BEGIN" in result.stdout
        assert "COMMIT" in result.stdout

    def test_rollback(self):
        result = self.run_interactive_session(
            ["BEGIN", "SELECT 4;", "ROLLBACK", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "ROLLBACK" in result.stdout
        assert "4" in result.stdout

    def test_rollback_transaction(self):
        result = self.run_interactive_session(
            ["BEGIN", "SELECT 41;", "ROLLBACK TRANSACTION", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "ROLLBACK" in result.stdout

    def test_lowercase_keywords(self):
        """TCL keywords must be case-insensitive."""
        result = self.run_interactive_session(
            ["begin", "SELECT 12;", "commit", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "BEGIN" in result.stdout
        assert "COMMIT" in result.stdout

    # ------------------------------------------------------------------
    # Trailing semicolons
    # ------------------------------------------------------------------

    def test_trailing_semicolon_in_begin(self):
        result = self.run_interactive_session(
            ["BEGIN;", "SELECT 21;", "COMMIT", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "BEGIN" in result.stdout
        assert "COMMIT" in result.stdout

    def test_trailing_semicolon_in_begin_with_mode(self):
        result = self.run_interactive_session(
            ["BEGIN snapshot-ro;", "SELECT 22;", "COMMIT", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "BEGIN" in result.stdout
        assert "COMMIT" in result.stdout

    def test_trailing_semicolon_in_commit(self):
        result = self.run_interactive_session(
            ["BEGIN", "SELECT 23;", "COMMIT;", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "COMMIT" in result.stdout

    def test_trailing_semicolon_in_rollback(self):
        result = self.run_interactive_session(
            ["BEGIN", "SELECT 24;", "ROLLBACK;", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "ROLLBACK" in result.stdout

    # ------------------------------------------------------------------
    # Modes that the Query service rejects for interactive transactions
    # ------------------------------------------------------------------

    def test_online_ro_rejected_as_interactive_mode(self):
        """online-ro is one-shot only; BEGIN must reject it client-side with a hint."""
        result = self.run_interactive_session(
            ["BEGIN online-ro", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        combined = result.stdout + result.stderr
        # Specific diagnostic must mention --tx-mode hint, not the generic
        # "unknown mode" path (which would imply the user mistyped).
        assert "online-ro" in combined
        assert "--tx-mode" in combined
        assert "BEGIN" not in result.stdout

    def test_stale_ro_rejected_as_interactive_mode(self):
        """stale-ro is one-shot only; BEGIN must reject it client-side with a hint."""
        result = self.run_interactive_session(
            ["BEGIN stale-ro", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        combined = result.stdout + result.stderr
        assert "stale-ro" in combined
        assert "--tx-mode" in combined
        assert "BEGIN" not in result.stdout

    def test_begin_rejects_trailing_tokens(self):
        """Extra tokens after a supported mode must not open a transaction."""
        result = self.run_interactive_session(
            ["BEGIN serializable-rw extra", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        combined = self._combined_lower(result)
        assert "unknown" in combined or "malformed" in combined
        assert "BEGIN" not in result.stdout
        assert "Supported modes:" in self._combined_output(result)

    # ------------------------------------------------------------------
    # Negative parsing: rejected forms
    # ------------------------------------------------------------------

    def test_invalid_mode_rejected(self):
        result = self.run_interactive_session(
            ["BEGIN garbage-mode", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        combined = self._combined_lower(result)
        assert "unknown" in combined or "malformed" in combined
        assert "BEGIN" not in result.stdout

    def test_psql_isolation_level_syntax_rejected(self):
        """The PostgreSQL-style "ISOLATION LEVEL READ COMMITTED" form is not supported.

        Only YDB-native mode names are accepted; using the psql wording must
        produce a parsing error and leave no transaction open.
        """
        result = self.run_interactive_session(
            [
                "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED",
                "exit",
            ],
            self.tmp_path,
        )
        assert result.exit_code == 0
        combined = self._combined_lower(result)
        assert "unknown" in combined or "malformed" in combined
        assert "BEGIN" not in result.stdout

    def test_draft_tcl_keywords_from_pr_41859_not_supported(self):
        """Legacy TCL from the draft PR must not open or commit interactive transactions."""
        # Not recognized as BEGIN — goes to YQL, no TCL BEGIN acknowledgement.
        result = self.run_interactive_session(
            ["START TRANSACTION", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "BEGIN" not in result.stdout

        # Parsed as BEGIN with unknown mode token WORK.
        result = self.run_interactive_session(
            ["BEGIN WORK", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        combined = result.stdout + result.stderr
        assert "unknown" in combined.lower() or "malformed" in combined.lower()
        assert "BEGIN" not in result.stdout

        # END / COMMIT WORK / ROLLBACK WORK are not COMMIT/ROLLBACK aliases.
        result = self.run_interactive_session(
            ["BEGIN", "SELECT 1;", "END", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "COMMIT" not in result.stdout

        result = self.run_interactive_session(
            ["BEGIN", "SELECT 1;", "COMMIT WORK", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "COMMIT" not in result.stdout

        result = self.run_interactive_session(
            ["BEGIN", "SELECT 1;", "ROLLBACK WORK", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "ROLLBACK" not in result.stdout

    def test_begin_word_boundary(self):
        """An identifier starting with the letters BEGIN must not be parsed as BEGIN."""
        result = self.run_interactive_session(
            ['SELECT 1 AS BEGINNING;', "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        # Must not produce TCL-related diagnostics.
        combined = result.stdout + result.stderr
        assert "no active transaction" not in combined.lower()
        assert "already a transaction" not in combined.lower()
        assert "BEGINNING" in result.stdout

    # ------------------------------------------------------------------
    # State-machine errors
    # ------------------------------------------------------------------

    def test_commit_without_transaction_fails(self):
        result = self.run_interactive_session(["COMMIT", "exit"], self.tmp_path)
        assert result.exit_code == 0
        combined = result.stdout + result.stderr
        assert "no active transaction" in combined.lower()

    def test_rollback_without_transaction_fails(self):
        result = self.run_interactive_session(["ROLLBACK", "exit"], self.tmp_path)
        assert result.exit_code == 0
        combined = result.stdout + result.stderr
        assert "no active transaction" in combined.lower()

    def test_nested_begin_fails(self):
        result = self.run_interactive_session(
            ["BEGIN", "BEGIN", "ROLLBACK", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        combined = result.stdout + result.stderr
        assert "already a transaction" in combined.lower()

    def test_exit_with_open_transaction_warns(self):
        result = self.run_interactive_session(
            ["BEGIN", "SELECT 1;", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        combined = result.stdout + result.stderr
        assert "open transaction" in combined.lower()

    # ------------------------------------------------------------------
    # Real DML rollback / commit semantics
    # ------------------------------------------------------------------

    def test_rollback_actually_reverts_dml(self):
        """ROLLBACK after UPSERT must leave no row in the table."""
        marker_id = 7777
        marker_name = "rollback-marker"
        result = self.run_interactive_session(
            [
                "BEGIN",
                'UPSERT INTO `{}` (id, name) VALUES ({}u, "{}");'.format(
                    self.table_path, marker_id, marker_name
                ),
                "ROLLBACK",
                "SELECT name FROM `{}` WHERE id = {}u;".format(self.table_path, marker_id),
                "exit",
            ],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "ROLLBACK" in result.stdout
        assert marker_name not in result.stdout

    def test_select_after_rollback_runs_outside_transaction(self):
        """After ROLLBACK the REPL must not reuse the closed transaction id."""
        result = self.run_interactive_session(
            ["BEGIN", "SELECT 1;", "ROLLBACK", "SELECT 2;", "exit"],
            self.tmp_path,
        )
        assert result.exit_code == 0
        combined = result.stdout + result.stderr
        assert "Transaction not found" not in combined
        assert "ROLLBACK" in result.stdout
        assert "2" in result.stdout

    def test_commit_actually_persists_dml(self):
        """COMMIT after UPSERT must persist the row."""
        marker_id = 7778
        marker_name = "commit-marker"
        result = self.run_interactive_session(
            [
                "BEGIN",
                'UPSERT INTO `{}` (id, name) VALUES ({}u, "{}");'.format(
                    self.table_path, marker_id, marker_name
                ),
                "COMMIT",
                "SELECT name FROM `{}` WHERE id = {}u;".format(self.table_path, marker_id),
                "exit",
            ],
            self.tmp_path,
        )
        assert result.exit_code == 0
        assert "COMMIT" in result.stdout
        assert marker_name in result.stdout

    def test_sequential_transactions(self):
        """Driver/session must be cleanly recreated between transactions."""
        result = self.run_interactive_session(
            [
                "BEGIN", "SELECT 51;", "COMMIT",
                "BEGIN", "SELECT 52;", "COMMIT",
                "BEGIN", "SELECT 53;", "ROLLBACK",
                "exit",
            ],
            self.tmp_path,
        )
        assert result.exit_code == 0
        # All three TCL boundaries must have fired.
        assert result.stdout.count("BEGIN") >= 3
        assert result.stdout.count("COMMIT") >= 2
        assert result.stdout.count("ROLLBACK") >= 1
        self._assert_stdout_contains(result, "51", "52", "53")

    def test_ddl_rejected_inside_transaction(self):
        """CREATE TABLE must not be sent to the server while a transaction is open."""
        ddl_table = f"ddl_blocked_{uuid.uuid4().hex}"
        result = self.run_interactive_session(
            [
                "BEGIN",
                f"CREATE TABLE `{ddl_table}` (id Uint64, PRIMARY KEY (id));",
                "ROLLBACK",
                f"SELECT COUNT(*) FROM `{ddl_table}`;",
                "exit",
            ],
            self.tmp_path,
        )
        assert result.exit_code == 0
        combined = self._combined_lower(result)
        assert "cannot be executed inside an interactive transaction" in combined
        assert "ROLLBACK" in result.stdout
        # CREATE was blocked; the table must not exist for the post-ROLLBACK scan.
        self._assert_combined_has_error(combined)

    def test_show_create_allowed_inside_transaction(self):
        """SHOW CREATE TABLE is read-only DDL and must pass through inside a transaction."""
        result = self.run_interactive_session(
            [
                "BEGIN",
                "SHOW CREATE TABLE `{}`;".format(self.table_path),
                "ROLLBACK",
                "exit",
            ],
            self.tmp_path,
        )
        assert result.exit_code == 0
        self._assert_show_create_table_ran(result)
        assert "ROLLBACK" in result.stdout

    def test_failed_query_invalidates_transaction(self):
        """After a failed in-tx query the CLI must drop local tx state (server auto-rollback)."""
        result = self.run_interactive_session(
            [
                "BEGIN",
                "SELECT * FROM `/path/that/does/not/exist`;",
                "SELECT 2;",
                "exit",
            ],
            self.tmp_path,
        )
        assert result.exit_code == 0
        combined = result.stdout + result.stderr
        assert "automatically rolled back" in combined
        assert "Transaction not found" not in combined
        assert "2" in result.stdout


class TestInteractiveTransactionsAutocomplete(BaseSqlInteractiveTest):
    """
    Verify TAB-completion for transaction control commands.

    These tests run ydb_int under a pty so we can drive the completer the way
    a human user would: send a partial prefix, press TAB, observe the result.
    """

    CLI_BINARY_ENV = "YDB_CLI_INT_BINARY"
    COMPLETION_TIMEOUT = 10

    @classmethod
    def spawn_interactive(cls, timeout=15, extra_args=None, env=None):
        return super().spawn_interactive(
            timeout=timeout,
            extra_args=extra_args,
            env_name=cls.CLI_BINARY_ENV,
            env=env,
        )

    def _discard_and_exit(self, child):
        for _ in range(4):
            child.sendcontrol('c')

    def _expect_keyword(self, child, keyword: str, *, anchor: str = ""):
        # Replxx may interleave color escape sequences inside the rendered
        # keyword/candidate text, so accept ANSI noise between letters.
        pattern = ".*".join(keyword)
        if anchor:
            pattern = ".*".join(anchor) + ".*" + pattern
        child.expect(pattern, timeout=self.COMPLETION_TIMEOUT)

    def _expect_s_prefixed_tx_modes(self, child, anchor: str):
        """Expect all tx modes starting with 's' in one completion list."""
        pattern = (
            ".*".join(anchor)
            + ".*"
            + ".*".join("serializable-rw")
            + ".*"
            + ".*".join("snapshot-ro")
            + ".*"
            + ".*".join("snapshot-rw")
        )
        child.expect(pattern, timeout=self.COMPLETION_TIMEOUT)

    def _expect_no_keyword(self, child, keyword: str, *, timeout: float = 1):
        pattern = ".*".join(keyword)
        try:
            child.expect(pattern, timeout=timeout)
            raise AssertionError(f"{keyword!r} must not appear in completions")
        except pexpect.TIMEOUT:
            pass

    def _wait_for_ready_after_begin(self, child):
        """Wait until BEGIN finished and replxx is ready for the next line."""
        child.expect("BEGIN", timeout=15)
        # Replxx may not echo the full colored prompt into the pty buffer; the empty-line
        # placeholder is a reliable readiness signal (same as other completion tests).
        child.expect("Type YQL query", timeout=self.PROMPT_TIMEOUT)

    def _send_line(self, child, line: str):
        child.send(line)
        child.send("\r")

    def test_no_ddl_completion_inside_transaction(self):
        """Inside a transaction, hints hide destructive DDL but keep SHOW CREATE.

        TAB completion lists are not rendered reliably in a pty (replxx may use
        inline completion only), so we use the same hint-based checks as
        test_hints_showed_for_ambiguous_prefix. Check CRE before S so that a
        prior SHOW CREATE hint does not pollute the negative assertion.
        """
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            self._send_line(child, "BEGIN")
            self._wait_for_ready_after_begin(child)

            child.send("CRE")
            self._expect_no_keyword(child, "CREATE", timeout=2)

            child.sendcontrol("u")
            child.send("S")
            child.expect("SELECT", timeout=self.COMPLETION_TIMEOUT)
            child.expect("SHOW CREATE", timeout=self.COMPLETION_TIMEOUT)
        finally:
            self._discard_and_exit(child)
            child.close()

    def test_tab_completes_beg_to_begin(self):
        """`BEG<TAB>` should expand to BEGIN."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.send("BEG")
            child.send("\t")
            self._expect_keyword(child, "BEGIN")
        finally:
            self._discard_and_exit(child)
            child.close()

    def test_tab_completes_partial_transaction(self):
        """`BEGIN T<TAB>` should expand to TRANSACTION (single candidate)."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.send("BEGIN T")
            child.send("\t")
            self._expect_keyword(child, "TRANSACTION")
        finally:
            self._discard_and_exit(child)
            child.close()

    def test_tab_after_begin_with_s_proposes_modes(self):
        """`BEGIN s<TAB>` proposes the s-prefixed mode names only."""
        # We exercise TAB on a non-empty partial because Replxx in pty mode
        # only renders the candidate list when the current partial is
        # non-empty; this matches the existing test_tab_shows_candidate_list_for_ambiguous_prefix.
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.send("BEGIN s")
            child.send("\t")
            # Single expect: avoid false positives on "Server" in the welcome
            # banner (s.*e.*r matches) and require the full candidate list.
            self._expect_s_prefixed_tx_modes(child, "BEGIN s")
        finally:
            self._discard_and_exit(child)
            child.close()

    def test_tab_completes_com_to_commit(self):
        """`COM<TAB>` should expand to COMMIT."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.send("COM")
            child.send("\t")
            self._expect_keyword(child, "COMMIT")
        finally:
            self._discard_and_exit(child)
            child.close()

    def test_tab_completes_rol_to_rollback(self):
        """`ROL<TAB>` should expand to ROLLBACK."""
        child = self.spawn_interactive()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_prompt(child)
            child.send("ROL")
            child.send("\t")
            self._expect_keyword(child, "ROLLBACK")
        finally:
            self._discard_and_exit(child)
            child.close()


class TestInteractiveTransactionsNotInPublicYdb(BaseSqlInteractiveTest):
    """Public ydb CLI must not expose interactive transaction commands."""

    def test_begin_not_available_in_public_ydb(self):
        result = self.run_interactive_session(["BEGIN", "exit"], self.tmp_path)
        assert result.exit_code == 0
        combined = result.stdout + result.stderr
        assert "ydb_int" in combined.lower()
        assert "BEGIN" not in result.stdout.split("\n")[-5:]

    def test_commit_not_available_in_public_ydb(self):
        result = self.run_interactive_session(["COMMIT", "exit"], self.tmp_path)
        assert result.exit_code == 0
        combined = result.stdout + result.stderr
        assert "ydb_int" in combined.lower()

    def test_rollback_not_available_in_public_ydb(self):
        result = self.run_interactive_session(["ROLLBACK", "exit"], self.tmp_path)
        assert result.exit_code == 0
        combined = result.stdout + result.stderr
        assert "ydb_int" in combined.lower()
