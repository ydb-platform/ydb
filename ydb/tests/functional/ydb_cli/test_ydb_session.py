# -*- coding: utf-8 -*-

import json
import time

from ydb.tests.functional.ydb_cli.ydb_cli_helpers import BaseCliTestWithDatabase
from ydb.tests.oss.ydb_sdk_import import ydb


class TestSessionCli(BaseCliTestWithDatabase):
    """End-to-end tests for the production `ydb session` commands."""

    poll_timeout = 10
    poll_interval = 0.1

    @staticmethod
    def _parse_json_rows(result):
        assert result.exit_code == 0, result.stderr
        payload = json.loads(result.stdout)
        assert isinstance(payload, dict), result.stdout
        rows = payload.get("sessions")
        assert isinstance(rows, list), result.stdout
        assert all(isinstance(row, dict) for row in rows), result.stdout
        return rows

    @staticmethod
    def _parse_json_session(result):
        assert result.exit_code == 0, result.stderr
        session = json.loads(result.stdout)
        assert isinstance(session, dict), result.stdout
        return session

    def _wait_until_listed(self, session_id):
        deadline = time.monotonic() + self.poll_timeout
        last_result = None
        while time.monotonic() < deadline:
            last_result = self.execute_ydb_cli_command(
                ["session", "list", "--format", "json"],
                check_exit_code=False,
            )
            if last_result.exit_code == 0:
                rows = self._parse_json_rows(last_result)
                if any(row.get("SessionId") == session_id for row in rows):
                    return rows
            time.sleep(self.poll_interval)

        assert False, (
            f"Session {session_id} did not appear in `session list`; "
            f"stdout: {last_result.stdout!r}; stderr: {last_result.stderr!r}"
        )

    def _wait_until_gone(self, session_id):
        deadline = time.monotonic() + self.poll_timeout
        last_result = None
        last_unexpected_result = None
        expected_error = f"Session not found or not visible: {session_id}"
        while time.monotonic() < deadline:
            last_result = self.execute_ydb_cli_command(
                ["session", "get", session_id, "--format", "json"],
                check_exit_code=False,
            )
            if last_result.exit_code != 0:
                if last_result.exit_code == 1 and last_result.stderr.strip() == expected_error:
                    return
                last_unexpected_result = last_result
                time.sleep(self.poll_interval)
                continue

            session = self._parse_json_session(last_result)
            assert session.get("SessionId") == session_id, last_result.stdout
            time.sleep(self.poll_interval)

        message = (
            f"Session {session_id} remained visible after termination request; "
            f"stdout: {last_result.stdout!r}; stderr: {last_result.stderr!r}"
        )
        if last_unexpected_result is not None:
            message += (
                "; last unexpected `session get` failure: "
                f"exit_code={last_unexpected_result.exit_code}; "
                f"stdout={last_unexpected_result.stdout!r}; "
                f"stderr={last_unexpected_result.stderr!r}"
            )
        assert False, message

    def _wait_until_bad_session(self, session):
        deadline = time.monotonic() + self.poll_timeout
        last_error = None
        while time.monotonic() < deadline:
            try:
                session.keep_alive()
                last_error = None
            except ydb.BadSession:
                return
            except ydb.Error as error:
                last_error = error
            time.sleep(self.poll_interval)

        assert False, (
            "The terminated session remained usable; "
            f"last SDK error: {last_error!r}"
        )

    def test_list_get_and_terminate_table_session(self):
        # Table Service and Query Service sessions share the same server-side
        # session registry. Creating the target through Table Service exercises
        # that contract while the CLI terminates it through Query Service.
        target_session = self.driver.table_client.session().create()
        session_id = target_session.session_id

        try:
            rows = self._wait_until_listed(session_id)
            listed = next(row for row in rows if row.get("SessionId") == session_id)
            assert listed["State"] == "IDLE"

            pretty_result = self.execute_ydb_cli_command([
                "session", "list",
                "--state", "idle",
                "--format", "pretty",
            ])
            assert f'"{session_id}"' in pretty_result.stdout

            filtered_result = self.execute_ydb_cli_command([
                "session", "list",
                "--state", "idle",
                "--node-id", str(listed["NodeId"]),
                "--limit", "1",
                "--format", "json",
            ])
            filtered_rows = self._parse_json_rows(filtered_result)
            assert len(filtered_rows) == 1, filtered_result.stdout
            assert filtered_rows[0]["State"] == "IDLE"
            assert filtered_rows[0]["NodeId"] == listed["NodeId"]

            older_result = self.execute_ydb_cli_command([
                "session", "list",
                "--state", "idle",
                "--older-than", "0s",
                "--format", "json",
            ])
            older_rows = self._parse_json_rows(older_result)
            assert any(row.get("SessionId") == session_id for row in older_rows)

            old_result = self.execute_ydb_cli_command([
                "session", "list",
                "--older-than", "1d",
                "--format", "json",
            ])
            old_rows = self._parse_json_rows(old_result)
            assert all(row.get("SessionId") != session_id for row in old_rows)

            running_result = self.execute_ydb_cli_command([
                "session", "list",
                "--query-running-for", "0s",
                "--format", "json",
            ])
            running_rows = self._parse_json_rows(running_result)
            assert all(row.get("State") == "EXECUTING" for row in running_rows)
            assert all(row.get("SessionId") != session_id for row in running_rows)

            get_result = self.execute_ydb_cli_command(
                ["session", "get", session_id, "--format", "json"]
            )
            session = self._parse_json_session(get_result)
            assert session["SessionId"] == session_id
            assert session["State"] == "IDLE"

            incomplete_result = self.execute_ydb_cli_command(
                ["session", "terminate", session_id.split("&", 1)[0]],
                check_exit_code=False,
            )
            assert incomplete_result.exit_code == 1
            assert "Session ID seems incomplete" in incomplete_result.stderr
            assert "interpreted by the shell" in incomplete_result.stderr

            terminate_result = self.execute_ydb_cli_command(
                ["session", "terminate", session_id]
            )
            assert terminate_result.stdout == f"Termination requested for session {session_id}\n"

            # DeleteSession is fire-and-forget, so independently wait for both
            # the registry and the original SDK session to observe the close.
            self._wait_until_gone(session_id)
            self._wait_until_bad_session(target_session)
        finally:
            # If an assertion fails before termination, do not leak the target
            # session into the remaining functional tests.
            if target_session.session_id:
                try:
                    target_session.delete()
                except ydb.Error:
                    pass
