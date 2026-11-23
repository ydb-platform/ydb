# -*- coding: utf-8 -*-

import json
import logging
import os
import shutil
import tempfile
import uuid

from ydb.tests.functional.ydb_cli.ydb_cli_helpers import BaseCliTestWithDatabase, ydb_bin

import yatest

logger = logging.getLogger(__name__)


def test_shell_completion_for_topic_commands(tmp_path):
    bash_completion = tmp_path / "ydb.bash"
    bash = yatest.common.execute([ydb_bin(), "config", "completion", "bash"])
    bash_completion.write_bytes(bash.std_out)

    # Supply the bash-completion helpers' inputs for simple, unquoted words.
    # Routing and candidate selection are performed by the generated script.
    complete_command = r"""
        source "$1" || exit
        shift
        complete -p ydb >/dev/null || exit
        COMP_WORDS=("$@")
        COMP_CWORD=$(($# - 1))
        _get_comp_words_by_ref() {
            words=("${COMP_WORDS[@]}")
            cword=$COMP_CWORD
            cur=${words[cword]}
            prev=${words[cword-1]}
        }
        __ltrim_colon_completions() { :; }
        _ydb
        printf '%s\n' "${COMPREPLY[@]}"
    """
    for words, expected in [
        (["topic", "co"], {"consumer"}),
        (["topic", "read", "--format", "json-"], {"json-array", "json-stream-concat"}),
        (["topic", "create", "--retention-period", "24h", "--retention-"], {"--retention-storage-mb"}),
    ]:
        result = yatest.common.execute(
            ["bash", "--noprofile", "--norc", "-c", complete_command, "completion", str(bash_completion), "ydb", *words],
            env=dict(os.environ, BASH_ENV=""),
        )
        assert set(result.std_out.decode("utf-8").split()) == expected, words

    zsh = yatest.common.execute([ydb_bin(), "config", "completion", "zsh"])
    script = zsh.std_out.decode("utf-8")
    assert script.startswith("#compdef ydb\n")
    assert "'consumer':" in script
    assert "'json-array':" in script
    assert "'json-stream-concat':" in script
    if zsh_binary := shutil.which("zsh"):
        zsh_completion = tmp_path / "_ydb"
        zsh_completion.write_bytes(zsh.std_out)
        yatest.common.execute([zsh_binary, "-f", "-n", str(zsh_completion)])


class TestConnectionDiagnostics(BaseCliTestWithDatabase):
    def test_check_query_path_and_record_latency(self):
        result = self.execute_ydb_cli_command([
            "debug", "ping", "--kind", "Select1", "--count", "2", "--interval", "0",
        ])
        assert "Total: 2, failed: 0" in result.stdout, result.stdout

        # Record a baseline for a live endpoint without expecting machine-specific timings.
        result = self.execute_ydb_cli_command([
            "debug", "latency", "--kind", "Select1", "--interval", "3",
            "--min-inflight", "1", "--max-inflight", "1", "--percentile", "50", "--percentile", "99",
            "--format", "JSON",
        ])
        report = json.loads(result.stdout)
        sample, = report["RawResults"][0]["Select1"]
        assert sample["Inflight"] == 1
        assert 0 <= sample["p50"] <= sample["p99"]
        assert report["Throughputs"] == {"Select1": sample["Throughput"]}
        assert sample["Throughput"] > 0


class TestProfileWithInvalidCaFile:
    @classmethod
    def setup_class(cls):
        cls.profile_file = os.path.join(tempfile.gettempdir(), f"ydb_test_profile_{uuid.uuid4().hex}.yaml")
        cls.profile_name = "test"
        cls.invalid_ca_file = "/non/existent/ca/certificate.pem"
        result = cls.execute_ydb_cli_command(
            ["config", "profile", "create", cls.profile_name, "--ca-file", cls.invalid_ca_file]
        )
        assert result['exit_code'] == 0, f"Profile creation failed: {result['stderr']}"

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'profile_file') and os.path.exists(cls.profile_file):
            os.remove(cls.profile_file)

    @classmethod
    def execute_ydb_cli_command(cls, args, stdin=None):
        full_args = ["--profile-file", cls.profile_file] + args
        execution = yatest.common.execute([ydb_bin()] + full_args, stdin=stdin, check_exit_code=False)
        result = {
            'stdout': execution.std_out.decode('utf-8') if execution.std_out else '',
            'stderr': execution.std_err.decode('utf-8') if execution.std_err else '',
            'exit_code': execution.exit_code
        }
        logger.debug("Command: %s", full_args)
        logger.debug("Exit code: %d", result['exit_code'])
        logger.debug("stdout:\n%s", result['stdout'])
        logger.debug("stderr:\n%s", result['stderr'])
        return result

    def test_local_command_with_invalid_ca_file_succeeds(self):
        result = self.execute_ydb_cli_command(
            ["-p", self.profile_name, "config", "profile", "get", self.profile_name]
        )
        assert result['exit_code'] == 0, f"Local command failed with exit code {result['exit_code']}: {result['stderr']}"
        assert "ca-file" in result['stdout'].lower() or self.invalid_ca_file in result['stdout']

    def test_connection_command_with_invalid_ca_file_fails(self):
        result = self.execute_ydb_cli_command(
            ["-p", self.profile_name, "sql", "-s", "SELECT 1"]
        )
        assert result['exit_code'] != 0, "Connection command should fail with invalid CA file"
        error_output = result['stderr'] + result['stdout']
        assert "ca" in error_output.lower() or "certificate" in error_output.lower()
