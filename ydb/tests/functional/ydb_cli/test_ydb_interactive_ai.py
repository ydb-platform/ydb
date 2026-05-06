# -*- coding: utf-8 -*-
"""
Functional tests for YDB CLI AI interactive mode.
"""

import json
import logging
import os
import pexpect
import threading
import time
import yaml

from http.server import HTTPServer, BaseHTTPRequestHandler
from ydb.tests.functional.ydb_cli.ydb_cli_interactive_helpers import BaseInteractiveTest

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Mock AI HTTP server
# ---------------------------------------------------------------------------

class _MockAIHandler(BaseHTTPRequestHandler):
    """Handles OpenAI ``/chat/completions`` and Anthropic ``/messages``."""

    def log_message(self, format, *args):
        logger.debug(format, *args)

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if self.path.endswith("/models"):
            self._send_json(200, {"data": [{"id": "sample-model"}]})
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        request = json.loads(body) if body else {}

        with self.server.lock:
            self.server.all_requests.append({
                "path": self.path,
                "headers": dict(self.headers),
                "body": request,
            })

        if self.path.endswith("/chat/completions"):
            handler = self.server.openai_handler
            resp = handler(request) if handler else self.server.default_openai_response
            self._send_json(200, resp)
        elif self.path.endswith("/messages"):
            handler = self.server.anthropic_handler
            resp = handler(request) if handler else self.server.default_anthropic_response
            self._send_json(200, resp)
        else:
            self.send_response(404)
            self.end_headers()

    def _send_json(self, code, data):
        payload = json.dumps(data).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()

        try:
            self.wfile.write(payload)
        except BrokenPipeError:
            pass


class MockAIServer:
    """Thin wrapper around :class:`HTTPServer` that runs in a daemon thread."""

    DEFAULT_OPENAI_RESPONSE = {
        "choices": [{
            "message": {
                "role": "assistant",
                "content": "Mock OpenAI response.",
            }
        }]
    }

    DEFAULT_ANTHROPIC_RESPONSE = {
        "content": [{
            "type": "text",
            "text": "Mock Anthropic response.",
        }]
    }

    def __init__(self):
        self._server = HTTPServer(("127.0.0.1", 0), _MockAIHandler)
        self._server.lock = threading.Lock()
        self._server.all_requests = []
        self._server.openai_handler = None
        self._server.anthropic_handler = None
        self._server.default_openai_response = self.DEFAULT_OPENAI_RESPONSE
        self._server.default_anthropic_response = self.DEFAULT_ANTHROPIC_RESPONSE
        self.port = self._server.server_address[1]
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self):
        self._server.shutdown()
        if self._thread:
            self._thread.join(timeout=5)

    def set_openai_response(self, text):
        self._server.default_openai_response = {
            "choices": [{
                "message": {"role": "assistant", "content": text}
            }]
        }

    def set_anthropic_response(self, text):
        self._server.default_anthropic_response = {
            "content": [{"type": "text", "text": text}]
        }

    def set_openai_handler(self, handler):
        self._server.openai_handler = handler

    def set_anthropic_handler(self, handler):
        self._server.anthropic_handler = handler

    def clear(self):
        with self._server.lock:
            self._server.all_requests.clear()
        self._server.openai_handler = None
        self._server.anthropic_handler = None
        self._server.default_openai_response = self.DEFAULT_OPENAI_RESPONSE
        self._server.default_anthropic_response = self.DEFAULT_ANTHROPIC_RESPONSE

    @property
    def requests(self):
        with self._server.lock:
            return list(self._server.all_requests)

    @property
    def last_request(self):
        with self._server.lock:
            return self._server.all_requests[-1] if self._server.all_requests else None


# ---------------------------------------------------------------------------
# Base test class
# ---------------------------------------------------------------------------

class BaseAiInteractiveTest(BaseInteractiveTest):
    FTXUI_SETTLE = 0.3
    mock_server = None

    @classmethod
    def setup_class(cls):
        super().setup_class()
        cls.mock_server = MockAIServer()
        cls.mock_server.start()

    @classmethod
    def teardown_class(cls):
        if cls.mock_server:
            cls.mock_server.stop()
        super().teardown_class()

    def _wait_for_ai_prompt(self, child):
        child.expect("AI\033\\[22;39m>", timeout=self.PROMPT_TIMEOUT)

    def _wait_for_yql_prompt(self, child):
        child.expect("YQL\033\\[22;39m>", timeout=self.PROMPT_TIMEOUT)

    def _create_ai_profile(self, api_type="openai", preset_id=None):
        """Write a minimal AI-profile YAML that points at the mock server.

        *preset_id* — when set, the profile will be associated with the given
        preset so that ``SelectAiProfile`` treats it as "used".
        """
        port = self.mock_server.port
        if api_type == "openai":
            entry = {
                "name": "Test OpenAI",
                "endpoint": "http://localhost:{}/openai/v1".format(port),
                "api_type": 0,
                "model": "sample-openai-model",
                "token": "test_token",
            }
            if preset_id is None:
                preset_id = "sample_openai_model"
            entry["preset_id"] = preset_id
            profile = {
                "current_profile": "test_openai",
                "interactive_mode": 1,
                "ai_profiles": {"test_openai": entry},
            }
        else:
            entry = {
                "name": "Test Anthropic",
                "endpoint": "http://localhost:{}/anthropic/v1".format(port),
                "api_type": 1,
                "model": "sample-anthropic-model",
                "token": "test_token",
            }
            if preset_id is None:
                preset_id = "sample_anthropic_model"
            entry["preset_id"] = preset_id
            profile = {
                "current_profile": "test_anthropic",
                "interactive_mode": 1,
                "ai_profiles": {"test_anthropic": entry},
            }
        path = str(self.tmp_path / "ai_profile.yaml")
        with open(path, "w") as f:
            yaml.dump(profile, f)
        return path

    def _create_minimal_ai_config(self):
        """Write a config that only sets ``interactive_mode: 1``.

        The CLI will auto-create a profile from the default preset
        (``sample_openai_model``) that is registered in the test binary.
        """
        path = str(self.tmp_path / "ai_profile.yaml")
        with open(path, "w") as f:
            yaml.dump({"interactive_mode": 1}, f)
        return path

    @classmethod
    def spawn_interactive(cls, timeout=15, extra_args=None, profile_path=None, env_name="YDB_CLI_WITH_ENABLED_AI_BINARY", env=None):
        if env is None:
            env = os.environ.copy()
        env["YDB_CLI_TEST_AI_PORT"] = str(cls.mock_server.port)
        if profile_path:
            env["YDB_CLI_AI_PROFILE_FILE"] = profile_path
        return super().spawn_interactive(timeout, extra_args, env_name, env=env)

    def spawn_ai_interactive(self, api_type="openai", timeout=15):
        """Spawn the AI-enabled CLI binary in AI mode."""
        profile_path = self._create_ai_profile(api_type)
        return self.spawn_interactive(timeout, profile_path=profile_path)

    # ------------------------------------------------------------------
    # FTXUI navigation helpers
    # ------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# OpenAI API tests
# ---------------------------------------------------------------------------

class TestAiOpenAIPexpect(BaseAiInteractiveTest):
    def test_ai_welcome_and_prompt(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            child.expect("Using model:", timeout=10)
            child.expect("/help", timeout=10)
            child.expect("/model", timeout=10)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
            child.expect(pexpect.EOF, timeout=5)
        finally:
            child.close()

    def test_simple_text_response(self):
        self.mock_server.set_openai_response("Hello from mock AI!")
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "say hello")
            child.expect("Hello from mock AI!", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_system_prompt_contains_ydb_rules(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "test message")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            req = self.mock_server.last_request["body"]
            messages = req["messages"]
            assert messages[0]["role"] == "system"
            assert "YDB" in messages[0]["content"]

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_connection_string_in_system_prompt(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "test")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            system = self.mock_server.last_request["body"]["messages"][0]
            assert system["role"] == "system"
            assert "[CONTEXT]" in system["content"]

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_user_message_forwarded(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "what tables exist?")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            messages = self.mock_server.last_request["body"]["messages"]
            user_msgs = [m for m in messages if m["role"] == "user"]
            assert len(user_msgs) == 1
            assert "what tables exist?" in user_msgs[0]["content"]

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_tools_registered_openai_format(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "list tables")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            req = self.mock_server.last_request["body"]
            assert "tools" in req
            tool_names = {t["function"]["name"] for t in req["tools"]}
            assert tool_names == {"list_directory", "exec_query", "explain_query", "describe", "ydb_help", "exec_shell"}
            for tool in req["tools"]:
                assert tool["type"] == "function"
                assert "parameters" in tool["function"]
                assert "description" in tool["function"]

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_model_name_sent(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "hello")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            assert self.mock_server.last_request["body"]["model"] == "sample-openai-model"

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_max_completion_tokens_sent(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "hello")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            assert self.mock_server.last_request["body"]["max_completion_tokens"] == 1024

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_auth_header_present(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "hello")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            hdrs = self.mock_server.last_request["headers"]
            auth = hdrs.get("Authorization", hdrs.get("authorization", ""))
            assert auth == "Bearer test_token"

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_conversation_history_maintained(self):
        self.mock_server.set_openai_response("First response")
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "first question")
            child.expect("First response", timeout=15)
            self._wait_for_ai_prompt(child)

            self.mock_server.set_openai_response("Second response")
            self._send_query(child, "second question")
            child.expect("Second response", timeout=15)
            self._wait_for_ai_prompt(child)

            messages = self.mock_server.last_request["body"]["messages"]
            roles = [m["role"] for m in messages]
            assert roles[0] == "system"
            assert roles.count("user") == 2
            assert any(r == "assistant" for r in roles)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_tool_call_round_trip(self):
        call_count = [0]

        def handler(request):
            call_count[0] += 1
            if call_count[0] == 1:
                return {
                    "choices": [{
                        "message": {
                            "invalid_arg": "invalid_value",
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [{
                                "invalid_arg": "invalid_value",
                                "id": "call_001",
                                "type": "function",
                                "function": {
                                    "name": "list_directory",
                                    "arguments": json.dumps({"directory": "/"})
                                }
                            }]
                        }
                    }]
                }
            return {
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "Found the directory listing.",
                        "invalid_arg": "invalid_value",
                    }
                }]
            }

        self.mock_server.set_openai_handler(handler)
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "list root directory")
            child.expect("Found the directory listing", timeout=30)
            self._wait_for_ai_prompt(child)

            assert call_count[0] >= 2
            messages = self.mock_server.last_request["body"]["messages"]
            tool_msgs = [m for m in messages if m["role"] == "tool"]
            assert len(tool_msgs) > 0
            assert tool_msgs[-1]["tool_call_id"] == "call_001"

            for m in messages:
                assert "invalid" not in str(m), str(m)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_help_command_no_api_call(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            before = len(self.mock_server.requests)
            self._send_query(child, "/help")
            child.expect("Hotkeys", timeout=10)
            self._wait_for_ai_prompt(child)
            assert len(self.mock_server.requests) == before

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_exit_and_quit(self):
        for cmd in ("exit", "quit"):
            child = self.spawn_ai_interactive("openai")
            try:
                child.expect("Welcome to YDB CLI", timeout=15)
                self._wait_for_ai_prompt(child)
                self._send_query(child, cmd)
                child.expect("Bye!", timeout=10)
                child.expect(pexpect.EOF, timeout=5)
            finally:
                child.close()

    def test_request_path_is_chat_completions(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "ping")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            assert self.mock_server.last_request["path"].endswith("/chat/completions")

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_large_ping_timeout(self):
        def handler(request):
            time.sleep(60)
            return {"choices": [{"message": {"role": "assistant", "content": "Ping response."}}]}

        self.mock_server.set_openai_handler(handler)
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "ping")
            child.expect("Agent is thinking...", timeout=15)
            child.expect("Ping response.", timeout=70)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()


# ---------------------------------------------------------------------------
# Anthropic API tests
# ---------------------------------------------------------------------------

class TestAiAnthropicPexpect(BaseAiInteractiveTest):
    def test_anthropic_welcome_and_prompt(self):
        child = self.spawn_ai_interactive("anthropic")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            child.expect("Using model:", timeout=10)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
            child.expect(pexpect.EOF, timeout=5)
        finally:
            child.close()

    def test_anthropic_simple_response(self):
        self.mock_server.set_anthropic_response("Hello from Anthropic mock!")
        child = self.spawn_ai_interactive("anthropic")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "greet me")
            child.expect("Hello from Anthropic mock!", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_system_prompt_is_top_level(self):
        child = self.spawn_ai_interactive("anthropic")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "test")
            child.expect("Mock Anthropic response", timeout=15)
            self._wait_for_ai_prompt(child)

            req = self.mock_server.last_request["body"]
            assert "system" in req
            assert "YDB" in req["system"]
            for msg in req["messages"]:
                assert msg["role"] != "system"

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_connection_string_in_system(self):
        child = self.spawn_ai_interactive("anthropic")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "test")
            child.expect("Mock Anthropic response", timeout=15)
            self._wait_for_ai_prompt(child)

            assert "[CONTEXT]" in self.mock_server.last_request["body"]["system"]

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_user_message_as_content_blocks(self):
        child = self.spawn_ai_interactive("anthropic")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "describe tables")
            child.expect("Mock Anthropic response", timeout=15)
            self._wait_for_ai_prompt(child)

            messages = self.mock_server.last_request["body"]["messages"]
            user_msgs = [m for m in messages if m["role"] == "user"]
            assert len(user_msgs) == 1
            content = user_msgs[0]["content"]
            assert isinstance(content, list)
            assert content[0]["type"] == "text"
            assert "describe tables" in content[0]["text"]

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_tools_use_input_schema(self):
        child = self.spawn_ai_interactive("anthropic")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "list tables")
            child.expect("Mock Anthropic response", timeout=15)
            self._wait_for_ai_prompt(child)

            tools = self.mock_server.last_request["body"]["tools"]
            tool_names = {t["name"] for t in tools}
            assert tool_names == {"list_directory", "exec_query", "explain_query", "describe", "ydb_help", "exec_shell"}
            for tool in tools:
                assert "input_schema" in tool
                assert "description" in tool
                assert "function" not in tool

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_model_name_sent(self):
        child = self.spawn_ai_interactive("anthropic")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "hello")
            child.expect("Mock Anthropic response", timeout=15)
            self._wait_for_ai_prompt(child)

            assert self.mock_server.last_request["body"]["model"] == "sample-anthropic-model"

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_max_tokens_field(self):
        child = self.spawn_ai_interactive("anthropic")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "hello")
            child.expect("Mock Anthropic response", timeout=15)
            self._wait_for_ai_prompt(child)

            body = self.mock_server.last_request["body"]
            assert body["max_tokens"] == 1024
            assert "max_completion_tokens" not in body

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_auth_header_present(self):
        child = self.spawn_ai_interactive("anthropic")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "hello")
            child.expect("Mock Anthropic response", timeout=15)
            self._wait_for_ai_prompt(child)

            hdrs = self.mock_server.last_request["headers"]
            auth = hdrs.get("Authorization", hdrs.get("authorization", ""))
            assert auth == "Bearer test_token"

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_conversation_history(self):
        self.mock_server.set_anthropic_response("First Anthropic answer")
        child = self.spawn_ai_interactive("anthropic")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "first question")
            child.expect("First Anthropic answer", timeout=15)
            self._wait_for_ai_prompt(child)

            self.mock_server.set_anthropic_response("Second Anthropic answer")
            self._send_query(child, "second question")
            child.expect("Second Anthropic answer", timeout=15)
            self._wait_for_ai_prompt(child)

            messages = self.mock_server.last_request["body"]["messages"]
            roles = [m["role"] for m in messages]
            assert "user" in roles
            assert "assistant" in roles
            assert "system" not in roles

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_tool_call_round_trip(self):
        call_count = [0]

        def handler(request):
            call_count[0] += 1
            if call_count[0] == 1:
                return {
                    "content": [{
                        "type": "tool_use",
                        "invalid_arg": "invalid_value",
                        "id": "tool_abc",
                        "name": "list_directory",
                        "input": {"directory": "/"}
                    }]
                }
            return {
                "content": [{"type": "text", "text": "Found directories in root.", "invalid_arg": "invalid_value"}]
            }

        self.mock_server.set_anthropic_handler(handler)
        child = self.spawn_ai_interactive("anthropic")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "list root")
            child.expect("Found directories in root", timeout=30)
            self._wait_for_ai_prompt(child)

            assert call_count[0] >= 2
            messages = self.mock_server.last_request["body"]["messages"]
            user_msgs = [m for m in messages if m["role"] == "user"]
            last_user_content = user_msgs[-1]["content"]
            tool_results = [b for b in last_user_content if b["type"] == "tool_result"]
            assert len(tool_results) > 0
            assert tool_results[0]["tool_use_id"] == "tool_abc"

            for m in messages:
                assert "invalid" not in str(m), str(m)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_request_path_is_messages(self):
        child = self.spawn_ai_interactive("anthropic")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "ping")
            child.expect("Mock Anthropic response", timeout=15)
            self._wait_for_ai_prompt(child)

            assert self.mock_server.last_request["path"].endswith("/messages")

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_exit_and_quit(self):
        for cmd in ("exit", "quit"):
            child = self.spawn_ai_interactive("anthropic")
            try:
                child.expect("Welcome to YDB CLI", timeout=15)
                self._wait_for_ai_prompt(child)
                self._send_query(child, cmd)
                child.expect("Bye!", timeout=10)
                child.expect(pexpect.EOF, timeout=5)
            finally:
                child.close()


# ---------------------------------------------------------------------------
# Common AI interactive tests (API-type-independent, using OpenAI profile)
# ---------------------------------------------------------------------------

class TestAiCommonPexpect(BaseAiInteractiveTest):
    """Tests for interactive features that are not specific to an API type:
    special commands, mode switching, configuration manager, presets, etc.
    """

    # -- /help ---------------------------------------------------------------

    def test_help_text_mentions_ai_api(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "/help")
            child.expect("YDB CLI AI Interactive Mode", timeout=10)
            child.expect("All input is sent to the AI API", timeout=10)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "/help")
            child.expect("Ctrl.T", timeout=10)
            child.expect("/switch", timeout=5)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "/help")
            child.expect("/model", timeout=10)
            child.expect("/config", timeout=5)
            child.expect("/help", timeout=5)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    # -- welcome message -----------------------------------------------------

    def test_welcome_shows_ai_profile_name(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            child.expect("AI.*interactive mode", timeout=10)
            child.expect("Using model.*Test OpenAI", timeout=10)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_welcome_on_empty_config(self):
        profile_path = self._create_minimal_ai_config()
        child = self.spawn_interactive(timeout=15, profile_path=profile_path)
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            child.expect("AI.*interactive mode", timeout=10)
            child.expect("Using model.*Sample Open AI model", timeout=10)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_welcome_shows_help_and_model_hints(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            child.expect("/help", timeout=10)
            child.expect("/model", timeout=5)
            self._wait_for_ai_prompt(child)
            child.expect("Enter to send, Ctrl\\+Enter for newline, Ctrl\\+T for YQL mode, Ctrl\\+D to exit", timeout=5)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    # -- /switch (AI / YQL) --------------------------------------------------

    def test_switch_yql_to_ai(self):
        profile_path = str(self.tmp_path / "ai_profile.yaml")
        child = self.spawn_interactive(timeout=15, profile_path=profile_path)
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_yql_prompt(child)
            self._send_query(child, "/switch")
            child.expect("Welcome to YDB CLI .*AI.* interactive mode!", timeout=10)
            child.expect("Using model.*Sample Open AI model", timeout=10)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_switch_ai_to_yql(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "/switch")
            self._wait_for_yql_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_switch_roundtrip_ai_yql_ai(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/switch")
            self._wait_for_yql_prompt(child)

            self._send_query(child, "/switch")
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_yql_query_works_after_switch(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/switch")
            self._wait_for_yql_prompt(child)

            self._send_query(child, "SELECT 42;")
            child.expect("42", timeout=15)
            self._wait_for_yql_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_ai_works_after_roundtrip_switch(self):
        self.mock_server.set_openai_response("AI still alive")
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/switch")
            self._wait_for_yql_prompt(child)

            self._send_query(child, "/switch")
            self._wait_for_ai_prompt(child)

            self._send_query(child, "are you there?")
            child.expect("AI still alive", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_ai_session_context_remains_after_roundtrip_switch(self):
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "XXX from preset")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "/switch")
            self._wait_for_yql_prompt(child)
            self._send_query(child, "/switch")
            self._wait_for_ai_prompt(child)
            self._send_query(child, "another hello from preset")
            child.expect("Mock OpenAI response", timeout=15)

            assert len(self.mock_server.requests) == 2
            last_request = str(self.mock_server.last_request["body"])
            assert "another hello from preset" in last_request, last_request
            assert "XXX from preset" in last_request, last_request

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    # -- default preset auto-creation ----------------------------------------

    def test_default_preset_auto_creates_profile(self):
        """With only ``interactive_mode: 1`` the CLI auto-creates a profile
        from the default preset and enters AI mode."""
        self.mock_server.set_openai_response("preset auto-created ok")
        profile_path = self._create_minimal_ai_config()
        child = self.spawn_interactive(timeout=15, profile_path=profile_path)
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            child.expect("AI", timeout=10)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "hello from preset")
            child.expect("preset auto-created ok", timeout=15)
            self._wait_for_ai_prompt(child)

            req = self.mock_server.last_request
            assert req["path"].endswith("/chat/completions")
            assert req["body"]["model"] == "sample-openai-model"

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    # -- profile file persistence --------------------------------------------

    def _read_profile_yaml(self):
        path = str(self.tmp_path / "ai_profile.yaml")
        with open(path) as f:
            return yaml.safe_load(f)

    def test_preset_creates_correct_profile_file(self):
        """After the CLI auto-creates a profile from the default preset the
        YAML file contains all expected keys with correct values."""
        self.mock_server.set_openai_response("ok")
        profile_path = self._create_minimal_ai_config()
        child = self.spawn_interactive(timeout=15, profile_path=profile_path)
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "trigger model init")
            child.expect("ok", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

        cfg = self._read_profile_yaml()
        assert cfg["interactive_mode"] == 1
        assert cfg.get("current_profile"), "current_profile must be set"

        profiles = cfg.get("ai_profiles", {})
        assert len(profiles) >= 1, "at least one profile must exist"
        profile_id = cfg["current_profile"]
        assert profile_id in profiles, "current_profile must reference an existing entry"

        p = profiles[profile_id]
        assert p["name"], "profile name must be non-empty"
        assert p["preset_id"] == "sample_openai_model"
        assert len(p.keys()) == 2, "only name and preset_id are expected"

    def test_model_switch_updates_profile_file(self):
        """Switching model via ``/model`` persists the new profile and
        ``current_profile`` to the YAML file."""
        self.mock_server.set_openai_response("openai ok")
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "init")
            child.expect("openai ok", timeout=15)
            self._wait_for_ai_prompt(child)
            self.mock_server.clear()

            self._send_query(child, "/model")
            child.expect("choose AI model", timeout=10)
            self._send_down(child)
            self._send_enter(child)
            child.expect("profile is changed|Switching AI profile", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

        cfg = self._read_profile_yaml()
        profiles = cfg.get("ai_profiles", {})
        assert len(profiles) >= 2, "both old and new profiles must be in the file"
        active_id = cfg["current_profile"]
        assert active_id != "test_openai", "active profile must have changed"

        active = profiles[active_id]
        assert active["name"] == "Sample Anthropic model"
        assert active["preset_id"] == "sample_anthropic_model"
        assert len(active.keys()) == 2, "only name and preset_id are expected"

        assert "test_openai" in profiles, "original profile must still exist"

    def test_config_edit_persists_to_file(self):
        """Changing a setting via ``/config`` -> "Change current AI model
        settings" -> "Finish editing" writes the update to the YAML file."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "init")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/config")
            child.expect("choose setting to change", timeout=10)
            self._send_down(child)
            self._send_down(child)
            self._send_enter(child)

            child.expect("choose setting to change.*API endpoint", timeout=10)
            for _ in range(2):
                self._send_down(child)
            self._send_enter(child)

            child.expect("Please choose model name:", timeout=10)
            self._send_enter(child)

            child.expect("choose setting to change.*API endpoint", timeout=10)
            for _ in range(5):
                self._send_down(child)
            self._send_enter(child)

            child.expect("profile is changed", timeout=10)
            time.sleep(1)
        finally:
            self.mock_server.clear()
            child.close()

        cfg = self._read_profile_yaml()
        assert "ai_profiles" in cfg
        assert cfg["current_profile"] in cfg["ai_profiles"]

        profile_id = cfg["current_profile"]
        p = cfg["ai_profiles"][profile_id]
        assert p["name"] == "Test OpenAI"

    # -- RemoveAiProfile (via /config) ---------------------------------------

    def test_config_remove_profile_creates_new_from_preset(self):
        """``/config`` -> "Remove current AI model" deletes the profile and
        auto-creates a new one from the default preset."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "init")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/config")
            child.expect("Remove current AI model", timeout=10)
            for _ in range(3):
                self._send_down(child)
            self._send_enter(child)

            child.expect("profile is changed|Switching AI profile", timeout=15)
            self._send_escape(child)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

        cfg = self._read_profile_yaml()
        profiles = cfg.get("ai_profiles", {})
        assert "test_openai" not in profiles, "removed profile must be gone"
        assert len(profiles) >= 1, "a replacement profile must have been created"
        assert cfg["current_profile"] in profiles

    def test_config_remove_and_verify_new_profile_structure(self):
        """After removing the profile and auto-creating a replacement, the
        new profile in the YAML has the expected structure from the preset."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "init")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/config")
            child.expect("Remove current AI model", timeout=10)
            for _ in range(3):
                self._send_down(child)
            self._send_enter(child)

            child.expect("profile is changed|Switching AI profile", timeout=15)
            self._send_escape(child)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

        cfg = self._read_profile_yaml()
        profile_id = cfg["current_profile"]
        p = cfg["ai_profiles"][profile_id]
        assert p.get("name"), "replacement profile must have a name"
        assert p.get("preset_id"), "replacement profile must reference a preset"
        assert len(p.keys()) == 2, "only name and preset_id are expected"

    # -- /model menu (presets from test binary) ------------------------------

    def test_model_menu_shows_presets(self):
        """``/model`` opens the FTXUI menu that lists the unused presets
        registered in the test binary."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/model")
            child.expect("choose AI model", timeout=10)
            child.expect("Sample Anthropic model", timeout=5)
            self._send_escape(child)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_model_menu_shows_existing_profile(self):
        """The current profile appears in the ``/model`` menu."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/model")
            child.expect("Test OpenAI", timeout=10)
            self._send_escape(child)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_model_menu_shows_setup_custom(self):
        """The ``/model`` menu includes the "Setup custom model" option."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/model")
            child.expect("Setup custom model", timeout=10)
            self._send_escape(child)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    # -- model switching via /model ------------------------------------------

    def test_switch_model_to_anthropic_via_menu(self):
        """Select the Anthropic preset in the ``/model`` menu and verify
        the next request uses the Anthropic API format."""
        self.mock_server.set_openai_response("openai answer")
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "warm up")
            child.expect("openai answer", timeout=15)
            self._wait_for_ai_prompt(child)
            self.mock_server.clear()

            self._send_query(child, "/model")
            child.expect("choose AI model", timeout=10)
            self._send_down(child)
            self._send_enter(child)

            child.expect("profile is changed|Switching AI profile", timeout=15)
            self._wait_for_ai_prompt(child)

            self.mock_server.set_anthropic_response("now using anthropic")
            self._send_query(child, "hello anthropic")
            child.expect("now using anthropic", timeout=15)
            self._wait_for_ai_prompt(child)

            req = self.mock_server.last_request
            assert req["path"].endswith("/messages")
            assert req["body"]["model"] == "sample-anthropic-model"
            assert "system" in req["body"]

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    # -- custom model creation (Setup flow) ----------------------------------

    def test_setup_custom_shows_endpoint_presets(self):
        """``/model`` -> "Setup custom model" opens the endpoint selection
        menu that lists the preset endpoints from the test binary."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/model")
            child.expect("Setup custom model", timeout=10)
            for _ in range(2):
                self._send_down(child)
            self._send_enter(child)

            child.expect("choose API endpoint", timeout=10)
            child.expect("Sample OpenAI models|Sample anthropic models", timeout=5)
            self._send_escape(child)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_setup_custom_shows_custom_endpoint_option(self):
        """The endpoint selection menu includes a "Setup custom endpoint"
        fallback option."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/model")
            child.expect("Setup custom model", timeout=10)
            for _ in range(2):
                self._send_down(child)
            self._send_enter(child)

            child.expect("Setup custom endpoint", timeout=10)
            self._send_escape(child)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            child.close()

    def test_setup_from_endpoint_preset_creates_profile(self):
        """Selecting a preset endpoint in the custom-model flow auto-fills
        the profile and creates it on disk."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/model")
            child.expect("Setup custom model", timeout=10)
            for _ in range(2):
                self._send_down(child)
            self._send_enter(child)

            child.expect("choose API endpoint", timeout=10)
            self._send_enter(child)

            child.expect("choose model name", timeout=10)
            self._send_enter(child)

            child.expect("Please enter profile name:", timeout=10)
            self._send_enter(child)

            child.expect("profile is changed|Switching AI profile", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

        cfg = self._read_profile_yaml()
        profiles = cfg.get("ai_profiles", {})
        assert len(profiles) >= 2, "new profile must be added alongside the original"

    def test_token_provider_resolves_correctly(self):
        """The ``test_token`` provider registered in the test binary resolves
        to ``"test_token"`` and is sent as the Bearer token."""
        self.mock_server.set_openai_response("token check ok")
        profile_path = self._create_minimal_ai_config()
        child = self.spawn_interactive(timeout=15, profile_path=profile_path)
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "check token")
            child.expect("token check ok", timeout=15)
            self._wait_for_ai_prompt(child)

            hdrs = self.mock_server.last_request["headers"]
            auth = hdrs.get("Authorization", hdrs.get("authorization", ""))
            assert auth == "Bearer test_token"

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

        cfg = self._read_profile_yaml()
        profile_id = cfg["current_profile"]
        p = cfg["ai_profiles"][profile_id]
        assert p["name"] == "Sample Open AI model"

    # -- /config menu --------------------------------------------------------

    def test_config_menu_shows_options(self):
        """``/config`` opens the settings menu with expected options."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "warm up")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/config")
            child.expect("choose setting to change", timeout=10)
            child.expect("Clear session context", timeout=5)
            self._send_escape(child)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_config_clear_context(self):
        """Selecting "Clear session context" via ``/config`` resets the
        conversation history so the next request starts fresh."""
        self.mock_server.set_openai_response("before clear")
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "remember this")
            child.expect("before clear", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/config")
            child.expect("Clear session context", timeout=10)
            self._send_enter(child)
            child.expect("Session context cleared", timeout=10)
            self._wait_for_ai_prompt(child)

            self.mock_server.clear()
            self.mock_server.set_openai_response("after clear")
            self._send_query(child, "new conversation")
            child.expect("after clear", timeout=15)
            self._wait_for_ai_prompt(child)

            messages = self.mock_server.last_request["body"]["messages"]
            user_msgs = [m for m in messages if m["role"] == "user"]
            assert len(user_msgs) == 1
            assert "new conversation" in user_msgs[0]["content"]
            assert not any("remember this" in m.get("content", "") for m in messages if m["role"] == "user")

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_config_shows_switch_model_option(self):
        """``/config`` menu includes "Switch AI model" with the current name."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "warm up")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/config")
            child.expect("Switch AI model", timeout=10)
            self._send_escape(child)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_config_shows_change_settings_option(self):
        """``/config`` menu includes "Change current AI model settings"."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "warm up")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/config")
            child.expect("Change current AI model settings", timeout=10)
            self._send_escape(child)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_config_shows_remove_option(self):
        """``/config`` menu includes "Remove current AI model"."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "warm up")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/config")
            child.expect("Remove current AI model", timeout=10)
            self._send_escape(child)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_config_edit_shows_current_settings(self):
        """``/config`` -> "Change current AI model settings" opens the Edit
        menu which displays all current profile fields."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "warm up")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/config")
            child.expect("choose setting to change", timeout=10)
            self._send_down(child)
            self._send_down(child)
            self._send_enter(child)

            child.expect("API endpoint", timeout=10)
            child.expect("API type", timeout=5)
            child.expect("Model name", timeout=5)
            child.expect("Token", timeout=5)
            child.expect("Profile display name", timeout=5)
            child.expect("Finish editing", timeout=5)

            self._send_escape(child)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_config_edit_token_shows_provider(self):
        """Navigating to the Token setting in the Edit menu shows the
        registered "Test token" provider from the test binary."""
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "warm up")
            child.expect("Mock OpenAI response", timeout=15)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "/config")
            child.expect("choose setting to change", timeout=10)
            self._send_down(child)
            self._send_down(child)
            self._send_enter(child)

            child.expect("choose setting to change.*API endpoint", timeout=10)
            for _ in range(3):
                self._send_down(child)
            self._send_enter(child)

            child.expect("Test token", timeout=10)

            self._send_down(child)
            self._send_enter(child)
            child.expect("Test token documentation", timeout=10)

            self._send_escape(child)
            self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    # -- multiple queries in a single session --------------------------------

    def test_multiple_queries_in_session(self):
        """Several successive queries all produce responses and the session
        stays in AI mode throughout."""
        responses = ["first reply", "second reply", "third reply"]
        idx = [0]

        def handler(request):
            r = responses[min(idx[0], len(responses) - 1)]
            idx[0] += 1
            return {"choices": [{"message": {"role": "assistant", "content": r}}]}

        self.mock_server.set_openai_handler(handler)
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)

            for expected in responses:
                self._send_query(child, "query")
                child.expect(expected, timeout=15)
                self._wait_for_ai_prompt(child)

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()


# ---------------------------------------------------------------------------
# Tool execution tests — shared helpers
# ---------------------------------------------------------------------------

class _ToolTestBase(BaseAiInteractiveTest):
    """Mixin providing helpers to test individual tool execution.

    Subclasses set ``API_TYPE`` to ``"openai"`` or ``"anthropic"`` and inherit
    concrete test methods that exercise each of the five registered tools.
    """

    API_TYPE = None  # Override in subclasses

    # -- handler factories ---------------------------------------------------

    def _make_tool_handler(self, tool_name, tool_args, final_text):
        """Return ``(handler, call_count)`` for the mock server.

        *First* request  → model returns a tool-use / tool_calls response.
        *Second* request → model returns plain text *final_text*.
        """
        call_count = [0]

        if self.API_TYPE == "openai":
            def handler(request):
                call_count[0] += 1
                if call_count[0] == 1:
                    return {
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [{
                                    "id": "call_tool_test",
                                    "type": "function",
                                    "function": {
                                        "name": tool_name,
                                        "arguments": json.dumps(tool_args),
                                    }
                                }]
                            }
                        }]
                    }
                return {
                    "choices": [{
                        "message": {"role": "assistant", "content": final_text}
                    }]
                }
        else:
            def handler(request):
                call_count[0] += 1
                if call_count[0] == 1:
                    return {
                        "content": [{
                            "type": "tool_use",
                            "id": "tool_test_abc",
                            "name": tool_name,
                            "input": tool_args,
                        }]
                    }
                return {
                    "content": [{"type": "text", "text": final_text}]
                }

        return handler, call_count

    def _set_handler(self, handler):
        if self.API_TYPE == "openai":
            self.mock_server.set_openai_handler(handler)
        else:
            self.mock_server.set_anthropic_handler(handler)

    # -- request validation helpers ------------------------------------------

    def _get_tool_result_content(self):
        """Extract the tool-result content string from the *last* mock request."""
        req = self.mock_server.last_request["body"]

        if self.API_TYPE == "openai":
            messages = req["messages"]
            tool_msgs = [m for m in messages if m["role"] == "tool"]
            assert len(tool_msgs) > 0, "Expected at least one tool-result message"
            assert tool_msgs[-1]["tool_call_id"] == "call_tool_test"
            return tool_msgs[-1]["content"]
        else:
            messages = req["messages"]
            user_msgs = [m for m in messages if m["role"] == "user"]
            last_content = user_msgs[-1]["content"]
            tool_results = [b for b in last_content if b["type"] == "tool_result"]
            assert len(tool_results) > 0, "Expected at least one tool_result block"
            assert tool_results[0]["tool_use_id"] == "tool_test_abc"
            return tool_results[0]["content"]

    def _validate_tool_call_in_first_request(self, tool_name, expected_args):
        """Verify the first request after the user query contained the right
        tool-call with the expected arguments."""
        reqs = self.mock_server.requests
        post_reqs = [r for r in reqs if r["path"].endswith(
            "/chat/completions" if self.API_TYPE == "openai" else "/messages"
        )]
        assert len(post_reqs) >= 2, "Expected at least two API round-trips (tool call + result)"

    # -- approval dialog helper ----------------------------------------------

    def _approve_tool_execution(self, child):
        """Wait for the FTXUI approval dialog and press Enter to approve."""
        child.expect("Approve execution", timeout=15)
        self._send_enter(child)

    # -- convenience ---------------------------------------------------------

    def _spawn(self):
        return self.spawn_ai_interactive(self.API_TYPE)

    # -----------------------------------------------------------------------
    # Tool tests
    # -----------------------------------------------------------------------

    def test_request_interrupting(self):
        def handler(request):
            time.sleep(5)
            return {"choices": [{"message": {"role": "assistant", "content": "Ping response."}}]}

        self.mock_server.set_openai_handler(handler)
        child = self.spawn_ai_interactive("openai")
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "ping")
            child.expect("Agent is thinking...", timeout=15)
            child.sendintr()
            child.expect("<INTERRUPTED>", timeout=10)
            child.expect("Request to model API was interrupted", timeout=10)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_list_directory_tool(self):
        """list_directory tool: lists database root directory, returns JSON
        with the test table, and sends correct parameters to the API."""
        handler, call_count = self._make_tool_handler(
            "list_directory", {"directory": ""}, "Directory listed successfully."
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "list the root directory")
            child.expect("Directory listed successfully", timeout=30)
            self._wait_for_ai_prompt(child)

            assert call_count[0] >= 2, "Tool round-trip requires at least 2 API calls"
            self._validate_tool_call_in_first_request("list_directory", {"directory": ""})

            tool_result = self._get_tool_result_content()
            parsed = json.loads(tool_result)
            assert isinstance(parsed, list), "list_directory must return a JSON array"
            names = [entry["name"] for entry in parsed]
            assert self.tmp_path.name in names, (
                "Expected test table '{}' in directory listing, got: {}".format(
                    self.tmp_path.name, names
                )
            )

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_list_directory_tool_schema_in_request(self):
        """list_directory tool definition sent to the API has the correct
        ``directory`` parameter in its schema."""
        handler, _ = self._make_tool_handler(
            "list_directory", {"directory": ""}, "ok"
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "list")
            child.expect("ok", timeout=30)
            self._wait_for_ai_prompt(child)

            reqs = self.mock_server.requests
            post_reqs = [r for r in reqs if r["path"].endswith(
                "/chat/completions" if self.API_TYPE == "openai" else "/messages"
            )]
            first_body = post_reqs[0]["body"]

            if self.API_TYPE == "openai":
                tools = first_body["tools"]
                ld = [t for t in tools if t["function"]["name"] == "list_directory"][0]
                props = ld["function"]["parameters"]["properties"]
                assert "directory" in props
            else:
                tools = first_body["tools"]
                ld = [t for t in tools if t["name"] == "list_directory"][0]
                props = ld["input_schema"]["properties"]
                assert "directory" in props

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_describe_tool(self):
        """describe tool: describes the test table and returns schema
        information including column names."""
        table_relative = self.tmp_path.name
        handler, call_count = self._make_tool_handler(
            "describe", {"path": table_relative}, "Table described successfully."
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "describe the table")
            child.expect("Table described successfully", timeout=30)
            self._wait_for_ai_prompt(child)

            assert call_count[0] >= 2
            self._validate_tool_call_in_first_request("describe", {"path": table_relative})

            tool_result = self._get_tool_result_content()
            assert len(tool_result) > 0, "describe must return non-empty content"
            # The table has columns id (Uint32) and name (Utf8)
            assert "id" in tool_result.lower() or "Id" in tool_result, \
                "describe result must mention column 'id'"

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_describe_tool_with_optional_params(self):
        """describe tool: optional boolean parameters are accepted."""
        table_relative = self.tmp_path.name
        handler, call_count = self._make_tool_handler(
            "describe",
            {"path": table_relative, "permissions": True, "stats": True},
            "Describe with options done."
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "describe table with stats")
            child.expect("Describe with options done", timeout=30)
            self._wait_for_ai_prompt(child)

            assert call_count[0] >= 2
            tool_result = self._get_tool_result_content()
            assert len(tool_result) > 0

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_describe_tool_schema_in_request(self):
        """describe tool definition has required ``path`` and optional boolean
        parameters in its schema."""
        handler, _ = self._make_tool_handler(
            "describe", {"path": "x"}, "ok"
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "desc")
            child.expect("ok", timeout=30)
            self._wait_for_ai_prompt(child)

            first_body = [
                r for r in self.mock_server.requests
                if r["path"].endswith("/chat/completions" if self.API_TYPE == "openai" else "/messages")
            ][0]["body"]

            if self.API_TYPE == "openai":
                tools = first_body["tools"]
                dt = [t for t in tools if t["function"]["name"] == "describe"][0]
                props = dt["function"]["parameters"]["properties"]
            else:
                tools = first_body["tools"]
                dt = [t for t in tools if t["name"] == "describe"][0]
                props = dt["input_schema"]["properties"]

            assert "path" in props
            assert "permissions" in props
            assert "partition_boundaries" in props
            assert "stats" in props
            assert "partition_stats" in props

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_ydb_help_tool(self):
        """ydb_help tool: returns help text for the CLI."""
        handler, call_count = self._make_tool_handler(
            "ydb_help", {"command": []}, "Help retrieved successfully."
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "show ydb help")
            child.expect("Help retrieved successfully", timeout=30)
            self._wait_for_ai_prompt(child)

            assert call_count[0] >= 2
            self._validate_tool_call_in_first_request("ydb_help", {"command": ""})

            tool_result = self._get_tool_result_content()
            assert len(tool_result) > 0, "ydb_help must return non-empty content"
            assert "Administrative cluster operations" in tool_result, tool_result

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_ydb_help_tool_with_subcommand(self):
        """ydb_help tool: returns help for a specific subcommand."""
        handler, call_count = self._make_tool_handler(
            "ydb_help", {"command": ["scheme"]}, "Scheme help done."
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "help for scheme")
            child.expect("Scheme help done", timeout=30)
            self._wait_for_ai_prompt(child)

            assert call_count[0] >= 2
            tool_result = self._get_tool_result_content()
            assert len(tool_result) > 0
            assert "Scheme service operations" in tool_result, tool_result

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_ydb_help_tool_with_invalid_subcommand(self):
        handler, call_count = self._make_tool_handler(
            "ydb_help", {"command": ["invalid"]}, "Invalid command 'invalid'"
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "help for invalid")
            child.expect("Invalid command 'invalid'", timeout=30)
            self._wait_for_ai_prompt(child)

            assert call_count[0] >= 2
            tool_result = self._get_tool_result_content()
            assert len(tool_result) > 0
            assert "Invalid command 'invalid'" in tool_result, tool_result

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_ydb_help_tool_schema_in_request(self):
        """ydb_help tool definition has ``command`` parameter in its schema."""
        handler, _ = self._make_tool_handler(
            "ydb_help", {"command": []}, "ok"
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "help")
            child.expect("ok", timeout=30)
            self._wait_for_ai_prompt(child)

            first_body = [
                r for r in self.mock_server.requests
                if r["path"].endswith("/chat/completions" if self.API_TYPE == "openai" else "/messages")
            ][0]["body"]

            if self.API_TYPE == "openai":
                tools = first_body["tools"]
                ht = [t for t in tools if t["function"]["name"] == "ydb_help"][0]
                props = ht["function"]["parameters"]["properties"]
            else:
                tools = first_body["tools"]
                ht = [t for t in tools if t["name"] == "ydb_help"][0]
                props = ht["input_schema"]["properties"]

            assert "command" in props

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_exec_query_tool(self):
        """exec_query tool: executes a SELECT query after user approval and
        returns result set with columns and rows."""
        handler, call_count = self._make_tool_handler(
            "exec_query", {"query": "SELECT 1 AS val"}, "Query executed."
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "run a query")
            child.expect("Agent wants to execute query", timeout=15)
            child.expect("SELECT.*1.*AS.*val", timeout=15)

            # The CLI shows an approval dialog before executing
            self._approve_tool_execution(child)

            child.expect("Query executed", timeout=30)
            self._wait_for_ai_prompt(child)

            assert call_count[0] >= 2
            self._validate_tool_call_in_first_request("exec_query", {"query": "SELECT 1 AS val"})

            tool_result = self._get_tool_result_content()
            parsed = json.loads(tool_result)
            assert isinstance(parsed, list), "exec_query must return a JSON array of result sets"
            assert len(parsed) == 1, "SELECT 1 must produce at least one result set"
            result_set = parsed[0]
            assert "columns" in result_set
            assert "rows" in result_set
            assert len(result_set["rows"]) == 1
            assert result_set["truncated"] is False
            assert result_set["row_count"] == 1
            assert result_set["byte_count"] > 0
            assert result_set["rows"][0]["val"] == 1

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_exec_query_tool_truncated(self):
        handler, call_count = self._make_tool_handler(
            "exec_query", {"query": "SELECT * FROM AS_TABLE(ListReplicate(<|val: 42|>, 1100))"}, "Query executed."
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "run a query")

            # The CLI shows an approval dialog before executing
            self._approve_tool_execution(child)

            child.expect("Query executed", timeout=30)
            self._wait_for_ai_prompt(child)

            assert call_count[0] >= 2
            self._validate_tool_call_in_first_request("exec_query", {"query": "SELECT * FROM AS_TABLE(ListReplicate(<|val: 42|>, 1100))"})

            tool_result = self._get_tool_result_content()
            parsed = json.loads(tool_result)
            assert isinstance(parsed, list), "exec_query must return a JSON array of result sets"
            assert len(parsed) == 1, "SELECT must produce at least one result set"
            result_set = parsed[0]
            assert len(result_set["rows"]) == 1000
            assert result_set["truncated"] is True
            assert result_set["row_count"] == 1100
            assert result_set["byte_count"] > 0
            assert "truncated" in result_set["truncatedMessage"]
            assert result_set["rows"][0]["val"] == 42

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_exec_query_tool_query_failed(self):
        handler, _ = self._make_tool_handler(
            "exec_query", {"query": "SELECT 1 FROM unknown_table"}, "Query executed."
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "run a query")
            child.expect("Agent wants to execute query", timeout=15)
            child.expect("SELECT.*1.*FROM.*unknown_table", timeout=15)
            self._approve_tool_execution(child)
            child.expect("Query execution failed.*unknown_table", timeout=30)
            child.expect("Query executed", timeout=30)
            self._wait_for_ai_prompt(child)
            tool_result = self._get_tool_result_content()
            assert "unknown_table" in tool_result

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_exec_query_tool_schema_in_request(self):
        """exec_query tool definition has ``query`` parameter in its schema."""
        handler, _ = self._make_tool_handler(
            "exec_query", {"query": "SELECT 1"}, "ok"
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "query")
            self._approve_tool_execution(child)
            child.expect("ok", timeout=30)
            self._wait_for_ai_prompt(child)

            first_body = [
                r for r in self.mock_server.requests
                if r["path"].endswith("/chat/completions" if self.API_TYPE == "openai" else "/messages")
            ][0]["body"]

            if self.API_TYPE == "openai":
                tools = first_body["tools"]
                eq = [t for t in tools if t["function"]["name"] == "exec_query"][0]
                props = eq["function"]["parameters"]["properties"]
            else:
                tools = first_body["tools"]
                eq = [t for t in tools if t["name"] == "exec_query"][0]
                props = eq["input_schema"]["properties"]

            assert "query" in props

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_explain_query_tool(self):
        """explain_query tool: explains a query and returns the query plan and AST."""
        handler, call_count = self._make_tool_handler(
            "explain_query", {"query": "SELECT 1 AS val"}, "Query explained."
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "explain a query")
            child.expect("Explaining query", timeout=15)
            child.expect("SELECT.*1.*AS.*val", timeout=15)
            child.expect("Query explained", timeout=30)
            self._wait_for_ai_prompt(child)

            assert call_count[0] >= 2
            self._validate_tool_call_in_first_request("explain_query", {"query": "SELECT 1 AS val"})
            tool_result = self._get_tool_result_content()
            parsed = json.loads(tool_result)
            assert isinstance(parsed, dict), "explain_query must return a JSON object"
            assert "plan" in parsed
            assert "ast" in parsed
            assert len(parsed["plan"]) > 0
            assert len(parsed["ast"]) > 0
            assert "val" in parsed["ast"]
            assert "val" in parsed["plan"]

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_explain_query_tool_query_failed(self):
        """explain_query tool: returns an error if the query is invalid."""
        handler, _ = self._make_tool_handler(
            "explain_query", {"query": "SELECT 1 FROM unknown_table"}, "Query explained."
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "explain a query")
            child.expect("Explaining query", timeout=15)
            child.expect("Query explain failed.*unknown_table", timeout=30)
            child.expect("Query explained", timeout=30)
            self._wait_for_ai_prompt(child)
            tool_result = self._get_tool_result_content()
            assert "unknown_table" in tool_result

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_explain_query_tool_schema_in_request(self):
        """explain_query tool definition has ``query`` parameter in its schema."""
        handler, _ = self._make_tool_handler(
            "explain_query", {"query": "SELECT 1"}, "ok"
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "explain")
            child.expect("ok", timeout=30)
            self._wait_for_ai_prompt(child)

            first_body = [
                r for r in self.mock_server.requests
                if r["path"].endswith("/chat/completions" if self.API_TYPE == "openai" else "/messages")
            ][0]["body"]

            if self.API_TYPE == "openai":
                tools = first_body["tools"]
                eq = [t for t in tools if t["function"]["name"] == "explain_query"][0]
                props = eq["function"]["parameters"]["properties"]
            else:
                tools = first_body["tools"]
                eq = [t for t in tools if t["name"] == "explain_query"][0]
                props = eq["input_schema"]["properties"]

            assert "query" in props

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_exec_shell_tool(self):
        """exec_shell tool: executes a shell command after user approval and
        returns the command output."""
        handler, call_count = self._make_tool_handler(
            "exec_shell", {"command": "echo tool_shell_test_output"}, "Shell command done."
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "run echo command")

            # The CLI shows an approval dialog before executing
            self._approve_tool_execution(child)

            child.expect("Shell command done", timeout=30)
            self._wait_for_ai_prompt(child)

            assert call_count[0] >= 2
            self._validate_tool_call_in_first_request("exec_shell", {"command": "echo tool_shell_test_output"})

            tool_result = self._get_tool_result_content()
            assert "tool_shell_test_output" in tool_result, \
                "exec_shell result must contain the echo output"

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_exec_shell_tool_nonzero_exit(self):
        """exec_shell tool: a command that exits with non-zero code reports
        the exit code in the result."""
        handler, call_count = self._make_tool_handler(
            "exec_shell", {"command": "exit 42"}, "Shell failure noted."
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "run failing command")
            self._approve_tool_execution(child)
            child.expect("Shell failure noted", timeout=30)
            self._wait_for_ai_prompt(child)

            assert call_count[0] >= 2
            tool_result = self._get_tool_result_content()
            assert "exit code" in tool_result.lower() or "42" in tool_result, \
                "exec_shell must report non-zero exit code"

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()

    def test_exec_shell_tool_schema_in_request(self):
        """exec_shell tool definition has ``command`` parameter in its schema."""
        handler, _ = self._make_tool_handler(
            "exec_shell", {"command": "echo x"}, "ok"
        )
        self._set_handler(handler)
        child = self._spawn()
        try:
            child.expect("Welcome to YDB CLI", timeout=15)
            self._wait_for_ai_prompt(child)
            self._send_query(child, "shell")
            self._approve_tool_execution(child)
            child.expect("ok", timeout=30)
            self._wait_for_ai_prompt(child)

            first_body = [
                r for r in self.mock_server.requests
                if r["path"].endswith("/chat/completions" if self.API_TYPE == "openai" else "/messages")
            ][0]["body"]

            if self.API_TYPE == "openai":
                tools = first_body["tools"]
                es = [t for t in tools if t["function"]["name"] == "exec_shell"][0]
                props = es["function"]["parameters"]["properties"]
            else:
                tools = first_body["tools"]
                es = [t for t in tools if t["name"] == "exec_shell"][0]
                props = es["input_schema"]["properties"]

            assert "command" in props

            self._send_query(child, "exit")
            child.expect("Bye!", timeout=10)
        finally:
            self.mock_server.clear()
            child.close()


# ---------------------------------------------------------------------------
# Tool tests — OpenAI API
# ---------------------------------------------------------------------------

class TestToolsOpenAI(_ToolTestBase):
    API_TYPE = "openai"


# ---------------------------------------------------------------------------
# Tool tests — Anthropic API
# ---------------------------------------------------------------------------

class TestToolsAnthropic(_ToolTestBase):
    API_TYPE = "anthropic"
