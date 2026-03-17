import logging
from typing import Optional

import pytest

import argclass
from argclass import SecretString


def test_secret_string():
    s = SecretString("password")
    assert repr(s) != str(s)
    assert repr(s) == repr(SecretString.PLACEHOLDER)
    assert f"{s!r}" == repr(SecretString.PLACEHOLDER)


def test_secret_log(caplog):
    s = SecretString("password")
    caplog.set_level(logging.INFO)

    with caplog.at_level(logging.INFO):
        logging.info(s)

    assert caplog.records[1].message == SecretString.PLACEHOLDER
    caplog.records.clear()

    with caplog.at_level(logging.INFO):
        logging.info("%s", s)

    assert caplog.records[0].message == SecretString.PLACEHOLDER
    caplog.records.clear()


def test_secret_with_type_annotation():
    """Test that Secret() with type annotation preserves the type."""

    class Parser(argclass.Parser):
        # No type annotation - should be SecretString
        password = argclass.Secret()
        # With int type - should preserve int
        seed: int = argclass.Secret()
        # With str type - should be SecretString
        api_key: str = argclass.Secret()
        # Optional int - should preserve int or None
        opt_seed: Optional[int] = argclass.Secret()

    parser = Parser()
    parser.parse_args(
        [
            "--password",
            "secret123",
            "--seed",
            "42",
            "--api-key",
            "key-abc",
        ]
    )

    # password (no type) should be SecretString
    assert isinstance(parser.password, SecretString)
    assert repr(parser.password) == repr(SecretString.PLACEHOLDER)

    # seed (int type) should be int
    assert isinstance(parser.seed, int)
    assert parser.seed == 42
    assert parser.seed + 10 == 52  # verify int operations work

    # api_key (str type) should be SecretString
    assert isinstance(parser.api_key, SecretString)
    assert repr(parser.api_key) == repr(SecretString.PLACEHOLDER)

    # opt_seed (Optional[int]) should be None when not provided
    assert parser.opt_seed is None


def test_secret_default_not_in_help(capsys):
    """Test that secret default values don't appear in --help output."""

    class Parser(argclass.Parser):
        # Non-secret with default - SHOULD show in help
        username: str = argclass.Argument(default="admin")
        # Secret with default - should NOT show in help
        password = argclass.Secret(default="secret-password-123")
        api_key: str = argclass.Secret(default="api-key-xyz")

    parser = Parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--help"])

    captured = capsys.readouterr()
    help_output = captured.out

    # Non-secret default SHOULD be visible
    assert "(default: admin)" in help_output

    # Secret defaults should NOT be visible
    assert "secret-password-123" not in help_output
    assert "api-key-xyz" not in help_output
    assert "(default:" not in help_output.split("--password")[1].split("\n")[0]


def test_secret_no_leak_in_help_with_env_var(monkeypatch, capsys):
    """Test that secret values from env vars don't leak in --help output."""

    class Parser(argclass.Parser):
        secret = argclass.Secret()
        api_key: str = argclass.Secret()

    monkeypatch.setenv("CONFIG_SECRET", "super-secret-password")
    monkeypatch.setenv("CONFIG_API_KEY", "api-key-12345")

    parser = Parser(auto_env_var_prefix="CONFIG_")

    with pytest.raises(SystemExit):
        parser.parse_args(["--help"])

    captured = capsys.readouterr()
    help_output = captured.out

    # Verify secrets are NOT in help output
    assert "super-secret-password" not in help_output
    assert "api-key-12345" not in help_output

    # Verify env var names ARE shown (but not values)
    assert "CONFIG_SECRET" in help_output
    assert "CONFIG_API_KEY" in help_output

    # Verify no "(default: ...)" for secrets
    assert "(default:" not in help_output


def test_secret_no_leak_in_help_with_config(tmp_path, capsys):
    """Test that secret values from config files don't leak in --help output."""
    config_file = tmp_path / "config.ini"
    config_file.write_text(
        "[DEFAULT]\nsecret = config-secret-value\napi_key = config-api-key\n"
    )

    class Parser(argclass.Parser):
        secret = argclass.Secret()
        api_key: str = argclass.Secret()

    parser = Parser(config_files=[str(config_file)])

    with pytest.raises(SystemExit):
        parser.parse_args(["--help"])

    captured = capsys.readouterr()
    help_output = captured.out

    # Verify secrets from config are NOT in help output
    assert "config-secret-value" not in help_output
    assert "config-api-key" not in help_output


def test_secret_from_env_var_is_secret_string(monkeypatch):
    """Test that secrets from env vars are wrapped in SecretString."""

    class Parser(argclass.Parser):
        secret = argclass.Secret()
        api_key: str = argclass.Secret()

    monkeypatch.setenv("CONFIG_SECRET", "env-secret")
    monkeypatch.setenv("CONFIG_API_KEY", "env-api-key")

    parser = Parser(auto_env_var_prefix="CONFIG_")
    args = parser.parse_args([])

    # Both should be SecretString
    assert isinstance(args.secret, SecretString)
    assert isinstance(args.api_key, SecretString)

    # repr should be masked
    assert repr(args.secret) == repr(SecretString.PLACEHOLDER)
    assert repr(args.api_key) == repr(SecretString.PLACEHOLDER)

    # Actual value should be accessible via real_str
    assert str.__str__(args.secret) == "env-secret"
    assert str.__str__(args.api_key) == "env-api-key"


def test_secret_from_config_file_is_secret_string(tmp_path):
    """Test that secrets from config files are wrapped in SecretString."""
    config_file = tmp_path / "config.ini"
    config_file.write_text(
        "[DEFAULT]\nsecret = config-secret\napi_key = config-api-key\n"
    )

    class Parser(argclass.Parser):
        secret = argclass.Secret()
        api_key: str = argclass.Secret()

    parser = Parser(config_files=[str(config_file)])
    args = parser.parse_args([])

    # Both should be SecretString
    assert isinstance(args.secret, SecretString)
    assert isinstance(args.api_key, SecretString)

    # repr should be masked
    assert repr(args.secret) == repr(SecretString.PLACEHOLDER)
    assert repr(args.api_key) == repr(SecretString.PLACEHOLDER)

    # Actual value should be accessible
    assert str.__str__(args.secret) == "config-secret"
    assert str.__str__(args.api_key) == "config-api-key"


def test_secret_from_cli_is_secret_string():
    """Test that secrets from CLI args are wrapped in SecretString."""

    class Parser(argclass.Parser):
        secret = argclass.Secret()
        api_key: str = argclass.Secret()

    parser = Parser()
    args = parser.parse_args(
        ["--secret", "cli-secret", "--api-key", "cli-api-key"]
    )

    # Both should be SecretString
    assert isinstance(args.secret, SecretString)
    assert isinstance(args.api_key, SecretString)

    # repr should be masked
    assert repr(args.secret) == repr(SecretString.PLACEHOLDER)
    assert repr(args.api_key) == repr(SecretString.PLACEHOLDER)


def test_secret_logging_does_not_leak(tmp_path, caplog):
    """Test that logging secrets doesn't leak the value."""
    config_file = tmp_path / "config.ini"
    config_file.write_text("[DEFAULT]\nsecret = logging-test-secret\n")

    class Parser(argclass.Parser):
        secret = argclass.Secret()

    parser = Parser(config_files=[str(config_file)])
    args = parser.parse_args([])

    caplog.set_level(logging.INFO)

    with caplog.at_level(logging.INFO):
        logging.info("Secret: %s", args.secret)

    # The log should contain the placeholder, not the actual secret
    assert "logging-test-secret" not in caplog.text
    assert SecretString.PLACEHOLDER in caplog.text


def test_non_string_secret_types():
    """Test that secrets with non-string types work correctly."""

    class Parser(argclass.Parser):
        # int secret - should remain int
        seed: int = argclass.Secret()
        # float secret - should remain float
        rate: float = argclass.Secret()
        # Optional int - should work with None
        port: Optional[int] = argclass.Secret()

    parser = Parser()
    args = parser.parse_args(["--seed", "42", "--rate", "3.14"])

    # Types should be preserved (not converted to SecretString)
    assert isinstance(args.seed, int)
    assert args.seed == 42

    assert isinstance(args.rate, float)
    assert abs(args.rate - 3.14) < 0.001

    # Optional should default to None
    assert args.port is None


def test_secret_combined_sources_priority(tmp_path, monkeypatch):
    """Test that CLI args override env vars which override config."""
    config_file = tmp_path / "config.ini"
    config_file.write_text("[DEFAULT]\nsecret = config-value\n")

    monkeypatch.setenv("CONFIG_SECRET", "env-value")

    class Parser(argclass.Parser):
        secret = argclass.Secret()

    # With all three sources, CLI should win
    parser1 = Parser(
        config_files=[str(config_file)], auto_env_var_prefix="CONFIG_"
    )
    args1 = parser1.parse_args(["--secret", "cli-value"])
    assert str.__str__(args1.secret) == "cli-value"

    # Without CLI, env var should win over config
    parser2 = Parser(
        config_files=[str(config_file)], auto_env_var_prefix="CONFIG_"
    )
    args2 = parser2.parse_args([])
    assert str.__str__(args2.secret) == "env-value"

    # Without env var, config should be used
    monkeypatch.delenv("CONFIG_SECRET")
    parser3 = Parser(
        config_files=[str(config_file)], auto_env_var_prefix="CONFIG_"
    )
    args3 = parser3.parse_args([])
    assert str.__str__(args3.secret) == "config-value"
