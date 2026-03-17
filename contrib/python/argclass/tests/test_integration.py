"""
Integration tests simulating a complex DevOps CLI tool ("infractl").

This module exercises all argclass features together:
- Subparsers (single and nested levels)
- Reusable groups
- All argument types (str, int, float, bool, Path, enums, lists, etc.)
- Configuration priority chain (default < config < env < CLI)
- Nested commands
- Secret arguments
- Nargs variations
- Custom type converters
- Config file formats (INI, JSON)
- Environment variables
- Error handling
"""

import json
import logging
from enum import IntEnum
from pathlib import Path
from typing import Any, List, Optional
from unittest.mock import patch

import pytest

import argclass


# ==============================================================================
# Reusable Groups (4 groups)
# ==============================================================================


class ConnectionGroup(argclass.Group):
    """Reusable connection settings."""

    host: str = "localhost"
    port: int = 8080
    timeout: int = 30
    ssl: bool = False


class LoggingGroup(argclass.Group):
    """Reusable logging settings."""

    level: int = argclass.LogLevel
    file: Optional[Path] = None
    format: str = argclass.Argument(
        choices=["json", "text", "compact"],
        default="text",
    )


class OutputGroup(argclass.Group):
    """Reusable output formatting."""

    format: str = argclass.Argument(
        choices=["json", "yaml", "table", "plain"],
        default="table",
    )
    verbose: bool = False
    quiet: bool = False


class RetryGroup(argclass.Group):
    """Reusable retry settings."""

    max_retries: int = 3
    delay: float = 1.0
    backoff: float = 2.0


# ==============================================================================
# Nested Subcommands for ServerCommand
# ==============================================================================


class ServerStartCommand(argclass.Parser):
    """Start a server."""

    daemon: bool = False
    workers: int = 4
    bind: str = "0.0.0.0"


class ServerStopCommand(argclass.Parser):
    """Stop a server."""

    force: bool = False
    timeout: int = 10


class ServerStatusCommand(argclass.Parser):
    """Check server status."""

    detailed: bool = False


class ServerRestartCommand(argclass.Parser):
    """Restart a server."""

    graceful: bool = True
    delay: int = 5


# ==============================================================================
# Nested Subcommands for DatabaseCommand
# ==============================================================================


class DatabaseBackupCommand(argclass.Parser):
    """Backup database."""

    output: Optional[Path] = None
    compress: bool = True
    tables: Optional[List[str]] = argclass.Argument(nargs="*")


class DatabaseRestoreCommand(argclass.Parser):
    """Restore database."""

    input_file: Optional[Path] = None
    drop_existing: bool = False


class DatabaseMigrateCommand(argclass.Parser):
    """Run database migrations."""

    target: Optional[str] = None
    dry_run: bool = False


# ==============================================================================
# Nested Subcommands for UserCommand
# ==============================================================================


class UserCreateCommand(argclass.Parser):
    """Create a user."""

    username: str
    email: str
    admin: bool = False
    groups: Optional[List[str]] = argclass.Argument(nargs="*")


class UserDeleteCommand(argclass.Parser):
    """Delete a user."""

    username: str
    force: bool = False


class UserListCommand(argclass.Parser):
    """List users."""

    filter: Optional[str] = None
    limit: int = 100


# ==============================================================================
# Nested Subcommands for ConfigCommand
# ==============================================================================


class ConfigShowCommand(argclass.Parser):
    """Show configuration."""

    section: Optional[str] = None
    include_defaults: bool = True


class ConfigSetCommand(argclass.Parser):
    """Set configuration value."""

    key: str
    value: str


class ConfigGetCommand(argclass.Parser):
    """Get configuration value."""

    key: str


# ==============================================================================
# Top-Level Subparsers (5 commands)
# ==============================================================================


class ServerCommand(argclass.Parser):
    """Server management."""

    connection: ConnectionGroup = ConnectionGroup(title="Connection settings")
    logging: LoggingGroup = LoggingGroup(title="Logging settings")

    start: Optional[ServerStartCommand] = ServerStartCommand()
    stop: Optional[ServerStopCommand] = ServerStopCommand()
    status: Optional[ServerStatusCommand] = ServerStatusCommand()
    restart: Optional[ServerRestartCommand] = ServerRestartCommand()


class DatabaseCommand(argclass.Parser):
    """Database operations."""

    connection: ConnectionGroup = ConnectionGroup(
        title="Connection settings",
        defaults={"host": "db.localhost", "port": 5432},
    )
    retry: RetryGroup = RetryGroup(title="Retry settings")

    backup: Optional[DatabaseBackupCommand] = DatabaseBackupCommand()
    restore: Optional[DatabaseRestoreCommand] = DatabaseRestoreCommand()
    migrate: Optional[DatabaseMigrateCommand] = DatabaseMigrateCommand()


class UserCommand(argclass.Parser):
    """User management."""

    output: OutputGroup = OutputGroup(title="Output settings")

    create: Optional[UserCreateCommand] = UserCreateCommand()
    delete: Optional[UserDeleteCommand] = UserDeleteCommand()
    list: Optional[UserListCommand] = UserListCommand()


class ConfigCommand(argclass.Parser):
    """Configuration management."""

    output: OutputGroup = OutputGroup(title="Output settings")

    show: Optional[ConfigShowCommand] = ConfigShowCommand()
    set: Optional[ConfigSetCommand] = ConfigSetCommand()
    get: Optional[ConfigGetCommand] = ConfigGetCommand()


class DeployEnvironment(IntEnum):
    """Deployment environment."""

    DEV = 1
    STAGING = 2
    PROD = 3


class DeployCommand(argclass.Parser):
    """Deployment operations."""

    connection: ConnectionGroup = ConnectionGroup(title="Connection settings")
    logging: LoggingGroup = LoggingGroup(title="Logging settings")
    retry: RetryGroup = RetryGroup(title="Retry settings")

    api_key: str = argclass.Secret(env_var="INFRACTL_API_KEY")
    targets: List[str] = argclass.Argument(
        "--targets",
        nargs="+",
        metavar="TARGET",
    )
    environment: DeployEnvironment = argclass.EnumArgument(
        DeployEnvironment,
        default=DeployEnvironment.DEV,
    )
    dry_run: bool = False
    parallel: int = 4


# ==============================================================================
# Main Parser
# ==============================================================================


class InfractlParser(argclass.Parser):
    """Infrastructure control CLI."""

    # Global options
    debug: bool = False
    config_file = argclass.Config(config_class=argclass.INIConfig)

    # Subcommands
    server: Optional[ServerCommand] = ServerCommand()
    database: Optional[DatabaseCommand] = DatabaseCommand()
    user: Optional[UserCommand] = UserCommand()
    config: Optional[ConfigCommand] = ConfigCommand()
    deploy: Optional[DeployCommand] = DeployCommand()


# ==============================================================================
# Fixtures
# ==============================================================================


@pytest.fixture
def ini_config(tmp_path: Path) -> Path:
    """Create test INI config file."""
    config = tmp_path / "infractl.ini"
    config.write_text(
        """[DEFAULT]
debug = true

[server]
host = config-server.example.com
port = 9000
ssl = yes

[database]
host = db.example.com
port = 5432

[logging]
level = debug
format = json
"""
    )
    return config


@pytest.fixture
def json_config(tmp_path: Path) -> Path:
    """Create test JSON config file."""
    config = tmp_path / "infractl.json"
    config.write_text(
        json.dumps(
            {
                "debug": True,
                "server": {"host": "json-server.example.com", "port": 8888},
                "database": {"host": "json-db.example.com"},
            }
        )
    )
    return config


# ==============================================================================
# Test Categories
# ==============================================================================


class TestParserConstruction:
    """Test parser instantiation and structure."""

    def test_parser_instantiation_no_args(self):
        """Test parser can be instantiated without args."""
        parser = InfractlParser()
        assert parser is not None

    def test_parser_repr(self):
        """Test parser repr shows correct counts."""
        parser = InfractlParser()
        r = repr(parser)
        assert "Parser" in r
        assert "arguments" in r
        assert "subparsers" in r

    def test_help_text_generation(self, capsys: pytest.CaptureFixture[str]):
        """Test help text is generated correctly."""
        parser = InfractlParser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])
        captured = capsys.readouterr()
        assert "server" in captured.out
        assert "database" in captured.out
        assert "user" in captured.out
        assert "config" in captured.out
        assert "deploy" in captured.out

    def test_all_subparsers_accessible(self):
        """Test all subparsers are defined."""
        parser = InfractlParser()
        parser.parse_args(["server", "start"])
        assert parser.server is not None
        assert parser.server.start is not None  # type: ignore[union-attr]

    def test_nested_subparser_help(self, capsys: pytest.CaptureFixture[str]):
        """Test nested subparser help text."""
        parser = ServerCommand()
        with pytest.raises(SystemExit):
            parser.parse_args(["start", "--help"])
        captured = capsys.readouterr()
        assert "--daemon" in captured.out
        assert "--workers" in captured.out


class TestReusableGroups:
    """Test reusable argument groups."""

    def test_group_defaults_applied(self):
        """Test group default values are applied."""
        parser = ServerCommand()
        parser.parse_args(["start"])
        assert parser.connection.host == "localhost"
        assert parser.connection.port == 8080
        assert parser.connection.timeout == 30
        assert parser.connection.ssl is False

    def test_group_prefix_in_option_names(
        self, capsys: pytest.CaptureFixture[str]
    ):
        """Test group prefix appears in option names."""
        parser = ServerCommand()
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])
        captured = capsys.readouterr()
        assert "--connection-host" in captured.out
        assert "--connection-port" in captured.out

    def test_group_values_from_cli(self):
        """Test group values can be set from CLI."""
        parser = ServerCommand()
        parser.parse_args(
            [
                "--connection-host=example.com",
                "--connection-port=9000",
                "--connection-ssl",
                "start",
            ]
        )
        assert parser.connection.host == "example.com"
        assert parser.connection.port == 9000
        assert parser.connection.ssl is True

    def test_same_group_class_reused(self):
        """Test same group class can be used in different commands."""
        server = ServerCommand()
        database = DatabaseCommand()

        server.parse_args(["--connection-host=server.com", "start"])
        database.parse_args(["--connection-host=db.com", "backup"])

        assert server.connection.host == "server.com"
        assert database.connection.host == "db.com"

    def test_group_with_custom_defaults(self):
        """Test group with custom defaults parameter."""
        parser = DatabaseCommand()
        parser.parse_args(["backup"])
        assert parser.connection.host == "db.localhost"
        assert parser.connection.port == 5432

    def test_logging_group_with_log_level(self):
        """Test LoggingGroup with LogLevel argument."""
        parser = ServerCommand()
        parser.parse_args(["--logging-level=debug", "start"])
        assert parser.logging.level == logging.DEBUG

    def test_logging_group_format_choices(self):
        """Test LoggingGroup format choices."""
        parser = ServerCommand()
        parser.parse_args(["--logging-format=json", "start"])
        assert parser.logging.format == "json"

    def test_output_group_multiple_flags(self):
        """Test OutputGroup with multiple flags."""
        parser = UserCommand()
        parser.parse_args(["--output-format=yaml", "--output-verbose", "list"])
        assert parser.output.format == "yaml"
        assert parser.output.verbose is True
        assert parser.output.quiet is False

    def test_retry_group_float_values(self):
        """Test RetryGroup with float values."""
        parser = DatabaseCommand()
        parser.parse_args(
            [
                "--retry-max-retries=5",
                "--retry-delay=2.5",
                "--retry-backoff=1.5",
                "backup",
            ]
        )
        assert parser.retry.max_retries == 5
        assert parser.retry.delay == 2.5
        assert parser.retry.backoff == 1.5

    def test_multiple_groups_in_one_parser(self):
        """Test multiple groups in one parser."""
        parser = DeployCommand()
        parser.parse_args(
            [
                "--connection-host=deploy.example.com",
                "--logging-level=warning",
                "--retry-max-retries=10",
                "--api-key=secret",
                "--targets=target1",
            ]
        )
        assert parser.connection.host == "deploy.example.com"
        assert parser.logging.level == logging.WARNING
        assert parser.retry.max_retries == 10


class TestSubparsers:
    """Test subparser functionality."""

    def test_nested_subparsers(self):
        """Test nested subparsers (e.g., server start)."""
        parser = InfractlParser()
        parser.parse_args(["server", "start", "--daemon", "--workers=8"])
        assert parser.server.start.daemon is True  # type: ignore[union-attr]
        assert parser.server.start.workers == 8  # type: ignore[union-attr]

    def test_current_subparser_property(self):
        """Test current_subparser returns deepest selected subparser."""
        parser = InfractlParser()
        parser.parse_args(["server", "start"])
        # current_subparser returns the deepest selected subparser
        assert parser.current_subparser is parser.server.start  # type: ignore[union-attr]

        # Check nested current_subparser
        assert parser.server.current_subparser is parser.server.start  # type: ignore[union-attr]

    def test_current_subparser_none_when_not_used(self):
        """Test current_subparser is None when no subparser used."""
        parser = InfractlParser()
        parser.parse_args(["--debug"])
        assert parser.current_subparser is None

    def test_subparser_specific_arguments(self):
        """Test subparser-specific arguments."""
        parser = InfractlParser()
        parser.parse_args(
            [
                "database",
                "backup",
                "--output=/tmp/backup.sql",
                "--tables",
                "users",
                "orders",
            ]
        )
        db = parser.database
        assert db.backup.output == Path("/tmp/backup.sql")  # type: ignore[union-attr]
        assert db.backup.compress is True  # type: ignore[union-attr]
        assert db.backup.tables == ["users", "orders"]  # type: ignore[union-attr]

    def test_unselected_subparser_raises_attribute_error(self):
        """Test accessing unselected subparser's attrs raises error."""
        parser = InfractlParser()
        parser.parse_args(["server", "start"])
        # Accessing nested subparser attribute that wasn't selected
        with pytest.raises(AttributeError):
            _ = parser.server.stop.force  # type: ignore[union-attr]

    def test_all_server_nested_commands(self):
        """Test all nested server commands work."""
        parser = ServerCommand()

        parser.parse_args(["start", "--daemon"])
        assert parser.start.daemon is True  # type: ignore[union-attr]

        parser = ServerCommand()
        parser.parse_args(["stop", "--force"])
        assert parser.stop.force is True  # type: ignore[union-attr]

        parser = ServerCommand()
        parser.parse_args(["status", "--detailed"])
        assert parser.status.detailed is True  # type: ignore[union-attr]

        parser = ServerCommand()
        parser.parse_args(["restart", "--graceful", "--delay=10"])
        # --graceful toggles default True
        assert parser.restart.graceful is False  # type: ignore[union-attr]
        assert parser.restart.delay == 10  # type: ignore[union-attr]

    def test_all_database_nested_commands(self):
        """Test all nested database commands work."""
        parser = DatabaseCommand()
        parser.parse_args(["backup", "--compress"])
        # --compress toggles default True
        assert parser.backup.compress is False  # type: ignore[union-attr]

        parser = DatabaseCommand()
        parser.parse_args(
            ["restore", "--input-file=/tmp/backup.sql", "--drop-existing"]
        )
        assert parser.restore.input_file == Path("/tmp/backup.sql")  # type: ignore[union-attr]
        assert parser.restore.drop_existing is True  # type: ignore[union-attr]

        parser = DatabaseCommand()
        parser.parse_args(["migrate", "--target=v2.0", "--dry-run"])
        assert parser.migrate.target == "v2.0"  # type: ignore[union-attr]
        assert parser.migrate.dry_run is True  # type: ignore[union-attr]

    def test_all_user_nested_commands(self):
        """Test all nested user commands work."""
        parser = UserCommand()
        parser.parse_args(
            [
                "create",
                "--username=john",
                "--email=john@example.com",
                "--admin",
                "--groups",
                "developers",
                "admins",
            ]
        )
        assert parser.create.username == "john"  # type: ignore[union-attr]
        assert parser.create.email == "john@example.com"  # type: ignore[union-attr]
        assert parser.create.admin is True  # type: ignore[union-attr]
        assert parser.create.groups == ["developers", "admins"]  # type: ignore[union-attr]

        parser = UserCommand()
        parser.parse_args(["delete", "--username=john", "--force"])
        assert parser.delete.username == "john"  # type: ignore[union-attr]
        assert parser.delete.force is True  # type: ignore[union-attr]

        parser = UserCommand()
        parser.parse_args(["list", "--filter=admin", "--limit=50"])
        assert parser.list.filter == "admin"  # type: ignore[union-attr]
        assert parser.list.limit == 50  # type: ignore[union-attr]

    def test_all_config_nested_commands(self):
        """Test all nested config commands work."""
        parser = ConfigCommand()
        parser.parse_args(["show", "--section=database"])
        assert parser.show.section == "database"  # type: ignore[union-attr]

        parser = ConfigCommand()
        parser.parse_args(["set", "--key=debug", "--value=true"])
        assert parser.set.key == "debug"  # type: ignore[union-attr]
        assert parser.set.value == "true"  # type: ignore[union-attr]

        parser = ConfigCommand()
        parser.parse_args(["get", "--key=timeout"])
        assert parser.get.key == "timeout"  # type: ignore[union-attr]

    def test_deep_nesting_chain(self):
        """Test parser chain with deep nesting."""
        parser = InfractlParser()
        parser.parse_args(["server", "start"])

        # Get chain from deepest parser
        chain = list(parser.server.start._get_chain())  # type: ignore[union-attr]
        assert len(chain) == 3
        assert chain[0] is parser.server.start  # type: ignore[union-attr]
        assert chain[1] is parser.server
        assert chain[2] is parser


class TestConfigurationPriority:
    """Test configuration priority: Default < Config < Env < CLI."""

    def test_default_value_used(self):
        """Test default value is used when nothing else specified."""
        parser = ServerCommand()
        parser.parse_args(["start"])
        assert parser.connection.host == "localhost"

    def test_config_overrides_default(self, ini_config: Path):
        """Test config file overrides default value."""

        class Parser(argclass.Parser):
            host: str = "localhost"

        parser = Parser(config_files=[ini_config])
        parser.parse_args([])
        # INI has [server] section but host is in DEFAULT section
        # When not in a specific section, the config won't apply
        # Let's test with a specific config

        config_file = ini_config.parent / "test.ini"
        config_file.write_text("[DEFAULT]\nhost = config.example.com\n")

        parser = Parser(config_files=[config_file])
        parser.parse_args([])
        assert parser.host == "config.example.com"

    def test_env_overrides_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Test env var overrides config file."""
        config_file = tmp_path / "config.ini"
        config_file.write_text("[DEFAULT]\nhost = config.example.com\n")

        monkeypatch.setenv("TEST_HOST", "env.example.com")

        class Parser(argclass.Parser):
            host: str = "localhost"

        parser = Parser(config_files=[config_file], auto_env_var_prefix="TEST_")
        parser.parse_args([])
        assert parser.host == "env.example.com"

    def test_cli_overrides_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Test CLI argument overrides env var."""
        config_file = tmp_path / "config.ini"
        config_file.write_text("[DEFAULT]\nhost = config.example.com\n")

        monkeypatch.setenv("TEST_HOST", "env.example.com")

        class Parser(argclass.Parser):
            host: str = "localhost"

        parser = Parser(config_files=[config_file], auto_env_var_prefix="TEST_")
        parser.parse_args(["--host=cli.example.com"])
        assert parser.host == "cli.example.com"

    def test_full_priority_chain(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Test full priority chain: Default < Config < Env < CLI."""
        # Create config file
        config_file = tmp_path / "config.ini"
        config_file.write_text(
            "[DEFAULT]\n"
            "default_only = config\n"
            "config_only = config\n"
            "env_only = config\n"
            "cli_only = config\n"
        )

        # Set env vars
        monkeypatch.setenv("TEST_ENV_ONLY", "env")
        monkeypatch.setenv("TEST_CLI_ONLY", "env")

        class Parser(argclass.Parser):
            default_only: str = "default"
            config_only: str = "default"
            env_only: str = "default"
            cli_only: str = "default"

        parser = Parser(config_files=[config_file], auto_env_var_prefix="TEST_")
        parser.parse_args(["--cli-only=cli"])

        assert parser.default_only == "config"  # config overrides default
        assert parser.config_only == "config"  # config is highest
        assert parser.env_only == "env"  # env overrides config
        assert parser.cli_only == "cli"  # CLI overrides env

    def test_priority_with_groups(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Test priority chain with groups."""
        config_file = tmp_path / "config.ini"
        config_file.write_text("[connection]\nhost = config.example.com\n")

        monkeypatch.setenv("TEST_CONNECTION_HOST", "env.example.com")

        class Parser(argclass.Parser):
            connection: ConnectionGroup = ConnectionGroup()

        parser = Parser(config_files=[config_file], auto_env_var_prefix="TEST_")
        parser.parse_args(["--connection-host=cli.example.com"])
        assert parser.connection.host == "cli.example.com"

    def test_partial_override_in_priority(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Test partial overrides in priority chain."""
        config_file = tmp_path / "config.ini"
        config_file.write_text(
            "[DEFAULT]\nhost = config.example.com\nport = 9000\n"
        )

        # Only override host in env
        monkeypatch.setenv("TEST_HOST", "env.example.com")

        class Parser(argclass.Parser):
            host: str = "localhost"
            port: int = 8080

        parser = Parser(config_files=[config_file], auto_env_var_prefix="TEST_")
        parser.parse_args([])

        # host from env, port from config
        assert parser.host == "env.example.com"
        assert parser.port == 9000

    def test_bool_priority_chain(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Test bool type through priority chain."""
        config_file = tmp_path / "config.ini"
        config_file.write_text("[DEFAULT]\ndebug = true\n")

        class Parser(argclass.Parser):
            debug: bool = False

        # Config should override default
        parser = Parser(config_files=[config_file])
        parser.parse_args([])
        assert parser.debug is True

        # Env should override config
        monkeypatch.setenv("TEST_DEBUG", "false")
        parser = Parser(config_files=[config_file], auto_env_var_prefix="TEST_")
        parser.parse_args([])
        assert parser.debug is False


class TestSecretArguments:
    """Test secret argument handling."""

    def test_secret_masking_in_repr(self, monkeypatch: pytest.MonkeyPatch):
        """Test secret values are masked in repr."""
        monkeypatch.setenv("INFRACTL_API_KEY", "super-secret-key")

        parser = DeployCommand()
        parser.parse_args(["--targets=target1"])

        # repr should show masked value
        assert repr(parser.api_key) == repr(argclass.SecretString.PLACEHOLDER)

    def test_secret_value_accessible(self, monkeypatch: pytest.MonkeyPatch):
        """Test secret actual value is accessible."""
        monkeypatch.setenv("INFRACTL_API_KEY", "super-secret-key")

        parser = DeployCommand()
        parser.parse_args(["--targets=target1"])

        # Actual value should be accessible via str.__str__
        assert str.__str__(parser.api_key) == "super-secret-key"

    def test_secret_from_env_var(self, monkeypatch: pytest.MonkeyPatch):
        """Test secret value from environment variable."""
        monkeypatch.setenv("INFRACTL_API_KEY", "env-api-key")

        parser = DeployCommand()
        parser.parse_args(["--targets=target1"])

        assert str.__str__(parser.api_key) == "env-api-key"

    def test_secret_from_cli(self):
        """Test secret value from CLI."""
        parser = DeployCommand()
        parser.parse_args(["--api-key=cli-api-key", "--targets=target1"])

        assert str.__str__(parser.api_key) == "cli-api-key"

    def test_sanitize_env_removes_secret(self, monkeypatch: pytest.MonkeyPatch):
        """Test sanitize_env removes secret env vars."""
        with patch("os.environ", new={}):
            import os

            os.environ["TEST_SECRET"] = "secret-value"

            class Parser(argclass.Parser):
                secret: str = argclass.Secret(env_var="TEST_SECRET")

            parser = Parser()
            parser.parse_args([])

            assert os.environ.get("TEST_SECRET") == "secret-value"
            parser.sanitize_env(only_secrets=True)
            assert os.environ.get("TEST_SECRET") is None

    def test_secret_not_in_help(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Test secret values don't appear in help."""
        monkeypatch.setenv("INFRACTL_API_KEY", "should-not-appear")

        parser = DeployCommand()
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])

        captured = capsys.readouterr()
        assert "should-not-appear" not in captured.out


class TestNargsVariations:
    """Test nargs parameter variations."""

    def test_nargs_optional_with_const(self):
        """Test nargs='?' with const value."""

        class Parser(argclass.Parser):
            config_path: Optional[str] = argclass.Argument(
                nargs="?",
                const="default.conf",
                default=None,
            )

        parser = Parser()
        parser.parse_args([])
        assert parser.config_path is None

        parser = Parser()
        parser.parse_args(["--config-path"])
        assert parser.config_path == "default.conf"

        parser = Parser()
        parser.parse_args(["--config-path=custom.conf"])
        assert parser.config_path == "custom.conf"

    def test_nargs_zero_or_more(self):
        """Test nargs='*' (zero or more)."""

        class Parser(argclass.Parser):
            files: List[str] = argclass.Argument(nargs="*", default=[])

        parser = Parser()
        parser.parse_args([])
        assert parser.files == []

        parser = Parser()
        parser.parse_args(["--files", "a.txt", "b.txt"])
        assert parser.files == ["a.txt", "b.txt"]

    def test_nargs_one_or_more(self):
        """Test nargs='+' (one or more)."""
        parser = DeployCommand()
        parser.parse_args(
            ["--api-key=key", "--targets", "target1", "target2", "target3"]
        )
        assert parser.targets == ["target1", "target2", "target3"]

    def test_nargs_one_or_more_required(self):
        """Test nargs='+' requires at least one value when used."""

        class Parser(argclass.Parser):
            # Use str type without List to make it required
            targets: str = argclass.Argument(nargs="+", required=True)

        parser = Parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])  # No targets - required=True

    def test_nargs_exact_count(self):
        """Test nargs=N (exact count)."""

        class Parser(argclass.Parser):
            coordinates: List[float] = argclass.Argument(
                nargs=3,
                type=float,
                metavar=("X", "Y", "Z"),
            )

        parser = Parser()
        parser.parse_args(["--coordinates", "1.0", "2.0", "3.0"])
        assert parser.coordinates == [1.0, 2.0, 3.0]

    def test_nargs_with_type_conversion(self):
        """Test nargs with type conversion."""

        class Parser(argclass.Parser):
            ports: List[int] = argclass.Argument(
                nargs="+",
                type=int,
            )

        parser = Parser()
        parser.parse_args(["--ports", "80", "443", "8080"])
        assert parser.ports == [80, 443, 8080]
        assert all(isinstance(p, int) for p in parser.ports)

    def test_nargs_with_converter(self):
        """Test nargs with converter to transform result."""

        class Parser(argclass.Parser):
            unique_tags: frozenset = argclass.Argument(  # type: ignore[type-arg]
                nargs="+",
                type=str,
                converter=frozenset,
            )

        parser = Parser()
        parser.parse_args(["--unique-tags", "a", "b", "a", "c"])
        assert parser.unique_tags == frozenset(["a", "b", "c"])

    def test_nargs_in_group(self):
        """Test nargs argument in a group."""

        class TargetsGroup(argclass.Group):
            hosts: List[str] = argclass.Argument(nargs="+")
            ports: List[int] = argclass.Argument(
                nargs="*", type=int, default=[]
            )

        class Parser(argclass.Parser):
            targets = TargetsGroup()

        parser = Parser()
        parser.parse_args(
            ["--targets-hosts", "h1", "h2", "--targets-ports", "80", "443"]
        )
        assert parser.targets.hosts == ["h1", "h2"]
        assert parser.targets.ports == [80, 443]

    def test_nargs_positional_with_multiple_values(self):
        """Test positional argument with nargs."""

        class Parser(argclass.Parser):
            files: List[str] = argclass.Argument("files", nargs="+")

        parser = Parser()
        parser.parse_args(["file1.txt", "file2.txt", "file3.txt"])
        assert parser.files == ["file1.txt", "file2.txt", "file3.txt"]

    def test_nargs_enum_variations(self):
        """Test nargs using Nargs enum."""

        class Parser(argclass.Parser):
            items: List[str] = argclass.Argument(
                nargs=argclass.Nargs.ONE_OR_MORE,
            )
            optional_items: List[str] = argclass.Argument(
                nargs=argclass.Nargs.ZERO_OR_MORE,
                default=[],
            )

        parser = Parser()
        parser.parse_args(["--items", "a", "b"])
        assert parser.items == ["a", "b"]
        assert parser.optional_items == []

    def test_database_backup_tables_nargs(self):
        """Test database backup tables with nargs='*'."""
        parser = DatabaseCommand()
        parser.parse_args(["backup", "--tables", "users", "orders", "products"])
        assert parser.backup.tables == ["users", "orders", "products"]  # type: ignore[union-attr]

    def test_user_create_groups_nargs(self):
        """Test user create groups with nargs='*'."""
        parser = UserCommand()
        parser.parse_args(
            [
                "create",
                "--username=test",
                "--email=test@test.com",
                "--groups",
                "admin",
                "users",
            ]
        )
        assert parser.create.groups == ["admin", "users"]  # type: ignore[union-attr]


class TestTypeConversion:
    """Test type conversion functionality."""

    def test_path_type_conversion(self):
        """Test Path type conversion."""
        parser = DatabaseCommand()
        parser.parse_args(["backup", "--output=/var/backups/db.sql"])
        assert parser.backup.output == Path("/var/backups/db.sql")  # type: ignore[union-attr]
        assert isinstance(parser.backup.output, Path)  # type: ignore[union-attr]

    def test_custom_type_converter(self):
        """Test custom type converter function."""

        def parse_size(s: str) -> int:
            """Parse size string like '1K', '2M', '3G'."""
            multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3}
            if s[-1] in multipliers:
                return int(s[:-1]) * multipliers[s[-1]]
            return int(s)

        class Parser(argclass.Parser):
            buffer_size: int = argclass.Argument(type=parse_size)

        parser = Parser()
        parser.parse_args(["--buffer-size=4K"])
        assert parser.buffer_size == 4096

        parser = Parser()
        parser.parse_args(["--buffer-size=2M"])
        assert parser.buffer_size == 2 * 1024 * 1024

    def test_enum_argument(self):
        """Test enum argument type."""
        parser = DeployCommand()
        parser.parse_args(
            ["--api-key=key", "--environment=PROD", "--targets=target1"]
        )
        assert parser.environment == DeployEnvironment.PROD

    def test_enum_argument_invalid_value(self):
        """Test enum argument with invalid value."""
        parser = DeployCommand()
        with pytest.raises(SystemExit):
            parser.parse_args(
                ["--api-key=key", "--environment=INVALID", "--targets=target1"]
            )

    def test_bool_conversion_from_cli(self):
        """Test bool conversion from CLI flags."""
        parser = ServerCommand()
        parser.parse_args(["start", "--daemon"])
        assert parser.start.daemon is True  # type: ignore[union-attr]

    def test_bool_default_true_toggled(self):
        """Test bool with default=True toggled by flag."""
        parser = ServerCommand()
        parser.parse_args(["restart", "--graceful"])
        # Toggled from True
        assert parser.restart.graceful is False  # type: ignore[union-attr]

    def test_int_type_conversion(self):
        """Test int type conversion."""
        parser = ServerCommand()
        parser.parse_args(["start", "--workers=16"])
        assert parser.start.workers == 16  # type: ignore[union-attr]
        assert isinstance(parser.start.workers, int)  # type: ignore[union-attr]

    def test_float_type_conversion(self):
        """Test float type conversion."""
        parser = DatabaseCommand()
        parser.parse_args(["--retry-delay=0.5", "backup"])
        assert parser.retry.delay == 0.5
        assert isinstance(parser.retry.delay, float)


class TestConfigFiles:
    """Test configuration file handling."""

    def test_ini_config_loading(self, tmp_path: Path):
        """Test INI config file loading."""
        config_file = tmp_path / "config.ini"
        config_file.write_text(
            "[DEFAULT]\nhost = ini.example.com\nport = 9000\n"
        )

        class Parser(argclass.Parser):
            host: str = "localhost"
            port: int = 8080

        parser = Parser(config_files=[config_file])
        parser.parse_args([])

        assert parser.host == "ini.example.com"
        assert parser.port == 9000

    def test_json_config_loading(self, tmp_path: Path):
        """Test JSON config file loading."""
        config_file = tmp_path / "config.json"
        config_file.write_text('{"host": "json.example.com", "port": 9000}')

        class Parser(argclass.Parser):
            host: str = "localhost"
            port: int = 8080

        parser = Parser(
            config_files=[config_file],
            config_parser_class=argclass.JSONDefaultsParser,
        )
        parser.parse_args([])

        assert parser.host == "json.example.com"
        assert parser.port == 9000

    def test_multiple_config_files_merge(self, tmp_path: Path):
        """Test multiple config files are merged correctly."""
        global_config = tmp_path / "global.ini"
        global_config.write_text(
            "[DEFAULT]\nhost = global.com\nport = 8080\ndebug = false\n"
        )

        user_config = tmp_path / "user.ini"
        user_config.write_text("[DEFAULT]\nhost = user.com\ndebug = true\n")

        class Parser(argclass.Parser):
            host: str = "localhost"
            port: int = 80
            debug: bool = False

        parser = Parser(config_files=[global_config, user_config])
        parser.parse_args([])

        # Later config overrides earlier
        assert parser.host == "user.com"
        # Preserved from global config
        assert parser.port == 8080
        # Overridden by user config
        assert parser.debug is True

    def test_config_with_groups(self, tmp_path: Path):
        """Test config file with group sections."""
        config_file = tmp_path / "config.ini"
        config_file.write_text(
            "[connection]\nhost = config.example.com\nport = 9000\nssl = true\n"
        )

        class Parser(argclass.Parser):
            connection: ConnectionGroup = ConnectionGroup()

        parser = Parser(config_files=[config_file])
        parser.parse_args([])

        assert parser.connection.host == "config.example.com"
        assert parser.connection.port == 9000
        assert parser.connection.ssl is True

    def test_missing_config_file_ignored(self, tmp_path: Path):
        """Test missing config files are gracefully ignored."""
        existing_config = tmp_path / "existing.ini"
        existing_config.write_text("[DEFAULT]\nhost = existing.com\n")

        missing_config = tmp_path / "missing.ini"

        class Parser(argclass.Parser):
            host: str = "default"

        parser = Parser(config_files=[existing_config, missing_config])
        parser.parse_args([])

        assert parser.host == "existing.com"

    def test_config_action_runtime_loading(self, tmp_path: Path):
        """Test Config action for runtime config loading."""
        config_file = tmp_path / "runtime.ini"
        config_file.write_text("[app]\nname = myapp\nversion = 1.0\n")

        class Parser(argclass.Parser):
            config = argclass.Config(config_class=argclass.INIConfig)

        parser = Parser()
        parser.parse_args(["--config", str(config_file)])

        assert parser.config["app"]["name"] == "myapp"
        assert parser.config["app"]["version"] == "1.0"

    def test_json_config_action_runtime_loading(self, tmp_path: Path):
        """Test JSON Config action for runtime loading."""
        config_file = tmp_path / "runtime.json"
        config_file.write_text('{"app": {"name": "myapp", "version": "2.0"}}')

        class Parser(argclass.Parser):
            config = argclass.Config(config_class=argclass.JSONConfig)

        parser = Parser()
        parser.parse_args(["--config", str(config_file)])

        assert parser.config["app"]["name"] == "myapp"
        assert parser.config["app"]["version"] == "2.0"

    def test_config_list_values(self, tmp_path: Path):
        """Test config file with list values."""
        config_file = tmp_path / "config.ini"
        config_file.write_text("[DEFAULT]\nports = [80, 443, 8080]\n")

        class Parser(argclass.Parser):
            ports: List[int] = argclass.Argument(
                nargs="+",
                type=int,
                default=[],
            )

        parser = Parser(config_files=[config_file])
        parser.parse_args([])

        assert parser.ports == [80, 443, 8080]

    def test_json_config_with_nested_groups(self, tmp_path: Path):
        """Test JSON config with nested group data."""
        config_file = tmp_path / "config.json"
        config_file.write_text(
            json.dumps(
                {
                    "connection": {
                        "host": "json.example.com",
                        "port": 9000,
                    },
                    "retry": {
                        "max_retries": 10,
                        "delay": 2.5,
                    },
                }
            )
        )

        class Parser(argclass.Parser):
            connection: ConnectionGroup = ConnectionGroup()
            retry: RetryGroup = RetryGroup()

        parser = Parser(
            config_files=[config_file],
            config_parser_class=argclass.JSONDefaultsParser,
        )
        parser.parse_args([])

        assert parser.connection.host == "json.example.com"
        assert parser.connection.port == 9000
        assert parser.retry.max_retries == 10
        assert parser.retry.delay == 2.5


class TestEnvironmentVariables:
    """Test environment variable handling."""

    def test_auto_env_var_prefix(self, monkeypatch: pytest.MonkeyPatch):
        """Test auto_env_var_prefix for automatic env var binding."""
        monkeypatch.setenv("MYAPP_HOST", "env.example.com")
        monkeypatch.setenv("MYAPP_PORT", "9000")

        class Parser(argclass.Parser):
            host: str = "localhost"
            port: int = 8080

        parser = Parser(auto_env_var_prefix="MYAPP_")
        parser.parse_args([])

        assert parser.host == "env.example.com"
        assert parser.port == 9000

    def test_explicit_env_var_parameter(self, monkeypatch: pytest.MonkeyPatch):
        """Test explicit env_var parameter on argument."""
        monkeypatch.setenv("CUSTOM_HOST", "custom.example.com")

        class Parser(argclass.Parser):
            host: str = argclass.Argument(
                env_var="CUSTOM_HOST",
                default="localhost",
            )

        parser = Parser()
        parser.parse_args([])

        assert parser.host == "custom.example.com"

    def test_env_var_in_groups(self, monkeypatch: pytest.MonkeyPatch):
        """Test env var with group prefix."""
        monkeypatch.setenv("APP_CONNECTION_HOST", "group-env.example.com")
        monkeypatch.setenv("APP_CONNECTION_PORT", "9999")

        class Parser(argclass.Parser):
            connection: ConnectionGroup = ConnectionGroup()

        parser = Parser(auto_env_var_prefix="APP_")
        parser.parse_args([])

        assert parser.connection.host == "group-env.example.com"
        assert parser.connection.port == 9999

    def test_env_var_bool_conversion(self, monkeypatch: pytest.MonkeyPatch):
        """Test bool conversion from env var."""
        monkeypatch.setenv("APP_DEBUG", "true")
        monkeypatch.setenv("APP_VERBOSE", "yes")
        monkeypatch.setenv("APP_QUIET", "1")

        class Parser(argclass.Parser):
            debug: bool = False
            verbose: bool = False
            quiet: bool = False

        parser = Parser(auto_env_var_prefix="APP_")
        parser.parse_args([])

        assert parser.debug is True
        assert parser.verbose is True
        assert parser.quiet is True

    def test_env_var_list_syntax(self, monkeypatch: pytest.MonkeyPatch):
        """Test list value from env var."""
        monkeypatch.setenv("APP_PORTS", "[80, 443, 8080]")

        class Parser(argclass.Parser):
            ports: List[int] = argclass.Argument(
                nargs="+",
                type=int,
                converter=list,
            )

        parser = Parser(auto_env_var_prefix="APP_")
        parser.parse_args([])

        assert parser.ports == [80, 443, 8080]

    def test_cli_overrides_env_var(self, monkeypatch: pytest.MonkeyPatch):
        """Test CLI argument overrides env var."""
        monkeypatch.setenv("APP_HOST", "env.example.com")

        class Parser(argclass.Parser):
            host: str = "localhost"

        parser = Parser(auto_env_var_prefix="APP_")
        parser.parse_args(["--host=cli.example.com"])

        assert parser.host == "cli.example.com"

    def test_sanitize_env_all_vars(self, monkeypatch: pytest.MonkeyPatch):
        """Test sanitize_env removes all bound env vars."""
        with patch("os.environ", new={}):
            import os

            os.environ["APP_HOST"] = "example.com"
            os.environ["APP_PORT"] = "9000"

            class Parser(argclass.Parser):
                host: str = "localhost"
                port: int = 8080

            parser = Parser(auto_env_var_prefix="APP_")
            parser.parse_args([])

            parser.sanitize_env()

            assert "APP_HOST" not in os.environ
            assert "APP_PORT" not in os.environ

    def test_parse_args_sanitize_secrets(self, monkeypatch: pytest.MonkeyPatch):
        """Test parse_args with sanitize_secrets=True."""
        with patch("os.environ", new={}):
            import os

            os.environ["APP_SECRET"] = "secret-value"
            os.environ["APP_PUBLIC"] = "public-value"

            class Parser(argclass.Parser):
                secret: str = argclass.Secret()
                public: str = "default"

            parser = Parser(auto_env_var_prefix="APP_")
            parser.parse_args([], sanitize_secrets=True)

            assert "APP_SECRET" not in os.environ
            assert os.environ.get("APP_PUBLIC") == "public-value"


class TestErrorHandling:
    """Test error handling and validation."""

    def test_invalid_subcommand(self):
        """Test invalid subcommand raises error."""
        parser = InfractlParser()
        with pytest.raises(SystemExit):
            parser.parse_args(["invalid-command"])

    def test_missing_required_argument(self):
        """Test missing required argument raises error."""
        parser = UserCommand()
        with pytest.raises(SystemExit):
            parser.parse_args(["create"])  # Missing --username and --email

    def test_invalid_type_conversion(self):
        """Test invalid type conversion raises error."""
        parser = ServerCommand()
        with pytest.raises(SystemExit):
            parser.parse_args(["--connection-port=not-a-number", "start"])

    def test_invalid_enum_value(self):
        """Test invalid enum value raises error."""
        parser = DeployCommand()
        with pytest.raises(SystemExit):
            parser.parse_args(
                ["--api-key=key", "--environment=INVALID", "target1"]
            )

    def test_invalid_choice(self):
        """Test invalid choice value raises error."""
        parser = ServerCommand()
        with pytest.raises(SystemExit):
            parser.parse_args(["--logging-format=invalid", "start"])

    def test_accessing_unparsed_attribute(self):
        """Test accessing unparsed required attribute raises error."""
        parser = InfractlParser()
        with pytest.raises(AttributeError):
            _ = parser.debug  # Not parsed yet

    def test_missing_nested_subcommand(self):
        """Test server command without nested subcommand still works."""
        parser = ServerCommand()
        # server without nested command should work (all nested are Optional)
        parser.parse_args([])
        assert parser.current_subparser is None

    def test_type_conversion_error_with_details(self):
        """Test type conversion error includes useful details."""

        def bad_converter(x: str) -> str:
            raise ValueError(f"cannot parse: {x}")

        class Parser(argclass.Parser):
            value: str = argclass.Argument(converter=bad_converter)

        parser = Parser()
        with pytest.raises(argclass.TypeConversionError) as exc_info:
            parser.parse_args(["--value", "test"])

        error_msg = str(exc_info.value)
        assert "value" in error_msg
        assert "test" in error_msg


class TestActionsVariations:
    """Test various action types."""

    def test_store_true_action(self):
        """Test store_true action."""
        parser = ServerCommand()
        parser.parse_args(["start", "--daemon"])
        assert parser.start.daemon is True  # type: ignore[union-attr]

    def test_store_false_action(self):
        """Test bool with default=True acts as store_false."""
        parser = ServerCommand()
        # graceful defaults to True, flag toggles it
        parser.parse_args(["restart", "--graceful"])
        assert parser.restart.graceful is False  # type: ignore[union-attr]

    def test_store_const_action(self):
        """Test store_const action."""

        class Parser(argclass.Parser):
            value = argclass.Argument(
                "--set-value",
                action=argclass.Actions.STORE_CONST,
                const=42,
                default=0,
            )

        parser = Parser()
        parser.parse_args([])
        assert parser.value == 0

        parser = Parser()
        parser.parse_args(["--set-value"])
        assert parser.value == 42

    def test_append_action(self):
        """Test append action accumulates values across multiple flags."""

        class Parser(argclass.Parser):
            # Don't use List[str] type - APPEND handles the list creation
            items = argclass.Argument(
                "-i",
                "--items",
                action=argclass.Actions.APPEND,
            )

        parser = Parser()
        parser.parse_args(["-i", "a", "-i", "b", "--items", "c"])
        assert parser.items == ["a", "b", "c"]

    def test_count_action(self):
        """Test count action."""

        class Parser(argclass.Parser):
            verbosity: int = argclass.Argument(
                "-v",
                "--verbose",
                action=argclass.Actions.COUNT,
                default=0,
            )

        parser = Parser()
        parser.parse_args(["-vvv"])
        assert parser.verbosity == 3

        parser = Parser()
        parser.parse_args(["-v", "-v", "--verbose"])
        assert parser.verbosity == 3


class TestComplexScenarios:
    """Test complex real-world scenarios."""

    def test_full_deploy_command(self, monkeypatch: pytest.MonkeyPatch):
        """Test a complete deploy command with all options."""
        monkeypatch.setenv("INFRACTL_API_KEY", "prod-api-key")

        parser = InfractlParser()
        parser.parse_args(
            [
                "--debug",
                "deploy",
                "--connection-host=deploy.example.com",
                "--connection-port=443",
                "--connection-ssl",
                "--logging-level=info",
                "--logging-format=json",
                "--retry-max-retries=5",
                "--retry-delay=2.0",
                "--environment=PROD",
                "--parallel=8",
                "--targets",
                "server1",
                "server2",
                "server3",
            ]
        )

        deploy = parser.deploy
        assert parser.debug is True
        assert deploy.connection.host == "deploy.example.com"  # type: ignore[union-attr]
        assert deploy.connection.port == 443  # type: ignore[union-attr]
        assert deploy.connection.ssl is True  # type: ignore[union-attr]
        assert deploy.logging.level == logging.INFO  # type: ignore[union-attr]
        assert deploy.logging.format == "json"  # type: ignore[union-attr]
        assert deploy.retry.max_retries == 5  # type: ignore[union-attr]
        assert deploy.retry.delay == 2.0  # type: ignore[union-attr]
        assert deploy.environment == DeployEnvironment.PROD  # type: ignore[union-attr]
        assert deploy.parallel == 8  # type: ignore[union-attr]
        assert deploy.targets == ["server1", "server2", "server3"]  # type: ignore[union-attr]
        assert str.__str__(deploy.api_key) == "prod-api-key"  # type: ignore[union-attr]

    def test_database_backup_with_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Test database backup with config file and env override."""
        config_file = tmp_path / "db.ini"
        config_file.write_text(
            "[connection]\n"
            "host = config-db.example.com\n"
            "port = 5432\n"
            "[retry]\n"
            "max_retries = 5\n"
        )

        monkeypatch.setenv("DB_CONNECTION_HOST", "env-db.example.com")

        class TestDatabaseCommand(argclass.Parser):
            connection: ConnectionGroup = ConnectionGroup()
            retry: RetryGroup = RetryGroup()
            backup: Optional[DatabaseBackupCommand] = DatabaseBackupCommand()

        parser = TestDatabaseCommand(
            config_files=[config_file], auto_env_var_prefix="DB_"
        )
        parser.parse_args(
            [
                "backup",
                "--output=/backups/db.sql",
                "--tables",
                "users",
                "orders",
            ]
        )

        # Env overrides config
        assert parser.connection.host == "env-db.example.com"
        # Config value preserved
        assert parser.connection.port == 5432
        assert parser.retry.max_retries == 5
        # CLI values
        assert parser.backup.output == Path("/backups/db.sql")  # type: ignore[union-attr]
        assert parser.backup.tables == ["users", "orders"]  # type: ignore[union-attr]

    def test_subparser_dispatch_pattern(self):
        """Test common subparser dispatch pattern."""
        from functools import singledispatch

        @singledispatch
        def handle_command(cmd: Any) -> str:
            raise NotImplementedError(f"Unknown command: {type(cmd)}")

        @handle_command.register(ServerStartCommand)
        def handle_start(cmd: ServerStartCommand) -> str:
            return f"Starting with {cmd.workers} workers"

        @handle_command.register(ServerStopCommand)
        def handle_stop(cmd: ServerStopCommand) -> str:
            return f"Stopping (force={cmd.force})"

        @handle_command.register(type(None))
        def handle_none(_: None) -> str:
            return "No command"

        parser = ServerCommand()

        parser.parse_args(["start", "--workers=8"])
        result = handle_command(parser.current_subparser)
        assert result == "Starting with 8 workers"

        parser = ServerCommand()
        parser.parse_args(["stop", "--force"])
        result = handle_command(parser.current_subparser)
        assert result == "Stopping (force=True)"

        parser = ServerCommand()
        parser.parse_args([])
        result = handle_command(parser.current_subparser)
        assert result == "No command"

    def test_parser_inheritance(self):
        """Test parser class inheritance."""

        class BaseCommand(argclass.Parser):
            verbose: bool = False
            output: str = "stdout"

        class ExtendedCommand(BaseCommand):
            format: str = argclass.Argument(
                choices=["json", "text"],
                default="text",
            )

        parser = ExtendedCommand()
        parser.parse_args(["--verbose", "--output=file.txt", "--format=json"])

        assert parser.verbose is True
        assert parser.output == "file.txt"
        assert parser.format == "json"

    def test_group_inheritance(self):
        """Test group class inheritance."""

        class BaseConnectionGroup(argclass.Group):
            host: str = "localhost"
            port: int = 80

        class SecureConnectionGroup(BaseConnectionGroup):
            ssl: bool = True
            cert_file: Optional[Path] = None

        class Parser(argclass.Parser):
            connection: SecureConnectionGroup = SecureConnectionGroup()

        parser = Parser()
        parser.parse_args(
            [
                "--connection-host=secure.example.com",
                "--connection-port=443",
                "--connection-cert-file=/etc/ssl/cert.pem",
            ]
        )

        assert parser.connection.host == "secure.example.com"
        assert parser.connection.port == 443
        assert parser.connection.ssl is True
        assert parser.connection.cert_file == Path("/etc/ssl/cert.pem")
