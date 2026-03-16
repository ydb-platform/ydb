"""Tests for edge cases in positional arguments, required flag handling,
and exceptions."""

import pytest
from typing import Optional, List

import argclass
from argclass.store import TypedArgument
from argclass.utils import _is_union_type


class TestUnionTypeDetection:
    """Test PEP 604 union type detection for coverage."""

    def test_pep604_union_detected(self):
        """PEP 604 union (int | str) should be detected as union type."""
        assert _is_union_type(int | str) is True

    def test_pep604_optional_detected(self):
        """PEP 604 optional (int | None) should be detected as union type."""
        assert _is_union_type(int | None) is True

    def test_non_union_not_detected(self):
        """Non-union types should not be detected as union."""
        assert _is_union_type(int) is False
        assert _is_union_type(str) is False
        assert _is_union_type(list) is False


class TestHasDefaultProperty:
    """Test TypedArgument.has_default property edge cases."""

    def test_has_default_with_none(self):
        """default=None should return has_default=False."""
        arg = TypedArgument(default=None)
        assert arg.has_default is False

    def test_has_default_with_ellipsis(self):
        """default=... should return has_default=False."""
        arg = TypedArgument(default=...)
        assert arg.has_default is False

    def test_has_default_with_zero(self):
        """default=0 should return has_default=True (falsy but valid)."""
        arg = TypedArgument(default=0)
        assert arg.has_default is True

    def test_has_default_with_empty_string(self):
        """default='' should return has_default=True (falsy but valid)."""
        arg = TypedArgument(default="")
        assert arg.has_default is True

    def test_has_default_with_false(self):
        """default=False should return has_default=True."""
        arg = TypedArgument(default=False)
        assert arg.has_default is True

    def test_has_default_with_empty_list(self):
        """default=[] should return has_default=True."""
        arg = TypedArgument(default=[])
        assert arg.has_default is True


class TestIsPositionalProperty:
    """Test TypedArgument.is_positional property edge cases."""

    def test_is_positional_empty_aliases(self):
        """Empty aliases should return is_positional=True."""
        arg = TypedArgument(aliases=frozenset())
        assert arg.is_positional is True

    def test_is_positional_with_dash_prefix(self):
        """Alias with -- prefix should return is_positional=False."""
        arg = TypedArgument(aliases=["--flag"])
        assert arg.is_positional is False

    def test_is_positional_single_dash(self):
        """Single-dash alias like -v should return is_positional=False."""
        arg = TypedArgument(aliases=["-v"])
        assert arg.is_positional is False

    def test_is_positional_no_dash(self):
        """Alias without dash should return is_positional=True."""
        arg = TypedArgument(aliases=["name"])
        assert arg.is_positional is True


class TestPositionalArgumentWithRequired:
    """Test positional arguments with required flag
    (argparse doesn't support this)."""

    def test_positional_with_required_true_raises(self):
        """Positional args with required=True should raise
        ArgumentDefinitionError."""

        class Parser(argclass.Parser):
            name: str = argclass.Argument("name", required=True)

        with pytest.raises(argclass.ArgumentDefinitionError) as exc_info:
            Parser().parse_args(["test"])

        assert "name" in str(exc_info.value)
        assert "positional" in str(exc_info.value).lower()

    def test_positional_with_required_false_raises(self):
        """Positional args with required=False should also raise error."""

        class Parser(argclass.Parser):
            name: str = argclass.Argument("name", required=False)

        with pytest.raises(argclass.ArgumentDefinitionError) as exc_info:
            Parser().parse_args(["test"])

        assert "name" in str(exc_info.value)


class TestPositionalArgumentWithNargs:
    """Test positional arguments with various nargs values."""

    def test_positional_nargs_question_without_value(self):
        """Positional with nargs='?' uses None when not provided."""

        class Parser(argclass.Parser):
            name: Optional[str] = argclass.Argument("name", nargs="?")

        parser = Parser()
        parser.parse_args([])
        assert parser.name is None

    def test_positional_nargs_question_with_value(self):
        """Positional with nargs='?' uses provided value."""

        class Parser(argclass.Parser):
            name: Optional[str] = argclass.Argument("name", nargs="?")

        parser = Parser()
        parser.parse_args(["hello"])
        assert parser.name == "hello"

    def test_positional_nargs_question_with_default(self):
        """Positional with nargs='?' and default uses default
        when not provided."""

        class Parser(argclass.Parser):
            name: str = argclass.Argument("name", nargs="?", default="world")

        parser = Parser()
        parser.parse_args([])
        assert parser.name == "world"

    def test_positional_nargs_star_without_values(self):
        """Positional with nargs='*' returns empty list when not provided."""

        class Parser(argclass.Parser):
            files: List[str] = argclass.Argument("files", nargs="*")

        parser = Parser()
        parser.parse_args([])
        assert parser.files == []

    def test_positional_nargs_star_with_values(self):
        """Positional with nargs='*' collects all values."""

        class Parser(argclass.Parser):
            files: List[str] = argclass.Argument("files", nargs="*")

        parser = Parser()
        parser.parse_args(["a.txt", "b.txt", "c.txt"])
        assert parser.files == ["a.txt", "b.txt", "c.txt"]

    def test_positional_nargs_plus_requires_value(self):
        """Positional with nargs='+' requires at least one value."""

        class Parser(argclass.Parser):
            files: List[str] = argclass.Argument("files", nargs="+")

        parser = Parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_positional_nargs_plus_with_values(self):
        """Positional with nargs='+' collects all values."""

        class Parser(argclass.Parser):
            files: List[str] = argclass.Argument("files", nargs="+")

        parser = Parser()
        parser.parse_args(["a.txt", "b.txt"])
        assert parser.files == ["a.txt", "b.txt"]

    def test_positional_nargs_fixed(self):
        """Positional with nargs=2 requires exactly 2 values."""

        class Parser(argclass.Parser):
            coords: List[int] = argclass.Argument("coords", type=int, nargs=2)

        parser = Parser()
        parser.parse_args(["10", "20"])
        assert parser.coords == [10, 20]

    def test_positional_nargs_fixed_wrong_count(self):
        """Positional with nargs=2 fails with wrong count."""

        class Parser(argclass.Parser):
            coords: List[int] = argclass.Argument("coords", type=int, nargs=2)

        parser = Parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["10"])


class TestPositionalWithDefault:
    """Test positional arguments with default values."""

    def test_positional_with_default_is_optional(self):
        """Positional with default can be omitted."""

        class Parser(argclass.Parser):
            name: str = argclass.Argument("name", nargs="?", default="world")

        parser = Parser()
        parser.parse_args([])
        assert parser.name == "world"

    def test_positional_with_default_can_be_overridden(self):
        """Positional with default can still accept values."""

        class Parser(argclass.Parser):
            name: str = argclass.Argument("name", nargs="?", default="world")

        parser = Parser()
        parser.parse_args(["universe"])
        assert parser.name == "universe"


class TestRequiredFlagAutoRemoval:
    """Test automatic removal of required flag when default is provided."""

    def test_required_removed_with_config_default(self, tmp_path):
        """required=True is auto-removed when config provides value."""
        config = tmp_path / "config.ini"
        config.write_text("[DEFAULT]\nvalue = 42\n")

        class Parser(argclass.Parser):
            value: int = argclass.Argument(required=True)

        parser = Parser(config_files=[str(config)])
        parser.parse_args([])
        assert parser.value == 42

    def test_required_removed_with_env_var(self, monkeypatch):
        """required=True is auto-removed when env var provides value."""
        monkeypatch.setenv("TEST_VALUE", "123")

        class Parser(argclass.Parser):
            value: int = argclass.Argument(env_var="TEST_VALUE", required=True)

        parser = Parser()
        parser.parse_args([])
        assert parser.value == 123

    def test_required_removed_with_zero_default(self):
        """required=True is auto-removed when default=0."""

        class Parser(argclass.Parser):
            count: int = argclass.Argument(default=0, required=True)

        parser = Parser()
        parser.parse_args([])
        assert parser.count == 0

    def test_required_removed_with_empty_string_default(self):
        """required=True is auto-removed when default=''."""

        class Parser(argclass.Parser):
            name: str = argclass.Argument(default="", required=True)

        parser = Parser()
        parser.parse_args([])
        assert parser.name == ""


class TestEnvVarInteraction:
    """Test environment variable and required flag interaction."""

    def test_env_var_empty_string_counts_as_value(self, monkeypatch):
        """Empty string env var should count as having a value."""
        monkeypatch.setenv("TEST_VAL", "")

        class Parser(argclass.Parser):
            val: str = argclass.Argument(env_var="TEST_VAL", required=True)

        parser = Parser()
        parser.parse_args([])
        assert parser.val == ""

    def test_env_var_zero_for_int(self, monkeypatch):
        """Env var '0' should parse to int 0."""
        monkeypatch.setenv("TEST_COUNT", "0")

        class Parser(argclass.Parser):
            count: int = argclass.Argument(env_var="TEST_COUNT", required=True)

        parser = Parser()
        parser.parse_args([])
        assert parser.count == 0


class TestGroupRequiredRemoval:
    """Test required flag removal in argument groups."""

    def test_group_member_required_removed_with_default(self):
        """Group member required=True is removed when default provided."""

        class ServerGroup(argclass.Group):
            port: int = argclass.Argument(required=True)

        class Parser(argclass.Parser):
            server: ServerGroup = ServerGroup(defaults={"port": 8080})

        parser = Parser()
        parser.parse_args([])
        assert parser.server.port == 8080

    def test_group_member_required_removed_with_config(self, tmp_path):
        """Group member required=True is removed when config provides value."""
        config = tmp_path / "config.ini"
        config.write_text("[server]\nport = 9000\n")

        class ServerGroup(argclass.Group):
            port: int = argclass.Argument(required=True)

        class Parser(argclass.Parser):
            server: ServerGroup = ServerGroup()

        parser = Parser(config_files=[str(config)])
        parser.parse_args([])
        assert parser.server.port == 9000


class TestOptionalNargsQuestionWithConst:
    """Test optional arguments with nargs='?' and const (3-state behavior).

    With optional args and nargs='?', there are THREE states:
    1. Flag not present -> uses default
    2. Flag present without value -> uses const
    3. Flag present with value -> uses the value
    """

    def test_flag_not_present_uses_default(self):
        """When flag is not provided, default value is used."""

        class Parser(argclass.Parser):
            output: Optional[str] = argclass.Argument(
                "--output",
                nargs="?",
                const="stdout",
                default="file.txt",
            )

        parser = Parser()
        parser.parse_args([])
        assert parser.output == "file.txt"

    def test_flag_present_without_value_uses_const(self):
        """When flag is present without value, const is used."""

        class Parser(argclass.Parser):
            output: Optional[str] = argclass.Argument(
                "--output",
                nargs="?",
                const="stdout",
                default="file.txt",
            )

        parser = Parser()
        parser.parse_args(["--output"])
        assert parser.output == "stdout"

    def test_flag_present_with_value_uses_value(self):
        """When flag is present with value, that value is used."""

        class Parser(argclass.Parser):
            output: Optional[str] = argclass.Argument(
                "--output",
                nargs="?",
                const="stdout",
                default="file.txt",
            )

        parser = Parser()
        parser.parse_args(["--output", "custom.txt"])
        assert parser.output == "custom.txt"

    def test_flag_with_equals_syntax(self):
        """Flag with --flag=value syntax works correctly."""

        class Parser(argclass.Parser):
            output: Optional[str] = argclass.Argument(
                "--output",
                nargs="?",
                const="stdout",
                default="file.txt",
            )

        parser = Parser()
        parser.parse_args(["--output=custom.txt"])
        assert parser.output == "custom.txt"

    def test_short_flag_without_value_uses_const(self):
        """Short flag without value uses const."""

        class Parser(argclass.Parser):
            output: Optional[str] = argclass.Argument(
                "-o",
                "--output",
                nargs="?",
                const="stdout",
                default="file.txt",
            )

        parser = Parser()
        parser.parse_args(["-o"])
        assert parser.output == "stdout"

    def test_flag_followed_by_another_flag_uses_const(self):
        """Flag followed by another flag uses const for first flag."""

        class Parser(argclass.Parser):
            output: Optional[str] = argclass.Argument(
                "--output",
                nargs="?",
                const="stdout",
                default="file.txt",
            )
            verbose: bool = False

        parser = Parser()
        parser.parse_args(["--output", "--verbose"])
        assert parser.output == "stdout"
        assert parser.verbose is True

    def test_nargs_question_with_type_converter(self):
        """Type converter is applied to the value."""

        class Parser(argclass.Parser):
            count: int = argclass.Argument(
                "--count",
                nargs="?",
                type=int,
                const=10,
                default=0,
            )

        parser = Parser()
        parser.parse_args(["--count", "42"])
        assert parser.count == 42
        assert isinstance(parser.count, int)

    def test_nargs_question_const_used_when_flag_alone(self):
        """Const value is used when flag is present without value."""

        class Parser(argclass.Parser):
            count: int = argclass.Argument(
                "--count",
                nargs="?",
                type=int,
                const=10,
                default=0,
            )

        parser = Parser()
        parser.parse_args(["--count"])
        assert parser.count == 10


class TestOptionalNargsWithEnvVar:
    """Test nargs with environment variables."""

    def test_nargs_question_env_var_as_scalar(self, monkeypatch):
        """Env var for nargs='?' should be treated as scalar value."""
        monkeypatch.setenv("TEST_OUTPUT", "from_env")

        class Parser(argclass.Parser):
            output: Optional[str] = argclass.Argument(
                "--output",
                nargs="?",
                env_var="TEST_OUTPUT",
                const="stdout",
                default="file.txt",
            )

        parser = Parser()
        parser.parse_args([])
        assert parser.output == "from_env"

    def test_nargs_question_cli_overrides_env_var(self, monkeypatch):
        """CLI value should override env var."""
        monkeypatch.setenv("TEST_OUTPUT", "from_env")

        class Parser(argclass.Parser):
            output: Optional[str] = argclass.Argument(
                "--output",
                nargs="?",
                env_var="TEST_OUTPUT",
                const="stdout",
                default="file.txt",
            )

        parser = Parser()
        parser.parse_args(["--output", "from_cli"])
        assert parser.output == "from_cli"

    def test_nargs_star_env_var_as_list(self, monkeypatch):
        """Env var for nargs='*' should be parsed as list."""
        monkeypatch.setenv("TEST_FILES", '["a.txt", "b.txt"]')

        class Parser(argclass.Parser):
            files: List[str] = argclass.Argument(
                "--files",
                nargs="*",
                env_var="TEST_FILES",
                default=[],
            )

        parser = Parser()
        parser.parse_args([])
        assert parser.files == ["a.txt", "b.txt"]

    def test_nargs_plus_env_var_as_list(self, monkeypatch):
        """Env var for nargs='+' should be parsed as list."""
        monkeypatch.setenv("TEST_ITEMS", '["x", "y", "z"]')

        class Parser(argclass.Parser):
            items: List[str] = argclass.Argument(
                "--items",
                nargs="+",
                env_var="TEST_ITEMS",
            )

        parser = Parser()
        parser.parse_args([])
        assert parser.items == ["x", "y", "z"]

    def test_nargs_int_env_var_as_list(self, monkeypatch):
        """Env var for nargs=2 should be parsed as list."""
        monkeypatch.setenv("TEST_COORDS", "[10, 20]")

        class Parser(argclass.Parser):
            coords: List[int] = argclass.Argument(
                "--coords",
                nargs=2,
                type=int,
                env_var="TEST_COORDS",
            )

        parser = Parser()
        parser.parse_args([])
        assert parser.coords == [10, 20]

    def test_nargs_star_env_var_empty_list(self, monkeypatch):
        """Env var with empty list for nargs='*' should work."""
        monkeypatch.setenv("TEST_FILES", "[]")

        class Parser(argclass.Parser):
            files: List[str] = argclass.Argument(
                "--files",
                nargs="*",
                env_var="TEST_FILES",
                default=["default.txt"],
            )

        parser = Parser()
        parser.parse_args([])
        assert parser.files == []


class TestNargsOneVsNone:
    """Test behavior difference between nargs=1 and nargs=None (default)."""

    def test_nargs_none_returns_scalar(self):
        """Without nargs, single value is returned as scalar."""

        class Parser(argclass.Parser):
            value: str = argclass.Argument("--value")

        parser = Parser()
        parser.parse_args(["--value", "test"])
        assert parser.value == "test"
        assert isinstance(parser.value, str)

    def test_nargs_one_returns_list(self):
        """With nargs=1, single value is returned as list."""

        class Parser(argclass.Parser):
            value: List[str] = argclass.Argument("--value", nargs=1)

        parser = Parser()
        parser.parse_args(["--value", "test"])
        assert parser.value == ["test"]
        assert isinstance(parser.value, list)

    def test_nargs_one_with_default_scalar(self):
        """nargs=1 with scalar default - default used as-is
        when not provided."""

        class Parser(argclass.Parser):
            value: List[str] = argclass.Argument(
                "--value", nargs=1, default=["default"]
            )

        parser = Parser()
        parser.parse_args([])
        assert parser.value == ["default"]


class TestNargsWithConfig:
    """Test nargs with config file values."""

    def test_nargs_question_config_scalar(self, tmp_path):
        """Config value for nargs='?' should be scalar."""
        config = tmp_path / "config.ini"
        config.write_text("[DEFAULT]\noutput = from_config\n")

        class Parser(argclass.Parser):
            output: Optional[str] = argclass.Argument(
                "--output",
                nargs="?",
                const="stdout",
                default="file.txt",
            )

        parser = Parser(config_files=[str(config)])
        parser.parse_args([])
        assert parser.output == "from_config"

    def test_nargs_star_config_list(self, tmp_path):
        """Config value for nargs='*' should be parsed as list."""
        config = tmp_path / "config.ini"
        config.write_text('[DEFAULT]\nfiles = ["a.txt", "b.txt"]\n')

        class Parser(argclass.Parser):
            files: List[str] = argclass.Argument(
                "--files", nargs="*", default=[]
            )

        parser = Parser(config_files=[str(config)])
        parser.parse_args([])
        assert parser.files == ["a.txt", "b.txt"]

    def test_nargs_plus_config_list(self, tmp_path):
        """Config value for nargs='+' should be parsed as list."""
        config = tmp_path / "config.ini"
        config.write_text('[DEFAULT]\nitems = ["x", "y"]\n')

        class Parser(argclass.Parser):
            items: List[str] = argclass.Argument("--items", nargs="+")

        parser = Parser(config_files=[str(config)])
        parser.parse_args([])
        assert parser.items == ["x", "y"]

    def test_nargs_cli_overrides_config(self, tmp_path):
        """CLI values should override config file values."""
        config = tmp_path / "config.ini"
        config.write_text('[DEFAULT]\nfiles = ["from_config.txt"]\n')

        class Parser(argclass.Parser):
            files: List[str] = argclass.Argument(
                "--files", nargs="*", default=[]
            )

        parser = Parser(config_files=[str(config)])
        parser.parse_args(["--files", "from_cli.txt"])
        assert parser.files == ["from_cli.txt"]
