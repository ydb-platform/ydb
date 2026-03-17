"""Tests for error message clarity and field name visibility.

These tests ensure that when users make mistakes in their argclass definitions,
the error messages clearly identify WHICH field is problematic and provide
helpful hints for fixing the issue.
"""

import json
import re
from enum import Enum, IntEnum
from typing import Union

import pytest

import argclass


class TestArgumentDefinitionErrors:
    """Test that argument definition errors show field names clearly."""

    def test_duplicate_aliases_shows_field_name(self):
        """When two fields use the same alias, error shows which field."""
        with pytest.raises(argclass.ArgumentDefinitionError) as exc_info:

            class Parser(argclass.Parser):
                verbose: bool = argclass.Argument(
                    "-v", "--verbose", default=False
                )
                version: str = argclass.Argument(
                    "-v", "--version", default="1.0"
                )

            Parser().parse_args([])

        error_msg = str(exc_info.value)
        # Error should identify the conflicting field
        assert "version" in error_msg or "verbose" in error_msg
        assert "-v" in error_msg

    def test_duplicate_short_alias_shows_field_name(self):
        """Duplicate short aliases should show which field caused conflict."""
        with pytest.raises(argclass.ArgumentDefinitionError) as exc_info:

            class Parser(argclass.Parser):
                debug: bool = argclass.Argument("-d", default=False)
                delete: bool = argclass.Argument("-d", default=False)

            Parser().parse_args([])

        error_msg = str(exc_info.value)
        assert "-d" in error_msg
        # Should show field name in brackets
        assert "[" in error_msg and "]" in error_msg

    def test_incompatible_nargs_and_action_shows_field(self):
        """Incompatible nargs+action combo should show field name."""
        with pytest.raises(argclass.ArgumentDefinitionError) as exc_info:

            class Parser(argclass.Parser):
                # store_true cannot have nargs
                flag: bool = argclass.Argument(
                    action=argclass.Actions.STORE_TRUE,
                    nargs="+",
                    default=False,
                )

            Parser().parse_args([])

        error_msg = str(exc_info.value)
        assert "[flag]" in error_msg
        assert "nargs" in error_msg.lower()


class TestEnumValueErrors:
    """Test that enum errors show field context and valid values."""

    def test_invalid_enum_default_string_shows_valid_values(self):
        """Invalid enum string default should list valid options."""

        class Color(Enum):
            RED = "red"
            GREEN = "green"
            BLUE = "blue"

        with pytest.raises(argclass.EnumValueError) as exc_info:
            argclass.EnumArgument(Color, default="purple")

        error_msg = str(exc_info.value)
        assert "purple" in error_msg
        assert "RED" in error_msg
        assert "GREEN" in error_msg
        assert "BLUE" in error_msg

    def test_invalid_enum_default_type_shows_expected(self):
        """Wrong type for enum default should show what's expected."""

        class Priority(IntEnum):
            LOW = 1
            MEDIUM = 2
            HIGH = 3

        with pytest.raises(argclass.EnumValueError) as exc_info:
            argclass.EnumArgument(Priority, default=42)  # type: ignore[call-overload]

        error_msg = str(exc_info.value)
        assert "int" in error_msg
        assert "Priority" in error_msg
        assert "LOW" in error_msg or "Valid values" in error_msg

    def test_wrong_enum_class_default_shows_mismatch(self):
        """Using member from different enum should show the mismatch."""

        class LogLevel(IntEnum):
            DEBUG = 10
            INFO = 20

        class Priority(IntEnum):
            LOW = 1
            HIGH = 2

        with pytest.raises(argclass.EnumValueError) as exc_info:
            argclass.EnumArgument(LogLevel, default=Priority.LOW)

        error_msg = str(exc_info.value)
        assert "LogLevel" in error_msg
        assert "Priority" in error_msg or "int" in error_msg


class TestComplexTypeErrors:
    """Test that complex type errors show the problematic type."""

    def test_union_multiple_types_shows_typespec(self):
        """Union with multiple non-None types should show the type."""
        from argclass.utils import unwrap_optional

        with pytest.raises(argclass.ComplexTypeError) as exc_info:
            unwrap_optional(Union[str, int, None])

        error_msg = str(exc_info.value)
        assert "Union" in error_msg or "str" in error_msg
        assert "Hint" in error_msg

    def test_union_two_types_shows_hint(self):
        """Union error should hint at using Argument with converter."""
        from argclass.utils import unwrap_optional

        with pytest.raises(argclass.ComplexTypeError) as exc_info:
            unwrap_optional(Union[str, int])

        error_msg = str(exc_info.value)
        assert "converter" in error_msg.lower() or "Argument" in error_msg


class TestTypeConversionErrors:
    """Test that converter errors show field name and value."""

    def test_converter_failure_shows_field_name(self):
        """Failed converter should show which field failed."""

        def strict_int(value):
            if not value.isdigit():
                raise ValueError(f"'{value}' is not a valid integer")
            return int(value)

        class Parser(argclass.Parser):
            count: int = argclass.Argument(converter=strict_int)

        parser = Parser()
        with pytest.raises(argclass.TypeConversionError) as exc_info:
            parser.parse_args(["--count", "abc"])

        error_msg = str(exc_info.value)
        assert "[count]" in error_msg
        assert "abc" in error_msg

    def test_converter_failure_shows_value(self):
        """Failed converter should show the problematic value."""

        def parse_json(value):
            return json.loads(value)

        class Parser(argclass.Parser):
            config: dict = argclass.Argument(converter=parse_json, default="{}")

        parser = Parser()
        with pytest.raises(argclass.TypeConversionError) as exc_info:
            parser.parse_args(["--config", "not valid json"])

        error_msg = str(exc_info.value)
        assert "not valid json" in error_msg
        assert "config" in error_msg

    def test_converter_division_error_shows_context(self):
        """Math errors in converter should show field and value."""

        def inverse(value):
            return 1 / int(value)

        class Parser(argclass.Parser):
            ratio: float = argclass.Argument(converter=inverse)

        parser = Parser()
        with pytest.raises(argclass.TypeConversionError) as exc_info:
            parser.parse_args(["--ratio", "0"])

        error_msg = str(exc_info.value)
        assert "[ratio]" in error_msg
        assert "division" in error_msg.lower() or "zero" in error_msg.lower()


class TestBoolFieldErrors:
    """Test that bool field errors are clear about requirements."""

    def test_bool_without_default_shows_field_name(self):
        """Bool without default should show which field needs fixing."""
        with pytest.raises(TypeError, match="verbose"):

            class Parser(argclass.Parser):
                verbose: bool  # Missing default

    def test_bool_invalid_default_shows_value(self):
        """Bool with invalid default should show the bad value."""
        with pytest.raises(TypeError, match="yes"):

            class Parser(argclass.Parser):
                flag: bool = "yes"  # type: ignore[assignment]

    def test_bool_error_suggests_optional(self):
        """Bool error should mention Optional[bool] as alternative."""
        with pytest.raises(TypeError) as exc_info:

            class Parser(argclass.Parser):
                enabled: bool

        error_msg = str(exc_info.value)
        assert "Optional[bool]" in error_msg or "tri-state" in error_msg


class TestConfigurationErrors:
    """Test that config file errors show field and file context."""

    def test_config_wrong_type_for_list_shows_field(self, tmp_path):
        """Config string for list field should show field name."""
        config = tmp_path / "config.ini"
        config.write_text("[DEFAULT]\nitems = not_a_list\n")

        class Parser(argclass.Parser):
            items: list[str] = []

        with pytest.raises(argclass.UnexpectedConfigValue) as exc_info:
            parser = Parser(config_files=[config])
            parser.parse_args([])

        error_msg = str(exc_info.value)
        assert "items" in error_msg
        assert "SEQUENCE" in error_msg

    def test_config_wrong_type_shows_expected_and_actual(self, tmp_path):
        """Config type error should show expected vs actual type."""
        config = tmp_path / "config.json"
        config.write_text('{"count": "not_an_int"}')

        class Parser(argclass.Parser):
            count: list[int] = []

        with pytest.raises(argclass.UnexpectedConfigValue) as exc_info:
            parser = Parser(
                config_files=[config],
                config_parser_class=argclass.JSONDefaultsParser,
            )
            parser.parse_args([])

        error_msg = str(exc_info.value)
        assert "count" in error_msg
        assert "SEQUENCE" in error_msg
        assert "str" in error_msg


class TestErrorMessageFormat:
    """Test that error messages follow consistent format."""

    def test_field_name_in_brackets(self):
        """Field names should appear in [brackets] for easy scanning."""

        def bad_converter(x):
            raise ValueError("always fails")

        class Parser(argclass.Parser):
            value: str = argclass.Argument(converter=bad_converter)

        parser = Parser()
        with pytest.raises(argclass.TypeConversionError) as exc_info:
            parser.parse_args(["--value", "test"])

        error_msg = str(exc_info.value)
        # Field name should be in brackets
        assert re.search(r"\[value\]", error_msg)

    def test_hint_provides_actionable_guidance(self):
        """Hints should tell user what to do, not just what went wrong."""

        def bad_converter(x):
            raise ValueError("cannot convert")

        class Parser(argclass.Parser):
            data: str = argclass.Argument(converter=bad_converter)

        parser = Parser()
        with pytest.raises(argclass.TypeConversionError) as exc_info:
            parser.parse_args(["--data", "x"])

        error_msg = str(exc_info.value)
        assert "Hint" in error_msg


class TestMultipleFieldErrors:
    """Test behavior when multiple fields could have errors."""

    def test_first_error_identifies_specific_field(self):
        """With multiple potential errors, identify the specific one."""

        class Color(Enum):
            RED = 1
            BLUE = 2

        class Size(Enum):
            SMALL = 1
            LARGE = 2

        # Only one has invalid default
        with pytest.raises(argclass.EnumValueError) as exc_info:
            argclass.EnumArgument(Color, default="INVALID")

        error_msg = str(exc_info.value)
        assert "Color" in error_msg
        assert "INVALID" in error_msg


class TestInheritanceErrors:
    """Test error handling with class inheritance."""

    def test_child_class_error_doesnt_blame_parent(self):
        """Errors in child class should identify child field."""

        class BaseParser(argclass.Parser):
            debug: bool = False

        with pytest.raises(argclass.ArgumentDefinitionError) as exc_info:

            class ChildParser(BaseParser):
                # Conflicts with inherited debug's auto-generated --debug
                other: str = argclass.Argument("--debug", default="")

            ChildParser().parse_args([])

        error_msg = str(exc_info.value)
        assert "--debug" in error_msg


class TestGroupErrors:
    """Test error handling within argument groups."""

    def test_group_field_error_shows_full_context(self):
        """Errors in group fields should show group context."""

        def bad_converter(x):
            raise ValueError("group converter failed")

        class ServerGroup(argclass.Group):
            port: int = argclass.Argument(converter=bad_converter)

        class Parser(argclass.Parser):
            server: ServerGroup = ServerGroup()

        parser = Parser()
        with pytest.raises(argclass.TypeConversionError) as exc_info:
            parser.parse_args(["--server-port", "8080"])

        error_msg = str(exc_info.value)
        # Should show the prefixed name
        assert "server_port" in error_msg or "port" in error_msg
