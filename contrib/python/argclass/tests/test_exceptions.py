"""Synthetic tests for exception formatting to achieve full coverage."""

from argclass.exceptions import (
    ArgclassError,
    ArgumentDefinitionError,
    TypeConversionError,
    ConfigurationError,
    EnumValueError,
    ComplexTypeError,
)


class TestArgclassErrorFormatting:
    """Test ArgclassError message formatting branches."""

    def test_message_only(self):
        """Message without optional fields."""
        exc = ArgclassError("something went wrong")
        assert str(exc) == "something went wrong"

    def test_message_with_field_name(self):
        """Message with field_name adds brackets."""
        exc = ArgclassError("invalid value", field_name="count")
        assert str(exc) == "[count] invalid value"

    def test_message_with_hint(self):
        """Message with hint adds Hint: prefix."""
        exc = ArgclassError("invalid value", hint="try a number")
        assert str(exc) == "invalid value Hint: try a number"

    def test_message_with_all_fields(self):
        """Message with all optional fields."""
        exc = ArgclassError(
            "invalid value",
            field_name="count",
            hint="try a number",
        )
        assert str(exc) == "[count] invalid value Hint: try a number"

    def test_attributes_accessible(self):
        """Verify attributes are stored correctly."""
        exc = ArgclassError("msg", field_name="f", hint="h")
        assert exc.message == "msg"
        assert exc.field_name == "f"
        assert exc.hint == "h"


class TestArgumentDefinitionErrorFormatting:
    """Test ArgumentDefinitionError message formatting branches."""

    def test_message_only(self):
        """Message without optional fields."""
        exc = ArgumentDefinitionError("conflicting option")
        assert str(exc) == "conflicting option"

    def test_message_with_field_name(self):
        """Message with field_name."""
        exc = ArgumentDefinitionError(
            "conflicting option", field_name="verbose"
        )
        assert str(exc) == "[verbose] conflicting option"

    def test_message_with_aliases(self):
        """Message with aliases tuple."""
        exc = ArgumentDefinitionError(
            "conflicting option",
            aliases=("-v", "--verbose"),
        )
        assert str(exc) == "conflicting option (aliases: -v, --verbose)"

    def test_message_with_hint(self):
        """Message with hint."""
        exc = ArgumentDefinitionError(
            "conflicting option",
            hint="remove the duplicate",
        )
        assert str(exc) == "conflicting option Hint: remove the duplicate"

    def test_message_with_all_fields(self):
        """Message with all optional fields."""
        exc = ArgumentDefinitionError(
            "conflicting option",
            field_name="verbose",
            aliases=("-v", "--verbose"),
            kwargs={"action": "store_true"},
            hint="remove the duplicate",
        )
        assert "[verbose]" in str(exc)
        assert "conflicting option" in str(exc)
        assert "(aliases: -v, --verbose)" in str(exc)
        assert "Hint: remove the duplicate" in str(exc)

    def test_attributes_accessible(self):
        """Verify attributes are stored correctly."""
        exc = ArgumentDefinitionError(
            "msg",
            field_name="f",
            aliases=("-a",),
            kwargs={"k": "v"},
            hint="h",
        )
        assert exc.message == "msg"
        assert exc.field_name == "f"
        assert exc.aliases == ("-a",)
        assert exc.kwargs == {"k": "v"}
        assert exc.hint == "h"


class TestTypeConversionErrorFormatting:
    """Test TypeConversionError message formatting branches."""

    def test_message_only(self):
        """Message without optional fields."""
        exc = TypeConversionError("conversion failed")
        assert str(exc) == "conversion failed"

    def test_message_with_field_name(self):
        """Message with field_name."""
        exc = TypeConversionError("conversion failed", field_name="port")
        assert str(exc) == "[port] conversion failed"

    def test_message_with_value(self):
        """Message with value shows repr."""
        exc = TypeConversionError("conversion failed", value="abc")
        assert str(exc) == "conversion failed (got 'abc')"

    def test_message_with_value_none_not_shown(self):
        """Value=None is not shown in message."""
        exc = TypeConversionError("conversion failed", value=None)
        assert "(got" not in str(exc)

    def test_message_with_target_type_has_name(self):
        """Target type with __name__ attribute."""
        exc = TypeConversionError("conversion failed", target_type=int)
        assert str(exc) == "conversion failed expected type: int"

    def test_message_with_target_type_no_name(self):
        """Target type without __name__ uses str()."""

        # Use an object instance that doesn't have __name__
        class CustomType:
            def __str__(self):
                return "CustomTypeStr"

        target = CustomType()  # Instance doesn't have __name__
        exc = TypeConversionError(
            "conversion failed",
            target_type=target,  # type: ignore[arg-type]
        )
        assert "expected type: CustomTypeStr" in str(exc)

    def test_message_with_hint(self):
        """Message with hint."""
        exc = TypeConversionError("conversion failed", hint="use integer")
        assert str(exc) == "conversion failed Hint: use integer"

    def test_message_with_all_fields(self):
        """Message with all optional fields."""
        exc = TypeConversionError(
            "conversion failed",
            field_name="port",
            value="abc",
            target_type=int,
            hint="use integer",
        )
        assert "[port]" in str(exc)
        assert "conversion failed" in str(exc)
        assert "(got 'abc')" in str(exc)
        assert "expected type: int" in str(exc)
        assert "Hint: use integer" in str(exc)

    def test_attributes_accessible(self):
        """Verify attributes are stored correctly."""
        exc = TypeConversionError(
            "msg",
            field_name="f",
            value="v",
            target_type=str,
            hint="h",
        )
        assert exc.message == "msg"
        assert exc.field_name == "f"
        assert exc.value == "v"
        assert exc.target_type is str
        assert exc.hint == "h"


class TestConfigurationErrorFormatting:
    """Test ConfigurationError message formatting branches."""

    def test_message_only(self):
        """Message without optional fields."""
        exc = ConfigurationError("parse error")
        assert str(exc) == "parse error"

    def test_message_with_file_path(self):
        """Message with file_path only."""
        exc = ConfigurationError("parse error", file_path="/etc/config.ini")
        assert str(exc) == "(/etc/config.ini) parse error"

    def test_message_with_file_path_and_section(self):
        """Message with file_path and section."""
        exc = ConfigurationError(
            "parse error",
            file_path="/etc/config.ini",
            section="database",
        )
        assert str(exc) == "(/etc/config.ini:[database]) parse error"

    def test_message_with_field_name(self):
        """Message with field_name."""
        exc = ConfigurationError("parse error", field_name="port")
        assert str(exc) == "[port] parse error"

    def test_message_with_hint(self):
        """Message with hint."""
        exc = ConfigurationError("parse error", hint="check syntax")
        assert str(exc) == "parse error Hint: check syntax"

    def test_message_with_all_fields(self):
        """Message with all optional fields."""
        exc = ConfigurationError(
            "invalid value",
            file_path="/etc/config.ini",
            section="database",
            field_name="port",
            hint="use integer",
        )
        assert "(/etc/config.ini:[database])" in str(exc)
        assert "[port]" in str(exc)
        assert "invalid value" in str(exc)
        assert "Hint: use integer" in str(exc)

    def test_attributes_accessible(self):
        """Verify attributes are stored correctly."""
        exc = ConfigurationError(
            "msg",
            file_path="f.ini",
            section="s",
            field_name="f",
            hint="h",
        )
        assert exc.message == "msg"
        assert exc.file_path == "f.ini"
        assert exc.section == "s"
        assert exc.field_name == "f"
        assert exc.hint == "h"


class TestEnumValueErrorFormatting:
    """Test EnumValueError message formatting branches."""

    def test_message_only(self):
        """Message without optional fields."""
        exc = EnumValueError("invalid enum")
        assert str(exc) == "invalid enum"

    def test_message_with_field_name(self):
        """Message with field_name."""
        exc = EnumValueError("invalid enum", field_name="color")
        assert str(exc) == "[color] invalid enum"

    def test_message_with_valid_values(self):
        """Message with valid_values tuple."""
        exc = EnumValueError(
            "invalid enum",
            valid_values=("RED", "GREEN", "BLUE"),
        )
        assert str(exc) == "invalid enum Valid values: RED, GREEN, BLUE"

    def test_message_with_hint(self):
        """Message with hint."""
        exc = EnumValueError("invalid enum", hint="use uppercase")
        assert str(exc) == "invalid enum Hint: use uppercase"

    def test_message_with_all_fields(self):
        """Message with all optional fields."""
        from enum import Enum

        class Color(Enum):
            RED = 1

        exc = EnumValueError(
            "invalid enum",
            field_name="color",
            enum_class=Color,
            valid_values=("RED", "GREEN"),
            hint="use uppercase",
        )
        assert "[color]" in str(exc)
        assert "invalid enum" in str(exc)
        assert "Valid values: RED, GREEN" in str(exc)
        assert "Hint: use uppercase" in str(exc)

    def test_attributes_accessible(self):
        """Verify attributes are stored correctly."""
        from enum import Enum

        class Color(Enum):
            RED = 1

        exc = EnumValueError(
            "msg",
            field_name="f",
            enum_class=Color,
            valid_values=("R",),
            hint="h",
        )
        assert exc.message == "msg"
        assert exc.field_name == "f"
        assert exc.enum_class == Color
        assert exc.valid_values == ("R",)
        assert exc.hint == "h"


class TestComplexTypeErrorFormatting:
    """Test ComplexTypeError message formatting branches."""

    def test_message_only(self):
        """Message without optional fields."""
        exc = ComplexTypeError("unsupported type")
        assert str(exc) == "unsupported type"

    def test_message_with_field_name(self):
        """Message with field_name."""
        exc = ComplexTypeError("unsupported type", field_name="value")
        assert str(exc) == "[value] unsupported type"

    def test_message_with_typespec(self):
        """Message with typespec shows repr."""
        exc = ComplexTypeError("unsupported type", typespec="str | int")
        assert str(exc) == "unsupported type (type: 'str | int')"

    def test_message_with_typespec_none_not_shown(self):
        """typespec=None is not shown in message."""
        exc = ComplexTypeError("unsupported type", typespec=None)
        assert "(type:" not in str(exc)

    def test_message_with_hint(self):
        """Message with hint."""
        exc = ComplexTypeError("unsupported type", hint="use converter")
        assert str(exc) == "unsupported type Hint: use converter"

    def test_message_with_all_fields(self):
        """Message with all optional fields."""
        exc = ComplexTypeError(
            "unsupported type",
            field_name="value",
            typespec="str | int",
            hint="use converter",
        )
        assert "[value]" in str(exc)
        assert "unsupported type" in str(exc)
        assert "(type: 'str | int')" in str(exc)
        assert "Hint: use converter" in str(exc)

    def test_attributes_accessible(self):
        """Verify attributes are stored correctly."""
        exc = ComplexTypeError(
            "msg",
            field_name="f",
            typespec="t",
            hint="h",
        )
        assert exc.message == "msg"
        assert exc.field_name == "f"
        assert exc.typespec == "t"
        assert exc.hint == "h"


class TestExceptionInheritance:
    """Test that all exceptions inherit from ArgclassError."""

    def test_argument_definition_error_is_argclass_error(self):
        exc = ArgumentDefinitionError("test")
        assert isinstance(exc, ArgclassError)

    def test_type_conversion_error_is_argclass_error(self):
        exc = TypeConversionError("test")
        assert isinstance(exc, ArgclassError)

    def test_configuration_error_is_argclass_error(self):
        exc = ConfigurationError("test")
        assert isinstance(exc, ArgclassError)

    def test_enum_value_error_is_argclass_error(self):
        exc = EnumValueError("test")
        assert isinstance(exc, ArgclassError)

    def test_complex_type_error_is_argclass_error(self):
        exc = ComplexTypeError("test")
        assert isinstance(exc, ArgclassError)

    def test_can_catch_all_with_argclass_error(self):
        """All exception types can be caught with ArgclassError."""
        exceptions = [
            ArgumentDefinitionError("test"),
            TypeConversionError("test"),
            ConfigurationError("test"),
            EnumValueError("test"),
            ComplexTypeError("test"),
        ]
        for exc in exceptions:
            try:
                raise exc
            except ArgclassError:
                pass  # Should be caught
