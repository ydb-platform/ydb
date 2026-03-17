"""Exception classes for argclass with rich context for debugging."""

from typing import Any, Optional, Tuple


class ArgclassError(Exception):
    """Base exception for all argclass errors.

    Provides structured context for debugging configuration issues.
    All argclass exceptions inherit from this class.

    Attributes:
        message: The error message describing what went wrong.
        field_name: The name of the field that caused the error, if applicable.
        hint: A suggestion for how to fix the error, if available.

    Example::

        try:
            # ... argclass operation that may fail
            pass
        except ArgclassError as e:
            print(f"Error: {e}")
            if e.field_name:
                print(f"Field: {e.field_name}")
            if e.hint:
                print(f"Hint: {e.hint}")
    """

    def __init__(
        self,
        message: str,
        *,
        field_name: Optional[str] = None,
        hint: Optional[str] = None,
    ):
        self.message = message
        self.field_name = field_name
        self.hint = hint
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        parts = []
        if self.field_name:
            parts.append(f"[{self.field_name}]")
        parts.append(self.message)
        if self.hint:
            parts.append(f"Hint: {self.hint}")
        return " ".join(parts)


class ArgumentDefinitionError(ArgclassError):
    """Error in argument definition or registration with argparse.

    Raised when an argument cannot be added to the parser due to
    invalid configuration (conflicting options, invalid types, etc.).

    Attributes:
        aliases: The conflicting aliases, e.g., ``("-v", "--verbose")``.
        kwargs: The kwargs passed to argparse when the error occurred.

    Example::

        # This exception is typically raised during parser construction
        # when argument definitions conflict:
        try:
            class Parser(argclass.Parser):
                verbose: bool = argclass.Argument("-h")  # conflicts with --help
        except ArgumentDefinitionError as e:
            print(f"Conflict: {e.aliases}")
    """

    def __init__(
        self,
        message: str,
        *,
        field_name: Optional[str] = None,
        aliases: Optional[Tuple[str, ...]] = None,
        kwargs: Optional[dict] = None,
        hint: Optional[str] = None,
    ):
        self.aliases = aliases
        self.kwargs = kwargs
        super().__init__(message, field_name=field_name, hint=hint)

    def _format_message(self) -> str:
        parts = []
        if self.field_name:
            parts.append(f"[{self.field_name}]")
        parts.append(self.message)
        if self.aliases:
            parts.append(f"(aliases: {', '.join(self.aliases)})")
        if self.hint:
            parts.append(f"Hint: {self.hint}")
        return " ".join(parts)


class TypeConversionError(ArgclassError):
    """Error during type conversion of argument values.

    Raised when a value cannot be converted to the expected type,
    either by the type function or a custom converter.

    Attributes:
        value: The original value that failed conversion.
        target_type: The type that the value was being converted to.

    Example::

        try:
            # When a custom converter fails:
            def strict_port(value: str) -> int:
                port = int(value)
                if not (1 <= port <= 65535):
                    raise TypeConversionError(
                        "Port must be between 1 and 65535",
                        field_name="port",
                        value=value,
                        target_type=int,
                        hint="Use a valid port number (1-65535)",
                    )
                return port
        except TypeConversionError as e:
            print(f"Invalid value: {e.value!r} for type {e.target_type}")
    """

    def __init__(
        self,
        message: str,
        *,
        field_name: Optional[str] = None,
        value: Any = None,
        target_type: Optional[type] = None,
        hint: Optional[str] = None,
    ):
        self.value = value
        self.target_type = target_type
        super().__init__(message, field_name=field_name, hint=hint)

    def _format_message(self) -> str:
        parts = []
        if self.field_name:
            parts.append(f"[{self.field_name}]")
        parts.append(self.message)
        if self.value is not None:
            parts.append(f"(got {self.value!r})")
        if self.target_type is not None:
            type_name = getattr(
                self.target_type, "__name__", str(self.target_type)
            )
            parts.append(f"expected type: {type_name}")
        if self.hint:
            parts.append(f"Hint: {self.hint}")
        return " ".join(parts)


class ConfigurationError(ArgclassError):
    """Error loading or parsing configuration files.

    Raised when a configuration file cannot be parsed or contains
    invalid values for the expected argument types.

    Attributes:
        file_path: Path to the configuration file that caused the error.
        section: The config section (e.g., INI section) where error occurred.

    Example::

        try:
            parser = Parser(
                config_files=["config.ini"],
                config_parser_class=argclass.INIDefaultsParser,
            )
        except ConfigurationError as e:
            print(f"Config error in {e.file_path}")
            if e.section:
                print(f"Section: [{e.section}]")
    """

    def __init__(
        self,
        message: str,
        *,
        field_name: Optional[str] = None,
        file_path: Optional[str] = None,
        section: Optional[str] = None,
        hint: Optional[str] = None,
    ):
        self.file_path = file_path
        self.section = section
        super().__init__(message, field_name=field_name, hint=hint)

    def _format_message(self) -> str:
        parts = []
        if self.file_path:
            location = self.file_path
            if self.section:
                location = f"{self.file_path}:[{self.section}]"
            parts.append(f"({location})")
        if self.field_name:
            parts.append(f"[{self.field_name}]")
        parts.append(self.message)
        if self.hint:
            parts.append(f"Hint: {self.hint}")
        return " ".join(parts)


class EnumValueError(ArgclassError):
    """Error with enum argument value or default.

    Raised when an enum default or value is not a valid member
    of the specified enum class.

    Attributes:
        enum_class: The enum class that was expected.
        valid_values: Tuple of valid enum member names.

    Example::

        from enum import Enum

        class Color(Enum):
            RED = "red"
            GREEN = "green"

        try:
            class Parser(argclass.Parser):
                # "YELLOW" is not a valid Color member
                color: Color = argclass.EnumArgument(Color, default="YELLOW")
        except EnumValueError as e:
            print(f"Invalid value for {e.enum_class.__name__}")
            print(f"Valid options: {', '.join(e.valid_values)}")
    """

    def __init__(
        self,
        message: str,
        *,
        field_name: Optional[str] = None,
        enum_class: Optional[type] = None,
        valid_values: Optional[Tuple[str, ...]] = None,
        hint: Optional[str] = None,
    ):
        self.enum_class = enum_class
        self.valid_values = valid_values
        super().__init__(message, field_name=field_name, hint=hint)

    def _format_message(self) -> str:
        parts = []
        if self.field_name:
            parts.append(f"[{self.field_name}]")
        parts.append(self.message)
        if self.valid_values:
            parts.append(f"Valid values: {', '.join(self.valid_values)}")
        if self.hint:
            parts.append(f"Hint: {self.hint}")
        return " ".join(parts)


class ComplexTypeError(ArgclassError):
    """Error with complex type annotations.

    Raised when a type annotation is too complex to be automatically
    handled and requires explicit converter specification.

    Attributes:
        typespec: The type annotation that could not be handled.

    Example::

        try:
            class Parser(argclass.Parser):
                # Union types (other than T | None) are not supported
                value: str | int  # Raises ComplexTypeError
        except ComplexTypeError as e:
            print(f"Cannot handle type: {e.typespec}")
            print("Provide an explicit converter with type=...")
    """

    def __init__(
        self,
        message: str,
        *,
        field_name: Optional[str] = None,
        typespec: Any = None,
        hint: Optional[str] = None,
    ):
        self.typespec = typespec
        super().__init__(message, field_name=field_name, hint=hint)

    def _format_message(self) -> str:
        parts = []
        if self.field_name:
            parts.append(f"[{self.field_name}]")
        parts.append(self.message)
        if self.typespec is not None:
            parts.append(f"(type: {self.typespec!r})")
        if self.hint:
            parts.append(f"Hint: {self.hint}")
        return " ".join(parts)
