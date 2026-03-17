import itertools
from typing import List, cast

from proto_schema_parser import ast


class Generator:
    def generate(self, file: ast.File) -> str:
        lines = []

        # Syntax
        if file.syntax:
            lines.append(f'syntax = "{file.syntax}";')

        # File Elements
        for element in file.file_elements:
            if isinstance(element, ast.Package):
                lines.append(f"package {element.name};")
            elif isinstance(element, ast.Import):
                modifier = ""
                if element.weak:
                    modifier = "weak "
                elif element.public:
                    modifier = "public "
                lines.append(f'import {modifier}"{element.name}";')
            elif isinstance(element, ast.Option):
                if isinstance(element.value, bool):
                    if element.value:
                        lines.append(f"option {element.name} = true;")
                    else:
                        lines.append(f"option {element.name} = false;")
                else:
                    lines.append(f'option {element.name} = "{element.value}";')
            elif isinstance(element, ast.Message):
                lines.append(self._generate_message(element))
            elif isinstance(element, ast.Enum):
                lines.append(self._generate_enum(element))
            elif isinstance(element, ast.Extension):
                lines.append(self._generate_extension(element))
            elif isinstance(element, ast.Service):
                lines.append(self._generate_service(element))
            elif isinstance(element, ast.Comment):
                lines.append(self._generate_comment(element))

        return "\n".join(lines)

    def _generate_message(self, message: ast.Message, indent_level: int = 0) -> str:
        lines = [f"{'  ' * indent_level}message {message.name} {{"]
        for element in message.elements:
            if isinstance(element, ast.Field):
                lines.append(self._generate_field(element, indent_level + 1))
            elif isinstance(element, ast.MapField):
                lines.append(self._generate_map_field(element, indent_level + 1))
            elif isinstance(element, ast.Group):
                lines.append(self._generate_group(element, indent_level + 1))
            elif isinstance(element, ast.OneOf):
                lines.append(self._generate_one_of(element, indent_level + 1))
            elif isinstance(element, ast.ExtensionRange):
                lines.append(self._generate_extension_range(element, indent_level + 1))
            elif isinstance(element, ast.Reserved):
                lines.append(self._generate_reserved(element, indent_level + 1))
            elif isinstance(element, ast.Option):
                lines.append(self._generate_option(element, indent_level + 1))
            elif isinstance(element, ast.Message):  # Nested Message
                lines.append(self._generate_message(element, indent_level + 1))
            elif isinstance(element, ast.Enum):  # Enum
                lines.append(self._generate_enum(element, indent_level + 1))
            elif isinstance(element, ast.Extension):  # Extension
                lines.append(self._generate_extension(element, indent_level + 1))
            elif isinstance(element, ast.Comment):
                lines.append(self._generate_comment(element, indent_level + 1))

        lines.append(f"{'  ' * indent_level}}}")
        return "\n".join(lines)

    def _generate_service(self, service: ast.Service, indent_level: int = 0) -> str:
        lines = [f"{'  ' * indent_level}service {service.name} {{"]
        for element in service.elements:
            if isinstance(element, ast.Method):
                lines.append(self._generate_method(element, indent_level + 1))
            elif isinstance(element, ast.Option):
                lines.append(self._generate_option(element, indent_level + 1))
            elif isinstance(element, ast.Comment):
                lines.append(self._generate_comment(element, indent_level + 1))
        lines.append(f"{'  ' * indent_level}}}")
        return "\n".join(lines)

    def _generate_comment(self, comment: ast.Comment, indent_level: int = 0) -> str:
        lines = [f"{'  ' * indent_level}{comment.text}"]
        return "\n".join(lines)

    def _generate_method(self, method: ast.Method, indent_level: int = 0) -> str:
        input_type_str = self._generate_message_type(method.input_type)
        output_type_str = self._generate_message_type(method.output_type)

        has_options = any(
            isinstance(element, ast.Option) for element in method.elements
        )
        lines = [
            f"{'  ' * indent_level}rpc {method.name} ({input_type_str}) returns ({output_type_str}){' {' if has_options else ';'}"
        ]
        if has_options:
            for element in method.elements:
                if isinstance(element, ast.Option):
                    lines.append(self._generate_option(element, indent_level + 1))
                elif isinstance(element, ast.Comment):
                    lines.append(self._generate_comment(element, indent_level + 1))
            lines.append(f"{'  ' * indent_level}}}")
        return "\n".join(lines)

    def _generate_message_type(self, message_type: ast.MessageType) -> str:
        stream = "stream " if message_type.stream else ""
        return f"{stream}{message_type.type}"

    def _generate_field(self, field: ast.Field, indent_level: int = 0) -> str:
        cardinality = ""
        if field.cardinality:
            cardinality = f"{field.cardinality.value.lower()} "

        options = ""
        print(field.options)
        if field.options:
            options = " ["
            options += ", ".join(
                f"{opt.name} = {self._generate_scalar(cast(ast.ScalarValue, opt.value))}"
                for opt in field.options
            )
            options += "]"

        return f"{'  ' * indent_level}{cardinality}{field.type} {field.name} = {field.number}{options};"

    def _generate_map_field(
        self, map_field: ast.MapField, indent_level: int = 0
    ) -> str:
        return f"{'  ' * indent_level}map<{map_field.key_type}, {map_field.value_type}> {map_field.name} = {map_field.number};"

    def _generate_group(self, group: ast.Group, indent_level: int = 0) -> str:
        lines = [f"{'  ' * indent_level}group {group.name} = {group.number} {{"]
        for element in group.elements:
            if isinstance(element, ast.Field):
                lines.append(self._generate_field(element, indent_level + 1))
            elif isinstance(element, ast.Option):
                lines.append(self._generate_option(element, indent_level + 1))
        lines.append(f"{'  ' * indent_level}}}")
        return "\n".join(lines)

    def _generate_one_of(self, one_of: ast.OneOf, indent_level: int = 0) -> str:
        lines = [f"{'  ' * indent_level}oneof {one_of.name} {{"]
        for element in one_of.elements:
            if isinstance(element, ast.Field):
                lines.append(self._generate_field(element, indent_level + 1))
            elif isinstance(element, ast.Option):
                lines.append(self._generate_option(element, indent_level + 1))
            elif isinstance(element, ast.Comment):
                lines.append(self._generate_comment(element, indent_level + 1))
        lines.append(f"{'  ' * indent_level}}}")
        return "\n".join(lines)

    def _generate_option(self, option: ast.Option, indent_level: int = 0) -> str:
        value = self._generate_option_value(option.value, indent_level)
        return f"{'  ' * indent_level}option {option.name} = {value};"

    def _generate_extension(
        self, extension: ast.Extension, indent_level: int = 0
    ) -> str:
        lines = [f"{'  ' * indent_level}extend {extension.typeName} {{"]
        for element in extension.elements:
            if isinstance(element, ast.Field):
                lines.append(self._generate_field(element, indent_level + 1))
            elif isinstance(element, ast.Group):
                lines.append(self._generate_group(element, indent_level + 1))
            elif isinstance(element, ast.Comment):
                lines.append(self._generate_comment(element, indent_level + 1))
        lines.append(f"{'  ' * indent_level}}}")
        return "\n".join(lines)

    def _generate_enum(self, enum: ast.Enum, indent_level: int = 0) -> str:
        lines = [f"{'  ' * indent_level}enum {enum.name} {{"]
        for element in enum.elements:
            if isinstance(element, ast.EnumValue):
                lines.append(
                    f"{'  ' * (indent_level + 1)}{element.name} = {element.number};"
                )
            elif isinstance(element, ast.EnumReserved):
                lines.append(self._generate_enum_reserved(element, indent_level + 1))
            elif isinstance(element, ast.Option):
                lines.append(self._generate_option(element, indent_level + 1))
            elif isinstance(element, ast.Comment):
                lines.append(self._generate_comment(element, indent_level + 1))
        lines.append(f"{'  ' * indent_level}}}")
        return "\n".join(lines)

    def _generate_enum_reserved(
        self, reserved: ast.EnumReserved, indent_level: int = 0
    ) -> str:
        ranges = ", ".join(reserved.ranges)
        names = ", ".join(reserved.names)
        return f"{'  ' * indent_level}reserved {ranges}, {names};"

    def _generate_extension_range(
        self, extension_range: ast.ExtensionRange, indent_level: int = 0
    ) -> str:
        ranges = ", ".join(extension_range.ranges)
        return f"{'  ' * indent_level}extensions {ranges};"

    def _generate_reserved(self, reserved: ast.Reserved, indent_level: int = 0) -> str:
        reserved_values = ", ".join(itertools.chain(reserved.ranges, reserved.names))
        return f"{'  ' * indent_level}reserved {reserved_values};"

    def _generate_message_literal(
        self, message_literal: ast.MessageLiteral, indent_level: int
    ) -> str:
        """Generate nested message literal with consistent indentation."""
        lines = [f"{'  ' * indent_level}{{"]
        for i, field in enumerate(message_literal.fields):
            field_line = self._generate_message_literal_field(field, indent_level + 1)
            lines.append(field_line)
            if i < len(message_literal.fields) - 1:
                lines[-1] += ","  # Add a comma except for the last field
        lines.append(f"{'  ' * indent_level}}}")
        if len(lines) == 2:
            lines = ["{}"]  # Don't include a linebreak if there are no fields
        return "\n".join(lines)

    def _generate_message_literal_field(
        self, field: ast.MessageLiteralField, indent_level: int
    ) -> str:
        """Generate individual field with correct indentation."""
        value = self._generate_option_value(field.value, indent_level)
        return f"{'  ' * indent_level}{field.name}: {value}"

    def _generate_option_value(self, value: ast.MessageValue, indent_level: int) -> str:
        """Generate the correct value for an option."""
        if isinstance(value, ast.MessageLiteral):
            # strip() to remove leading/trailing whitespace since it's appended on the
            # same line as the field name.
            return self._generate_message_literal(value, indent_level).strip()
        elif isinstance(value, list):
            # No + 1 for indent_level since the list literal is on the same line as the field name.
            return self._generate_list_literal(value, indent_level)
        else:
            return self._generate_scalar(value)

    def _generate_list_literal(
        self, elements: List[ast.MessageValue], indent_level: int
    ) -> str:
        """Generate a list literal."""
        message_values = [
            self._generate_option_value(element, indent_level) for element in elements
        ]
        return f"[{', '.join(message_values)}]"

    def _generate_scalar(self, scalar: ast.ScalarValue) -> str:
        """Generate scalar values like strings, numbers, or identifiers."""
        if isinstance(scalar, str):
            return f'"{self._escape_string(scalar)}"'
        elif isinstance(scalar, ast.Identifier):
            return scalar.name
        elif isinstance(scalar, bool):
            return "true" if scalar else "false"
        return str(scalar)

    def _escape_string(self, value: str) -> str:
        """
        Escapes a string so it is safe to use as a scalar string value
        in a Protobuf definition.
        """
        # Define the escape mappings for special characters
        escape_map = {
            "\\": "\\\\",  # Backslash
            '"': '\\"',  # Double quote
            "\n": "\\n",  # Newline
            "\t": "\\t",  # Tab
            "\r": "\\r",  # Carriage return
        }

        # Use the escape map to replace special characters
        return "".join(escape_map.get(char, char) for char in value)

    @staticmethod
    def _indent(line: str, indent_level: int = 0) -> str:
        return f"{'  ' * indent_level}{line}"
