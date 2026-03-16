from __future__ import annotations

import locale
from argparse import ArgumentParser, HelpFormatter, Namespace
from operator import attrgetter
from pathlib import Path
from typing import TYPE_CHECKING

from datamodel_code_generator import DataModelType, InputFileType, OpenAPIScope
from datamodel_code_generator.format import DatetimeClassType, Formatter, PythonVersion
from datamodel_code_generator.model.pydantic_v2 import UnionMode
from datamodel_code_generator.parser import LiteralType
from datamodel_code_generator.types import StrictTypes

if TYPE_CHECKING:
    from argparse import Action
    from collections.abc import Iterable

DEFAULT_ENCODING = locale.getpreferredencoding()

namespace = Namespace(no_color=False)


class SortingHelpFormatter(HelpFormatter):
    def _bold_cyan(self, text: str) -> str:  # noqa: PLR6301
        return f"\x1b[36;1m{text}\x1b[0m"

    def add_arguments(self, actions: Iterable[Action]) -> None:
        actions = sorted(actions, key=attrgetter("option_strings"))
        super().add_arguments(actions)

    def start_section(self, heading: str | None) -> None:
        return super().start_section(heading if namespace.no_color or not heading else self._bold_cyan(heading))


arg_parser = ArgumentParser(
    usage="\n  datamodel-codegen [options]",
    description="Generate Python data models from schema definitions or structured data",
    formatter_class=SortingHelpFormatter,
    add_help=False,
)

base_options = arg_parser.add_argument_group("Options")
typing_options = arg_parser.add_argument_group("Typing customization")
field_options = arg_parser.add_argument_group("Field customization")
model_options = arg_parser.add_argument_group("Model customization")
extra_fields_model_options = model_options.add_mutually_exclusive_group()
template_options = arg_parser.add_argument_group("Template customization")
openapi_options = arg_parser.add_argument_group("OpenAPI-only options")
general_options = arg_parser.add_argument_group("General options")

# ======================================================================================
# Base options for input/output
# ======================================================================================
base_options.add_argument(
    "--http-headers",
    nargs="+",
    metavar="HTTP_HEADER",
    help='Set headers in HTTP requests to the remote host. (example: "Authorization: Basic dXNlcjpwYXNz")',
)
base_options.add_argument(
    "--http-query-parameters",
    nargs="+",
    metavar="HTTP_QUERY_PARAMETERS",
    help='Set query parameters in HTTP requests to the remote host. (example: "ref=branch")',
)
base_options.add_argument(
    "--http-ignore-tls",
    help="Disable verification of the remote host's TLS certificate",
    action="store_true",
    default=None,
)
base_options.add_argument(
    "--input",
    help="Input file/directory (default: stdin)",
)
base_options.add_argument(
    "--input-file-type",
    help="Input file type (default: auto)",
    choices=[i.value for i in InputFileType],
)
base_options.add_argument(
    "--output",
    help="Output file (default: stdout)",
)
base_options.add_argument(
    "--output-model-type",
    help="Output model type (default: pydantic.BaseModel)",
    choices=[i.value for i in DataModelType],
)
base_options.add_argument(
    "--url",
    help="Input file URL. `--input` is ignored when `--url` is used",
)

# ======================================================================================
# Customization options for generated models
# ======================================================================================
extra_fields_model_options.add_argument(
    "--allow-extra-fields",
    help="Deprecated: Allow passing extra fields. This flag is deprecated. Use `--extra-fields=allow` instead.",
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--allow-population-by-field-name",
    help="Allow population by field name",
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--class-name",
    help="Set class name of root model",
    default=None,
)
model_options.add_argument(
    "--collapse-root-models",
    action="store_true",
    default=None,
    help="Models generated with a root-type field will be merged into the models using that root-type model",
)
model_options.add_argument(
    "--disable-appending-item-suffix",
    help="Disable appending `Item` suffix to model name in an array",
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--disable-timestamp",
    help="Disable timestamp on file headers",
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--enable-faux-immutability",
    help="Enable faux immutability",
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--enable-version-header",
    help="Enable package version on file headers",
    action="store_true",
    default=None,
)
extra_fields_model_options.add_argument(
    "--extra-fields",
    help="Set the generated models to allow, forbid, or ignore extra fields.",
    choices=["allow", "ignore", "forbid"],
    default=None,
)
model_options.add_argument(
    "--keep-model-order",
    help="Keep generated models' order",
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--keyword-only",
    help="Defined models as keyword only (for example dataclass(kw_only=True)).",
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--frozen-dataclasses",
    help="Generate frozen dataclasses (dataclass(frozen=True)). Only applies to dataclass output.",
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--reuse-model",
    help="Reuse models on the field when a module has the model with the same content",
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--target-python-version",
    help="target python version",
    choices=[v.value for v in PythonVersion],
)
model_options.add_argument(
    "--treat-dot-as-module",
    help="treat dotted module names as modules",
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--use-schema-description",
    help="Use schema description to populate class docstring",
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--use-title-as-name",
    help="use titles as class names of models",
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--use-pendulum",
    help="use pendulum instead of datetime",
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--use-exact-imports",
    help='import exact types instead of modules, for example: "from .foo import Bar" instead of '
    '"from . import foo" with "foo.Bar"',
    action="store_true",
    default=None,
)
model_options.add_argument(
    "--output-datetime-class",
    help="Choose Datetime class between AwareDatetime, NaiveDatetime or datetime. "
    "Each output model has its default mapping (for example pydantic: datetime, dataclass: str, ...)",
    choices=[i.value for i in DatetimeClassType],
    default=None,
)
model_options.add_argument(
    "--parent-scoped-naming",
    help="Set name of models defined inline from the parent model",
    action="store_true",
    default=None,
)

# ======================================================================================
# Typing options for generated models
# ======================================================================================
typing_options.add_argument(
    "--base-class",
    help="Base Class (default: pydantic.BaseModel)",
    type=str,
)
typing_options.add_argument(
    "--enum-field-as-literal",
    help="Parse enum field as literal. "
    "all: all enum field type are Literal. "
    "one: field type is Literal when an enum has only one possible value",
    choices=[lt.value for lt in LiteralType],
    default=None,
)
typing_options.add_argument(
    "--field-constraints",
    help="Use field constraints and not con* annotations",
    action="store_true",
    default=None,
)
typing_options.add_argument(
    "--set-default-enum-member",
    help="Set enum members as default values for enum field",
    action="store_true",
    default=None,
)
typing_options.add_argument(
    "--strict-types",
    help="Use strict types",
    choices=[t.value for t in StrictTypes],
    nargs="+",
)
typing_options.add_argument(
    "--use-annotated",
    help="Use typing.Annotated for Field(). Also, `--field-constraints` option will be enabled.",
    action="store_true",
    default=None,
)
typing_options.add_argument(
    "--use-generic-container-types",
    help="Use generic container types for type hinting (typing.Sequence, typing.Mapping). "
    "If `--use-standard-collections` option is set, then import from collections.abc instead of typing",
    action="store_true",
    default=None,
)
typing_options.add_argument(
    "--use-non-positive-negative-number-constrained-types",
    help="Use the Non{Positive,Negative}{FloatInt} types instead of the corresponding con* constrained types.",
    action="store_true",
    default=None,
)
typing_options.add_argument(
    "--use-one-literal-as-default",
    help="Use one literal as default value for one literal field",
    action="store_true",
    default=None,
)
typing_options.add_argument(
    "--use-standard-collections",
    help="Use standard collections for type hinting (list, dict)",
    action="store_true",
    default=None,
)
typing_options.add_argument(
    "--use-subclass-enum",
    help="Define Enum class as subclass with field type when enum has type (int, float, bytes, str)",
    action="store_true",
    default=None,
)
typing_options.add_argument(
    "--use-union-operator",
    help="Use | operator for Union type (PEP 604).",
    action="store_true",
    default=None,
)
typing_options.add_argument(
    "--use-unique-items-as-set",
    help="define field type as `set` when the field attribute has `uniqueItems`",
    action="store_true",
    default=None,
)
typing_options.add_argument(
    "--disable-future-imports",
    help="Disable __future__ imports",
    action="store_true",
    default=None,
)

# ======================================================================================
# Customization options for generated model fields
# ======================================================================================
field_options.add_argument(
    "--capitalise-enum-members",
    "--capitalize-enum-members",
    help="Capitalize field names on enum",
    action="store_true",
    default=None,
)
field_options.add_argument(
    "--empty-enum-field-name",
    help="Set field name when enum value is empty (default:  `_`)",
    default=None,
)
field_options.add_argument(
    "--field-extra-keys",
    help="Add extra keys to field parameters",
    type=str,
    nargs="+",
)
field_options.add_argument(
    "--field-extra-keys-without-x-prefix",
    help="Add extra keys with `x-` prefix to field parameters. The extra keys are stripped of the `x-` prefix.",
    type=str,
    nargs="+",
)
field_options.add_argument(
    "--field-include-all-keys",
    help="Add all keys to field parameters",
    action="store_true",
    default=None,
)
field_options.add_argument(
    "--force-optional",
    help="Force optional for required fields",
    action="store_true",
    default=None,
)
field_options.add_argument(
    "--original-field-name-delimiter",
    help="Set delimiter to convert to snake case. This option only can be used with --snake-case-field (default: `_` )",
    default=None,
)
field_options.add_argument(
    "--remove-special-field-name-prefix",
    help="Remove field name prefix if it has a special meaning e.g. underscores",
    action="store_true",
    default=None,
)
field_options.add_argument(
    "--snake-case-field",
    help="Change camel-case field name to snake-case",
    action="store_true",
    default=None,
)
field_options.add_argument(
    "--special-field-name-prefix",
    help="Set field name prefix when first character can't be used as Python field name (default:  `field`)",
    default=None,
)
field_options.add_argument(
    "--strip-default-none",
    help="Strip default None on fields",
    action="store_true",
    default=None,
)
field_options.add_argument(
    "--use-default",
    help="Use default value even if a field is required",
    action="store_true",
    default=None,
)
field_options.add_argument(
    "--use-default-kwarg",
    action="store_true",
    help="Use `default=` instead of a positional argument for Fields that have default values.",
    default=None,
)
field_options.add_argument(
    "--use-field-description",
    help="Use schema description to populate field docstring",
    action="store_true",
    default=None,
)
field_options.add_argument(
    "--union-mode",
    help="Union mode for only pydantic v2 field",
    choices=[u.value for u in UnionMode],
    default=None,
)
field_options.add_argument(
    "--no-alias",
    help="""Do not add a field alias. E.g., if --snake-case-field is used along with a base class, which has an
            alias_generator""",
    action="store_true",
    default=None,
)

# ======================================================================================
# Options for templating output
# ======================================================================================
template_options.add_argument(
    "--aliases",
    help="Alias mapping file",
    type=Path,
)
template_options.add_argument(
    "--custom-file-header",
    help="Custom file header",
    type=str,
    default=None,
)
template_options.add_argument(
    "--custom-file-header-path",
    help="Custom file header file path",
    default=None,
    type=str,
)
template_options.add_argument(
    "--custom-template-dir",
    help="Custom template directory",
    type=str,
)
template_options.add_argument(
    "--encoding",
    help=f"The encoding of input and output (default: {DEFAULT_ENCODING})",
    default=None,
)
template_options.add_argument(
    "--extra-template-data",
    help="Extra template data for output models. Input is supposed to be a json/yaml file. "
    "For OpenAPI and Jsonschema the keys are the spec path of the object, or the name of the object if you want to "
    "apply the template data to multiple objects with the same name. "
    "If you are using another input file type (e.g. GraphQL), the key is the name of the object. "
    "The value is a dictionary of the template data to add.",
    type=Path,
)
template_options.add_argument(
    "--use-double-quotes",
    action="store_true",
    default=None,
    help="Model generated with double quotes. Single quotes or "
    "your black config skip_string_normalization value will be used without this option.",
)
template_options.add_argument(
    "--wrap-string-literal",
    help="Wrap string literal by using black `experimental-string-processing` option (require black 20.8b0 or later)",
    action="store_true",
    default=None,
)
base_options.add_argument(
    "--additional-imports",
    help='Custom imports for output (delimited list input). For example "datetime.date,datetime.datetime"',
    type=str,
    default=None,
)
base_options.add_argument(
    "--formatters",
    help="Formatters for output (default: [black, isort])",
    choices=[f.value for f in Formatter],
    nargs="+",
    default=None,
)
base_options.add_argument(
    "--custom-formatters",
    help="List of modules with custom formatter (delimited list input).",
    type=str,
    default=None,
)
template_options.add_argument(
    "--custom-formatters-kwargs",
    help="A file with kwargs for custom formatters.",
    type=Path,
)

# ======================================================================================
# Options specific to OpenAPI input schemas
# ======================================================================================
openapi_options.add_argument(
    "--openapi-scopes",
    help="Scopes of OpenAPI model generation (default: schemas)",
    choices=[o.value for o in OpenAPIScope],
    nargs="+",
    default=None,
)
openapi_options.add_argument(
    "--strict-nullable",
    help="Treat default field as a non-nullable field (Only OpenAPI)",
    action="store_true",
    default=None,
)
openapi_options.add_argument(
    "--use-operation-id-as-name",
    help="use operation id of OpenAPI as class names of models",
    action="store_true",
    default=None,
)
openapi_options.add_argument(
    "--include-path-parameters",
    help="Include path parameters in generated parameter models in addition to query parameters (Only OpenAPI)",
    action="store_true",
    default=None,
)
openapi_options.add_argument(
    "--validation",
    help="Deprecated: Enable validation (Only OpenAPI). this option is deprecated. it will be removed in future "
    "releases",
    action="store_true",
    default=None,
)

# ======================================================================================
# General options
# ======================================================================================
general_options.add_argument(
    "--debug",
    help="show debug message (require \"debug\". `$ pip install 'datamodel-code-generator[debug]'`)",
    action="store_true",
    default=None,
)
general_options.add_argument(
    "--disable-warnings",
    help="disable warnings",
    action="store_true",
    default=None,
)
general_options.add_argument(
    "-h",
    "--help",
    action="help",
    default="==SUPPRESS==",
    help="show this help message and exit",
)
general_options.add_argument(
    "--no-color",
    action="store_true",
    default=False,
    help="disable colorized output",
)
general_options.add_argument(
    "--version",
    action="store_true",
    help="show version",
)

__all__ = [
    "DEFAULT_ENCODING",
    "arg_parser",
    "namespace",
]
