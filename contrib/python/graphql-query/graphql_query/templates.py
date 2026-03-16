from jinja2 import Environment, Template, PackageLoader

__all__ = [
    "_template_key_value",
    "_template_key_values",
    "_template_key_argument",
    "_template_key_variable",
    "_template_key_arguments",
    "_template_key_objects",
    "_template_directive",
    "_template_variable",
    "_template_operation",
    "_template_query",
    "_template_fragment",
    "_template_inline_fragment",
    "_template_field",
]

template_env = Environment(loader=PackageLoader(__name__, 'templates'))


_template_key_value: Template = template_env.get_template("argument_key_value.jinja2")
_template_key_values: Template = template_env.get_template("argument_key_values.jinja2")
_template_key_argument: Template = template_env.get_template("argument_key_argument.jinja2")
_template_key_variable: Template = template_env.get_template("argument_key_variable.jinja2")
_template_key_arguments: Template = template_env.get_template("argument_key_arguments.jinja2")
_template_key_objects: Template = template_env.get_template("argument_key_objects.jinja2")
_template_directive: Template = template_env.get_template("directive.jinja2")
_template_variable: Template = template_env.get_template("variable.jinja2")
_template_operation: Template = template_env.get_template("operation.jinja2")
_template_query: Template = template_env.get_template("query.jinja2")
_template_fragment: Template = template_env.get_template("fragment.jinja2")
_template_inline_fragment: Template = template_env.get_template("inline_fragment.jinja2")
_template_field: Template = template_env.get_template("field.jinja2")
