"""
Simple util to dynamically load the conf help text for documentation.
"""
from .base import TestPlan # Import all stuff used by TestPlan so that conf variables are declared.
from openhtf.util import conf

class ConfigHelpText(object):
    pass

docstring = """
Attributes:
"""

for name, decl in conf._declarations.items():
    description = decl.description.replace('\n', ' ').strip() if decl.description else "(no description)"
    default_value = decl.default_value
    docstring += (f"""
    {name}: {description}\n
        Default Value= {default_value!r}
    """)

ConfigHelpText.__doc__ = docstring

