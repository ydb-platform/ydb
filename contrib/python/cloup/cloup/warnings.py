"""
This module contains a boolean variable for each warning that may be raised
by Cloup. To suppress a warning, set the corresponding variable to ``False``.
This is an alternative to using ``warnings.filterwarnings`` that:

- IMHO, offers a better developer experience since setting a boolean is
  easier and cleaner than writing a regex that matches a warning message
- can be used by Cloup itself to skip the checks that (may) generate a warning.
"""

formatter_settings_conflict = True
