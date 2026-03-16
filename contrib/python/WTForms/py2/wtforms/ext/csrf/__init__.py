import warnings

from wtforms.ext.csrf.form import SecureForm

warnings.warn(
    "'wtforms.ext.csrf' will be removed in WTForms 3.0. It is built-in"
    " to WTForms core now.",
    DeprecationWarning,
)
