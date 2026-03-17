import warnings

from .. import widgets
from .core import Field, StringField, BooleanField


__all__ = (
    'BooleanField', 'TextAreaField', 'PasswordField', 'FileField', 'MultipleFileField',
    'HiddenField', 'SubmitField', 'TextField'
)


class TextField(StringField):
    """
    Legacy alias for StringField

    .. deprecated:: 2.0
    """
    def __init__(self, *args, **kw):
        super(TextField, self).__init__(*args, **kw)
        warnings.warn(
            'The TextField alias for StringField has been deprecated and will be removed in WTForms 3.0',
            DeprecationWarning, stacklevel=2
        )


class TextAreaField(StringField):
    """
    This field represents an HTML ``<textarea>`` and can be used to take
    multi-line input.
    """
    widget = widgets.TextArea()


class PasswordField(StringField):
    """
    A StringField, except renders an ``<input type="password">``.

    Also, whatever value is accepted by this field is not rendered back
    to the browser like normal fields.
    """
    widget = widgets.PasswordInput()


class FileField(Field):
    """Renders a file upload field.

    By default, the value will be the filename sent in the form data.
    WTForms **does not** deal with frameworks' file handling capabilities.
    A WTForms extension for a framework may replace the filename value
    with an object representing the uploaded data.
    """

    widget = widgets.FileInput()

    def _value(self):
        # browser ignores value of file input for security
        return False


class MultipleFileField(FileField):
    """A :class:`FileField` that allows choosing multiple files."""

    widget = widgets.FileInput(multiple=True)

    def process_formdata(self, valuelist):
        self.data = valuelist


class HiddenField(StringField):
    """
    HiddenField is a convenience for a StringField with a HiddenInput widget.

    It will render as an ``<input type="hidden">`` but otherwise coerce to a string.
    """
    widget = widgets.HiddenInput()


class SubmitField(BooleanField):
    """
    Represents an ``<input type="submit">``.  This allows checking if a given
    submit button has been pressed.
    """
    widget = widgets.SubmitInput()
