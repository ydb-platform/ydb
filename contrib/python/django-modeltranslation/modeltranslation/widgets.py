from __future__ import unicode_literals

from django.forms.widgets import Media, Widget, CheckboxInput
from django.utils.html import conditional_escape
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _


class ClearableWidgetWrapper(Widget):
    """
    Wraps another widget adding a clear checkbox, making it possible to
    reset the field to some empty value even if the original input doesn't
    have means to.

    Useful for ``TextInput`` and ``Textarea`` based widgets used in combination
    with nullable text fields.

    Use it in ``Field.formfield`` or ``ModelAdmin.formfield_for_dbfield``:

        field.widget = ClearableWidgetWrapper(field.widget)

    ``None`` is assumed to be a proper choice for the empty value, but you may
    pass another one to the constructor.
    """

    clear_checkbox_label = _("None")
    template = '<span class="clearable-input">{0} <span>{2}</span> {3}</span>'
    # TODO: Label would be proper, but admin applies some hardly undoable
    #       styling to labels.
    # template = '<span class="clearable-input">{} <label for="{}">{}</label> {}</span>'

    class Media:
        js = ('modeltranslation/js/clearable_inputs.js',)

    def __init__(self, widget, empty_value=None):
        """
        Remebers the widget we are wrapping and precreates a checkbox input.
        Allows overriding the empty value.
        """
        self.widget = widget
        self.checkbox = CheckboxInput(attrs={'tabindex': '-1'})
        self.empty_value = empty_value

    def __getattr__(self, name):
        """
        If we don't have a property or a method, chances are the wrapped
        widget does.
        """
        if name != 'widget':
            return getattr(self.widget, name)
        raise AttributeError

    @property
    def media(self):
        """
        Combines media of both components and adds a small script that unchecks
        the clear box, when a value in any wrapped input is modified.
        """
        return self.widget.media + self.checkbox.media + Media(self.Media)

    def render(self, name, value, attrs=None, renderer=None):
        """
        Appends a checkbox for clearing the value (that is, setting the field
        with the ``empty_value``).
        """
        wrapped = self.widget.render(name, value, attrs, renderer)
        checkbox_name = self.clear_checkbox_name(name)
        checkbox_id = self.clear_checkbox_id(checkbox_name)
        checkbox_label = self.clear_checkbox_label
        checkbox = self.checkbox.render(
            checkbox_name, value == self.empty_value, attrs={'id': checkbox_id}, renderer=renderer
        )
        return mark_safe(
            self.template.format(
                conditional_escape(wrapped),
                conditional_escape(checkbox_id),
                conditional_escape(checkbox_label),
                conditional_escape(checkbox),
            )
        )

    def value_from_datadict(self, data, files, name):
        """
        If the clear checkbox is checked returns the configured empty value,
        completely ignoring the original input.
        """
        clear = self.checkbox.value_from_datadict(data, files, self.clear_checkbox_name(name))
        if clear:
            return self.empty_value
        return self.widget.value_from_datadict(data, files, name)

    def clear_checkbox_name(self, name):
        """
        Given the name of the input, returns the name of the clear checkbox.
        """
        return name + '-clear'

    def clear_checkbox_id(self, name):
        """
        Given the name of the clear checkbox input, returns the HTML id for it.
        """
        return name + '_id'
