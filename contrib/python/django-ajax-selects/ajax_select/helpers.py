from django.db.models.fields.related import ForeignKey, ManyToManyField
from django.forms.models import ModelForm
from django.utils.encoding import force_str
from django.utils.text import capfirst
from django.utils.translation import gettext_lazy as _


def make_ajax_form(model, fieldlist, superclass=ModelForm, show_help_text=False, **kwargs):
    """
    Creates a ModelForm subclass with AutoComplete fields.

    Args:
        model (type): Model class for which you are making the ModelForm
        fieldlist (dict): {field_name -> channel_name, ...}
        superclass (type): optional ModelForm superclass
        show_help_text (bool): suppress or show the widget help text

    Returns:
        ModelForm: a ModelForm suitable for use in an Admin

    Usage::

        from django.contrib import admin
        from ajax_select import make_ajax_form
        from yourapp.models import YourModel

        @admin.register(YourModel)
        class YourModelAdmin(Admin):

            form = make_ajax_form(YourModel, {
                'contacts': 'contact',  # ManyToManyField
                'author':'contact'      # ForeignKeyField
            })

    Where 'contacts' is a ManyToManyField specifying to use the lookup channel 'contact'
    and 'author' is a ForeignKeyField specifying here to also use the same lookup channel 'contact'

    """
    # will support previous arg name for several versions before deprecating
    # TODO: time to go
    if "show_m2m_help" in kwargs:
        show_help_text = kwargs.pop("show_m2m_help")

    class TheForm(superclass):
        class Meta:
            exclude = []

        Meta.model = model
        if hasattr(superclass, "Meta"):
            if hasattr(superclass.Meta, "fields"):
                Meta.fields = superclass.Meta.fields
            if hasattr(superclass.Meta, "exclude"):
                Meta.exclude = superclass.Meta.exclude
            if hasattr(superclass.Meta, "widgets"):
                Meta.widgets = superclass.Meta.widgets

    for model_fieldname, channel in fieldlist.items():
        f = make_ajax_field(model, model_fieldname, channel, show_help_text)

        TheForm.declared_fields[model_fieldname] = f
        TheForm.base_fields[model_fieldname] = f

    return TheForm


def make_ajax_field(related_model, fieldname_on_model, channel, show_help_text=False, **kwargs):
    """
    Makes an AutoComplete field for use in a Form.

    Args:
        related_model (Model): model of the related object
        fieldname_on_model (str): field name on the model being edited
        channel (str): channel name of a registered LookupChannel
        show_help_text (bool): show or supress help text below the widget
            Django admin will show help text below the widget, but not for ManyToMany inside of admin inlines
            This setting will show the help text inside the widget itself.
        kwargs: optional args

            - help_text: default is the model db field's help_text.
                            None will disable all help text
            - label: default is the model db field's verbose name
            - required: default is the model db field's (not) blank

    Returns:
        (AutoCompleteField, AutoCompleteSelectField, AutoCompleteSelectMultipleField): field

    """
    from ajax_select.fields import AutoCompleteField, AutoCompleteSelectField, AutoCompleteSelectMultipleField

    field = related_model._meta.get_field(fieldname_on_model)
    if "label" not in kwargs:
        kwargs["label"] = _(capfirst(force_str(field.verbose_name)))

    if ("help_text" not in kwargs) and field.help_text:
        kwargs["help_text"] = field.help_text
    if "required" not in kwargs:
        kwargs["required"] = not field.blank

    kwargs["show_help_text"] = show_help_text
    if isinstance(field, ManyToManyField):
        f = AutoCompleteSelectMultipleField(channel, **kwargs)
    elif isinstance(field, ForeignKey):
        f = AutoCompleteSelectField(channel, **kwargs)
    else:
        f = AutoCompleteField(channel, **kwargs)
    return f
