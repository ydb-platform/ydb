from django.forms import Form


def set_form_widgets_attrs(form: Form, attrs: dict):
    """Applies a given HTML attributes to each field widget of a given form.

    Example:

        set_form_widgets_attrs(my_form, {'class': 'clickable'})

    """
    for _, field in form.fields.items():
        attrs_ = dict(attrs)

        for name, val in attrs.items():
            if hasattr(val, '__call__'):
                attrs_[name] = val(field)

        field.widget.attrs = field.widget.build_attrs(attrs_)
