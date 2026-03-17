import hashlib
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from os.path import join

from django import conf, forms
from django.contrib import messages
from django.contrib.admin import widgets
from django.core.exceptions import ImproperlyConfigured
from django.core.files.storage import default_storage
from django.forms import fields
from django.utils import timezone
from django.utils.encoding import smart_bytes
from django.utils.module_loading import import_string
from django.utils.text import normalize_newlines
from django.utils.translation import gettext_lazy as _

from . import LazyConfig, settings
from .checks import get_inconsistent_fieldnames

config = LazyConfig()

NUMERIC_WIDGET = forms.TextInput(attrs={'size': 10})

INTEGER_LIKE = (fields.IntegerField, {'widget': NUMERIC_WIDGET})
STRING_LIKE = (fields.CharField, {
    'widget': forms.Textarea(attrs={'rows': 3}),
    'required': False,
})

FIELDS = {
    bool: (fields.BooleanField, {'required': False}),
    int: INTEGER_LIKE,
    Decimal: (fields.DecimalField, {'widget': NUMERIC_WIDGET}),
    str: STRING_LIKE,
    datetime: (
        fields.SplitDateTimeField, {'widget': widgets.AdminSplitDateTime}
    ),
    timedelta: (
        fields.DurationField, {'widget': widgets.AdminTextInputWidget}
    ),
    date: (fields.DateField, {'widget': widgets.AdminDateWidget}),
    time: (fields.TimeField, {'widget': widgets.AdminTimeWidget}),
    float: (fields.FloatField, {'widget': NUMERIC_WIDGET}),
}

def parse_additional_fields(fields):
    for key in fields:
        field = list(fields[key])

        if len(field) == 1:
            field.append({})

        field[0] = import_string(field[0])

        if 'widget' in field[1]:
            klass = import_string(field[1]['widget'])
            field[1]['widget'] = klass(
                **(field[1].get('widget_kwargs', {}) or {})
            )

            if 'widget_kwargs' in field[1]:
                del field[1]['widget_kwargs']

        fields[key] = field

    return fields


FIELDS.update(parse_additional_fields(settings.ADDITIONAL_FIELDS))



class ConstanceForm(forms.Form):
    version = forms.CharField(widget=forms.HiddenInput)

    def __init__(self, initial, request=None, *args, **kwargs):
        super().__init__(*args, initial=initial, **kwargs)
        version_hash = hashlib.sha256()

        only_view = request and not request.user.has_perm('constance.change_config')
        if only_view:
            messages.warning(
                request,
                _("You don't have permission to change these values"),
            )

        for name, options in settings.CONFIG.items():
            default = options[0]
            if len(options) == 3:
                config_type = options[2]
                if config_type not in settings.ADDITIONAL_FIELDS and not isinstance(default, config_type):
                    raise ImproperlyConfigured(_("Default value type must be "
                                                 "equal to declared config "
                                                 "parameter type. Please fix "
                                                 "the default value of "
                                                 "'%(name)s'.")
                                               % {'name': name})
            else:
                config_type = type(default)

            if config_type not in FIELDS:
                raise ImproperlyConfigured(_("Constance doesn't support "
                                             "config values of the type "
                                             "%(config_type)s. Please fix "
                                             "the value of '%(name)s'.")
                                           % {'config_type': config_type,
                                              'name': name})
            field_class, kwargs = FIELDS[config_type]
            if only_view:
                kwargs['disabled'] = True
            self.fields[name] = field_class(label=name, **kwargs)

            version_hash.update(smart_bytes(initial.get(name, '')))
        self.initial['version'] = version_hash.hexdigest()

    def save(self):
        for file_field in self.files:
            file = self.cleaned_data[file_field]
            self.cleaned_data[file_field] = default_storage.save(join(settings.FILE_ROOT, file.name), file)

        for name in settings.CONFIG:
            current = getattr(config, name)
            new = self.cleaned_data[name]

            if isinstance(new, str):
                new = normalize_newlines(new)

            if conf.settings.USE_TZ and isinstance(current, datetime) and not timezone.is_aware(current):
                current = timezone.make_aware(current)

            if current != new:
                setattr(config, name, new)

    def clean_version(self):
        value = self.cleaned_data['version']

        if settings.IGNORE_ADMIN_VERSION_CHECK:
            return value

        if value != self.initial['version']:
            raise forms.ValidationError(_('The settings have been modified '
                                          'by someone else. Please reload the '
                                          'form and resubmit your changes.'))
        return value

    def clean(self):
        cleaned_data = super().clean()

        if not settings.CONFIG_FIELDSETS:
            return cleaned_data

        missing_keys, extra_keys = get_inconsistent_fieldnames()
        if missing_keys or extra_keys:
            raise forms.ValidationError(_('CONSTANCE_CONFIG_FIELDSETS is missing '
                                          'field(s) that exists in CONSTANCE_CONFIG.'))

        return cleaned_data
