from collections import OrderedDict
from datetime import date, datetime
from operator import itemgetter

from django import VERSION, forms
from django.apps import apps
from django.contrib import admin, messages
from django.contrib.admin.options import csrf_protect_m
from django.core.exceptions import PermissionDenied
from django.http import HttpResponseRedirect
from django.template.response import TemplateResponse
from django.urls import path
from django.utils.formats import localize
from django.utils.translation import gettext_lazy as _

from . import LazyConfig, settings
from .forms import ConstanceForm
from .utils import get_values

config = LazyConfig()


class ConstanceAdmin(admin.ModelAdmin):
    change_list_template = 'admin/constance/change_list.html'
    change_list_form = ConstanceForm

    def __init__(self, model, admin_site):
        model._meta.concrete_model = Config
        super().__init__(model, admin_site)

    def get_urls(self):
        info = self.model._meta.app_label, self.model._meta.module_name
        return [
            path('',
                self.admin_site.admin_view(self.changelist_view),
                name='%s_%s_changelist' % info),
            path('',
                self.admin_site.admin_view(self.changelist_view),
                name='%s_%s_add' % info),
        ]

    def get_config_value(self, name, options, form, initial):
        default, help_text = options[0], options[1]
        field_type = None
        if len(options) == 3:
            field_type = options[2]
        # First try to load the value from the actual backend
        value = initial.get(name)
        # Then if the returned value is None, get the default
        if value is None:
            value = getattr(config, name)

        form_field = form[name]
        config_value = {
            'name': name,
            'default': localize(default),
            'raw_default': default,
            'help_text': _(help_text),
            'value': localize(value),
            'modified': localize(value) != localize(default),
            'form_field': form_field,
            'is_date': isinstance(default, date),
            'is_datetime': isinstance(default, datetime),
            'is_checkbox': isinstance(form_field.field.widget, forms.CheckboxInput),
            'is_file': isinstance(form_field.field.widget, forms.FileInput),
        }
        if field_type and field_type in settings.ADDITIONAL_FIELDS:
            serialized_default = form[name].field.prepare_value(default)
            config_value['default'] = serialized_default
            config_value['raw_default'] = serialized_default
            config_value['value'] = form[name].field.prepare_value(value)

        return config_value

    def get_changelist_form(self, request):
        """
        Returns a Form class for use in the changelist_view.
        """
        # Defaults to self.change_list_form in order to preserve backward
        # compatibility
        return self.change_list_form

    @csrf_protect_m
    def changelist_view(self, request, extra_context=None):
        if not self.has_view_or_change_permission(request):
            raise PermissionDenied
        initial = get_values()
        form_cls = self.get_changelist_form(request)
        form = form_cls(initial=initial, request=request)
        if request.method == 'POST' and request.user.has_perm('constance.change_config'):
            form = form_cls(
                data=request.POST, files=request.FILES, initial=initial, request=request
            )
            if form.is_valid():
                form.save()
                messages.add_message(
                    request,
                    messages.SUCCESS,
                    _('Live settings updated successfully.'),
                )
                return HttpResponseRedirect('.')
            else:
                messages.add_message(
                    request,
                    messages.ERROR,
                    _('Failed to update live settings.'),
                )
        context = dict(
            self.admin_site.each_context(request),
            config_values=[],
            title=self.model._meta.app_config.verbose_name,
            app_label='constance',
            opts=self.model._meta,
            form=form,
            media=self.media + form.media,
            icon_type='gif' if VERSION < (1, 9) else 'svg',
        )
        for name, options in settings.CONFIG.items():
            context['config_values'].append(
                self.get_config_value(name, options, form, initial)
            )

        if settings.CONFIG_FIELDSETS:
            if isinstance(settings.CONFIG_FIELDSETS, dict):
                fieldset_items = settings.CONFIG_FIELDSETS.items()
            else:
                fieldset_items = settings.CONFIG_FIELDSETS

            context['fieldsets'] = []
            for fieldset_title, fieldset_data in fieldset_items:
                if type(fieldset_data) == dict:
                    fields_list = fieldset_data['fields']
                    collapse = fieldset_data.get('collapse', False)
                else:
                    fields_list = fieldset_data
                    collapse = False

                absent_fields = [field for field in fields_list
                                 if field not in settings.CONFIG]
                assert not any(absent_fields), (
                    "CONSTANCE_CONFIG_FIELDSETS contains field(s) that does "
                    "not exist: %s" % ', '.join(absent_fields))

                config_values = []

                for name in fields_list:
                    options = settings.CONFIG.get(name)
                    if options:
                        config_values.append(
                            self.get_config_value(name, options, form, initial)
                        )
                fieldset_context = {
                    'title': fieldset_title,
                    'config_values': config_values
                }

                if collapse:
                    fieldset_context['collapse'] = True
                context['fieldsets'].append(fieldset_context)
            if not isinstance(settings.CONFIG_FIELDSETS, (OrderedDict, tuple)):
                context['fieldsets'].sort(key=itemgetter('title'))

        if not isinstance(settings.CONFIG, OrderedDict):
            context['config_values'].sort(key=itemgetter('name'))
        request.current_app = self.admin_site.name
        return TemplateResponse(request, self.change_list_template, context)

    def has_add_permission(self, *args, **kwargs):
        return False

    def has_delete_permission(self, *args, **kwargs):
        return False

    def has_change_permission(self, request, obj=None):
        if settings.SUPERUSER_ONLY:
            return request.user.is_superuser
        return super().has_change_permission(request, obj)


class Config:
    class Meta:
        app_label = 'constance'
        object_name = 'Config'
        concrete_model = None
        model_name = module_name = 'config'
        verbose_name_plural = _('config')
        abstract = False
        swapped = False
        is_composite_pk = False

        def get_ordered_objects(self):
            return False

        def get_change_permission(self):
            return 'change_%s' % self.model_name

        @property
        def app_config(self):
            return apps.get_app_config(self.app_label)

        @property
        def label(self):
            return '%s.%s' % (self.app_label, self.object_name)

        @property
        def label_lower(self):
            return '%s.%s' % (self.app_label, self.model_name)

    _meta = Meta()


admin.site.register([Config], ConstanceAdmin)