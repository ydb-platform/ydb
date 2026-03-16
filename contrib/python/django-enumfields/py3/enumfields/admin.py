from django.contrib.admin.filters import ChoicesFieldListFilter
from django.utils.encoding import force_str
from django.utils.translation import gettext_lazy as _


class EnumFieldListFilter(ChoicesFieldListFilter):
    def choices(self, cl):
        yield {
            'selected': self.lookup_val is None,
            'query_string': cl.get_query_string({}, [self.lookup_kwarg]),
            'display': _('All'),
        }
        for enum_value in self.field.enum:
            str_value = force_str(enum_value.value)
            yield {
                'selected': (str_value == self.lookup_val),
                'query_string': cl.get_query_string({self.lookup_kwarg: str_value}),
                'display': getattr(enum_value, 'label', None) or force_str(enum_value),
            }

    def queryset(self, request, queryset):
        try:
            self.field.enum(self.lookup_val)
        except ValueError:
            # since `used_parameters` will always contain strings,
            # for non-string-valued enums we'll need to fall back to attempt a slower
            # linear stringly-typed lookup.
            for enum_value in self.field.enum:
                if force_str(enum_value.value) == self.lookup_val:
                    self.used_parameters[self.lookup_kwarg] = enum_value
                    break
        return super().queryset(request, queryset)
