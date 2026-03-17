from django.conf import settings
from django.contrib import admin


if 'django.contrib.auth' in settings.INSTALLED_APPS:
    from tastypie.models import ApiKey

    class ApiKeyInline(admin.StackedInline):
        model = ApiKey
        extra = 0

    class ApiKeyAdmin(admin.ModelAdmin):
        search_fields = ['key', 'user__username']
        raw_id_fields = ['user']
        list_display = ['id', 'user', 'key', 'created']

    ABSTRACT_APIKEY = getattr(settings, 'TASTYPIE_ABSTRACT_APIKEY', False)

    if ABSTRACT_APIKEY and not isinstance(ABSTRACT_APIKEY, bool):
        raise TypeError("'TASTYPIE_ABSTRACT_APIKEY' must be either 'True' "
                        "or 'False'.")

    if not ABSTRACT_APIKEY:
        admin.site.register(ApiKey, ApiKeyAdmin)
