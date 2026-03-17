import uuid
from django.contrib import admin
from django.contrib.contenttypes.models import ContentType
from django.urls import reverse
from django.http import HttpResponseRedirect
try:
    # Django < 4.0
    from django.utils.translation import ugettext_lazy as _
except ImportError:
    # Django >= 4.0
    from django.utils.translation import gettext_lazy as _


def export_selected_objects(modeladmin, request, queryset):
    selected = list(queryset.values_list('pk', flat=True))
    ct = ContentType.objects.get_for_model(queryset.model)
    url = reverse("export_action:export")

    if len(selected) > 1000:
        session_key = "export_action_%s" % uuid.uuid4()
        request.session[session_key] = selected
        return HttpResponseRedirect("%s?ct=%s&session_key=%s" % (url, ct.pk, session_key))
    else:
        return HttpResponseRedirect(
            "%s?ct=%s&ids=%s" % (url, ct.pk, ",".join(str(pk) for pk in selected)))


export_selected_objects.short_description = _("Export selected items...")

admin.site.add_action(export_selected_objects)
