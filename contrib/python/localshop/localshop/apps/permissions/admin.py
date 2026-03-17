from django.contrib import admin

from localshop.apps.permissions import models


@admin.register(models.CIDR)
class CidrAdmin(admin.ModelAdmin):

    raw_id_fields = (
        'repository',

    )

    list_display = (
        'cidr',
        'label',
    )


@admin.register(models.Credential)
class CredentialAdmin(admin.ModelAdmin):

    raw_id_fields = (
        'repository',
    )

    list_display = (
        'repository',
        'access_key',
        'created',
        'comment',
        'allow_upload',
    )

    list_filter = (
        'repository',
        'allow_upload',
    )
