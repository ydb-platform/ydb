# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


def forwards(apps, schema_editor):
    AccessKey = apps.get_model('accounts', 'AccessKey')
    Credential = apps.get_model('permissions', 'Credential')

    for credential in Credential.objects.all():
        AccessKey.objects.create(
            access_key=credential.access_key,
            secret_key=credential.secret_key,
            comment=credential.comment,
            user=credential.creator,
        )


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0002_migrate_users'),
        ('permissions', '0001_squashed_0002_remove_userena'),
    ]

    operations = [
        migrations.RunPython(code=forwards),
    ]
