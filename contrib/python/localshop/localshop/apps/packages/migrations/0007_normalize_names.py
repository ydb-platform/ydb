# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


def forwards(apps, schema_editor):
    from localshop.apps.packages.pypi import normalize_name

    Package = apps.get_model('packages', 'Package')
    for package in Package.objects.all():
        package.normalized_name = normalize_name(package.name)
        package.save()


class Migration(migrations.Migration):

    dependencies = [
        ('packages', '0006_repository_upstream_pypi_url'),
    ]

    operations = [
        migrations.AddField(
            model_name='package',
            name='normalized_name',
            field=models.SlugField(max_length=200, null=True, blank=True),
            preserve_default=True,
        ),
        migrations.RunPython(code=forwards),
        migrations.AlterField(
            model_name='package',
            name='normalized_name',
            field=models.SlugField(max_length=200),
            preserve_default=True,
        ),
    ]
