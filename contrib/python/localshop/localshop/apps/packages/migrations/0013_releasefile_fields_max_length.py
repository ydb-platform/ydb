# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('packages', '0012_releasefile_python_version_max_length'),
    ]

    operations = [
        migrations.AlterField(
            model_name='releasefile',
            name='python_version',
            field=models.CharField(max_length=255),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='releasefile',
            name='filename',
            field=models.CharField(max_length=255),
            preserve_default=True,
        ),
    ]
