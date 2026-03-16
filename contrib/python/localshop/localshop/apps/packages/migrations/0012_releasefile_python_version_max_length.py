# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('packages', '0011_package_name'),
    ]

    operations = [
        migrations.AlterField(
            model_name='releasefile',
            name='python_version',
            field=models.CharField(max_length=100),
            preserve_default=True,
        ),
    ]
