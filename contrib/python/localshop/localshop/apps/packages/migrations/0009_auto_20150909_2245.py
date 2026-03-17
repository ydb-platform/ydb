# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('packages', '0008_requires_python'),
    ]

    operations = [
        migrations.AlterField(
            model_name='releasefile',
            name='python_version',
            field=models.CharField(max_length=50),
            preserve_default=True,
        ),
    ]
