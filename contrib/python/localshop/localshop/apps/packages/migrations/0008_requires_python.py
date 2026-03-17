# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('packages', '0007_normalize_names'),
    ]

    operations = [
        migrations.AddField(
            model_name='releasefile',
            name='requires_python',
            field=models.CharField(max_length=255, null=True, blank=True),
        ),
    ]
