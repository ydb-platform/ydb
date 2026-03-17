# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models

import localshop.apps.packages.models


class Migration(migrations.Migration):

    dependencies = [
        ('packages', '0004_auto_20150517_1612'),
    ]

    operations = [
        migrations.AddField(
            model_name='repository',
            name='enable_auto_mirroring',
            field=models.BooleanField(default=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='releasefile',
            name='distribution',
            field=models.FileField(max_length=512, upload_to=localshop.apps.packages.models.release_file_upload_to),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='repository',
            name='description',
            field=models.CharField(max_length=500, blank=True),
            preserve_default=True,
        ),
    ]
