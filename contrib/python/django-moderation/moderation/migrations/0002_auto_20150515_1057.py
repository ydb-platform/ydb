# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('moderation', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='moderatedobject',
            name='date_updated',
            field=models.DateTimeField(auto_now=True),
        ),
    ]
