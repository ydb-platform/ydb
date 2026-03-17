# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('ModelTracker', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='history',
            name='name',
            field=models.CharField(default='', max_length=255),
        ),
        migrations.AlterField(
            model_name='history',
            name='id',
            field=models.AutoField(serialize=False, primary_key=True),
        ),
    ]
