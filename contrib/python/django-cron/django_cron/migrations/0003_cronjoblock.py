# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('django_cron', '0002_remove_max_length_from_CronJobLog_message'),
    ]

    operations = [
        migrations.CreateModel(
            name='CronJobLock',
            fields=[
                (
                    'id',
                    models.AutoField(
                        verbose_name='ID',
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
                ('job_name', models.CharField(max_length=200, unique=True)),
                ('locked', models.BooleanField(default=False)),
            ],
        ),
    ]
