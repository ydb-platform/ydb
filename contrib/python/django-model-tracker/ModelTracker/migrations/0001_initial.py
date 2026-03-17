# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
try:
    from django.db.models import JSONField
except ImportError:
    try:
        from jsonfield.fields import JSONField
    except ImportError:
        raise ImportError("Can't find a JSONField implementation, please install jsonfield if django < 4.0")


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='History',
            fields=[
                ('id', models.AutoField(serialize=False, primary_key=True, db_column='id')),
                ('table', models.CharField(max_length=255)),
                ('primary_key', models.CharField(max_length=255)),
                ('old_state', JSONField(default={})),
                ('new_state', JSONField(default={})),
                ('done_by', models.CharField(max_length=255)),
                ('done_on', models.DateTimeField(auto_now_add=True)),
            ],
        ),
    ]
