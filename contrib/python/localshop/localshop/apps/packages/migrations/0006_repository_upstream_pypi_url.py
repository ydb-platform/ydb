# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('packages', '0005_auto_20150525_1931'),
    ]

    operations = [
        migrations.AddField(
            model_name='repository',
            name='upstream_pypi_url',
            field=models.CharField(default=b'https://pypi.python.org/simple', help_text="The upstream pypi URL (default 'https://pypi.python.org/simple'", max_length=500),
            preserve_default=True,
        ),
    ]
