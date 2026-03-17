# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('packages', '0003_default_repo'),
    ]

    operations = [
        migrations.AddField(
            model_name='package',
            name='repository',
            field=models.ForeignKey(related_name='packages', default=1, to='packages.Repository', on_delete=models.CASCADE),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='package',
            name='name',
            field=models.SlugField(max_length=200),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='package',
            unique_together=set([('repository', 'name')]),
        ),
    ]
