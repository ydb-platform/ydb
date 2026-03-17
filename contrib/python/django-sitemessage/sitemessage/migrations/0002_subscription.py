# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('sitemessage', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Subscription',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('time_created', models.DateTimeField(auto_now_add=True, verbose_name='Time created')),
                ('message_cls', models.CharField(help_text='Message logic class identifier.', max_length=250, verbose_name='Message class', db_index=True)),
                ('messenger_cls', models.CharField(help_text='Messenger class identifier.', max_length=250, verbose_name='Messenger', db_index=True)),
                ('address', models.CharField(help_text='Recipient address.', max_length=250, null=True, verbose_name='Address')),
                ('recipient', models.ForeignKey(verbose_name='Recipient', blank=True, to=settings.AUTH_USER_MODEL, null=True, on_delete=models.CASCADE)),
            ],
            options={
                'verbose_name': 'Subscription',
                'verbose_name_plural': 'Subscriptions',
            },
            bases=(models.Model,),
        ),
    ]
