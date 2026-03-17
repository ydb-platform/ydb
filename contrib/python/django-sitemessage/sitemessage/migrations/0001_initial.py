# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings
import sitemessage.models


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Dispatch',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('time_created', models.DateTimeField(auto_now_add=True, verbose_name='Time created')),
                ('time_dispatched', models.DateTimeField(help_text='Time of the last delivery attempt.', verbose_name='Time dispatched', null=True, editable=False, blank=True)),
                ('messenger', models.CharField(help_text='Messenger class identifier.', max_length=250, verbose_name='Messenger', db_index=True)),
                ('address', models.CharField(help_text='Recipient address.', max_length=250, verbose_name='Address')),
                ('retry_count', models.PositiveIntegerField(default=0, help_text='A number of delivery retries has already been made.', verbose_name='Retry count')),
                ('message_cache', models.TextField(verbose_name='Message cache', null=True, editable=False)),
                ('dispatch_status', models.PositiveIntegerField(default=1, verbose_name='Dispatch status', choices=[(1, 'Pending'), (2, 'Sent'), (3, 'Error'), (4, 'Failed')])),
                ('read_status', models.PositiveIntegerField(default=0, verbose_name='Read status', choices=[(0, 'Unread'), (1, 'Read')])),
            ],
            options={
                'verbose_name': 'Dispatch',
                'verbose_name_plural': 'Dispatches',
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='DispatchError',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('time_created', models.DateTimeField(auto_now_add=True, verbose_name='Time created')),
                ('error_log', models.TextField(verbose_name='Text')),
                ('dispatch', models.ForeignKey(verbose_name='Dispatch', to='sitemessage.Dispatch', on_delete=models.CASCADE)),
            ],
            options={
                'verbose_name': 'Dispatch error',
                'verbose_name_plural': 'Dispatch errors',
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Message',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('time_created', models.DateTimeField(auto_now_add=True, verbose_name='Time created')),
                ('cls', models.CharField(help_text='Message logic class identifier.', max_length=250, verbose_name='Message class', db_index=True)),
                ('context', sitemessage.models.ContextField(verbose_name='Message context')),
                ('priority', models.PositiveIntegerField(default=0, help_text='Number describing message sending priority. Messages with different priorities can be sent with different periodicity.', verbose_name='Priority', db_index=True)),
                ('dispatches_ready', models.BooleanField(default=False, help_text='Indicates whether dispatches for this message are already formed and ready to delivery.', db_index=True, verbose_name='Dispatches ready')),
                ('sender', models.ForeignKey(verbose_name='Sender', blank=True, to=settings.AUTH_USER_MODEL, null=True, on_delete=models.CASCADE)),
            ],
            options={
                'verbose_name': 'Message',
                'verbose_name_plural': 'Messages',
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='dispatch',
            name='message',
            field=models.ForeignKey(verbose_name='Message', to='sitemessage.Message', on_delete=models.CASCADE),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='dispatch',
            name='recipient',
            field=models.ForeignKey(verbose_name='Recipient', blank=True, to=settings.AUTH_USER_MODEL, null=True, on_delete=models.CASCADE),
            preserve_default=True,
        ),
    ]
