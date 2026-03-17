# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import django.utils.timezone
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='AuthProfile',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                #('mugshot', easy_thumbnails.fields.ThumbnailerImageField(help_text='A personal image displayed in your profile.', upload_to=userena.models.upload_to_mugshot, verbose_name='mugshot', blank=True)),
                #('privacy', models.CharField(default=b'registered', help_text='Designates who can view your profile.', max_length=15, verbose_name='privacy', choices=[(b'open', 'Open'), (b'registered', 'Registered'), (b'closed', 'Closed')])),
                #('user', models.OneToOneField(related_name='auth_profile', verbose_name='user', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'abstract': False,
                'permissions': (('view_profile', 'Can view profile'),),
            },
            bases=(models.Model,),
        ),

        migrations.CreateModel(
            name='CIDR',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('cidr', models.CharField(help_text=b'IP addresses and/or subnet', unique=True, max_length=128, verbose_name=b'CIDR')),
                ('label', models.CharField(help_text=b'Human-readable name (optional)', max_length=128, null=True, verbose_name=b'label', blank=True)),
                ('require_credentials', models.BooleanField(default=True)),
            ],
            options={
                'permissions': (('view_cidr', 'Can view CIDR'),),
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Credential',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('access_key', models.UUIDField(editable=False, max_length=32, blank=True, help_text=b'The access key', unique=True, verbose_name=b'Access key', db_index=True)),
                ('secret_key', models.UUIDField(editable=False, max_length=32, blank=True, help_text=b'The secret key', unique=True, verbose_name=b'Secret key', db_index=True)),
                ('created', models.DateTimeField(default=django.utils.timezone.now)),
                ('deactivated', models.DateTimeField(null=True, blank=True)),
                ('comment', models.CharField(default=b'', max_length=255, null=True, help_text=b"A comment about this credential, e.g. where it's being used", blank=True)),
                ('creator', models.ForeignKey(to=settings.AUTH_USER_MODEL, on_delete=models.CASCADE)),
            ],
            options={
                'ordering': ['-created'],
                'permissions': (('view_credential', 'Can view credential'),),
            },
            bases=(models.Model,),
        ),
    ]
