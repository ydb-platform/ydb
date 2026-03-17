# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import django.utils.timezone
import model_utils.fields
from django.conf import settings
from django.db import migrations, models

import localshop.apps.packages.models


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Classifier',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(unique=True, max_length=255)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Package',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('created', model_utils.fields.AutoCreatedField(default=django.utils.timezone.now, editable=False, db_index=True)),
                ('modified', model_utils.fields.AutoLastModifiedField(default=django.utils.timezone.now, editable=False)),
                ('name', models.SlugField(unique=True, max_length=200)),
                ('is_local', models.BooleanField(default=False)),
                ('update_timestamp', models.DateTimeField(null=True)),
                ('owners', models.ManyToManyField(to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'ordering': ['name'],
                'permissions': (('view_package', 'Can view package'),),
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Release',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('created', model_utils.fields.AutoCreatedField(default=django.utils.timezone.now, editable=False)),
                ('modified', model_utils.fields.AutoLastModifiedField(default=django.utils.timezone.now, editable=False)),
                ('author', models.CharField(max_length=128, blank=True)),
                ('author_email', models.CharField(max_length=255, blank=True)),
                ('description', models.TextField(blank=True)),
                ('download_url', models.CharField(max_length=200, null=True, blank=True)),
                ('home_page', models.CharField(max_length=200, null=True, blank=True)),
                ('license', models.TextField(blank=True)),
                ('metadata_version', models.CharField(default=1.0, max_length=64)),
                ('summary', models.TextField(blank=True)),
                ('version', models.CharField(max_length=512)),
                ('classifiers', models.ManyToManyField(to='packages.Classifier')),
                ('package', models.ForeignKey(related_name='releases', to='packages.Package', on_delete=models.CASCADE)),
                ('user', models.ForeignKey(to=settings.AUTH_USER_MODEL, null=True, on_delete=models.CASCADE)),
            ],
            options={
                'ordering': ['-version'],
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ReleaseFile',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('created', model_utils.fields.AutoCreatedField(default=django.utils.timezone.now, editable=False)),
                ('modified', model_utils.fields.AutoLastModifiedField(default=django.utils.timezone.now, editable=False)),
                ('size', models.IntegerField(null=True)),
                ('filetype', models.CharField(max_length=25, choices=[(b'sdist', b'Source'), (b'bdist_egg', b'Egg'), (b'bdist_msi', b'MSI'), (b'bdist_dmg', b'DMG'), (b'bdist_rpm', b'RPM'), (b'bdist_dumb', b'bdist_dumb'), (b'bdist_wininst', b'bdist_wininst'), (b'bdist_wheel', b'bdist_wheel')])),
                ('distribution', models.FileField(max_length=512, upload_to=localshop.apps.packages.models.release_file_upload_to)),
                ('filename', models.CharField(max_length=200, null=True, blank=True)),
                ('md5_digest', models.CharField(max_length=512)),
                ('python_version', models.CharField(max_length=25)),
                ('url', models.CharField(max_length=1024, blank=True)),
                ('release', models.ForeignKey(related_name='files', to='packages.Release', on_delete=models.CASCADE)),
                ('user', models.ForeignKey(to=settings.AUTH_USER_MODEL, null=True, on_delete=models.CASCADE)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.AlterUniqueTogether(
            name='releasefile',
            unique_together=set([('release', 'filetype', 'python_version', 'filename')]),
        ),
    ]
