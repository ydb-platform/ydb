from django.db import models, migrations

import post_office.fields
import post_office.validators
import post_office.models


class Migration(migrations.Migration):
    dependencies = []

    operations = [
        migrations.CreateModel(
            name='Attachment',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('file', models.FileField(upload_to=post_office.models.get_upload_path)),
                ('name', models.CharField(help_text='The original filename', max_length=255)),
            ],
            options={},
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Email',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                (
                    'from_email',
                    models.CharField(max_length=254, validators=[post_office.validators.validate_email_with_name]),
                ),
                ('to', post_office.fields.CommaSeparatedEmailField(blank=True)),
                ('cc', post_office.fields.CommaSeparatedEmailField(blank=True)),
                ('bcc', post_office.fields.CommaSeparatedEmailField(blank=True)),
                ('subject', models.CharField(max_length=255, blank=True)),
                ('message', models.TextField(blank=True)),
                ('html_message', models.TextField(blank=True)),
                (
                    'status',
                    models.PositiveSmallIntegerField(
                        blank=True, null=True, db_index=True, choices=[(0, 'sent'), (1, 'failed'), (2, 'queued')]
                    ),
                ),
                (
                    'priority',
                    models.PositiveSmallIntegerField(
                        blank=True, null=True, choices=[(0, 'low'), (1, 'medium'), (2, 'high'), (3, 'now')]
                    ),
                ),
                ('created', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('last_updated', models.DateTimeField(auto_now=True, db_index=True)),
                ('scheduled_time', models.DateTimeField(db_index=True, null=True, blank=True)),
                ('headers', models.JSONField(null=True, blank=True)),
                ('context', models.JSONField(null=True, blank=True)),
            ],
            options={},
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='EmailTemplate',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(help_text="e.g: 'welcome_email'", max_length=255)),
                ('description', models.TextField(help_text='Description of this template.', blank=True)),
                (
                    'subject',
                    models.CharField(
                        blank=True, max_length=255, validators=[post_office.validators.validate_template_syntax]
                    ),
                ),
                ('content', models.TextField(blank=True, validators=[post_office.validators.validate_template_syntax])),
                (
                    'html_content',
                    models.TextField(blank=True, validators=[post_office.validators.validate_template_syntax]),
                ),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('last_updated', models.DateTimeField(auto_now=True)),
            ],
            options={},
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Log',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('date', models.DateTimeField(auto_now_add=True)),
                ('status', models.PositiveSmallIntegerField(choices=[(0, 'sent'), (1, 'failed')])),
                ('exception_type', models.CharField(max_length=255, blank=True)),
                ('message', models.TextField()),
                (
                    'email',
                    models.ForeignKey(
                        related_name='logs',
                        editable=False,
                        on_delete=models.deletion.CASCADE,
                        to='post_office.Email',
                    ),
                ),
            ],
            options={},
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='email',
            name='template',
            field=models.ForeignKey(
                blank=True, on_delete=models.deletion.SET_NULL, to='post_office.EmailTemplate', null=True
            ),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='attachment',
            name='emails',
            field=models.ManyToManyField(related_name='attachments', to='post_office.Email'),
            preserve_default=True,
        ),
    ]
