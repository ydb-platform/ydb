from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('packages', '0013_releasefile_fields_max_length'),
    ]

    operations = [
        migrations.AddField(
            model_name='releasefile',
            name='upstream_pypi_upload_time',
            field=models.DateTimeField(null=True, blank=True),
        ),
    ]
