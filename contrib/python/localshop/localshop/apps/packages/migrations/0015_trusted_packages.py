from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('packages', '0014_releasefile_upload_time'),
    ]

    operations = [
        migrations.AddField(
            model_name='package',
            name='is_trusted',
            field=models.BooleanField(default=False),
        ),
    ]
