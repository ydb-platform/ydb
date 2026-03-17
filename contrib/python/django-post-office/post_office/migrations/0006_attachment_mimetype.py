from django.db import models, migrations


class Migration(migrations.Migration):
    dependencies = [
        ('post_office', '0005_auto_20170515_0013'),
    ]

    operations = [
        migrations.AddField(
            model_name='attachment',
            name='mimetype',
            field=models.CharField(default='', max_length=255, blank=True),
        ),
    ]
