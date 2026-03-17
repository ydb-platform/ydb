from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('advanced_filters', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='advancedfilter',
            name='created_at',
            field=models.DateTimeField(auto_now_add=True, null=True),
        ),
    ]
