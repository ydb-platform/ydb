from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('django_celery_beat', '0003_auto_20161209_0049'),
    ]

    operations = [
        migrations.AlterField(
            model_name='solarschedule',
            name='longitude',
            field=models.DecimalField(
                verbose_name='longitude',
                max_digits=9,
                decimal_places=6),
        ),
    ]
