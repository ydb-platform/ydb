import random
from django.db import migrations, models

from post_office.models import STATUS
from post_office.settings import get_message_id_enabled, get_message_id_fqdn


def forwards(apps, schema_editor):
    if not get_message_id_enabled():
        return
    msg_id_fqdn = get_message_id_fqdn()
    Email = apps.get_model('post_office', 'Email')
    for email in Email.objects.using(schema_editor.connection.alias).filter(message_id__isnull=True):
        if email.status in [STATUS.queued, STATUS.requeued]:
            # create a unique Message-ID for all emails which have not been send yet
            randint1, randint2 = random.getrandbits(64), random.getrandbits(16)
            email.message_id = f'<{email.id}.{randint1}.{randint2}@{msg_id_fqdn}>'
            email.save()


class Migration(migrations.Migration):
    dependencies = [
        ('post_office', '0009_requeued_mode'),
    ]

    operations = [
        migrations.AddField(
            model_name='email',
            name='message_id',
            field=models.CharField(editable=False, max_length=255, null=True, verbose_name='Message-ID'),
        ),
        migrations.AlterField(
            model_name='email',
            name='expires_at',
            field=models.DateTimeField(
                blank=True, help_text="Email won't be sent after this timestamp", null=True, verbose_name='Expires at'
            ),
        ),
        migrations.RunPython(forwards, reverse_code=migrations.RunPython.noop),
    ]
