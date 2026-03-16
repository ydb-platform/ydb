import datetime

from django.core.management.base import BaseCommand
from django.utils.timezone import now

from ...utils import cleanup_expired_mails


class Command(BaseCommand):
    help = 'Place deferred messages back in the queue.'

    def add_arguments(self, parser):
        parser.add_argument(
            '-d', '--days', type=int, default=90, help='Cleanup mails older than this many days, defaults to 90.'
        )

        parser.add_argument('-da', '--delete-attachments', action='store_true', help='Delete orphaned attachments.')

        parser.add_argument('-b', '--batch-size', type=int, default=1000, help='Batch size for cleanup.')

    def handle(self, verbosity, days, delete_attachments, batch_size, **options):
        # Delete mails and their related logs and queued created before X days
        cutoff_date = now() - datetime.timedelta(days)
        num_emails, num_attachments = cleanup_expired_mails(cutoff_date, delete_attachments, batch_size)
        msg = 'Deleted {0} mails created before {1} and {2} attachments.'
        self.stdout.write(msg.format(num_emails, cutoff_date, num_attachments))
