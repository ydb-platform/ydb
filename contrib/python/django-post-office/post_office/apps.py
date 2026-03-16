from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class PostOfficeConfig(AppConfig):
    name = 'post_office'
    verbose_name = _('Post Office')
    default_auto_field = 'django.db.models.AutoField'

    def ready(self):
        from post_office import tasks
        from post_office.settings import get_celery_enabled
        from post_office.signals import email_queued

        if get_celery_enabled():
            email_queued.connect(tasks.queued_mail_handler)
