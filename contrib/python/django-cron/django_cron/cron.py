from django.conf import settings

from django_common.helper import send_mail

from django_cron import CronJobBase, Schedule, get_class
from django_cron.models import CronJobLog


class FailedRunsNotificationCronJob(CronJobBase):
    """
    Send email if cron failed to run X times in a row
    """

    RUN_EVERY_MINS = 30

    schedule = Schedule(run_every_mins=RUN_EVERY_MINS)
    code = 'django_cron.FailedRunsNotificationCronJob'

    def do(self):

        crons_to_check = [get_class(x) for x in settings.CRON_CLASSES]
        emails = [admin[1] for admin in settings.ADMINS]

        failed_runs_cronjob_email_prefix = getattr(
            settings, 'FAILED_RUNS_CRONJOB_EMAIL_PREFIX', ''
        )

        for cron in crons_to_check:

            min_failures = getattr(cron, 'MIN_NUM_FAILURES', 10)
            jobs = CronJobLog.objects.filter(code=cron.code).order_by('-end_time')[
                :min_failures
            ]
            failures = 0
            message = ''

            for job in jobs:
                if not job.is_success:
                    failures += 1
                    message += 'Job ran at %s : \n\n %s \n\n' % (
                        job.start_time,
                        job.message,
                    )

            if failures >= min_failures:
                send_mail(
                    '%s%s failed %s times in a row!'
                    % (
                        failed_runs_cronjob_email_prefix,
                        cron.code,
                        min_failures,
                    ),
                    message,
                    settings.DEFAULT_FROM_EMAIL,
                    emails,
                )
