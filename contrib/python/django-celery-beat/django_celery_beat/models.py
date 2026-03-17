"""Database models."""
try:
    from zoneinfo import available_timezones
except ImportError:
    from backports.zoneinfo import available_timezones
from datetime import timedelta

import timezone_field
from celery import schedules, current_app
from django.conf import settings
from django.core.exceptions import MultipleObjectsReturned, ValidationError
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.db.models import signals
from django.utils.translation import gettext_lazy as _

from . import managers, validators
from .tzcrontab import TzAwareCrontab
from .utils import make_aware, now
from .clockedschedule import clocked


DAYS = 'days'
HOURS = 'hours'
MINUTES = 'minutes'
SECONDS = 'seconds'
MICROSECONDS = 'microseconds'

PERIOD_CHOICES = (
    (DAYS, _('Days')),
    (HOURS, _('Hours')),
    (MINUTES, _('Minutes')),
    (SECONDS, _('Seconds')),
    (MICROSECONDS, _('Microseconds')),
)

SINGULAR_PERIODS = (
    (DAYS, _('Day')),
    (HOURS, _('Hour')),
    (MINUTES, _('Minute')),
    (SECONDS, _('Second')),
    (MICROSECONDS, _('Microsecond')),
)

SOLAR_SCHEDULES = [
    ("dawn_astronomical", _("Astronomical dawn")),
    ("dawn_civil", _("Civil dawn")),
    ("dawn_nautical", _("Nautical dawn")),
    ("dusk_astronomical", _("Astronomical dusk")),
    ("dusk_civil", _("Civil dusk")),
    ("dusk_nautical", _("Nautical dusk")),
    ("solar_noon", _("Solar noon")),
    ("sunrise", _("Sunrise")),
    ("sunset", _("Sunset")),
]


def cronexp(field):
    """Representation of cron expression."""
    return field and str(field).replace(' ', '') or '*'


def crontab_schedule_celery_timezone():
    """Return timezone string from Django settings ``CELERY_TIMEZONE`` variable.

    If is not defined or is not a valid timezone, return ``"UTC"`` instead.
    """
    try:
        CELERY_TIMEZONE = getattr(
            settings, '%s_TIMEZONE' % current_app.namespace)
    except AttributeError:
        return 'UTC'
    if CELERY_TIMEZONE in available_timezones():
        return CELERY_TIMEZONE
    return 'UTC'


class SolarSchedule(models.Model):
    """Schedule following astronomical patterns.

    Example: to run every sunrise in New York City:

    >>> event='sunrise', latitude=40.7128, longitude=74.0060
    """

    event = models.CharField(
        max_length=24, choices=SOLAR_SCHEDULES,
        verbose_name=_('Solar Event'),
        help_text=_('The type of solar event when the job should run'),
    )
    latitude = models.DecimalField(
        max_digits=9, decimal_places=6,
        verbose_name=_('Latitude'),
        help_text=_('Run the task when the event happens at this latitude'),
        validators=[MinValueValidator(-90), MaxValueValidator(90)],
    )
    longitude = models.DecimalField(
        max_digits=9, decimal_places=6,
        verbose_name=_('Longitude'),
        help_text=_('Run the task when the event happens at this longitude'),
        validators=[MinValueValidator(-180), MaxValueValidator(180)],
    )

    class Meta:
        """Table information."""

        verbose_name = _('solar event')
        verbose_name_plural = _('solar events')
        ordering = ('event', 'latitude', 'longitude')
        unique_together = ('event', 'latitude', 'longitude')

    @property
    def schedule(self):
        return schedules.solar(self.event,
                               self.latitude,
                               self.longitude,
                               nowfun=lambda: make_aware(now()))

    @classmethod
    def from_schedule(cls, schedule):
        spec = {'event': schedule.event,
                'latitude': schedule.lat,
                'longitude': schedule.lon}

        # we do not check for MultipleObjectsReturned exception here because
        # the unique_together constraint safely prevents from duplicates
        try:
            return cls.objects.get(**spec)
        except cls.DoesNotExist:
            return cls(**spec)

    def __str__(self):
        return '{0} ({1}, {2})'.format(
            self.get_event_display(),
            self.latitude,
            self.longitude
        )


class IntervalSchedule(models.Model):
    """Schedule executing on a regular interval.

    Example: execute every 2 days:

    >>> every=2, period=DAYS
    """

    DAYS = DAYS
    HOURS = HOURS
    MINUTES = MINUTES
    SECONDS = SECONDS
    MICROSECONDS = MICROSECONDS

    PERIOD_CHOICES = PERIOD_CHOICES

    every = models.IntegerField(
        null=False,
        verbose_name=_('Number of Periods'),
        help_text=_('Number of interval periods to wait before '
                    'running the task again'),
        validators=[MinValueValidator(1)],
    )
    period = models.CharField(
        max_length=24, choices=PERIOD_CHOICES,
        verbose_name=_('Interval Period'),
        help_text=_('The type of period between task runs (Example: days)'),
    )

    class Meta:
        """Table information."""

        verbose_name = _('interval')
        verbose_name_plural = _('intervals')
        ordering = ['period', 'every']

    @property
    def schedule(self):
        return schedules.schedule(
            timedelta(**{self.period: self.every}),
            nowfun=lambda: make_aware(now())
        )

    @classmethod
    def from_schedule(cls, schedule, period=SECONDS):
        every = max(schedule.run_every.total_seconds(), 0)
        try:
            return cls.objects.get(every=every, period=period)
        except cls.DoesNotExist:
            return cls(every=every, period=period)
        except MultipleObjectsReturned:
            return cls.objects.filter(every=every, period=period).first()

    def __str__(self):
        readable_period = None
        if self.every == 1:
            for period, _readable_period in SINGULAR_PERIODS:
                if period == self.period:
                    readable_period = _readable_period.lower()
                    break
            return _('every {}').format(readable_period)
        for period, _readable_period in PERIOD_CHOICES:
            if period == self.period:
                readable_period = _readable_period.lower()
                break
        return _('every {} {}').format(self.every, readable_period)

    @property
    def period_singular(self):
        return self.period[:-1]


class ClockedSchedule(models.Model):
    """clocked schedule."""

    clocked_time = models.DateTimeField(
        verbose_name=_('Clock Time'),
        help_text=_('Run the task at clocked time'),
    )

    class Meta:
        """Table information."""

        verbose_name = _('clocked')
        verbose_name_plural = _('clocked')
        ordering = ['clocked_time']

    def __str__(self):
        return '{}'.format(make_aware(self.clocked_time))

    @property
    def schedule(self):
        c = clocked(clocked_time=self.clocked_time)
        return c

    @classmethod
    def from_schedule(cls, schedule):
        spec = {'clocked_time': schedule.clocked_time}
        try:
            return cls.objects.get(**spec)
        except cls.DoesNotExist:
            return cls(**spec)
        except MultipleObjectsReturned:
            return cls.objects.filter(**spec).first()


class CrontabSchedule(models.Model):
    """Timezone Aware Crontab-like schedule.

    Example:  Run every hour at 0 minutes for days of month 10-15:

    >>> minute="0", hour="*", day_of_week="*",
    ... day_of_month="10-15", month_of_year="*"
    """

    #
    # The worst case scenario for day of month is a list of all 31 day numbers
    # '[1, 2, ..., 31]' which has a length of 115. Likewise, minute can be
    # 0..59 and hour can be 0..23. Ensure we can accomodate these by allowing
    # 4 chars for each value (what we save on 0-9 accomodates the []).
    # We leave the other fields at their historical length.
    #
    minute = models.CharField(
        max_length=60 * 4, default='*',
        verbose_name=_('Minute(s)'),
        help_text=_(
            'Cron Minutes to Run. Use "*" for "all". (Example: "0,30")'),
        validators=[validators.minute_validator],
    )
    hour = models.CharField(
        max_length=24 * 4, default='*',
        verbose_name=_('Hour(s)'),
        help_text=_(
            'Cron Hours to Run. Use "*" for "all". (Example: "8,20")'),
        validators=[validators.hour_validator],
    )
    day_of_week = models.CharField(
        max_length=64, default='*',
        verbose_name=_('Day(s) Of The Week'),
        help_text=_(
            'Cron Days Of The Week to Run. Use "*" for "all". '
            '(Example: "0,5")'),
        validators=[validators.day_of_week_validator],
    )
    day_of_month = models.CharField(
        max_length=31 * 4, default='*',
        verbose_name=_('Day(s) Of The Month'),
        help_text=_(
            'Cron Days Of The Month to Run. Use "*" for "all". '
            '(Example: "1,15")'),
        validators=[validators.day_of_month_validator],
    )
    month_of_year = models.CharField(
        max_length=64, default='*',
        verbose_name=_('Month(s) Of The Year'),
        help_text=_(
            'Cron Months Of The Year to Run. Use "*" for "all". '
            '(Example: "0,6")'),
        validators=[validators.month_of_year_validator],
    )

    timezone = timezone_field.TimeZoneField(
        default=crontab_schedule_celery_timezone,
        use_pytz=False,
        verbose_name=_('Cron Timezone'),
        help_text=_(
            'Timezone to Run the Cron Schedule on. Default is UTC.'),
    )

    class Meta:
        """Table information."""

        verbose_name = _('crontab')
        verbose_name_plural = _('crontabs')
        ordering = ['month_of_year', 'day_of_month',
                    'day_of_week', 'hour', 'minute', 'timezone']

    def __str__(self):
        return '{0} {1} {2} {3} {4} (m/h/dM/MY/d) {5}'.format(
            cronexp(self.minute), cronexp(self.hour),
            cronexp(self.day_of_month), cronexp(self.month_of_year),
            cronexp(self.day_of_week), str(self.timezone)
        )

    @property
    def schedule(self):
        crontab = schedules.crontab(
            minute=self.minute,
            hour=self.hour,
            day_of_week=self.day_of_week,
            day_of_month=self.day_of_month,
            month_of_year=self.month_of_year,
        )
        if getattr(settings, 'DJANGO_CELERY_BEAT_TZ_AWARE', True):
            crontab = TzAwareCrontab(
                minute=self.minute,
                hour=self.hour,
                day_of_week=self.day_of_week,
                day_of_month=self.day_of_month,
                month_of_year=self.month_of_year,
                tz=self.timezone
            )
        return crontab

    @classmethod
    def from_schedule(cls, schedule):
        spec = {'minute': schedule._orig_minute,
                'hour': schedule._orig_hour,
                'day_of_week': schedule._orig_day_of_week,
                'day_of_month': schedule._orig_day_of_month,
                'month_of_year': schedule._orig_month_of_year,
                'timezone': schedule.tz
                }
        try:
            return cls.objects.get(**spec)
        except cls.DoesNotExist:
            return cls(**spec)
        except MultipleObjectsReturned:
            return cls.objects.filter(**spec).first()


class PeriodicTasks(models.Model):
    """Helper table for tracking updates to periodic tasks.

    This stores a single row with ``ident=1``. ``last_update`` is updated via
    signals whenever anything changes in the :class:`~.PeriodicTask` model.
    Basically this acts like a DB data audit trigger.
    Doing this so we also track deletions, and not just insert/update.
    """

    ident = models.SmallIntegerField(default=1, primary_key=True, unique=True)
    last_update = models.DateTimeField(null=False)

    objects = managers.ExtendedManager()

    @classmethod
    def changed(cls, instance, **kwargs):
        if not instance.no_changes:
            cls.update_changed()

    @classmethod
    def update_changed(cls, **kwargs):
        cls.objects.update_or_create(ident=1, defaults={'last_update': now()})

    @classmethod
    def last_change(cls):
        try:
            return cls.objects.get(ident=1).last_update
        except cls.DoesNotExist:
            pass


class PeriodicTask(models.Model):
    """Model representing a periodic task."""

    name = models.CharField(
        max_length=200, unique=True,
        verbose_name=_('Name'),
        help_text=_('Short Description For This Task'),
    )
    task = models.CharField(
        max_length=200,
        verbose_name='Task Name',
        help_text=_('The Name of the Celery Task that Should be Run.  '
                    '(Example: "proj.tasks.import_contacts")'),
    )

    # You can only set ONE of the following schedule FK's
    # TODO: Redo this as a GenericForeignKey
    interval = models.ForeignKey(
        IntervalSchedule, on_delete=models.CASCADE,
        null=True, blank=True, verbose_name=_('Interval Schedule'),
        help_text=_('Interval Schedule to run the task on.  '
                    'Set only one schedule type, leave the others null.'),
    )
    crontab = models.ForeignKey(
        CrontabSchedule, on_delete=models.CASCADE, null=True, blank=True,
        verbose_name=_('Crontab Schedule'),
        help_text=_('Crontab Schedule to run the task on.  '
                    'Set only one schedule type, leave the others null.'),
    )
    solar = models.ForeignKey(
        SolarSchedule, on_delete=models.CASCADE, null=True, blank=True,
        verbose_name=_('Solar Schedule'),
        help_text=_('Solar Schedule to run the task on.  '
                    'Set only one schedule type, leave the others null.'),
    )
    clocked = models.ForeignKey(
        ClockedSchedule, on_delete=models.CASCADE, null=True, blank=True,
        verbose_name=_('Clocked Schedule'),
        help_text=_('Clocked Schedule to run the task on.  '
                    'Set only one schedule type, leave the others null.'),
    )
    # TODO: use django's JsonField
    args = models.TextField(
        blank=True, default='[]',
        verbose_name=_('Positional Arguments'),
        help_text=_(
            'JSON encoded positional arguments '
            '(Example: ["arg1", "arg2"])'),
    )
    kwargs = models.TextField(
        blank=True, default='{}',
        verbose_name=_('Keyword Arguments'),
        help_text=_(
            'JSON encoded keyword arguments '
            '(Example: {"argument": "value"})'),
    )

    queue = models.CharField(
        max_length=200, blank=True, null=True, default=None,
        verbose_name=_('Queue Override'),
        help_text=_(
            'Queue defined in CELERY_TASK_QUEUES. '
            'Leave None for default queuing.'),
    )

    # you can use low-level AMQP routing options here,
    # but you almost certaily want to leave these as None
    # http://docs.celeryproject.org/en/latest/userguide/routing.html#exchanges-queues-and-routing-keys
    exchange = models.CharField(
        max_length=200, blank=True, null=True, default=None,
        verbose_name=_('Exchange'),
        help_text=_('Override Exchange for low-level AMQP routing'),
    )
    routing_key = models.CharField(
        max_length=200, blank=True, null=True, default=None,
        verbose_name=_('Routing Key'),
        help_text=_('Override Routing Key for low-level AMQP routing'),
    )
    headers = models.TextField(
        blank=True, default='{}',
        verbose_name=_('AMQP Message Headers'),
        help_text=_('JSON encoded message headers for the AMQP message.'),
    )

    priority = models.PositiveIntegerField(
        default=None, validators=[MaxValueValidator(255)],
        blank=True, null=True,
        verbose_name=_('Priority'),
        help_text=_(
            'Priority Number between 0 and 255. '
            'Supported by: RabbitMQ, Redis (priority reversed, 0 is highest).')
    )
    expires = models.DateTimeField(
        blank=True, null=True,
        verbose_name=_('Expires Datetime'),
        help_text=_(
            'Datetime after which the schedule will no longer '
            'trigger the task to run'),
    )
    expire_seconds = models.PositiveIntegerField(
        blank=True, null=True,
        verbose_name=_('Expires timedelta with seconds'),
        help_text=_(
            'Timedelta with seconds which the schedule will no longer '
            'trigger the task to run'),

    )
    one_off = models.BooleanField(
        default=False,
        verbose_name=_('One-off Task'),
        help_text=_(
            'If True, the schedule will only run the task a single time'),
    )
    start_time = models.DateTimeField(
        blank=True, null=True,
        verbose_name=_('Start Datetime'),
        help_text=_(
            'Datetime when the schedule should begin '
            'triggering the task to run'),
    )
    enabled = models.BooleanField(
        default=True,
        verbose_name=_('Enabled'),
        help_text=_('Set to False to disable the schedule'),
    )
    last_run_at = models.DateTimeField(
        auto_now=False, auto_now_add=False,
        editable=False, blank=True, null=True,
        verbose_name=_('Last Run Datetime'),
        help_text=_(
            'Datetime that the schedule last triggered the task to run. '
            'Reset to None if enabled is set to False.'),
    )
    total_run_count = models.PositiveIntegerField(
        default=0, editable=False,
        verbose_name=_('Total Run Count'),
        help_text=_(
            'Running count of how many times the schedule '
            'has triggered the task'),
    )
    date_changed = models.DateTimeField(
        auto_now=True,
        verbose_name=_('Last Modified'),
        help_text=_('Datetime that this PeriodicTask was last modified'),
    )
    description = models.TextField(
        blank=True,
        verbose_name=_('Description'),
        help_text=_(
            'Detailed description about the details of this Periodic Task'),
    )

    objects = managers.PeriodicTaskManager()
    no_changes = False

    class Meta:
        """Table information."""

        verbose_name = _('periodic task')
        verbose_name_plural = _('periodic tasks')

    def validate_unique(self, *args, **kwargs):
        super().validate_unique(*args, **kwargs)

        schedule_types = ['interval', 'crontab', 'solar', 'clocked']
        selected_schedule_types = [s for s in schedule_types
                                   if getattr(self, s)]

        if len(selected_schedule_types) == 0:
            raise ValidationError(
                'One of clocked, interval, crontab, or solar '
                'must be set.'
            )

        err_msg = 'Only one of clocked, interval, crontab, '\
            'or solar must be set'
        if len(selected_schedule_types) > 1:
            error_info = {}
            for selected_schedule_type in selected_schedule_types:
                error_info[selected_schedule_type] = [err_msg]
            raise ValidationError(error_info)

        # clocked must be one off task
        if self.clocked and not self.one_off:
            err_msg = 'clocked must be one off, one_off must set True'
            raise ValidationError(err_msg)

    def save(self, *args, **kwargs):
        self.exchange = self.exchange or None
        self.routing_key = self.routing_key or None
        self.queue = self.queue or None
        self.headers = self.headers or None
        if not self.enabled:
            self.last_run_at = None
        self._clean_expires()
        self.validate_unique()
        super().save(*args, **kwargs)

    def _clean_expires(self):
        if self.expire_seconds is not None and self.expires:
            raise ValidationError(
                _('Only one can be set, in expires and expire_seconds')
            )

    @property
    def expires_(self):
        return self.expires or self.expire_seconds

    def __str__(self):
        fmt = '{0.name}: {{no schedule}}'
        if self.interval:
            fmt = '{0.name}: {0.interval}'
        if self.crontab:
            fmt = '{0.name}: {0.crontab}'
        if self.solar:
            fmt = '{0.name}: {0.solar}'
        if self.clocked:
            fmt = '{0.name}: {0.clocked}'
        return fmt.format(self)

    @property
    def schedule(self):
        if self.interval:
            return self.interval.schedule
        if self.crontab:
            return self.crontab.schedule
        if self.solar:
            return self.solar.schedule
        if self.clocked:
            return self.clocked.schedule


signals.pre_delete.connect(PeriodicTasks.changed, sender=PeriodicTask)
signals.pre_save.connect(PeriodicTasks.changed, sender=PeriodicTask)
signals.pre_delete.connect(
    PeriodicTasks.update_changed, sender=IntervalSchedule)
signals.post_save.connect(
    PeriodicTasks.update_changed, sender=IntervalSchedule)
signals.post_delete.connect(
    PeriodicTasks.update_changed, sender=CrontabSchedule)
signals.post_save.connect(
    PeriodicTasks.update_changed, sender=CrontabSchedule)
signals.post_delete.connect(
    PeriodicTasks.update_changed, sender=SolarSchedule)
signals.post_save.connect(
    PeriodicTasks.update_changed, sender=SolarSchedule)
signals.post_delete.connect(
    PeriodicTasks.update_changed, sender=ClockedSchedule)
signals.post_save.connect(
    PeriodicTasks.update_changed, sender=ClockedSchedule)
