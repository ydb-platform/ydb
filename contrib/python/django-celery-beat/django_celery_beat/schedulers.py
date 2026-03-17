"""Beat Scheduler Implementation."""
import datetime
import logging
import math

from multiprocessing.util import Finalize

from celery import current_app
from celery import schedules
from celery.beat import Scheduler, ScheduleEntry

from celery.utils.log import get_logger
from celery.utils.time import maybe_make_aware
from kombu.utils.encoding import safe_str, safe_repr
from kombu.utils.json import dumps, loads

from django.conf import settings
from django.db import transaction, close_old_connections
from django.db.utils import DatabaseError, InterfaceError
from django.core.exceptions import ObjectDoesNotExist

from .models import (
    PeriodicTask, PeriodicTasks,
    CrontabSchedule, IntervalSchedule,
    SolarSchedule, ClockedSchedule
)
from .clockedschedule import clocked
from .utils import NEVER_CHECK_TIMEOUT

# This scheduler must wake up more frequently than the
# regular of 5 minutes because it needs to take external
# changes to the schedule into account.
DEFAULT_MAX_INTERVAL = 5  # seconds

ADD_ENTRY_ERROR = """\
Cannot add entry %r to database schedule: %r. Contents: %r
"""

logger = get_logger(__name__)
debug, info, warning = logger.debug, logger.info, logger.warning


class ModelEntry(ScheduleEntry):
    """Scheduler entry taken from database row."""

    model_schedules = (
        (schedules.crontab, CrontabSchedule, 'crontab'),
        (schedules.schedule, IntervalSchedule, 'interval'),
        (schedules.solar, SolarSchedule, 'solar'),
        (clocked, ClockedSchedule, 'clocked')
    )
    save_fields = ['last_run_at', 'total_run_count', 'no_changes']

    def __init__(self, model, app=None):
        """Initialize the model entry."""
        self.app = app or current_app._get_current_object()
        self.name = model.name
        self.task = model.task
        try:
            self.schedule = model.schedule
        except model.DoesNotExist:
            logger.error(
                'Disabling schedule %s that was removed from database',
                self.name,
            )
            self._disable(model)
        try:
            self.args = loads(model.args or '[]')
            self.kwargs = loads(model.kwargs or '{}')
        except ValueError as exc:
            logger.exception(
                'Removing schedule %s for argument deseralization error: %r',
                self.name, exc,
            )
            self._disable(model)

        self.options = {}
        for option in ['queue', 'exchange', 'routing_key', 'priority']:
            value = getattr(model, option)
            if value is None:
                continue
            self.options[option] = value

        if getattr(model, 'expires_', None):
            self.options['expires'] = getattr(model, 'expires_')

        self.options['headers'] = loads(model.headers or '{}')
        self.options['periodic_task_name'] = model.name

        self.total_run_count = model.total_run_count
        self.model = model

        if not model.last_run_at:
            model.last_run_at = self._default_now()

        self.last_run_at = model.last_run_at

    def _disable(self, model):
        model.no_changes = True
        model.enabled = False
        model.save()

    def is_due(self):
        if not self.model.enabled:
            # 5 second delay for re-enable.
            return schedules.schedstate(False, 5.0)

        # START DATE: only run after the `start_time`, if one exists.
        if self.model.start_time is not None:
            now = self._default_now()
            if getattr(settings, 'DJANGO_CELERY_BEAT_TZ_AWARE', True):
                now = maybe_make_aware(self._default_now())

            if now < self.model.start_time:
                # The datetime is before the start date - don't run.
                # send a delay to retry on start_time
                delay = math.ceil(
                    (self.model.start_time - now).total_seconds()
                )
                return schedules.schedstate(False, delay)

        # ONE OFF TASK: Disable one off tasks after they've ran once
        if self.model.one_off and self.model.enabled \
                and self.model.total_run_count > 0:
            self.model.enabled = False
            self.model.total_run_count = 0  # Reset
            self.model.no_changes = False  # Mark the model entry as changed
            self.model.save()
            # Don't recheck
            return schedules.schedstate(False, NEVER_CHECK_TIMEOUT)

        # CAUTION: make_aware assumes settings.TIME_ZONE for naive datetimes,
        # while maybe_make_aware assumes utc for naive datetimes
        tz = self.app.timezone
        last_run_at_in_tz = maybe_make_aware(self.last_run_at).astimezone(tz)
        return self.schedule.is_due(last_run_at_in_tz)

    def _default_now(self):
        if getattr(settings, 'DJANGO_CELERY_BEAT_TZ_AWARE', True):
            now = datetime.datetime.now(self.app.timezone)
        else:
            # this ends up getting passed to maybe_make_aware, which expects
            # all naive datetime objects to be in utc time.
            now = datetime.datetime.utcnow()
        return now

    def __next__(self):
        self.model.last_run_at = self._default_now()
        self.model.total_run_count += 1
        self.model.no_changes = True
        return self.__class__(self.model)
    next = __next__  # for 2to3

    def save(self):
        # Object may not be synchronized, so only
        # change the fields we care about.
        obj = type(self.model)._default_manager.get(pk=self.model.pk)
        for field in self.save_fields:
            setattr(obj, field, getattr(self.model, field))

        obj.save()

    @classmethod
    def to_model_schedule(cls, schedule):
        for schedule_type, model_type, model_field in cls.model_schedules:
            schedule = schedules.maybe_schedule(schedule)
            if isinstance(schedule, schedule_type):
                model_schedule = model_type.from_schedule(schedule)
                model_schedule.save()
                return model_schedule, model_field
        raise ValueError(
            'Cannot convert schedule type {0!r} to model'.format(schedule))

    @classmethod
    def from_entry(cls, name, app=None, **entry):
        return cls(PeriodicTask._default_manager.update_or_create(
            name=name, defaults=cls._unpack_fields(**entry),
        ), app=app)

    @classmethod
    def _unpack_fields(cls, schedule,
                       args=None, kwargs=None, relative=None, options=None,
                       **entry):
        model_schedule, model_field = cls.to_model_schedule(schedule)
        entry.update(
            {model_field: model_schedule},
            args=dumps(args or []),
            kwargs=dumps(kwargs or {}),
            **cls._unpack_options(**options or {})
        )
        return entry

    @classmethod
    def _unpack_options(cls, queue=None, exchange=None, routing_key=None,
                        priority=None, headers=None, expire_seconds=None,
                        **kwargs):
        return {
            'queue': queue,
            'exchange': exchange,
            'routing_key': routing_key,
            'priority': priority,
            'headers': dumps(headers or {}),
            'expire_seconds': expire_seconds,
        }

    def __repr__(self):
        return '<ModelEntry: {0} {1}(*{2}, **{3}) {4}>'.format(
            safe_str(self.name), self.task, safe_repr(self.args),
            safe_repr(self.kwargs), self.schedule,
        )


class DatabaseScheduler(Scheduler):
    """Database-backed Beat Scheduler."""

    Entry = ModelEntry
    Model = PeriodicTask
    Changes = PeriodicTasks

    _schedule = None
    _last_timestamp = None
    _initial_read = True
    _heap_invalidated = False

    def __init__(self, *args, **kwargs):
        """Initialize the database scheduler."""
        self._dirty = set()
        Scheduler.__init__(self, *args, **kwargs)
        self._finalize = Finalize(self, self.sync, exitpriority=5)
        self.max_interval = (
            kwargs.get('max_interval')
            or self.app.conf.beat_max_loop_interval
            or DEFAULT_MAX_INTERVAL)

    def setup_schedule(self):
        self.install_default_entries(self.schedule)
        self.update_from_dict(self.app.conf.beat_schedule)

    def all_as_schedule(self):
        debug('DatabaseScheduler: Fetching database schedule')
        s = {}
        for model in self.Model.objects.enabled():
            try:
                s[model.name] = self.Entry(model, app=self.app)
            except ValueError:
                pass
        return s

    def schedule_changed(self):
        try:
            close_old_connections()

            # If MySQL is running with transaction isolation level
            # REPEATABLE-READ (default), then we won't see changes done by
            # other transactions until the current transaction is
            # committed (Issue #41).
            try:
                transaction.commit()
            except transaction.TransactionManagementError:
                pass  # not in transaction management.

            last, ts = self._last_timestamp, self.Changes.last_change()
        except DatabaseError as exc:
            logger.exception('Database gave error: %r', exc)
            return False
        except InterfaceError:
            warning(
                'DatabaseScheduler: InterfaceError in schedule_changed(), '
                'waiting to retry in next call...'
            )
            return False

        try:
            if ts and ts > (last if last else ts):
                return True
        finally:
            self._last_timestamp = ts
        return False

    def reserve(self, entry):
        new_entry = next(entry)
        # Need to store entry by name, because the entry may change
        # in the mean time.
        self._dirty.add(new_entry.name)
        return new_entry

    def sync(self):
        if logger.isEnabledFor(logging.DEBUG):
            debug('Writing entries...')
        _tried = set()
        _failed = set()
        try:
            close_old_connections()

            while self._dirty:
                name = self._dirty.pop()
                try:
                    self.schedule[name].save()
                    _tried.add(name)
                except (KeyError, ObjectDoesNotExist):
                    _failed.add(name)
        except DatabaseError as exc:
            logger.exception('Database error while sync: %r', exc)
        except InterfaceError:
            warning(
                'DatabaseScheduler: InterfaceError in sync(), '
                'waiting to retry in next call...'
            )
        finally:
            # retry later, only for the failed ones
            self._dirty |= _failed

    def update_from_dict(self, mapping):
        s = {}
        for name, entry_fields in mapping.items():
            try:
                entry = self.Entry.from_entry(name,
                                              app=self.app,
                                              **entry_fields)
                if entry.model.enabled:
                    s[name] = entry

            except Exception as exc:
                logger.exception(ADD_ENTRY_ERROR, name, exc, entry_fields)
        self.schedule.update(s)

    def install_default_entries(self, data):
        entries = {}
        if self.app.conf.result_expires:
            entries.setdefault(
                'celery.backend_cleanup', {
                    'task': 'celery.backend_cleanup',
                    'schedule': schedules.crontab('0', '4', '*'),
                    'options': {'expire_seconds': 12 * 3600},
                },
            )
        self.update_from_dict(entries)

    def schedules_equal(self, *args, **kwargs):
        if self._heap_invalidated:
            self._heap_invalidated = False
            return False
        return super().schedules_equal(*args, **kwargs)

    @property
    def schedule(self):
        initial = update = False
        if self._initial_read:
            debug('DatabaseScheduler: initial read')
            initial = update = True
            self._initial_read = False
        elif self.schedule_changed():
            info('DatabaseScheduler: Schedule changed.')
            update = True

        if update:
            self.sync()
            self._schedule = self.all_as_schedule()
            # the schedule changed, invalidate the heap in Scheduler.tick
            if not initial:
                self._heap = []
                self._heap_invalidated = True
            if logger.isEnabledFor(logging.DEBUG):
                debug('Current schedule:\n%s', '\n'.join(
                    repr(entry) for entry in self._schedule.values()),
                )
        return self._schedule
