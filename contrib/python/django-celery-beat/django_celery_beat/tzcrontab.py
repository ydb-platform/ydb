"""Timezone aware Cron schedule Implementation."""
from celery import schedules

from collections import namedtuple
from datetime import datetime, timezone


schedstate = namedtuple('schedstate', ('is_due', 'next'))


class TzAwareCrontab(schedules.crontab):
    """Timezone Aware Crontab."""

    def __init__(
            self, minute='*', hour='*', day_of_week='*',
            day_of_month='*', month_of_year='*', tz=timezone.utc, app=None
    ):
        """Overwrite Crontab constructor to include a timezone argument."""
        self.tz = tz

        nowfun = self.nowfunc

        super().__init__(
            minute=minute, hour=hour, day_of_week=day_of_week,
            day_of_month=day_of_month,
            month_of_year=month_of_year, nowfun=nowfun, app=app
        )

    def nowfunc(self):
        return datetime.now(self.tz)

    def is_due(self, last_run_at):
        """Calculate when the next run will take place.

        Return tuple of ``(is_due, next_time_to_check)``.
        The ``last_run_at`` argument needs to be timezone aware.

        """
        # convert last_run_at to the schedule timezone
        last_run_at = last_run_at.astimezone(self.tz)

        rem_delta = self.remaining_estimate(last_run_at)
        rem = max(rem_delta.total_seconds(), 0)
        due = rem == 0
        if due:
            rem_delta = self.remaining_estimate(self.now())
            rem = max(rem_delta.total_seconds(), 0)
        return schedstate(due, rem)

    # Needed to support pickling
    def __repr__(self):
        return """<crontab: {0._orig_minute} {0._orig_hour}
         {0._orig_day_of_week} {0._orig_day_of_month}
          {0._orig_month_of_year} (m/h/d/dM/MY), {0.tz}>
        """.format(self)

    def __reduce__(self):
        return (self.__class__, (self._orig_minute,
                                 self._orig_hour,
                                 self._orig_day_of_week,
                                 self._orig_day_of_month,
                                 self._orig_month_of_year,
                                 self.tz), None)

    def __eq__(self, other):
        if isinstance(other, schedules.crontab):
            return (other.month_of_year == self.month_of_year
                    and other.day_of_month == self.day_of_month
                    and other.day_of_week == self.day_of_week
                    and other.hour == self.hour
                    and other.minute == self.minute
                    and other.tz == self.tz)
        return NotImplemented
