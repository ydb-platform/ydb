"""Clocked schedule Implementation."""

from celery import schedules
from celery.utils.time import maybe_make_aware
from .utils import NEVER_CHECK_TIMEOUT


class clocked(schedules.BaseSchedule):
    """clocked schedule.

    Depends on PeriodicTask one_off=True
    """

    def __init__(self, clocked_time, nowfun=None, app=None):
        """Initialize clocked."""
        self.clocked_time = maybe_make_aware(clocked_time)
        super().__init__(nowfun=nowfun, app=app)

    def remaining_estimate(self, last_run_at):
        return self.clocked_time - self.now()

    def is_due(self, last_run_at):
        rem_delta = self.remaining_estimate(None)
        remaining_s = max(rem_delta.total_seconds(), 0)
        if remaining_s == 0:
            return schedules.schedstate(is_due=True, next=NEVER_CHECK_TIMEOUT)
        return schedules.schedstate(is_due=False, next=remaining_s)

    def __repr__(self):
        return '<clocked: {}>'.format(self.clocked_time)

    def __eq__(self, other):
        if isinstance(other, clocked):
            return self.clocked_time == other.clocked_time
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __reduce__(self):
        return self.__class__, (self.clocked_time, self.nowfun)
