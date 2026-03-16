import threading
from enum import Enum


class Reservoir:
    """
    Centralized thread-safe reservoir which holds fixed sampling
    quota, borrowed count and TTL.
    """
    def __init__(self):
        self._lock = threading.Lock()

        self._quota = None
        self._TTL = None

        self._this_sec = 0
        self._taken_this_sec = 0
        self._borrowed_this_sec = 0

        self._report_interval = 1
        self._report_elapsed = 0

    def borrow_or_take(self, now, can_borrow):
        """
        Decide whether to borrow or take one quota from
        the reservoir. Return ``False`` if it can neither
        borrow nor take. This method is thread-safe.
        """
        with self._lock:
            return self._borrow_or_take(now, can_borrow)

    def load_quota(self, quota, TTL, interval):
        """
        Load new quota with a TTL. If the input is None,
        the reservoir will continue using old quota until it
        expires or has a non-None quota/TTL in a future load.
        """
        if quota is not None:
            self._quota = quota
        if TTL is not None:
            self._TTL = TTL
        if interval is not None:
            self._report_interval = interval / 10

    @property
    def quota(self):
        return self._quota

    @property
    def TTL(self):
        return self._TTL

    def _time_to_report(self):
        if self._report_elapsed + 1 >= self._report_interval:
            self._report_elapsed = 0
            return True
        else:
            self._report_elapsed += 1

    def _borrow_or_take(self, now, can_borrow):
        self._adjust_this_sec(now)
        # Don't borrow if the quota is available and fresh.
        if (self._quota is not None and self._quota >= 0 and
                self._TTL is not None and self._TTL >= now):
            if(self._taken_this_sec >= self._quota):
                return ReservoirDecision.NO

            self._taken_this_sec = self._taken_this_sec + 1
            return ReservoirDecision.TAKE

        # Otherwise try to borrow if the quota is not present or expired.
        if can_borrow:
            if self._borrowed_this_sec >= 1:
                return ReservoirDecision.NO

            self._borrowed_this_sec = self._borrowed_this_sec + 1
            return ReservoirDecision.BORROW

    def _adjust_this_sec(self, now):
        if now != self._this_sec:
            self._taken_this_sec = 0
            self._borrowed_this_sec = 0
            self._this_sec = now


class ReservoirDecision(Enum):
    """
    An Enum of decisions the reservoir could make based on
    assigned quota with TTL and the current timestamp/usage.
    """
    TAKE = 'take'
    BORROW = 'borrow'
    NO = 'no'
