from ..utils import str_coercible
from .weekday import WeekDay


@str_coercible
class WeekDays:
    def __init__(self, bit_string_or_week_days):
        if isinstance(bit_string_or_week_days, str):
            self._days = set()

            if len(bit_string_or_week_days) != WeekDay.NUM_WEEK_DAYS:
                raise ValueError(
                    'Bit string must be {} characters long.'.format(
                        WeekDay.NUM_WEEK_DAYS
                    )
                )

            for index, bit in enumerate(bit_string_or_week_days):
                if bit not in '01':
                    raise ValueError(
                        'Bit string may only contain zeroes and ones.'
                    )
                if bit == '1':
                    self._days.add(WeekDay(index))
        elif isinstance(bit_string_or_week_days, WeekDays):
            self._days = bit_string_or_week_days._days
        else:
            self._days = set(bit_string_or_week_days)

    def __eq__(self, other):
        if isinstance(other, WeekDays):
            return self._days == other._days
        elif isinstance(other, str):
            return self.as_bit_string() == other
        else:
            return NotImplemented

    def __iter__(self):
        yield from sorted(self._days)

    def __contains__(self, value):
        return value in self._days

    def __repr__(self):
        return '{}({!r})'.format(
            self.__class__.__name__,
            self.as_bit_string()
        )

    def __unicode__(self):
        return ', '.join(str(day) for day in self)

    def as_bit_string(self):
        return ''.join(
            '1' if WeekDay(index) in self._days else '0'
            for index in range(WeekDay.NUM_WEEK_DAYS)
        )
