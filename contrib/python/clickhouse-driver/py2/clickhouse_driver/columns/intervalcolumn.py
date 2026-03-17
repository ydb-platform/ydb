from .intcolumn import Int64Column


class IntervalColumn(Int64Column):
    pass


class IntervalDayColumn(IntervalColumn):
    ch_type = 'IntervalDay'


class IntervalWeekColumn(IntervalColumn):
    ch_type = 'IntervalWeek'


class IntervalMonthColumn(IntervalColumn):
    ch_type = 'IntervalMonth'


class IntervalYearColumn(IntervalColumn):
    ch_type = 'IntervalYear'


class IntervalHourColumn(IntervalColumn):
    ch_type = 'IntervalHour'


class IntervalMinuteColumn(IntervalColumn):
    ch_type = 'IntervalMinute'


class IntervalSecondColumn(IntervalColumn):
    ch_type = 'IntervalSecond'
