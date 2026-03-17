import datetime


def load_date_bound(interval_days: int = 30):
    def make_end_date(s: datetime, e: datetime, i: int):
        end_date = s + datetime.timedelta(days=i)
        if end_date > e:
            return e
        return end_date

    def parse_if_needed(dt: datetime or str):
        if isinstance(dt, datetime.datetime):
            return dt
        return datetime.datetime.fromisoformat(dt)

    date_range = {}

    def decorator(function):
        def wrapper(*args, **kwargs):
            date_range.update({
                'dataStartTime': parse_if_needed(kwargs['dataStartTime']),
                'dataEndTime': parse_if_needed(kwargs.get('dataEndTime', datetime.datetime.utcnow()))
            })
            kwargs.update({
                'dataEndTime': make_end_date(date_range['dataStartTime'], date_range['dataEndTime'], interval_days)
            })
            while kwargs['dataStartTime'] < kwargs['dataEndTime']:
                yield function(*args, **kwargs)
                kwargs.update({
                    'dataStartTime': parse_if_needed(kwargs['dataEndTime']),
                    'dataEndTime': make_end_date(parse_if_needed(kwargs['dataEndTime']), date_range['dataEndTime'],
                                                 interval_days)
                })

        wrapper.__doc__ = function.__doc__
        return wrapper

    return decorator
