from cpython.datetime cimport import_datetime, datetime_new

import_datetime()
import datetime

utc = datetime.timezone.utc
epoch = datetime_new(1970, 1, 1, 0, 0, 0, 0, tz=utc)
