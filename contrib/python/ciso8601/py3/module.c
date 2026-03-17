#include <Python.h>
#include <ctype.h>
#include <datetime.h>

#include "isocalendar.h"
#include "timezone.h"

#define STRINGIZE(x)            #x
#define EXPAND_AND_STRINGIZE(x) STRINGIZE(x)

#define PY_VERSION_AT_LEAST_33 \
    ((PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 3) || PY_MAJOR_VERSION > 3)
#define PY_VERSION_AT_LEAST_36 \
    ((PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 6) || PY_MAJOR_VERSION > 3)
#define PY_VERSION_AT_LEAST_37 \
    ((PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 7) || PY_MAJOR_VERSION > 3)
#define PY_VERSION_AT_LEAST_38 \
    ((PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 8) || PY_MAJOR_VERSION > 3)

/* PyPy compatibility for cPython 3.7's Timezone API was added to PyPy 7.3.6
 * https://foss.heptapod.net/pypy/pypy/-/merge_requests/826
 * But was then reverted in 7.3.7 for PyPy 3.7:
 * https://foss.heptapod.net/pypy/pypy/-/commit/eeeafcf905afa0f26049ac29dc00f5b295171f99
 * It is still present in 7.3.7 for PyPy 3.8+
 */
#ifdef PYPY_VERSION
#define SUPPORTS_37_TIMEZONE_API \
    (PYPY_VERSION_NUM >= 0x07030600) && PY_VERSION_AT_LEAST_38
#else
#define SUPPORTS_37_TIMEZONE_API PY_VERSION_AT_LEAST_37
#endif

static PyObject *utc;

#if CISO8601_CACHING_ENABLED
/* 2879 = (1439 * 2) + 1, number of offsets from UTC possible in
 * Python (i.e., [-1439, 1439]).
 *
 * 0 - 1438 = Negative offsets [-1439..-1]
 * 1439 = Zero offset
 * 1440 - 2878 = Positive offsets [1...1439]
 */
static PyObject *tz_cache[2879] = {NULL};
#endif

#define PARSE_INTEGER(field, length, field_name)                      \
    for (i = 0; i < length; i++) {                                    \
        if (*c >= '0' && *c <= '9') {                                 \
            field = 10 * field + *c++ - '0';                          \
        }                                                             \
        else {                                                        \
            return format_unexpected_character_exception(             \
                field_name, c, (c - str) / sizeof(char), length - i); \
        }                                                             \
    }

#define PARSE_FRACTIONAL_SECOND()                             \
    for (i = 0; i < 6; i++) {                                 \
        if (*c >= '0' && *c <= '9') {                         \
            usecond = 10 * usecond + *c++ - '0';              \
        }                                                     \
        else if (i == 0) {                                    \
            /* We need at least one digit. */                 \
            /* Trailing '.' or ',' is not allowed */          \
            return format_unexpected_character_exception(     \
                "subsecond", c, (c - str) / sizeof(char), 1); \
        }                                                     \
        else                                                  \
            break;                                            \
    }                                                         \
                                                              \
    /* Omit excessive digits */                               \
    while (*c >= '0' && *c <= '9') c++;                       \
                                                              \
    /* If we break early, fully expand the usecond */         \
    while (i++ < 6) usecond *= 10;

#if PY_VERSION_AT_LEAST_33
#define PARSE_SEPARATOR(separator, field_name)                                \
    if (separator) {                                                          \
        c++;                                                                  \
    }                                                                         \
    else {                                                                    \
        PyObject *unicode_str = PyUnicode_FromString(c);                      \
        PyObject *unicode_char = PyUnicode_Substring(unicode_str, 0, 1);      \
        PyErr_Format(PyExc_ValueError,                                        \
                     "Invalid character while parsing %s ('%U', Index: %lu)", \
                     field_name, unicode_char, (c - str) / sizeof(char));     \
        Py_DECREF(unicode_str);                                               \
        Py_DECREF(unicode_char);                                              \
        return NULL;                                                          \
    }
#else
#define PARSE_SEPARATOR(separator, field_name)                              \
    if (separator) {                                                        \
        c++;                                                                \
    }                                                                       \
    else {                                                                  \
        if (isascii((int)*c)) {                                             \
            PyErr_Format(                                                   \
                PyExc_ValueError,                                           \
                "Invalid character while parsing %s ('%c', Index: %lu)",    \
                field_name, *c, (c - str) / sizeof(char));                  \
        }                                                                   \
        else {                                                              \
            PyErr_Format(PyExc_ValueError,                                  \
                         "Invalid character while parsing %s (Index: %lu)", \
                         field_name, (c - str) / sizeof(char));             \
        }                                                                   \
        return NULL;                                                        \
    }
#endif

static void *
format_unexpected_character_exception(char *field_name, const char *c,
                                      size_t index,
                                      int expected_character_count)
{
    if (*c == '\0') {
        PyErr_Format(
            PyExc_ValueError,
            "Unexpected end of string while parsing %s. Expected %d more "
            "character%s",
            field_name, expected_character_count,
            (expected_character_count != 1) ? "s" : "");
    }
    else if (*c == '-' && index == 0 && strcmp(field_name, "year") == 0) {
        PyErr_Format(
            PyExc_ValueError,
            "Invalid character while parsing %s ('-', Index: 0). "
            "While valid ISO 8601 years, BCE years are not supported by "
            "Python's `datetime` objects.",
            field_name);
    }
    else {
#if PY_VERSION_AT_LEAST_33
        PyObject *unicode_str = PyUnicode_FromString(c);
        PyObject *unicode_char = PyUnicode_Substring(unicode_str, 0, 1);
        PyErr_Format(PyExc_ValueError,
                     "Invalid character while parsing %s ('%U', Index: %zu)",
                     field_name, unicode_char, index);
        Py_DECREF(unicode_str);
        Py_DECREF(unicode_char);
#else
        if (isascii((int)*c)) {
            PyErr_Format(
                PyExc_ValueError,
                "Invalid character while parsing %s ('%c', Index: %zu)",
                field_name, *c, index);
        }
        else {
            PyErr_Format(PyExc_ValueError,
                         "Invalid character while parsing %s (Index: %zu)",
                         field_name, index);
        }
#endif
    }
    return NULL;
}

#define IS_CALENDAR_DATE_SEPARATOR (*c == '-')
#define IS_ISOCALENDAR_SEPARATOR   (*c == 'W')
#define IS_DATE_AND_TIME_SEPARATOR (*c == 'T' || *c == ' ' || *c == 't')
#define IS_TIME_SEPARATOR          (*c == ':')
#define IS_TIME_ZONE_SEPARATOR \
    (*c == 'Z' || *c == '-' || *c == '+' || *c == 'z')
#define IS_FRACTIONAL_SEPARATOR (*c == '.' || (*c == ',' && !rfc3339_only))

static PyObject *
_parse(PyObject *self, PyObject *dtstr, int parse_any_tzinfo, int rfc3339_only)
{
    PyObject *obj;
    PyObject *tzinfo = Py_None;
#if PY_VERSION_AT_LEAST_33
    Py_ssize_t len;
#endif

    int i;
    const char *str;
    const char *c;
    int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0,
        usecond = 0;
    int iso_week = 0, iso_day = 0;
    int ordinal_day = 0;
    int time_is_midnight = 0;
    int tzhour = 0, tzminute = 0, tzsign = 0;
#if CISO8601_CACHING_ENABLED
    int tz_index = 0;
#endif
    PyObject *delta;
    PyObject *temp;
    int extended_date_format = 0;

#if PY_MAJOR_VERSION >= 3
    if (!PyUnicode_Check(dtstr)) {
#else
    if (!PyString_Check(dtstr)) {
#endif
        PyErr_SetString(PyExc_TypeError, "argument must be str");
        return NULL;
    }

#if PY_VERSION_AT_LEAST_33
    str = c = PyUnicode_AsUTF8AndSize(dtstr, &len);
#else
    str = c = PyString_AsString(dtstr);
#endif

    /* Year */
    PARSE_INTEGER(year, 4, "year")

#if !PY_VERSION_AT_LEAST_36
    /* Python 3.6+ does this validation as part of datetime's C API
     * constructor. See
     * https://github.com/python/cpython/commit/b67f0967386a9c9041166d2bbe0a421bd81e10bc
     * We skip ` || year < datetime.MAXYEAR)`, since ciso8601 currently doesn't
     * support 5 character years, so it is impossible.
     */
    if (year <
        1) /* datetime.MINYEAR = 1, which is not exposed to the C API. */
    {
        PyErr_Format(PyExc_ValueError, "year %d is out of range", year);
        return NULL;
    }
#endif

    if (IS_CALENDAR_DATE_SEPARATOR) {
        c++;
        extended_date_format = 1;

        if (IS_ISOCALENDAR_SEPARATOR) { /* Separated ISO Calendar week and day
                                           (i.e., Www-D) */
            c++;

            if (rfc3339_only) {
                PyErr_SetString(PyExc_ValueError,
                                "Datetime string not in RFC 3339 format.");
                return NULL;
            }

            PARSE_INTEGER(iso_week, 2, "iso_week")

            if (*c != '\0' && !IS_DATE_AND_TIME_SEPARATOR) { /* Optional Day */
                PARSE_SEPARATOR(IS_CALENDAR_DATE_SEPARATOR,
                                "date separator ('-')")
                PARSE_INTEGER(iso_day, 1, "iso_day")
            }
            else {
                iso_day = 1;
            }

            int rv = iso_to_ymd(year, iso_week, iso_day, &year, &month, &day);
            if (rv) {
                PyErr_Format(PyExc_ValueError, "Invalid ISO Calendar date");
                return NULL;
            }
        }
        else { /* Separated month and may (i.e., MM-DD) or
                  ordinal date (i.e., DDD) */
            /* For sake of simplicity, we'll assume that it is a month
             * If we find out later that it's an ordinal day, then we'll adjust
             */
            PARSE_INTEGER(month, 2, "month")

            if (*c != '\0' && !IS_DATE_AND_TIME_SEPARATOR) {
                if (IS_CALENDAR_DATE_SEPARATOR) { /* Optional day */
                    c++;
                    PARSE_INTEGER(day, 2, "day")
                }
                else { /* Ordinal day */
                    PARSE_INTEGER(ordinal_day, 1, "ordinal day")
                    ordinal_day = (month * 10) + ordinal_day;

                    int rv =
                        ordinal_to_ymd(year, ordinal_day, &year, &month, &day);
                    if (rv) {
                        PyErr_Format(
                            PyExc_ValueError,
                            "Invalid ordinal day: %d is %s for year %d",
                            ordinal_day, rv == -1 ? "too small" : "too large",
                            year);
                        return NULL;
                    }
                }
            }
            else if (rfc3339_only) {
                PyErr_SetString(PyExc_ValueError,
                                "Datetime string not in RFC 3339 format.");
                return NULL;
            }
            else {
                day = 1;
            }
        }
    }
    else if (rfc3339_only) {
        PyErr_SetString(PyExc_ValueError,
                        "Datetime string not in RFC 3339 format.");
        return NULL;
    }
    else {
        if (IS_ISOCALENDAR_SEPARATOR) { /* Non-separated ISO Calendar week and
                                           day (i.e., WwwD) */
            c++;

            PARSE_INTEGER(iso_week, 2, "iso_week")

            if (*c != '\0' && !IS_DATE_AND_TIME_SEPARATOR) { /* Optional Day */
                PARSE_INTEGER(iso_day, 1, "iso_day")
            }
            else {
                iso_day = 1;
            }

            int rv = iso_to_ymd(year, iso_week, iso_day, &year, &month, &day);
            if (rv) {
                PyErr_Format(PyExc_ValueError, "Invalid ISO Calendar date");
                return NULL;
            }
        }
        else { /* Non-separated Month and Day (i.e., MMDD) or
                  ordinal date (i.e., DDD)*/
            /* For sake of simplicity, we'll assume that it is a month
             * If we find out later that it's an ordinal day, then we'll adjust
             */
            PARSE_INTEGER(month, 2, "month")

            PARSE_INTEGER(ordinal_day, 1, "ordinal day")

            if (*c == '\0' || IS_DATE_AND_TIME_SEPARATOR) { /* Ordinal day */
                ordinal_day = (month * 10) + ordinal_day;
                int rv =
                    ordinal_to_ymd(year, ordinal_day, &year, &month, &day);
                if (rv) {
                    PyErr_Format(PyExc_ValueError,
                                 "Invalid ordinal day: %d is %s for year %d",
                                 ordinal_day,
                                 rv == -1 ? "too small" : "too large", year);
                    return NULL;
                }
            }
            else { /* Day */
                /* Note that YYYYMM is not a valid timestamp. If the calendar
                 * date is not separated, a day is required (i.e., YYMMDD)
                 */
                PARSE_INTEGER(day, 1, "day")
                day = (ordinal_day * 10) + day;
            }
        }
    }

#if !PY_VERSION_AT_LEAST_36
    /* Validation of date fields
     * These checks are needed for Python <3.6 support. See
     * https://github.com/closeio/ciso8601/pull/30 Python 3.6+ does this
     * validation as part of datetime's C API constructor. See
     * https://github.com/python/cpython/commit/b67f0967386a9c9041166d2bbe0a421bd81e10bc
     */
    if (month < 1 || month > 12) {
        PyErr_SetString(PyExc_ValueError, "month must be in 1..12");
        return NULL;
    }

    if (day < 1) {
        PyErr_SetString(PyExc_ValueError, "day is out of range for month");
        return NULL;
    }

    /* Validate max day based on month */
    switch (month) {
        case 2:
            /* In the Gregorian calendar three criteria must be taken into
             * account to identify leap years:
             *     -The year can be evenly divided by 4;
             *     -If the year can be evenly divided by 100, it is NOT a leap
             * year, unless;
             *     -The year is also evenly divisible by 400. Then it is a leap
             * year.
             */
            if (day > 28) {
                unsigned int leap =
                    (year % 4 == 0) && (year % 100 || (year % 400 == 0));
                if (leap == 0 || day > 29) {
                    PyErr_SetString(PyExc_ValueError,
                                    "day is out of range for month");
                    return NULL;
                }
            }
            break;
        case 4:
        case 6:
        case 9:
        case 11:
            if (day > 30) {
                PyErr_SetString(PyExc_ValueError,
                                "day is out of range for month");
                return NULL;
            }
            break;
        default:
            /* For other months i.e. 1, 3, 5, 7, 8, 10 and 12 */
            if (day > 31) {
                PyErr_SetString(PyExc_ValueError,
                                "day is out of range for month");
                return NULL;
            }
            break;
    }
#endif

    if (*c != '\0') {
        /* Date and time separator */
        PARSE_SEPARATOR(IS_DATE_AND_TIME_SEPARATOR,
                        "date and time separator (i.e., 'T', 't', or ' ')")

        /* Hour */
        PARSE_INTEGER(hour, 2, "hour")

        if (*c != '\0' &&
            !IS_TIME_ZONE_SEPARATOR) { /* Optional minute and second */

            if (IS_TIME_SEPARATOR) { /* Separated Minute and Second
                                      *  (i.e., mm:ss)
                                      */
                c++;

                /* Minute */
                PARSE_INTEGER(minute, 2, "minute")

                if (*c != '\0' &&
                    !IS_TIME_ZONE_SEPARATOR) { /* Optional Second */
                    PARSE_SEPARATOR(IS_TIME_SEPARATOR, "time separator (':')")

                    /* Second */
                    PARSE_INTEGER(second, 2, "second")

                    /* Optional Fractional Second */
                    if (IS_FRACTIONAL_SEPARATOR) {
                        c++;
                        PARSE_FRACTIONAL_SECOND()
                    }
                }
                else if (rfc3339_only) {
                    PyErr_SetString(PyExc_ValueError,
                                    "RFC 3339 requires the second to be "
                                    "specified.");
                    return NULL;
                }

                if (!extended_date_format) {
                    PyErr_SetString(
                        PyExc_ValueError,
                        "Cannot combine \"basic\" date format with"
                        " \"extended\" time format (Should be either "
                        "`YYYY-MM-DDThh:mm:ss` or `YYYYMMDDThhmmss`).");
                    return NULL;
                }
            }
            else if (rfc3339_only) {
                PyErr_SetString(PyExc_ValueError,
                                "Colons separating time components are "
                                "mandatory in RFC 3339.");
                return NULL;
            }
            else { /* Non-separated Minute and Second (i.e., mmss) */
                /* Minute */
                PARSE_INTEGER(minute, 2, "minute")
                if (*c != '\0' &&
                    !IS_TIME_ZONE_SEPARATOR) { /* Optional Second */
                    /* Second */
                    PARSE_INTEGER(second, 2, "second")

                    /* Optional Fractional Second */
                    if (IS_FRACTIONAL_SEPARATOR) {
                        c++;
                        PARSE_FRACTIONAL_SECOND()
                    }
                }

                if (extended_date_format) {
                    PyErr_SetString(
                        PyExc_ValueError,
                        "Cannot combine \"extended\" date format with"
                        " \"basic\" time format (Should be either "
                        "`YYYY-MM-DDThh:mm:ss` or `YYYYMMDDThhmmss`).");
                    return NULL;
                }
            }
        }
        else if (rfc3339_only) {
            PyErr_SetString(PyExc_ValueError,
                            "Minute and second are mandatory in RFC 3339");
            return NULL;
        }

        if (hour == 24 && minute == 0 && second == 0 && usecond == 0) {
            /* Special case of 24:00:00, that is allowed in ISO 8601. It is
             * equivalent to 00:00:00 the following day
             */
            if (rfc3339_only) {
                PyErr_SetString(PyExc_ValueError,
                                "An hour value of 24, while sometimes legal "
                                "in ISO 8601, is explicitly forbidden by RFC "
                                "3339.");
                return NULL;
            }
            hour = 0, minute = 0, second = 0, usecond = 0;
            time_is_midnight = 1;
        }

#if !PY_VERSION_AT_LEAST_36
        /* Validate hour/minute/second
         * Only needed for Python <3.6 support.
         * Python 3.6+ does this validation as part of datetime's constructor).
         */
        if (hour > 23) {
            PyErr_SetString(PyExc_ValueError, "hour must be in 0..23");
            return NULL;
        }
        if (minute > 59) {
            PyErr_SetString(PyExc_ValueError, "minute must be in 0..59");
            return NULL;
        }
        if (second > 59) {
            PyErr_SetString(PyExc_ValueError, "second must be in 0..59");
            return NULL;
        }
#endif

        /* Optional tzinfo */
        if (IS_TIME_ZONE_SEPARATOR) {
            if (*c == '+') {
                tzsign = 1;
            }
            else if (*c == '-') {
                tzsign = -1;
            }
            c++;

            if (tzsign != 0) {
                /* tz hour */
                PARSE_INTEGER(tzhour, 2, "tz hour")

                if (IS_TIME_SEPARATOR) { /* Optional separator */
                    c++;
                    /* tz minute */
                    PARSE_INTEGER(tzminute, 2, "tz minute")
                }
                else if (rfc3339_only) {
                    PyErr_SetString(PyExc_ValueError,
                                    "Separator between hour and minute in UTC "
                                    "offset is mandatory in RFC 3339");
                    return NULL;
                }
                else if (*c != '\0') { /* Optional tz minute */
                    PARSE_INTEGER(tzminute, 2, "tz minute")
                }
            }

            /* It's not entirely clear whether this validation check is
             * necessary under ISO 8601. For now, we will err on the side of
             * caution and prevent suspected invalid timestamps. If we need to
             * loosen this restriction later, we can.
             */
            if (tzminute > 59) {
                PyErr_SetString(PyExc_ValueError, "tzminute must be in 0..59");
                return NULL;
            }

            if (parse_any_tzinfo) {
                tzminute += 60 * tzhour;
                tzminute *= tzsign;

                if (tzminute == 0) {
                    tzinfo = utc;
                }
                else if (abs(tzminute) >= 1440) {
                    /* Format the error message as if we were still using pytz
                     * for Python 2 and datetime.timezone for Python 3.
                     * This is done to maintain complete backwards
                     * compatibility with ciso8601 2.0.x. Perhaps change to a
                     * simpler message in ciso8601 v3.0.0.
                     */
#if PY_MAJOR_VERSION >= 3
                    delta = PyDelta_FromDSU(0, tzminute * 60, 0);
                    PyErr_Format(PyExc_ValueError,
                                 "offset must be a timedelta"
                                 " strictly between -timedelta(hours=24) and"
                                 " timedelta(hours=24),"
                                 " not %R.",
                                 delta);
                    Py_DECREF(delta);
#else
                    PyErr_Format(PyExc_ValueError,
                                 "('absolute offset is too large', %d)",
                                 tzminute);
#endif
                    return NULL;
                }
                else {
#if CISO8601_CACHING_ENABLED
                    tz_index = tzminute + 1439;
                    if ((tzinfo = tz_cache[tz_index]) == NULL) {
                        tzinfo = new_fixed_offset(60 * tzminute);

                        if (tzinfo == NULL) /* i.e., PyErr_Occurred() */
                            return NULL;
                        tz_cache[tz_index] = tzinfo;
                    }
#else
                    tzinfo = new_fixed_offset(60 * tzminute);
                    if (tzinfo == NULL) /* i.e., PyErr_Occurred() */
                        return NULL;
#endif
                }
            }
        }
        else if (rfc3339_only) {
            PyErr_SetString(PyExc_ValueError,
                            "UTC offset is mandatory in RFC 3339 format.");
            return NULL;
        }
    }
    else if (rfc3339_only) {
        PyErr_SetString(PyExc_ValueError,
                        "Time is mandatory in RFC 3339 format.");
        return NULL;
    }

    /* Make sure that there is no more to parse. */
    if (*c != '\0') {
        PyErr_Format(PyExc_ValueError, "unconverted data remains: '%s'", c);
#if !CISO8601_CACHING_ENABLED
        if (tzinfo != Py_None && tzinfo != utc)
            Py_DECREF(tzinfo);
#endif
        return NULL;
    }

    obj = PyDateTimeAPI->DateTime_FromDateAndTime(
        year, month, day, hour, minute, second, usecond, tzinfo,
        PyDateTimeAPI->DateTimeType);

#if !CISO8601_CACHING_ENABLED
    if (tzinfo != Py_None && tzinfo != utc)
        Py_DECREF(tzinfo);
#endif

    if (obj && time_is_midnight) {
        delta = PyDelta_FromDSU(1, 0, 0); /* 1 day */
        temp = obj;
        obj = PyNumber_Add(temp, delta);
        Py_DECREF(delta);
        Py_DECREF(temp);
    }

    return obj;
}

static PyObject *
parse_datetime_as_naive(PyObject *self, PyObject *dtstr)
{
    return _parse(self, dtstr, 0, 0);
}

static PyObject *
parse_datetime(PyObject *self, PyObject *dtstr)
{
    return _parse(self, dtstr, 1, 0);
}

static PyObject *
parse_rfc3339(PyObject *self, PyObject *dtstr)
{
    return _parse(self, dtstr, 1, 1);
}

static PyObject *
_hard_coded_benchmark_timestamp(PyObject *self, PyObject *ignored)
{
    return PyDateTimeAPI->DateTime_FromDateAndTime(
        2014, 1, 9, 21, 48, 0, 0, Py_None, PyDateTimeAPI->DateTimeType);
}

static PyMethodDef CISO8601Methods[] = {
    {"parse_datetime", (PyCFunction)parse_datetime, METH_O,
     "Parse a ISO8601 date time string."},
    {"parse_datetime_as_naive", parse_datetime_as_naive, METH_O,
     "Parse a ISO8601 date time string, ignoring the time zone component."},
    {"parse_rfc3339", parse_rfc3339, METH_O,
     "Parse an RFC 3339 date time string."},
    {"_hard_coded_benchmark_timestamp", _hard_coded_benchmark_timestamp,
     METH_NOARGS,
     "Return a datetime using hardcoded values (for benchmarking purposes)"},
    {NULL, NULL, 0, NULL}};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "ciso8601",
    NULL,
    -1,
    CISO8601Methods,
    NULL,
    NULL,
    NULL,
    NULL,
};
#endif

PyMODINIT_FUNC
#if PY_MAJOR_VERSION >= 3
PyInit_ciso8601(void)
#else
initciso8601(void)
#endif
{
#if PY_MAJOR_VERSION >= 3
    PyObject *module = PyModule_Create(&moduledef);
#else
    PyObject *module = Py_InitModule("ciso8601", CISO8601Methods);
#endif
    /* CISO8601_VERSION is defined in setup.py */
    PyModule_AddStringConstant(module, "__version__",
                               EXPAND_AND_STRINGIZE(CISO8601_VERSION));

    PyDateTime_IMPORT;

    // PyMODINIT_FUNC is void in Python 2, returns PyObject* in Python 3
    if (initialize_timezone_code(module) < 0) {
#if PY_MAJOR_VERSION >= 3
        return NULL;
#else
        return;
#endif
    }

#if SUPPORTS_37_TIMEZONE_API
    utc = PyDateTime_TimeZone_UTC;
#else
    utc = new_fixed_offset(0);
#endif

// PyMODINIT_FUNC is void in Python 2, returns PyObject* in Python 3
#if PY_MAJOR_VERSION >= 3
    return module;
#endif
}
