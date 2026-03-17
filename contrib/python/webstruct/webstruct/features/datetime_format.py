# This file define date/time notations among different countries.

__all__ = ['MONTHS', 'WEEKDAYS']

# http://en.wikipedia.org/wiki/Date_and_time_notation_in_the_Netherlands
_NL_MONTHS = [
    ("Jan", "January", "Januari", "jan."),
    ("Feb", "February", "Februari", "feb."),
    ("Mar", "Mrt", "Maart", "March", "mrt.", "maa.", "maa"),
    ("Apr", "April", "apr."),
    ("May", "Mei"),
    ("Jun", "June", "Juni", "jun."),
    ("Jul", "July", "Juli", "jul."),
    ("Aug", "August", "Augustus", "aug."),
    ("Sep", "September", "sep."),
    ("Okt", "Oct", "October", "Oktober", "okt."),
    ("Nov", "November", "nov."),
    ("Dec", "December", "dec.")
]

_NL_WEEKDAYS = [
    ("Mon", "Monday", "Maandag", "ma.", "ma"),
    ("Tue", "Tuesday", "Dinsdag", "di.", "di"),
    ("Wed", "Wednesday", "Woensdag", "wo.", "wo"),
    ("Thu", "Thursday", "Donderdag", "do.", "do"),
    ("Fri", "Friday", "Vrijdag", 'vr.', "vr"),
    ("Sat", "Saturday", "Zaterdag", 'za.', "za"),
    ("Sun", "Sunday", "Zondag", 'zo.', "zo")
]

_NL_HMS = [
    ("h", "tijd", "uur", "u"),
    ("m", "minuut", "notulen"),
    ("s", "seconde", "seconden")
]

_EN_MONTHS = [
    ("Jan", "January"),
    ("Feb", "February"),
    ("Mar", "March"),
    ("Apr", "April"),
    ("May", "May"),
    ("Jun", "June"),
    ("Jul", "July"),
    ("Aug", "August"),
    ("Sep", "September"),
    ("Oct", "October"),
    ("Nov", "November"),
    ("Dec", "December")
]

WEEKDAYS = (_NL_WEEKDAYS, )
MONTHS = (_EN_MONTHS, _NL_MONTHS)
