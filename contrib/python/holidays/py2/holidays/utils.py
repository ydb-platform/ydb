import inspect
import holidays
from datetime import date


def list_supported_countries():
    """List all supported countries incl. their abbreviation."""
    return [name for name, obj in
            inspect.getmembers(holidays.countries, inspect.isclass)]


def CountryHoliday(country, years=[], prov=None, state=None, expand=True,
                   observed=True):
    try:
        country_classes = inspect.getmembers(holidays.countries,
                                             inspect.isclass)
        country = next(obj for name, obj in country_classes if name == country)
        country_holiday = country(years=years, prov=prov, state=state,
                                  expand=expand, observed=observed)
    except StopIteration:
        raise KeyError("Country %s not available" % country)
    return country_holiday


def get_gre_date(year, Hmonth, Hday):
    """
    Returns the gregorian dates within the gregorian year 'year'
    of all instances of islamic calendar 'Hmonth' and 'Hday'.
    Defaults to using the hijri-converter library if it is installed
    otherwise it uses the less-precise convertdate one (which is a
    requirement).
    """
    try:
        from hijri_converter import convert

        Hyear = convert.Gregorian(year, 1, 1).to_hijri().datetuple()[0]
        gres = [convert.Hijri(y, Hmonth, Hday).to_gregorian()
                for y in range(Hyear - 1, Hyear + 2)]
        gre_dates = [date(*gre.datetuple())
                     for gre in gres if gre.year == year]
        return gre_dates
    except ImportError:
        import warnings
        from convertdate import islamic

        def warning_on_one_line(message, category, filename, lineno,
                                file=None, line=None):
            return filename + ': ' + str(message) + '\n'

        warnings.formatwarning = warning_on_one_line
        warnings.warn("Islamic Holidays estimated using 'convertdate'"
                      " package.")
        warnings.warn("For higher precision, install 'hijri-converter'"
                      " package: pip install -U hijri-converter")
        warnings.warn("(see https://hijri-converter.readthedocs.io/ )")
        Hyear = islamic.from_gregorian(year, 1, 1)[0]
        gres = [islamic.to_gregorian(y, Hmonth, Hmonth)
                for y in range(Hyear - 1, Hyear + 2)]
        gre_dates = [date(*gre) for gre in gres if gre[0] == year]
        return gre_dates
