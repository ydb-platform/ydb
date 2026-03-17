from ..base import TemplatedValue


class Var(TemplatedValue):

    pass


class Func(Var):

    pass


class VarGeoip(Var):
    """Returns Geoip data.

    http://uwsgi.readthedocs.io/en/latest/GeoIP.html

    """
    tpl = '${geoip[%s]}'

    COUNTRY_CODE = 'country_code'
    COUNTRY_CODE3 = 'country_code3'
    COUNTRY_NAME = 'country_name'
    CONTINENT = 'continent'
    REGION = 'region'
    REGION_NAME = 'region_name'
    CITY = 'city'
    POSTAL_CODE = 'postal_code'
    LATITUDE = 'lat'
    LONGITUDE = 'lon'
    DMA = 'dma'
    AREA = 'area'

    vars_country = [
        COUNTRY_CODE,
        COUNTRY_CODE3,
        COUNTRY_NAME,
    ]
    """Keys avaiable for country database."""

    vars_city = [
        CONTINENT,
        COUNTRY_CODE,
        COUNTRY_CODE3,
        COUNTRY_NAME,
        REGION,
        REGION_NAME,
        CITY,
        POSTAL_CODE,
        LATITUDE,
        LONGITUDE,
        DMA,
        AREA,
    ]
    """Keys avaiable for city database."""


class VarRequest(Var):
    """Returns request variable. Examples: PATH_INFO, SCRIPT_NAME, REQUEST_METHOD."""
    tpl = '${%s}'


class VarMetric(Var):
    """Returns metric (see ``monitoring``) variable."""
    tpl = '${metric[%s]}'


class VarCookie(Var):
    """Returns cookie variable"""
    tpl = '${cookie[%s]}'


class VarQuery(Var):
    """Returns query string variable."""
    tpl = '${qs[%s]}'


class VarUwsgi(Var):
    """Returns internal uWSGI information.

     Supported variables:
        * wid
        * pid
        * uuid
        * status

    """
    tpl = '${uwsgi[%s]}'


class VarTime(Var):
    """Returns time/date in various forms.

    Supported variables:
        * unix

    """
    tpl = '${time[%s]}'


class VarHttptime(Var):
    """Returns http date adding the numeric argument (if specified)
    to the current time (use empty arg for current server time).

    """
    tpl = '${httptime[%s]}'


class FuncMime(Func):
    """Returns mime type of a variable."""
    tpl = '${mime[%s]}'


class FuncMath(Func):
    """Perform a math operation. Example: CONTENT_LENGTH+1

    Supported operations: + - * /

    .. warning:: Requires matheval support.

    """
    tpl = '${math[%s]}'


class FuncBase64(Func):
    """Encodes the specified var in base64"""
    tpl = '${base64[%s]}'


class FuncHex(Func):
    """Encodes the specified var in hex."""
    tpl = '${hex[%s]}'


class FuncUpper(Func):
    """Uppercase the specified var."""
    tpl = '${upper[%s]}'


class FuncLower(Func):
    """Lowercase the specified var."""
    tpl = '${lower[%s]}'
