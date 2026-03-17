import pkg_resources
from humanize.filesize import naturalsize
from humanize.i18n import activate, deactivate
from humanize.number import apnumber, fractional, intcomma, intword, ordinal
from humanize.time import naturaldate, naturalday, naturaldelta, naturaltime

__version__ = VERSION = pkg_resources.get_distribution(__name__).version


__all__ = [
    "__version__",
    "activate",
    "apnumber",
    "deactivate",
    "fractional",
    "intcomma",
    "intword",
    "naturaldate",
    "naturalday",
    "naturaldelta",
    "naturalsize",
    "naturaltime",
    "ordinal",
    "VERSION",
]
