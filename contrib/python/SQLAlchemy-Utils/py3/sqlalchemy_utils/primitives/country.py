from functools import total_ordering

from .. import i18n
from ..utils import str_coercible


@total_ordering
@str_coercible
class Country:
    """
    Country class wraps a 2 to 3 letter country code. It provides various
    convenience properties and methods.

    ::

        from babel import Locale
        from sqlalchemy_utils import Country, i18n


        # First lets add a locale getter for testing purposes
        i18n.get_locale = lambda: Locale('en')


        Country('FI').name  # Finland
        Country('FI').code  # FI

        Country(Country('FI')).code  # 'FI'

    Country always validates the given code if you use at least the optional
    dependency list 'babel', otherwise no validation are performed.

    ::

        Country(None)  # raises TypeError

        Country('UnknownCode')  # raises ValueError


    Country supports equality operators.

    ::

        Country('FI') == Country('FI')
        Country('FI') != Country('US')


    Country objects are hashable.


    ::

        assert hash(Country('FI')) == hash('FI')

    """
    def __init__(self, code_or_country):
        if isinstance(code_or_country, Country):
            self.code = code_or_country.code
        elif isinstance(code_or_country, str):
            self.validate(code_or_country)
            self.code = code_or_country
        else:
            raise TypeError(
                "Country() argument must be a string or a country, not '{}'"
                .format(
                    type(code_or_country).__name__
                )
            )

    @property
    def name(self):
        return i18n.get_locale().territories[self.code]

    @classmethod
    def validate(self, code):
        try:
            i18n.babel.Locale('en').territories[code]
        except KeyError:
            raise ValueError(
                f'Could not convert string to country code: {code}'
            )
        except AttributeError:
            # As babel is optional, we may raise an AttributeError accessing it
            pass

    def __eq__(self, other):
        if isinstance(other, Country):
            return self.code == other.code
        elif isinstance(other, str):
            return self.code == other
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self.code)

    def __ne__(self, other):
        return not (self == other)

    def __lt__(self, other):
        if isinstance(other, Country):
            return self.code < other.code
        elif isinstance(other, str):
            return self.code < other
        return NotImplemented

    def __repr__(self):
        return f'{self.__class__.__name__}({self.code!r})'

    def __unicode__(self):
        return self.name
