# The MIT License (MIT)
#
# Copyright (c) 2016 Adam Schubert
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
from __future__ import annotations

import locale
import warnings

from .CasingTypeEnum import CasingTypeEnum


class Options:
    """
    Options for parsing and describing a Cron Expression
    """

    locale_code: str
    casing_type: CasingTypeEnum
    verbose: bool
    day_of_week_start_index_zero: bool
    use_24hour_time_format: bool
    locale_location: str | None

    _twelve_hour_locales = (
        "en_US",  # United States
        "en_CA",  # Canada (English)
        "fr_CA",  # Canada (French)
        "en_AU",  # Australia
        "en_NZ",  # New Zealand
        "en_IE",  # Ireland
        "en_PH",  # Philippines
        "es_MX",  # Mexico
        "en_PK",  # Pakistan
        "en_IN",  # India
        "ar_SA",  # Saudi Arabia
        "bn_BD",  # Bangladesh
        "es_HN",  # Honduras
        "es_SV",  # El Salvador
        "es_NI",  # Nicaragua
        "ar_JO",  # Jordan
        "ar_EG",  # Egypt
        "es_CO",  # Colombia
    )

    def __init__(self,
                 casing_type: CasingTypeEnum = CasingTypeEnum.Sentence,
                 *,
                 verbose: bool = False,
                 day_of_week_start_index_zero: bool = True,
                 use_24hour_time_format: bool | None = None,
                 locale_code: str | None = None,
                 locale_location: str | None = None,
                 ) -> None:
        self.casing_type = casing_type
        self.verbose = verbose
        self.day_of_week_start_index_zero = day_of_week_start_index_zero
        self.locale_location = locale_location

        if not locale_code:
            # Autodetect
            code, _encoding = locale.getlocale()
            if not code:
                warnings.warn(
                    "No system locale set. Falling back to 'en_US'. "
                    "Set LANG/LC_ALL or pass locale_code to override.",
                    stacklevel=2,
                )
                code = "en_US"
            self.locale_code = code
        else:
            self.locale_code = locale_code

        if use_24hour_time_format is None:
            # Autodetect
            self.use_24hour_time_format = self.locale_code not in self._twelve_hour_locales
        else:
            self.use_24hour_time_format = use_24hour_time_format
