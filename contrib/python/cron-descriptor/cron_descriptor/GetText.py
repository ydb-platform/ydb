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

import gettext
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class FallBackNull(gettext.NullTranslations):
    def gettext(self, _message: str)  -> str:
        # If we get here, that means that original translator failed, we will return empty string
        return ""


class GetText:
    """Handles language translations
    """

    def __init__(self, locale_code: str, locale_location: str | None = None) -> None:
        """Initialize GetText
        :param locale_code selected locale
        """
        try:
            self.trans = self.load_locale(locale_code, locale_location)
        except OSError:
            logger.debug("Failed to find locale %s", locale_code)
            logger.debug("Attempting to load en_US as fallback")
            self.trans = self.load_locale("en_US")

        # Add fallback that does not return original string, this is hack to add
        # support for _("") or _("")
        self.trans.add_fallback(FallBackNull())

    def load_locale(self, locale_code: str, locale_location: str | None=None) -> gettext.GNUTranslations:
        dir_path = Path(locale_location) if locale_location else Path(__file__).resolve().parent.joinpath("locale")
        filename = dir_path.joinpath(f"{locale_code}.mo")
        with filename.open("rb") as f:
            trans = gettext.GNUTranslations(f)
        logger.debug("%s Loaded", filename)
        return trans


