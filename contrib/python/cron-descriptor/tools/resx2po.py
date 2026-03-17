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
import datetime
import xml.etree.ElementTree as ET
from logging import getLogger
from pathlib import Path

import polib

log = getLogger(__name__)

class Resx2Po:
    def __init__(self, en_resx: Path, translation_resx: Path, code: str, output_po: Path) -> None:
        if not en_resx.is_file():
            msg = f"EN Resx {en_resx.absolute()} not found"
            raise FileNotFoundError(msg)

        if not translation_resx.is_file():
            msg = f"Translation {code} {translation_resx.absolute()} Resx bound not found"
            raise FileNotFoundError(msg)

        self.en_resx = self.resx2dict(en_resx)
        self.translation_resx = self.resx2dict(translation_resx)
        self.code = code
        self.output_po = output_po

        self.generate()

    def resx2dict(self, resx: Path) -> dict[str, str]:
        tree = ET.parse(resx)
        root = tree.getroot()
        translation_table = {}
        for first in root.findall("./data"):
            found_value = first.find("./value")
            if found_value is not None and found_value.text:
                translation_table[first.attrib["name"]] = found_value.text

        return translation_table

    def generate(self) -> None:
        po = polib.POFile()
        now = datetime.datetime.now(datetime.timezone.utc)
        po.metadata = {
            "Project-Id-Version": "1.0",
            "Report-Msgid-Bugs-To": "adam.schubert@sg1-game.net",
            "POT-Creation-Date": now.strftime("%Y-%m-%d %H:%M%z"),
            "PO-Revision-Date": now.strftime("%Y-%m-%d %H:%M%z"),
            "Last-Translator": "Adam Schubert <adam.schubert@sg1-game.net>",
            "Language-Team": "",
            "MIME-Version": "1.0",
            "Content-Type": "text/plain; charset=utf-8",
            "Content-Transfer-Encoding": "8bit",
            "Language": self.code,
        }

        for message_en_id, message_en in self.en_resx.items():
            if message_en_id in self.translation_resx:
                entry = polib.POEntry(
                    msgid=message_en,
                    msgstr=self.translation_resx[message_en_id],
                    comment=message_en_id,
                )
                po.append(entry)
            else:
                log.warning("%s not found in %s resx", message_en_id, self.code)

        po.save(str(self.output_po.absolute()))


code_list = {
    "cs": "cs_CZ",        # Czech
    "da": "da_DK",        # Danish
    "de": "de_DE",        # German
    "el": "el_GR",        # Greek
    "es": "es_ES",        # Spanish (Spain)
    "es-MX": "es_MX",     # Spanish (Mexico)
    "fa": "fa_IR",        # Persian (Iran)
    "fi": "fi_FI",        # Finnish
    "fr": "fr_FR",        # French (France)
    "he-IL": "he_IL",     # Hebrew (Israel)
    "hu": "hu_HU",        # Hungarian
    "it": "it_IT",        # Italian
    "ja": "ja_JP",        # Japanese
    "kk": "kk_KZ",        # Kazakh
    "ko": "ko_KR",        # Korean
    "nb": "nb_NO",        # Norwegian Bokmål
    "nl": "nl_NL",        # Dutch (Netherlands)
    "pl": "pl_PL",        # Polish
    "pt": "pt_PT",        # Portuguese (Portugal)
    "ro": "ro_RO",        # Romanian
    "ru": "ru_RU",        # Russian
    "sl": "sl_SI",        # Slovenian
    "sv": "sv_SE",        # Swedish
    "tr": "tr_TR",        # Turkish
    "uk": "uk_UA",        # Ukrainian
    "vi": "vi_VN",        # Vietnamese
    "zh-Hans": "zh_CN",   # Simplified Chinese (China)
    "zh-Hant": "zh_TW",   # Traditional Chinese (Taiwan)
}

output_dir = Path("./locale")
input_dir = Path("./source")

for from_code, to_code in code_list.items():
    output_file = output_dir.joinpath(f"{to_code}.po")
    Resx2Po(
        input_dir.joinpath("Resources.resx"),
        input_dir.joinpath(f"Resources.{from_code}.resx"),
        to_code,
        output_file,
    )
