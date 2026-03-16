#  holidays
#  --------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Authors: Vacanza Team and individual contributors (see CONTRIBUTORS file)
#           dr-prodigy <dr.prodigy.github@gmail.com> (c) 2017-2023
#           ryanss <ryanssdev@icloud.com> (c) 2014-2017
#  Website: https://github.com/vacanza/holidays
#  License: MIT (see LICENSE file)

from gettext import gettext as tr

from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class VaticanCity(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Vatican City holidays.

    References:
        * <https://web.archive.org/web/20250125172412/https://www.vatican.va/roman_curia/labour_office/docs/documents/ulsa_b18_7_it.html>
        * <https://web.archive.org/web/20250320055902/https://cdn.restorethe54.com/media/pdf/1917-code-of-canon-law-english.pdf>
        * <https://web.archive.org/web/20250425093046/https://www.vatican.va/archive/cod-iuris-canonici/eng/documents/cic_lib4-cann1244-1253_en.html>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Vatican_City>
        * <https://en.wikipedia.org/wiki/Holy_day_of_obligation>
        * <https://web.archive.org/web/20241004015531/https://www.ewtn.com/catholicism/library/solemnity-of-mary-mother-of-god-5826>
        * <https://web.archive.org/web/20241130180934/https://www.franciscanmedia.org/saint-of-the-day/saint-joseph-the-worker/>

    Cross-checked With:
        * <https://web.archive.org/web/20250427173707/https://www.vaticanstate.va/images/pdf/CALENDARIO_2020.pdf>
        * <https://web.archive.org/web/20220723175312/https://www.farmaciavaticana.va/images/pdf/calendario_2021.pdf>
        * <https://web.archive.org/web/20221018201001/https://www.farmaciavaticana.va/images/pdf/calendario_2022.pdf>
        * <https://web.archive.org/web/20231101221620/https://www.farmaciavaticana.va/images/pdf/calendario_2023.pdf>
        * <https://web.archive.org/web/20241208034527/https://www.farmaciavaticana.va/media/attachments/2024/01/02/calendario_2024.pdf>
        * <https://web.archive.org/web/20250208160702/https://www.farmaciavaticana.va/media/attachments/2025/01/02/calendario-2025.pdf>
    """

    country = "VA"
    default_language = "it"
    supported_languages = ("en_US", "it", "th")
    # Lateran Treaty, FEB 11, 2029.
    start_year = 1929

    def __init__(self, *args, **kwargs) -> None:
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self) -> None:
        # This is supposedly the same as International New Year.
        # Modern adoption across the entire Latin Church in 1931 though this
        # was already celebrated in Rome as the Octave day of Christmas.

        # Solemnity of Mary, Mother of God.
        self._add_holiday_jan_1(tr("Solennità di Maria Santissima Madre di Dio"))

        # Epiphany.
        self._add_epiphany_day(tr("Epifania del Signore"))

        self._add_holiday_feb_11(
            # Anniversary of the Foundation of Vatican City.
            tr("Anniversario della istituzione dello Stato della Città del Vaticano")
        )

        # Name Day of the Holy Father.
        name_day = tr("Onomastico del Santo Padre")

        if self._year >= 2025:
            # Pope Leo XIV (Robert Francis Prevost).
            # Name Day: Saint Robert Bellarmine Day (SEP 17).
            self._add_holiday_sep_17(name_day)

            if self._year == 2025:
                # Pope Francis (cont.).
                self._add_saint_georges_day(name_day)
        elif self._year >= 2013:
            # Pope Francis (Jorge Mario Bergoglio).
            # Name Day: Saint George's Day (APR 23).
            self._add_saint_georges_day(name_day)
        elif self._year >= 2006:
            # Pope Benedict XVI (Josef Aloisius Ratzinger).
            # Name Day: Saint Joseph's Day (MAR 19).
            self._add_saint_josephs_day(name_day)
        elif 1978 <= self._year <= 2004:
            # Pope John Paul II (Karol Józef Wojtyła).
            # Name Day: Saint Charles Borromeo Day (NOV 4).
            self._add_holiday_nov_4(name_day)

        # Anniversary of the Election of the Holy Father.
        name_election = tr("Anniversario dell'Elezione del Santo Padre")

        if self._year >= 2026:
            # Pope Leo XIV (Robert Francis Prevost).
            self._add_holiday_may_8(name_election)
        elif self._year >= 2014:
            # Pope Francis (Jorge Mario Bergoglio).
            self._add_holiday_mar_13(name_election)
        elif 2006 <= self._year <= 2012:
            # Pope Benedict XVI (Josef Aloisius Ratzinger).
            self._add_holiday_apr_19(name_election)
        elif 1979 <= self._year <= 2004:
            # Pope John Paul II (Karol Józef Wojtyła).
            self._add_holiday_oct_16(name_election)
        elif 1964 <= self._year <= 1978:
            # Pope Paul VI (Giovanni Battista Enrico Antonio Maria Montini).
            self._add_holiday_jun_21(name_election)
        elif 1959 <= self._year <= 1962:
            # Pope John XXIII (Angelo Giuseppe Roncalli).
            self._add_holiday_oct_28(name_election)
        elif 1940 <= self._year <= 1958:
            # Pope Pius XII (Eugenio Maria Giuseppe Giovanni Pacelli).
            self._add_holiday_mar_2(name_election)
        elif self._year <= 1939:
            # Pope Pius XI (Achille Ambrogio Damiano Ratti).
            self._add_holiday_feb_6(name_election)

        # Saint Joseph's Day.
        self._add_saint_josephs_day(tr("San Giuseppe"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Giovedì Santo"))

        # Good Friday.
        self._add_good_friday(tr("Venerdì Santo"))

        # Holy Saturday.
        self._add_holy_saturday(tr("Sabato Santo"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Pasqua di Resurrezione"))

        # Easter Monday.
        self._add_easter_monday(tr("Lunedì dell'Angelo"))

        # Easter Tuesday.
        self._add_easter_tuesday(tr("Martedì in Albis"))

        # Created in response to May Day holidays by Pope Pius XII in 1955.
        if self._year >= 1955:
            # Saint Joseph the Worker.
            self._add_holiday_may_1(tr("San Giuseppe Artigiano"))

        # Solemnity of Pentecost.
        self._add_whit_sunday(tr("Solennità della Pentecoste"))

        # Solemnity of Holy Trinity.
        self._add_trinity_sunday(tr("Solennità della Santissima Trinità"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Ascensione del Signore"))

        # Corpus Domini.
        self._add_corpus_christi_day(tr("Corpus Domini"))

        # Saints Peter and Paul's Day.
        self._add_saints_peter_and_paul_day(tr("Santi Pietro e Paolo"))

        # Day Before Assumption of Mary.
        self._add_holiday_aug_14(tr("Vigilia dell'Assunzione di Maria Santissima"))

        # Assumption of Mary Day.
        self._add_assumption_of_mary_day(tr("Assunzione di Maria Santissima"))

        # Day After Assumption of Mary.
        self._add_holiday_aug_16(tr("Giorno Successivo all'Assunzione di Maria Santissima"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Tutti i Santi"))

        # All Souls' Day.
        self._add_all_souls_day(tr("Tutti i Fedeli Defunti"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Immacolata Concezione"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Vigilia di Natale"))

        # Christmas Day.
        self._add_christmas_day(tr("Natale"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Santo Stefano"))

        # Saint John the Evangelist's Day.
        self._add_christmas_day_three(tr("San Giovanni"))

        # Last Day of the Year.
        self._add_new_years_eve(tr("Ultimo giorno dell'anno"))


class VA(VaticanCity):
    pass


class VAT(VaticanCity):
    pass
