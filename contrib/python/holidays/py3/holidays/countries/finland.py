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

from holidays.calendars.gregorian import _timedelta
from holidays.constants import PUBLIC, UNOFFICIAL, WORKDAY
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Finland(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Finland holidays.

    Official Flag Days are included in the `WORKDAY` category, while Customary Flag Days
    are included in `UNOFFICIAL` category.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Finland>
        * [Bank holidays (Finnish)](https://web.archive.org/web/20250416185850/https://www.suomenpankki.fi/fi/raha-ja-maksaminen/pankkivapaapaivat/)
        * [Bank holidays (English)](https://web.archive.org/web/20250327200736/https://www.suomenpankki.fi/en/money-and-payments/bank-holidays/)
        * [Bank holidays (Swedish)](https://web.archive.org/web/20250217014536/https://www.suomenpankki.fi/sv/pengar-och-betalningar/bankfria-dagar-i-finland/)
        * <https://en.wikipedia.org/wiki/Flag_flying_days_in_Finland#Customary_flag_days>
        * <https://web.archive.org/web/20250327201901/https://intermin.fi/en/flag-and-arms/flag-flying-days>
        * <https://web.archive.org/web/20241227163324/https://intermin.fi/en/flag-and-arms/flag-days/2024>
        * <https://web.archive.org/web/20250515053310/https://almanakka.helsinki.fi/en/flag-days-and-holidays/>
        * <https://web.archive.org/web/20250515054129/https://www.finlex.fi/en/legislation/collection/1978/383>
        * <https://web.archive.org/web/20250515064749/https://itsenäisyys.fi/itsenaisyyspaivan-perinteet-vakiintuivat-jo-itsenaisyyden-alkuvuosina/>
        * <https://web.archive.org/web/20250515072453/https://yle.fi/a/20-152398>
        * <https://en.wikipedia.org/wiki/Independence_Day_(Finland)>
        * <https://en.wikipedia.org/wiki/Åland's_Autonomy_Day>
        * <https://web.archive.org/web/20250509184304/https://wiki.aineetonkulttuuriperinto.fi/wiki/Workers’_Labour_Day_on_May_1>
    """

    country = "FI"
    default_language = "fi"
    supported_languages = ("en_US", "fi", "sv_FI", "th", "uk")
    supported_categories = (PUBLIC, UNOFFICIAL, WORKDAY)
    subdivisions: tuple[str, ...] = (
        "01",  # Ahvenanmaan maakunta (Landskapet Åland).
        "02",  # Etelä-Karjala (Södra Karelen).
        "03",  # Etelä-Pohjanmaa (Södra Österbotten).
        "04",  # Etelä-Savo (Södra Savolax).
        "05",  # Kainuu (Kajanaland).
        "06",  # Kanta-Häme (Egentliga Tavastland).
        "07",  # Keski-Pohjanmaa (Mellersta Österbotten).
        "08",  # Keski-Suomi (Mellersta Finland).
        "09",  # Kymenlaakso (Kymmenedalen).
        "10",  # Lappi (Lappland).
        "11",  # Pirkanmaa (Birkaland).
        "12",  # Pohjanmaa (Österbotten).
        "13",  # Pohjois-Karjala (Norra Karelen).
        "14",  # Pohjois-Pohjanmaa (Norra Österbotten).
        "15",  # Pohjois-Savo (Norra Savolax).
        "16",  # Päijät-Häme (Päijänne-Tavastland).
        "17",  # Satakunta.
        "18",  # Uusimaa (Nyland).
        "19",  # Varsinais-Suomi (Egentliga Finland).
    )
    subdivisions_aliases = {
        "Ahvenanmaan maakunta": "01",
        "Landskapet Åland": "01",
        "Etelä-Karjala": "02",
        "Södra Karelen": "02",
        "Etelä-Pohjanmaa": "03",
        "Södra Österbotten": "03",
        "Etelä-Savo": "04",
        "Södra Savolax": "04",
        "Kainuu": "05",
        "Kajanaland": "05",
        "Kanta-Häme": "06",
        "Egentliga Tavastland": "06",
        "Keski-Pohjanmaa": "07",
        "Mellersta Österbotten": "07",
        "Keski-Suomi": "08",
        "Mellersta Finland": "08",
        "Kymenlaakso": "09",
        "Kymmenedalen": "09",
        "Lappi": "10",
        "Lappland": "10",
        "Pirkanmaa": "11",
        "Birkaland": "11",
        "Pohjanmaa": "12",
        "Österbotten": "12",
        "Pohjois-Karjala": "13",
        "Norra Karelen": "13",
        "Pohjois-Pohjanmaa": "14",
        "Norra Österbotten": "14",
        "Pohjois-Savo": "15",
        "Norra Savolax": "15",
        "Päijät-Häme": "16",
        "Päijänne-Tavastland": "16",
        "Satakunta": "17",
        "Uusimaa": "18",
        "Nyland": "18",
        "Varsinais-Suomi": "19",
        "Egentliga Finland": "19",
    }
    start_year = 1853

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Uudenvuodenpäivä"))

        # Epiphany.
        name = tr("Loppiainen")
        if 1973 <= self._year <= 1990:
            self._add_holiday_1st_sat_from_jan_6(name)
        else:
            self._add_epiphany_day(name)

        # Good Friday.
        self._add_good_friday(tr("Pitkäperjantai"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Pääsiäispäivä"))

        # Easter Monday.
        self._add_easter_monday(tr("Toinen pääsiäispäivä"))

        if self._year >= 1944:
            # May Day.
            self._add_holiday_may_1(tr("Vappu"))

        # Ascension Day.
        name = tr("Helatorstai")
        if 1973 <= self._year <= 1990:
            self._add_holiday_34_days_past_easter(name)
        else:
            self._add_ascension_thursday(name)

        # Whit Sunday.
        self._add_whit_sunday(tr("Helluntaipäivä"))

        # Midsummer Eve.
        name = tr("Juhannusaatto")
        if self._year >= 1955:
            dt = self._add_holiday_1st_fri_from_jun_19(name)
        else:
            dt = self._add_holiday_jun_23(name)

        # Midsummer Day.
        self._add_holiday(tr("Juhannuspäivä"), _timedelta(dt, +1))

        # All Saints' Day.
        name = tr("Pyhäinpäivä")
        if self._year >= 1955:
            self._add_holiday_1st_sat_from_oct_31(name)
        else:
            self._add_holiday_nov_1(name)

        # Designated as Public Holiday on NOV 20th, 1919.
        if self._year >= 1919:
            # Independence Day.
            self._add_holiday_dec_6(tr("Itsenäisyyspäivä"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Jouluaatto"))

        # Christmas Day.
        self._add_christmas_day(tr("Joulupäivä"))

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("Tapaninpäivä"))

    def _populate_unofficial_holidays(self):
        # Customary Flag Days.

        # Unofficial observance starts in 1854.
        # Starting in 1929, become a name day of J.L. Runeberg.
        # Added to the Almanac in 1950 as J.L. Runeberg's Day.
        # Become a Flag Day in 1976.
        if self._year >= 1976:
            # Runeberg Day.
            self._add_holiday_feb_5(tr("Runebergin päivä"))

        # Petition for Flag Day status starts in 2003.
        # Become a Flag Day in 2007.
        if self._year >= 2007:
            #  Minna Canth Day, Day of Equality.
            self._add_holiday_mar_19(tr("Minna Canthin päivä, tasa-arvon päivä"))

        # Added to the Almanac in 1960 as Mikael Agricola Day.
        # Also considered the "Day of Finnish Language" from 1980 onwards.
        # Become a Flag Day in 1980.
        if self._year >= 1980:
            # Mikael Agricola Day, Day of the Finnish Language.
            self._add_holiday_apr_9(tr("Mikael Agricolan päivä, suomen kielen päivä"))

        # Become a Flag Day in 1987.
        if self._year >= 1987:
            # National War Veterans' Day.
            self._add_holiday_apr_27(tr("Kansallinen veteraanipäivä"))

        # Become a Flag Day in 2019.
        if self._year >= 2019:
            # Europe Day.
            self._add_europe_day(tr("Eurooppa-päivä"))

        # Petition for Flag Day status starts in the 1920s.
        # Become a Flag Day in 1952.
        # Also considered the "Day of Finnish Heritage" from 1978 onward.
        if self._year >= 1952:
            self._add_holiday_may_12(
                # J. V. Snellman Day, Day of Finnish Heritage.
                tr("J.V. Snellmanin päivä, suomalaisuuden päivä")
                if self._year >= 1978
                # J. V. Snellman Day.
                else tr("J.V. Snellmanin päivä")
            )

        # Become a Flag Day in 1977.
        if self._year >= 1977:
            # Remembrance Day.
            self._add_holiday_3rd_sun_of_may(tr("Kaatuneitten muistopäivä"))

        # Become a Flag Day in 1998.
        if self._year >= 1998:
            # Eino Leino Day, Day of Summer and Poetry.
            self._add_holiday_jul_6(tr("Eino Leinon päivä, runon ja suven päivä"))

        # Added to the Almanac in 2020.
        # Become a Flag Day in 2023.
        if self._year >= 2023:
            # Finland's Nature Day.
            self._add_holiday_last_sat_of_aug(tr("Suomen luonnon päivä"))

        # Become a Flag Day in 2016.
        if self._year >= 2016:
            # Miina Sillanpää Day, Day of Civic Participation.
            self._add_holiday_oct_1(tr("Miina Sillanpään ja kansalaisvaikuttamisen päivä"))

        # Become a Flag Day in 1950.
        # Also considered the "Day of Finnish Literature" from 1978 onward.
        if self._year >= 1950:
            self._add_holiday_oct_10(
                # Aleksis Kivi Day, Day of Finnish Literature.
                tr("Aleksis Kiven päivä, suomalaisen kirjallisuuden päivä")
                if self._year >= 1978
                # Aleksis Kivi Day.
                else tr("Aleksis Kiven päivä")
            )

        # Become a Flag Day in 1987.
        if self._year >= 1987:
            # United Nations Day.
            self._add_united_nations_day(tr("YK:n päivä"))

        # Become a Flag Day in 1979.
        if self._year >= 1979:
            # Finnish Swedish Heritage Day, svenska dagen.
            self._add_holiday_nov_6(tr("Ruotsalaisuuden päivä, Kustaa Aadolfin päivä"))

        # Become a Customary Flag Day in 1987.
        # Become an Official Flag Day in 2019.
        if 1987 <= self._year <= 2018:
            # Father's Day.
            self._add_holiday_2nd_sun_of_nov(tr("Isänpäivä"))

        # Become a Flag Day in 2020.
        if self._year >= 2020:
            # Day of Children's Rights.
            self._add_holiday_nov_20(tr("Lapsen oikeuksien päivä"))

        # First recommendation starts in 2005.
        # Become a Flag Day in 2011.
        if self._year >= 2011:
            # Jean Sibelius Day, Day of Finnish Music.
            self._add_holiday_dec_8(tr("Jean Sibeliuksen päivä, suomalaisen musiikin päivä"))

    def _populate_workday_holidays(self):
        # Official Flag Days.

        # First general observance starts in 1885.
        # Become a Flag Day in 1920.
        # Added to the Almanac in 1950, confirmed by a decree in 1978.
        if self._year >= 1920:
            # Kalevala Day, Day of Finnish Culture.
            self._add_holiday_feb_28(tr("Kalevalan päivä, suomalaisen kulttuurin päivä"))

        # While May Day is already a Public Holiday since 1944, it gains Flag Day status in 1978,
        # although actual observance starts in 1979.
        if self._year >= 1979:
            # May Day.
            self._add_holiday_may_1(tr("Vappu"))

        # First observed in parts of Finland in 1918.
        # Moved from 3rd to 2nd Sunday of May in 1927.
        # Become Flag Day in 1947.
        if self._year >= 1947:
            # Mother's Day.
            self._add_holiday_2nd_sun_of_may(tr("Äitienpäivä"))

        # Become a Flag Day in 1942.
        # Got its current name in 1950.
        if self._year >= 1942:
            self._add_holiday_jun_6(
                # Flag Day of the Finnish Defense Forces.
                tr("Puolustusvoimain lippujuhlan päivä")
                if self._year >= 1950
                # Birthday of the Marshal of Finland.
                else tr("Suomen marsalkan syntymäpäivä")
            )

        # Day of the Finnish Flag was first created in 1934.
        # This coincides with Midsummer Day.

        # Day of the Finnish Flag.
        name = tr("Suomen lipun päivä")
        if self._year >= 1955:
            self._add_holiday_1st_sat_from_jun_20(name)
        elif self._year >= 1934:
            self._add_holiday_jun_24(name)

        # Become a Customary Flag Day in 1987.
        # Become an Official Flag Day in 2019.
        if self._year >= 2019:
            # Father's Day.
            self._add_holiday_2nd_sun_of_nov(tr("Isänpäivä"))

        # Flag Day status assumed to start in 1919.
        if self._year >= 1919:
            # Independence Day.
            self._add_holiday_dec_6(tr("Itsenäisyyspäivä"))

    def _populate_subdiv_01_public_holidays(self):
        # Celebrated since 1993 when the 70th anniversary of the Autonomy Act of 1920 was
        # first formally recognized as a flag day.
        if self._year >= 1993:
            # Åland's Autonomy Day.
            self._add_holiday_jun_9(tr("Ahvenanmaan itsehallintopäivä"))


class FI(Finland):
    pass


class FIN(Finland):
    pass
