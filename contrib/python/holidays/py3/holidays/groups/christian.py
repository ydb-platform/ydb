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

from datetime import date

from dateutil.easter import EASTER_ORTHODOX, EASTER_WESTERN, easter

from holidays.calendars.ethiopian import ETHIOPIAN_CALENDAR, is_ethiopian_leap_year
from holidays.calendars.gregorian import GREGORIAN_CALENDAR, JAN, AUG, SEP, DEC, _timedelta
from holidays.calendars.julian import JULIAN_CALENDAR, julian_calendar_drift
from holidays.calendars.julian_revised import JULIAN_REVISED_CALENDAR


class ChristianHolidays:
    """
    Christian holidays.
    """

    def __init__(self, calendar=GREGORIAN_CALENDAR) -> None:
        self.__verify_calendar(calendar)
        self.__calendar = calendar

    def __get_christmas_day(self, calendar=None):
        """
        Get Christmas Day date.
        """
        calendar = calendar or self.__calendar
        self.__verify_calendar(calendar)

        return (
            _timedelta(date(self._year, JAN, 7), julian_calendar_drift(self._year - 1))
            if self.__is_julian_calendar(calendar) or self.__is_ethiopian_calendar(calendar)
            else date(self._year, DEC, 25)
        )

    def __get_easter_sunday(self, calendar=None):
        """
        Get Easter Sunday date.
        """
        calendar = calendar or self.__calendar
        self.__verify_calendar(calendar)

        return easter(
            self._year,
            method=EASTER_WESTERN if self.__is_gregorian_calendar(calendar) else EASTER_ORTHODOX,
        )

    @staticmethod
    def __is_ethiopian_calendar(calendar):
        """
        Return True if `calendar` is Ethiopian calendar.
        Return False otherwise.
        """
        return calendar == ETHIOPIAN_CALENDAR

    @staticmethod
    def __is_gregorian_calendar(calendar):
        """
        Return True if `calendar` is Gregorian calendar.
        Return False otherwise.
        """
        return calendar == GREGORIAN_CALENDAR

    @staticmethod
    def __is_julian_calendar(calendar):
        """
        Return True if `calendar` is Julian calendar.
        Return False otherwise.
        """
        return calendar == JULIAN_CALENDAR

    @staticmethod
    def __verify_calendar(calendar):
        """
        Verify calendar type.
        """
        if calendar not in {
            ETHIOPIAN_CALENDAR,
            GREGORIAN_CALENDAR,
            JULIAN_CALENDAR,
            JULIAN_REVISED_CALENDAR,
        }:
            raise ValueError(
                f"Unknown calendar name: {calendar}. Use `{ETHIOPIAN_CALENDAR}`, "
                f"`{GREGORIAN_CALENDAR}`, `{JULIAN_CALENDAR}` or `{JULIAN_REVISED_CALENDAR}`."
            )

    @property
    def _christmas_day(self):
        """
        Return Christmas Day date.
        """
        return self.__get_christmas_day()

    @property
    def _easter_sunday(self):
        """
        Return Easter Sunday date.
        """
        return self.__get_easter_sunday()

    def _add_all_saints_day(self, name) -> date:
        """
        Add All Saints' Day (November 1st).

        Also known as All Hallows' Day, the Feast of All Saints,
        the Feast of All Hallows, the Solemnity of All Saints, and Hallowmas.
        https://en.wikipedia.org/wiki/All_Saints'_Day
        """
        return self._add_holiday_nov_1(name)

    def _add_all_souls_day(self, name) -> date:
        """
        Add All Souls' Day (November 2nd).

        All Souls' Day is a day of prayer and remembrance for the faithful
        departed, observed by certain Christian denominations on 2 November.
        In Belarussian tradition it is called Dziady.
        https://en.wikipedia.org/wiki/All_Souls'_Day
        https://en.wikipedia.org/wiki/Dziady
        """
        return self._add_holiday_nov_2(name)

    def _add_ascension_thursday(self, name, calendar=None) -> date:
        """
        Add Ascension Thursday (39 days after the Easter Sunday).

        The Solemnity of the Ascension of Jesus Christ, also called Ascension
        Day, or sometimes Holy Thursday.
        https://en.wikipedia.org/wiki/Feast_of_the_Ascension
        """
        return self._add_holiday(name, _timedelta(self.__get_easter_sunday(calendar), +39))

    def _add_ash_monday(self, name) -> date:
        """
        Add Ash Monday (48 days before Easter Sunday).

        The Clean Monday, also known as Pure Monday, Monday of Lent
        or Green Monday. The first day of Great Lent.
        https://en.wikipedia.org/wiki/Clean_Monday
        """
        return self._add_holiday(name, _timedelta(self._easter_sunday, -48))

    def _add_ash_wednesday(self, name) -> date:
        """
        Add Ash Wednesday (46 days before Easter Sunday).

        A holy day of prayer and fasting. It marks the beginning of Lent.
        https://en.wikipedia.org/wiki/Ash_Wednesday
        """
        return self._add_holiday(name, _timedelta(self._easter_sunday, -46))

    def _add_assumption_of_mary_day(self, name, calendar=None) -> date:
        """
        Add Assumption Of Mary (August 15th).

        The Feast of the Assumption of Mary, or simply The Assumption marks the
        occasion of the Virgin Mary's bodily ascent to heaven at the end of
        her life.
        https://en.wikipedia.org/wiki/Assumption_of_Mary
        """
        calendar = calendar or self.__calendar
        self.__verify_calendar(calendar)

        return (
            self._add_holiday(
                name, _timedelta(date(self._year, AUG, 28), julian_calendar_drift(self._year))
            )
            if self.__is_julian_calendar(calendar)
            else self._add_holiday_aug_15(name)
        )

    def _add_candlemas(self, name) -> date:
        """
        Add Candlemas (February 2nd).

        Also known as the Feast of the Presentation of Jesus Christ,
        the Feast of the Purification of the Blessed Virgin Mary, or the Feast
        of the Holy Encounter, is a Christian holiday commemorating the
        presentation of Jesus at the Temple.
        https://en.wikipedia.org/wiki/Candlemas
        """
        return self._add_holiday_feb_2(name)

    def _add_carnival_sunday(self, name) -> date:
        """
        Add Carnival Sunday (49 days before Easter Sunday).

        Carnival is a Catholic Christian festive season that occurs before
        the liturgical season of Lent.
        https://en.wikipedia.org/wiki/Carnival
        """
        return self._add_holiday(name, _timedelta(self._easter_sunday, -49))

    def _add_carnival_monday(self, name) -> date:
        """
        Add Carnival Monday (48 days before Easter Sunday).

        Carnival is a Catholic Christian festive season that occurs before
        the liturgical season of Lent.
        https://en.wikipedia.org/wiki/Carnival
        """
        return self._add_holiday(name, _timedelta(self._easter_sunday, -48))

    def _add_carnival_tuesday(self, name) -> date:
        """
        Add Carnival Tuesday (47 days before Easter Sunday).

        Carnival is a Catholic Christian festive season that occurs before
        the liturgical season of Lent.
        https://en.wikipedia.org/wiki/Carnival
        """
        return self._add_holiday(name, _timedelta(self._easter_sunday, -47))

    def _add_christmas_day(self, name, calendar=None) -> date:
        """
        Add Christmas Day.

        Christmas is an annual festival commemorating the birth of
        Jesus Christ.
        https://en.wikipedia.org/wiki/Christmas
        """
        return self._add_holiday(name, self.__get_christmas_day(calendar))

    def _add_christmas_day_two(self, name, calendar=None) -> date:
        """
        Add Christmas Day 2.

        A holiday celebrated after Christmas Day, also known as Boxing Day.
        https://en.wikipedia.org/wiki/Boxing_Day
        https://en.wikipedia.org/wiki/Christmas
        """
        return self._add_holiday(name, _timedelta(self.__get_christmas_day(calendar), +1))

    def _add_christmas_day_three(self, name, calendar=None) -> date:
        """
        Add Christmas Day 3.

        A holiday celebrated 2 days after Christmas Day (in some countries).
        https://en.wikipedia.org/wiki/Christmas
        """
        return self._add_holiday(name, _timedelta(self.__get_christmas_day(calendar), +2))

    def _add_christmas_eve(self, name, calendar=None) -> date:
        """
        Add Christmas Eve.

        Christmas Eve is the evening or entire day before Christmas Day,
        the festival commemorating the birth of Jesus Christ.
        https://en.wikipedia.org/wiki/Christmas_Eve
        """
        return self._add_holiday(name, _timedelta(self.__get_christmas_day(calendar), -1))

    def _add_corpus_christi_day(self, name) -> date:
        """
        Add Feast Of Corpus Christi (60 days after Easter Sunday).

        The Feast of Corpus Christi, also known as the Solemnity of the Most
        Holy Body and Blood of Christ, is a Christian liturgical solemnity
        celebrating the Real Presence of the Body and Blood, Soul and Divinity
        of Jesus Christ in the elements of the Eucharist.
        https://en.wikipedia.org/wiki/Feast_of_Corpus_Christi
        """
        return self._add_holiday(name, _timedelta(self._easter_sunday, +60))

    def _add_easter_monday(self, name, calendar=None) -> date:
        """
        Add Easter Monday (1 day after Easter Sunday).

        Easter Monday refers to the day after Easter Sunday in either the
        Eastern or Western Christian traditions. It is a public holiday in
        some countries.
        https://en.wikipedia.org/wiki/Easter_Monday
        """
        return self._add_holiday(name, _timedelta(self.__get_easter_sunday(calendar), +1))

    def _add_easter_sunday(self, name, calendar=None) -> date:
        """
        Add Easter Sunday.

        Easter, also called Pascha or Resurrection Sunday is a Christian
        festival and cultural holiday commemorating the resurrection of Jesus
        from the dead.
        https://en.wikipedia.org/wiki/Easter
        """
        return self._add_holiday(name, self.__get_easter_sunday(calendar))

    def _add_easter_tuesday(self, name, calendar=None) -> date:
        """
        Add Easter Tuesday (2 day after Easter Sunday).

        Easter Tuesday is the third day of Eastertide and is a holiday in some areas.
        https://en.wikipedia.org/wiki/Easter_Tuesday
        """
        return self._add_holiday(name, _timedelta(self.__get_easter_sunday(calendar), +2))

    def _add_epiphany_day(self, name, calendar=None) -> date:
        """
        Add Epiphany Day.

        Epiphany, also known as Theophany in Eastern Christian traditions,
        is a Christian feast day that celebrates the revelation of God
        incarnate as Jesus Christ.
        https://en.wikipedia.org/wiki/Epiphany_(holiday)
        """
        calendar = calendar or self.__calendar
        self.__verify_calendar(calendar)

        if self.__is_julian_calendar(calendar) or self.__is_ethiopian_calendar(calendar):
            dt = _timedelta(date(self._year, JAN, 19), julian_calendar_drift(self._year - 1))
            return self._add_holiday(
                name,
                _timedelta(dt, +1)
                if self.__is_ethiopian_calendar(calendar)
                and is_ethiopian_leap_year(self._year - 1)
                else dt,
            )
        else:
            return self._add_holiday_jan_6(name)

    def _add_finding_of_true_cross(self, name) -> date:
        """
        Add Finding of True Cross.

        Finding of True Cross, also known as Meskel, is an Ethiopian and Eritrean Orthodox
        Tewahedo Church holiday that commemorates the discovery of the True Cross by the
        Roman Empress Saint Helena of Constantinople in the fourth century.
        https://en.wikipedia.org/wiki/Meskel
        """
        dt = _timedelta(date(self._year, SEP, 27), julian_calendar_drift(self._year))
        return self._add_holiday(
            name, _timedelta(dt, +1) if is_ethiopian_leap_year(self._year) else dt
        )

    def _add_good_friday(self, name, calendar=None) -> date:
        """
        Add Good Friday (2 days before Easter Sunday).

        Good Friday is a Christian holiday commemorating the crucifixion of
        Jesus and his death at Calvary. It is also known as Holy Friday,
        Great Friday, Great and Holy Friday.
        https://en.wikipedia.org/wiki/Good_Friday
        """
        return self._add_holiday(name, _timedelta(self.__get_easter_sunday(calendar), -2))

    def _add_holy_saturday(self, name, calendar=None) -> date:
        """
        Add Holy Saturday (1 day before Easter Sunday).

        Great and Holy Saturday is a day between Good Friday and Easter Sunday.
        https://en.wikipedia.org/wiki/Holy_Saturday
        """
        return self._add_holiday(name, _timedelta(self.__get_easter_sunday(calendar), -1))

    def _add_holy_thursday(self, name, calendar=None) -> date:
        """
        Add Holy Thursday (3 days before Easter Sunday).

        Holy Thursday or Maundy Thursday is the day during Holy Week that
        commemorates the Washing of the Feet (Maundy) and Last Supper of
        Jesus Christ with the Apostles, as described in the canonical gospels.
        https://en.wikipedia.org/wiki/Maundy_Thursday
        """
        return self._add_holiday(name, _timedelta(self.__get_easter_sunday(calendar), -3))

    def _add_immaculate_conception_day(self, name) -> date:
        """
        Add Immaculate Conception Day (December 8th).

        https://en.wikipedia.org/wiki/Immaculate_Conception
        """
        return self._add_holiday_dec_8(name)

    def _add_nativity_of_mary_day(self, name) -> date:
        """
        Add Nativity Of Mary Day (September 8th).

        The Nativity of the Blessed Virgin Mary, the Nativity of Mary,
        the Marymas or the Birth of the Virgin Mary, refers to a Christian
        feast day celebrating the birth of Mary, mother of Jesus.
        https://en.wikipedia.org/wiki/Nativity_of_Mary
        """
        return self._add_holiday_sep_8(name)

    def _add_palm_sunday(self, name, calendar=None) -> date:
        """
        Add Palm Sunday (7 days before Easter Sunday).

        Palm Sunday is a Christian moveable feast that falls on the Sunday
        before Easter. The feast commemorates Christ's triumphal entry into
        Jerusalem, an event mentioned in each of the four canonical Gospels.
        Palm Sunday marks the first day of Holy Week.
        https://en.wikipedia.org/wiki/Palm_Sunday
        """
        return self._add_holiday(name, _timedelta(self.__get_easter_sunday(calendar), -7))

    def _add_rejoicing_day(self, name) -> date:
        """
        Add Day Of Rejoicing (9 days after Easter Sunday).

        Add Day Of Rejoicing ("Radonitsa"), in the Russian Orthodox Church is
        a commemoration of the departed observed on the second Tuesday of
        Pascha (Easter). In Ukrainian tradition it is called Provody.
        https://en.wikipedia.org/wiki/Radonitsa
        """
        return self._add_holiday(name, _timedelta(self._easter_sunday, +9))

    def _add_saint_anthonys_day(self, name) -> date:
        """
        Add Saint Anthony of Padua's Day (June 13th).

        Saint Anthony's Day is celebrated on 13 June, the traditionally
        accepted date of the saint's death.
        https://en.wikipedia.org/wiki/Anthony_of_Padua
        """
        return self._add_holiday_jun_13(name)

    def _add_saint_georges_day(self, name) -> date:
        """
        Add Saint George's Day (April 23th).

        Saint George's Day is celebrated on 23 April, the traditionally
        accepted date of the saint's death.
        https://en.wikipedia.org/wiki/Saint_George's_Day
        """
        return self._add_holiday_apr_23(name)

    def _add_saint_james_day(self, name) -> date:
        """
        Add Saint James' Day (July 25th).

        James the Great was one of the Twelve Apostles of Jesus.
        https://en.wikipedia.org/wiki/James_the_Great#Feast
        """
        return self._add_holiday_jul_25(name)

    def _add_saint_johns_day(self, name) -> date:
        """
        Add Saint John's Day (June 24th).

        The Nativity of John the Baptist is a Christian feast day celebrating
        the birth of John the Baptist.
        https://en.wikipedia.org/wiki/Nativity_of_John_the_Baptist
        """
        return self._add_holiday_jun_24(name)

    def _add_saint_josephs_day(self, name) -> date:
        """
        Add Saint Joseph's Day (March 19th).

        Saint Joseph's Day, also called the Feast of Saint Joseph or the
        Solemnity of Saint Joseph, is in Western Christianity the principal
        feast day of Saint Joseph, husband of the Virgin Mary and legal father
        of Jesus Christ.
        https://en.wikipedia.org/wiki/Saint_Joseph's_Day
        """
        return self._add_holiday_mar_19(name)

    def _add_saint_martins_day(self, name) -> date:
        """
        Add Saint Martin of Tours Day (November 11th).

        Saint Martin of Tours is the patron saint of many communities and organizations
        across Europe, including France's Third Republic.
        https://en.wikipedia.org/wiki/Martin_of_Tours
        """
        return self._add_holiday_nov_11(name)

    def _add_saint_patricks_day(self, name) -> date:
        """
        Add Saint Patrick's Day (March 17th).

        Saint Patrick's Day is a religious and cultural holiday held on 17 March,
        the traditional death date of Saint Patrick, the foremost patron saint of Ireland.
        https://en.wikipedia.org/wiki/Saint_Patrick's_Day
        """
        return self._add_holiday_mar_17(name)

    def _add_saints_peter_and_paul_day(self, name) -> date:
        """
        Add Feast of Saints Peter and Paul (June 29th).

        A liturgical feast in honor of the martyrdom in Rome of the apostles
        Saint Peter and Saint Paul, which is observed on 29 June.
        https://en.wikipedia.org/wiki/Feast_of_Saints_Peter_and_Paul
        """
        return self._add_holiday_jun_29(name)

    def _add_whit_monday(self, name) -> date:
        """
        Add Whit Monday (50 days after Easter Sunday).

        Whit Monday or Pentecost Monday, also known as Monday of the
        Holy Spirit, is the holiday celebrated the day after Pentecost.
        https://en.wikipedia.org/wiki/Pentecost
        https://en.wikipedia.org/wiki/Whit_Monday
        """
        return self._add_holiday(name, _timedelta(self._easter_sunday, +50))

    def _add_whit_sunday(self, name, calendar=None) -> date:
        """
        Add Whit Sunday (49 days after Easter Sunday).

        Whit Sunday, also called Pentecost, is a holiday which commemorates
        the descent of the Holy Spirit upon the Apostles and other followers
        of Jesus Christ while they were in Jerusalem celebrating the
        Feast of Weeks.
        https://en.wikipedia.org/wiki/Pentecost
        """
        return self._add_holiday(name, _timedelta(self.__get_easter_sunday(calendar), +49))

    def _add_trinity_sunday(self, name) -> date:
        """
        Add Trinity Sunday (56 days after Easter Sunday).

        Trinity Sunday, also called Solemnity of Holy Trinity, is the first Sunday
        after Pentecost in the Western Christian liturgical calendar, and the Sunday
        of Pentecost in Eastern Christianity.
        """
        return self._add_holiday(name, _timedelta(self._easter_sunday, +56))
