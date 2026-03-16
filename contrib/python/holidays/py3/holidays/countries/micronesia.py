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
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_TO_PREV_FRI, SUN_TO_NEXT_MON


class Micronesia(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Micronesia holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_Federated_States_of_Micronesia>
        * <https://web.archive.org/web/20250605035443/https://www.timeanddate.com/holidays/micronesia/>
        * [2014 FSM Code](https://web.archive.org/web/20241213112548/http://www.fsmlaw.org/fsm/code/PDF/FSMCA2014Tit01.pdf)
        * [Section 601, 602, 603](https://web.archive.org/web/20020731194201/http://fsmlaw.org/fsm/code/title01/t01ch06.htm)
        * [United Nations Day](https://web.archive.org/web/20241213120703/http://www.fsmlaw.org/fsm/code/PDF/CODE%201/PL%207-20.pdf)
        * [FSM Veterans of Foreign Wars Day](https://web.archive.org/web/20241213120631/http://www.fsmlaw.org/fsm/code/PDF/CODE%201/PL%2013-38.pdf)
        * [Micronesian Culture and Tradition Day](https://web.archive.org/web/20241213114748/http://fsmlaw.org/fsm/code/PDF/CODE%201/PL%2016-27.pdf)
        * [Presidents Day](https://web.archive.org/web/20241213120719/http://www.fsmlaw.org/fsm/code/PDF/CODE%201/PL%2021-209.pdf)

    Subdivisions Holidays References:
        * Chuuk:
            * [Chuuk State Code, T. 1, Chap. 3](https://web.archive.org/web/20250214151354/http://fsmlaw.org/chuuk/code/title01/T01_Ch03.htm)
            * [State Charter Day](https://web.archive.org/web/20241213113932/http://fsmlaw.org/chuuk/pdf/tsl_a/TSL%203-10%20EV.pdf)
            * [Chuuk State Constitution Day](https://web.archive.org/web/20241213113821/http://fsmlaw.org/chuuk/pdf/csl_a/CSL%20190-03%20EV.pdf)
        * Kosrae:
            * [Kosrae State Code, Section 2.501](https://web.archive.org/web/20250211055635/http://fsmlaw.org/kosrae/code/title02/t02c05.htm)
            * [Seventh Korsae State Legislature](https://web.archive.org/web/20241213115951/https://fsmlaw.org/kosrae/Law/pdf/123.pdf)
            * [Fourth Kosrae State Legislature](https://web.archive.org/web/20241213115949/https://fsmlaw.org/kosrae/Law/pdf/Kosrae%20State%20Law%201989-90.pdf)
            * [State Law 4-32](https://web.archive.org/web/20250211151328/https://www.kosraestatelegislature.com/4th-ksl-state-laws)
            * [State Law 7-76](https://web.archive.org/web/20250211151710/https://www.kosraestatelegislature.com/7th-ksl-state-laws)
            * [State Law 9-50](https://web.archive.org/web/20250430191523/https://www.kosraestatelegislature.com/9th-ksl-state-laws)
            * [State Law 13-79](https://web.archive.org/web/20241211232231/https://www.kosraestatelegislature.com/_files/ugd/f1e94d_cc244ef15bea4c74ba81cb1a4621c30f.pdf)
        * Pohnpei:
            * [State Code 2012](https://web.archive.org/web/20250209195119/https://fsmlaw.org/pohnpei/code/pdf/pohnpei%20state%202012%20code.pdf)
            * [2025](https://web.archive.org/web/20250124225740/https://pohnpeistate.gov.fm/fy-2025-holiday-schedule/)
            * [Women's Day](https://web.archive.org/web/20250403093235/https://pohnpeistate.gov.fm/2022/02/22/congratulations-pohnpei-womens-council-and-all-who-supported-this-important-undertaking/)
        * [Yap](https://web.archive.org/web/20250223222339/http://fsmlaw.org/yap/code/title01/T01_Ch08.htm)

        According to Section 602:

        > All holidays set forth in section 601 shall, if they occur on a Saturday,
        > be observed on the preceding Friday, and shall, if they occur on a Sunday,
        > be observed on the following Monday.

        According to Kosrae State Legislature State Laws:

        * 4-32: Government Holidays were amended.
        * 4-135: September 8 was declared a public holiday.
        * 7-27: Good Friday was declared a public holiday.
        * 7-76: Gospel Day was declared a public holiday.
        * 9-50: Thanksgiving Day was declared a public holiday.
        * 13-79: Kosrae Disability Day was declared a public holiday.

        According to Pohnpei State Code 2012 Section 7-101:

        * Pohnpei Constitution Day is a public holiday.
        * Liberation Day is a public holiday.
        * Pohnpei Cultural Day is a public holiday.
        * Good Friday is a public holiday.
    """

    country = "FM"
    default_language = "en_FM"
    # %s (observed).
    observed_label = tr("%s (observed)")
    # Federated States of Micronesia gained independence on November 3, 1986.
    start_year = 1987
    subdivisions = (
        "KSA",  # Kosrae.
        "PNI",  # Pohnpei.
        "TRK",  # Chuuk.
        "YAP",  # Yap.
    )
    subdivisions_aliases = {
        "Kosrae": "KSA",
        "Kusaie": "KSA",
        "Pohnpei": "PNI",
        "Ponape": "PNI",
        "Chuuk": "TRK",
        "Truk": "TRK",
        "Yap": "YAP",
    }
    supported_languages = ("en_FM", "en_US")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SAT_TO_PREV_FRI + SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        name = tr("New Year's Day")
        self._add_observed(self._add_new_years_day(name))
        self._add_observed(self._next_year_new_years_day, name=name, rule=SAT_TO_PREV_FRI)

        if self._year >= 2011:
            self._add_observed(
                # Micronesian Culture and Tradition Day.
                self._add_holiday_mar_31(tr("Micronesian Culture and Tradition Day"))
            )

        # Federated States of Micronesia Day.
        self._add_observed(self._add_holiday_may_10(tr("Federated States of Micronesia Day")))

        if self._year >= 1991:
            # United Nations Day.
            self._add_observed(self._add_united_nations_day(tr("United Nations Day")))

        # Independence Day.
        self._add_observed(self._add_holiday_nov_3(tr("Independence Day")))

        if self._year >= 2004:
            # FSM Veterans of Foreign Wars Day.
            self._add_observed(self._add_remembrance_day(tr("FSM Veterans of Foreign Wars Day")))

        if self._year >= 2021:
            # Presidents Day.
            self._add_observed(self._add_holiday_nov_23(tr("Presidents Day")))

        # Christmas Day.
        self._add_observed(self._add_christmas_day(tr("Christmas Day")))

    # Kosrae.
    def _populate_subdiv_ksa_public_holidays(self):
        if self._year >= 1991:
            # Kosrae State Constitution Day.
            self._add_observed(self._add_holiday_jan_11(tr("Kosrae State Constitution Day")))

        if self._year >= 2000:
            # Good Friday.
            self._add_good_friday(tr("Good Friday"))

            # Gospel Day.
            self._add_observed(self._add_holiday_aug_21(tr("Gospel Day")))

        if self._year >= 1991:
            # Kosrae Liberation Day.
            self._add_observed(self._add_holiday_sep_8(tr("Kosrae Liberation Day")))

        # Self Government Day.
        self._add_observed(self._add_holiday_nov_3(tr("Self Government Day")))

        if self._year >= 2011:
            # Thanksgiving Day.
            self._add_holiday_4th_thu_of_nov(tr("Thanksgiving Day"))

        if self._year >= 2024:
            # Kosrae Disability Day.
            self._add_observed(self._add_holiday_dec_3(tr("Kosrae Disability Day")))

    # Pohnpei.
    def _populate_subdiv_pni_public_holidays(self):
        if self._year >= 2022:
            # Women's Day.
            self._add_observed(self._add_womens_day(tr("Women's Day")))

        # Pohnpei Cultural Day.
        self._add_observed(self._add_holiday_mar_31(tr("Pohnpei Cultural Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Liberation Day.
        self._add_observed(self._add_holiday_sep_11(tr("Liberation Day")))

        # Pohnpei Constitution Day.
        self._add_observed(self._add_holiday_nov_8(tr("Pohnpei Constitution Day")))

    # Chuuk.
    def _populate_subdiv_trk_public_holidays(self):
        # State Charter Day.
        self._add_observed(self._add_holiday_sep_26(tr("State Charter Day")))

        if self._year >= 1990:
            # Chuuk State Constitution Day.
            self._add_observed(self._add_holiday_oct_1(tr("Chuuk State Constitution Day")))

    # Yap.
    def _populate_subdiv_yap_public_holidays(self):
        # Yap Day.
        self._add_observed(self._add_holiday_mar_1(tr("Yap Day")))

        # Yap State Constitution Day.
        self._add_observed(self._add_holiday_dec_24(tr("Yap State Constitution Day")))


class FM(Micronesia):
    pass


class FSM(Micronesia):
    pass
