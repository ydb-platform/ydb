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

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.constants import GOVERNMENT, PUBLIC, WORKDAY
from holidays.groups import (
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.holiday_base import HolidayBase


class TimorLeste(
    HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays, StaticHolidays
):
    """Timor Leste holidays.

    References:
        * [2005 Law](https://web.archive.org/web/20240813193212/https://mj.gov.tl/jornal/lawsTL/RDTL-Law/RDTL-Laws/Law-2005-10.pdf)
        * [2016 Amendment](https://web.archive.org/web/20250414152148/https://timor-leste.gov.tl/?p=14494&lang=en)
        * [2022](https://web.archive.org/web/20240802065911/https://timor-leste.gov.tl/?p=30266&lang=en)
        * [2023 (en_US)](https://web.archive.org/web/20250414071435/https://timor-leste.gov.tl/?lang=en&p=31750)
        * [2023 (pt_PT)](https://web.archive.org/web/20250416013718/https://timor-leste.gov.tl/?lang=pt&p=31750)
        * [2023 (tet)](https://web.archive.org/web/20250415232148/https://timor-leste.gov.tl/?p=31750&lang=tp)
        * [2024](https://web.archive.org/web/20250421164147/https://timor-leste.gov.tl/?p=35833&lang=en)
        * [2025](https://web.archive.org/web/20250416022640/https://timor-leste.gov.tl/?lang=en&p=41492)

    Limitations:
        * Exact Islamic holidays dates are only available for 2011-2025; the rest are estimates.
    """

    country = "TL"
    supported_categories = (GOVERNMENT, PUBLIC, WORKDAY)
    default_language = "pt_TL"
    # %s (estimated).
    estimated_label = tr("%s (aproximada)")
    supported_languages = ("en_TL", "en_US", "pt_TL", "tet", "th")
    # Law No. 10/2005 Of 10 August, Public Holidays and Official Commemorative Dates.
    start_year = 2006

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=TimorLesteIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, TimorLesteStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Fixed Date Public Holidays.

        # New Year's Day.
        self._add_new_years_day(tr("Dia de Ano Novo"))

        # Dia dos Veteranos.
        # First appeared in 2017.

        if self._year >= 2017:
            # Veteran's Day.
            self._add_holiday_mar_3(tr("Dia dos Veteranos"))

        # Good Friday.
        self._add_good_friday(tr("Sexta-Feira Santa"))

        # International Worker's Day.
        self._add_labor_day(tr("Dia Mundial do Trabalhador"))

        # Restoration of Independence Day.
        self._add_holiday_may_20(tr("Dia da Restauração da Independência"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Festa do Corpo de Deus"))

        # Popular Consultation Day.
        self._add_holiday_aug_30(tr("Dia da Consulta Popular"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Dia de Todos os Santos"))

        # All Souls' Day.
        self._add_all_souls_day(tr("Dia de Todos os Fiéis Defuntos"))

        # Dia Nacional da Mulher.
        # Originally classed as "Commemorative Date" only, reclassified in 2023.

        if self._year >= 2023:
            # National Women's Day.
            self._add_holiday_nov_3(tr("Dia Nacional da Mulher"))

        # National Youth Day.
        self._add_holiday_nov_12(tr("Dia Nacional da Juventude"))

        # Proclamation of Independence Day.
        self._add_holiday_nov_28(tr("Dia da Proclamação da Independência"))

        # Dia da Memória
        # Created to replaced the original National Heroes Day in 2017.

        if self._year >= 2017:
            # Memorial Day.
            self._add_holiday_dec_7(tr("Dia da Memória"))

        self._add_immaculate_conception_day(
            # Day of Our Lady of Immaculate Conception and Timor-Leste Patroness.
            tr("Dia de Nossa Senhora da Imaculada Conceição, padroeira de Timor-Leste")
        )

        # Christmas Day.
        self._add_christmas_day(tr("Dia de Natal"))

        # Dia dos Heróis Nacionais.
        # Moved to Dec 31 in 2017.

        # National Heroes Day.
        name = tr("Dia dos Heróis Nacionais")
        if self._year >= 2017:
            self._add_holiday_dec_31(name)
        else:
            self._add_holiday_dec_7(name)

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("Idul Fitri"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("Idul Adha"))

    def _populate_workday_holidays(self):
        # Ash Wednesday.
        self._add_ash_wednesday(tr("Quarta-Feira de Cinzas"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Quinta-Feira Santa"))

        # The Day of Ascension of Jesus Christ into Heaven.
        self._add_ascension_thursday(tr("Dia da Ascensão de Jesus Cristo ao Céu"))

        # International Children's Day.
        self._add_childrens_day(tr("Dia Mundial da Criança"))

        self._add_holiday_aug_20(
            # Day of the Armed Forces for the National Liberation of Timor-Leste (FALINTIL).
            tr("Dia das Forças Armadas de Libertação Nacional de Timor-Leste (FALINTIL)")
        )

        # Dia Nacional da Mulher.
        # Originally classed as "Commemorative Date" only, reclassified in 2023.
        # Prior to reclassification, this is usually only observed as half-day holiday. i.e.
        # 2010:
        # https://web.archive.org/web/20250427130447/https://timor-leste.gov.tl/?p=4183&lang=en
        # 2011:
        # https://web.archive.org/web/20250414065353/https://timor-leste.gov.tl/?p=5979&lang=en
        if self._year <= 2022:
            # National Women's Day.
            self._add_holiday_nov_3(tr("Dia Nacional da Mulher"))

        # International Human Rights Day.
        self._add_holiday_dec_10(tr("Dia Mundial dos Direitos Humanos"))


class TL(TimorLeste):
    pass


class TLS(TimorLeste):
    pass


class TimorLesteIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2011, 2025)
    EID_AL_ADHA_DATES = {
        2011: (NOV, 7),
        2015: (SEP, 24),
        2016: (SEP, 18),
        2021: (JUL, 19),
        2023: (JUN, 29),
        2024: (JUN, 17),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2011, 2025)
    EID_AL_FITR_DATES = {
        2011: (AUG, 31),
        2012: (AUG, 20),
        2016: (JUL, 7),
        2017: (JUN, 26),
        2019: (JUN, 6),
        2023: (APR, 22),
        2025: (MAR, 31),
    }


class TimorLesteStaticHolidays:
    # Special Holidays.

    # National Holidays (Special).
    special_national_holidays = tr("Feriados Nacionais (Especiais)")

    # Presidential Election Day.
    presidential_election = tr("Dia da Eleição Presidencial")

    # Parliamentary Election Day.
    parliamentary_election = tr("Dia de Eleições Parlamentares")

    # Local Election Day.
    local_election = tr("Dia de eleições locais")

    # Centenary of the Revolt of Dom Boaventura.
    dom_boaventura_centenary = tr("Centenário da Revolta de Dom Boaventura")

    # Funeral Ceremonies of Fernando 'La Sama' de Araújo.
    la_sama_funeral = tr("Cerimónias Fúnebres de Fernando 'La Sama' de Araújo")

    # 20th Anniversary Celebrations of the Popular Consultation.
    popular_consultation_20th = tr("Celebrações do 20.º Aniversário da Consulta Popular")

    # 25th Anniversary Celebrations of the Popular Consultation.
    popular_consultation_25th = tr("Celebrações do 25.º Aniversário da Consulta Popular")

    # Visit of His Holiness Pope Francis to Timor-Leste.
    pope_francis_visit = tr("Visita de Sua Santidade o Papa Francisco a Timor-Leste")

    special_government_holidays = {
        2010: (
            # https://web.archive.org/web/20250427130447/https://timor-leste.gov.tl/?p=4183&lang=en
            (NOV, 3, special_national_holidays),
            # https://web.archive.org/web/20250414065444/https://timor-leste.gov.tl/?p=4437&lang=en
            (DEC, 24, special_national_holidays),
            (DEC, 31, special_national_holidays),
        ),
        2011: (
            # https://web.archive.org/web/20250414065414/https://timor-leste.gov.tl/?p=5496&lang=en
            (AUG, 15, special_national_holidays),
            # https://web.archive.org/web/20250414065353/https://timor-leste.gov.tl/?p=5979&lang=en
            (NOV, 3, special_national_holidays),
            # https://web.archive.org/web/20250427130448/https://timor-leste.gov.tl/?p=6264&lang=en
            (DEC, 26, special_national_holidays),
        ),
        2012: (
            # https://web.archive.org/web/20250427130448/https://timor-leste.gov.tl/?p=6264&lang=en
            (JAN, 2, special_national_holidays),
            # https://web.archive.org/web/20250414065422/https://timor-leste.gov.tl/?p=6347&lang=en
            (JAN, 23, special_national_holidays),
            # https://web.archive.org/web/20250414065452/https://timor-leste.gov.tl/?p=6471&lang=en
            (FEB, 22, special_national_holidays),
            # https://web.archive.org/web/20250427130503/https://timor-leste.gov.tl/?p=6621&lang=en
            (MAR, 16, presidential_election),
            # https://web.archive.org/web/20250414065432/https://timor-leste.gov.tl/?p=6760&lang=en
            (APR, 16, presidential_election),
            (APR, 17, presidential_election),
            # https://web.archive.org/web/20250427130515/https://timor-leste.gov.tl/?p=7035&lang=en
            (JUL, 6, parliamentary_election),
            # https://web.archive.org/web/20250427130516/https://timor-leste.gov.tl/?p=7046&lang=en
            (JUL, 9, parliamentary_election),
            # https://web.archive.org/web/20250414065639/https://timor-leste.gov.tl/?p=7474&lang=en
            (NOV, 27, dom_boaventura_centenary),
            (NOV, 29, dom_boaventura_centenary),
            # https://web.archive.org/web/20250414065642/http://timor-leste.gov.tl/?p=7550&lang=en
            (DEC, 24, special_national_holidays),
            (DEC, 26, special_national_holidays),
            (DEC, 31, special_national_holidays),
        ),
        2013: (
            # https://web.archive.org/web/20250427130528/https://timor-leste.gov.tl/?p=7715&lang=en
            (FEB, 13, special_national_holidays),
            # https://web.archive.org/web/20250427130538/https://timor-leste.gov.tl/?p=7918&lang=en
            (MAR, 28, special_national_holidays),
            (APR, 1, special_national_holidays),
            # https://web.archive.org/web/20250414065612/https://timor-leste.gov.tl/?p=8664&lang=en
            (AUG, 20, special_national_holidays),
            # https://web.archive.org/web/20250414065748/https://timor-leste.gov.tl/?p=9392&lang=en
            (NOV, 29, special_national_holidays),
            # https://web.archive.org/web/20250414065744/http://timor-leste.gov.tl/?p=9475&lang=en
            (DEC, 24, special_national_holidays),
            (DEC, 26, special_national_holidays),
            (DEC, 31, special_national_holidays),
        ),
        2014: (
            # https://web.archive.org/web/20250427130548/https://timor-leste.gov.tl/?p=9759&lang=en
            (MAR, 5, special_national_holidays),
            # https://web.archive.org/web/20210826174115/http://timor-leste.gov.tl/?p=9964&lang=en
            (APR, 17, special_national_holidays),
            (APR, 21, special_national_holidays),
            # https://web.archive.org/web/20250427130153/http://timor-leste.gov.tl/?p=10294&lang=en
            (JUL, 22, special_national_holidays),
            (JUL, 23, special_national_holidays),
            # https://web.archive.org/web/20250414065707/https://timor-leste.gov.tl/?p=10524&lang=en
            (AUG, 15, special_national_holidays),
            (AUG, 20, special_national_holidays),
            # https://web.archive.org/web/20250427130202/https://timor-leste.gov.tl/?p=11036&lang=en
            (DEC, 24, special_national_holidays),
            (DEC, 26, special_national_holidays),
            (DEC, 31, special_national_holidays),
        ),
        2015: (
            # https://web.archive.org/web/20250427130202/https://timor-leste.gov.tl/?p=11036&lang=en
            (JAN, 2, special_national_holidays),
            # https://web.archive.org/web/20250414065729/https://timor-leste.gov.tl/?p=11247&lang=en
            (FEB, 18, special_national_holidays),
            # https://web.archive.org/web/20250414065741/https://timor-leste.gov.tl/?p=11544&lang=en
            (APR, 2, special_national_holidays),
            # https://web.archive.org/web/20250414065752/https://timor-leste.gov.tl/?p=11966&lang=en
            (MAY, 13, special_national_holidays),
            # https://web.archive.org/web/20230624130431/http://timor-leste.gov.tl/?p=12246&lang=en
            (JUN, 5, la_sama_funeral),
            # https://web.archive.org/web/20250414065950/https://timor-leste.gov.tl/?p=13105&lang=en
            (AUG, 20, special_national_holidays),
            # https://web.archive.org/web/20250414065829/https://timor-leste.gov.tl/?p=14271&lang=en
            (DEC, 24, special_national_holidays),
            (DEC, 31, special_national_holidays),
        ),
        2016: (
            # https://web.archive.org/web/20240816060741/https://timor-leste.gov.tl/?p=14482&lang=en
            (FEB, 10, special_national_holidays),
            # https://web.archive.org/web/20250414070020/https://timor-leste.gov.tl/?p=14827&lang=en
            (MAR, 24, special_national_holidays),
            # https://web.archive.org/web/20250414065859/https://timor-leste.gov.tl/?p=15740&lang=en
            (JUL, 6, special_national_holidays),
            # https://web.archive.org/web/20250414070052/https://timor-leste.gov.tl/?p=16626&lang=en
            (NOV, 3, special_national_holidays),
            # https://web.archive.org/web/20250427130231/https://timor-leste.gov.tl/?p=16998&lang=en
            (DEC, 26, special_national_holidays),
        ),
        2017: (
            # https://web.archive.org/web/20250427130231/https://timor-leste.gov.tl/?p=16998&lang=en
            (JAN, 2, special_national_holidays),
            # https://web.archive.org/web/20250427130238/https://timor-leste.gov.tl/?p=17428&lang=en
            (MAR, 1, special_national_holidays),
            # https://web.archive.org/web/20250427130247/https://timor-leste.gov.tl/?p=17548&lang=en
            (MAR, 20, presidential_election),
            (MAR, 21, presidential_election),
            # https://web.archive.org/web/20250414070116/https://timor-leste.gov.tl/?p=17698&lang=en
            (APR, 13, special_national_holidays),
            # https://web.archive.org/web/20250427130254/https://timor-leste.gov.tl/?p=19189&lang=en
            (DEC, 26, special_national_holidays),
        ),
        2018: (
            # https://web.archive.org/web/20250427130254/https://timor-leste.gov.tl/?p=19189&lang=en
            (JAN, 2, special_national_holidays),
            # https://web.archive.org/web/20240813145627/https://timor-leste.gov.tl/?p=19411&lang=en
            (FEB, 14, special_national_holidays),
            # https://web.archive.org/web/20250414070149/https://timor-leste.gov.tl/?p=19452&lang=en
            (FEB, 16, special_national_holidays),
            # https://web.archive.org/web/20240813145438/https://timor-leste.gov.tl/?p=19693&lang=en
            (MAR, 29, special_national_holidays),
            # https://web.archive.org/web/20250414070232/https://timor-leste.gov.tl/?p=20199&lang=en
            (AUG, 22, special_national_holidays),
        ),
        2019: (
            # https://web.archive.org/web/20250414070216/https://timor-leste.gov.tl/?p=21116&lang=en
            (FEB, 5, special_national_holidays),
            # https://web.archive.org/web/20250414070241/https://timor-leste.gov.tl/?p=21207&lang=en
            (MAR, 6, special_national_holidays),
            # https://web.archive.org/web/20240812151629/https://timor-leste.gov.tl/?p=21607&lang=en
            (APR, 18, special_national_holidays),
            # https://web.archive.org/web/20250427130325/https://timor-leste.gov.tl/?p=22642&lang=en
            (AUG, 12, special_national_holidays),
            # https://web.archive.org/web/20250414070318/https://timor-leste.gov.tl/?p=22681&lang=en
            (AUG, 20, special_national_holidays),
            # https://web.archive.org/web/20240812044011/https://timor-leste.gov.tl/?p=22701&lang=en
            (AUG, 26, popular_consultation_20th),
            (AUG, 27, popular_consultation_20th),
            (AUG, 28, popular_consultation_20th),
            (AUG, 29, popular_consultation_20th),
            # https://web.archive.org/web/20250414070227/https://timor-leste.gov.tl/?p=23277&lang=en
            (OCT, 31, special_national_holidays),
            # https://web.archive.org/web/20250414070409/https://timor-leste.gov.tl/?p=23417&lang=en
            (DEC, 24, special_national_holidays),
            (DEC, 26, special_national_holidays),
            (DEC, 30, special_national_holidays),
        ),
        2020: (
            # https://web.archive.org/web/20250414070409/https://timor-leste.gov.tl/?p=23417&lang=en
            (JAN, 2, special_national_holidays),
            # https://web.archive.org/web/20250427130330/https://timor-leste.gov.tl/?p=23607&lang=en
            (FEB, 26, special_national_holidays),
            # https://web.archive.org/web/20250427130347/https://timor-leste.gov.tl/?p=25455&lang=en
            (AUG, 20, special_national_holidays),
            # https://web.archive.org/web/20250414070335/https://timor-leste.gov.tl/?p=25502&lang=en
            (AUG, 31, special_national_holidays),
            # https://web.archive.org/web/20250414070433/https://timor-leste.gov.tl/?p=26030&lang=en
            (NOV, 3, special_national_holidays),
            # https://web.archive.org/web/20250427130521/https://timor-leste.gov.tl/?p=26365&lang=en
            (DEC, 24, special_national_holidays),
        ),
        2021: (
            # https://web.archive.org/web/20250414070431/https://timor-leste.gov.tl/?p=26865&lang=en
            (FEB, 12, special_national_holidays),
            # https://web.archive.org/web/20250414070435/https://timor-leste.gov.tl/?p=26896&lang=en
            (FEB, 17, special_national_holidays),
            # https://web.archive.org/web/20250414070527/https://timor-leste.gov.tl/?p=29682&lang=en
            (NOV, 3, special_national_holidays),
        ),
        2022: (
            # https://web.archive.org/web/20250414070535/https://timor-leste.gov.tl/?p=30029&lang=en
            (FEB, 1, special_national_holidays),
            # https://web.archive.org/web/20250414070533/https://timor-leste.gov.tl/?p=30194&lang=en
            (MAR, 2, special_national_holidays),
            # https://web.archive.org/web/20250414070535/https://timor-leste.gov.tl/?p=30254&lang=en
            (MAR, 18, presidential_election),
            # https://web.archive.org/web/20250414070614/https://timor-leste.gov.tl/?p=30429&lang=en
            (APR, 14, special_national_holidays),
            (APR, 18, presidential_election),
            (APR, 19, presidential_election),
            (APR, 20, presidential_election),
            # https://web.archive.org/web/20250414070557/https://timor-leste.gov.tl/?p=31107&lang=en
            (AUG, 29, special_national_holidays),
            # https://web.archive.org/web/20250414070537/https://timor-leste.gov.tl/?p=31152&lang=en
            (SEP, 6, special_national_holidays),
            # https://web.archive.org/web/20250414070629/https://timor-leste.gov.tl/?p=31404&lang=en
            (OCT, 31, special_national_holidays),
            # https://web.archive.org/web/20250427130438/https://timor-leste.gov.tl/?p=31574&lang=en
            (DEC, 9, special_national_holidays),
            # https://web.archive.org/web/20250414070644/https://timor-leste.gov.tl/?p=31633&lang=en
            (DEC, 26, special_national_holidays),
        ),
        2023: (
            # https://web.archive.org/web/20250414070658/https://timor-leste.gov.tl/?p=31641&lang=en
            (JAN, 2, special_national_holidays),
            # https://web.archive.org/web/20250414070652/https://timor-leste.gov.tl/?p=31798&lang=en
            (JAN, 23, special_national_holidays),
            # https://web.archive.org/web/20250414070730/https://timor-leste.gov.tl/?p=32191&lang=en
            (FEB, 22, special_national_holidays),
            # https://web.archive.org/web/20250414070922/https://timor-leste.gov.tl/?p=32561&lang=en
            (APR, 6, special_national_holidays),
            (APR, 10, special_national_holidays),
            # https://web.archive.org/web/20250414070723/https://timor-leste.gov.tl/?p=32590&lang=en
            (APR, 20, special_national_holidays),
            (APR, 21, special_national_holidays),
            # https://web.archive.org/web/20250414070814/https://timor-leste.gov.tl/?p=32617&lang=en
            (MAY, 19, parliamentary_election),
            (MAY, 22, parliamentary_election),
            # https://web.archive.org/web/20250414035027/https://timor-leste.gov.tl/?p=34792&lang=en
            (OCT, 27, local_election),
            # https://web.archive.org/web/20250414070835/https://timor-leste.gov.tl/?p=35060&lang=en
            (NOV, 13, local_election),
            # https://web.archive.org/web/20250429134659/https://timor-leste.gov.tl/?p=35627&lang=en&
            (DEC, 26, special_national_holidays),
        ),
        2024: (
            # https://web.archive.org/web/20250417085916/https://timor-leste.gov.tl/?p=35627&lang=en
            (JAN, 2, special_national_holidays),
            # https://web.archive.org/web/20250414070914/https://timor-leste.gov.tl/?p=36002&lang=en
            (FEB, 14, special_national_holidays),
            # https://web.archive.org/web/20250420193703/https://timor-leste.gov.tl/?p=36859&lang=en
            (MAR, 28, special_national_holidays),
            # https://web.archive.org/web/20250414121144/https://timor-leste.gov.tl/?p=39062&lang=en
            (AUG, 28, popular_consultation_25th),
            (AUG, 29, popular_consultation_25th),
            # https://web.archive.org/web/20250414070900/https://timor-leste.gov.tl/?p=39068&lang=en
            (SEP, 9, pope_francis_visit),
            (SEP, 10, pope_francis_visit),
            (SEP, 11, pope_francis_visit),
            # https://web.archive.org/web/20250421064400/https://timor-leste.gov.tl/?p=40592&lang=en
            (OCT, 31, special_national_holidays),
            # https://web.archive.org/web/20250414071012/https://timor-leste.gov.tl/?p=40955&lang=en
            (NOV, 29, special_national_holidays),
            # https://web.archive.org/web/20250414071025/https://timor-leste.gov.tl/?p=41325&lang=en
            (DEC, 24, special_national_holidays),
        ),
        2025: (
            # https://web.archive.org/web/20250414070915/https://timor-leste.gov.tl/?p=41361&lang=en
            (JAN, 2, special_national_holidays),
            # https://web.archive.org/web/20250414070923/https://timor-leste.gov.tl/?p=41592&lang=en
            (JAN, 29, special_national_holidays),
            # https://web.archive.org/web/20250414070934/https://timor-leste.gov.tl/?p=42076&lang=en
            (MAR, 5, special_national_holidays),
        ),
    }
