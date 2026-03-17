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

import importlib
from collections.abc import Iterable
from threading import RLock
from typing import Any

from holidays.holiday_base import HolidayBase

RegistryDict = dict[str, tuple[str, ...]]

COUNTRIES: RegistryDict = {
    "afghanistan": ("Afghanistan", "AF", "AFG"),
    "aland_islands": ("AlandIslands", "AX", "ALA", "HolidaysAX"),
    "albania": ("Albania", "AL", "ALB"),
    "algeria": ("Algeria", "DZ", "DZA"),
    "american_samoa": ("AmericanSamoa", "AS", "ASM", "HolidaysAS"),
    "andorra": ("Andorra", "AD", "AND"),
    "angola": ("Angola", "AO", "AGO"),
    "anguilla": ("Anguilla", "AI", "AIA"),
    "antarctica": ("Antarctica", "AQ", "ATA"),
    "antigua_and_barbuda": ("AntiguaAndBarbuda", "AG", "ATG"),
    "argentina": ("Argentina", "AR", "ARG"),
    "armenia": ("Armenia", "AM", "ARM"),
    "aruba": ("Aruba", "AW", "ABW"),
    "australia": ("Australia", "AU", "AUS"),
    "austria": ("Austria", "AT", "AUT"),
    "azerbaijan": ("Azerbaijan", "AZ", "AZE"),
    "bahamas": ("Bahamas", "BS", "BHS"),
    "bahrain": ("Bahrain", "BH", "BAH"),
    "bangladesh": ("Bangladesh", "BD", "BGD"),
    "barbados": ("Barbados", "BB", "BRB"),
    "belarus": ("Belarus", "BY", "BLR"),
    "belgium": ("Belgium", "BE", "BEL"),
    "belize": ("Belize", "BZ", "BLZ"),
    "benin": ("Benin", "BJ", "BEN"),
    "bermuda": ("Bermuda", "BM", "BMU"),
    "bhutan": ("Bhutan", "BT", "BTN"),
    "bolivia": ("Bolivia", "BO", "BOL"),
    "bonaire_sint_eustatius_and_saba": ("BonaireSintEustatiusAndSaba", "BQ", "BES"),
    "bosnia_and_herzegovina": ("BosniaAndHerzegovina", "BA", "BIH"),
    "botswana": ("Botswana", "BW", "BWA"),
    "bouvet_island": ("BouvetIsland", "BV", "BVT"),
    "brazil": ("Brazil", "BR", "BRA"),
    "british_indian_ocean_territory": ("BritishIndianOceanTerritory", "IO", "IOT"),
    "british_virgin_islands": ("BritishVirginIslands", "VG", "VGB"),
    "brunei": ("Brunei", "BN", "BRN"),
    "bulgaria": ("Bulgaria", "BG", "BLG"),
    "burkina_faso": ("BurkinaFaso", "BF", "BFA"),
    "burundi": ("Burundi", "BI", "BDI"),
    "cabo_verde": ("CaboVerde", "CV", "CPV"),
    "cambodia": ("Cambodia", "KH", "KHM"),
    "cameroon": ("Cameroon", "CM", "CMR"),
    "canada": ("Canada", "CA", "CAN"),
    "cayman_islands": ("CaymanIslands", "KY", "CYM"),
    "central_african_republic": ("CentralAfricanRepublic", "CF", "CAF"),
    "chad": ("Chad", "TD", "TCD"),
    "chile": ("Chile", "CL", "CHL"),
    "china": ("China", "CN", "CHN"),
    "christmas_island": ("ChristmasIsland", "CX", "CXR"),
    "cocos_islands": ("CocosIslands", "CC", "CCK"),
    "colombia": ("Colombia", "CO", "COL"),
    "comoros": ("Comoros", "KM", "COM"),
    "congo": ("Congo", "CG", "COG"),
    "cook_islands": ("CookIslands", "CK", "COK"),
    "costa_rica": ("CostaRica", "CR", "CRI"),
    "croatia": ("Croatia", "HR", "HRV"),
    "cuba": ("Cuba", "CU", "CUB"),
    "curacao": ("Curacao", "CW", "CUW"),
    "cyprus": ("Cyprus", "CY", "CYP"),
    "czechia": ("Czechia", "CZ", "CZE"),
    "denmark": ("Denmark", "DK", "DNK"),
    "djibouti": ("Djibouti", "DJ", "DJI"),
    "dominica": ("Dominica", "DM", "DMA"),
    "dominican_republic": ("DominicanRepublic", "DO", "DOM"),
    "dr_congo": ("DRCongo", "CD", "COD"),
    "ecuador": ("Ecuador", "EC", "ECU"),
    "egypt": ("Egypt", "EG", "EGY"),
    "jordan": ("Jordan", "JO", "JOR"),
    "el_salvador": ("ElSalvador", "SV", "SLV"),
    "equatorial_guinea": ("EquatorialGuinea", "GQ", "GNQ"),
    "eritrea": ("Eritrea", "ER", "ERI"),
    "estonia": ("Estonia", "EE", "EST"),
    "eswatini": ("Eswatini", "SZ", "SZW", "Swaziland"),
    "ethiopia": ("Ethiopia", "ET", "ETH"),
    "falkland_islands": ("FalklandIslands", "FK", "FLK"),
    "faroe_islands": ("FaroeIslands", "FO", "FRO"),
    "fiji": ("Fiji", "FJ", "FJI"),
    "finland": ("Finland", "FI", "FIN"),
    "france": ("France", "FR", "FRA"),
    "french_guiana": ("FrenchGuiana", "GF", "GUF", "HolidaysGF"),
    "french_polynesia": ("FrenchPolynesia", "PF", "PYF", "HolidaysPF"),
    "french_southern_territories": ("FrenchSouthernTerritories", "TF", "ATF", "HolidaysTF"),
    "gabon": ("Gabon", "GA", "GAB"),
    "gambia": ("Gambia", "GM", "GMB"),
    "georgia": ("Georgia", "GE", "GEO"),
    "germany": ("Germany", "DE", "DEU"),
    "ghana": ("Ghana", "GH", "GHA"),
    "gibraltar": ("Gibraltar", "GI", "GIB"),
    "greece": ("Greece", "GR", "GRC"),
    "greenland": ("Greenland", "GL", "GRL"),
    "grenada": ("Grenada", "GD", "GRD"),
    "guadeloupe": ("Guadeloupe", "GP", "GLP", "HolidaysGP"),
    "guam": ("Guam", "GU", "GUM", "HolidaysGU"),
    "guatemala": ("Guatemala", "GT", "GUA"),
    "guernsey": ("Guernsey", "GG", "GGY"),
    "guinea": ("Guinea", "GN", "GIN"),
    "guinea_bissau": ("GuineaBissau", "GW", "GNB"),
    "guyana": ("Guyana", "GY", "GUY"),
    "haiti": ("Haiti", "HT", "HTI"),
    "heard_island_and_mcdonald_islands": ("HeardIslandAndMcDonaldIslands", "HM", "HMD"),
    "honduras": ("Honduras", "HN", "HND"),
    "hongkong": ("HongKong", "HK", "HKG"),
    "hungary": ("Hungary", "HU", "HUN"),
    "iceland": ("Iceland", "IS", "ISL"),
    "india": ("India", "IN", "IND"),
    "indonesia": ("Indonesia", "ID", "IDN"),
    "iran": ("Iran", "IR", "IRN"),
    "iraq": ("Iraq", "IQ", "IRQ"),
    "ireland": ("Ireland", "IE", "IRL"),
    "isle_of_man": ("IsleOfMan", "IM", "IMN"),
    "israel": ("Israel", "IL", "ISR"),
    "italy": ("Italy", "IT", "ITA"),
    "ivory_coast": ("IvoryCoast", "CI", "CIV"),
    "jamaica": ("Jamaica", "JM", "JAM"),
    "japan": ("Japan", "JP", "JPN"),
    "jersey": ("Jersey", "JE", "JEY"),
    "kazakhstan": ("Kazakhstan", "KZ", "KAZ"),
    "kenya": ("Kenya", "KE", "KEN"),
    "kiribati": ("Kiribati", "KI", "KIR"),
    "kuwait": ("Kuwait", "KW", "KWT"),
    "kyrgyzstan": ("Kyrgyzstan", "KG", "KGZ"),
    "laos": ("Laos", "LA", "LAO"),
    "latvia": ("Latvia", "LV", "LVA"),
    "lebanon": ("Lebanon", "LB", "LBN"),
    "lesotho": ("Lesotho", "LS", "LSO"),
    "liberia": ("Liberia", "LR", "LBR"),
    "libya": ("Libya", "LY", "LBY"),
    "liechtenstein": ("Liechtenstein", "LI", "LIE"),
    "lithuania": ("Lithuania", "LT", "LTU"),
    "luxembourg": ("Luxembourg", "LU", "LUX"),
    "macau": ("Macau", "MO", "MAC"),
    "madagascar": ("Madagascar", "MG", "MDG"),
    "malawi": ("Malawi", "MW", "MWI"),
    "malaysia": ("Malaysia", "MY", "MYS"),
    "maldives": ("Maldives", "MV", "MDV"),
    "mali": ("Mali", "ML", "MLI"),
    "malta": ("Malta", "MT", "MLT"),
    "marshall_islands": ("MarshallIslands", "MH", "MHL", "HolidaysMH"),
    "martinique": ("Martinique", "MQ", "MTQ", "HolidaysMQ"),
    "mauritania": ("Mauritania", "MR", "MRT"),
    "mauritius": ("Mauritius", "MU", "MUS"),
    "mayotte": ("Mayotte", "YT", "MYT", "HolidaysYT"),
    "mexico": ("Mexico", "MX", "MEX"),
    "micronesia": ("Micronesia", "FM", "FSM"),
    "moldova": ("Moldova", "MD", "MDA"),
    "monaco": ("Monaco", "MC", "MCO"),
    "mongolia": ("Mongolia", "MN", "MNG"),
    "montenegro": ("Montenegro", "ME", "MNE"),
    "montserrat": ("Montserrat", "MS", "MSR"),
    "morocco": ("Morocco", "MA", "MOR"),
    "mozambique": ("Mozambique", "MZ", "MOZ"),
    "myanmar": ("Myanmar", "MM", "MMR"),
    "namibia": ("Namibia", "NA", "NAM"),
    "nauru": ("Nauru", "NR", "NRU"),
    "nepal": ("Nepal", "NP", "NPL"),
    "netherlands": ("Netherlands", "NL", "NLD"),
    "new_caledonia": ("NewCaledonia", "NC", "NCL", "HolidaysNC"),
    "new_zealand": ("NewZealand", "NZ", "NZL"),
    "nicaragua": ("Nicaragua", "NI", "NIC"),
    "niger": ("Niger", "NE", "NER"),
    "nigeria": ("Nigeria", "NG", "NGA"),
    "niue": ("Niue", "NU", "NIU"),
    "norfolk_island": ("NorfolkIsland", "NF", "NFK"),
    "north_korea": ("NorthKorea", "KP", "PRK"),
    "north_macedonia": ("NorthMacedonia", "MK", "MKD"),
    "northern_mariana_islands": ("NorthernMarianaIslands", "MP", "MNP", "HolidaysMP"),
    "norway": ("Norway", "NO", "NOR"),
    "oman": ("Oman", "OM", "OMN"),
    "pakistan": ("Pakistan", "PK", "PAK"),
    "palau": ("Palau", "PW", "PLW"),
    "palestine": ("Palestine", "PS", "PSE"),
    "panama": ("Panama", "PA", "PAN"),
    "papua_new_guinea": ("PapuaNewGuinea", "PG", "PNG"),
    "paraguay": ("Paraguay", "PY", "PRY"),
    "peru": ("Peru", "PE", "PER"),
    "philippines": ("Philippines", "PH", "PHL"),
    "pitcairn_islands": ("PitcairnIslands", "PN", "PCN"),
    "poland": ("Poland", "PL", "POL"),
    "portugal": ("Portugal", "PT", "PRT"),
    "puerto_rico": ("PuertoRico", "PR", "PRI", "HolidaysPR"),
    "qatar": ("Qatar", "QA", "QAT"),
    "reunion": ("Reunion", "RE", "REU", "HolidaysRE"),
    "romania": ("Romania", "RO", "ROU"),
    "russia": ("Russia", "RU", "RUS"),
    "rwanda": ("Rwanda", "RW", "RWA"),
    "saint_barthelemy": ("SaintBarthelemy", "BL", "BLM", "HolidaysBL"),
    "saint_helena_ascension_and_tristan_da_cunha": (
        "SaintHelenaAscensionAndTristanDaCunha",
        "SH",
        "SHN",
    ),
    "saint_kitts_and_nevis": ("SaintKittsAndNevis", "KN", "KNA"),
    "saint_lucia": ("SaintLucia", "LC", "LCA"),
    "saint_martin": ("SaintMartin", "MF", "MAF", "HolidaysMF"),
    "saint_pierre_and_miquelon": ("SaintPierreAndMiquelon", "PM", "SPM", "HolidaysPM"),
    "saint_vincent_and_the_grenadines": ("SaintVincentAndTheGrenadines", "VC", "VCT"),
    "samoa": ("Samoa", "WS", "WSM"),
    "san_marino": ("SanMarino", "SM", "SMR"),
    "sao_tome_and_principe": ("SaoTomeAndPrincipe", "ST", "STP"),
    "saudi_arabia": ("SaudiArabia", "SA", "SAU"),
    "senegal": ("Senegal", "SN", "SEN"),
    "serbia": ("Serbia", "RS", "SRB"),
    "seychelles": ("Seychelles", "SC", "SYC"),
    "sierra_leone": ("SierraLeone", "SL", "SLE"),
    "singapore": ("Singapore", "SG", "SGP"),
    "sint_maarten": ("SintMaarten", "SX", "SXM"),
    "slovakia": ("Slovakia", "SK", "SVK"),
    "slovenia": ("Slovenia", "SI", "SVN"),
    "solomon_islands": ("SolomonIslands", "SB", "SLB"),
    "somalia": ("Somalia", "SO", "SOM"),
    "south_africa": ("SouthAfrica", "ZA", "ZAF"),
    "south_georgia_and_the_south_sandwich_islands": (
        "SouthGeorgiaAndTheSouthSandwichIslands",
        "GS",
        "SGS",
    ),
    "south_korea": ("SouthKorea", "KR", "KOR", "Korea"),
    "south_sudan": ("SouthSudan", "SS", "SSD"),
    "spain": ("Spain", "ES", "ESP"),
    "sri_lanka": ("SriLanka", "LK", "LKA"),
    "sudan": ("Sudan", "SD", "SDN"),
    "suriname": ("Suriname", "SR", "SUR"),
    "svalbard_and_jan_mayen": ("SvalbardAndJanMayen", "SJ", "SJM", "HolidaysSJ"),
    "sweden": ("Sweden", "SE", "SWE"),
    "switzerland": ("Switzerland", "CH", "CHE"),
    "syrian_arab_republic": ("SyrianArabRepublic", "SY", "SYR"),
    "taiwan": ("Taiwan", "TW", "TWN"),
    "tajikistan": ("Tajikistan", "TJ", "TJK"),
    "tanzania": ("Tanzania", "TZ", "TZA"),
    "thailand": ("Thailand", "TH", "THA"),
    "timor_leste": ("TimorLeste", "TL", "TLS"),
    "togo": ("Togo", "TG", "TGO"),
    "tokelau": ("Tokelau", "TK", "TKL"),
    "tonga": ("Tonga", "TO", "TON"),
    "trinidad_and_tobago": ("TrinidadAndTobago", "TT", "TTO"),
    "tunisia": ("Tunisia", "TN", "TUN"),
    "turkey": ("Turkey", "TR", "TUR"),
    "turkmenistan": ("Turkmenistan", "TM", "TKM"),
    "turks_and_caicos_islands": ("TurksAndCaicosIslands", "TC", "TCA"),
    "tuvalu": ("Tuvalu", "TV", "TUV"),
    "uganda": ("Uganda", "UG", "UGA"),
    "ukraine": ("Ukraine", "UA", "UKR"),
    "united_arab_emirates": ("UnitedArabEmirates", "AE", "ARE"),
    "united_kingdom": ("UnitedKingdom", "GB", "GBR", "UK"),
    "united_states_minor_outlying_islands": (
        "UnitedStatesMinorOutlyingIslands",
        "UM",
        "UMI",
        "HolidaysUM",
    ),
    "united_states_virgin_islands": ("UnitedStatesVirginIslands", "VI", "VIR", "HolidaysVI"),
    "united_states": ("UnitedStates", "US", "USA"),
    "uruguay": ("Uruguay", "UY", "URY"),
    "uzbekistan": ("Uzbekistan", "UZ", "UZB"),
    "vanuatu": ("Vanuatu", "VU", "VTU"),
    "vatican_city": ("VaticanCity", "VA", "VAT"),
    "venezuela": ("Venezuela", "VE", "VEN"),
    "vietnam": ("Vietnam", "VN", "VNM"),
    "wallis_and_futuna": ("WallisAndFutuna", "WF", "WLF", "HolidaysWF"),
    "western_sahara": ("WesternSahara", "EH", "ESH"),
    "yemen": ("Yemen", "YE", "YEM"),
    "zambia": ("Zambia", "ZM", "ZMB"),
    "zimbabwe": ("Zimbabwe", "ZW", "ZWE"),
}

FINANCIAL: RegistryDict = {
    "bombay_stock_exchange": ("BombayStockExchange", "XBOM", "BSE"),
    "brasil_bolsa_balcao": ("BrasilBolsaBalcao", "BVMF", "B3"),
    "european_central_bank": ("EuropeanCentralBank", "XECB", "ECB", "TAR"),
    "ice_futures_europe": ("IceFuturesEurope", "IFEU", "ICEFuturesEurope"),
    "national_stock_exchange_of_india": ("NationalStockExchangeOfIndia", "XNSE", "NSE"),
    "ny_stock_exchange": ("NewYorkStockExchange", "XNYS", "NYSE"),
}

# A re-entrant lock. Once a thread has acquired a re-entrant lock,
# the same thread may acquire it again without blocking.
# https://docs.python.org/3/library/threading.html#rlock-objects
IMPORT_LOCK = RLock()


class EntityLoader:
    """Country and financial holidays entities lazy loader."""

    __slots__ = ("entity", "entity_name", "module_name")

    def __init__(self, path: str, *args, **kwargs) -> None:
        """Set up a lazy loader."""
        if args:
            raise TypeError(
                "This is a holidays entity loader class. "
                "For entity inheritance purposes please import a class you "
                "want to derive from directly: e.g., "
                "`from holidays.countries import Entity` or "
                "`from holidays.financial import Entity`."
            )

        entity_path = path.split(".")

        self.entity = None
        self.entity_name = entity_path[-1]
        self.module_name = ".".join(entity_path[0:-1])

        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs) -> HolidayBase:
        """Create a new instance of a lazy-loaded entity."""
        cls = self.get_entity()
        return cls(*args, **kwargs)  # type: ignore[misc, operator]

    def __getattr__(self, name: str) -> Any | None:
        """Return attribute of a lazy-loaded entity."""
        cls = self.get_entity()
        return getattr(cls, name)

    def __str__(self) -> str:
        """Return lazy loader object string representation."""
        return (
            f"A lazy loader for {self.get_entity()}. For inheritance please "
            f"use the '{self.module_name}.{self.entity_name}' class directly."
        )

    def get_entity(self) -> HolidayBase | None:
        """Return lazy-loaded entity."""
        if self.entity is None:
            # Avoid deadlock due to importlib.import_module not being thread-safe by caching all
            # the first imports in a dedicated thread.
            with IMPORT_LOCK:
                self.entity = getattr(importlib.import_module(self.module_name), self.entity_name)

        return self.entity

    @staticmethod
    def _get_entity_codes(
        container: RegistryDict,
        *,
        include_aliases: bool = True,
        max_code_length: int = 3,
        min_code_length: int = 2,
    ) -> Iterable[str]:
        for entities in container.values():
            for code in entities[1:]:
                if min_code_length <= len(code) <= max_code_length:
                    yield code

                # Stop after the first matching code if aliases are not requested.
                # Assuming that the alpha-2 code goes first.
                if not include_aliases:
                    break

    @staticmethod
    def get_country_codes(include_aliases: bool = True) -> Iterable[str]:
        """Get supported country codes.

        :param include_aliases:
            Whether to include entity aliases (e.g. GBR and UK for GB,
            UKR for UA, USA for US, etc).
        """
        return EntityLoader._get_entity_codes(
            COUNTRIES,
            include_aliases=include_aliases,
        )

    @staticmethod
    def get_financial_codes(include_aliases: bool = True) -> Iterable[str]:
        """Get supported financial codes.

        :param include_aliases:
            Whether to include entity aliases (e.g. B3 for BVMF,
            TAR for ECB, NYSE for XNYS, etc).
        """
        return EntityLoader._get_entity_codes(
            FINANCIAL,
            include_aliases=include_aliases,
            max_code_length=4,
        )

    @staticmethod
    def load(prefix: str, scope: dict) -> None:
        """Load country or financial entities."""
        entity_mapping = COUNTRIES if prefix == "countries" else FINANCIAL
        for module, entities in entity_mapping.items():
            scope.update(
                {
                    entity: EntityLoader(f"holidays.{prefix}.{module}.{entity}")
                    for entity in entities
                }
            )
