# -*- coding: utf-8 -*-

import re
from typing import Dict, Iterator, NamedTuple, Type, TypeVar, Union, overload

__all__ = ["countries"]

StrOrInt = Union[str, int]
_D = TypeVar("_D")


class Country(NamedTuple):
    name: str
    alpha2: str
    alpha3: str
    numeric: str
    apolitical_name: str


_records = [
    Country("Afghanistan", "AF", "AFG", "004", "Afghanistan"),
    Country("Åland Islands", "AX", "ALA", "248", "Åland Islands"),
    Country("Albania", "AL", "ALB", "008", "Albania"),
    Country("Algeria", "DZ", "DZA", "012", "Algeria"),
    Country("American Samoa", "AS", "ASM", "016", "American Samoa"),
    Country("Andorra", "AD", "AND", "020", "Andorra"),
    Country("Angola", "AO", "AGO", "024", "Angola"),
    Country("Anguilla", "AI", "AIA", "660", "Anguilla"),
    Country("Antarctica", "AQ", "ATA", "010", "Antarctica"),
    Country("Antigua and Barbuda", "AG", "ATG", "028", "Antigua and Barbuda"),
    Country("Argentina", "AR", "ARG", "032", "Argentina"),
    Country("Armenia", "AM", "ARM", "051", "Armenia"),
    Country("Aruba", "AW", "ABW", "533", "Aruba"),
    Country("Australia", "AU", "AUS", "036", "Australia"),
    Country("Austria", "AT", "AUT", "040", "Austria"),
    Country("Azerbaijan", "AZ", "AZE", "031", "Azerbaijan"),
    Country("Bahamas", "BS", "BHS", "044", "Bahamas"),
    Country("Bahrain", "BH", "BHR", "048", "Bahrain"),
    Country("Bangladesh", "BD", "BGD", "050", "Bangladesh"),
    Country("Barbados", "BB", "BRB", "052", "Barbados"),
    Country("Belarus", "BY", "BLR", "112", "Belarus"),
    Country("Belgium", "BE", "BEL", "056", "Belgium"),
    Country("Belize", "BZ", "BLZ", "084", "Belize"),
    Country("Benin", "BJ", "BEN", "204", "Benin"),
    Country("Bermuda", "BM", "BMU", "060", "Bermuda"),
    Country("Bhutan", "BT", "BTN", "064", "Bhutan"),
    Country(
        "Bolivia, Plurinational State of",
        "BO",
        "BOL",
        "068",
        "Bolivia, Plurinational State of",
    ),
    Country(
        "Bonaire, Sint Eustatius and Saba",
        "BQ",
        "BES",
        "535",
        "Bonaire, Sint Eustatius and Saba",
    ),
    Country(
        "Bosnia and Herzegovina", "BA", "BIH", "070", "Bosnia and Herzegovina"
    ),
    Country("Botswana", "BW", "BWA", "072", "Botswana"),
    Country("Bouvet Island", "BV", "BVT", "074", "Bouvet Island"),
    Country("Brazil", "BR", "BRA", "076", "Brazil"),
    Country(
        "British Indian Ocean Territory",
        "IO",
        "IOT",
        "086",
        "British Indian Ocean Territory",
    ),
    Country("Brunei Darussalam", "BN", "BRN", "096", "Brunei Darussalam"),
    Country("Bulgaria", "BG", "BGR", "100", "Bulgaria"),
    Country("Burkina Faso", "BF", "BFA", "854", "Burkina Faso"),
    Country("Burundi", "BI", "BDI", "108", "Burundi"),
    Country("Cambodia", "KH", "KHM", "116", "Cambodia"),
    Country("Cameroon", "CM", "CMR", "120", "Cameroon"),
    Country("Canada", "CA", "CAN", "124", "Canada"),
    Country("Cabo Verde", "CV", "CPV", "132", "Cabo Verde"),
    Country("Cayman Islands", "KY", "CYM", "136", "Cayman Islands"),
    Country(
        "Central African Republic",
        "CF",
        "CAF",
        "140",
        "Central African Republic",
    ),
    Country("Chad", "TD", "TCD", "148", "Chad"),
    Country("Chile", "CL", "CHL", "152", "Chile"),
    Country("China", "CN", "CHN", "156", "China"),
    Country("Christmas Island", "CX", "CXR", "162", "Christmas Island"),
    Country(
        "Cocos (Keeling) Islands",
        "CC",
        "CCK",
        "166",
        "Cocos (Keeling) Islands",
    ),
    Country("Colombia", "CO", "COL", "170", "Colombia"),
    Country("Comoros", "KM", "COM", "174", "Comoros"),
    Country("Congo", "CG", "COG", "178", "Congo"),
    Country(
        "Congo, Democratic Republic of the",
        "CD",
        "COD",
        "180",
        "Congo, Democratic Republic of the",
    ),
    Country("Cook Islands", "CK", "COK", "184", "Cook Islands"),
    Country("Costa Rica", "CR", "CRI", "188", "Costa Rica"),
    Country("Côte d'Ivoire", "CI", "CIV", "384", "Côte d'Ivoire"),
    Country("Croatia", "HR", "HRV", "191", "Croatia"),
    Country("Cuba", "CU", "CUB", "192", "Cuba"),
    Country("Curaçao", "CW", "CUW", "531", "Curaçao"),
    Country("Cyprus", "CY", "CYP", "196", "Cyprus"),
    Country("Czechia", "CZ", "CZE", "203", "Czechia"),
    Country("Denmark", "DK", "DNK", "208", "Denmark"),
    Country("Djibouti", "DJ", "DJI", "262", "Djibouti"),
    Country("Dominica", "DM", "DMA", "212", "Dominica"),
    Country("Dominican Republic", "DO", "DOM", "214", "Dominican Republic"),
    Country("Ecuador", "EC", "ECU", "218", "Ecuador"),
    Country("Egypt", "EG", "EGY", "818", "Egypt"),
    Country("El Salvador", "SV", "SLV", "222", "El Salvador"),
    Country("Equatorial Guinea", "GQ", "GNQ", "226", "Equatorial Guinea"),
    Country("Eritrea", "ER", "ERI", "232", "Eritrea"),
    Country("Estonia", "EE", "EST", "233", "Estonia"),
    Country("Ethiopia", "ET", "ETH", "231", "Ethiopia"),
    Country(
        "Falkland Islands (Malvinas)",
        "FK",
        "FLK",
        "238",
        "Falkland Islands (Malvinas)",
    ),
    Country("Faroe Islands", "FO", "FRO", "234", "Faroe Islands"),
    Country("Fiji", "FJ", "FJI", "242", "Fiji"),
    Country("Finland", "FI", "FIN", "246", "Finland"),
    Country("France", "FR", "FRA", "250", "France"),
    Country("French Guiana", "GF", "GUF", "254", "French Guiana"),
    Country("French Polynesia", "PF", "PYF", "258", "French Polynesia"),
    Country(
        "French Southern Territories",
        "TF",
        "ATF",
        "260",
        "French Southern Territories",
    ),
    Country("Gabon", "GA", "GAB", "266", "Gabon"),
    Country("Gambia", "GM", "GMB", "270", "Gambia"),
    Country("Georgia", "GE", "GEO", "268", "Georgia"),
    Country("Germany", "DE", "DEU", "276", "Germany"),
    Country("Ghana", "GH", "GHA", "288", "Ghana"),
    Country("Gibraltar", "GI", "GIB", "292", "Gibraltar"),
    Country("Greece", "GR", "GRC", "300", "Greece"),
    Country("Greenland", "GL", "GRL", "304", "Greenland"),
    Country("Grenada", "GD", "GRD", "308", "Grenada"),
    Country("Guadeloupe", "GP", "GLP", "312", "Guadeloupe"),
    Country("Guam", "GU", "GUM", "316", "Guam"),
    Country("Guatemala", "GT", "GTM", "320", "Guatemala"),
    Country("Guernsey", "GG", "GGY", "831", "Guernsey"),
    Country("Guinea", "GN", "GIN", "324", "Guinea"),
    Country("Guinea-Bissau", "GW", "GNB", "624", "Guinea-Bissau"),
    Country("Guyana", "GY", "GUY", "328", "Guyana"),
    Country("Haiti", "HT", "HTI", "332", "Haiti"),
    Country(
        "Heard Island and McDonald Islands",
        "HM",
        "HMD",
        "334",
        "Heard Island and McDonald Islands",
    ),
    Country("Holy See", "VA", "VAT", "336", "Holy See"),
    Country("Honduras", "HN", "HND", "340", "Honduras"),
    Country("Hong Kong", "HK", "HKG", "344", "Hong Kong"),
    Country("Hungary", "HU", "HUN", "348", "Hungary"),
    Country("Iceland", "IS", "ISL", "352", "Iceland"),
    Country("India", "IN", "IND", "356", "India"),
    Country("Indonesia", "ID", "IDN", "360", "Indonesia"),
    Country(
        "Iran, Islamic Republic of",
        "IR",
        "IRN",
        "364",
        "Iran, Islamic Republic of",
    ),
    Country("Iraq", "IQ", "IRQ", "368", "Iraq"),
    Country("Ireland", "IE", "IRL", "372", "Ireland"),
    Country("Isle of Man", "IM", "IMN", "833", "Isle of Man"),
    Country("Israel", "IL", "ISR", "376", "Israel"),
    Country("Italy", "IT", "ITA", "380", "Italy"),
    Country("Jamaica", "JM", "JAM", "388", "Jamaica"),
    Country("Japan", "JP", "JPN", "392", "Japan"),
    Country("Jersey", "JE", "JEY", "832", "Jersey"),
    Country("Jordan", "JO", "JOR", "400", "Jordan"),
    Country("Kazakhstan", "KZ", "KAZ", "398", "Kazakhstan"),
    Country("Kenya", "KE", "KEN", "404", "Kenya"),
    Country("Kiribati", "KI", "KIR", "296", "Kiribati"),
    Country(
        "Korea, Democratic People's Republic of",
        "KP",
        "PRK",
        "408",
        "Korea, Democratic People's Republic of",
    ),
    Country("Korea, Republic of", "KR", "KOR", "410", "Korea, Republic of"),
    Country("Kosovo", "XK", "XKX", "983", "Kosovo"),
    Country("Kuwait", "KW", "KWT", "414", "Kuwait"),
    Country("Kyrgyzstan", "KG", "KGZ", "417", "Kyrgyzstan"),
    Country(
        "Lao People's Democratic Republic",
        "LA",
        "LAO",
        "418",
        "Lao People's Democratic Republic",
    ),
    Country("Latvia", "LV", "LVA", "428", "Latvia"),
    Country("Lebanon", "LB", "LBN", "422", "Lebanon"),
    Country("Lesotho", "LS", "LSO", "426", "Lesotho"),
    Country("Liberia", "LR", "LBR", "430", "Liberia"),
    Country("Libya", "LY", "LBY", "434", "Libya"),
    Country("Liechtenstein", "LI", "LIE", "438", "Liechtenstein"),
    Country("Lithuania", "LT", "LTU", "440", "Lithuania"),
    Country("Luxembourg", "LU", "LUX", "442", "Luxembourg"),
    Country("Macao", "MO", "MAC", "446", "Macao"),
    Country("North Macedonia", "MK", "MKD", "807", "North Macedonia"),
    Country("Madagascar", "MG", "MDG", "450", "Madagascar"),
    Country("Malawi", "MW", "MWI", "454", "Malawi"),
    Country("Malaysia", "MY", "MYS", "458", "Malaysia"),
    Country("Maldives", "MV", "MDV", "462", "Maldives"),
    Country("Mali", "ML", "MLI", "466", "Mali"),
    Country("Malta", "MT", "MLT", "470", "Malta"),
    Country("Marshall Islands", "MH", "MHL", "584", "Marshall Islands"),
    Country("Martinique", "MQ", "MTQ", "474", "Martinique"),
    Country("Mauritania", "MR", "MRT", "478", "Mauritania"),
    Country("Mauritius", "MU", "MUS", "480", "Mauritius"),
    Country("Mayotte", "YT", "MYT", "175", "Mayotte"),
    Country("Mexico", "MX", "MEX", "484", "Mexico"),
    Country(
        "Micronesia, Federated States of",
        "FM",
        "FSM",
        "583",
        "Micronesia, Federated States of",
    ),
    Country(
        "Moldova, Republic of", "MD", "MDA", "498", "Moldova, Republic of"
    ),
    Country("Monaco", "MC", "MCO", "492", "Monaco"),
    Country("Mongolia", "MN", "MNG", "496", "Mongolia"),
    Country("Montenegro", "ME", "MNE", "499", "Montenegro"),
    Country("Montserrat", "MS", "MSR", "500", "Montserrat"),
    Country("Morocco", "MA", "MAR", "504", "Morocco"),
    Country("Mozambique", "MZ", "MOZ", "508", "Mozambique"),
    Country("Myanmar", "MM", "MMR", "104", "Myanmar"),
    Country("Namibia", "NA", "NAM", "516", "Namibia"),
    Country("Nauru", "NR", "NRU", "520", "Nauru"),
    Country("Nepal", "NP", "NPL", "524", "Nepal"),
    Country("Netherlands", "NL", "NLD", "528", "Netherlands"),
    Country("New Caledonia", "NC", "NCL", "540", "New Caledonia"),
    Country("New Zealand", "NZ", "NZL", "554", "New Zealand"),
    Country("Nicaragua", "NI", "NIC", "558", "Nicaragua"),
    Country("Niger", "NE", "NER", "562", "Niger"),
    Country("Nigeria", "NG", "NGA", "566", "Nigeria"),
    Country("Niue", "NU", "NIU", "570", "Niue"),
    Country("Norfolk Island", "NF", "NFK", "574", "Norfolk Island"),
    Country(
        "Northern Mariana Islands",
        "MP",
        "MNP",
        "580",
        "Northern Mariana Islands",
    ),
    Country("Norway", "NO", "NOR", "578", "Norway"),
    Country("Oman", "OM", "OMN", "512", "Oman"),
    Country("Pakistan", "PK", "PAK", "586", "Pakistan"),
    Country("Palau", "PW", "PLW", "585", "Palau"),
    Country("Palestine, State of", "PS", "PSE", "275", "Palestine"),
    Country("Panama", "PA", "PAN", "591", "Panama"),
    Country("Papua New Guinea", "PG", "PNG", "598", "Papua New Guinea"),
    Country("Paraguay", "PY", "PRY", "600", "Paraguay"),
    Country("Peru", "PE", "PER", "604", "Peru"),
    Country("Philippines", "PH", "PHL", "608", "Philippines"),
    Country("Pitcairn", "PN", "PCN", "612", "Pitcairn"),
    Country("Poland", "PL", "POL", "616", "Poland"),
    Country("Portugal", "PT", "PRT", "620", "Portugal"),
    Country("Puerto Rico", "PR", "PRI", "630", "Puerto Rico"),
    Country("Qatar", "QA", "QAT", "634", "Qatar"),
    Country("Réunion", "RE", "REU", "638", "Réunion"),
    Country("Romania", "RO", "ROU", "642", "Romania"),
    Country("Russian Federation", "RU", "RUS", "643", "Russian Federation"),
    Country("Rwanda", "RW", "RWA", "646", "Rwanda"),
    Country("Saint Barthélemy", "BL", "BLM", "652", "Saint Barthélemy"),
    Country(
        "Saint Helena, Ascension and Tristan da Cunha",
        "SH",
        "SHN",
        "654",
        "Saint Helena, Ascension and Tristan da Cunha",
    ),
    Country(
        "Saint Kitts and Nevis", "KN", "KNA", "659", "Saint Kitts and Nevis"
    ),
    Country("Saint Lucia", "LC", "LCA", "662", "Saint Lucia"),
    Country(
        "Saint Martin (French part)",
        "MF",
        "MAF",
        "663",
        "Saint Martin (French part)",
    ),
    Country(
        "Saint Pierre and Miquelon",
        "PM",
        "SPM",
        "666",
        "Saint Pierre and Miquelon",
    ),
    Country(
        "Saint Vincent and the Grenadines",
        "VC",
        "VCT",
        "670",
        "Saint Vincent and the Grenadines",
    ),
    Country("Samoa", "WS", "WSM", "882", "Samoa"),
    Country("San Marino", "SM", "SMR", "674", "San Marino"),
    Country(
        "Sao Tome and Principe", "ST", "STP", "678", "Sao Tome and Principe"
    ),
    Country("Saudi Arabia", "SA", "SAU", "682", "Saudi Arabia"),
    Country("Senegal", "SN", "SEN", "686", "Senegal"),
    Country("Serbia", "RS", "SRB", "688", "Serbia"),
    Country("Seychelles", "SC", "SYC", "690", "Seychelles"),
    Country("Sierra Leone", "SL", "SLE", "694", "Sierra Leone"),
    Country("Singapore", "SG", "SGP", "702", "Singapore"),
    Country(
        "Sint Maarten (Dutch part)",
        "SX",
        "SXM",
        "534",
        "Sint Maarten (Dutch part)",
    ),
    Country("Slovakia", "SK", "SVK", "703", "Slovakia"),
    Country("Slovenia", "SI", "SVN", "705", "Slovenia"),
    Country("Solomon Islands", "SB", "SLB", "090", "Solomon Islands"),
    Country("Somalia", "SO", "SOM", "706", "Somalia"),
    Country("South Africa", "ZA", "ZAF", "710", "South Africa"),
    Country(
        "South Georgia and the South Sandwich Islands",
        "GS",
        "SGS",
        "239",
        "South Georgia and the South Sandwich Islands",
    ),
    Country("South Sudan", "SS", "SSD", "728", "South Sudan"),
    Country("Spain", "ES", "ESP", "724", "Spain"),
    Country("Sri Lanka", "LK", "LKA", "144", "Sri Lanka"),
    Country("Sudan", "SD", "SDN", "729", "Sudan"),
    Country("Suriname", "SR", "SUR", "740", "Suriname"),
    Country(
        "Svalbard and Jan Mayen", "SJ", "SJM", "744", "Svalbard and Jan Mayen"
    ),
    Country("Eswatini", "SZ", "SWZ", "748", "Eswatini"),
    Country("Sweden", "SE", "SWE", "752", "Sweden"),
    Country("Switzerland", "CH", "CHE", "756", "Switzerland"),
    Country(
        "Syrian Arab Republic", "SY", "SYR", "760", "Syrian Arab Republic"
    ),
    Country("Taiwan, Province of China", "TW", "TWN", "158", "Taiwan"),
    Country("Tajikistan", "TJ", "TJK", "762", "Tajikistan"),
    Country(
        "Tanzania, United Republic of",
        "TZ",
        "TZA",
        "834",
        "Tanzania, United Republic of",
    ),
    Country("Thailand", "TH", "THA", "764", "Thailand"),
    Country("Timor-Leste", "TL", "TLS", "626", "Timor-Leste"),
    Country("Togo", "TG", "TGO", "768", "Togo"),
    Country("Tokelau", "TK", "TKL", "772", "Tokelau"),
    Country("Tonga", "TO", "TON", "776", "Tonga"),
    Country("Trinidad and Tobago", "TT", "TTO", "780", "Trinidad and Tobago"),
    Country("Tunisia", "TN", "TUN", "788", "Tunisia"),
    Country("Türkiye", "TR", "TUR", "792", "Türkiye"),
    Country("Turkmenistan", "TM", "TKM", "795", "Turkmenistan"),
    Country(
        "Turks and Caicos Islands",
        "TC",
        "TCA",
        "796",
        "Turks and Caicos Islands",
    ),
    Country("Tuvalu", "TV", "TUV", "798", "Tuvalu"),
    Country("Uganda", "UG", "UGA", "800", "Uganda"),
    Country("Ukraine", "UA", "UKR", "804", "Ukraine"),
    Country(
        "United Arab Emirates", "AE", "ARE", "784", "United Arab Emirates"
    ),
    Country(
        "United Kingdom of Great Britain and Northern Ireland",
        "GB",
        "GBR",
        "826",
        "United Kingdom of Great Britain and Northern Ireland",
    ),
    Country(
        "United States of America",
        "US",
        "USA",
        "840",
        "United States of America",
    ),
    Country(
        "United States Minor Outlying Islands",
        "UM",
        "UMI",
        "581",
        "United States Minor Outlying Islands",
    ),
    Country("Uruguay", "UY", "URY", "858", "Uruguay"),
    Country("Uzbekistan", "UZ", "UZB", "860", "Uzbekistan"),
    Country("Vanuatu", "VU", "VUT", "548", "Vanuatu"),
    Country(
        "Venezuela, Bolivarian Republic of",
        "VE",
        "VEN",
        "862",
        "Venezuela, Bolivarian Republic of",
    ),
    Country("Viet Nam", "VN", "VNM", "704", "Viet Nam"),
    Country(
        "Virgin Islands, British",
        "VG",
        "VGB",
        "092",
        "Virgin Islands, British",
    ),
    Country(
        "Virgin Islands, U.S.", "VI", "VIR", "850", "Virgin Islands, U.S."
    ),
    Country("Wallis and Futuna", "WF", "WLF", "876", "Wallis and Futuna"),
    Country("Western Sahara", "EH", "ESH", "732", "Western Sahara"),
    Country("Yemen", "YE", "YEM", "887", "Yemen"),
    Country("Zambia", "ZM", "ZMB", "894", "Zambia"),
    Country("Zimbabwe", "ZW", "ZWE", "716", "Zimbabwe"),
]


def _build_index(idx: int) -> Dict[str, Country]:
    return dict((r[idx].upper(), r) for r in _records)


# Internal country indexes
_by_alpha2 = _build_index(1)
_by_alpha3 = _build_index(2)
_by_numeric = _build_index(3)
_by_name = _build_index(0)
_by_apolitical_name = _build_index(4)


# Documented accessors for the country indexes
countries_by_alpha2 = _by_alpha2
countries_by_alpha3 = _by_alpha3
countries_by_numeric = _by_numeric
countries_by_name = _by_name
countries_by_apolitical_name = _by_apolitical_name


class NotFound:
    pass


class _CountryLookup:
    @overload
    def get(self, key: StrOrInt) -> Country:
        ...

    @overload
    def get(self, key: StrOrInt, default: _D) -> Union[Country, _D]:
        ...

    def get(
        self, key: StrOrInt, default: Union[Type[NotFound], _D] = NotFound
    ) -> Union[Country, _D]:
        if isinstance(key, int):
            k = f"{key:03d}"
            r = _by_numeric.get(k, default)
        else:
            k = key.upper()
            if len(k) == 2:
                r = _by_alpha2.get(k, default)
            elif len(k) == 3 and re.match(r"[0-9]{3}", k) and k != "000":
                r = _by_numeric.get(k, default)
            elif len(k) == 3:
                r = _by_alpha3.get(k, default)
            elif k in _by_name:
                r = _by_name.get(k, default)
            else:
                r = _by_apolitical_name.get(k, default)

        if r == NotFound:
            raise KeyError(key)

        return r

    __getitem__ = get

    def __len__(self) -> int:
        return len(_records)

    def __iter__(self) -> Iterator[Country]:
        return iter(_records)

    def __contains__(self, item: StrOrInt) -> bool:
        try:
            self.get(item)
            return True
        except KeyError:
            return False


countries = _CountryLookup()
