# -*- coding: utf-8 -*-

import re
from numbers import Integral
from collections import namedtuple

__all__ = ["countries"]

try:
    basestring
except NameError:
    basestring = str

Country = namedtuple('Country',
                     'name alpha2 alpha3 numeric apolitical_name')

_records = [
    Country(u"Afghanistan", "AF", "AFG", "004", u"Afghanistan"),
    Country(u"Åland Islands", "AX", "ALA", "248", u"Åland Islands"),
    Country(u"Albania", "AL", "ALB", "008", u"Albania"),
    Country(u"Algeria", "DZ", "DZA", "012", u"Algeria"),
    Country(u"American Samoa", "AS", "ASM", "016", u"American Samoa"),
    Country(u"Andorra", "AD", "AND", "020", u"Andorra"),
    Country(u"Angola", "AO", "AGO", "024", u"Angola"),
    Country(u"Anguilla", "AI", "AIA", "660", u"Anguilla"),
    Country(u"Antarctica", "AQ", "ATA", "010", u"Antarctica"),
    Country(u"Antigua and Barbuda", "AG", "ATG", "028",
            u"Antigua and Barbuda"),
    Country(u"Argentina", "AR", "ARG", "032", u"Argentina"),
    Country(u"Armenia", "AM", "ARM", "051", u"Armenia"),
    Country(u"Aruba", "AW", "ABW", "533", u"Aruba"),
    Country(u"Australia", "AU", "AUS", "036", u"Australia"),
    Country(u"Austria", "AT", "AUT", "040", u"Austria"),
    Country(u"Azerbaijan", "AZ", "AZE", "031", u"Azerbaijan"),
    Country(u"Bahamas", "BS", "BHS", "044", u"Bahamas"),
    Country(u"Bahrain", "BH", "BHR", "048", u"Bahrain"),
    Country(u"Bangladesh", "BD", "BGD", "050", u"Bangladesh"),
    Country(u"Barbados", "BB", "BRB", "052", u"Barbados"),
    Country(u"Belarus", "BY", "BLR", "112", u"Belarus"),
    Country(u"Belgium", "BE", "BEL", "056", u"Belgium"),
    Country(u"Belize", "BZ", "BLZ", "084", u"Belize"),
    Country(u"Benin", "BJ", "BEN", "204", u"Benin"),
    Country(u"Bermuda", "BM", "BMU", "060", u"Bermuda"),
    Country(u"Bhutan", "BT", "BTN", "064", u"Bhutan"),
    Country(u"Bolivia, Plurinational State of", "BO", "BOL", "068",
            u"Bolivia, Plurinational State of"),
    Country(u"Bonaire, Sint Eustatius and Saba", "BQ", "BES", "535",
            u"Bonaire, Sint Eustatius and Saba"),
    Country(u"Bosnia and Herzegovina", "BA", "BIH", "070",
            u"Bosnia and Herzegovina"),
    Country(u"Botswana", "BW", "BWA", "072", u"Botswana"),
    Country(u"Bouvet Island", "BV", "BVT", "074", u"Bouvet Island"),
    Country(u"Brazil", "BR", "BRA", "076", u"Brazil"),
    Country(u"British Indian Ocean Territory", "IO", "IOT", "086",
            u"British Indian Ocean Territory"),
    Country(u"Brunei Darussalam", "BN", "BRN", "096",
            u"Brunei Darussalam"),
    Country(u"Bulgaria", "BG", "BGR", "100", u"Bulgaria"),
    Country(u"Burkina Faso", "BF", "BFA", "854", u"Burkina Faso"),
    Country(u"Burundi", "BI", "BDI", "108", u"Burundi"),
    Country(u"Cambodia", "KH", "KHM", "116", u"Cambodia"),
    Country(u"Cameroon", "CM", "CMR", "120", u"Cameroon"),
    Country(u"Canada", "CA", "CAN", "124", u"Canada"),
    Country(u"Cabo Verde", "CV", "CPV", "132", u"Cabo Verde"),
    Country(u"Cayman Islands", "KY", "CYM", "136", u"Cayman Islands"),
    Country(u"Central African Republic", "CF", "CAF", "140",
            u"Central African Republic"),
    Country(u"Chad", "TD", "TCD", "148", u"Chad"),
    Country(u"Chile", "CL", "CHL", "152", u"Chile"),
    Country(u"China", "CN", "CHN", "156", u"China"),
    Country(u"Christmas Island", "CX", "CXR", "162", u"Christmas Island"),
    Country(u"Cocos (Keeling) Islands", "CC", "CCK", "166",
            u"Cocos (Keeling) Islands"),
    Country(u"Colombia", "CO", "COL", "170", u"Colombia"),
    Country(u"Comoros", "KM", "COM", "174", u"Comoros"),
    Country(u"Congo", "CG", "COG", "178", u"Congo"),
    Country(u"Congo, Democratic Republic of the", "CD", "COD", "180",
            u"Congo, Democratic Republic of the"),
    Country(u"Cook Islands", "CK", "COK", "184", u"Cook Islands"),
    Country(u"Costa Rica", "CR", "CRI", "188", u"Costa Rica"),
    Country(u"Côte d'Ivoire", "CI", "CIV", "384", u"Côte d'Ivoire"),
    Country(u"Croatia", "HR", "HRV", "191", u"Croatia"),
    Country(u"Cuba", "CU", "CUB", "192", u"Cuba"),
    Country(u"Curaçao", "CW", "CUW", "531", u"Curaçao"),
    Country(u"Cyprus", "CY", "CYP", "196", u"Cyprus"),
    Country(u"Czechia", "CZ", "CZE", "203", u"Czechia"),
    Country(u"Denmark", "DK", "DNK", "208", u"Denmark"),
    Country(u"Djibouti", "DJ", "DJI", "262", u"Djibouti"),
    Country(u"Dominica", "DM", "DMA", "212", u"Dominica"),
    Country(u"Dominican Republic", "DO", "DOM", "214", u"Dominican Republic"),
    Country(u"Ecuador", "EC", "ECU", "218", u"Ecuador"),
    Country(u"Egypt", "EG", "EGY", "818", u"Egypt"),
    Country(u"El Salvador", "SV", "SLV", "222", u"El Salvador"),
    Country(u"Equatorial Guinea", "GQ", "GNQ", "226", u"Equatorial Guinea"),
    Country(u"Eritrea", "ER", "ERI", "232", u"Eritrea"),
    Country(u"Estonia", "EE", "EST", "233", u"Estonia"),
    Country(u"Ethiopia", "ET", "ETH", "231", u"Ethiopia"),
    Country(u"Falkland Islands (Malvinas)", "FK", "FLK", "238",
            u"Falkland Islands (Malvinas)"),
    Country(u"Faroe Islands", "FO", "FRO", "234", u"Faroe Islands"),
    Country(u"Fiji", "FJ", "FJI", "242", u"Fiji"),
    Country(u"Finland", "FI", "FIN", "246", u"Finland"),
    Country(u"France", "FR", "FRA", "250", u"France"),
    Country(u"French Guiana", "GF", "GUF", "254", u"French Guiana"),
    Country(u"French Polynesia", "PF", "PYF", "258", u"French Polynesia"),
    Country(u"French Southern Territories", "TF", "ATF", "260",
            u"French Southern Territories"),
    Country(u"Gabon", "GA", "GAB", "266", u"Gabon"),
    Country(u"Gambia", "GM", "GMB", "270", u"Gambia"),
    Country(u"Georgia", "GE", "GEO", "268", u"Georgia"),
    Country(u"Germany", "DE", "DEU", "276", u"Germany"),
    Country(u"Ghana", "GH", "GHA", "288", u"Ghana"),
    Country(u"Gibraltar", "GI", "GIB", "292", u"Gibraltar"),
    Country(u"Greece", "GR", "GRC", "300", u"Greece"),
    Country(u"Greenland", "GL", "GRL", "304", u"Greenland"),
    Country(u"Grenada", "GD", "GRD", "308", u"Grenada"),
    Country(u"Guadeloupe", "GP", "GLP", "312", u"Guadeloupe"),
    Country(u"Guam", "GU", "GUM", "316", u"Guam"),
    Country(u"Guatemala", "GT", "GTM", "320", u"Guatemala"),
    Country(u"Guernsey", "GG", "GGY", "831", u"Guernsey"),
    Country(u"Guinea", "GN", "GIN", "324", u"Guinea"),
    Country(u"Guinea-Bissau", "GW", "GNB", "624", u"Guinea-Bissau"),
    Country(u"Guyana", "GY", "GUY", "328", u"Guyana"),
    Country(u"Haiti", "HT", "HTI", "332", u"Haiti"),
    Country(u"Heard Island and McDonald Islands", "HM", "HMD", "334",
            u"Heard Island and McDonald Islands"),
    Country(u"Holy See", "VA", "VAT", "336", u"Holy See"),
    Country(u"Honduras", "HN", "HND", "340", u"Honduras"),
    Country(u"Hong Kong", "HK", "HKG", "344", u"Hong Kong"),
    Country(u"Hungary", "HU", "HUN", "348", u"Hungary"),
    Country(u"Iceland", "IS", "ISL", "352", u"Iceland"),
    Country(u"India", "IN", "IND", "356", u"India"),
    Country(u"Indonesia", "ID", "IDN", "360", u"Indonesia"),
    Country(u"Iran, Islamic Republic of", "IR", "IRN", "364",
            u"Iran, Islamic Republic of"),
    Country(u"Iraq", "IQ", "IRQ", "368", u"Iraq"),
    Country(u"Ireland", "IE", "IRL", "372", u"Ireland"),
    Country(u"Isle of Man", "IM", "IMN", "833", u"Isle of Man"),
    Country(u"Israel", "IL", "ISR", "376", u"Israel"),
    Country(u"Italy", "IT", "ITA", "380", u"Italy"),
    Country(u"Jamaica", "JM", "JAM", "388", u"Jamaica"),
    Country(u"Japan", "JP", "JPN", "392", u"Japan"),
    Country(u"Jersey", "JE", "JEY", "832", u"Jersey"),
    Country(u"Jordan", "JO", "JOR", "400", u"Jordan"),
    Country(u"Kazakhstan", "KZ", "KAZ", "398", u"Kazakhstan"),
    Country(u"Kenya", "KE", "KEN", "404", u"Kenya"),
    Country(u"Kiribati", "KI", "KIR", "296", u"Kiribati"),
    Country(u"Korea, Democratic People's Republic of", "KP", "PRK", "408",
            u"Korea, Democratic People's Republic of"),
    Country(u"Korea, Republic of", "KR", "KOR", "410", u"Korea, Republic of"),
    Country(u"Kosovo", "XK", "XKX", "983", u"Kosovo"),
    Country(u"Kuwait", "KW", "KWT", "414", u"Kuwait"),
    Country(u"Kyrgyzstan", "KG", "KGZ", "417", u"Kyrgyzstan"),
    Country(u"Lao People's Democratic Republic", "LA", "LAO", "418",
            u"Lao People's Democratic Republic"),
    Country(u"Latvia", "LV", "LVA", "428", u"Latvia"),
    Country(u"Lebanon", "LB", "LBN", "422", u"Lebanon"),
    Country(u"Lesotho", "LS", "LSO", "426", u"Lesotho"),
    Country(u"Liberia", "LR", "LBR", "430", u"Liberia"),
    Country(u"Libya", "LY", "LBY", "434", u"Libya"),
    Country(u"Liechtenstein", "LI", "LIE", "438", u"Liechtenstein"),
    Country(u"Lithuania", "LT", "LTU", "440", u"Lithuania"),
    Country(u"Luxembourg", "LU", "LUX", "442", u"Luxembourg"),
    Country(u"Macao", "MO", "MAC", "446", u"Macao"),
    Country(u"North Macedonia", "MK", "MKD", "807", u"North Macedonia"),
    Country(u"Madagascar", "MG", "MDG", "450", u"Madagascar"),
    Country(u"Malawi", "MW", "MWI", "454", u"Malawi"),
    Country(u"Malaysia", "MY", "MYS", "458", u"Malaysia"),
    Country(u"Maldives", "MV", "MDV", "462", u"Maldives"),
    Country(u"Mali", "ML", "MLI", "466", u"Mali"),
    Country(u"Malta", "MT", "MLT", "470", u"Malta"),
    Country(u"Marshall Islands", "MH", "MHL", "584", u"Marshall Islands"),
    Country(u"Martinique", "MQ", "MTQ", "474", u"Martinique"),
    Country(u"Mauritania", "MR", "MRT", "478", u"Mauritania"),
    Country(u"Mauritius", "MU", "MUS", "480", u"Mauritius"),
    Country(u"Mayotte", "YT", "MYT", "175", u"Mayotte"),
    Country(u"Mexico", "MX", "MEX", "484", u"Mexico"),
    Country(u"Micronesia, Federated States of", "FM", "FSM", "583",
            u"Micronesia, Federated States of"),
    Country(u"Moldova, Republic of", "MD", "MDA", "498",
            u"Moldova, Republic of"),
    Country(u"Monaco", "MC", "MCO", "492", u"Monaco"),
    Country(u"Mongolia", "MN", "MNG", "496", u"Mongolia"),
    Country(u"Montenegro", "ME", "MNE", "499", u"Montenegro"),
    Country(u"Montserrat", "MS", "MSR", "500", u"Montserrat"),
    Country(u"Morocco", "MA", "MAR", "504", u"Morocco"),
    Country(u"Mozambique", "MZ", "MOZ", "508", u"Mozambique"),
    Country(u"Myanmar", "MM", "MMR", "104", u"Myanmar"),
    Country(u"Namibia", "NA", "NAM", "516", u"Namibia"),
    Country(u"Nauru", "NR", "NRU", "520", u"Nauru"),
    Country(u"Nepal", "NP", "NPL", "524", u"Nepal"),
    Country(u"Netherlands", "NL", "NLD", "528", u"Netherlands"),
    Country(u"New Caledonia", "NC", "NCL", "540", u"New Caledonia"),
    Country(u"New Zealand", "NZ", "NZL", "554", u"New Zealand"),
    Country(u"Nicaragua", "NI", "NIC", "558", u"Nicaragua"),
    Country(u"Niger", "NE", "NER", "562", u"Niger"),
    Country(u"Nigeria", "NG", "NGA", "566", u"Nigeria"),
    Country(u"Niue", "NU", "NIU", "570", u"Niue"),
    Country(u"Norfolk Island", "NF", "NFK", "574", u"Norfolk Island"),
    Country(u"Northern Mariana Islands", "MP", "MNP", "580",
            u"Northern Mariana Islands"),
    Country(u"Norway", "NO", "NOR", "578", u"Norway"),
    Country(u"Oman", "OM", "OMN", "512", u"Oman"),
    Country(u"Pakistan", "PK", "PAK", "586", u"Pakistan"),
    Country(u"Palau", "PW", "PLW", "585", u"Palau"),
    Country(u"Palestine, State of", "PS", "PSE", "275",
            u"Palestine"),
    Country(u"Panama", "PA", "PAN", "591", u"Panama"),
    Country(u"Papua New Guinea", "PG", "PNG", "598",
            u"Papua New Guinea"),
    Country(u"Paraguay", "PY", "PRY", "600", u"Paraguay"),
    Country(u"Peru", "PE", "PER", "604", u"Peru"),
    Country(u"Philippines", "PH", "PHL", "608", u"Philippines"),
    Country(u"Pitcairn", "PN", "PCN", "612", u"Pitcairn"),
    Country(u"Poland", "PL", "POL", "616", u"Poland"),
    Country(u"Portugal", "PT", "PRT", "620", u"Portugal"),
    Country(u"Puerto Rico", "PR", "PRI", "630", u"Puerto Rico"),
    Country(u"Qatar", "QA", "QAT", "634", u"Qatar"),
    Country(u"Réunion", "RE", "REU", "638", u"Réunion"),
    Country(u"Romania", "RO", "ROU", "642", u"Romania"),
    Country(u"Russian Federation", "RU", "RUS", "643",
            u"Russian Federation"),
    Country(u"Rwanda", "RW", "RWA", "646", u"Rwanda"),
    Country(u"Saint Barthélemy", "BL", "BLM", "652",
            u"Saint Barthélemy"),
    Country(u"Saint Helena, Ascension and Tristan da Cunha",
            "SH", "SHN", "654",
            u"Saint Helena, Ascension and Tristan da Cunha"),
    Country(u"Saint Kitts and Nevis", "KN", "KNA", "659",
            u"Saint Kitts and Nevis"),
    Country(u"Saint Lucia", "LC", "LCA", "662", u"Saint Lucia"),
    Country(u"Saint Martin (French part)", "MF", "MAF", "663",
            u"Saint Martin (French part)"),
    Country(u"Saint Pierre and Miquelon", "PM", "SPM", "666",
            u"Saint Pierre and Miquelon"),
    Country(u"Saint Vincent and the Grenadines", "VC", "VCT", "670",
            u"Saint Vincent and the Grenadines"),
    Country(u"Samoa", "WS", "WSM", "882", u"Samoa"),
    Country(u"San Marino", "SM", "SMR", "674", u"San Marino"),
    Country(u"Sao Tome and Principe", "ST", "STP", "678",
            u"Sao Tome and Principe"),
    Country(u"Saudi Arabia", "SA", "SAU", "682", u"Saudi Arabia"),
    Country(u"Senegal", "SN", "SEN", "686", u"Senegal"),
    Country(u"Serbia", "RS", "SRB", "688", u"Serbia"),
    Country(u"Seychelles", "SC", "SYC", "690", u"Seychelles"),
    Country(u"Sierra Leone", "SL", "SLE", "694", u"Sierra Leone"),
    Country(u"Singapore", "SG", "SGP", "702", u"Singapore"),
    Country(u"Sint Maarten (Dutch part)", "SX", "SXM", "534",
            u"Sint Maarten (Dutch part)"),
    Country(u"Slovakia", "SK", "SVK", "703", u"Slovakia"),
    Country(u"Slovenia", "SI", "SVN", "705", u"Slovenia"),
    Country(u"Solomon Islands", "SB", "SLB", "090", u"Solomon Islands"),
    Country(u"Somalia", "SO", "SOM", "706", u"Somalia"),
    Country(u"South Africa", "ZA", "ZAF", "710", u"South Africa"),
    Country(u"South Georgia and the South Sandwich Islands",
            "GS", "SGS", "239",
            u"South Georgia and the South Sandwich Islands",),
    Country(u"South Sudan", "SS", "SSD", "728", u"South Sudan"),
    Country(u"Spain", "ES", "ESP", "724", u"Spain"),
    Country(u"Sri Lanka", "LK", "LKA", "144", u"Sri Lanka"),
    Country(u"Sudan", "SD", "SDN", "729", u"Sudan"),
    Country(u"Suriname", "SR", "SUR", "740", u"Suriname"),
    Country(u"Svalbard and Jan Mayen", "SJ", "SJM", "744",
            u"Svalbard and Jan Mayen"),
    Country(u"Eswatini", "SZ", "SWZ", "748", u"Eswatini"),
    Country(u"Sweden", "SE", "SWE", "752", u"Sweden"),
    Country(u"Switzerland", "CH", "CHE", "756", u"Switzerland"),
    Country(u"Syrian Arab Republic", "SY", "SYR", "760",
            u"Syrian Arab Republic"),
    Country(u"Taiwan, Province of China", "TW", "TWN", "158",
            u"Taiwan"),
    Country(u"Tajikistan", "TJ", "TJK", "762", u"Tajikistan"),
    Country(u"Tanzania, United Republic of", "TZ", "TZA", "834",
            u"Tanzania, United Republic of"),
    Country(u"Thailand", "TH", "THA", "764", u"Thailand"),
    Country(u"Timor-Leste", "TL", "TLS", "626", u"Timor-Leste"),
    Country(u"Togo", "TG", "TGO", "768", u"Togo"),
    Country(u"Tokelau", "TK", "TKL", "772", u"Tokelau"),
    Country(u"Tonga", "TO", "TON", "776", u"Tonga"),
    Country(u"Trinidad and Tobago", "TT", "TTO", "780",
            u"Trinidad and Tobago"),
    Country(u"Tunisia", "TN", "TUN", "788", u"Tunisia"),
    Country(u"Turkey", "TR", "TUR", "792", u"Turkey"),
    Country(u"Turkmenistan", "TM", "TKM", "795", u"Turkmenistan"),
    Country(u"Turks and Caicos Islands", "TC", "TCA", "796",
            u"Turks and Caicos Islands"),
    Country(u"Tuvalu", "TV", "TUV", "798", u"Tuvalu"),
    Country(u"Uganda", "UG", "UGA", "800", u"Uganda"),
    Country(u"Ukraine", "UA", "UKR", "804", u"Ukraine"),
    Country(u"United Arab Emirates", "AE", "ARE", "784",
            u"United Arab Emirates"),
    Country(u"United Kingdom of Great Britain and Northern Ireland",
            "GB", "GBR", "826",
            u"United Kingdom of Great Britain and Northern Ireland"),
    Country(u"United States of America", "US", "USA", "840",
            u"United States of America"),
    Country(u"United States Minor Outlying Islands", "UM", "UMI", "581",
            u"United States Minor Outlying Islands"),
    Country(u"Uruguay", "UY", "URY", "858", u"Uruguay"),
    Country(u"Uzbekistan", "UZ", "UZB", "860", u"Uzbekistan"),
    Country(u"Vanuatu", "VU", "VUT", "548", u"Vanuatu"),
    Country(u"Venezuela, Bolivarian Republic of", "VE", "VEN", "862",
            u"Venezuela, Bolivarian Republic of"),
    Country(u"Viet Nam", "VN", "VNM", "704", u"Viet Nam"),
    Country(u"Virgin Islands, British", "VG", "VGB", "092",
            u"Virgin Islands, British"),
    Country(u"Virgin Islands, U.S.", "VI", "VIR", "850",
            u"Virgin Islands, U.S."),
    Country(u"Wallis and Futuna", "WF", "WLF", "876", u"Wallis and Futuna"),
    Country(u"Western Sahara", "EH", "ESH", "732", u"Western Sahara"),
    Country(u"Yemen", "YE", "YEM", "887", u"Yemen"),
    Country(u"Zambia", "ZM", "ZMB", "894", u"Zambia"),
    Country(u"Zimbabwe", "ZW", "ZWE", "716", u"Zimbabwe")]


def _build_index(idx):
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


NOT_FOUND = object()


class _CountryLookup(object):

    def get(self, key, default=NOT_FOUND):
        if isinstance(key, Integral):
            r = _by_numeric.get("%03d" % key, default)
        elif isinstance(key, basestring):
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
        else:
            r = default

        if r == NOT_FOUND:
            raise KeyError(key)

        return r

    __getitem__ = get

    def __len__(self):
        return len(_records)

    def __iter__(self):
        return iter(_records)

    def __contains__(self, item):
        try:
            self.get(item)
            return True
        except KeyError:
            return False


countries = _CountryLookup()
