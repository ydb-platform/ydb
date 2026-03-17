import sys
import os
import logging
from enum import Enum

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s"
)

'''
class AuthorizationError(Exception):
    def __init__(self, code, message=".env problem set your file in the root project"):
        logging.warning(message)


class AWS_ENV(Enum):
    PRODUCTION = "PRODUCTION"
    SANDBOX = "SANDBOX"
'''

class CurrencySymbols(Enum):
    EUR = "€"
    SEK = "kr"
    PLN = "zł"
    TRY = "₺"
    EGP = "£"
    AED = "د.إ"
    SAR = "﷼"
    GBP = "£"
    USD = "$"
    MXN = "$"
    BRL = "R$"
    CAD = "$"
    AUD = "$"
    INR = "₹"
    JPY = "¥"
    SGD = "$"


class Locales(Enum):
    ES = "es_ES"
    PT = "pt_PT"
    DE = "de_DE"
    FR = "fr_FR"
    BE = "be_BE"
    IT = "it_IT"
    NL = "nl_NL"
    SE = "sv_SE"
    PL = "pl_PL"
    TR = "tr_TR"
    EG = "ar_EG"
    AE = "en_AE"
    SA = "en_SA"
    GB = UK = "en_GB"
    US = "en_US"
    MX = "es_MX"
    BR = "pt_BR"
    CA = "en_CA"
    AU = "en_AU"
    IN = "en_IN"
    JP = "ja_JP"
    SG = "en_SG"
    CN = "zh_CN"


class Currencies(Enum):
    EU = ES = DE = FR = IT = NL = BE = "EUR"
    SE = "SEK"
    PL = "PLN"
    TR = "TRY"
    EG = "EGP"
    AE = "AED"
    SA = "SAR"
    GB = UK = "GBP"
    US = "USD"
    MX = "MXN"
    BR = "BRL"
    CA = "CAD"
    AU = "AUD"
    IN = "INR"
    JP = "JPY"
    SG = "SGD"


class MarketplacesIds(Enum):
    CA = 'A2EUQ1WTGCTBG2'
    US = 'ATVPDKIKX0DER'
    MX = 'A1AM78C64UM0Y8'
    BR = 'A2Q3Y263D00KWC'
    ES = 'A1RKKUPIHCS9HS'
    UK = GB = 'A1F83G8C2ARO7P'
    FR = 'A13V1IB3VIYZZH'
    BE = 'AMEN7PMS3EDWL'
    NL = 'A1805IZSGTT6HS'
    DE = 'A1PA6795UKMFR9'
    IT = 'APJ6JRA9NG5V4'
    SE = 'A2NODRKZP88ZB9'
    PL = 'A1C3SOZRARQ6R3'
    EG = 'ARBP9OOSHTCHU'
    TR = 'A33AVAJ2PDY3EV'
    SA = 'A17E79C6D8DWNP'
    AE = 'A2VIGQ35RCS4UG'
    IN = 'A21TJRUUN4KGV'
    SG = 'A19VAU5U5O7RUS'
    AU = 'A39IBJ37TRP1C6'
    JP = 'A1VC38T7YXB528'


class Marketplaces(Enum):
    # North America
    NA = US = {
        'prod': 'advertising-api.amazon.com',
        'currency': 'USD',
        'token_url': 'api.amazon.com/auth/o2/token'
    }
    CA = {
        'prod': 'advertising-api.amazon.com',
        'currency': 'CAD',
        'token_url': 'api.amazon.com/auth/o2/token'
    }
    MX = {
        'prod': 'advertising-api.amazon.com',
        'currency': 'MXN',
        'token_url': 'api.amazon.com/auth/o2/token'
    }
    BR = {
        'prod': 'advertising-api.amazon.com',
        'currency': 'BRL',
        'token_url': 'api.amazon.com/auth/o2/token'
    }
    # Far East
    JP = {
        'prod': 'advertising-api-fe.amazon.com',
        'currency': 'JPY',
        'token_url': 'api.amazon.co.jp/auth/o2/token'
    }
    AU = {
        'prod': 'advertising-api-fe.amazon.com',
        'currency': 'AUD',
        'token_url': 'api.amazon.co.jp/auth/o2/token'
    }
    SG = {
        'prod': 'advertising-api-fe.amazon.com',
        'currency': 'SGD',
        'token_url': 'api.amazon.co.jp/auth/o2/token'
    }
    # Europe
    EU = ES = DE = FR = IT = NL = BE = {
        'prod': 'advertising-api-eu.amazon.com',
        'currency': 'EUR',
        'token_url': 'api.amazon.co.uk/auth/o2/token'
    }
    UK = GB = {
        'prod': 'advertising-api-eu.amazon.com',
        'currency': 'GBP',
        'token_url': 'api.amazon.co.uk/auth/o2/token'
    }
    AE = {
        'prod': 'advertising-api-eu.amazon.com',
        'currency': 'AED',
        'token_url': 'api.amazon.co.uk/auth/o2/token'
    }
    SE = {
        'prod': 'advertising-api-eu.amazon.com',
        'currency': 'SEK',
        'token_url': 'api.amazon.co.uk/auth/o2/token'
    }
    PL = {
        'prod': 'advertising-api-eu.amazon.com',
        'currency': 'PLN',
        'token_url': 'api.amazon.co.uk/auth/o2/token'
    }
    TR = {
        'prod': 'advertising-api-eu.amazon.com',
        'currency': 'TRY',
        'token_url': 'api.amazon.co.uk/auth/o2/token'
    }
    IN = {
        'prod': 'advertising-api-eu.amazon.com',
        'currency': 'INR',
        'token_url': 'api.amazon.co.uk/auth/o2/token'
    }
    SA = {
        'prod': 'advertising-api-eu.amazon.com',
        'currency': 'SAR',
        'token_url': 'api.amazon.co.uk/auth/o2/token'
    }
    EG = {
        'prod': 'advertising-api-eu.amazon.com',
        'currency': 'EGP',
        'token_url': 'api.amazon.co.uk/auth/o2/token'
    }
    
    def __init__(self, info):
        self.region_url = info.get('prod')
        self.endpoint = 'https://{}'.format(self.region_url)
        self.currency = info.get('currency')
