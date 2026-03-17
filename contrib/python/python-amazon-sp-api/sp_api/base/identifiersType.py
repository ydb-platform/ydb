from enum import Enum


class IdentifiersType(str, Enum):
    ASIN = 'ASIN'       # Amazon Standard Identification Number.
    EAN = 'EAN'         # European Article Number.
    GTIN = 'GTIN'       # Global Trade Item Number.
    ISBN = 'ISBN'       # International Standard Book Number.
    JAN = 'JAN'         # Japanese Article Number.
    MINSAN = 'MINSAN'   # Minsan Code.
    SKU = 'SKU'         # Stock Keeping Unit, a seller-specified identifier for an Amazon listing. Note: Must be accompanied by sellerId.
    UPC = 'UPC'         # Universal Product Code.