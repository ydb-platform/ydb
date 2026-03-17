"""Helper methods for Hijri conversion."""


def jdn_to_ordinal(jdn: int) -> int:
    """Convert Julian day number (JDN) to date ordinal number.

    Args:
        jdn: Julian day number (JDN).
    """

    return jdn - 1721425


def ordinal_to_jdn(don: int) -> int:
    """Convert date ordinal number to Julian day number (JDN).

    Args:
        don: Date ordinal number.
    """

    return don + 1721425


def jdn_to_rjd(jdn: int) -> int:
    """Return Reduced Julian Day (RJD) number from Julian day number (JDN).

    Args:
        jdn: Julian day number (JDN).
    """

    return jdn - 2400000


def rjd_to_jdn(rjd: int) -> int:
    """Return Julian day number (JDN) from Reduced Julian Day (RJD) number.

    Args:
        rjd: Reduced Julian Day (RJD) number.
    """

    return rjd + 2400000
