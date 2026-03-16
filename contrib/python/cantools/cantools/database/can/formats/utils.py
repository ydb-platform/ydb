

def num(number_as_string: str) -> int | float:
    """Convert given string to an integer or a float.

    """

    try:
        return int(number_as_string)
    except ValueError:
        return float(number_as_string)
    except Exception:
        raise ValueError('Expected integer or floating point number.') from None
