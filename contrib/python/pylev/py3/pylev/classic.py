def classic_levenshtein(string_1, string_2):
    """
    Calculates the Levenshtein distance between two strings.

    This version is easier to read, but significantly slower than the version
    below (up to several orders of magnitude). Useful for learning, less so
    otherwise.

    Usage::

        >>> classic_levenshtein('kitten', 'sitting')
        3
        >>> classic_levenshtein('kitten', 'kitten')
        0
        >>> classic_levenshtein('', '')
        0

    """
    len_1 = len(string_1)
    len_2 = len(string_2)
    cost = 0

    if len_1 and len_2 and string_1[0] != string_2[0]:
        cost = 1

    if len_1 == 0:
        return len_2
    elif len_2 == 0:
        return len_1
    else:
        return min(
            classic_levenshtein(string_1[1:], string_2) + 1,
            classic_levenshtein(string_1, string_2[1:]) + 1,
            classic_levenshtein(string_1[1:], string_2[1:]) + cost,
        )
