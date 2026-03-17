def recursive_levenshtein(
    string_1, string_2, len_1=None, len_2=None, offset_1=0, offset_2=0, memo=None
):
    """
    Calculates the Levenshtein distance between two strings.

    Usage::

        >>> recursive_levenshtein('kitten', 'sitting')
        3
        >>> recursive_levenshtein('kitten', 'kitten')
        0
        >>> recursive_levenshtein('', '')
        0

    """
    if len_1 is None:
        len_1 = len(string_1)

    if len_2 is None:
        len_2 = len(string_2)

    if memo is None:
        memo = {}

    key = ",".join([str(offset_1), str(len_1), str(offset_2), str(len_2)])

    if memo.get(key) is not None:
        return memo[key]

    if len_1 == 0:
        return len_2
    elif len_2 == 0:
        return len_1

    cost = 0

    if string_1[offset_1] != string_2[offset_2]:
        cost = 1

    dist = min(
        recursive_levenshtein(
            string_1, string_2, len_1 - 1, len_2, offset_1 + 1, offset_2, memo
        )
        + 1,
        recursive_levenshtein(
            string_1, string_2, len_1, len_2 - 1, offset_1, offset_2 + 1, memo
        )
        + 1,
        recursive_levenshtein(
            string_1, string_2, len_1 - 1, len_2 - 1, offset_1 + 1, offset_2 + 1, memo
        )
        + cost,
    )
    memo[key] = dist
    return dist
