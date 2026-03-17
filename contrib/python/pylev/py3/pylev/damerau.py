import sys


PY2 = sys.version_info[0] == 2

if PY2:
    range = xrange


def damerau_levenshtein(string_1, string_2):
    """
    Calculates the Damerau-Levenshtein distance between two strings.

    In addition to insertions, deletions and substitutions,
    Damerau-Levenshtein considers adjacent transpositions.

    This version is based on an iterative version of the Wagner-Fischer algorithm.

    Usage::

        >>> damerau_levenshtein('kitten', 'sitting')
        3
        >>> damerau_levenshtein('kitten', 'kittne')
        1
        >>> damerau_levenshtein('', '')
        0

    """
    if string_1 == string_2:
        return 0

    len_1 = len(string_1)
    len_2 = len(string_2)

    if len_1 == 0:
        return len_2
    if len_2 == 0:
        return len_1

    if len_1 > len_2:
        string_2, string_1 = string_1, string_2
        len_2, len_1 = len_1, len_2

    prev_cost = 0
    d0 = [i for i in range(len_2 + 1)]
    d1 = [j for j in range(len_2 + 1)]
    dprev = d0[:]

    s1 = string_1
    s2 = string_2

    for i in range(len_1):
        d1[0] = i + 1
        for j in range(len_2):
            cost = d0[j]

            if s1[i] != s2[j]:
                # substitution
                cost += 1

                # insertion
                x_cost = d1[j] + 1
                if x_cost < cost:
                    cost = x_cost

                # deletion
                y_cost = d0[j + 1] + 1
                if y_cost < cost:
                    cost = y_cost

                # transposition
                if i > 0 and j > 0 and s1[i] == s2[j - 1] and s1[i - 1] == s2[j]:
                    transp_cost = dprev[j - 1] + 1
                    if transp_cost < cost:
                        cost = transp_cost
            d1[j + 1] = cost

        dprev, d0, d1 = d0, d1, dprev

    return d0[-1]
