def wildcard_match(pattern, text, case_insensitive=True):
    """
    Performs a case-insensitive wildcard match against two strings.
    This method works with pseduo-regex chars; specifically ? and * are supported.
    An asterisk (*) represents any combination of characters.
    A question mark (?) represents any single character.
    :param str pattern: the regex-like pattern to be compared against
    :param str text: the string to compare against the pattern
    :param boolean case_insensitive: dafault is True
    return whether the text matches the pattern
    """
    if pattern is None or text is None:
        return False

    if len(pattern) == 0:
        return len(text) == 0

    # Check the special case of a single * pattern, as it's common
    if pattern == '*':
        return True

    # If elif logic Checking different conditions like match between the first i chars in text
    # and the first p chars in pattern, checking pattern has '?' or '*' also check for case_insensitivity
    # iStar is introduced to store length of the text and i, p and pStar for indexing
    i = 0
    p = 0
    iStar = len(text)
    pStar = 0
    while i < len(text):
        if p < len(pattern) and text[i] == pattern[p]:
            i = i + 1
            p = p + 1

        elif p < len(pattern) and case_insensitive and text[i].lower() == pattern[p].lower():
            i = i + 1
            p = p + 1

        elif p < len(pattern) and pattern[p] == '?':
            i = i + 1
            p = p + 1

        elif p < len(pattern) and pattern[p] == '*':
            iStar = i
            pStar = p
            p += 1

        elif iStar != len(text):
            iStar += 1
            i = iStar
            p = pStar + 1

        else:
            return False

    while p < len(pattern) and pattern[p] == '*':
        p = p + 1

    return p == len(pattern) and i == len(text)
