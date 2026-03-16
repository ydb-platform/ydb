"""Common text manipulation functions."""
import unicodedata
import regex

matched_punctuation_marks = [('“', '”'), ('‘', '’'), ('(', ')'), ('[', ']'), ('{', '}')]
terminal_punctuation_marks = ['.', ',', '!', ':', ';', '?']


def normalise_unicode(text):
    """Normalise unicode such that things that are visually equivalent map to the same unicode string where possible."""
    normal_form = "NFKC"
    text = unicodedata.normalize(normal_form, text)
    return text


def normalise_whitespace(text):
    """Replace runs of whitespace characters with a single space as this is what happens when HTML text is displayed."""
    text = regex.sub(r"\s+", " ", text)
    # Remove leading and trailing whitespace
    text = text.strip()
    return text


def normalise_text(text):
    """Normalise unicode and whitespace."""
    # Normalise unicode first to try and standardise whitespace characters as much as possible before normalising them
    text = strip_control_characters(text)
    text = normalise_unicode(text)
    text = normalise_whitespace(text)
    return text


def strip_html_whitespace(text):
    """Simplify HTML by stripping whitespace."""
    # Normalise unicode first to try and standardise whitespace characters as much as possible before normalising them
    text = normalise_text(text)
    text = text.replace(" <", "<").replace("> ", ">")
    return text


def strip_control_characters(text):
    """Strip out unicode control characters which might break the parsing."""
    # Unicode control characters
    #   [Cc]: Other, Control [includes new lines]
    #   [Cf]: Other, Format
    #   [Cn]: Other, Not Assigned
    #   [Co]: Other, Private Use
    #   [Cs]: Other, Surrogate
    control_chars = set(['Cc', 'Cf', 'Cn', 'Co', 'Cs'])
    retained_chars = ['\t', '\n', '\r', '\f']

    # Remove non-printing control characters
    return "".join(["" if (unicodedata.category(char) in control_chars) and (char not in retained_chars) else char for char in text])
