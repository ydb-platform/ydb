import netaddr


class mac_unix_common(netaddr.mac_eui48):
    """Common form of UNIX MAC address dialect class"""
    word_sep = ':'
    word_fmt = '%.2x'


class mac_eui64:
    """A standard IEEE EUI-64 dialect class."""

    #: The individual word size (in bits) of this address type.
    word_size = 8

    #: The number of words in this address type.
    num_words = 64 // word_size

    #: The maximum integer value for an individual word in this address type.
    max_word = 2 ** word_size - 1

    #: The separator character used between each word.
    word_sep = ":"

    #: The format string to be used when converting words to string values.
    word_fmt = "%.2x"

    #: The number base to be used when interpreting word values as integers.
    word_base = 16
