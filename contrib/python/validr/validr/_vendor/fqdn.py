"""
https://github.com/ypcrts/fqdn/blob/develop/fqdn/__init__.py

Changes: disable cached_property
"""
import re

try:
    from functools import cached_property  # Python 3.8
except ImportError:
    cached_property = property


class FQDN:
    """
    From https://tools.ietf.org/html/rfc1035#page-9,  RFC 1035 3.1. Name space
    definitions:

        Domain names in messages are expressed in terms of a sequence of
        labels. Each label is represented as a one octet length field followed
        by that number of octets.  Since every domain name ends with the null
        label of the root, a domain name is terminated by a length byte of
        zero.  The high order two bits of every length octet must be zero, and
        the remaining six bits of the length field limit the label to 63 octets
        or less.

        To simplify implementations, the total length of a domain name (i.e.,
        label octets and label length octets) is restricted to 255 octets or
        less.


    Therefore the max length of a domain name is actually 253 ASCII bytes
    without the trailing null byte or the leading length byte, and the max
    length of a label is 62 bytes without the leading length byte.
    """

    FQDN_REGEX = re.compile(
        r"^((?!-)[-A-Z\d]{1,63}(?<!-)\.)+(?!-)[-A-Z\d]{1,63}(?<!-)\.?$", re.IGNORECASE
    )

    def __init__(self, fqdn):
        if not (fqdn and isinstance(fqdn, str)):
            raise ValueError("fqdn must be str")
        self.fqdn = fqdn

    def __str__(self):
        return self.fqdn

    @cached_property
    def is_valid(self):
        """
        True for a validated fully-qualified domain nam (FQDN), in full
        compliance with RFC 1035, and the "preferred form" specified in RFC
        3686 s. 2, whether relative or absolute.

        https://tools.ietf.org/html/rfc3696#section-2
        https://tools.ietf.org/html/rfc1035

        If and only if the FQDN ends with a dot (in place of the RFC1035
        trailing null byte), it may have a total length of 254 bytes, still it
        must be less than 253 bytes.
        """
        length = len(self.fqdn)
        if self.fqdn.endswith("."):
            length -= 1
        if length > 253:
            return False
        return bool(self.FQDN_REGEX.match(self.fqdn))

    @cached_property
    def is_valid_absolute(self):
        """
        True for a fully-qualified domain name (FQDN) that is RFC
        preferred-form compliant and ends with a `.`.

        With relative FQDNS in DNS lookups, the current hosts domain name or
        search domains may be appended.
        """
        return self.fqdn.endswith(".") and self.is_valid

    @cached_property
    def is_valid_relative(self):
        """
        True for a validated fully-qualified domain name that compiles with the
        RFC preferred-form and does not ends with a `.`.
        """
        return not self.fqdn.endswith(".") and self.is_valid

    @cached_property
    def absolute(self):
        """
        The FQDN as a string in absolute form
        """
        if not self.is_valid:
            raise ValueError("invalid FQDN `{0}`".format(self.fqdn))

        if self.is_valid_absolute:
            return self.fqdn

        return "{0}.".format(self.fqdn)

    @cached_property
    def relative(self):
        """
        The FQDN as a string in relative form
        """
        if not self.is_valid:
            raise ValueError("invalid FQDN `{0}`".format(self.fqdn))

        if self.is_valid_absolute:
            return self.fqdn[:-1]

        return self.fqdn
