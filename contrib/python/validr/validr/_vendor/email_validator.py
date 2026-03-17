"""
https://github.com/JoshData/python-email-validator/blob/master/email_validator/__init__.py

Changes: remove check_deliverability, remove allow_smtputf8
"""
import sys
import re
import idna  # implements IDNA 2008; Python's codec is only IDNA 2003


# Based on RFC 2822 section 3.2.4 / RFC 5322 section 3.2.3, these
# characters are permitted in email addresses (not taking into
# account internationalization):
ATEXT = r'a-zA-Z0-9_!#\$%&\'\*\+\-/=\?\^`\{\|\}~'

# A "dot atom text", per RFC 2822 3.2.4:
DOT_ATOM_TEXT = '[' + ATEXT + ']+(?:\\.[' + ATEXT + ']+)*'

# RFC 6531 section 3.3 extends the allowed characters in internationalized
# addresses to also include three specific ranges of UTF8 defined in
# RFC3629 section 4, which appear to be the Unicode code points from
# U+0080 to U+10FFFF.
ATEXT_UTF8 = ATEXT + u"\u0080-\U0010FFFF"
DOT_ATOM_TEXT_UTF8 = '[' + ATEXT_UTF8 + ']+(?:\\.[' + ATEXT_UTF8 + ']+)*'

# The domain part of the email address, after IDNA (ASCII) encoding,
# must also satisfy the requirements of RFC 952/RFC 1123 which restrict
# the allowed characters of hostnames further. The hyphen cannot be at
# the beginning or end of a *dot-atom component* of a hostname either.
ATEXT_HOSTNAME = r'(?:(?:[a-zA-Z0-9][a-zA-Z0-9\-]*)?[a-zA-Z0-9])'

# ease compatibility in type checking
if sys.version_info >= (3,):
    unicode_class = str
else:
    unicode_class = unicode  # noqa: F821

    # turn regexes to unicode (because 'ur' literals are not allowed in Py3)
    ATEXT = ATEXT.decode("ascii")
    DOT_ATOM_TEXT = DOT_ATOM_TEXT.decode("ascii")
    ATEXT_HOSTNAME = ATEXT_HOSTNAME.decode("ascii")

DEFAULT_TIMEOUT = 15  # secs


class EmailNotValidError(ValueError):
    """Parent class of all exceptions raised by this module."""
    pass


class EmailSyntaxError(EmailNotValidError):
    """Exception raised when an email address fails validation because of its form."""
    pass


class EmailUndeliverableError(EmailNotValidError):
    """Exception raised when an email address fails validation because its domain name does not appear deliverable."""
    pass


def validate_email(
    email,
    allow_smtputf8=True,
    allow_empty_local=False,
    check_deliverability=True,
    timeout=DEFAULT_TIMEOUT,
):
    """
    Validates an email address, raising an EmailNotValidError if the address is not valid or returning a dict of
    information when the address is valid. The email argument can be a str or a bytes instance,
    but if bytes it must be ASCII-only.
    """

    # Allow email to be a str or bytes instance. If bytes,
    # it must be ASCII because that's how the bytes work
    # on the wire with SMTP.
    if not isinstance(email, (str, unicode_class)):
        try:
            email = email.decode("ascii")
        except ValueError:
            raise EmailSyntaxError("The email address is not valid ASCII.")

    # At-sign.
    parts = email.split('@')
    if len(parts) != 2:
        raise EmailSyntaxError("The email address is not valid. It must have exactly one @-sign.")

    # Prepare a dict to return on success.
    ret = {}

    # Validate the email address's local part syntax and update the return
    # dict with metadata.
    ret.update(validate_email_local_part(parts[0], allow_smtputf8=allow_smtputf8, allow_empty_local=allow_empty_local))

    # Validate the email address's domain part syntax and update the return
    # dict with metadata.
    ret.update(validate_email_domain_part(parts[1]))

    # If the email address has an ASCII form, add it.
    ret["email"] = ret["local"] + "@" + ret["domain_i18n"]
    if not ret["smtputf8"]:
        ret["email_ascii"] = ret["local"] + "@" + ret["domain"]

    return ret


def validate_email_local_part(local, allow_smtputf8=True, allow_empty_local=False):
    # Validates the local part of an email address.

    if len(local) == 0:
        if not allow_empty_local:
            raise EmailSyntaxError("There must be something before the @-sign.")
        else:
            # The caller allows an empty local part. Useful for validating certain
            # Postfix aliases.
            return {
                "local": local,
                "smtputf8": False,
            }

    # RFC 5321 4.5.3.1.1
    if len(local) > 64:
        raise EmailSyntaxError("The email address is too long before the @-sign.")

    # Check the local part against the regular expression for the older ASCII requirements.
    m = re.match(DOT_ATOM_TEXT + "$", local)
    if m:
        # Return the local part unchanged and flag that SMTPUTF8 is not needed.
        return {
            "local": local,
            "smtputf8": False,
        }

    else:
        # The local part failed the ASCII check. Now try the extended internationalized requirements.
        m = re.match(DOT_ATOM_TEXT_UTF8 + "$", local)
        if not m:
            # It's not a valid internationalized address either. Report which characters were not valid.
            bad_chars = ', '.join(sorted(set(
                c for c in local if not re.match(u"[" + (ATEXT if not allow_smtputf8 else ATEXT_UTF8) + u"]", c)
            )))
            raise EmailSyntaxError("The email address contains invalid characters before the @-sign: %s." % bad_chars)

        # It would be valid if internationalized characters were allowed by the caller.
        raise EmailSyntaxError("Internationalized characters before the @-sign are not supported.")


def validate_email_domain_part(domain):
    # Empty?
    if len(domain) == 0:
        raise EmailSyntaxError("There must be something after the @-sign.")

    # Perform UTS-46 normalization, which includes casefolding, NFC normalization,
    # and converting all label separators (the period/full stop, fullwidth full stop,
    # ideographic full stop, and halfwidth ideographic full stop) to basic periods.
    # It will also raise an exception if there is an invalid character in the input,
    # such as "⒈" which is invalid because it would expand to include a period.
    try:
        domain = idna.uts46_remap(domain, std3_rules=False, transitional=False)
    except idna.IDNAError as e:
        raise EmailSyntaxError("The domain name %s contains invalid characters (%s)." % (domain, str(e)))

    # Now we can perform basic checks on the use of periods (since equivalent
    # symbols have been mapped to periods). These checks are needed because the
    # IDNA library doesn't handle well domains that have empty labels (i.e. initial
    # dot, trailing dot, or two dots in a row).
    if domain.endswith("."):
        raise EmailSyntaxError("An email address cannot end with a period.")
    if domain.startswith("."):
        raise EmailSyntaxError("An email address cannot have a period immediately after the @-sign.")
    if ".." in domain:
        raise EmailSyntaxError("An email address cannot have two periods in a row.")

    # Regardless of whether international characters are actually used,
    # first convert to IDNA ASCII. For ASCII-only domains, the transformation
    # does nothing. If internationalized characters are present, the MTA
    # must either support SMTPUTF8 or the mail client must convert the
    # domain name to IDNA before submission.
    #
    # Unfortunately this step incorrectly 'fixes' domain names with leading
    # periods by removing them, so we have to check for this above. It also gives
    # a funky error message ("No input") when there are two periods in a
    # row, also checked separately above.
    try:
        domain = idna.encode(domain, uts46=False).decode("ascii")
    except idna.IDNAError as e:
        raise EmailSyntaxError("The domain name %s contains invalid characters (%s)." % (domain, str(e)))

    # We may have been given an IDNA ASCII domain to begin with. Check
    # that the domain actually conforms to IDNA. It could look like IDNA
    # but not be actual IDNA. For ASCII-only domains, the conversion out
    # of IDNA just gives the same thing back.
    #
    # This gives us the canonical internationalized form of the domain,
    # which we should use in all error messages.
    try:
        domain_i18n = idna.decode(domain.encode('ascii'))
    except idna.IDNAError as e:
        raise EmailSyntaxError("The domain name %s is not valid IDNA (%s)." % (domain, str(e)))

    # RFC 5321 4.5.3.1.2
    if len(domain) > 255:
        raise EmailSyntaxError("The email address is too long after the @-sign.")

    # A "dot atom text", per RFC 2822 3.2.4, but using the restricted
    # characters allowed in a hostname (see ATEXT_HOSTNAME above).
    DOT_ATOM_TEXT = ATEXT_HOSTNAME + r'(?:\.' + ATEXT_HOSTNAME + r')*'

    # Check the regular expression. This is probably entirely redundant
    # with idna.decode, which also checks this format.
    m = re.match(DOT_ATOM_TEXT + "$", domain)
    if not m:
        raise EmailSyntaxError("The email address contains invalid characters after the @-sign.")

    # All publicly deliverable addresses have domain named with at least
    # one period. We also know that all TLDs end with a letter.
    if "." not in domain:
        raise EmailSyntaxError("The domain name %s is not valid. It should have a period." % domain_i18n)
    if not re.search(r"[A-Za-z]$", domain):
        raise EmailSyntaxError(
            "The domain name %s is not valid. It is not within a valid top-level domain." % domain_i18n
        )

    # Return the IDNA ASCII-encoded form of the domain, which is how it
    # would be transmitted on the wire (except when used with SMTPUTF8
    # possibly), as well as the canonical Unicode form of the domain,
    # which is better for display purposes. This should also take care
    # of RFC 6532 section 3.1's suggestion to apply Unicode NFC
    # normalization to addresses.
    return {
        "domain": domain,
        "domain_i18n": domain_i18n,
    }


def main():
    import sys
    import json

    if len(sys.argv) == 1:
        # Read lines for STDIN and validate the email address on each line.
        allow_smtputf8 = True
        for line in sys.stdin:
            try:
                email = line.strip()
                if sys.version_info < (3,):
                    email = email.decode("utf8")  # assume utf8 in input
                validate_email(email, allow_smtputf8=allow_smtputf8)
            except EmailNotValidError as e:
                print(email, e)
    else:
        # Validate the email address passed on the command line.
        email = sys.argv[1]
        allow_smtputf8 = True
        check_deliverability = True
        if sys.version_info < (3,):
            email = email.decode("utf8")  # assume utf8 in input
        try:
            result = validate_email(email, allow_smtputf8=allow_smtputf8, check_deliverability=check_deliverability)
            print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False))
        except EmailNotValidError as e:
            if sys.version_info < (3,):
                print(unicode_class(e).encode("utf8"))
            else:
                print(e)


if __name__ == "__main__":
    main()
