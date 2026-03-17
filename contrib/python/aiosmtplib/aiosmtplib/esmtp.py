"""
ESMTP utils
"""

import re


__all__ = ("parse_esmtp_extensions",)


OLDSTYLE_AUTH_REGEX = re.compile(r"auth=(?P<auth>.*)", flags=re.I)
EXTENSIONS_REGEX = re.compile(r"(?P<ext>[A-Za-z0-9][A-Za-z0-9\-]*) ?")


def parse_esmtp_extensions(message: str) -> tuple[dict[str, str], list[str]]:
    """
    Parse an EHLO response from the server into a dict of {extension: params}
    and a list of auth method names.

    It might look something like:

         220 size.does.matter.af.MIL (More ESMTP than Crappysoft!)
         EHLO heaven.af.mil
         250-size.does.matter.af.MIL offers FIFTEEN extensions:
         250-8BITMIME
         250-PIPELINING
         250-DSN
         250-ENHANCEDSTATUSCODES
         250-EXPN
         250-HELP
         250-SAML
         250-SEND
         250-SOML
         250-TURN
         250-XADR
         250-XSTA
         250-ETRN
         250-XGEN
         250 SIZE 51200000
    """
    esmtp_extensions: dict[str, str] = {}
    auth_types: list[str] = []

    response_lines = message.split("\n")

    # ignore the first line
    for line in response_lines[1:]:
        # To be able to communicate with as many SMTP servers as possible,
        # we have to take the old-style auth advertisement into account,
        # because:
        # 1) Else our SMTP feature parser gets confused.
        # 2) There are some servers that only advertise the auth methods we
        #    support using the old style.
        auth_match = OLDSTYLE_AUTH_REGEX.match(line)
        if auth_match is not None:
            auth_type = auth_match.group("auth")
            auth_types.append(auth_type.lower().strip())

        # RFC 1869 requires a space between ehlo keyword and parameters.
        # It's actually stricter, in that only spaces are allowed between
        # parameters, but were not going to check for that here.  Note
        # that the space isn't present if there are no parameters.
        extensions = EXTENSIONS_REGEX.match(line)
        if extensions is not None:
            extension = extensions.group("ext").lower()
            params = extensions.string[extensions.end("ext") :].strip()
            esmtp_extensions[extension] = params

            if extension == "auth":
                auth_types.extend([param.strip().lower() for param in params.split()])

    return esmtp_extensions, auth_types
