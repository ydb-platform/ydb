"""
This contains a bunch of RFC7230 definitions and regular expressions that are
needed to properly parse HTTP messages.
"""

import re

from .compat import tobytes

WS = "[ \t]"
OWS = WS + "{0,}?"
RWS = WS + "{1,}?"
BWS = OWS

# RFC 7230 Section 3.2.6 "Field Value Components":
# tchar          = "!" / "#" / "$" / "%" / "&" / "'" / "*"
#                / "+" / "-" / "." / "^" / "_" / "`" / "|" / "~"
#                / DIGIT / ALPHA
# obs-text      = %x80-FF
TCHAR = r"[!#$%&'*+\-.^_`|~0-9A-Za-z]"
OBS_TEXT = r"\x80-\xff"

TOKEN = TCHAR + "{1,}"

# RFC 5234 Appendix B.1 "Core Rules":
# VCHAR         =  %x21-7E
#                  ; visible (printing) characters
VCHAR = r"\x21-\x7e"

# header-field   = field-name ":" OWS field-value OWS
# field-name     = token
# field-value    = *( field-content / obs-fold )
# field-content  = field-vchar [ 1*( SP / HTAB ) field-vchar ]
# field-vchar    = VCHAR / obs-text

# Errata from: https://www.rfc-editor.org/errata_search.php?rfc=7230&eid=4189
# changes field-content to:
#
# field-content  = field-vchar [ 1*( SP / HTAB / field-vchar )
#                  field-vchar ]

FIELD_VCHAR = "[" + VCHAR + OBS_TEXT + "]"
# Field content is more greedy than the ABNF, in that it will match the whole value
FIELD_CONTENT = FIELD_VCHAR + "+(?:[ \t]+" + FIELD_VCHAR + "+)*"
# Which allows the field value here to just see if there is even a value in the first place
FIELD_VALUE = "(?:" + FIELD_CONTENT + ")?"

HEADER_FIELD = re.compile(
    tobytes(
        "^(?P<name>" + TOKEN + "):" + OWS + "(?P<value>" + FIELD_VALUE + ")" + OWS + "$"
    )
)
