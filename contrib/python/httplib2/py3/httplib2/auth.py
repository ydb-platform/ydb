import re

import pyparsing as pp

from .error import MalformedHeader


UNQUOTE_PAIRS = re.compile(r"\\(.)")
unquote = lambda s, _, t: UNQUOTE_PAIRS.sub(r"\1", t[0][1:-1])

# https://tools.ietf.org/html/rfc7235#section-1.2
# https://tools.ietf.org/html/rfc7235#appendix-B
tchar = "!#$%&'*+-.^_`|~" + pp.nums + pp.alphas
token = pp.Word(tchar).set_name("token")
token68 = pp.Combine(pp.Word("-._~+/" + pp.nums + pp.alphas) + pp.Optional(pp.Word("=").leave_whitespace())).set_name(
    "token68"
)

quoted_string = pp.dbl_quoted_string.copy().set_name("quoted-string").set_parse_action(unquote)
auth_param_name = token.copy().set_name("auth-param-name").add_parse_action(pp.common.downcase_tokens)
auth_param = auth_param_name + pp.Suppress("=") + (quoted_string | token)
params = pp.Dict(pp.DelimitedList(pp.Group(auth_param)))

scheme = token("scheme")
challenge = scheme + (params("params") | token68("token"))

authentication_info = params.copy()
www_authenticate = pp.DelimitedList(pp.Group(challenge))


def _parse_authentication_info(headers, headername="authentication-info"):
    """https://tools.ietf.org/html/rfc7615
    """
    header = headers.get(headername, "").strip()
    if not header:
        return {}
    try:
        parsed = authentication_info.parse_string(header)
    except pp.ParseException:
        # print(ex.explain(ex))
        raise MalformedHeader(headername)

    return parsed.as_dict()


def _parse_www_authenticate(headers, headername="www-authenticate"):
    """Returns a dictionary of dictionaries, one dict per auth_scheme."""
    header = headers.get(headername, "").strip()
    if not header:
        return {}
    try:
        parsed = www_authenticate.parse_string(header)
    except pp.ParseException:
        # print(ex.explain(ex))
        raise MalformedHeader(headername)

    retval = {
        challenge["scheme"].lower(): challenge["params"].as_dict()
        if "params" in challenge
        else {"token": challenge.get("token")}
        for challenge in parsed
    }
    return retval
