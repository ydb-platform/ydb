from .errors import LineParseError
from .style_mapping_parser import parse_style_mapping
from .tokeniser import tokenise
from .token_iterator import TokenIterator
from ... import results


def read_style_mapping(string):
    try:
        tokens = tokenise(string)
        return results.success(parse_style_mapping(TokenIterator(tokens)))
    except LineParseError:
        warning = "Did not understand this style mapping, so ignored it: " + string
        return results.Result(None, [results.warning(warning)])
