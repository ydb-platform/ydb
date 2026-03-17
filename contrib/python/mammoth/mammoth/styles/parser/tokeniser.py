import collections
import re


Token = collections.namedtuple("Token", ["character_index", "type", "value"])


class TokenType(object):
    IDENTIFIER = "identifier"
    SYMBOL = "symbol"
    WHITESPACE = "whitespace"
    STRING = "string"
    UNTERMINATED_STRING = "unterminated string"
    INTEGER = "integer"
    END = "end"
    


def regex_tokeniser(rules):
    rules = [(token_type, _to_regex(regex)) for token_type, regex in rules]
    rules.append(("unknown", re.compile(".")))
    
    def tokenise(value):
        tokens = []
        index = 0
        while index < len(value):
            for token_type, regex in rules:
                match = regex.match(value, index)
                if match is not None:
                    tokens.append(Token(index, token_type, match.group(0)))
                    index = match.end()
                    break
            else:
                # Should be impossible
                raise Exception("Remaining: " + value[index:])

        tokens.append(Token(index, TokenType.END, ""))

        return tokens

    return tokenise
    

def _to_regex(value):
    if hasattr(value, "match"):
        return value
    else:
        return re.compile(value)


_string_prefix = r"'(?:\\.|[^'])*"
_identifier_character = r"(?:[a-zA-Z\-_]|\\.)"

tokenise = regex_tokeniser([
    (TokenType.IDENTIFIER, _identifier_character + "(?:" + _identifier_character + "|[0-9])*"),
    (TokenType.SYMBOL, r":|>|=>|\^=|=|\(|\)|\[|\]|\||!|\."),
    (TokenType.WHITESPACE, r"\s+"),
    (TokenType.STRING, _string_prefix + "'"),
    (TokenType.UNTERMINATED_STRING, _string_prefix),
    (TokenType.INTEGER, "([0-9]+)"),
])
