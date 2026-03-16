from .tokeniser import TokenType
from .document_matcher_parser import parse_document_matcher
from .html_path_parser import parse_html_path
from ...styles import Style


def parse_style_mapping(tokens):
    document_matcher = parse_document_matcher(tokens)
    tokens.skip(TokenType.WHITESPACE)
    tokens.skip(TokenType.SYMBOL, "=>")
    tokens.try_skip(TokenType.WHITESPACE)
    html_path = parse_html_path(tokens)
    tokens.skip(TokenType.END)
    
    return Style(document_matcher, html_path)
