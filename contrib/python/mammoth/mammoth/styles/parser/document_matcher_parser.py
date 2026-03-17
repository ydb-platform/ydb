from ... import documents, document_matchers
from .errors import LineParseError
from .tokeniser import TokenType
from .token_parser import try_parse_class_name, parse_string


def parse_document_matcher(tokens):
    if tokens.try_skip(TokenType.IDENTIFIER, "p"):
        style_id = try_parse_class_name(tokens)
        style_name = _parse_style_name(tokens)
        numbering = _parse_numbering(tokens)

        return document_matchers.paragraph(
            style_id=style_id,
            style_name=style_name,
            numbering=numbering,
        )

    elif tokens.try_skip(TokenType.IDENTIFIER, "r"):
        style_id = try_parse_class_name(tokens)
        style_name = _parse_style_name(tokens)

        return document_matchers.run(
            style_id=style_id,
            style_name=style_name,
        )

    elif tokens.try_skip(TokenType.IDENTIFIER, "table"):
        style_id = try_parse_class_name(tokens)
        style_name = _parse_style_name(tokens)

        return document_matchers.table(
            style_id=style_id,
            style_name=style_name,
        )

    elif tokens.try_skip(TokenType.IDENTIFIER, "b"):
        return document_matchers.bold

    elif tokens.try_skip(TokenType.IDENTIFIER, "i"):
        return document_matchers.italic

    elif tokens.try_skip(TokenType.IDENTIFIER, "u"):
        return document_matchers.underline

    elif tokens.try_skip(TokenType.IDENTIFIER, "strike"):
        return document_matchers.strikethrough

    elif tokens.try_skip(TokenType.IDENTIFIER, "all-caps"):
        return document_matchers.all_caps

    elif tokens.try_skip(TokenType.IDENTIFIER, "small-caps"):
        return document_matchers.small_caps

    elif tokens.try_skip(TokenType.IDENTIFIER, "highlight"):
        return _parse_highlight(tokens)

    elif tokens.try_skip(TokenType.IDENTIFIER, "comment-reference"):
        return document_matchers.comment_reference

    elif tokens.try_skip(TokenType.IDENTIFIER, "br"):
        return _parse_break(tokens)

    else:
        raise LineParseError("Unrecognised document element: {0}".format(tokens.next_value(TokenType.IDENTIFIER)))

def _parse_style_name(tokens):
    if tokens.try_skip(TokenType.SYMBOL, "["):
        tokens.skip(TokenType.IDENTIFIER, "style-name")
        string_matcher = _parse_string_matcher(tokens)
        tokens.skip(TokenType.SYMBOL, "]")
        return string_matcher
    else:
        return None


def _parse_string_matcher(tokens):
    if tokens.try_skip(TokenType.SYMBOL, "="):
        return document_matchers.equal_to(parse_string(tokens))
    elif tokens.try_skip(TokenType.SYMBOL, "^="):
        return document_matchers.starts_with(parse_string(tokens))
    else:
        raise LineParseError("Unrecognised string matcher: {0}".format(tokens.next_value()))

def _parse_numbering(tokens):
    if tokens.try_skip(TokenType.SYMBOL, ":"):
        is_ordered = _parse_list_type(tokens)
        tokens.skip(TokenType.SYMBOL, "(")
        level = int(tokens.next_value(TokenType.INTEGER)) - 1
        tokens.skip(TokenType.SYMBOL, ")")
        return documents.numbering_level(level, is_ordered=is_ordered)


def _parse_list_type(tokens):
    list_type = tokens.next_value(TokenType.IDENTIFIER)
    if list_type == "ordered-list":
        return True
    elif list_type == "unordered-list":
        return False
    else:
        raise LineParseError("Unrecognised list type: {0}".format(list_type))


def _parse_highlight(tokens):
    if tokens.try_skip(TokenType.SYMBOL, "["):
        tokens.skip(TokenType.IDENTIFIER, "color")
        tokens.skip(TokenType.SYMBOL, "=")
        color = parse_string(tokens)
        tokens.skip(TokenType.SYMBOL, "]");
    else:
        color = None

    return document_matchers.highlight(color=color)


def _parse_break(tokens):
    tokens.skip(TokenType.SYMBOL, "[")
    tokens.skip(TokenType.IDENTIFIER, "type")
    tokens.skip(TokenType.SYMBOL, "=")
    type_name = parse_string(tokens)
    tokens.skip(TokenType.SYMBOL, "]");

    if type_name == "line":
        return document_matchers.line_break
    elif type_name == "page":
        return document_matchers.page_break
    elif type_name == "column":
        return document_matchers.column_break
    else:
        raise LineParseError("Unrecognised break type: {0}".format(type_name))
