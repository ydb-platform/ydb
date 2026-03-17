import cobble

from ... import html_paths
from .tokeniser import TokenType
from .token_parser import parse_identifier, parse_string


@cobble.data
class _AttributeOrClassName(object):
    name = cobble.field()
    value = cobble.field()
    append = cobble.field()


def parse_html_path(tokens):
    if tokens.try_skip(TokenType.SYMBOL, "!"):
        return html_paths.ignore
    else:
        return html_paths.path(_parse_html_path_elements(tokens))


def _parse_html_path_elements(tokens):
    elements = []

    if tokens.peek_token_type() == TokenType.IDENTIFIER:
        elements.append(_parse_element(tokens))

        while tokens.try_skip_many(((TokenType.WHITESPACE, None), (TokenType.SYMBOL, ">"))):
            tokens.skip(TokenType.WHITESPACE)
            elements.append(_parse_element(tokens))

    return elements


def _parse_element(tokens):
    tag_names = _parse_tag_names(tokens)
    attributes_list = _parse_attribute_or_class_names(tokens)
    is_fresh = _parse_is_fresh(tokens)
    separator = _parse_separator(tokens)

    attributes = {}
    for attribute in attributes_list:
        if attribute.append and attributes.get(attribute.name):
            attributes[attribute.name] += " " + attribute.value
        else:
            attributes[attribute.name] = attribute.value

    return html_paths.element(
        tag_names,
        attributes=attributes,
        fresh=is_fresh,
        separator=separator,
    )


def _parse_tag_names(tokens):
    tag_names = [parse_identifier(tokens)]

    while tokens.try_skip(TokenType.SYMBOL, "|"):
        tag_names.append(parse_identifier(tokens))

    return tag_names


def _parse_attribute_or_class_names(tokens):
    attribute_or_class_names = []

    while True:
        attribute_or_class_name = _try_parse_attribute_or_class_name(tokens)
        if attribute_or_class_name is None:
            break
        else:
            attribute_or_class_names.append(attribute_or_class_name)

    return attribute_or_class_names


def _try_parse_attribute_or_class_name(tokens):
    if tokens.is_next(TokenType.SYMBOL, "["):
        return _parse_attribute(tokens)
    if tokens.is_next(TokenType.SYMBOL, "."):
        return _parse_class_name(tokens)
    else:
        return None


def _parse_attribute(tokens):
    tokens.skip(TokenType.SYMBOL, "[")
    name = parse_identifier(tokens)
    tokens.skip(TokenType.SYMBOL, "=")
    value = parse_string(tokens)
    tokens.skip(TokenType.SYMBOL, "]")
    return _AttributeOrClassName(name=name, value=value, append=False)


def _parse_class_name(tokens):
    tokens.skip(TokenType.SYMBOL, ".")
    class_name = parse_identifier(tokens)
    return _AttributeOrClassName(name="class", value=class_name, append=True)


def _parse_is_fresh(tokens):
    return tokens.try_skip_many((
        (TokenType.SYMBOL, ":"),
        (TokenType.IDENTIFIER, "fresh"),
    ))


def _parse_separator(tokens):
    is_separator = tokens.try_skip_many((
        (TokenType.SYMBOL, ":"),
        (TokenType.IDENTIFIER, "separator"),
    ))
    if is_separator:
        tokens.skip(TokenType.SYMBOL, "(")
        value = parse_string(tokens)
        tokens.skip(TokenType.SYMBOL, ")")
        return value
    else:
        return None
