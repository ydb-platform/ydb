from bisect import bisect_left
from collections import deque
from html.entities import html5 as entities

from .constants import (
    EOF,
    Token,
    ascii_letters,
    ascii_upper_to_lower,
    digits,
    hexdigits,
    replacement_characters,
    space_characters,
    tag_token_types,
)
from .inputstream import HTMLInputStream

entity_keys = tuple(sorted(entities))


def has_keys_with_prefix(prefix):
    if prefix in entities:
        return True
    if (i := bisect_left(entity_keys, prefix)) == len(entities):
        return False
    return entity_keys[i].startswith(prefix)


def longest_prefix(prefix):
    if prefix in entities:
        return prefix
    for i in range(1, len(prefix) + 1):
        if prefix[:-i] in entities:
            return prefix[:-i]
    raise KeyError(prefix)


class HTMLTokenizer:
    """HTML tokenizer."""

    def __init__(self, stream, parser=None, **kwargs):
        self.stream = HTMLInputStream(stream, **kwargs)  # HTMLInputStream object
        self.parser = parser

        # Setup the initial tokenizer state
        self.state = self.data_state  # method to be invoked
        self.current_token = None  # token currently being processed

    def __iter__(self):
        """This is where the magic happens.

        We do our usually processing through the states and when we have a token
        to return we yield the token which pauses processing until the next token
        is requested.

        """
        self.token_queue = deque([])
        # Start processing. When EOF is reached self.state will return False
        # instead of True and the loop will terminate.
        while self.state():
            while self.stream.errors:
                yield {
                    "type": Token.PARSE_ERROR,
                    "data": self.stream.errors.pop(0),
                }
            while self.token_queue:
                yield self.token_queue.popleft()

    def parse_error(self, _data, **datavars):
        """Add a parse error to the token queue."""
        token = {"type": Token.PARSE_ERROR, "data": _data}
        if datavars:
            token["datavars"] = datavars
        self.token_queue.append(token)

    def characters(self, _data):
        """Add a characters string to the token queue."""
        self.token_queue.append({"type": Token.CHARACTERS, "data": _data})

    def consume_number_entity(self, is_hex):
        """Return either U+FFFD or the character based on the representation.

        It also discards ";" if present. If not present self.parse_error is
        invoked.

        """
        allowed = hexdigits if is_hex else digits
        radix = 16 if is_hex else 10
        stack = []

        # Consume all the characters that are in range while making sure we
        # don't hit an EOF.
        character = self.stream.character()
        while character in allowed:
            stack.append(character)
            character = self.stream.character()

        # Convert the set of characters consumed to an int.
        integer = int("".join(stack), radix)

        # Certain characters get replaced with others
        if integer in replacement_characters:
            replacement = replacement_characters[integer]
            self.parse_error("illegal-codepoint-for-numeric-entity", integer=integer)
        elif (0xD800 <= integer <= 0xDFFF) or (integer > 0x10FFFF):
            replacement = "\uFFFD"
            self.parse_error("illegal-codepoint-for-numeric-entity", integer=integer)
        else:
            # Should speed up this check somehow (e.g. move the set to a constant).
            if ((0x0001 <= integer <= 0x0008) or
                (0x000E <= integer <= 0x001F) or
                (0x007F <= integer <= 0x009F) or
                (0xFDD0 <= integer <= 0xFDEF) or
                integer in frozenset([
                    0x000B, 0xFFFE, 0xFFFF, 0x1FFFE, 0x1FFFF, 0x2FFFE, 0x2FFFF,
                    0x3FFFE, 0x3FFFF, 0x4FFFE, 0x4FFFF, 0x5FFFE, 0x5FFFF,
                    0x6FFFE, 0x6FFFF, 0x7FFFE, 0x7FFFF, 0x8FFFE, 0x8FFFF,
                    0x9FFFE, 0x9FFFF, 0xAFFFE, 0xAFFFF, 0xBFFFE, 0xBFFFF,
                    0xCFFFE, 0xCFFFF, 0xDFFFE, 0xDFFFF, 0xEFFFE, 0xEFFFF,
                    0xFFFFE, 0xFFFFF, 0x10FFFE, 0x10FFFF])):
                self.parse_error(
                    "illegal-codepoint-for-numeric-entity", integer=integer)
            replacement = chr(integer)

        # Discard the ; if present. Otherwise, put it back on the queue and
        # invoke parse_error on parser.
        if character != ";":
            self.parse_error("numeric-entity-without-semicolon")
            self.stream.unget(character)

        return replacement

    def consume_entity(self, allowed=None, from_attribute=False):
        # Initialise to the default output for when no entity is matched.
        output = "&"

        stack = [self.stream.character()]
        unget = (
            stack[0] in (EOF, "<", "&", *space_characters) or
            (allowed is not None and allowed == stack[0]))
        if unget:
            self.stream.unget(stack[0])

        elif stack[0] == "#":
            # Read the next character to see if it's hex or decimal.
            hex = False
            stack.append(self.stream.character())
            if stack[-1] in ("x", "X"):
                hex = True
                stack.append(self.stream.character())

            # stack[-1] should be the first digit.
            if stack[-1] in (hexdigits if hex else digits):
                # At least one digit found, so consume the whole number.
                self.stream.unget(stack[-1])
                output = self.consume_number_entity(hex)
            else:
                # No digits found.
                self.parse_error("expected-numeric-entity")
                self.stream.unget(stack.pop())
                output = f"&{''.join(stack)}"

        else:
            # At this point in the process might have named entity. Entities
            # are stored in the global variable "entities". Consume characters
            # and compare to these to a substring of the entity names in the
            # list until the substring no longer matches.
            while stack[-1] is not EOF:
                if not has_keys_with_prefix("".join(stack)):
                    break
                stack.append(self.stream.character())

            # At this point we have a string that starts with some characters
            # that may match an entity
            # Try to find the longest entity the string will match to take care
            # of &noti for instance.
            try:
                entity_name = longest_prefix("".join(stack[:-1]))
            except KeyError:
                self.parse_error("expected-named-entity")
                self.stream.unget(stack.pop())
                output = f"&{''.join(stack)}"
            else:
                if entity_name[-1] != ";":
                    self.parse_error("named-entity-without-semicolon")
                entity_length = len(entity_name)
                allowed_character = (
                    stack[entity_length] in ascii_letters or
                    stack[entity_length] in digits or
                    stack[entity_length] == "=")
                if entity_name[-1] != ";" and from_attribute and allowed_character:
                    self.stream.unget(stack.pop())
                    output = f"&{''.join(stack)}"
                else:
                    self.stream.unget(stack.pop())
                    output = f"{entities[entity_name]}{''.join(stack[entity_length:])}"

        if from_attribute:
            self.current_token["data"][-1][1] += output
        else:
            type = "SPACE_CHARACTERS" if output in space_characters else "CHARACTERS"
            self.token_queue.append({"type": Token[type], "data": output})

    def process_entity_in_attribute(self, allowed):
        """Replace the need for entity_in_attribute_value_state."""
        self.consume_entity(allowed=allowed, from_attribute=True)

    def emit_current_token(self):
        """This method is a generic handler for emitting the tags.

        It also sets the state to "data" because that's what's needed after a
        token has been emitted.

        """
        token = self.current_token
        # Add token to the queue to be yielded.
        if token["type"] in tag_token_types:
            token["name"] = token["name"].translate(ascii_upper_to_lower)
            if token["type"] == Token.START_TAG:
                raw = token["data"]
                data = dict(raw)
                if len(raw) > len(data):
                    # We had some duplicated attribute, fix so first wins.
                    data.update(raw[::-1])
                token["data"] = data

            if token["type"] == Token.END_TAG:
                if token["data"]:
                    self.parse_error("attributes-in-end-tag")
                if token["selfClosing"]:
                    self.parse_error("self-closing-flag-on-end-tag")
        self.token_queue.append(token)
        self.state = self.data_state

    # Below are the various tokenizer states worked out.
    def data_state(self):
        data = self.stream.character()
        if data == "&":
            self.state = self.entity_data_state
        elif data == "<":
            self.state = self.tag_open_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.characters("\u0000")
        elif data is EOF:
            return False
        elif data in space_characters:
            # Directly after emitting a token you switch back to the "data
            # state". At that point space characters are important so they are
            # emitted separately.
            self.token_queue.append({
                "type": Token.SPACE_CHARACTERS,
                "data": data + self.stream.chars_until(space_characters, True),
            })
            # No need to update lastFourChars here, since the first space will
            # have already been appended to lastFourChars and will have broken
            # any <!-- or --> sequences.
        else:
            characters = self.stream.chars_until(("&", "<", "\u0000"))
            self.characters(data + characters)
        return True

    def entity_data_state(self):
        self.consume_entity()
        self.state = self.data_state
        return True

    def rcdata_state(self):
        data = self.stream.character()
        if data == "&":
            self.state = self.character_reference_in_rc_data_state
        elif data == "<":
            self.state = self.rcdata_less_than_sign_state
        elif data is EOF:
            # Tokenization ends.
            return False
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.characters("\uFFFD")
        elif data in space_characters:
            # Directly after emitting a token you switch back to the "data
            # state". At that point space_characters are important so they are
            # emitted separately.
            self.token_queue.append({
                "type": Token.SPACE_CHARACTERS,
                "data": data + self.stream.chars_until(space_characters, True),
            })
            # No need to update lastFourChars here, since the first space will
            # have already been appended to lastFourChars and will have broken
            # any <!-- or --> sequences.
        else:
            chars = self.stream.chars_until(("&", "<", "\u0000"))
            self.characters(data + chars)
        return True

    def character_reference_in_rc_data_state(self):
        self.consume_entity()
        self.state = self.rcdata_state
        return True

    def rawtext_state(self):
        data = self.stream.character()
        if data == "<":
            self.state = self.rawtext_less_than_sign_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.characters("\uFFFD")
        elif data is EOF:
            return False
        else:
            characters = self.stream.chars_until(("<", "\u0000"))
            self.characters(data + characters)
        return True

    def script_data_state(self):
        data = self.stream.character()
        if data == "<":
            self.state = self.script_data_less_than_sign_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.characters("\uFFFD")
        elif data is EOF:
            return False
        else:
            characters = self.stream.chars_until(("<", "\u0000"))
            self.characters(data + characters)
        return True

    def plaintext_state(self):
        data = self.stream.character()
        if data is EOF:
            return False
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.characters("\uFFFD")
        else:
            self.characters(data + self.stream.chars_until("\u0000"))
        return True

    def tag_open_state(self):
        data = self.stream.character()
        if data == "!":
            self.state = self.markup_declaration_open_state
        elif data == "/":
            self.state = self.close_tag_open_state
        elif data in ascii_letters:
            self.current_token = {
                "type": Token.START_TAG,
                "name": data,
                "data": [],
                "selfClosing": False,
                "selfClosingAcknowledged": False,
            }
            self.state = self.tag_name_state
        elif data == ">":
            # XXX In theory it could be something besides a tag name. But
            # do we really care?
            self.parse_error("expected-tag-name-but-got-right-bracket")
            self.characters("<>")
            self.state = self.data_state
        elif data == "?":
            # XXX In theory it could be something besides a tag name. But
            # do we really care?
            self.parse_error("expected-tag-name-but-got-question-mark")
            self.stream.unget(data)
            self.state = self.bogus_comment_state
        else:
            # XXX
            self.parse_error("expected-tag-name")
            self.characters("<")
            self.stream.unget(data)
            self.state = self.data_state
        return True

    def close_tag_open_state(self):
        data = self.stream.character()
        if data in ascii_letters:
            self.current_token = {
                "type": Token.END_TAG,
                "name": data,
                "data": [],
                "selfClosing": False,
            }
            self.state = self.tag_name_state
        elif data == ">":
            self.parse_error("expected-closing-tag-but-got-right-bracket")
            self.state = self.data_state
        elif data is EOF:
            self.parse_error("expected-closing-tag-but-got-eof")
            self.characters("</")
            self.state = self.data_state
        else:
            # XXX data can be _'_...
            self.parse_error("expected-closing-tag-but-got-char", data=data)
            self.stream.unget(data)
            self.state = self.bogus_comment_state
        return True

    def tag_name_state(self):
        data = self.stream.character()
        if data in space_characters:
            self.state = self.before_attribute_name_state
        elif data == ">":
            self.emit_current_token()
        elif data is EOF:
            self.parse_error("eof-in-tag-name")
            self.state = self.data_state
        elif data == "/":
            self.state = self.self_closing_start_tag_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["name"] += "\uFFFD"
        else:
            self.current_token["name"] += data
            # (Don't use chars_until here, because tag names are
            # very short and it's faster to not do anything fancy.)
        return True

    def rcdata_less_than_sign_state(self):
        data = self.stream.character()
        if data == "/":
            self.temporary_buffer = ""
            self.state = self.rcdata_end_tag_open_state
        else:
            self.characters("<")
            self.stream.unget(data)
            self.state = self.rcdata_state
        return True

    def rcdata_end_tag_open_state(self):
        data = self.stream.character()
        if data in ascii_letters:
            self.temporary_buffer += data
            self.state = self.rcdata_end_tag_name_state
        else:
            self.characters("</")
            self.stream.unget(data)
            self.state = self.rcdata_state
        return True

    def rcdata_end_tag_name_state(self):
        appropriate = (
            self.current_token and
            self.current_token["name"].lower() == self.temporary_buffer.lower())
        data = self.stream.character()
        if data in space_characters and appropriate:
            self.current_token = {
                "type": Token.END_TAG,
                "name": self.temporary_buffer,
                "data": [],
                "selfClosing": False,
            }
            self.state = self.before_attribute_name_state
        elif data == "/" and appropriate:
            self.current_token = {
                "type": Token.END_TAG,
                "name": self.temporary_buffer,
                "data": [],
                "selfClosing": False,
            }
            self.state = self.self_closing_start_tag_state
        elif data == ">" and appropriate:
            self.current_token = {
                "type": Token.END_TAG,
                "name": self.temporary_buffer,
                "data": [],
                "selfClosing": False,
            }
            self.emit_current_token()
            self.state = self.data_state
        elif data in ascii_letters:
            self.temporary_buffer += data
        else:
            self.characters(f"</{self.temporary_buffer}")
            self.stream.unget(data)
            self.state = self.rcdata_state
        return True

    def rawtext_less_than_sign_state(self):
        data = self.stream.character()
        if data == "/":
            self.temporary_buffer = ""
            self.state = self.rawtext_end_tag_open_state
        else:
            self.characters("<")
            self.stream.unget(data)
            self.state = self.rawtext_state
        return True

    def rawtext_end_tag_open_state(self):
        data = self.stream.character()
        if data in ascii_letters:
            self.temporary_buffer += data
            self.state = self.rawtext_end_tag_name_state
        else:
            self.characters("</")
            self.stream.unget(data)
            self.state = self.rawtext_state
        return True

    def rawtext_end_tag_name_state(self):
        appropriate = (
            self.current_token and
            self.current_token["name"].lower() == self.temporary_buffer.lower())
        data = self.stream.character()
        if data in space_characters and appropriate:
            self.current_token = {
                "type": Token.END_TAG,
                "name": self.temporary_buffer,
                "data": [],
                "selfClosing": False,
            }
            self.state = self.before_attribute_name_state
        elif data == "/" and appropriate:
            self.current_token = {
                "type": Token.END_TAG,
                "name": self.temporary_buffer,
                "data": [],
                "selfClosing": False,
            }
            self.state = self.self_closing_start_tag_state
        elif data == ">" and appropriate:
            self.current_token = {
                "type": Token.END_TAG,
                "name": self.temporary_buffer,
                "data": [],
                "selfClosing": False,
            }
            self.emit_current_token()
            self.state = self.data_state
        elif data in ascii_letters:
            self.temporary_buffer += data
        else:
            self.characters(f"</{self.temporary_buffer}")
            self.stream.unget(data)
            self.state = self.rawtext_state
        return True

    def script_data_less_than_sign_state(self):
        data = self.stream.character()
        if data == "/":
            self.temporary_buffer = ""
            self.state = self.script_data_end_tag_open_state
        elif data == "!":
            self.characters("<!")
            self.state = self.script_data_escape_start_state
        else:
            self.characters("<")
            self.stream.unget(data)
            self.state = self.script_data_state
        return True

    def script_data_end_tag_open_state(self):
        data = self.stream.character()
        if data in ascii_letters:
            self.temporary_buffer += data
            self.state = self.script_data_end_tag_name_state
        else:
            self.characters("</")
            self.stream.unget(data)
            self.state = self.script_data_state
        return True

    def script_data_end_tag_name_state(self):
        appropriate = (
            self.current_token and
            self.current_token["name"].lower() == self.temporary_buffer.lower())
        data = self.stream.character()
        if data in space_characters and appropriate:
            self.current_token = {
                "type": Token.END_TAG,
                "name": self.temporary_buffer,
                "data": [],
                "selfClosing": False,
            }
            self.state = self.before_attribute_name_state
        elif data == "/" and appropriate:
            self.current_token = {
                "type": Token.END_TAG,
                "name": self.temporary_buffer,
                "data": [],
                "selfClosing": False,
            }
            self.state = self.self_closing_start_tag_state
        elif data == ">" and appropriate:
            self.current_token = {
                "type": Token.END_TAG,
                "name": self.temporary_buffer,
                "data": [],
                "selfClosing": False,
            }
            self.emit_current_token()
            self.state = self.data_state
        elif data in ascii_letters:
            self.temporary_buffer += data
        else:
            self.characters(f"</{self.temporary_buffer}")
            self.stream.unget(data)
            self.state = self.script_data_state
        return True

    def script_data_escape_start_state(self):
        data = self.stream.character()
        if data == "-":
            self.characters("-")
            self.state = self.script_data_escape_start_dash_state
        else:
            self.stream.unget(data)
            self.state = self.script_data_state
        return True

    def script_data_escape_start_dash_state(self):
        data = self.stream.character()
        if data == "-":
            self.characters("-")
            self.state = self.script_data_escaped_dash_dash_state
        else:
            self.stream.unget(data)
            self.state = self.script_data_state
        return True

    def script_data_escaped_state(self):
        data = self.stream.character()
        if data == "-":
            self.characters("-")
            self.state = self.script_data_escaped_dash_state
        elif data == "<":
            self.state = self.script_data_escaped_less_than_sign_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.characters("\uFFFD")
        elif data is EOF:
            self.state = self.data_state
        else:
            self.characters(data + self.stream.chars_until(("<", "-", "\u0000")))
        return True

    def script_data_escaped_dash_state(self):
        data = self.stream.character()
        if data == "-":
            self.characters("-")
            self.state = self.script_data_escaped_dash_dash_state
        elif data == "<":
            self.state = self.script_data_escaped_less_than_sign_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.characters("\uFFFD")
            self.state = self.script_data_escaped_state
        elif data is EOF:
            self.state = self.data_state
        else:
            self.characters(data)
            self.state = self.script_data_escaped_state
        return True

    def script_data_escaped_dash_dash_state(self):
        data = self.stream.character()
        if data == "-":
            self.characters("-")
        elif data == "<":
            self.state = self.script_data_escaped_less_than_sign_state
        elif data == ">":
            self.characters(">")
            self.state = self.script_data_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.characters("\uFFFD")
            self.state = self.script_data_escaped_state
        elif data is EOF:
            self.state = self.data_state
        else:
            self.characters(data)
            self.state = self.script_data_escaped_state
        return True

    def script_data_escaped_less_than_sign_state(self):
        data = self.stream.character()
        if data == "/":
            self.temporary_buffer = ""
            self.state = self.script_data_escaped_end_tag_open_state
        elif data in ascii_letters:
            self.characters(f"<{data}")
            self.temporary_buffer = data
            self.state = self.script_data_double_escape_start_state
        else:
            self.characters("<")
            self.stream.unget(data)
            self.state = self.script_data_escaped_state
        return True

    def script_data_escaped_end_tag_open_state(self):
        data = self.stream.character()
        if data in ascii_letters:
            self.temporary_buffer = data
            self.state = self.script_data_escaped_end_tag_name_state
        else:
            self.characters("</")
            self.stream.unget(data)
            self.state = self.script_data_escaped_state
        return True

    def script_data_escaped_end_tag_name_state(self):
        appropriate = (
            self.current_token and
            self.current_token["name"].lower() == self.temporary_buffer.lower())
        data = self.stream.character()
        if data in space_characters and appropriate:
            self.current_token = {
                "type": Token.END_TAG,
                "name": self.temporary_buffer,
                "data": [],
                "selfClosing": False,
            }
            self.state = self.before_attribute_name_state
        elif data == "/" and appropriate:
            self.current_token = {
                "type": Token.END_TAG,
                "name": self.temporary_buffer,
                "data": [],
                "selfClosing": False,
            }
            self.state = self.self_closing_start_tag_state
        elif data == ">" and appropriate:
            self.current_token = {
                "type": Token.END_TAG,
                "name": self.temporary_buffer,
                "data": [],
                "selfClosing": False,
            }
            self.emit_current_token()
            self.state = self.data_state
        elif data in ascii_letters:
            self.temporary_buffer += data
        else:
            self.characters(f"</{self.temporary_buffer}")
            self.stream.unget(data)
            self.state = self.script_data_escaped_state
        return True

    def script_data_double_escape_start_state(self):
        data = self.stream.character()
        if data in (space_characters | frozenset(("/", ">"))):
            self.characters(data)
            if self.temporary_buffer.lower() == "script":
                self.state = self.script_data_double_escaped_state
            else:
                self.state = self.script_data_escaped_state
        elif data in ascii_letters:
            self.characters(data)
            self.temporary_buffer += data
        else:
            self.stream.unget(data)
            self.state = self.script_data_escaped_state
        return True

    def script_data_double_escaped_state(self):
        data = self.stream.character()
        if data == "-":
            self.characters("-")
            self.state = self.script_data_double_escaped_dash_state
        elif data == "<":
            self.characters("<")
            self.state = self.script_data_double_escaped_less_than_sign_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.characters("\uFFFD")
        elif data is EOF:
            self.parse_error("eof-in-script-in-script")
            self.state = self.data_state
        else:
            self.characters(data)
        return True

    def script_data_double_escaped_dash_state(self):
        data = self.stream.character()
        if data == "-":
            self.characters("-")
            self.state = self.script_data_double_escaped_dash_dash_state
        elif data == "<":
            self.characters("<")
            self.state = self.script_data_double_escaped_less_than_sign_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.characters("\uFFFD")
            self.state = self.script_data_double_escaped_state
        elif data is EOF:
            self.parse_error("eof-in-script-in-script")
            self.state = self.data_state
        else:
            self.characters(data)
            self.state = self.script_data_double_escaped_state
        return True

    def script_data_double_escaped_dash_dash_state(self):
        data = self.stream.character()
        if data == "-":
            self.characters("-")
        elif data == "<":
            self.characters("<")
            self.state = self.script_data_double_escaped_less_than_sign_state
        elif data == ">":
            self.characters(">")
            self.state = self.script_data_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.characters("\uFFFD")
            self.state = self.script_data_double_escaped_state
        elif data is EOF:
            self.parse_error("eof-in-script-in-script")
            self.state = self.data_state
        else:
            self.characters(data)
            self.state = self.script_data_double_escaped_state
        return True

    def script_data_double_escaped_less_than_sign_state(self):
        data = self.stream.character()
        if data == "/":
            self.characters("/")
            self.temporary_buffer = ""
            self.state = self.script_data_double_escape_end_state
        else:
            self.stream.unget(data)
            self.state = self.script_data_double_escaped_state
        return True

    def script_data_double_escape_end_state(self):
        data = self.stream.character()
        if data in (space_characters | frozenset(("/", ">"))):
            self.characters(data)
            if self.temporary_buffer.lower() == "script":
                self.state = self.script_data_escaped_state
            else:
                self.state = self.script_data_double_escaped_state
        elif data in ascii_letters:
            self.characters(data)
            self.temporary_buffer += data
        else:
            self.stream.unget(data)
            self.state = self.script_data_double_escaped_state
        return True

    def before_attribute_name_state(self):
        data = self.stream.character()
        if data in space_characters:
            self.stream.chars_until(space_characters, True)
        elif data in ascii_letters:
            self.current_token["data"].append([data, ""])
            self.state = self.attribute_name_state
        elif data == ">":
            self.emit_current_token()
        elif data == "/":
            self.state = self.self_closing_start_tag_state
        elif data in ("'", '"', "=", "<"):
            self.parse_error("invalid-character-in-attribute-name")
            self.current_token["data"].append([data, ""])
            self.state = self.attribute_name_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["data"].append(["\uFFFD", ""])
            self.state = self.attribute_name_state
        elif data is EOF:
            self.parse_error("expected-attribute-name-but-got-eof")
            self.state = self.data_state
        else:
            self.current_token["data"].append([data, ""])
            self.state = self.attribute_name_state
        return True

    def attribute_name_state(self):
        data = self.stream.character()
        leaving_this_state = True
        emit_token = False
        if data == "=":
            self.state = self.before_attribute_value_state
        elif data in ascii_letters:
            self.current_token["data"][-1][0] += (
                data + self.stream.chars_until(ascii_letters, True))
            leaving_this_state = False
        elif data == ">":
            # XXX If we emit here the attributes are converted to a dict
            # without being checked and when the code below runs we error
            # because data is a dict not a list.
            emit_token = True
        elif data in space_characters:
            self.state = self.after_attribute_name_state
        elif data == "/":
            self.state = self.self_closing_start_tag_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["data"][-1][0] += "\uFFFD"
            leaving_this_state = False
        elif data in ("'", '"', "<"):
            self.parse_error("invalid-character-in-attribute-name")
            self.current_token["data"][-1][0] += data
            leaving_this_state = False
        elif data is EOF:
            self.parse_error("eof-in-attribute-name")
            self.state = self.data_state
        else:
            self.current_token["data"][-1][0] += data
            leaving_this_state = False

        if leaving_this_state:
            # Attributes are not dropped at this stage. That happens when the
            # start tag token is emitted so values can still be safely appended
            # to attributes, but we do want to report the parse error in time.
            self.current_token["data"][-1][0] = (
                self.current_token["data"][-1][0].translate(ascii_upper_to_lower))
            for name, _ in self.current_token["data"][:-1]:
                if self.current_token["data"][-1][0] == name:
                    self.parse_error("duplicate-attribute")
                    break
            # XXX Fix for above.
            if emit_token:
                self.emit_current_token()
        return True

    def after_attribute_name_state(self):
        data = self.stream.character()
        if data in space_characters:
            self.stream.chars_until(space_characters, True)
        elif data == "=":
            self.state = self.before_attribute_value_state
        elif data == ">":
            self.emit_current_token()
        elif data in ascii_letters:
            self.current_token["data"].append([data, ""])
            self.state = self.attribute_name_state
        elif data == "/":
            self.state = self.self_closing_start_tag_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["data"].append(["\uFFFD", ""])
            self.state = self.attribute_name_state
        elif data in ("'", '"', "<"):
            self.parse_error("invalid-character-after-attribute-name")
            self.current_token["data"].append([data, ""])
            self.state = self.attribute_name_state
        elif data is EOF:
            self.parse_error("expected-end-of-tag-but-got-eof")
            self.state = self.data_state
        else:
            self.current_token["data"].append([data, ""])
            self.state = self.attribute_name_state
        return True

    def before_attribute_value_state(self):
        data = self.stream.character()
        if data in space_characters:
            self.stream.chars_until(space_characters, True)
        elif data == "\"":
            self.state = self.attribute_value_double_quoted_state
        elif data == "&":
            self.state = self.attribute_value_unquoted_state
            self.stream.unget(data)
        elif data == "'":
            self.state = self.attribute_value_single_quoted_state
        elif data == ">":
            self.parse_error("expected-attribute-value-but-got-right-bracket")
            self.emit_current_token()
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["data"][-1][1] += "\uFFFD"
            self.state = self.attribute_value_unquoted_state
        elif data in ("=", "<", "`"):
            self.parse_error("equals-in-unquoted-attribute-value")
            self.current_token["data"][-1][1] += data
            self.state = self.attribute_value_unquoted_state
        elif data is EOF:
            self.parse_error("expected-attribute-value-but-got-eof")
            self.state = self.data_state
        else:
            self.current_token["data"][-1][1] += data
            self.state = self.attribute_value_unquoted_state
        return True

    def attribute_value_double_quoted_state(self):
        data = self.stream.character()
        if data == "\"":
            self.state = self.after_attribute_value_state
        elif data == "&":
            self.process_entity_in_attribute('"')
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["data"][-1][1] += "\uFFFD"
        elif data is EOF:
            self.parse_error("eof-in-attribute-value-double-quote")
            self.state = self.data_state
        else:
            self.current_token["data"][-1][1] += (
                data + self.stream.chars_until(("\"", "&", "\u0000")))
        return True

    def attribute_value_single_quoted_state(self):
        data = self.stream.character()
        if data == "'":
            self.state = self.after_attribute_value_state
        elif data == "&":
            self.process_entity_in_attribute("'")
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["data"][-1][1] += "\uFFFD"
        elif data is EOF:
            self.parse_error("eof-in-attribute-value-single-quote")
            self.state = self.data_state
        else:
            self.current_token["data"][-1][1] += (
                data + self.stream.chars_until(("'", "&", "\u0000")))
        return True

    def attribute_value_unquoted_state(self):
        data = self.stream.character()
        if data in space_characters:
            self.state = self.before_attribute_name_state
        elif data == "&":
            self.process_entity_in_attribute(">")
        elif data == ">":
            self.emit_current_token()
        elif data in ('"', "'", "=", "<", "`"):
            self.parse_error("unexpected-character-in-unquoted-attribute-value")
            self.current_token["data"][-1][1] += data
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["data"][-1][1] += "\uFFFD"
        elif data is EOF:
            self.parse_error("eof-in-attribute-value-no-quotes")
            self.state = self.data_state
        else:
            self.current_token["data"][-1][1] += (
                data + self.stream.chars_until(
                    frozenset(("&", ">", '"', "'", "=", "<", "`", "\u0000")) |
                    space_characters))
        return True

    def after_attribute_value_state(self):
        data = self.stream.character()
        if data in space_characters:
            self.state = self.before_attribute_name_state
        elif data == ">":
            self.emit_current_token()
        elif data == "/":
            self.state = self.self_closing_start_tag_state
        elif data is EOF:
            self.parse_error("unexpected-eof-after-attribute-value")
            self.stream.unget(data)
            self.state = self.data_state
        else:
            self.parse_error("unexpected-character-after-attribute-value")
            self.stream.unget(data)
            self.state = self.before_attribute_name_state
        return True

    def self_closing_start_tag_state(self):
        data = self.stream.character()
        if data == ">":
            self.current_token["selfClosing"] = True
            self.emit_current_token()
        elif data is EOF:
            self.parse_error("unexpected-eof-after-solidus-in-tag")
            self.stream.unget(data)
            self.state = self.data_state
        else:
            self.parse_error("unexpected-character-after-solidus-in-tag")
            self.stream.unget(data)
            self.state = self.before_attribute_name_state
        return True

    def bogus_comment_state(self):
        # Make a new comment token and give it as value all the characters
        # until the first > or EOF (chars_until checks for EOF automatically)
        # and emit it.
        data = self.stream.chars_until(">")
        data = data.replace("\u0000", "\uFFFD")
        self.token_queue.append({"type": Token.COMMENT, "data": data})

        # Eat the character directly after the bogus comment which is either a
        # ">" or an EOF.
        self.stream.character()
        self.state = self.data_state
        return True

    def markup_declaration_open_state(self):
        stack = [self.stream.character()]
        if stack[-1] == "-":
            stack.append(self.stream.character())
            if stack[-1] == "-":
                self.current_token = {"type": Token.COMMENT, "data": ""}
                self.state = self.comment_start_state
                return True
        elif stack[-1] and stack[-1] in 'dD':
            matched = True
            for expected in ('oO', 'cC', 'tT', 'yY', 'pP', 'eE'):
                stack.append(self.stream.character())
                if not stack[-1] or stack[-1] not in expected:
                    matched = False
                    break
            if matched:
                self.current_token = {
                    "type": Token.DOCTYPE,
                    "name": "",
                    "publicId": None,
                    "systemId": None,
                    "correct": True,
                }
                self.state = self.doctype_state
                return True
        elif (stack[-1] == "[" and
              self.parser is not None and
              self.parser.tree.open_elements and
              self.parser.tree.open_elements[-1].namespace !=
              self.parser.tree.default_namespace):
            matched = True
            for expected in "CDATA[":
                stack.append(self.stream.character())
                if stack[-1] != expected:
                    matched = False
                    break
            if matched:
                self.state = self.cdata_section_state
                return True

        self.parse_error("expected-dashes-or-doctype")

        while stack:
            self.stream.unget(stack.pop())
        self.state = self.bogus_comment_state
        return True

    def comment_start_state(self):
        data = self.stream.character()
        if data == "-":
            self.state = self.comment_start_dash_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["data"] += "\uFFFD"
        elif data == ">":
            self.parse_error("incorrect-comment")
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data is EOF:
            self.parse_error("eof-in-comment")
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.current_token["data"] += data
            self.state = self.comment_state
        return True

    def comment_start_dash_state(self):
        data = self.stream.character()
        if data == "-":
            self.state = self.comment_end_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["data"] += "-\uFFFD"
        elif data == ">":
            self.parse_error("incorrect-comment")
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data is EOF:
            self.parse_error("eof-in-comment")
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.current_token["data"] += f"-{data}"
            self.state = self.comment_state
        return True

    def comment_state(self):
        data = self.stream.character()
        if data == "-":
            self.state = self.comment_end_dash_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["data"] += "\uFFFD"
        elif data is EOF:
            self.parse_error("eof-in-comment")
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.current_token["data"] += (
                data + self.stream.chars_until(("-", "\u0000")))
        return True

    def comment_end_dash_state(self):
        data = self.stream.character()
        if data == "-":
            self.state = self.comment_end_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["data"] += "-\uFFFD"
            self.state = self.comment_state
        elif data is EOF:
            self.parse_error("eof-in-comment-end-dash")
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.current_token["data"] += f"-{data}"
            self.state = self.comment_state
        return True

    def comment_end_state(self):
        data = self.stream.character()
        if data == ">":
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["data"] += "--\uFFFD"
            self.state = self.comment_state
        elif data == "!":
            self.parse_error("unexpected-bang-after-double-dash-in-comment")
            self.state = self.comment_end_bang_state
        elif data == "-":
            self.parse_error("unexpected-dash-after-double-dash-in-comment")
            self.current_token["data"] += data
        elif data is EOF:
            self.parse_error("eof-in-comment-double-dash")
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            # XXX
            self.parse_error("unexpected-char-in-comment")
            self.current_token["data"] += f"--{data}"
            self.state = self.comment_state
        return True

    def comment_end_bang_state(self):
        data = self.stream.character()
        if data == ">":
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data == "-":
            self.current_token["data"] += "--!"
            self.state = self.comment_end_dash_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["data"] += "--!\uFFFD"
            self.state = self.comment_state
        elif data is EOF:
            self.parse_error("eof-in-comment-end-bang-state")
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.current_token["data"] += f"--!{data}"
            self.state = self.comment_state
        return True

    def doctype_state(self):
        data = self.stream.character()
        if data in space_characters:
            self.state = self.before_doctype_name_state
        elif data is EOF:
            self.parse_error("expected-doctype-name-but-got-eof")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.parse_error("need-space-after-doctype")
            self.stream.unget(data)
            self.state = self.before_doctype_name_state
        return True

    def before_doctype_name_state(self):
        data = self.stream.character()
        if data in space_characters:
            pass
        elif data == ">":
            self.parse_error("expected-doctype-name-but-got-right-bracket")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["name"] = "\uFFFD"
            self.state = self.doctype_name_state
        elif data is EOF:
            self.parse_error("expected-doctype-name-but-got-eof")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.current_token["name"] = data
            self.state = self.doctype_name_state
        return True

    def doctype_name_state(self):
        data = self.stream.character()
        if data in space_characters:
            self.current_token["name"] = (
                self.current_token["name"].translate(ascii_upper_to_lower))
            self.state = self.after_doctype_name_state
        elif data == ">":
            self.current_token["name"] = (
                self.current_token["name"].translate(ascii_upper_to_lower))
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["name"] += "\uFFFD"
            self.state = self.doctype_name_state
        elif data is EOF:
            self.parse_error("eof-in-doctype-name")
            self.current_token["correct"] = False
            self.current_token["name"] = (
                self.current_token["name"].translate(ascii_upper_to_lower))
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.current_token["name"] += data
        return True

    def after_doctype_name_state(self):
        data = self.stream.character()
        if data in space_characters:
            pass
        elif data == ">":
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data is EOF:
            self.current_token["correct"] = False
            self.stream.unget(data)
            self.parse_error("eof-in-doctype")
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            if data and data in "pP":
                matched = True
                for expected in ("uU", "bB", "lL", "iI", "cC"):
                    data = self.stream.character()
                    if not data or data not in expected:
                        matched = False
                        break
                if matched:
                    self.state = self.after_doctype_public_keyword_state
                    return True
            elif data and data in "sS":
                matched = True
                for expected in ("yY", "sS", "tT", "eE", "mM"):
                    data = self.stream.character()
                    if not data or data not in expected:
                        matched = False
                        break
                if matched:
                    self.state = self.after_doctype_system_keyword_state
                    return True

            # All the characters read before the current 'data' will be
            # [a-zA-Z], so they're garbage in the bogus doctype and can be
            # discarded; only the latest character might be '>' or EOF and
            # needs to be ungetted.
            self.stream.unget(data)
            self.parse_error("expected-space-or-right-bracket-in-doctype", data=data)
            self.current_token["correct"] = False
            self.state = self.bogus_doctype_state
        return True

    def after_doctype_public_keyword_state(self):
        data = self.stream.character()
        if data in space_characters:
            self.state = self.before_doctype_public_identifier_state
        elif data in ("'", '"'):
            self.parse_error("unexpected-char-in-doctype")
            self.stream.unget(data)
            self.state = self.before_doctype_public_identifier_state
        elif data is EOF:
            self.parse_error("eof-in-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.stream.unget(data)
            self.state = self.before_doctype_public_identifier_state
        return True

    def before_doctype_public_identifier_state(self):
        data = self.stream.character()
        if data in space_characters:
            pass
        elif data == "\"":
            self.current_token["publicId"] = ""
            self.state = self.doctype_public_identifier_double_quoted_state
        elif data == "'":
            self.current_token["publicId"] = ""
            self.state = self.doctype_public_identifier_single_quoted_state
        elif data == ">":
            self.parse_error("unexpected-end-of-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data is EOF:
            self.parse_error("eof-in-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.parse_error("unexpected-char-in-doctype")
            self.current_token["correct"] = False
            self.state = self.bogus_doctype_state
        return True

    def doctype_public_identifier_double_quoted_state(self):
        data = self.stream.character()
        if data == '"':
            self.state = self.after_doctype_public_identifier_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["publicId"] += "\uFFFD"
        elif data == ">":
            self.parse_error("unexpected-end-of-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data is EOF:
            self.parse_error("eof-in-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.current_token["publicId"] += data
        return True

    def doctype_public_identifier_single_quoted_state(self):
        data = self.stream.character()
        if data == "'":
            self.state = self.after_doctype_public_identifier_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["publicId"] += "\uFFFD"
        elif data == ">":
            self.parse_error("unexpected-end-of-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data is EOF:
            self.parse_error("eof-in-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.current_token["publicId"] += data
        return True

    def after_doctype_public_identifier_state(self):
        data = self.stream.character()
        if data in space_characters:
            self.state = self.between_doctype_public_and_system_identifiers_state
        elif data == ">":
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data == '"':
            self.parse_error("unexpected-char-in-doctype")
            self.current_token["systemId"] = ""
            self.state = self.doctype_system_identifier_double_quoted_state
        elif data == "'":
            self.parse_error("unexpected-char-in-doctype")
            self.current_token["systemId"] = ""
            self.state = self.doctype_system_identifier_single_quoted_state
        elif data is EOF:
            self.parse_error("eof-in-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.parse_error("unexpected-char-in-doctype")
            self.current_token["correct"] = False
            self.state = self.bogus_doctype_state
        return True

    def between_doctype_public_and_system_identifiers_state(self):
        data = self.stream.character()
        if data in space_characters:
            pass
        elif data == ">":
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data == '"':
            self.current_token["systemId"] = ""
            self.state = self.doctype_system_identifier_double_quoted_state
        elif data == "'":
            self.current_token["systemId"] = ""
            self.state = self.doctype_system_identifier_single_quoted_state
        elif data is EOF:
            self.parse_error("eof-in-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.parse_error("unexpected-char-in-doctype")
            self.current_token["correct"] = False
            self.state = self.bogus_doctype_state
        return True

    def after_doctype_system_keyword_state(self):
        data = self.stream.character()
        if data in space_characters:
            self.state = self.before_doctype_system_identifier_state
        elif data in ("'", '"'):
            self.parse_error("unexpected-char-in-doctype")
            self.stream.unget(data)
            self.state = self.before_doctype_system_identifier_state
        elif data is EOF:
            self.parse_error("eof-in-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.stream.unget(data)
            self.state = self.before_doctype_system_identifier_state
        return True

    def before_doctype_system_identifier_state(self):
        data = self.stream.character()
        if data in space_characters:
            pass
        elif data == '"':
            self.current_token["systemId"] = ""
            self.state = self.doctype_system_identifier_double_quoted_state
        elif data == "'":
            self.current_token["systemId"] = ""
            self.state = self.doctype_system_identifier_single_quoted_state
        elif data == ">":
            self.parse_error("unexpected-char-in-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data is EOF:
            self.parse_error("eof-in-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.parse_error("unexpected-char-in-doctype")
            self.current_token["correct"] = False
            self.state = self.bogus_doctype_state
        return True

    def doctype_system_identifier_double_quoted_state(self):
        data = self.stream.character()
        if data == "\"":
            self.state = self.after_doctype_system_identifier_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["systemId"] += "\uFFFD"
        elif data == ">":
            self.parse_error("unexpected-end-of-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data is EOF:
            self.parse_error("eof-in-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.current_token["systemId"] += data
        return True

    def doctype_system_identifier_single_quoted_state(self):
        data = self.stream.character()
        if data == "'":
            self.state = self.after_doctype_system_identifier_state
        elif data == "\u0000":
            self.parse_error("invalid-codepoint")
            self.current_token["systemId"] += "\uFFFD"
        elif data == ">":
            self.parse_error("unexpected-end-of-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data is EOF:
            self.parse_error("eof-in-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.current_token["systemId"] += data
        return True

    def after_doctype_system_identifier_state(self):
        data = self.stream.character()
        if data in space_characters:
            pass
        elif data == ">":
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data is EOF:
            self.parse_error("eof-in-doctype")
            self.current_token["correct"] = False
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            self.parse_error("unexpected-char-in-doctype")
            self.state = self.bogus_doctype_state
        return True

    def bogus_doctype_state(self):
        data = self.stream.character()
        if data == ">":
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        elif data is EOF:
            # XXX EMIT
            self.stream.unget(data)
            self.token_queue.append(self.current_token)
            self.state = self.data_state
        else:
            pass
        return True

    def cdata_section_state(self):
        data = []
        while True:
            data.append(self.stream.chars_until("]"))
            data.append(self.stream.chars_until(">"))
            char = self.stream.character()
            if char is EOF:
                break
            else:
                assert char == ">"
                if data[-1][-2:] == "]]":
                    data[-1] = data[-1][:-2]
                    break
                else:
                    data.append(char)

        data = "".join(data)
        # Deal with null here rather than in the parser.
        if (null_count := data.count("\u0000")) > 0:
            for _ in range(null_count):
                self.parse_error("invalid-codepoint")
            data = data.replace("\u0000", "\uFFFD")
        if data:
            self.characters(data)
        self.state = self.data_state
        return True
