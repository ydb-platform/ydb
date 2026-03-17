"""
This module contains text related things that make life easier.
"""

import shutil
import textwrap


def columns(default=79):
    """
    Return the number of text columns for the current console or TTY.

    On certain systems it may be necessary to set the ``COLUMNS`` environment
    variable if you want this method to work correctly.

    Uses `default` (79) if no value can be found.

    """
    width, _ = shutil.get_terminal_size()
    return width - 1


def wrap(text, width=None, indent=""):
    """
    Return `text` wrapped to `width` while trimming leading indentation and
    preserving paragraphs.

    This function is handy for turning indented inline strings into unindented
    strings that preserve paragraphs, whitespace, and any indentation beyond
    the baseline.

    The default wrap width is the number of columns available in the current
    console or TTY. If the columns can't be found, then the default wrap width
    is ``79``.

    :param text: Text to wrap
    :param width: Width to wrap text at (default: text column width)
    :param indent: String to indent text with (default: '')
    :type text: str
    :type width: int
    :type indent: str

    ::

        >>> import pytool
        >>> text = '''
                All this is indented by 8, but will be 0.
                        This is indented by 16, and a really long long long
                        line which is hard wrapped at a random character width,
                        but will be wrapped appropriately at 70 chars
                        afterwards.

                This is indented by 8 again.
            '''
        >>> print pytool.text.wrap(text)
        All this is indented by 8, but will be 0.
                This is indented by 16, and a really long long long line which
                is hard wrapped at a random character width, but will be
                wrapped appropriately at 70 chars afterwards.

        This is indented by 8 again.
        >>>

    """
    width = width or columns()
    # Preserve this for later, since we reuse `indent` as a variable
    initial_indent = indent
    # De-indent the text and remove any leading newlines
    text = textwrap.dedent(text).lstrip("\n")
    # Split the text into lines
    lines = text.split("\n")
    # Hey, if there's no lines, we do nothing, but this shouldn't ever happen
    if not lines:  # pragma: no cover
        return
    # Iterate over the lines looking for differences in indentation, which
    # indicates a paragraph break
    c = 0  # The current paragraph index
    line = lines[0]  # The current line we're parsing
    last_indent = len(line) - len(line.lstrip())  # Last line indent
    # List of paragraphs, primed with the first wrapped line fragment
    paragraphs = [line + " "]
    for i in range(1, len(lines)):
        # Strip trailing spaces, which may just be random whitespace
        line = lines[i].rstrip()
        # If the line is empty, it's a blank line, so we treat it specially
        if not line:
            paragraphs.append(line)
            c += 1
            # Set the last indentation to -1, so it is always a new
            # paragraph
            last_indent = -1
            continue
        # Get the current line's indentation
        indent = len(line) - len(line.lstrip())
        # If it doesn't match the last line, it's a new paragraph
        if indent != last_indent:
            # Start a new unwrapped line
            c += 1
            last_indent = indent
            paragraphs.append(line + " ")
            continue

        # If we got this far, the indentation matched, so it was part of the
        # same paragraph, and we just add it to the current paragraph
        paragraphs[c] += line.lstrip() + " "

    # Iterate over the paragraphs rewrapping them at 70 chrs
    for i in range(len(paragraphs)):
        # Get the paragraph as a single line
        line = paragraphs[i]
        # Calculate the indentation
        sub_indent = len(line) - len(line.lstrip())
        sub_indent = initial_indent + (sub_indent * " ")
        # Wrap the line into a paragraph
        wrapper = textwrap.TextWrapper(
            initial_indent=initial_indent, subsequent_indent=sub_indent, width=width
        )
        line = wrapper.fill(line)
        # Remove trailing whitespace
        paragraphs[i] = line.rstrip()

    # Join all the wrapped paragraphs into a single string
    text = "\n".join(paragraphs)
    return text
