import re

from markdown_it.rules_block import StateBlock


def is_code_block(state: StateBlock, line: int) -> bool:
    """Check if the line is part of a code block, compat for markdown-it-py v2."""
    try:
        # markdown-it-py v3+
        return state.is_code_block(line)
    except AttributeError:
        pass

    return (state.sCount[line] - state.blkIndent) >= 4


# Regex for subscript and superscript plugins
UNESCAPE_RE = re.compile(r"\\([ \\!\"#$%&'()*+,./:;<=>?@[\]^_`{|}~-])")
WHITESPACE_RE = re.compile(r"(^|[^\\])(\\\\)*\s")
