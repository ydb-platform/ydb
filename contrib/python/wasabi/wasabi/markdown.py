from typing import Iterable, List, Optional, Sequence

from .compat import Literal


class MarkdownRenderer:
    """Simple helper for generating raw Markdown."""

    def __init__(self, no_emoji: bool = False):
        """Initialize the renderer.

        no_emoji (bool): Don't show emoji in titles etc.
        """
        self.data: List = []
        self.no_emoji = no_emoji

    @property
    def text(self) -> str:
        """RETURNS (str): The Markdown document."""
        return "\n\n".join(self.data)

    def add(self, content: str):
        """Add a string to the Markdown document.

        content (str): Add content to the document.
        """
        self.data.append(content)

    def table(
        self,
        data: Iterable[Iterable[str]],
        header: Sequence[str],
        aligns: Optional[Sequence[Literal["r", "c", "l"]]] = None,
    ) -> str:
        """Create a Markdown table.

        data (Iterable[Iterable[str]]): The body, one iterable per row,
            containig an interable of column contents.
        header (Sequence[str]): The column names.
        aligns (Optional[Sequence[Literal["r", "c", "l"]]]): Optional alignment-mode for each column. Values should
            either be 'l' (left), 'r' (right), or 'c' (center). Optional.
        RETURNS (str): The rendered table.
        """
        if aligns is None:
            aligns = ["l"] * len(header)
        if len(aligns) != len(header):
            err = "Invalid aligns: {} (header length: {})".format(aligns, len(header))
            raise ValueError(err)
        get_divider = lambda a: ":---:" if a == "c" else "---:" if a == "r" else "---"
        head = "| {} |".format(" | ".join(header))
        divider = "| {} |".format(
            " | ".join(get_divider(aligns[i]) for i in range(len(header)))
        )
        body = "\n".join("| {} |".format(" | ".join(row)) for row in data)
        return "{}\n{}\n{}".format(head, divider, body)

    def title(self, level: int, text: str, emoji: Optional[str] = None) -> str:
        """Create a Markdown heading.

        level (int): The heading level, e.g. 3 for ###
        text (str): The heading text.
        emoji (Optional[str]): Optional emoji to show before heading text, if enabled.
        RETURNS (str): The rendered title.
        """
        prefix = "{} ".format(emoji) if emoji and not self.no_emoji else ""
        return "{} {}{}".format("#" * level, prefix, text)

    def list(self, items: Iterable[str], numbered: bool = False) -> str:
        """Create a non-nested list.

        items (Iterable[str]): The list items.
        numbered (bool): Whether to use a numbered list.
        RETURNS (str): The rendered list.
        """
        content = []
        for i, item in enumerate(items):
            if numbered:
                content.append("{}. {}".format(i + 1, item))
            else:
                content.append("- {}".format(item))
        return "\n".join(content)

    def link(self, text: str, url: str) -> str:
        """Create a Markdown link.

        text (str): The link text.
        url (str): The link URL.
        RETURNS (str): The rendered link.
        """
        return "[{}]({})".format(text, url)

    def code_block(self, text: str, lang: str = "") -> str:
        """Create a Markdown code block.

        text (str): The code text.
        lang (str): Optional code language.
        RETURNS (str): The rendered code block.
        """
        return "```{}\n{}\n```".format(lang, text)

    def code(self, text: str) -> str:
        """Create Markdown inline code.

        text (str): The inline code text.
        RETURNS (str): The rendered code text.
        """
        return self._wrap(text, "`")

    def bold(self, text: str) -> str:
        """Create bold text.

        text (str): The text to format in boldface.
        RETURNS (str): The formatted text.
        """
        return self._wrap(text, "**")

    def italic(self, text: str):
        """Create italic text.

        text (str): The text to italicize.
        RETURNS (str): The formatted text.
        """
        return self._wrap(text, "_")

    def _wrap(self, text, marker):
        return "{}{}{}".format(marker, text, marker)
