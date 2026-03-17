from enum import Enum, auto
import warnings


class AtxHeaderLevel(Enum):
    TITLE = auto()          # H1 - Main title (largest and most important)
    HEADING = auto()        # H2 - Section headings
    SUBHEADING = auto()     # H3 - Subsection headings
    SUBSUBHEADING = auto()  # H4 - Smaller subsection headings
    MINORHEADING = auto()   # H5 - Even smaller headings
    LEASTHEADING = auto()   # H6 - The smallest heading level


class SetextHeaderLevel(Enum):
    TITLE = auto()
    HEADING = auto()


class HeaderStyle(Enum):
    ATX = auto()
    SETEXT = auto()


class Header:
    """Contain the main methods to define Headers on a Markdown file.

    Features available:
    - Create Markdown Titles: *atx* and *setext* formats are available.
    - Create Header Anchors.

    :Example:
    >>> str(Header(level=1, title='New Header', style=HeaderStyle.ATX))
    """

    def __init__(self, level: int, title: str, style: HeaderStyle, header_id: str = None) -> None:
        """Choose and return the header based on the given level, style, and title.

        :param level: HeaderLevel enum member, e.g., TITLE, HEADING, SUBHEADING, etc.
        :param title: Text title.
        :param style: HeaderStyle enum member, e.g., ATX, SETEXT.
        :param header_id: ID of the header for extended Markdown syntax (optional)
        :return: a header string based on the given level, style, and title
        """
        self.level = level
        self.title = title
        self.style = style
        self.header_id = header_id

    def __str__(self) -> str:
        return self._new(self.level, self.title, self.style, self.header_id)

    @staticmethod
    def atx(level: AtxHeaderLevel, title: str, header_id: str = None) -> str:
        """Return an atx-style header.

        :param title: Text title.
        :param level: HeaderLevel enum member, e.g., TITLE, HEADING, SUBHEADING, etc.
        :param header_id: ID of the header for extended Markdown syntax (optional)
        :return: an atx-style header string
        """
        header_id = Header._get_header_id(header_id)
        return "\n" + "#" * level.value + " " + title + header_id + "\n"

    @staticmethod
    def setext(level: SetextHeaderLevel, title: str) -> str:
        """Return a setext-style header.

        :param title: Text title.
        :param level: HeaderLevel enum member, e.g., TITLE, HEADING.
        :return: a setext-style header string
        """
        if level == SetextHeaderLevel.TITLE:
            separator = "="
        else:
            separator = "-"

        return "\n" + title + "\n" + separator * len(title) + "\n"

    @staticmethod
    def header_anchor(text: str, link: str = None) -> str:
        """Create an internal link to a defined header level in the markdown file.

        :param text: Displayed text for the link.
        :param link: Internal link (optional). If not provided, it is generated based on the text.
        :return: a header anchor string

        Examples:

        1. Using the default generated link based on the text:
        
        >>> header_link = Header.header_anchor("Section 1")
        >>> print(header_link)
        [Section 1](#section-1)

        2. Providing a custom link:

        >>> header_link = Header.header_anchor("Section 1", "custom-link")
        >>> print(header_link)
        [Section 1](#custom-link)

        3. Providing a link with an existing '#' symbol:

        >>> header_link = Header.header_anchor("Section 1", "#existing-link")
        >>> print(header_link)
        [Section 1](#existing-link)
        """
        if not link:
            link = "#" + text.lower().replace(" ", "-")
        elif link[0] != "#":
            link = link.lower().replace(" ", "-")
        else:
            link = "#" + link

        return "[" + text + "](" + link + ")"

    @staticmethod
    def choose_header(
        level: int, title: str, style: str = "atx", header_id: str = ""
    ) -> str:
        # noinspection SpellCheckingInspection
        """This method choose the style and the header level.

            :Examples:
            >>> from mdutils.tools.Header import Header
            >>> Header.choose_header(level=1, title='New Header', style='atx')
            '\\n# New Header\\n'

            >>> Header.choose_header(level=2, title='Another Header 1', style='setext')
            '\\nAnother Header 1\\n----------------\\n'

        :param level: Header Level, For Atx-style 1 til 6. For Setext-style 1 and 2 header levels.
        :param title: Header Title.
        :param style: Header Style atx or setext.
        :param header_id: ID of the header for extended Markdown syntax
        :return:
        """
        warnings.warn(
            "This method is deprecated. Use the Header class instead, `str(Header())`, this method will be removed on 3.0.0 version",
            DeprecationWarning,
            stacklevel=2,
        )
        return str(Header(level, title, HeaderStyle[style.upper()], header_id))

    def _new(self, level: int, title: str, style: HeaderStyle = HeaderStyle.ATX, header_id: str = None) -> str:
        if style == HeaderStyle.ATX:
            return Header.atx(AtxHeaderLevel(level), title, header_id)
        elif style == HeaderStyle.SETEXT:
            return Header.setext(SetextHeaderLevel(level), title)
        else:
            raise ValueError("style's expected value: 'HeaderStyle.ATX' or 'HeaderStyle.SETEXT'")

    @staticmethod
    def _get_header_id(header_id: str = None) -> str:
        if header_id:
            return " {#" + header_id + "}"
        return ""


if __name__ == "__main__":
    import doctest

    doctest.testmod()
