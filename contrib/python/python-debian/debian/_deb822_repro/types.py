try:
    from typing import TypeVar, Union, Tuple, List, Callable, Iterator, TYPE_CHECKING

    if TYPE_CHECKING:
        from debian._deb822_repro.tokens import Deb822Token, Deb822FieldNameToken
        from debian._deb822_repro.parsing import (
            Deb822Element,
            Deb822CommentElement,
            Deb822ParsedValueElement,
        )
        from debian._deb822_repro.formatter import FormatterContentToken

    TokenOrElement = Union["Deb822Element", "Deb822Token"]
    TE = TypeVar("TE", bound=TokenOrElement)

    # Used as a resulting element for "mapping" functions that map TE -> R (see _combine_parts)
    R = TypeVar("R", bound="Deb822Element")

    VE = TypeVar("VE", bound="Deb822Element")

    ST = TypeVar("ST", bound="Deb822Token")

    # Internal type for part of the paragraph key.  Used to facility _unpack_key.
    ParagraphKeyBase = Union["Deb822FieldNameToken", str]

    ParagraphKey = Union[ParagraphKeyBase, Tuple[str, int]]

    Commentish = Union[List[str], "Deb822CommentElement"]

    FormatterCallback = Callable[
        [str, "FormatterContentToken", Iterator["FormatterContentToken"]],
        Iterator[Union["FormatterContentToken", str]],
    ]
    try:
        # Set __doc__ attributes if possible
        TE.__doc__ = """
        Generic "Token or Element" type
        """
        R.__doc__ = """
        For internal usage in _deb822_repro
        """
        VE.__doc__ = """
        Value type/element in a list interpretation of a field value
        """
        ST.__doc__ = """
        Separator type/token in a list interpretation of a field value
        """
        ParagraphKeyBase.__doc__ = """
        For internal usage in _deb822_repro
        """
        ParagraphKey.__doc__ = """
        Anything accepted as a key for a paragraph field lookup.  The simple case being
        a str. Alternative variants are mostly interesting for paragraphs with repeated
        fields (to enable unambiguous lookups)
        """
        Commentish.__doc__ = """
        Anything accepted as input for a Comment. The simple case is the list
        of string (each element being a line of comment). The alternative format is
        there for enable reuse of an existing element (e.g. to avoid "unpacking"
        only to "re-pack" an existing comment element).
        """
        FormatterCallback.__doc__ = """\
        Formatter callback used with the round-trip safe parser
    
        See debian._repro_deb822.formatter.format_field for details
        """
    except AttributeError:
        # Python 3.5 does not allow update to the __doc__ attribute - ignore that
        pass
except ImportError:
    pass


class AmbiguousDeb822FieldKeyError(KeyError):
    """Specialized version of KeyError to denote a valid but ambiguous field name

    This exception occurs if:
      * the field is accessed via a str on a configured view that does not automatically
        resolve ambiguous field names (see Deb822ParagraphElement.configured_view), AND
      * a concrete paragraph contents a repeated field (which is not valid in deb822
        but the module supports parsing them)

    Note that the default is to automatically resolve ambiguous fields. Accordingly
    you will only see this exception if you have "opted in" on wanting to know that
    the lookup was ambiguous.

    The ambiguity can be resolved by using a tuple of (<field-name>, <filed-index>)
    instead of <field-name>.
    """


class SyntaxOrParseError(ValueError):
    """Specialized version of ValueError for syntax/parse errors."""
