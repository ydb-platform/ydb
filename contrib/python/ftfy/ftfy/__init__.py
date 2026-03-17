"""
ftfy: fixes text for you

This is a module for making text less broken. See the `fix_text` function
for more information.
"""

from __future__ import annotations

import unicodedata
import warnings
from collections.abc import Iterator
from typing import (
    Any,
    BinaryIO,
    Callable,
    Literal,
    NamedTuple,
    TextIO,
    cast,
)

from ftfy import bad_codecs, chardata, fixes
from ftfy.badness import is_bad
from ftfy.formatting import display_ljust

__version__ = "6.3.1"


# Though this function does nothing, it lets linters know that we're using
# ftfy.bad_codecs. See the docstring in `bad_codecs/__init__.py` for more.
bad_codecs.ok()


class ExplanationStep(NamedTuple):
    """
    A step in an ExplainedText, explaining how to decode text.

    The possible actions are:

    - "encode": take in a string and encode it as bytes, with the given encoding
    - "decode": take in bytes and decode them as a string, with the given encoding
    - "transcode": convert bytes to bytes with a particular named function
    - "apply": convert str to str with a particular named function

    The `parameter` is the name of the encoding or function to use. If it's a
    function, it must appear in the FIXERS dictionary.
    """

    action: str
    parameter: str

    def __repr__(self) -> str:
        """
        Get the string representation of an ExplanationStep. We output the
        representation of the equivalent tuple, for simplicity.
        """
        return repr(tuple(self))


class ExplainedText(NamedTuple):
    """
    The return type from ftfy's functions that provide an "explanation" of which
    steps it applied to fix the text, such as :func:`fix_and_explain()`.

    When the 'explain' option is disabled, these functions return the same
    type, but the `explanation` will be None.
    """

    text: str
    explanation: list[ExplanationStep] | None


# Functions that can be applied using `apply_plan`.
FIXERS: dict[str, Callable] = {  # type: ignore[type-arg]
    "unescape_html": fixes.unescape_html,
    "remove_terminal_escapes": fixes.remove_terminal_escapes,
    "restore_byte_a0": fixes.restore_byte_a0,
    "replace_lossy_sequences": fixes.replace_lossy_sequences,
    "decode_inconsistent_utf8": fixes.decode_inconsistent_utf8,
    "fix_c1_controls": fixes.fix_c1_controls,
    "fix_latin_ligatures": fixes.fix_latin_ligatures,
    "fix_character_width": fixes.fix_character_width,
    "uncurl_quotes": fixes.uncurl_quotes,
    "fix_line_breaks": fixes.fix_line_breaks,
    "fix_surrogates": fixes.fix_surrogates,
    "remove_control_chars": fixes.remove_control_chars,
}


class TextFixerConfig(NamedTuple):
    r"""
    A TextFixerConfig object stores configuration options for ftfy.

    It's implemented as a namedtuple with defaults, so you can instantiate
    it by providing the values to change from their defaults as keyword arguments.
    For example, to disable 'unescape_html' and keep the rest of the defaults::

        TextFixerConfig(unescape_html=False)

    Here are the options and their default values:

    - `unescape_html`: "auto"

      Configures whether to replace HTML entities such as &amp; with the character
      they represent. "auto" says to do this by default, but disable it when a
      literal < character appears, indicating that the input is actual HTML and
      entities should be preserved. The value can be True, to always enable this
      fixer, or False, to always disable it.

    - `remove_terminal_escapes`: True

      Removes "ANSI" terminal escapes, such as for changing the color of text in a
      terminal window.

    - `fix_encoding`: True

      Detect mojibake and attempt to fix it by decoding the text in a different
      encoding standard.

      The following four options affect `fix_encoding` works, and do nothing if
      `fix_encoding` is False:

      - `restore_byte_a0`: True

        Allow a literal space (U+20) to be interpreted as a non-breaking space
        (U+A0) when that would make it part of a fixable mojibake string.

        Because spaces are very common characters, this could lead to false
        positives, but we try to apply it only when there's strong evidence for
        mojibake. Disabling `restore_byte_a0` is safer from false positives,
        but creates false negatives.

      - `replace_lossy_sequences`: True

        Detect mojibake that has been partially replaced by the characters
        '�' or '?'. If the mojibake could be decoded otherwise, replace the
        detected sequence with '�'.

      - `decode_inconsistent_utf8`: True

        When we see sequences that distinctly look like UTF-8 mojibake, but
        there's no consistent way to reinterpret the string in a new encoding,
        replace the mojibake with the appropriate UTF-8 characters anyway.

        This helps to decode strings that are concatenated from different
        encodings.

      - `fix_c1_controls`: True

        Replace C1 control characters (the useless characters U+80 - U+9B that
        come from Latin-1) with their Windows-1252 equivalents, like HTML5 does,
        even if the whole string doesn't decode as Latin-1.

    - `fix_latin_ligatures`: True

      Replace common Latin-alphabet ligatures, such as ``ﬁ``, with the
      letters they're made of.

    - `fix_character_width`: True

      Replace fullwidth Latin characters and halfwidth Katakana with
      their more standard widths.

    - `uncurl_quotes`: True

      Replace curly quotes with straight quotes.

    - `fix_line_breaks`: True

      Replace various forms of line breaks with the standard Unix line
      break, ``\n``.

    - `fix_surrogates`: True

      Replace sequences of UTF-16 surrogate codepoints with the character
      they were meant to encode. This fixes text that was decoded with the
      obsolete UCS-2 standard, and allows it to support high-numbered
      codepoints such as emoji.

    - `remove_control_chars`: True

      Remove certain control characters that have no displayed effect on text.

    - `normalization`: "NFC"

      Choose what kind of Unicode normalization is applied. Usually, we apply
      NFC normalization, so that letters followed by combining characters become
      single combined characters.

      Changing this to "NFKC" applies more compatibility conversions, such as
      replacing the 'micro sign' with a standard Greek lowercase mu, which looks
      identical. However, some NFKC normalizations change the meaning of text,
      such as converting "10³" to "103".

    `normalization` can be None, to apply no normalization.

    - `max_decode_length`: 1_000_000

      The maximum size of "segment" that ftfy will try to fix all at once.

    - `explain`: True

      Whether to compute 'explanations', lists describing what ftfy changed.
      When this is False, the explanation will be None, and the code that
      builds the explanation will be skipped, possibly saving time.

      Functions that accept TextFixerConfig and don't return an explanation
      will automatically set `explain` to False.
    """

    unescape_html: str | bool = "auto"
    remove_terminal_escapes: bool = True
    fix_encoding: bool = True
    restore_byte_a0: bool = True
    replace_lossy_sequences: bool = True
    decode_inconsistent_utf8: bool = True
    fix_c1_controls: bool = True
    fix_latin_ligatures: bool = True
    fix_character_width: bool = True
    uncurl_quotes: bool = True
    fix_line_breaks: bool = True
    fix_surrogates: bool = True
    remove_control_chars: bool = True
    normalization: Literal["NFC", "NFD", "NFKC", "NFKD"] | None = "NFC"
    max_decode_length: int = 1000000
    explain: bool = True


def _config_from_kwargs(
    config: TextFixerConfig, kwargs: dict[str, Any]
) -> TextFixerConfig:
    """
    Handle parameters provided as keyword arguments to ftfy's top-level
    functions, converting them into a TextFixerConfig.
    """
    if "fix_entities" in kwargs:
        warnings.warn(
            "`fix_entities` has been renamed to `unescape_html`",
            DeprecationWarning,
            stacklevel=2,
        )
        kwargs = kwargs.copy()
        kwargs["unescape_html"] = kwargs["fix_entities"]
        del kwargs["fix_entities"]
    config = config._replace(**kwargs)
    return config


BYTES_ERROR_TEXT = """Hey wait, this isn't Unicode.

ftfy is designed to fix problems with text. Treating bytes like they're
interchangeable with Unicode text is usually something that introduces
problems with text.

You should first decode these bytes from the encoding you think they're in.
If you're not sure what encoding they're in:

- First, try to find out. 'utf-8' is a good assumption.
- If the encoding is simply unknowable, try running your bytes through
  ftfy.guess_bytes. As the name implies, this may not always be accurate.

For more information on the distinction between bytes and text, read the
Python Unicode HOWTO:

    http://docs.python.org/3/howto/unicode.html
"""


def _try_fix(
    fixer_name: str,
    text: str,
    config: TextFixerConfig,
    steps: list[ExplanationStep] | None,
) -> str:
    """
    A helper function used across several 'fixer' steps, deciding whether to
    apply the fix and whether to record the fix in `steps`.
    """
    if getattr(config, fixer_name):
        fixer = FIXERS[fixer_name]
        fixed = fixer(text)
        if steps is not None and fixed != text:
            steps.append(ExplanationStep("apply", fixer_name))
        return cast(str, fixed)

    return text


def fix_text(text: str, config: TextFixerConfig | None = None, **kwargs: Any) -> str:
    r"""
    Given Unicode text as input, fix inconsistencies and glitches in it,
    such as mojibake (text that was decoded in the wrong encoding).

    Let's start with some examples:

        >>> fix_text('âœ” No problems')
        '✔ No problems'

        >>> print(fix_text("&macr;\\_(ã\x83\x84)_/&macr;"))
        ¯\_(ツ)_/¯

        >>> fix_text('Broken text&hellip; it&#x2019;s ﬂubberiﬁc!')
        "Broken text... it's flubberific!"

        >>> fix_text('ＬＯＵＤ　ＮＯＩＳＥＳ')
        'LOUD NOISES'

    ftfy applies a number of different fixes to the text, and can accept
    configuration to select which fixes to apply.

    The configuration takes the form of a :class:`TextFixerConfig` object,
    and you can see a description of the options in that class's docstring
    or in the full documentation at ftfy.readthedocs.org.

    For convenience and backward compatibility, the configuration can also
    take the form of keyword arguments, which will set the equivalently-named
    fields of the TextFixerConfig object.

    For example, here are two ways to fix text but skip the "uncurl_quotes"
    step::

        fix_text(text, TextFixerConfig(uncurl_quotes=False))
        fix_text(text, uncurl_quotes=False)

    This function fixes text in independent segments, which are usually lines
    of text, or arbitrarily broken up every 1 million codepoints (configurable
    with `config.max_decode_length`) if there aren't enough line breaks. The
    bound on segment lengths helps to avoid unbounded slowdowns.

    ftfy can also provide an 'explanation', a list of transformations it applied
    to the text that would fix more text like it. This function doesn't provide
    explanations (because there may be different fixes for different segments
    of text).

    To get an explanation, use the :func:`fix_and_explain()` function, which
    fixes the string in one segment and explains what it fixed.
    """

    if config is None:
        config = TextFixerConfig(explain=False)
    config = _config_from_kwargs(config, kwargs)
    if isinstance(text, bytes):
        raise UnicodeError(BYTES_ERROR_TEXT)

    out = []
    pos = 0
    while pos < len(text):
        textbreak = text.find("\n", pos) + 1
        if textbreak == 0:
            textbreak = len(text)
        if (textbreak - pos) > config.max_decode_length:
            textbreak = pos + config.max_decode_length

        segment = text[pos:textbreak]
        if config.unescape_html == "auto" and "<" in segment:
            config = config._replace(unescape_html=False)
        fixed_segment, _ = fix_and_explain(segment, config)
        out.append(fixed_segment)
        pos = textbreak
    return "".join(out)


def fix_and_explain(
    text: str, config: TextFixerConfig | None = None, **kwargs: Any
) -> ExplainedText:
    """
    Fix text as a single segment, returning the fixed text and an explanation
    of what was fixed.

    The explanation is a list of steps that can be applied with
    :func:`apply_plan`, or if config.explain is False, it will be None.
    """
    if config is None:
        config = TextFixerConfig()
    if isinstance(text, bytes):
        raise UnicodeError(BYTES_ERROR_TEXT)
    config = _config_from_kwargs(config, kwargs)

    if config.unescape_html == "auto" and "<" in text:
        config = config._replace(unescape_html=False)

    if config.explain:
        steps: list[ExplanationStep] | None = []
    else:
        # If explanations aren't desired, `steps` will be None
        steps = None

    while True:
        origtext = text

        text = _try_fix("unescape_html", text, config, steps)

        if config.fix_encoding:
            if steps is None:
                text = fix_encoding(text)
            else:
                text, encoding_steps = fix_encoding_and_explain(text, config)
                if encoding_steps is not None:
                    steps.extend(encoding_steps)

        for fixer in [
            "fix_c1_controls",
            "fix_latin_ligatures",
            "fix_character_width",
            "uncurl_quotes",
            "fix_line_breaks",
            "fix_surrogates",
            "remove_terminal_escapes",
            "remove_control_chars",
        ]:
            text = _try_fix(fixer, text, config, steps)

        if config.normalization is not None:
            fixed = unicodedata.normalize(config.normalization, text)
            if steps is not None and fixed != text:
                steps.append(ExplanationStep("normalize", config.normalization))
            text = fixed

        if text == origtext:
            return ExplainedText(text, steps)


def fix_encoding_and_explain(
    text: str, config: TextFixerConfig | None = None, **kwargs: Any
) -> ExplainedText:
    """
    Apply the steps of ftfy that detect mojibake and fix it. Returns the fixed
    text and a list explaining what was fixed.

    This includes fixing text by encoding and decoding it in different encodings,
    as well as the subordinate fixes `restore_byte_a0`, `replace_lossy_sequences`,
    `decode_inconsistent_utf8`, and `fix_c1_controls`.

    Examples::

        >>> fix_encoding_and_explain("sÃ³")
        ExplainedText(text='só', explanation=[('encode', 'latin-1'), ('decode', 'utf-8')])

        >>> result = fix_encoding_and_explain("voilÃ le travail")
        >>> result.text
        'voilà le travail'
        >>> result.explanation
        [('encode', 'latin-1'), ('transcode', 'restore_byte_a0'), ('decode', 'utf-8')]

    """
    if config is None:
        config = TextFixerConfig()
    if isinstance(text, bytes):
        raise UnicodeError(BYTES_ERROR_TEXT)
    config = _config_from_kwargs(config, kwargs)

    if not config.fix_encoding:
        # A weird trivial case: we're asked to fix the encoding, but skip
        # fixing the encoding
        return ExplainedText(text, [])

    plan_so_far: list[ExplanationStep] = []
    while True:
        prevtext = text
        text, plan = _fix_encoding_one_step_and_explain(text, config)
        if plan is not None:
            plan_so_far.extend(plan)
        if text == prevtext:
            return ExplainedText(text, plan_so_far)


def _fix_encoding_one_step_and_explain(
    text: str, config: TextFixerConfig
) -> ExplainedText:
    """
    Perform one step of fixing the encoding of text.
    """
    if config is None:
        config = TextFixerConfig()

    if len(text) == 0:
        return ExplainedText(text, [])

    # The first plan is to return ASCII text unchanged, as well as text
    # that doesn't look like it contains mojibake
    if chardata.possible_encoding(text, "ascii") or not is_bad(text):
        return ExplainedText(text, [])

    # As we go through the next step, remember the possible encodings
    # that we encounter but don't successfully fix yet. We may need them
    # later.
    possible_1byte_encodings = []

    # Suppose the text was supposed to be UTF-8, but it was decoded using
    # a single-byte encoding instead. When these cases can be fixed, they
    # are usually the correct thing to do, so try them next.
    for encoding in chardata.CHARMAP_ENCODINGS:
        if chardata.possible_encoding(text, encoding):
            possible_1byte_encodings.append(encoding)
            encoded_bytes = text.encode(encoding)
            encode_step = ExplanationStep("encode", encoding)
            transcode_steps = []

            # Now, find out if it's UTF-8 (or close enough). Otherwise,
            # remember the encoding for later.
            try:
                decoding = "utf-8"
                # Check encoded_bytes for sequences that would be UTF-8,
                # except they have b' ' where b'\xa0' would belong.
                #
                # Don't do this in the macroman encoding, where it would match
                # an en dash followed by a space, leading to false positives.
                if (
                    config.restore_byte_a0
                    and encoding != "macroman"
                    and chardata.ALTERED_UTF8_RE.search(encoded_bytes)
                ):
                    replaced_bytes = fixes.restore_byte_a0(encoded_bytes)
                    if replaced_bytes != encoded_bytes:
                        transcode_steps.append(
                            ExplanationStep("transcode", "restore_byte_a0")
                        )
                        encoded_bytes = replaced_bytes

                # Replace sequences where information has been lost
                if config.replace_lossy_sequences and encoding.startswith("sloppy"):
                    replaced_bytes = fixes.replace_lossy_sequences(encoded_bytes)
                    if replaced_bytes != encoded_bytes:
                        transcode_steps.append(
                            ExplanationStep("transcode", "replace_lossy_sequences")
                        )
                        encoded_bytes = replaced_bytes

                if 0xED in encoded_bytes or 0xC0 in encoded_bytes:
                    decoding = "utf-8-variants"

                decode_step = ExplanationStep("decode", decoding)
                steps = [encode_step] + transcode_steps + [decode_step]
                fixed = encoded_bytes.decode(decoding)
                return ExplainedText(fixed, steps)

            except UnicodeDecodeError:
                pass

    # Look for a-hat-euro sequences that remain, and fix them in isolation.
    if config.decode_inconsistent_utf8 and chardata.UTF8_DETECTOR_RE.search(text):
        steps = [ExplanationStep("apply", "decode_inconsistent_utf8")]
        fixed = fixes.decode_inconsistent_utf8(text)
        if fixed != text:
            return ExplainedText(fixed, steps)

    # The next most likely case is that this is Latin-1 that was intended to
    # be read as Windows-1252, because those two encodings in particular are
    # easily confused.
    if "latin-1" in possible_1byte_encodings:
        if "windows-1252" in possible_1byte_encodings:
            # This text is in the intersection of Latin-1 and
            # Windows-1252, so it's probably legit.
            return ExplainedText(text, [])
        else:
            # Otherwise, it means we have characters that are in Latin-1 but
            # not in Windows-1252. Those are C1 control characters. Nobody
            # wants those. Assume they were meant to be Windows-1252.
            try:
                fixed = text.encode("latin-1").decode("windows-1252")
                if fixed != text:
                    steps = [
                        ExplanationStep("encode", "latin-1"),
                        ExplanationStep("decode", "windows-1252"),
                    ]
                    return ExplainedText(fixed, steps)
            except UnicodeDecodeError:
                pass

    # Fix individual characters of Latin-1 with a less satisfying explanation
    if config.fix_c1_controls and chardata.C1_CONTROL_RE.search(text):
        steps = [ExplanationStep("transcode", "fix_c1_controls")]
        fixed = fixes.fix_c1_controls(text)
        return ExplainedText(fixed, steps)

    # The cases that remain are mixups between two different single-byte
    # encodings, and not the common case of Latin-1 vs. Windows-1252.
    #
    # With the new heuristic in 6.0, it's possible that we're closer to solving
    # these in some cases. It would require a lot of testing and tuning, though.
    # For now, we leave the text unchanged in these cases.
    return ExplainedText(text, [])


def fix_encoding(
    text: str, config: TextFixerConfig | None = None, **kwargs: Any
) -> str:
    """
    Apply just the encoding-fixing steps of ftfy to this text. Returns the
    fixed text, discarding the explanation.

        >>> fix_encoding("Ã³")
        'ó'
        >>> fix_encoding("&ATILDE;&SUP3;")
        '&ATILDE;&SUP3;'
    """
    if config is None:
        config = TextFixerConfig(explain=False)
    config = _config_from_kwargs(config, kwargs)
    fixed, _explan = fix_encoding_and_explain(text, config)
    return fixed


# Some alternate names for the main functions
ftfy = fix_text


def fix_text_segment(
    text: str, config: TextFixerConfig | None = None, **kwargs: Any
) -> str:
    """
    Fix text as a single segment, with a consistent sequence of steps that
    are applied to fix the text. Discard the explanation.
    """
    if config is None:
        config = TextFixerConfig(explain=False)
    config = _config_from_kwargs(config, kwargs)
    fixed, _explan = fix_and_explain(text, config)
    return fixed


def fix_file(
    input_file: TextIO | BinaryIO,
    encoding: str | None = None,
    config: TextFixerConfig | None = None,
    **kwargs: Any,
) -> Iterator[str]:
    """
    Fix text that is found in a file.

    If the file is being read as Unicode text, use that. If it's being read as
    bytes, then we hope an encoding was supplied. If not, unfortunately, we
    have to guess what encoding it is. We'll try a few common encodings, but we
    make no promises. See the `guess_bytes` function for how this is done.

    The output is a stream of fixed lines of text.
    """
    if config is None:
        config = TextFixerConfig()
    config = _config_from_kwargs(config, kwargs)

    for line in input_file:
        if isinstance(line, bytes):
            if encoding is None:
                line, encoding = guess_bytes(line)
            else:
                line = line.decode(encoding)
        if config.unescape_html == "auto" and "<" in line:
            config = config._replace(unescape_html=False)

        fixed_line, _explan = fix_and_explain(line, config)
        yield fixed_line


def guess_bytes(bstring: bytes) -> tuple[str, str]:
    """
    NOTE: Using `guess_bytes` is not the recommended way of using ftfy. ftfy
    is not designed to be an encoding detector.

    In the unfortunate situation that you have some bytes in an unknown
    encoding, ftfy can guess a reasonable strategy for decoding them, by trying
    a few common encodings that can be distinguished from each other.

    Unlike the rest of ftfy, this may not be accurate, and it may *create*
    Unicode problems instead of solving them!

    The encodings we try here are:

    - UTF-16 with a byte order mark, because a UTF-16 byte order mark looks
      like nothing else
    - UTF-8, because it's the global standard, which has been used by a
      majority of the Web since 2008
    - "utf-8-variants", or buggy implementations of UTF-8
    - MacRoman, because Microsoft Office thinks it's still a thing, and it
      can be distinguished by its line breaks. (If there are no line breaks in
      the string, though, you're out of luck.)
    - "sloppy-windows-1252", the Latin-1-like encoding that is the most common
      single-byte encoding.
    """
    if isinstance(bstring, str):
        raise UnicodeError(
            "This string was already decoded as Unicode. You should pass "
            "bytes to guess_bytes, not Unicode."
        )

    if bstring.startswith(b"\xfe\xff") or bstring.startswith(b"\xff\xfe"):
        return bstring.decode("utf-16"), "utf-16"

    byteset = set(bstring)
    try:
        if 0xED in byteset or 0xC0 in byteset:
            # Byte 0xed can be used to encode a range of codepoints that
            # are UTF-16 surrogates. UTF-8 does not use UTF-16 surrogates,
            # so when we see 0xed, it's very likely we're being asked to
            # decode CESU-8, the variant that encodes UTF-16 surrogates
            # instead of the original characters themselves.
            #
            # This will occasionally trigger on standard UTF-8, as there
            # are some Korean characters that also use byte 0xed, but that's
            # not harmful because standard UTF-8 characters will decode the
            # same way in our 'utf-8-variants' codec.
            #
            # Byte 0xc0 is impossible because, numerically, it would only
            # encode characters lower than U+0040. Those already have
            # single-byte representations, and UTF-8 requires using the
            # shortest possible representation. However, Java hides the null
            # codepoint, U+0000, in a non-standard longer representation -- it
            # encodes it as 0xc0 0x80 instead of 0x00, guaranteeing that 0x00
            # will never appear in the encoded bytes.
            #
            # The 'utf-8-variants' decoder can handle both of these cases, as
            # well as standard UTF-8, at the cost of a bit of speed.
            return bstring.decode("utf-8-variants"), "utf-8-variants"
        else:
            return bstring.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        pass

    if 0x0D in byteset and 0x0A not in byteset:
        # Files that contain CR and not LF are likely to be MacRoman.
        return bstring.decode("macroman"), "macroman"

    return bstring.decode("sloppy-windows-1252"), "sloppy-windows-1252"


def apply_plan(text: str, plan: list[tuple[str, str]]) -> str:
    """
    Apply a plan for fixing the encoding of text.

    The plan is a list of tuples of the form (operation, arg).

    `operation` is one of:

    - `'encode'`: convert a string to bytes, using `arg` as the encoding
    - `'decode'`: convert bytes to a string, using `arg` as the encoding
    - `'transcode'`: convert bytes to bytes, using the function named `arg`
    - `'apply'`: convert a string to a string, using the function named `arg`

    The functions that can be applied by 'transcode' and 'apply' are
    specifically those that appear in the dictionary named `FIXERS`. They
    can also can be imported from the `ftfy.fixes` module.

    Example::

        >>> mojibake = "schÃ¶n"
        >>> text, plan = fix_and_explain(mojibake)
        >>> apply_plan(mojibake, plan)
        'schön'
    """
    obj = text
    for operation, encoding in plan:
        if operation == "encode":
            obj = obj.encode(encoding)  # type: ignore
        elif operation == "decode":
            obj = obj.decode(encoding)  # type: ignore
        elif operation in ("transcode", "apply"):
            if encoding in FIXERS:
                obj = FIXERS[encoding](obj)
            else:
                raise ValueError(f"Unknown function to apply: {encoding}")
        else:
            raise ValueError(f"Unknown plan step: {operation}")

    return obj


def explain_unicode(text: str) -> None:
    """
    A utility method that's useful for debugging mysterious Unicode.

    It breaks down a string, showing you for each codepoint its number in
    hexadecimal, its glyph, its category in the Unicode standard, and its name
    in the Unicode standard.

        >>> explain_unicode('(╯°□°)╯︵ ┻━┻')
        U+0028  (       [Ps] LEFT PARENTHESIS
        U+256F  ╯       [So] BOX DRAWINGS LIGHT ARC UP AND LEFT
        U+00B0  °       [So] DEGREE SIGN
        U+25A1  □       [So] WHITE SQUARE
        U+00B0  °       [So] DEGREE SIGN
        U+0029  )       [Pe] RIGHT PARENTHESIS
        U+256F  ╯       [So] BOX DRAWINGS LIGHT ARC UP AND LEFT
        U+FE35  ︵      [Ps] PRESENTATION FORM FOR VERTICAL LEFT PARENTHESIS
        U+0020          [Zs] SPACE
        U+253B  ┻       [So] BOX DRAWINGS HEAVY UP AND HORIZONTAL
        U+2501  ━       [So] BOX DRAWINGS HEAVY HORIZONTAL
        U+253B  ┻       [So] BOX DRAWINGS HEAVY UP AND HORIZONTAL
    """
    for char in text:
        if char.isprintable():
            display = char
        else:
            display = char.encode("unicode-escape").decode("ascii")
        print(
            "U+{code:04X}  {display} [{category}] {name}".format(
                display=display_ljust(display, 7),
                code=ord(char),
                category=unicodedata.category(char),
                name=unicodedata.name(char, "<unknown>"),
            )
        )
