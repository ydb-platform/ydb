# encoding: utf-8

"""Objects related to layout of rendered text, such as TextFitter."""

from PIL import ImageFont


class TextFitter(tuple):
    """
    Value object that knows how to fit text into given rectangular extents.
    """

    def __new__(cls, line_source, extents, font_file):
        width, height = extents
        return tuple.__new__(cls, (line_source, width, height, font_file))

    @classmethod
    def best_fit_font_size(cls, text, extents, max_size, font_file):
        """Return whole-number best fit point size less than or equal to `max_size`.

        The return value is the largest whole-number point size less than or equal to
        `max_size` that allows `text` to fit completely within `extents` when rendered
        using font defined in `font_file`.
        """
        line_source = _LineSource(text)
        text_fitter = cls(line_source, extents, font_file)
        return text_fitter._best_fit_font_size(max_size)

    def _best_fit_font_size(self, max_size):
        """
        Return the largest whole-number point size less than or equal to
        *max_size* that this fitter can fit.
        """
        predicate = self._fits_inside_predicate
        sizes = _BinarySearchTree.from_ordered_sequence(range(1, int(max_size) + 1))
        return sizes.find_max(predicate)

    def _break_line(self, line_source, point_size):
        """
        Return a (line, remainder) pair where *line* is the longest line in
        *line_source* that will fit in this fitter's width and *remainder* is
        a |_LineSource| object containing the text following the break point.
        """
        lines = _BinarySearchTree.from_ordered_sequence(line_source)
        predicate = self._fits_in_width_predicate(point_size)
        return lines.find_max(predicate)

    def _fits_in_width_predicate(self, point_size):
        """
        Return a function taking a text string value and returns |True| if
        that text fits in this fitter when rendered at *point_size*. Used as
        predicate for _break_line()
        """

        def predicate(line):
            """
            Return |True| if *line* fits in this fitter when rendered at
            *point_size*.
            """
            cx = _rendered_size(line.text, point_size, self._font_file)[0]
            return cx <= self._width

        return predicate

    @property
    def _fits_inside_predicate(self):
        """Return  function taking an integer point size argument.

        The function returns |True| if the text in this fitter can be wrapped to fit
        entirely within its extents when rendered at that point size.
        """

        def predicate(point_size):
            """Return |True| when text in `line_source` can be wrapped to fit.

            Fit means text can be broken into lines that fit entirely within `extents`
            when rendered at `point_size` using the font defined in `font_file`.
            """
            text_lines = self._wrap_lines(self._line_source, point_size)
            cy = _rendered_size("Ty", point_size, self._font_file)[1]
            return (cy * len(text_lines)) <= self._height

        return predicate

    @property
    def _font_file(self):
        return self[3]

    @property
    def _height(self):
        return self[2]

    @property
    def _line_source(self):
        return self[0]

    @property
    def _width(self):
        return self[1]

    def _wrap_lines(self, line_source, point_size):
        """
        Return a sequence of str values representing the text in
        *line_source* wrapped within this fitter when rendered at
        *point_size*.
        """
        text, remainder = self._break_line(line_source, point_size)
        lines = [text]
        if remainder:
            lines.extend(self._wrap_lines(remainder, point_size))
        return lines


class _BinarySearchTree(object):
    """
    A node in a binary search tree. Uniform for root, subtree root, and leaf
    nodes.
    """

    def __init__(self, value):
        self._value = value
        self._lesser = None
        self._greater = None

    def find_max(self, predicate, max_=None):
        """
        Return the largest item in or under this node that satisfies
        *predicate*.
        """
        if predicate(self.value):
            max_ = self.value
            next_node = self._greater
        else:
            next_node = self._lesser
        if next_node is None:
            return max_
        return next_node.find_max(predicate, max_)

    @classmethod
    def from_ordered_sequence(cls, iseq):
        """
        Return the root of a balanced binary search tree populated with the
        values in iterable *iseq*.
        """
        seq = list(iseq)
        # optimize for usually all fits by making longest first
        bst = cls(seq.pop())
        bst._insert_from_ordered_sequence(seq)
        return bst

    def insert(self, value):
        """
        Insert a new node containing *value* into this tree such that its
        structure as a binary search tree is preserved.
        """
        side = "_lesser" if value < self.value else "_greater"
        child = getattr(self, side)
        if child is None:
            setattr(self, side, _BinarySearchTree(value))
        else:
            child.insert(value)

    def tree(self, level=0, prefix=""):
        """
        A string representation of the tree rooted in this node, useful for
        debugging purposes.
        """
        text = "%s%s\n" % (prefix, self.value.text)
        prefix = "%s└── " % ("    " * level)
        if self._lesser:
            text += self._lesser.tree(level + 1, prefix)
        if self._greater:
            text += self._greater.tree(level + 1, prefix)
        return text

    @property
    def value(self):
        """
        The value object contained in this node.
        """
        return self._value

    @staticmethod
    def _bisect(seq):
        """
        Return a (medial_value, greater_values, lesser_values) 3-tuple
        obtained by bisecting sequence *seq*.
        """
        if len(seq) == 0:
            return [], None, []
        mid_idx = int(len(seq) / 2)
        mid = seq[mid_idx]
        greater = seq[mid_idx + 1 :]
        lesser = seq[:mid_idx]
        return mid, greater, lesser

    def _insert_from_ordered_sequence(self, seq):
        """
        Insert the new values contained in *seq* into this tree such that
        a balanced tree is produced.
        """
        if len(seq) == 0:
            return
        mid, greater, lesser = self._bisect(seq)
        self.insert(mid)
        self._insert_from_ordered_sequence(greater)
        self._insert_from_ordered_sequence(lesser)


class _LineSource(object):
    """
    Generates all the possible even-word line breaks in a string of text,
    each in the form of a (line, remainder) 2-tuple where *line* contains the
    text before the break and *remainder* the text after as a |_LineSource|
    object. Its boolean value is |True| when it contains text, |False| when
    its text is the empty string or whitespace only.
    """

    def __init__(self, text):
        self._text = text

    def __bool__(self):
        """
        Gives this object boolean behaviors (in Python 3). bool(line_source)
        is False if it contains the empty string or whitespace only.
        """
        return self._text.strip() != ""

    def __eq__(self, other):
        return self._text == other._text

    def __iter__(self):
        """
        Generate a (text, remainder) pair for each possible even-word line
        break in this line source, where *text* is a str value and remainder
        is a |_LineSource| value.
        """
        words = self._text.split()
        for idx in range(1, len(words) + 1):
            line_text = " ".join(words[:idx])
            remainder_text = " ".join(words[idx:])
            remainder = _LineSource(remainder_text)
            yield _Line(line_text, remainder)

    def __nonzero__(self):
        """
        Gives this object boolean behaviors (in Python 2). bool(line_source)
        is False if it contains the empty string or whitespace only.
        """
        return self._text.strip() != ""

    def __repr__(self):
        return "<_LineSource('%s')>" % self._text


class _Line(tuple):
    """
    A candidate line broken at an even word boundary from a string of text,
    and a |_LineSource| value containing the text that remains after the line
    is broken at this spot.
    """

    def __new__(cls, text, remainder):
        return tuple.__new__(cls, (text, remainder))

    def __gt__(self, other):
        return len(self.text) > len(other.text)

    def __lt__(self, other):
        return not self.__gt__(other)

    def __len__(self):
        return len(self.text)

    def __repr__(self):
        return "'%s' => '%s'" % (self.text, self.remainder)

    @property
    def remainder(self):
        return self[1]

    @property
    def text(self):
        return self[0]


class _Fonts(object):
    """
    A memoizing cache for ImageFont objects.
    """

    fonts = {}

    @classmethod
    def font(cls, font_path, point_size):
        if (font_path, point_size) not in cls.fonts:
            cls.fonts[(font_path, point_size)] = ImageFont.truetype(
                font_path, point_size
            )
        return cls.fonts[(font_path, point_size)]


def _rendered_size(text, point_size, font_file):
    """
    Return a (width, height) pair representing the size of *text* in English
    Metric Units (EMU) when rendered at *point_size* in the font defined in
    *font_file*.
    """
    emu_per_inch = 914400
    px_per_inch = 72.0

    font = _Fonts.font(font_file, point_size)
    try:
        px_width, px_height = font.getsize(text)
    except AttributeError:
        left, top, right, bottom = font.getbbox(text)
        px_width, px_height = right - left, bottom - top

    emu_width = int(px_width / px_per_inch * emu_per_inch)
    emu_height = int(px_height / px_per_inch * emu_per_inch)

    return emu_width, emu_height
