"""Data label-related objects."""

from __future__ import annotations

from pptx.text.text import Font, TextFrame
from pptx.util import lazyproperty


class DataLabels(object):
    """Provides access to properties of data labels for a plot or a series.

    This is not a collection and does not provide access to individual data
    labels. Access to individual labels is via the |Point| object. The
    properties this object provides control formatting of *all* the data
    labels in its scope.
    """

    def __init__(self, dLbls):
        super(DataLabels, self).__init__()
        self._element = dLbls

    @lazyproperty
    def font(self):
        """
        The |Font| object that provides access to the text properties for
        these data labels, such as bold, italic, etc.
        """
        defRPr = self._element.defRPr
        font = Font(defRPr)
        return font

    @property
    def number_format(self):
        """
        Read/write string specifying the format for the numbers on this set
        of data labels. Returns 'General' if no number format has been set.
        Note that this format string has no effect on rendered data labels
        when :meth:`number_format_is_linked` is |True|. Assigning a format
        string to this property automatically sets
        :meth:`number_format_is_linked` to |False|.
        """
        numFmt = self._element.numFmt
        if numFmt is None:
            return "General"
        return numFmt.formatCode

    @number_format.setter
    def number_format(self, value):
        self._element.get_or_add_numFmt().formatCode = value
        self.number_format_is_linked = False

    @property
    def number_format_is_linked(self):
        """
        Read/write boolean specifying whether number formatting should be
        taken from the source spreadsheet rather than the value of
        :meth:`number_format`.
        """
        numFmt = self._element.numFmt
        if numFmt is None:
            return True
        souceLinked = numFmt.sourceLinked
        if souceLinked is None:
            return True
        return numFmt.sourceLinked

    @number_format_is_linked.setter
    def number_format_is_linked(self, value):
        numFmt = self._element.get_or_add_numFmt()
        numFmt.sourceLinked = value

    @property
    def position(self):
        """
        Read/write :ref:`XlDataLabelPosition` enumeration value specifying
        the position of the data labels with respect to their data point, or
        |None| if no position is specified. Assigning |None| causes
        PowerPoint to choose the default position, which varies by chart
        type.
        """
        dLblPos = self._element.dLblPos
        if dLblPos is None:
            return None
        return dLblPos.val

    @position.setter
    def position(self, value):
        if value is None:
            self._element._remove_dLblPos()
            return
        self._element.get_or_add_dLblPos().val = value

    @property
    def show_category_name(self):
        """Read/write. True when name of category should appear in label."""
        return self._element.get_or_add_showCatName().val

    @show_category_name.setter
    def show_category_name(self, value):
        self._element.get_or_add_showCatName().val = bool(value)

    @property
    def show_legend_key(self):
        """Read/write. True when data label displays legend-color swatch."""
        return self._element.get_or_add_showLegendKey().val

    @show_legend_key.setter
    def show_legend_key(self, value):
        self._element.get_or_add_showLegendKey().val = bool(value)

    @property
    def show_percentage(self):
        """Read/write. True when data label displays percentage.

        This option is not operative on all chart types. Percentage appears
        on polar charts such as pie and donut.
        """
        return self._element.get_or_add_showPercent().val

    @show_percentage.setter
    def show_percentage(self, value):
        self._element.get_or_add_showPercent().val = bool(value)

    @property
    def show_series_name(self):
        """Read/write. True when data label displays series name."""
        return self._element.get_or_add_showSerName().val

    @show_series_name.setter
    def show_series_name(self, value):
        self._element.get_or_add_showSerName().val = bool(value)

    @property
    def show_value(self):
        """Read/write. True when label displays numeric value of datapoint."""
        return self._element.get_or_add_showVal().val

    @show_value.setter
    def show_value(self, value):
        self._element.get_or_add_showVal().val = bool(value)


class DataLabel(object):
    """
    The data label associated with an individual data point.
    """

    def __init__(self, ser, idx):
        super(DataLabel, self).__init__()
        self._ser = self._element = ser
        self._idx = idx

    @lazyproperty
    def font(self):
        """The |Font| object providing text formatting for this data label.

        This font object is used to customize the appearance of automatically
        inserted text, such as the data point value. The font applies to the
        entire data label. More granular control of the appearance of custom
        data label text is controlled by a font object on runs in the text
        frame.
        """
        txPr = self._get_or_add_txPr()
        text_frame = TextFrame(txPr, self)
        paragraph = text_frame.paragraphs[0]
        return paragraph.font

    @property
    def has_text_frame(self):
        """
        Return |True| if this data label has a text frame (implying it has
        custom data label text), and |False| otherwise. Assigning |True|
        causes a text frame to be added if not already present. Assigning
        |False| causes any existing text frame to be removed along with any
        text contained in the text frame.
        """
        dLbl = self._dLbl
        if dLbl is None:
            return False
        if dLbl.xpath("c:tx/c:rich"):
            return True
        return False

    @has_text_frame.setter
    def has_text_frame(self, value):
        if bool(value) is True:
            self._get_or_add_tx_rich()
        else:
            self._remove_tx_rich()

    @property
    def position(self):
        """
        Read/write :ref:`XlDataLabelPosition` member specifying the position
        of this data label with respect to its data point, or |None| if no
        position is specified. Assigning |None| causes PowerPoint to choose
        the default position, which varies by chart type.
        """
        dLbl = self._dLbl
        if dLbl is None:
            return None
        dLblPos = dLbl.dLblPos
        if dLblPos is None:
            return None
        return dLblPos.val

    @position.setter
    def position(self, value):
        if value is None:
            dLbl = self._dLbl
            if dLbl is None:
                return
            dLbl._remove_dLblPos()
            return
        dLbl = self._get_or_add_dLbl()
        dLbl.get_or_add_dLblPos().val = value

    @property
    def text_frame(self):
        """
        |TextFrame| instance for this data label, containing the text of the
        data label and providing access to its text formatting properties.
        """
        rich = self._get_or_add_rich()
        return TextFrame(rich, self)

    @property
    def _dLbl(self):
        """
        Return the |CT_DLbl| instance referring specifically to this
        individual data label (having the same index value), or |None| if not
        present.
        """
        return self._ser.get_dLbl(self._idx)

    def _get_or_add_dLbl(self):
        """
        The ``CT_DLbl`` instance referring specifically to this individual
        data label, newly created if not yet present in the XML.
        """
        return self._ser.get_or_add_dLbl(self._idx)

    def _get_or_add_rich(self):
        """
        Return the `c:rich` element representing the text frame for this data
        label, newly created with its ancestors if not present.
        """
        dLbl = self._get_or_add_dLbl()

        # having a c:spPr or c:txPr when a c:tx is present causes the "can't
        # save" bug on bubble charts. Remove c:spPr and c:txPr when present.
        dLbl._remove_spPr()
        dLbl._remove_txPr()

        return dLbl.get_or_add_rich()

    def _get_or_add_tx_rich(self):
        """
        Return the `c:tx` element for this data label, with its `c:rich`
        child and descendants, newly created if not yet present.
        """
        dLbl = self._get_or_add_dLbl()

        # having a c:spPr or c:txPr when a c:tx is present causes the "can't
        # save" bug on bubble charts. Remove c:spPr and c:txPr when present.
        dLbl._remove_spPr()
        dLbl._remove_txPr()

        return dLbl.get_or_add_tx_rich()

    def _get_or_add_txPr(self):
        """Return the `c:txPr` element for this data label.

        The `c:txPr` element and its parent `c:dLbl` element are created if
        not yet present.
        """
        dLbl = self._get_or_add_dLbl()
        return dLbl.get_or_add_txPr()

    def _remove_tx_rich(self):
        """
        Remove any `c:tx/c:rich` child of the `c:dLbl` element for this data
        label. Do nothing if that element is not present.
        """
        dLbl = self._dLbl
        if dLbl is None:
            return
        dLbl.remove_tx_rich()
