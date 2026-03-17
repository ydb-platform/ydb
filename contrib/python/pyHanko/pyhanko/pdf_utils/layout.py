"""Layout utilities (to be expanded)"""

import enum
import logging
from dataclasses import dataclass
from fractions import Fraction
from typing import Optional, Union

from pyhanko.config.api import ConfigurableMixin
from pyhanko.config.errors import ConfigurationError

__all__ = [
    'LayoutError',
    'BoxSpecificationError',
    'BoxConstraints',
    'AxisAlignment',
    'Margins',
    'InnerScaling',
    'SimpleBoxLayoutRule',
    'Positioning',
]

logger = logging.getLogger(__name__)


class LayoutError(ValueError):
    """Indicates an error in a layout computation."""

    def __init__(self, msg: str, *args):
        self.msg = msg
        super().__init__(msg, *args)


class BoxSpecificationError(LayoutError):
    """Raised when a box constraint is over/underspecified."""

    def __init__(self, msg: Optional[str] = None):
        super().__init__(msg=msg or "box constraint is over/underspecified")


class BoxConstraints:
    """Represents a box of potentially variable width and height.
    Among other uses, this can be leveraged to produce a variably sized
    box with a fixed aspect ratio.

    If width/height are not defined yet, they can be set by assigning to the
    :attr:`width` and :attr:`height` attributes.
    """

    _width: Optional[int]
    _height: Optional[int]
    _ar: Optional[Fraction]
    _fully_specified: bool

    def __init__(
        self,
        width: Union[int, float, None] = None,
        height: Union[int, float, None] = None,
        aspect_ratio: Optional[Fraction] = None,
    ):
        int_width = int(width) if width is not None else None
        int_height = int(height) if height is not None else None
        self._width = int_width
        self._height = int_height

        fully_specified = False

        self._ar = None
        if int_width is None and int_height is None and aspect_ratio is None:
            return
        elif int_width is not None and int_height is not None:
            if aspect_ratio is not None:
                raise BoxSpecificationError  # overspecified
            self._ar = Fraction(int_width, int_height)
            fully_specified = True
        elif aspect_ratio is not None:
            self._ar = aspect_ratio
            if int_height is not None:
                self._width = int(round(int_height * aspect_ratio))
            elif int_width is not None:
                self._height = int(round(int_width / aspect_ratio))

        self._fully_specified = fully_specified

    def _recalculate(self):
        if self._width is not None and self._height is not None:
            self._ar = Fraction(self._width, self._height)
            self._fully_specified = True
        elif self._ar is not None:
            if self._height is not None:
                self._width = int(self._height * self._ar)
                self._fully_specified = True
            elif self._width is not None:
                self._height = int(self._width / self._ar)
                self._fully_specified = True

    @property
    def width(self) -> int:
        """
        :return:
            The width of the box.
        :raises BoxSpecificationError:
            if the box's width could not be determined.
        """
        if self._width is not None:
            return self._width
        else:
            raise BoxSpecificationError

    @width.setter
    def width(self, width):
        if self._width is None:
            self._width = width
            self._recalculate()
        else:
            raise BoxSpecificationError

    @property
    def width_defined(self) -> bool:
        """
        :return:
            ``True`` if the box currently has a well-defined width,
            ``False`` otherwise.
        """
        return self._width is not None

    @property
    def height(self) -> int:
        """
        :return:
            The height of the box.
        :raises BoxSpecificationError:
            if the box's height could not be determined.
        """
        if self._height is not None:
            return self._height
        else:
            raise BoxSpecificationError

    @height.setter
    def height(self, height):
        if self._height is None:
            self._height = height
            self._recalculate()
        else:
            raise BoxSpecificationError

    @property
    def height_defined(self) -> bool:
        """
        :return:
            ``True`` if the box currently has a well-defined height,
            ``False`` otherwise.
        """
        return self._height is not None

    @property
    def aspect_ratio(self) -> Fraction:
        """
        :return:
            The aspect ratio of the box.
        :raises BoxSpecificationError:
            if the box's aspect ratio could not be determined.
        """
        if self._ar is not None:
            return self._ar
        else:
            raise BoxSpecificationError

    @property
    def aspect_ratio_defined(self) -> bool:
        """
        :return:
            ``True`` if the box currently has a well-defined aspect ratio,
            ``False`` otherwise.
        """
        return self._ar is not None


class InnerScaling(enum.Enum):
    """Class representing a scaling convention."""

    NO_SCALING = enum.auto()
    """Never scale content."""

    STRETCH_FILL = enum.auto()
    """Scale content to fill the entire container."""

    STRETCH_TO_FIT = enum.auto()
    """
    Scale content while preserving aspect ratio until either the maximal 
    width or maximal height is reached.
    """

    SHRINK_TO_FIT = enum.auto()
    """
    Scale content down to fit in the container, while preserving the original
    aspect ratio.
    """

    @classmethod
    def from_config(cls, config_str: str) -> 'InnerScaling':
        """
        Convert from a configuration string.

        :param config_str:
            A string: 'none', 'stretch-fill', 'stretch-to-fit', 'shrink-to-fit'
        :return:
            An :class:`.InnerScaling` value.
        :raise ConfigurationError: on unexpected string inputs.
        """
        try:
            return {
                'none': InnerScaling.NO_SCALING,
                'stretch-fill': InnerScaling.STRETCH_FILL,
                'stretch-to-fit': InnerScaling.STRETCH_TO_FIT,
                'shrink-to-fit': InnerScaling.SHRINK_TO_FIT,
            }[config_str.lower()]
        except KeyError:
            raise ConfigurationError(
                f"'{config_str}' is not a valid inner scaling setting; valid "
                f"values are 'none', 'stretch-fill', 'stretch-to-fit', "
                f"'shrink-to-fit'."
            )


class AxisAlignment(enum.Enum):
    """Class representing one-dimensional alignment along an axis."""

    ALIGN_MIN = enum.auto()
    """
    Align maximally towards the negative end of the axis.
    """

    ALIGN_MID = enum.auto()
    """
    Center content along the axis.
    """

    ALIGN_MAX = enum.auto()
    """
    Align maximally towards the positive end of the axis.
    """

    @classmethod
    def from_x_align(cls, align_str: str) -> 'AxisAlignment':
        """
        Convert from a horizontal alignment config string.

        :param align_str:
            A string: 'left', 'mid' or 'right'.
        :return:
            An :class:`.AxisAlignment` value.
        :raise ConfigurationError: on unexpected string inputs.
        """
        try:
            return {
                'left': AxisAlignment.ALIGN_MIN,
                'mid': AxisAlignment.ALIGN_MID,
                'right': AxisAlignment.ALIGN_MAX,
            }[align_str.lower()]
        except KeyError:
            raise ConfigurationError(
                f"'{align_str}' is not a valid horizontal alignment; valid "
                f"values are 'left', 'mid', 'right'."
            )

    @classmethod
    def from_y_align(cls, align_str: str) -> 'AxisAlignment':
        """
        Convert from a vertical alignment config string.

        :param align_str:
            A string: 'bottom', 'mid' or 'top'.
        :return:
            An :class:`.AxisAlignment` value.
        :raise ConfigurationError: on unexpected string inputs.
        """
        try:
            return {
                'bottom': AxisAlignment.ALIGN_MIN,
                'mid': AxisAlignment.ALIGN_MID,
                'top': AxisAlignment.ALIGN_MAX,
            }[align_str.lower()]
        except KeyError:
            raise ConfigurationError(
                f"'{align_str}' is not a valid vertical alignment; valid "
                f"values are 'bottom', 'mid', 'top'."
            )

    @property
    def flipped(self):
        return _alignment_opposites[self]

    def align(
        self, container_len: int, inner_len: int, pre_margin, post_margin
    ) -> int:
        effective_max_len = Margins.effective(
            'length', container_len, pre_margin, post_margin
        )

        if self == AxisAlignment.ALIGN_MAX:
            # we want to start as far up the axis as possible.
            # Ignoring margins, that would be at container_len - inner_len
            # This computation makes sure that there's room for post_margin
            # in the back.
            return container_len - inner_len - post_margin
        elif self == AxisAlignment.ALIGN_MIN:
            return pre_margin
        elif inner_len > effective_max_len:
            logger.warning(
                f"Content box width/height {inner_len} is too wide for "
                f"container size {container_len} with margins "
                f"({pre_margin}, {post_margin}); post_margin will be ignored"
            )
            return pre_margin
        elif self == AxisAlignment.ALIGN_MID:
            # we'll center the inner content *within* the margins
            inner_offset = (effective_max_len - inner_len) // 2
            return pre_margin + inner_offset
        raise TypeError


# Class variables in enums are weird, so let's put this here
_alignment_opposites = {
    AxisAlignment.ALIGN_MID: AxisAlignment.ALIGN_MID,
    AxisAlignment.ALIGN_MIN: AxisAlignment.ALIGN_MAX,
    AxisAlignment.ALIGN_MAX: AxisAlignment.ALIGN_MIN,
}


@dataclass(frozen=True)
class Positioning(ConfigurableMixin):
    """
    Class describing the position and scaling of an object in a container.
    """

    x_pos: int
    """Horizontal coordinate"""

    y_pos: int
    """Vertical coordinate"""

    x_scale: float
    """Horizontal scaling"""

    y_scale: float
    """Vertical scaling"""

    def as_cm(self):
        """
        Convenience method to convert this :class:`.Positioning` into a PDF
        ``cm`` operator.

        :return:
            A byte string representing the ``cm`` operator corresponding
            to this :class:`.Positioning`.
        """
        return b'%g 0 0 %g %g %g cm' % (
            self.x_scale,
            self.y_scale,
            self.x_pos,
            self.y_pos,
        )


def _aln_width(
    alignment: AxisAlignment,
    container_box: BoxConstraints,
    inner_nat_width: int,
    pre_margin: int,
    post_margin: int,
):
    if container_box.width_defined:
        return alignment.align(
            container_box.width, inner_nat_width, pre_margin, post_margin
        )
    else:
        container_box.width = inner_nat_width + pre_margin + post_margin
        return pre_margin


def _aln_height(
    alignment: AxisAlignment,
    container_box: BoxConstraints,
    inner_nat_height: int,
    pre_margin: int,
    post_margin: int,
):
    if container_box.height_defined:
        return alignment.align(
            container_box.height, inner_nat_height, pre_margin, post_margin
        )
    else:
        container_box.height = inner_nat_height + pre_margin + post_margin
        return pre_margin


@dataclass(frozen=True)
class Margins(ConfigurableMixin):
    """Class describing a set of margins."""

    left: int = 0

    right: int = 0

    top: int = 0

    bottom: int = 0

    @classmethod
    def uniform(cls, num):
        """
        Return a set of uniform margins.

        :param num:
            The uniform margin to apply to all four sides.
        :return:
            ``Margins(num, num, num, num)``
        """
        return Margins(num, num, num, num)

    @staticmethod
    def effective(dim_name, container_len, pre, post):
        """Internal helper method to compute effective margins."""
        eff = container_len - pre - post
        if eff < 0:
            raise LayoutError(
                f"Margins ({pre}, {post}) too wide for container "
                f"{dim_name} {container_len}."
            )
        return eff

    def effective_width(self, width):
        """
        Compute width without margins.

        :param width:
            The container width.
        :return:
            The width after subtracting the left and right margins.
        :raises LayoutError:
            if the container width is too short to accommodate the margins.
        """
        return Margins.effective('width', width, self.left, self.right)

    def effective_height(self, height):
        """
        Compute height without margins.

        :param height:
            The container height.
        :return:
            The height after subtracting the top and bottom margins.
        :raises LayoutError:
            if the container height is too short to accommodate the margins.
        """
        return Margins.effective('height', height, self.bottom, self.top)

    @classmethod
    def from_config(cls, config_dict):
        # convenience
        if isinstance(config_dict, list):
            config_dict = dict(
                zip(("left", "right", "top", "bottom"), config_dict)
            )
        return super().from_config(config_dict)


@dataclass(frozen=True)
class SimpleBoxLayoutRule(ConfigurableMixin):
    """
    Class describing alignment, scaling and margin rules for a box
    positioned inside another box.
    """

    x_align: AxisAlignment
    """
    Horizontal alignment settings.
    """

    y_align: AxisAlignment
    """
    Vertical alignment settings.
    """

    margins: Margins = Margins()
    """
    Container (inner) margins. Defaults to all zeroes.
    """

    inner_content_scaling: InnerScaling = InnerScaling.SHRINK_TO_FIT
    """
    Inner content scaling rule.
    """

    @classmethod
    def process_entries(cls, config_dict):
        # in config processing, we default to MID for everything
        x_align = config_dict.get('x_align', AxisAlignment.ALIGN_MID)
        if isinstance(x_align, str):
            x_align = AxisAlignment.from_x_align(x_align)
        config_dict['x_align'] = x_align

        y_align = config_dict.get('y_align', AxisAlignment.ALIGN_MID)
        if isinstance(y_align, str):
            y_align = AxisAlignment.from_y_align(y_align)
        config_dict['y_align'] = y_align

        scaling = config_dict.get('inner_content_scaling', None)
        if scaling is not None:
            config_dict['inner_content_scaling'] = InnerScaling.from_config(
                scaling
            )

    def substitute_margins(self, new_margins: Margins) -> 'SimpleBoxLayoutRule':
        return SimpleBoxLayoutRule(
            x_align=self.x_align,
            y_align=self.y_align,
            margins=new_margins,
            inner_content_scaling=self.inner_content_scaling,
        )

    def fit(
        self,
        container_box: BoxConstraints,
        inner_nat_width: int,
        inner_nat_height: int,
    ) -> Positioning:
        """
        Position and possibly scale a box within a container, according
        to this layout rule.

        :param container_box:
            :class:`.BoxConstraints` describing the container.
        :param inner_nat_width:
            The inner box's natural width.
        :param inner_nat_height:
            The inner box's natural height.
        :return:
            A :class:`.Positioning` describing the scaling & position of the
            lower left corner of the inner box.
        """

        margins = self.margins
        scaling = self.inner_content_scaling
        x_scale = y_scale = 1
        if (
            scaling != InnerScaling.NO_SCALING
            and container_box.width_defined
            and container_box.height_defined
        ):
            eff_width = margins.effective_width(container_box.width)
            eff_height = margins.effective_height(container_box.height)

            x_scale = (
                (eff_width / inner_nat_width) if inner_nat_width != 0 else 1
            )
            y_scale = (
                (eff_height / inner_nat_height) if inner_nat_height != 0 else 1
            )
            if scaling == InnerScaling.STRETCH_TO_FIT:
                x_scale = y_scale = min(x_scale, y_scale)
            elif scaling == InnerScaling.SHRINK_TO_FIT:
                # same as stretch to fit, with the additional stipulation
                # that it can't scale up, only down.
                x_scale = y_scale = min(x_scale, y_scale, 1)

        x_pos = _aln_width(
            self.x_align,
            container_box,
            inner_nat_width * x_scale,
            margins.left,
            margins.right,
        )
        y_pos = _aln_height(
            self.y_align,
            container_box,
            inner_nat_height * y_scale,
            margins.bottom,
            margins.top,
        )
        return Positioning(
            x_pos=x_pos, y_pos=y_pos, x_scale=x_scale, y_scale=y_scale
        )
