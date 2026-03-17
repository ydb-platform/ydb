from datetime import date

from .utils import (
    len_color,
    streams
)
from .shortcuts import progressbar

from .widgets import (
    Timer,
    ETA,
    AdaptiveETA,
    AbsoluteETA,
    DataSize,
    FileTransferSpeed,
    AdaptiveTransferSpeed,
    AnimatedMarker,
    Counter,
    Percentage,
    FormatLabel,
    SimpleProgress,
    Bar,
    ReverseBar,
    BouncingBar,
    RotatingMarker,
    VariableMixin,
    MultiRangeBar,
    MultiProgressBar,
    GranularBar,
    FormatLabelBar,
    PercentageLabelBar,
    Variable,
    DynamicMessage,
    FormatCustomText,
    CurrentTime
)

from .bar import (
    ProgressBar,
    DataTransferBar,
    NullBar,
)
from .base import UnknownLength


from .__about__ import (
    __author__,
    __version__,
)

__date__ = str(date.today())
__all__ = [
    'progressbar',
    'len_color',
    'streams',
    'Timer',
    'ETA',
    'AdaptiveETA',
    'AbsoluteETA',
    'DataSize',
    'FileTransferSpeed',
    'AdaptiveTransferSpeed',
    'AnimatedMarker',
    'Counter',
    'Percentage',
    'FormatLabel',
    'SimpleProgress',
    'Bar',
    'ReverseBar',
    'BouncingBar',
    'UnknownLength',
    'ProgressBar',
    'DataTransferBar',
    'RotatingMarker',
    'VariableMixin',
    'MultiRangeBar',
    'MultiProgressBar',
    'GranularBar',
    'FormatLabelBar',
    'PercentageLabelBar',
    'Variable',
    'DynamicMessage',
    'FormatCustomText',
    'CurrentTime',
    'NullBar',
    '__author__',
    '__version__',
]
