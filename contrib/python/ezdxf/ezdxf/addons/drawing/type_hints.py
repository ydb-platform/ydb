# Copyright (c) 2020-2022, Matthew Broadway
# License: MIT License
from typing import Callable
from typing_extensions import TypeAlias
from ezdxf.entities import DXFGraphic

LayerName: TypeAlias = str
Color: TypeAlias = str
Radians: TypeAlias = float
FilterFunc: TypeAlias = Callable[[DXFGraphic], bool]
