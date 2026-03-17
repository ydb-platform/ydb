# Copyright (c) 2018-2022, Manfred Moitzi
# License: MIT License
""" ezdxf typing hints

Only usable in type checking mode:

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.eztypes import GenericLayoutType

Tips for Type Imports
---------------------

Import Drawing class:

    from ezdxf.document import Drawing

Import DXF entities from ezdxf.entities:

    from ezdxf.entities import Line, Point, ...

Import layouts from ezdxf.layouts:

   from ezdxf.layouts import BaseLayout, Layout, Modelspace, Paperspace, BlockLayout

Import math tools from ezdxf.math:

    from ezdxf.math import Vec2, Vec3, Matrix44, ...

Import path tools from ezdxf.path:

    from ezdxf.path import Path, make_path, ...

"""
from __future__ import annotations
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    Iterable,
    List,
    Sequence,
    TYPE_CHECKING,
    Tuple,
    Union,
)
from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from ezdxf.entities import DXFEntity
    from ezdxf.layouts.base import VirtualLayout
    from ezdxf.layouts.blocklayout import BlockLayout
    from ezdxf.layouts.layout import Layout
    from ezdxf.lldxf.extendedtags import ExtendedTags
    from ezdxf.lldxf.tags import Tags
    from ezdxf.math import UVec

    IterableTags: TypeAlias = Iterable[Tuple[int, Any]]
    SectionDict: TypeAlias = Dict[str, List[Union[Tags, ExtendedTags]]]
    KeyFunc: TypeAlias = Callable[[DXFEntity], Hashable]
    FaceType: TypeAlias = Sequence[UVec]
    GenericLayoutType: TypeAlias = Union[Layout, BlockLayout, VirtualLayout]
