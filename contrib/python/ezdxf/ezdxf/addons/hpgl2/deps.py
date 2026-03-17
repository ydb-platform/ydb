#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
#
# Central import location of frontend dependencies.
# To extract te hpgl2 add-on from the ezdxf package, the following tools have to be
# implemented or extracted too. The dependencies of the implemented backends are not
# listed here.

from ezdxf.math import Vec2, ConstructionCircle, BoundingBox2d, Bezier4P, AnyVec, Matrix44
from ezdxf.path import Path, bbox as path_bbox, transform_paths
from ezdxf.tools.standards import PAGE_SIZES
from ezdxf import colors
NULLVEC2 = Vec2(0, 0)
