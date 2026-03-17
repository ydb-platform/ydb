"""
https://github.com/mikedh/trimesh
------------------------------------

Trimesh is a pure Python (2.7- 3.3+) library for loading and using triangular
meshes with an emphasis on watertight meshes. The goal of the library is to
provide a fully featured Trimesh object which allows for easy manipulation
and analysis, in the style of the Polygon object in the Shapely library.
"""

# avoid a circular import in trimesh.base
from . import (
    boolean,
    bounds,
    caching,
    collision,
    comparison,
    convex,
    creation,
    curvature,
    decomposition,
    exceptions,
    geometry,
    graph,
    grouping,
    inertia,
    intersections,
    iteration,
    nsphere,
    permutate,
    poses,
    primitives,
    proximity,
    ray,
    registration,
    remesh,
    repair,
    sample,
    smoothing,
    transformations,
    triangles,
    units,
    util,
)
from .base import Trimesh

# general numeric tolerances
from .constants import tol

# loader functions
from .exchange.load import (
    available_formats,
    load,
    load_mesh,
    load_path,
    load_remote,
    load_scene,
)

# geometry objects
from .parent import Geometry
from .points import PointCloud
from .scene.scene import Scene
from .transformations import transform_points

# utility functions
from .util import unitize
from .version import __version__

try:
    # handle vector paths
    from . import path
except BaseException as E:
    # raise a useful error if path hasn't loaded
    path = exceptions.ExceptionWrapper(E)


try:
    from . import voxel
except BaseException as E:
    # requires non-minimal imports
    voxel = exceptions.ExceptionWrapper(E)


__all__ = [
    "Geometry",
    "PointCloud",
    "Scene",
    "Trimesh",
    "__version__",
    "available_formats",
    "boolean",
    "bounds",
    "caching",
    "collision",
    "comparison",
    "convex",
    "creation",
    "curvature",
    "decomposition",
    "exceptions",
    "geometry",
    "graph",
    "grouping",
    "inertia",
    "intersections",
    "iteration",
    "load",
    "load_mesh",
    "load_path",
    "load_remote",
    "load_scene",
    "nsphere",
    "path",
    "permutate",
    "poses",
    "primitives",
    "proximity",
    "ray",
    "registration",
    "remesh",
    "repair",
    "sample",
    "smoothing",
    "tol",
    "transform_points",
    "transformations",
    "triangles",
    "unitize",
    "units",
    "util",
    "voxel",
]
