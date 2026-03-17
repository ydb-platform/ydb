"""
boolean.py
-------------

Do boolean operations on meshes using either Blender or Manifold.
"""

import numpy as np

from . import interfaces
from .exceptions import ExceptionWrapper
from .iteration import reduce_cascade
from .typed import BooleanEngineType, BooleanOperationType, Callable, Dict, Sequence

try:
    from manifold3d import Manifold, Mesh
except BaseException as E:
    Mesh = ExceptionWrapper(E)
    Manifold = ExceptionWrapper(E)


def difference(
    meshes: Sequence,
    engine: BooleanEngineType = None,
    check_volume: bool = True,
    **kwargs,
):
    """
    Compute the boolean difference between a mesh an n other meshes.

    Parameters
    ----------
    meshes : sequence of trimesh.Trimesh
      Meshes to be processed.
    engine
      Which backend to use, i.e. 'blender' or 'manifold'
    check_volume
      Raise an error if not all meshes are watertight
      positive volumes. Advanced users may want to ignore
      this check as it is expensive.
    kwargs
      Passed through to the `engine`.

    Returns
    ----------
    difference
      A `Trimesh` that contains `meshes[0] - meshes[1:]`
    """

    return _engines[engine](
        meshes, operation="difference", check_volume=check_volume, **kwargs
    )


def union(
    meshes: Sequence,
    engine: BooleanEngineType = None,
    check_volume: bool = True,
    **kwargs,
):
    """
    Compute the boolean union between a mesh an n other meshes.

    Parameters
    ----------
    meshes : list of trimesh.Trimesh
      Meshes to be processed
    engine : str
      Which backend to use, i.e. 'blender' or 'manifold'
    check_volume
      Raise an error if not all meshes are watertight
      positive volumes. Advanced users may want to ignore
      this check as it is expensive.
    kwargs
      Passed through to the `engine`.

    Returns
    ----------
    union
      A `Trimesh` that contains the union of all passed meshes.
    """
    return _engines[engine](
        meshes, operation="union", check_volume=check_volume, **kwargs
    )


def intersection(
    meshes: Sequence,
    engine: BooleanEngineType = None,
    check_volume: bool = True,
    **kwargs,
):
    """
    Compute the boolean intersection between a mesh and other meshes.

    Parameters
    ----------
    meshes : list of trimesh.Trimesh
      Meshes to be processed
    engine : str
      Which backend to use, i.e. 'blender' or 'manifold'
    check_volume
      Raise an error if not all meshes are watertight
      positive volumes. Advanced users may want to ignore
      this check as it is expensive.
    kwargs
      Passed through to the `engine`.

    Returns
    ----------
    intersection
      A `Trimesh` that contains the intersection geometry.
    """
    return _engines[engine](
        meshes, operation="intersection", check_volume=check_volume, **kwargs
    )


def boolean_manifold(
    meshes: Sequence,
    operation: BooleanOperationType,
    check_volume: bool = True,
    **kwargs,
):
    """
    Run an operation on a set of meshes using the Manifold engine.

    Parameters
    ----------
    meshes : list of trimesh.Trimesh
      Meshes to be processed
    operation
      Which boolean operation to do.
    check_volume
      Raise an error if not all meshes are watertight
      positive volumes. Advanced users may want to ignore
      this check as it is expensive.
    kwargs
      Passed through to the `engine`.
    """
    if check_volume and not all(m.is_volume for m in meshes):
        raise ValueError("Not all meshes are volumes!")

    # Convert to manifold meshes
    manifolds = [
        Manifold(
            mesh=Mesh(
                vert_properties=np.array(mesh.vertices, dtype=np.float32),
                tri_verts=np.array(mesh.faces, dtype=np.uint32),
            )
        )
        for mesh in meshes
    ]

    # Perform operations
    if operation == "difference":
        if len(meshes) < 2:
            raise ValueError("Difference only defined over two meshes.")
        elif len(meshes) == 2:
            # apply the single difference
            result_manifold = manifolds[0] - manifolds[1]
        elif len(meshes) > 2:
            # union all the meshes to be subtracted from the final result
            unioned = reduce_cascade(lambda a, b: a + b, manifolds[1:])
            # apply the difference
            result_manifold = manifolds[0] - unioned
    elif operation == "union":
        result_manifold = reduce_cascade(lambda a, b: a + b, manifolds)
    elif operation == "intersection":
        result_manifold = reduce_cascade(lambda a, b: a ^ b, manifolds)
    else:
        raise ValueError(f"Invalid boolean operation: '{operation}'")

    # Convert back to trimesh meshes
    from . import Trimesh

    result_mesh = result_manifold.to_mesh()

    return Trimesh(
        vertices=result_mesh.vert_properties, faces=result_mesh.tri_verts, process=False
    )


# which backend boolean engines do we have
_engines: Dict[str, Callable] = {}

if isinstance(Manifold, ExceptionWrapper):
    # manifold isn't available so use the import error
    _engines["manifold"] = Manifold
else:
    # manifold3d is the preferred option
    _engines["manifold"] = boolean_manifold


if interfaces.blender.exists:
    # we have `blender` in the path which  we can call with subprocess
    _engines["blender"] = interfaces.blender.boolean
else:
    # failing that add a helpful error message
    _engines["blender"] = ExceptionWrapper(ImportError("`blender` is not in `PATH`"))

# pick the first value that isn't an ExceptionWrapper.
_engines[None] = next(
    (v for v in _engines.values() if not isinstance(v, ExceptionWrapper)),
    ExceptionWrapper(
        ImportError("No boolean backend: `pip install manifold3d` or install `blender`")
    ),
)

engines_available = {
    k for k, v in _engines.items() if not isinstance(v, ExceptionWrapper)
}
