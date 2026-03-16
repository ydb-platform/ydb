import numpy as np

from .typed import Dict, List


def convex_decomposition(mesh, **kwargs) -> List[Dict]:
    """
    Compute an approximate convex decomposition of a mesh.

    VHACD Parameters which can be passed as kwargs:

    Name                              Default
    -----------------------------------------
    maxConvexHulls                    64
    resolution                        400000
    minimumVolumePercentErrorAllowed  1.0
    maxRecursionDepth                 10
    shrinkWrap                        True
    fillMode                          "flood"
    maxNumVerticesPerCH               64
    asyncACD                          True
    minEdgeLength                     2
    findBestPlane                     False

    Parameters
    ----------
    mesh : trimesh.Trimesh
      Mesh to be decomposed into convex parts
    **kwargs : VHACD keyword arguments

    Returns
    -------
    mesh_args : list
      List of **kwargs for Trimeshes that are nearly
      convex and approximate the original.
    """
    from vhacdx import compute_vhacd

    # the faces are triangulated in a (len(face), ...vertex-index)
    # for vtkPolyData
    # i.e. so if shaped to four columns the first column is all 3
    faces = (
        np.column_stack((np.ones(len(mesh.faces), dtype=np.int64) * 3, mesh.faces))
        .ravel()
        .astype(np.uint32)
    )

    return [
        {"vertices": v, "faces": f}
        for v, f in compute_vhacd(mesh.vertices, faces, **kwargs)
    ]
