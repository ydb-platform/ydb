"""
convex.py

Deal with creating and checking convex objects in 2, 3 and N dimensions.

Convex is defined as:
1) "Convex, meaning "curving out" or "extending outward" (compare to concave)
2) having an outline or surface curved like the exterior of a circle or sphere.
3) (of a polygon) having only interior angles measuring less than 180
"""

from dataclasses import dataclass, fields

import numpy as np

from . import triangles, util
from .constants import tol
from .parent import Geometry3D
from .typed import NDArray, Union

try:
    from scipy.spatial import ConvexHull
except ImportError as E:
    from .exceptions import ExceptionWrapper

    ConvexHull = ExceptionWrapper(E)

try:
    from scipy.spatial import QhullError
except BaseException:
    QhullError = BaseException


@dataclass
class QhullOptions:
    """
    A helper class for constructing correct Qhull option strings.
    More details available at: http://www.qhull.org/html/qh-quick.htm#options

    Currently only includes the boolean flag options, which is most of them.

    Parameters
    -----------
    Qa
      Allow input with fewer or more points than coordinates
    Qc
      Keep coplanar points with nearest facet
    Qi
      Keep interior points with nearest facet.
    QJ
      Joggled input to avoid precision problems
    Qt
      Triangulated output.
    Qu
      Compute upper hull for furthest-site Delaunay triangulation
    Qw
      Allow warnings about Qhull options
    Qbb
      Scale last coordinate to [0,m] for Delaunay
    Qs
      Search all points for the initial simplex
    Qv
      Test vertex neighbors for convexity
    Qx
      Exact pre-merges (allows coplanar facets)
    Qz
      Add a point-at-infinity for Delaunay triangulations
    QbB
      Scale input to fit the unit cube
    QR0
      Random rotation (n=seed, n=0 time, n=-1 time/no rotate)
    Qg
      only build good facets (needs 'QGn', 'QVn', or 'Pdk')
    Pp
      Do not print statistics about precision problems and remove
      some of the warnings including the narrow hull warning.
    """

    Qa: bool = False
    """ Allow input with fewer or more points than coordinates"""

    Qc: bool = False
    """ Keep coplanar points with nearest facet"""

    Qi: bool = False
    """ Keep interior points with nearest facet. """

    QJ: bool = False
    """ Joggled input to avoid precision problems """

    Qt: bool = False
    """ Triangulated output. """

    Qu: bool = False
    """ Compute upper hull for furthest-site Delaunay triangulation """

    Qw: bool = False
    """ Allow warnings about Qhull options """

    # Precision handling
    Qbb: bool = False
    """ Scale last coordinate to [0,m] for Delaunay """

    Qs: bool = False
    """ Search all points for the initial simplex """

    Qv: bool = False
    """ Test vertex neighbors for convexity """

    Qx: bool = False
    """ Exact pre-merges (allows coplanar facets)  """

    Qz: bool = False
    """ Add a point-at-infinity for Delaunay triangulations """

    QbB: bool = False
    """ Scale input to fit the unit cube """

    QR0: bool = False
    """ Random rotation (n=seed, n=0 time, n=-1 time/no rotate) """

    # Select facets
    Qg: bool = False
    """ Only build good facets (needs 'QGn', 'QVn', or 'Pdk') """

    Pp: bool = False
    """ Do not print statistics about precision problems and remove
      some of the warnings including the narrow hull warning. """

    # TODO : not included non-boolean options
    # QBk: Optional[Floating] = None
    # """ Scale coord[k] to upper bound of n (default 0.5) """

    # Qbk: Optional[Floating] = None
    # """ Scale coord[k] to low bound of n (default -0.5) """

    # Qbk:0Bk:0
    # """ drop dimension k from input """

    # QGn
    #    good facet if visible from point n, -n for not visible

    # QVn
    #    good facet if it includes point n, -n if not

    def __str__(self) -> str:
        """
        Construct the `qhull_options` string used by `scipy.spatial`
        objects and functions.

        Returns
        ----------
        qhull_options
          Can be passed to `scipy.spatial.[ConvexHull,Delaunay,Voronoi]`
        """
        return " ".join(f.name for f in fields(self) if getattr(self, f.name))


QHULL_DEFAULT = QhullOptions(QbB=True, Pp=True, Qt=True)


def convex_hull(
    obj: Union[Geometry3D, NDArray],
    qhull_options: Union[QhullOptions, str, None] = QHULL_DEFAULT,
    repair: bool = True,
) -> "trimesh.Trimesh":  # noqa: F821
    """
    Get a new Trimesh object representing the convex hull of the
    current mesh attempting to return a watertight mesh with correct
    normals.

    Arguments
    --------
    obj
      Mesh or `(n, 3)` points.
    qhull_options
      Options to pass to qhull.

    Returns
    --------
    convex
      Mesh of convex hull.
    """
    # would be a circular import at the module level
    from .base import Trimesh

    # compose the
    if qhull_options is None:
        qhull_str = None
    elif isinstance(qhull_options, QhullOptions):
        # use the __str__ method to compose this options string
        qhull_str = str(qhull_options)
    elif isinstance(qhull_options, str):
        qhull_str = qhull_options
    else:
        raise TypeError(type(qhull_options))

    if hasattr(obj, "vertices"):
        points = obj.vertices.view(np.ndarray)
    else:
        # will remove subclassing
        points = np.asarray(obj, dtype=np.float64)
        if not util.is_shape(points, (-1, 3)):
            raise ValueError("Object must be Trimesh or (n,3) points!")

    try:
        hull = ConvexHull(points, qhull_options=qhull_str)
    except QhullError:
        util.log.debug("Failed to compute convex hull: retrying with `QJ`", exc_info=True)
        # try with "joggle" enabled
        hull = ConvexHull(points, qhull_options="QJ")

    # hull object doesn't remove unreferenced vertices
    # create a mask to re- index faces for only referenced vertices
    vid = np.sort(hull.vertices)
    mask = np.zeros(len(hull.points), dtype=np.int64)
    mask[vid] = np.arange(len(vid))
    # remove unreferenced vertices here
    faces = mask[hull.simplices].copy()
    # rescale vertices back to original size
    vertices = hull.points[vid].copy()

    if not repair:
        # create the Trimesh object for the convex hull
        return Trimesh(vertices=vertices, faces=faces, process=True, validate=False)

    # qhull returns faces with random winding
    # calculate the returned normal of each face
    crosses = triangles.cross(vertices[faces])

    # qhull returns zero magnitude faces like an asshole
    normals, valid = util.unitize(crosses, check_valid=True)

    # remove zero magnitude faces
    faces = faces[valid]
    crosses = crosses[valid]

    # each triangle area and mean center
    triangles_area = triangles.area(crosses=crosses)
    triangles_center = vertices[faces].mean(axis=1)

    # since the convex hull is (hopefully) convex, the vector from
    # the centroid to the center of each face
    # should have a positive dot product with the normal of that face
    # if it doesn't it is probably backwards
    # note that this sometimes gets screwed up by precision issues
    centroid = np.average(triangles_center, weights=triangles_area, axis=0)
    # a vector from the centroid to a point on each face
    test_vector = triangles_center - centroid
    # check the projection against face normals
    backwards = util.diagonal_dot(normals, test_vector) < 0.0

    # flip the winding outward facing
    faces[backwards] = np.fliplr(faces[backwards])
    # flip the normal
    normals[backwards] *= -1.0

    # save the work we did to the cache so it doesn't have to be recomputed
    initial_cache = {
        "triangles_cross": crosses,
        "triangles_center": triangles_center,
        "area_faces": triangles_area,
        "centroid": centroid,
    }

    # create the Trimesh object for the convex hull
    convex = Trimesh(
        vertices=vertices,
        faces=faces,
        face_normals=normals,
        initial_cache=initial_cache,
        process=True,
        validate=False,
    )

    # we did the gross case above, but sometimes precision issues
    # leave some faces backwards anyway
    # this call will exit early if the winding is consistent
    # and if not will fix it by traversing the adjacency graph
    convex.fix_normals(multibody=False)

    # sometimes the QbB option will cause precision issues
    # so try the hull again without it and
    # check for qhull_options is None to avoid infinite recursion
    if qhull_options is None and not convex.is_winding_consistent:
        return convex_hull(convex, qhull_options=None)

    return convex


def adjacency_projections(mesh):
    """
    Test if a mesh is convex by projecting the vertices of
    a triangle onto the normal of its adjacent face.

    Parameters
    ----------
    mesh : Trimesh
      Input geometry

    Returns
    ----------
    projection : (len(mesh.face_adjacency),) float
      Distance of projection of adjacent vertex onto plane
    """
    # normals and origins from the first column of face adjacency
    normals = mesh.face_normals[mesh.face_adjacency[:, 0]]
    # one of the vertices on the shared edge
    origins = mesh.vertices[mesh.face_adjacency_edges[:, 0]]

    # faces from the second column of face adjacency
    vid_other = mesh.face_adjacency_unshared[:, 1]
    vector_other = mesh.vertices[vid_other] - origins

    # get the projection with a dot product
    dots = util.diagonal_dot(vector_other, normals)

    return dots


def is_convex(mesh):
    """
    Check if a mesh is convex.

    Parameters
    -----------
    mesh : Trimesh
      Input geometry

    Returns
    -----------
    convex : bool
      Was passed mesh convex or not
    """
    # non-watertight meshes are not convex
    # meshes with multiple bodies are not convex
    if not mesh.is_watertight or mesh.body_count != 1:
        return False

    # don't consider zero- area faces
    nonzero = mesh.area_faces > tol.zero
    # adjacencies with two nonzero faces
    adj_ok = nonzero[mesh.face_adjacency].all(axis=1)

    # if none of our face pairs are both nonzero exit
    # TODO : is this the correct check?
    # or should we just compare the projections
    # to the mesh scale
    if not adj_ok.any():
        return False

    # make threshold of convexity scale- relative
    threshold = tol.planar * mesh.scale

    # if projections of vertex onto plane of adjacent
    # face is negative, it means the face pair is locally
    # convex, and if that is true for all faces the mesh is convex
    convex = bool(mesh.face_adjacency_projections[adj_ok].max() < threshold)

    return convex


def hull_points(obj, qhull_options="QbB Pp"):
    """
    Try to extract a convex set of points from multiple input formats.

    Details on qhull options:
      http://www.qhull.org/html/qh-quick.htm#options

    Parameters
    ---------
    obj: Trimesh object
         (n,d) points
         (m,) Trimesh objects

    Returns
    --------
    points: (o,d) convex set of points
    """
    if hasattr(obj, "convex_hull"):
        return obj.convex_hull.vertices

    initial = np.asanyarray(obj, dtype=np.float64)
    if len(initial.shape) != 2:
        raise ValueError("points must be (n, dimension)!")
    hull = ConvexHull(initial, qhull_options=qhull_options)
    points = hull.points[hull.vertices]

    return points
