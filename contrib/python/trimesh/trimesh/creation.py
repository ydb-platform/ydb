"""
creation.py
--------------

Create meshes from primitives, or with operations.
"""

import collections
import warnings

import numpy as np

from . import exceptions, grouping, triangles, util
from . import transformations as tf
from .base import Trimesh
from .constants import log, tol
from .geometry import align_vectors, faces_to_edges, plane_transform
from .resources import get_json
from .typed import ArrayLike, Dict, Integer, NDArray, Number, Optional, Tuple

try:
    # shapely is a soft dependency
    from shapely.geometry import Polygon
    from shapely.wkb import loads as load_wkb
except BaseException as E:
    # re-raise the exception when someone tries
    # to use the module that they don't have
    Polygon = exceptions.ExceptionWrapper(E)
    load_wkb = exceptions.ExceptionWrapper(E)

# get stored values for simple box and icosahedron primitives
_data = get_json("creation.json")
# check available triangulation engines without importing them
_engines = [
    ("earcut", util.has_module("mapbox_earcut")),
    ("manifold", util.has_module("manifold3d")),
    ("triangle", util.has_module("triangle")),
]


def revolve(
    linestring: ArrayLike,
    angle: Optional[Number] = None,
    cap: bool = False,
    sections: Optional[Integer] = None,
    transform: Optional[ArrayLike] = None,
    **kwargs,
) -> Trimesh:
    """
    Revolve a 2D line string around the 2D Y axis, with a result with
    the 2D Y axis pointing along the 3D Z axis.

    This function is intended to handle the complexity of indexing
    and is intended to be used to create all radially symmetric primitives,
    eventually including cylinders, annular cylinders, capsules, cones,
    and UV spheres.

    Note that if your linestring is closed, it needs to be counterclockwise
    if you would like face winding and normals facing outwards.

    Parameters
    -------------
    linestring : (n, 2) float
      Lines in 2D which will be revolved
    angle
      Angle in radians to revolve curve by or if not
      passed will be a full revolution (`angle = 2*pi`)
    cap
      If not a full revolution (`0.0 < angle < 2 * pi`)
      and cap is True attempt to add a tessellated cap.
    sections
      Number of sections result should have
      If not specified default is 32 per revolution
    transform : None or (4, 4) float
      Transform to apply to mesh after construction
    **kwargs : dict
      Passed to Trimesh constructor

    Returns
    --------------
    revolved : Trimesh
      Mesh representing revolved result
    """
    linestring = np.asanyarray(linestring, dtype=np.float64)

    # linestring must be ordered 2D points
    if len(linestring.shape) != 2 or linestring.shape[1] != 2:
        raise ValueError("linestring must be 2D!")

    if angle is None:
        # default to closing the revolution
        angle = np.pi * 2.0
        closed = True
    else:
        # check passed angle value
        closed = util.isclose(angle, np.pi * 2, atol=1e-10)

    if sections is None:
        # default to 32 sections for a full revolution
        sections = int(angle / (np.pi * 2) * 32)

    # change to face count
    sections += 1
    # create equally spaced angles
    theta = np.linspace(0, angle, sections)

    # 2D points around the revolution
    points = np.column_stack((np.cos(theta), np.sin(theta)))

    # how many points per slice
    per = len(linestring)

    # use the 2D X component as radius
    radius = linestring[:, 0]
    # use the 2D Y component as the height along revolution
    height = linestring[:, 1]
    # a lot of tiling to get our 3D vertices
    vertices = np.column_stack(
        (
            np.tile(points, (1, per)).reshape((-1, 2))
            * np.tile(radius, len(points)).reshape((-1, 1)),
            np.tile(height, len(points)),
        )
    )

    if closed:
        # should be a duplicate set of vertices
        if tol.strict:
            assert util.allclose(vertices[:per], vertices[-per:], atol=1e-8)

        # chop off duplicate vertices
        vertices = vertices[:-per]

    # how many slices of the pie
    slices = len(theta) - 1

    # start with a quad for every segment
    # this is a superset which will then be reduced
    quad = np.array([0, per, 1, 1, per, per + 1])
    # stack the faces for a single slice of the revolution
    single = np.tile(quad, per - 1).reshape((-1, 3))
    # `per` is basically the stride of the vertices
    single += np.tile(np.arange(per - 1), (2, 1)).T.reshape((-1, 1))
    # remove any zero-area triangle
    # this covers many cases without having to think too much
    single = single[triangles.area(vertices[single]) > tol.merge]

    # how much to offset each slice
    # note arange multiplied by vertex stride
    # but tiled by the number of faces we actually have
    offset = np.tile(np.arange(slices) * per, (len(single), 1)).T.reshape((-1, 1))
    # stack a single slice into N slices
    stacked = np.tile(single.ravel(), slices).reshape((-1, 3))

    if tol.strict:
        # make sure we didn't screw up stacking operation
        assert np.allclose(stacked.reshape((-1, single.shape[0], 3)) - single, 0)

    # offset stacked and wrap vertices
    faces = (stacked + offset) % len(vertices)

    # Handle capping before applying any transformation
    if not closed and cap:
        # Use the triangulated linestring as the base cap faces (cap_0), assuming no new vertices
        # are added, indices defining triangles of cap_0 should be reusable for cap_angle
        cap_0_vertices, cap_0_faces = triangulate_polygon(
            Polygon(linestring), force_vertices=True
        )

        if tol.strict:
            # make sure we didn't screw up triangulation
            unique = grouping.unique_rows(cap_0_vertices)[0]
            assert set(unique) == set(range(len(linestring))), (
                "Triangulation added vertices!"
            )

        # Use the last set of vertices as the top cap contour (cap_angle)
        offset = len(vertices) - per
        cap_angle_faces = cap_0_faces + offset
        flipped_cap_angle_faces = np.fliplr(cap_angle_faces)  # reverse the winding

        # Append cap faces to the face array
        faces = np.vstack([faces, cap_0_faces, flipped_cap_angle_faces])

    if transform is not None:
        # apply transform to vertices
        vertices = tf.transform_points(vertices, transform)

    # create the mesh from our vertices and faces
    mesh = Trimesh(vertices=vertices, faces=faces, **kwargs)

    # strict checks run only in unit tests and when cap is True
    if tol.strict and (
        np.allclose(radius[[0, -1]], 0.0) or np.allclose(linestring[0], linestring[-1])
    ):
        if closed or cap:
            # if revolved curve starts and ends with zero radius
            # it should really be a valid volume, unless the sign
            # reversed on the input linestring
            assert mesh.is_volume
        assert mesh.body_count == 1

    return mesh


def extrude_polygon(
    polygon: "Polygon",
    height: Number,
    transform: Optional[ArrayLike] = None,
    mid_plane: bool = False,
    **kwargs,
) -> Trimesh:
    """
    Extrude a 2D shapely polygon into a 3D mesh

    Parameters
    ----------
    polygon : shapely.geometry.Polygon
      2D geometry to extrude
    height : float
      Distance to extrude polygon along Z
    transform : None or (4, 4) float
      Transform to apply to mesh after construction
    triangle_args : str or None
      Passed to triangle
    **kwargs : dict
      Passed to `triangulate_polygon`

    Returns
    ----------
    mesh : trimesh.Trimesh
      Resulting extrusion as watertight body
    """
    # create a triangulation from the polygon
    vertices, faces = triangulate_polygon(polygon, **kwargs)

    if mid_plane:
        translation = np.eye(4)
        translation[2, 3] = abs(float(height)) / -2.0
        if transform is None:
            transform = translation
        else:
            transform = np.dot(transform, translation)

    # extrude that triangulation along Z
    mesh = extrude_triangulation(
        vertices=vertices, faces=faces, height=height, transform=transform, **kwargs
    )
    return mesh


def sweep_polygon(
    polygon: "Polygon",
    path: ArrayLike,
    angles: Optional[ArrayLike] = None,
    cap: bool = True,
    connect: bool = True,
    kwargs: Optional[Dict] = None,
    **triangulation,
) -> Trimesh:
    """
    Extrude a 2D polygon into a 3D mesh along a 3D path. Note that this
    does *not* handle the case where there is very sharp curvature leading
    the polygon to intersect the plane of a previous slice, and does *not*
    scale the polygon along the induced normal to result in a constant cross section.

    You may want to resample your path with a B-spline, i.e:
      `trimesh.path.simplify.resample_spline(path, smooth=0.2, count=100)`

    Parameters
    ----------
    polygon : shapely.geometry.Polygon
      Profile to sweep along path
    path : (n, 3) float
      A path in 3D
    angles : (n,) float
      Optional rotation angle relative to prior vertex
      at each vertex.
    cap
      If an open path is passed apply a cap to both ends.
    connect
      If a closed path is passed connect the sweep into
      a single watertight mesh.
    kwargs : dict
      Passed to the mesh constructor.
    **triangulation
      Passed to `triangulate_polygon`, i.e. `engine='triangle'`

    Returns
    -------
    mesh : trimesh.Trimesh
      Geometry of result
    """

    path = np.asanyarray(path, dtype=np.float64)
    if not util.is_shape(path, (-1, 3)):
        raise ValueError("Path must be (n, 3)!")

    if angles is not None:
        angles = np.asanyarray(angles, dtype=np.float64)
        if angles.shape != (len(path),):
            raise ValueError(angles.shape)
    else:
        # set all angles to zero
        angles = np.zeros(len(path), dtype=np.float64)

    # check to see if path is closed i.e. first and last vertex are the same
    closed = np.linalg.norm(path[0] - path[-1]) < tol.merge
    # Extract 2D vertices and triangulation
    vertices_2D, faces_2D = triangulate_polygon(polygon, **triangulation)

    # stack the `(n, 3)` faces into `(3 * n, 2)` edges
    edges = faces_to_edges(faces_2D)
    # edges which only occur once are on the boundary of the polygon
    # since the triangulation may have subdivided the boundary of the
    # shapely polygon, we need to find it again
    edges_unique = grouping.group_rows(np.sort(edges, axis=1), require_count=1)
    # subset the vertices to only ones included in the boundary
    unique, inverse = np.unique(edges[edges_unique].reshape(-1), return_inverse=True)
    # take only the vertices in the boundary
    # and stack them with zeros and ones so we can use dot
    # products to transform them all over the place
    vertices_tf = np.column_stack(
        (vertices_2D[unique], np.zeros(len(unique)), np.ones(len(unique)))
    )
    # the indices of vertices_tf
    boundary = inverse.reshape((-1, 2))

    # now create the normals for the plane each slice will lie on
    # consider the simple path with 3 vertices and therefore 2 vectors:
    # - the first plane will be exactly along the first vector
    # - the second plane will be the average of the two vectors
    # - the last plane will be exactly along the last vector
    # and each plane origin will be the corresponding vertex on the path
    vector = util.unitize(path[1:] - path[:-1])
    # unitize instead of / 2 as they may be degenerate / zero
    vector_mean = util.unitize(vector[1:] + vector[:-1])
    # collect the vectors into plane normals
    normal = np.concatenate([[vector[0]], vector_mean, [vector[-1]]], axis=0)

    if closed and connect:
        # if we have a closed loop average the first and last planes
        normal[0] = util.unitize(normal[[0, -1]].mean(axis=0))

    # planes should have one unit normal and one vertex each
    assert normal.shape == path.shape

    # get the spherical coordinates for the normal vectors
    theta, phi = util.vector_to_spherical(normal).T

    # collect the trig values into numpy arrays we can compose into matrices
    cos_theta, sin_theta = np.cos(theta), np.sin(theta)
    cos_phi, sin_phi = np.cos(phi), np.sin(phi)
    cos_roll, sin_roll = np.cos(angles), np.sin(angles)

    # we want a rotation which will be the identity for a Z+ vector
    # this was constructed and unrolled from the following sympy block
    # theta, phi, roll = sp.symbols("theta phi roll")
    # matrix = (
    #     tf.rotation_matrix(roll, [0, 0, 1]) @
    #     tf.rotation_matrix(phi, [1, 0, 0]) @
    #     tf.rotation_matrix((sp.pi / 2) - theta, [0, 0, 1])
    # ).inv()
    # matrix.simplify()

    # shorthand for stacking
    zeros = np.zeros(len(theta))
    ones = np.ones(len(theta))

    # stack initially as one unrolled matrix per row
    transforms = np.column_stack(
        [
            -sin_roll * cos_phi * cos_theta + sin_theta * cos_roll,
            sin_roll * sin_theta + cos_phi * cos_roll * cos_theta,
            sin_phi * cos_theta,
            path[:, 0],
            -sin_roll * sin_theta * cos_phi - cos_roll * cos_theta,
            -sin_roll * cos_theta + sin_theta * cos_phi * cos_roll,
            sin_phi * sin_theta,
            path[:, 1],
            sin_phi * sin_roll,
            -sin_phi * cos_roll,
            cos_phi,
            path[:, 2],
            zeros,
            zeros,
            zeros,
            ones,
        ]
    ).reshape((-1, 4, 4))

    if tol.strict:
        # make sure that each transform moves the Z+ vector to the requested normal
        for n, matrix in zip(normal, transforms):
            check = tf.transform_points([[0.0, 0.0, 1.0]], matrix, translate=False)[0]
            assert np.allclose(check, n)

    # apply transforms to prebaked homogeneous coordinates
    vertices_3D = np.concatenate(
        [np.dot(vertices_tf, matrix.T) for matrix in transforms], axis=0
    )[:, :3]

    # now construct the faces with one group of boundary faces per slice
    stride = len(unique)
    boundary_next = boundary + stride
    faces_slice = np.column_stack(
        [boundary, boundary_next[:, :1], boundary_next[:, ::-1], boundary[:, 1:]]
    ).reshape((-1, 3))

    # offset the slices
    faces = [faces_slice + offset for offset in np.arange(len(path) - 1) * stride]

    # connect only applies to closed paths
    if closed and connect:
        # the last slice will not be required
        max_vertex = (len(path) - 1) * stride
        # clip off the duplicated vertices
        vertices_3D = vertices_3D[:max_vertex]
        # apply the modulus in-place to a conservative subset
        faces[-1] %= max_vertex
    elif cap:
        # these are indices of `vertices_2D` that were not on the boundary
        # which can happen for triangulation algorithms that added vertices
        # we don't currently support that but you could append the unconsumed
        # vertices and then update the mapping below to reflect that
        unconsumed = set(unique).difference(faces_2D.ravel())
        if len(unconsumed) > 0:
            raise NotImplementedError("triangulation added vertices: no logic to cap!")

        # map the 2D faces to the order we used
        mapped = np.zeros(unique.max() + 2, dtype=np.int64)
        mapped[unique] = np.arange(len(unique))

        # now should correspond to the first vertex block
        cap_zero = mapped[faces_2D]
        # winding will be along +Z so flip for the bottom cap
        faces.append(np.fliplr(cap_zero))
        # offset the end cap
        faces.append(cap_zero + stride * (len(path) - 1))

    if kwargs is None:
        kwargs = {}

    if "process" not in kwargs:
        # we should be constructing clean meshes here
        # so we don't need to run an expensive verex merge
        kwargs["process"] = False

    # stack the faces used
    faces = np.concatenate(faces, axis=0)

    # generate the mesh from the face data
    mesh = Trimesh(vertices=vertices_3D, faces=faces, **kwargs)

    if tol.strict:
        # we should not have included any unused vertices
        assert len(np.unique(faces)) == len(vertices_3D)

        if cap:
            # mesh should always be a volume if cap is true
            assert mesh.is_volume

        if closed and connect:
            assert mesh.is_volume
            assert mesh.body_count == 1

    return mesh


def _cross_2d(a: NDArray, b: NDArray) -> NDArray:
    """
    Numpy 2.0 depreciated cross products of 2D arrays.
    """
    return a[:, 0] * b[:, 1] - a[:, 1] * b[:, 0]


def extrude_triangulation(
    vertices: ArrayLike,
    faces: ArrayLike,
    height: Number,
    transform: Optional[ArrayLike] = None,
    **kwargs,
) -> Trimesh:
    """
    Extrude a 2D triangulation into a watertight mesh.

    Parameters
    ----------
    vertices : (n, 2) float
      2D vertices
    faces : (m, 3) int
      Triangle indexes of vertices
    height : float
      Distance to extrude triangulation
    transform : None or (4, 4) float
      Transform to apply to mesh after construction
    **kwargs : dict
      Passed to Trimesh constructor

    Returns
    ---------
    mesh : trimesh.Trimesh
      Mesh created from extrusion
    """
    vertices = np.asanyarray(vertices, dtype=np.float64)
    height = float(height)
    faces = np.asanyarray(faces, dtype=np.int64)

    if not util.is_shape(vertices, (-1, 2)):
        raise ValueError("Vertices must be (n,2)")
    if not util.is_shape(faces, (-1, 3)):
        raise ValueError("Faces must be (n,3)")
    if np.abs(height) < tol.merge:
        raise ValueError("Height must be nonzero!")

    # check the winding of the first few triangles
    signs = _cross_2d(
        np.subtract(*vertices[faces[:10, :2].T]), np.subtract(*vertices[faces[:10, 1:].T])
    )

    # make sure the triangulation is aligned with the sign of
    # the height we've been passed
    if len(signs) > 0 and np.sign(signs.mean()) != np.sign(height):
        faces = np.fliplr(faces)

    # stack the (n,3) faces into (3*n, 2) edges
    edges = faces_to_edges(faces)
    edges_sorted = np.sort(edges, axis=1)
    # edges which only occur once are on the boundary of the polygon
    # since the triangulation may have subdivided the boundary of the
    # shapely polygon, we need to find it again
    edges_unique = grouping.group_rows(edges_sorted, require_count=1)

    # (n, 2, 2) set of line segments (positions, not references)
    boundary = vertices[edges[edges_unique]]

    # we are creating two vertical  triangles for every 2D line segment
    # on the boundary of the 2D triangulation
    vertical = np.tile(boundary.reshape((-1, 2)), 2).reshape((-1, 2))
    vertical = np.column_stack((vertical, np.tile([0, height, 0, height], len(boundary))))
    vertical_faces = np.tile([3, 1, 2, 2, 1, 0], (len(boundary), 1))
    vertical_faces += np.arange(len(boundary)).reshape((-1, 1)) * 4
    vertical_faces = vertical_faces.reshape((-1, 3))

    # stack the (n,2) vertices with zeros to make them (n, 3)
    vertices_3D = util.stack_3D(vertices)

    # a sequence of zero- indexed faces, which will then be appended
    # with offsets to create the final mesh
    faces_seq = [faces[:, ::-1], faces.copy(), vertical_faces]
    vertices_seq = [vertices_3D, vertices_3D.copy() + [0.0, 0, height], vertical]

    # append sequences into flat nicely indexed arrays
    vertices, faces = util.append_faces(vertices_seq, faces_seq)
    if transform is not None:
        # apply transform here to avoid later bookkeeping
        vertices = tf.transform_points(vertices, transform)
        # if the transform flips the winding flip faces back
        # so that the normals will be facing outwards
        if tf.flips_winding(transform):
            # fliplr makes arrays non-contiguous
            faces = np.ascontiguousarray(np.fliplr(faces))
    # create mesh object with passed keywords
    mesh = Trimesh(vertices=vertices, faces=faces, **kwargs)
    # only check in strict mode (unit tests)
    if tol.strict:
        assert mesh.volume > 0.0

    return mesh


def triangulate_polygon(
    polygon,
    triangle_args: Optional[str] = None,
    engine: Optional[str] = None,
    force_vertices: bool = False,
    **kwargs,
) -> Tuple[NDArray[np.float64], NDArray[np.int64]]:
    """
    Given a shapely polygon create a triangulation using a
    python interface to the permissively licensed `mapbox-earcut`
    or the more robust `triangle.c`.
    > pip install manifold3d
    > pip install triangle
    > pip install mapbox_earcut

    Parameters
    ---------
    polygon : Shapely.geometry.Polygon
        Polygon object to be triangulated.
    triangle_args
        Passed to triangle.triangulate i.e: 'p', 'pq30', 'pY'="don't insert vert"
    engine
      None or 'earcut' will use earcut, 'triangle' will use triangle
    force_vertices
      Many operations can't handle new vertices being inserted, so this will
      attempt to generate a triangulation without new vertices and raise a
      ValueError if it is unable to do so.

    Returns
    --------------
    vertices : (n, 2) float
       Points in space
    faces : (n, 3) int
       Index of vertices that make up triangles
    """

    if engine is None:
        # try getting the first engine that is installed
        engine = next((name for name, exists in _engines if exists), None)

    if polygon is None or polygon.is_empty:
        return [], []

    vertices = None

    if engine == "earcut":
        from mapbox_earcut import triangulate_float64

        # get vertices as sequence where exterior
        # is the first value
        vertices = [np.array(polygon.exterior.coords)]
        vertices.extend(np.array(i.coords) for i in polygon.interiors)
        # record the index from the length of each vertex array
        rings = np.cumsum([len(v) for v in vertices])
        # stack vertices into (n, 2) float array
        vertices = np.vstack(vertices)
        # run triangulation
        faces = (
            triangulate_float64(vertices, rings)
            .reshape((-1, 3))
            .astype(np.int64)
            .reshape((-1, 3))
        )

    elif engine == "manifold":
        import manifold3d

        # the outer ring is wound counter-clockwise
        rings = [
            np.array(polygon.exterior.coords)[:: (1 if polygon.exterior.is_ccw else -1)][
                :-1
            ]
        ]
        # wind interiors
        rings.extend(
            np.array(b.coords)[:: (-1 if b.is_ccw else 1)][:-1] for b in polygon.interiors
        )
        faces = manifold3d.triangulate(rings).astype(np.int64)
        vertices = np.vstack(rings, dtype=np.float64)

    elif engine == "triangle":
        from triangle import triangulate

        # set default triangulation arguments if not specified
        if triangle_args is None:
            triangle_args = "p"
            # turn the polygon in to vertices, segments, and holes
        arg = _polygon_to_kwargs(polygon)
        # run the triangulation
        blob = triangulate(arg, triangle_args)
        vertices, faces = blob["vertices"], blob["triangles"].astype(np.int64)

        # triangle may insert vertices
        if force_vertices:
            assert np.allclose(arg["vertices"], vertices)

    if vertices is None:
        log.warning(
            "try running `pip install mapbox-earcut manifold3d`"
            + "or `triangle`, `mapbox_earcut`, then explicitly pass:\n"
            + '`triangulate_polygon(*args, engine="triangle")`\n'
            + "to use the non-FSF-approved-license triangle engine"
        )
        raise ValueError("No available triangulation engine!")

    return vertices, faces


def _polygon_to_kwargs(polygon) -> Dict:
    """
    Given a shapely polygon generate the data to pass to
    the triangle mesh generator

    Parameters
    ---------
    polygon : Shapely.geometry.Polygon
      Input geometry

    Returns
    --------
    result : dict
      Has keys: vertices, segments, holes
    """

    if not polygon.is_valid:
        raise ValueError("invalid shapely polygon passed!")

    def round_trip(start, length):
        """
        Given a start index and length, create a series of (n, 2) edges which
        create a closed traversal.

        Examples
        ---------
        start, length = 0, 3
        returns:  [(0,1), (1,2), (2,0)]
        """
        tiled = np.tile(np.arange(start, start + length).reshape((-1, 1)), 2)
        tiled = tiled.reshape(-1)[1:-1].reshape((-1, 2))
        tiled = np.vstack((tiled, [tiled[-1][-1], tiled[0][0]]))
        return tiled

    def add_boundary(boundary, start):
        # coords is an (n, 2) ordered list of points on the polygon boundary
        # the first and last points are the same, and there are no
        # guarantees on points not being duplicated (which will
        # later cause meshpy/triangle to shit a brick)
        coords = np.array(boundary.coords)
        # find indices points which occur only once, and sort them
        # to maintain order
        unique = np.sort(grouping.unique_rows(coords)[0])
        cleaned = coords[unique]

        vertices.append(cleaned)
        facets.append(round_trip(start, len(cleaned)))

        # holes require points inside the region of the hole, which we find
        # by creating a polygon from the cleaned boundary region, and then
        # using a representative point. You could do things like take the mean of
        # the points, but this is more robust (to things like concavity), if
        # slower.
        test = Polygon(cleaned)
        holes.append(np.array(test.representative_point().coords)[0])

        return len(cleaned)

    # sequence of (n,2) points in space
    vertices = collections.deque()
    # sequence of (n,2) indices of vertices
    facets = collections.deque()
    # list of (2) vertices in interior of hole regions
    holes = collections.deque()

    start = add_boundary(polygon.exterior, 0)
    for interior in polygon.interiors:
        try:
            start += add_boundary(interior, start)
        except BaseException:
            log.warning("invalid interior, continuing")
            continue

    # create clean (n,2) float array of vertices
    # and (m, 2) int array of facets
    # by stacking the sequence of (p,2) arrays
    vertices = np.vstack(vertices)
    facets = np.vstack(facets).tolist()
    # shapely polygons can include a Z component
    # strip it out for the triangulation
    if vertices.shape[1] == 3:
        vertices = vertices[:, :2]
    result = {"vertices": vertices, "segments": facets}
    # holes in meshpy lingo are a (h, 2) list of (x,y) points
    # which are inside the region of the hole
    # we added a hole for the exterior, which we slice away here
    holes = np.array(holes)[1:]
    if len(holes) > 0:
        result["holes"] = holes
    return result


def box(
    extents: Optional[ArrayLike] = None,
    transform: Optional[ArrayLike] = None,
    bounds: Optional[ArrayLike] = None,
    **kwargs,
):
    """
    Return a cuboid.

    Parameters
    ------------
    extents : (3,) float
      Edge lengths
    transform: (4, 4) float
      Transformation matrix
    bounds : None or (2, 3) float
      Corners of AABB, overrides extents and transform.
    **kwargs:
        passed to Trimesh to create box

    Returns
    ------------
    geometry : trimesh.Trimesh
      Mesh of a cuboid
    """
    # vertices of the cube from reference
    vertices = np.array(_data["box"]["vertices"], order="C", dtype=np.float64)
    faces = np.array(_data["box"]["faces"], order="C", dtype=np.int64)
    face_normals = np.array(_data["box"]["face_normals"], order="C", dtype=np.float64)

    # resize cube based on passed extents
    if bounds is not None:
        if transform is not None or extents is not None:
            raise ValueError("`bounds` overrides `extents`/`transform`!")
        bounds = np.array(bounds, dtype=np.float64)
        if bounds.shape != (2, 3):
            raise ValueError("`bounds` must be (2, 3) float!")
        extents = np.ptp(bounds, axis=0)
        vertices *= extents
        vertices += bounds[0]
    elif extents is not None:
        extents = np.asanyarray(extents, dtype=np.float64)
        if extents.shape != (3,):
            raise ValueError("Extents must be (3,)!")
        vertices -= 0.5
        vertices *= extents
    else:
        vertices -= 0.5
        extents = np.asarray((1.0, 1.0, 1.0), dtype=np.float64)

    if "metadata" not in kwargs:
        kwargs["metadata"] = {}
    kwargs["metadata"].update({"shape": "box", "extents": extents})

    box = Trimesh(
        vertices=vertices, faces=faces, face_normals=face_normals, process=False, **kwargs
    )

    # do the transform here to preserve face normals
    if transform is not None:
        box.apply_transform(transform)

    return box


def icosahedron(**kwargs) -> Trimesh:
    """
    Create an icosahedron, one of the platonic solids which is has 20 faces.

    Parameters
    ------------
    kwargs : dict
      Passed through to `Trimesh` constructor.

    Returns
    -------------
    ico : trimesh.Trimesh
      Icosahederon centered at the origin.
    """
    # get stored pre-baked primitive values
    vertices = np.array(_data["icosahedron"]["vertices"], dtype=np.float64)
    faces = np.array(_data["icosahedron"]["faces"], dtype=np.int64)
    return Trimesh(
        vertices=vertices, faces=faces, process=kwargs.pop("process", False), **kwargs
    )


def icosphere(subdivisions: Integer = 3, radius: Number = 1.0, **kwargs):
    """
    Create an icosphere centered at the origin.

    Parameters
    ----------
    subdivisions : int
      How many times to subdivide the mesh.
      Note that the number of faces will grow as function of
      4 ** subdivisions, so you probably want to keep this under ~5
    radius : float
      Desired radius of sphere
    kwargs : dict
      Passed through to `Trimesh` constructor.

    Returns
    ---------
    ico : trimesh.Trimesh
      Meshed sphere
    """
    radius = float(radius)
    subdivisions = int(subdivisions)

    ico = icosahedron()
    ico._validate = False

    for _ in range(subdivisions):
        ico = ico.subdivide()
        vectors = ico.vertices
        scalar = np.sqrt(np.dot(vectors**2, [1, 1, 1]))
        unit = vectors / scalar.reshape((-1, 1))
        ico.vertices += unit * (radius - scalar).reshape((-1, 1))

    # if we didn't subdivide we still need to refine the radius
    if subdivisions <= 0:
        vectors = ico.vertices
        scalar = np.sqrt(np.dot(vectors**2, [1, 1, 1]))
        unit = vectors / scalar.reshape((-1, 1))
        ico.vertices += unit * (radius - scalar).reshape((-1, 1))

    if "color" in kwargs:
        warnings.warn(
            "`icosphere(color=...)` is deprecated and will "
            + "be removed in June 2024: replace with Trimesh constructor "
            + "kewyword argument `icosphere(face_colors=...)`",
            category=DeprecationWarning,
            stacklevel=2,
        )
        kwargs["face_colors"] = kwargs.pop("color")

    return Trimesh(
        vertices=ico.vertices,
        faces=ico.faces,
        metadata={"shape": "sphere", "radius": radius},
        process=kwargs.pop("process", False),
        **kwargs,
    )


def uv_sphere(
    radius: Number = 1.0,
    count: Optional[ArrayLike] = None,
    transform: Optional[ArrayLike] = None,
    **kwargs,
) -> Trimesh:
    """
    Create a UV sphere (latitude + longitude) centered at the
    origin. Roughly one order of magnitude faster than an
    icosphere but slightly uglier.

    Parameters
    ----------
    radius : float
      Radius of sphere
    count : (2,) int
      Number of latitude and longitude lines
    transform : None or (4, 4) float
      Transform to apply to mesh after construction
    kwargs : dict
      Passed thgrough
    Returns
    ----------
    mesh : trimesh.Trimesh
       Mesh of UV sphere with specified parameters
    """

    # set the resolution of the uv sphere
    if count is None:
        count = np.array([32, 64], dtype=np.int64)
    else:
        count = np.array(count, dtype=np.int64)
        count += np.mod(count, 2)
        count[1] *= 2

    # generate the 2D curve for the UV sphere
    theta = np.linspace(0.0, np.pi, num=count[0])
    linestring = np.column_stack((np.sin(theta), -np.cos(theta))) * radius

    # revolve the curve to create a volume
    return revolve(
        linestring=linestring,
        sections=count[1],
        transform=transform,
        metadata={"shape": "sphere", "radius": radius},
        **kwargs,
    )


def capsule(
    height: Number = 1.0,
    radius: Number = 1.0,
    count: Optional[ArrayLike] = None,
    transform: Optional[ArrayLike] = None,
    **kwargs,
) -> Trimesh:
    """
    Create a mesh of a capsule, or a cylinder with hemispheric ends.

    Parameters
    ----------
    height : float
      Center to center distance of two spheres
    radius : float
      Radius of the cylinder and hemispheres
    count : (2,) int
      Number of sections on latitude and longitude
    transform : None or (4, 4) float
      Transform to apply to mesh after construction
    Returns
    ----------
    capsule : trimesh.Trimesh
      Capsule geometry with:
        - cylinder axis is along Z
        - one hemisphere is centered at the origin
        - other hemisphere is centered along the Z axis at height
    """
    if count is None:
        count = np.array([32, 64], dtype=np.int64)
    else:
        count = np.array(count, dtype=np.int64)
    count += np.mod(count, 2)

    height = abs(float(height))
    radius = abs(float(radius))

    # create a half circle
    theta = np.linspace(-np.pi / 2.0, np.pi / 2.0, count[0])
    linestring = np.column_stack((np.cos(theta), np.sin(theta))) * radius

    # offset the top and bottom by half the height
    half = len(linestring) // 2
    linestring[:half][:, 1] -= height / 2.0
    linestring[half:][:, 1] += height / 2.0

    return revolve(
        linestring,
        sections=count[1],
        transform=transform,
        metadata={"shape": "capsule", "height": height, "radius": radius},
        **kwargs,
    )


def cone(
    radius: Number,
    height: Number,
    sections: Optional[Integer] = None,
    transform: Optional[ArrayLike] = None,
    **kwargs,
) -> Trimesh:
    """
    Create a mesh of a cone along Z centered at the origin.

    Parameters
    ----------
    radius : float
      The radius of the cone at the widest part.
    height : float
      The height of the cone.
    sections : int or None
      How many pie wedges per revolution
    transform : (4, 4) float or None
      Transform to apply after creation
    **kwargs : dict
      Passed to Trimesh constructor

    Returns
    ----------
    cone: trimesh.Trimesh
      Resulting mesh of a cone
    """
    # create the 2D outline of a cone
    linestring = [[0, 0], [radius, 0], [0, height]]
    # revolve the profile to create a cone
    if "metadata" not in kwargs:
        kwargs["metadata"] = {}
    kwargs["metadata"].update({"shape": "cone", "radius": radius, "height": height})
    cone = revolve(
        linestring=linestring, sections=sections, transform=transform, **kwargs
    )

    return cone


def cylinder(
    radius: Number,
    height: Optional[Number] = None,
    sections: Optional[Integer] = None,
    segment: Optional[ArrayLike] = None,
    transform: Optional[ArrayLike] = None,
    **kwargs,
):
    """
    Create a mesh of a cylinder along Z centered at the origin.

    Parameters
    ----------
    radius : float
      The radius of the cylinder
    height : float or None
      The height of the cylinder, or None if `segment` has been passed.
    sections : int or None
      How many pie wedges should the cylinder have
    segment : (2, 3) float
      Endpoints of axis, overrides transform and height
    transform : None or (4, 4) float
      Transform to apply to mesh after construction
    **kwargs:
        passed to Trimesh to create cylinder

    Returns
    ----------
    cylinder: trimesh.Trimesh
      Resulting mesh of a cylinder
    """

    if segment is not None:
        # override transform and height with the segment
        transform, height = _segment_to_cylinder(segment=segment)

    if height is None:
        raise ValueError("either `height` or `segment` must be passed!")

    half = abs(float(height)) / 2.0
    # create a profile to revolve
    linestring = [[0, -half], [radius, -half], [radius, half], [0, half]]
    if "metadata" not in kwargs:
        kwargs["metadata"] = {}
    kwargs["metadata"].update({"shape": "cylinder", "height": height, "radius": radius})
    # generate cylinder through simple revolution
    return revolve(
        linestring=linestring, sections=sections, transform=transform, **kwargs
    )


def annulus(
    r_min: Number,
    r_max: Number,
    height: Optional[Number] = None,
    sections: Optional[Integer] = None,
    transform: Optional[ArrayLike] = None,
    segment: Optional[ArrayLike] = None,
    **kwargs,
):
    """
    Create a mesh of an annular cylinder along Z centered at the origin.

    Parameters
    ----------
    r_min : float
      The inner radius of the annular cylinder
    r_max : float
      The outer radius of the annular cylinder
    height : float
      The height of the annular cylinder
    sections : int or None
      How many pie wedges should the annular cylinder have
    transform : (4, 4) float or None
      Transform to apply to move result from the origin
    segment : None or (2, 3) float
      Override transform and height with a line segment
    **kwargs:
        passed to Trimesh to create annulus

    Returns
    ----------
    annulus : trimesh.Trimesh
      Mesh of annular cylinder
    """
    if segment is not None:
        # override transform and height with the segment if passed
        transform, height = _segment_to_cylinder(segment=segment)

    if height is None:
        raise ValueError("either `height` or `segment` must be passed!")

    r_min = abs(float(r_min))
    # if center radius is zero this is a cylinder
    if r_min < tol.merge:
        return cylinder(
            radius=r_max, height=height, sections=sections, transform=transform, **kwargs
        )
    r_max = abs(float(r_max))
    # we're going to center at XY plane so take half the height
    half = abs(float(height)) / 2.0
    # create counter-clockwise rectangle
    linestring = [
        [r_min, -half],
        [r_max, -half],
        [r_max, half],
        [r_min, half],
        [r_min, -half],
    ]

    if "metadata" not in kwargs:
        kwargs["metadata"] = {}
    kwargs["metadata"].update(
        {"shape": "annulus", "r_min": r_min, "r_max": r_max, "height": height}
    )

    # revolve the curve
    annulus = revolve(
        linestring=linestring, sections=sections, transform=transform, **kwargs
    )

    return annulus


def _segment_to_cylinder(segment: ArrayLike):
    """
    Convert a line segment to a transform and height for a cylinder
    or cylinder-like primitive.

    Parameters
    -----------
    segment : (2, 3) float
      3D line segment in space

    Returns
    -----------
    transform : (4, 4) float
      Matrix to move a Z-extruded origin cylinder to segment
    height : float
      The height of the cylinder needed
    """
    segment = np.asanyarray(segment, dtype=np.float64)
    if segment.shape != (2, 3):
        raise ValueError("segment must be 2 3D points!")
    vector = segment[1] - segment[0]
    # override height with segment length
    height = np.linalg.norm(vector)
    # point in middle of line
    midpoint = segment[0] + (vector * 0.5)
    # align Z with our desired direction
    rotation = align_vectors([0, 0, 1], vector)
    # translate to midpoint of segment
    translation = tf.translation_matrix(midpoint)
    # compound the rotation and translation
    transform = np.dot(translation, rotation)
    return transform, height


def random_soup(face_count: Integer = 100):
    """
    Return random triangles as a Trimesh

    Parameters
    -----------
    face_count : int
      Number of faces desired in mesh

    Returns
    -----------
    soup : trimesh.Trimesh
      Geometry with face_count random faces
    """
    vertices = np.random.random((face_count * 3, 3)) - 0.5
    faces = np.arange(face_count * 3).reshape((-1, 3))
    soup = Trimesh(vertices=vertices, faces=faces)
    return soup


def axis(
    origin_size: Number = 0.04,
    transform: Optional[ArrayLike] = None,
    origin_color: Optional[ArrayLike] = None,
    axis_radius: Optional[Number] = None,
    axis_length: Optional[Number] = None,
):
    """
    Return an XYZ axis marker as a  Trimesh, which represents position
    and orientation. If you set the origin size the other parameters
    will be set relative to it.

    Parameters
    ----------
    origin_size : float
      Radius of sphere that represents the origin
    transform : (4, 4) float
      Transformation matrix
    origin_color : (3,) float or int, uint8 or float
      Color of the origin
    axis_radius : float
      Radius of cylinder that represents x, y, z axis
    axis_length: float
      Length of cylinder that represents x, y, z axis

    Returns
    -------
    marker : trimesh.Trimesh
      Mesh geometry of axis indicators
    """
    # the size of the ball representing the origin
    origin_size = float(origin_size)

    # set the transform and use origin-relative
    # sized for other parameters if not specified
    if transform is None:
        transform = np.eye(4)
    if origin_color is None:
        origin_color = [255, 255, 255, 255]
    if axis_radius is None:
        axis_radius = origin_size / 5.0
    if axis_length is None:
        axis_length = origin_size * 10.0

    # generate a ball for the origin
    axis_origin = icosphere(radius=origin_size)
    axis_origin.apply_transform(transform)

    # apply color to the origin ball
    axis_origin.visual.face_colors = origin_color

    # create the cylinder for the z-axis
    translation = tf.translation_matrix([0, 0, axis_length / 2])
    z_axis = cylinder(
        radius=axis_radius, height=axis_length, transform=transform.dot(translation)
    )
    # XYZ->RGB, Z is blue
    z_axis.visual.face_colors = [0, 0, 255]

    # create the cylinder for the y-axis
    translation = tf.translation_matrix([0, 0, axis_length / 2])
    rotation = tf.rotation_matrix(np.radians(-90), [1, 0, 0])
    y_axis = cylinder(
        radius=axis_radius,
        height=axis_length,
        transform=transform.dot(rotation).dot(translation),
    )
    # XYZ->RGB, Y is green
    y_axis.visual.face_colors = [0, 255, 0]

    # create the cylinder for the x-axis
    translation = tf.translation_matrix([0, 0, axis_length / 2])
    rotation = tf.rotation_matrix(np.radians(90), [0, 1, 0])
    x_axis = cylinder(
        radius=axis_radius,
        height=axis_length,
        transform=transform.dot(rotation).dot(translation),
    )
    # XYZ->RGB, X is red
    x_axis.visual.face_colors = [255, 0, 0]

    # append the sphere and three cylinders
    marker = util.concatenate([axis_origin, x_axis, y_axis, z_axis])
    return marker


def camera_marker(
    camera, marker_height: Number = 0.4, origin_size: Optional[Number] = None
):
    """
    Create a visual marker for a camera object, including an axis and FOV.

    Parameters
    ---------------
    camera : trimesh.scene.Camera
      Camera object with FOV and transform defined
    marker_height : float
      How far along the camera Z should FOV indicators be
    origin_size : float
      Sphere radius of the origin (default: marker_height / 10.0)

    Returns
    ------------
    meshes : list
      Contains Trimesh and Path3D objects which can be visualized
    """

    # create sane origin size from marker height
    if origin_size is None:
        origin_size = marker_height / 10.0

    # append the visualizations to an array
    meshes = [axis(origin_size=origin_size)]

    try:
        # path is a soft dependency
        from .path.exchange.load import load_path
    except ImportError:
        # they probably don't have shapely installed
        log.warning("unable to create FOV visualization!", exc_info=True)
        return meshes

    # calculate vertices from camera FOV angles
    x = marker_height * np.tan(np.deg2rad(camera.fov[0]) / 2.0)
    y = marker_height * np.tan(np.deg2rad(camera.fov[1]) / 2.0)
    z = marker_height

    # combine the points into the vertices of an FOV visualization
    points = np.array(
        [(0, 0, 0), (-x, -y, -z), (x, -y, -z), (x, y, -z), (-x, y, -z)], dtype=float
    )

    # create line segments for the FOV visualization
    # a segment from the origin to each bound of the FOV
    segments = np.column_stack((np.zeros_like(points), points)).reshape((-1, 3))

    # add a loop for the outside of the FOV then reshape
    # the whole thing into multiple line segments
    segments = np.vstack((segments, points[[1, 2, 2, 3, 3, 4, 4, 1]])).reshape((-1, 2, 3))

    # add a single Path3D object for all line segments
    meshes.append(load_path(segments))

    return meshes


def truncated_prisms(
    tris: ArrayLike,
    origin: Optional[ArrayLike] = None,
    normal: Optional[ArrayLike] = None,
):
    """
    Return a mesh consisting of multiple watertight prisms below
    a list of triangles, truncated by a specified plane.

    Parameters
    -------------
    triangles : (n, 3, 3) float
      Triangles in space
    origin : None or (3,) float
      Origin of truncation plane
    normal : None or (3,) float
      Unit normal vector of truncation plane

    Returns
    -----------
    mesh : trimesh.Trimesh
      Triangular mesh
    """
    if origin is None:
        transform = np.eye(4)
    else:
        transform = plane_transform(origin=origin, normal=normal)

    # transform the triangles to the specified plane
    transformed = tf.transform_points(tris.reshape((-1, 3)), transform).reshape((-1, 9))

    # stack triangles such that every other one is repeated
    vs = np.column_stack((transformed, transformed)).reshape((-1, 3, 3))
    # set the Z value of the second triangle to zero
    vs[1::2, :, 2] = 0
    # reshape triangles to a flat array of points and transform back to
    # original frame
    vertices = tf.transform_points(vs.reshape((-1, 3)), matrix=np.linalg.inv(transform))

    # face indexes for a *single* truncated triangular prism
    f = np.array(
        [
            [2, 1, 0],
            [3, 4, 5],
            [0, 1, 4],
            [1, 2, 5],
            [2, 0, 3],
            [4, 3, 0],
            [5, 4, 1],
            [3, 5, 2],
        ]
    )
    # find the projection of each triangle with the normal vector
    cross = np.dot([0, 0, 1], triangles.cross(transformed.reshape((-1, 3, 3))).T)
    # stack faces into one prism per triangle
    f_seq = np.tile(f, (len(transformed), 1)).reshape((-1, len(f), 3))
    # if the normal of the triangle was positive flip the winding
    f_seq[cross > 0] = np.fliplr(f)
    # offset stacked faces to create correct indices
    faces = (f_seq + (np.arange(len(f_seq)) * 6).reshape((-1, 1, 1))).reshape((-1, 3))

    # create a mesh from the data
    mesh = Trimesh(vertices=vertices, faces=faces, process=False)

    return mesh


def torus(
    major_radius: Number,
    minor_radius: Number,
    major_sections: Integer = 32,
    minor_sections: Integer = 32,
    transform: Optional[ArrayLike] = None,
    **kwargs,
):
    """Create a mesh of a torus around Z centered at the origin.

    Parameters
    ------------
    major_radius: (float)
      Radius from the center of the torus to the center of the tube.
    minor_radius: (float)
      Radius of the tube.
    major_sections: int
      Number of sections around major radius result should have
      If not specified default is 32 per revolution
    minor_sections: int
      Number of sections around minor radius result should have
      If not specified default is 32 per revolution
    transform : (4, 4) float
      Transformation matrix

    **kwargs:
      passed to Trimesh to create torus

    Returns
    ------------
    geometry : trimesh.Trimesh
      Mesh of a torus
    """
    phi = np.linspace(0, 2 * np.pi, minor_sections + 1, endpoint=True)
    linestring = np.column_stack(
        (minor_radius * np.cos(phi), minor_radius * np.sin(phi))
    ) + [major_radius, 0]

    if "metadata" not in kwargs:
        kwargs["metadata"] = {}
    kwargs["metadata"].update(
        {"shape": "torus", "major_radius": major_radius, "minor_radius": minor_radius}
    )

    # generate torus through simple revolution
    return revolve(
        linestring=linestring, sections=major_sections, transform=transform, **kwargs
    )
