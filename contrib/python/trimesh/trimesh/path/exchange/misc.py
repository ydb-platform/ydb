import numpy as np

from ... import graph, grouping, util
from ...constants import tol_path
from ...typed import ArrayLike, Dict, NDArray, Optional
from ..entities import Arc, Line


def dict_to_path(as_dict):
    """
    Turn a pure dict into a dict containing entity objects that
    can be sent directly to a Path constructor.

    Parameters
    ------------
    as_dict : dict
      Has keys: 'vertices', 'entities'

    Returns
    ------------
    kwargs : dict
      Has keys: 'vertices', 'entities'
    """
    # start kwargs with initial value
    result = as_dict.copy()
    # map of constructors
    loaders = {"Arc": Arc, "Line": Line}
    # pre- allocate entity array
    entities = [None] * len(as_dict["entities"])
    # run constructor for dict kwargs
    for entity_index, entity in enumerate(as_dict["entities"]):
        if entity["type"] == "Line":
            entities[entity_index] = loaders[entity["type"]](points=entity["points"])
        else:
            entities[entity_index] = loaders[entity["type"]](
                points=entity["points"], closed=entity["closed"]
            )
    result["entities"] = entities

    return result


def lines_to_path(lines: ArrayLike, index: Optional[NDArray[np.int64]] = None) -> Dict:
    """
    Turn line segments into argument to be used for a Path2D or Path3D.

    Parameters
    ------------
    lines : (n, 2, dimension) or (n, dimension) float
      Line segments or connected polyline curve in 2D or 3D
    index : (n,) int64
      If passed save an index for each line segment.

    Returns
    -----------
    kwargs : Dict
      kwargs for Path constructor
    """
    lines = np.asanyarray(lines, dtype=np.float64)

    if index is not None:
        index = np.asanyarray(index, dtype=np.int64)

    if util.is_shape(lines, (-1, (2, 3))):
        # the case where we have a list of points
        # we are going to assume they are connected
        result = {"entities": np.array([Line(np.arange(len(lines)))]), "vertices": lines}
        return result
    elif util.is_shape(lines, (-1, 2, (2, 3))):
        # case where we have line segments in 2D or 3D
        dimension = lines.shape[-1]
        # convert lines to even number of (n, dimension) points
        lines = lines.reshape((-1, dimension))
        # merge duplicate vertices
        unique, inverse = grouping.unique_rows(lines, digits=tol_path.merge_digits)
        # use scipy edges_to_path to skip creating
        # a bajillion individual line entities which
        # will be super slow vs. fewer polyline entities
        return edges_to_path(edges=inverse.reshape((-1, 2)), vertices=lines[unique])
    else:
        raise ValueError("Lines must be (n,(2|3)) or (n,2,(2|3))")
    return result


def polygon_to_path(polygon):
    """
    Load shapely Polygon objects into a trimesh.path.Path2D object

    Parameters
    -------------
    polygon : shapely.geometry.Polygon
      Input geometry

    Returns
    -----------
    kwargs : dict
      Keyword arguments for Path2D constructor
    """
    # start with a single polyline for the exterior
    entities = []
    # start vertices
    vertices = []

    if hasattr(polygon.boundary, "geoms"):
        boundaries = polygon.boundary.geoms
    else:
        boundaries = [polygon.boundary]

    # append interiors as single Line objects
    current = 0
    for boundary in boundaries:
        entities.append(Line(np.arange(len(boundary.coords)) + current))
        current += len(boundary.coords)
        # append the new vertex array
        vertices.append(np.array(boundary.coords))

    # make sure result arrays are numpy
    kwargs = {
        "entities": entities,
        "vertices": np.vstack(vertices) if len(vertices) > 0 else vertices,
    }

    return kwargs


def linestrings_to_path(multi) -> Dict:
    """
    Load shapely LineString objects into arguments to create a Path2D or Path3D.

    Parameters
    -------------
    multi : shapely.geometry.LineString or MultiLineString
      Input 2D or 3D geometry

    Returns
    -------------
    kwargs : Dict
      Keyword arguments for Path2D or Path3D constructor
    """
    import shapely

    # append to result as we go
    entities = []
    vertices = []

    if isinstance(multi, shapely.MultiLineString):
        multi = list(multi.geoms)
    else:
        multi = [multi]

    for line in multi:
        # only append geometry with points
        if hasattr(line, "coords"):
            coords = np.array(line.coords)
            if len(coords) < 2:
                continue
            entities.append(Line(np.arange(len(coords)) + len(vertices)))
            vertices.extend(coords)

    kwargs = {"entities": np.array(entities), "vertices": np.array(vertices)}
    return kwargs


def faces_to_path(mesh, face_ids=None, **kwargs):
    """
    Given a mesh and face indices find the outline edges and
    turn them into a Path3D.

    Parameters
    ------------
    mesh : trimesh.Trimesh
      Triangulated surface in 3D
    face_ids : (n,) int
      Indexes referencing mesh.faces

    Returns
    ---------
    kwargs : dict
      Kwargs for Path3D constructor
    """
    if face_ids is None:
        edges = mesh.edges_sorted
    else:
        # take advantage of edge ordering to index as single row
        edges = mesh.edges_sorted.reshape((-1, 6))[face_ids].reshape((-1, 2))
    # an edge which occurs onely once is on the boundary
    unique_edges = grouping.group_rows(edges, require_count=1)
    # add edges and vertices to kwargs
    kwargs.update(edges_to_path(edges=edges[unique_edges], vertices=mesh.vertices))

    return kwargs


def edges_to_path(edges: ArrayLike, vertices: ArrayLike, **kwargs) -> Dict:
    """
    Given an edge list of indices and associated vertices
    representing lines, generate kwargs for a Path object.

    Parameters
    -----------
    edges : (n, 2) int
      Vertex indices of line segments
    vertices : (m, dimension) float
      Vertex positions where dimension is 2 or 3

    Returns
    ----------
    kwargs : dict
      Kwargs for Path constructor
    """
    # sequence of ordered traversals
    dfs = graph.traversals(edges, mode="dfs")
    # make sure every consecutive index in DFS
    # traversal is an edge in the source edge list
    dfs_connected = graph.fill_traversals(dfs, edges=edges)
    # kwargs for Path constructor
    # turn traversals into Line objects
    lines = [Line(d) for d in dfs_connected]

    kwargs.update({"entities": lines, "vertices": vertices, "process": False})
    return kwargs
