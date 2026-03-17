from ... import util
from ...exceptions import ExceptionWrapper
from ...exchange.ply import load_ply
from ...typed import Optional, Set
from ..path import Path
from . import misc
from .dxf import _dxf_loaders
from .svg_io import _svg_loaders


def load_path(file_obj, file_type: Optional[str] = None, **kwargs):
    """
    Load a file to a Path file_object.

    Parameters
    -----------
    file_obj
      Accepts many types:
         - Path, Path2D, or Path3D file_objects
         - open file file_object (dxf or svg)
         - file name (dxf or svg)
         - shapely.geometry.Polygon
         - shapely.geometry.MultiLineString
         - dict with kwargs for Path constructor
         - `(n, 2, (2|3)) float` line segments
    file_type
        Type of file is required if file
        object is passed.

    Returns
    ---------
    path : Path, Path2D, Path3D file_object
      Data as a native trimesh Path file_object
    """
    # avoid a circular import
    from ...exchange.load import _load_kwargs, _parse_file_args

    arg = _parse_file_args(file_obj=file_obj, file_type=file_type, **kwargs)

    if isinstance(file_obj, Path):
        # we have been passed a file object that is already a loaded
        # trimesh.path.Path object so do nothing and return
        return file_obj
    elif util.is_file(arg.file_obj):
        if arg.file_type in path_loaders:
            kwargs.update(
                path_loaders[arg.file_type](
                    file_obj=arg.file_obj, file_type=arg.file_type
                )
            )
        elif arg.file_type == "ply":
            # we cannot register this exporter to path_loaders since
            # this is already reserved by Trimesh in ply format in trimesh.load()
            kwargs.update(load_ply(file_obj=arg.file_obj, file_type=arg.file_type))
    elif util.is_instance_named(file_obj, ["Polygon", "MultiPolygon"]):
        # convert from shapely polygons to Path2D
        kwargs.update(misc.polygon_to_path(file_obj))
    elif util.is_instance_named(file_obj, "MultiLineString"):
        # convert from shapely LineStrings to Path2D
        kwargs.update(misc.linestrings_to_path(file_obj))
    elif isinstance(file_obj, dict):
        # load as kwargs
        kwargs = file_obj
    elif util.is_sequence(file_obj):
        # load as lines in space
        kwargs.update(misc.lines_to_path(file_obj))
    else:
        raise ValueError("Not a supported object type!")

    # actually load
    result = _load_kwargs(kwargs)
    result._source = arg

    return result


def path_formats() -> Set[str]:
    """
    Get a list of supported path formats.

    Returns
    ------------
    loaders
       Extensions of loadable formats, i.e. {'svg', 'dxf'}
    """

    return {k for k, v in path_loaders.items() if not isinstance(v, ExceptionWrapper)}


path_loaders = {}
path_loaders.update(_svg_loaders)
path_loaders.update(_dxf_loaders)
