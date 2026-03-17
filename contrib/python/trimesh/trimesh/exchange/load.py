import json
import os
from copy import deepcopy

import numpy as np

from .. import resolvers, util
from ..base import Trimesh
from ..exceptions import ExceptionWrapper
from ..parent import Geometry, LoadSource
from ..points import PointCloud
from ..scene.scene import Scene, append_scenes
from ..typed import Dict, Loadable, Optional, Set
from ..util import log
from . import misc
from .binvox import _binvox_loaders
from .cascade import _cascade_loaders
from .dae import _collada_loaders
from .gltf import _gltf_loaders
from .misc import _misc_loaders
from .obj import _obj_loaders
from .off import _off_loaders
from .ply import _ply_loaders
from .stl import _stl_loaders
from .threedxml import _threedxml_loaders
from .threemf import _three_loaders
from .xaml import _xaml_loaders
from .xyz import _xyz_loaders

try:
    from ..path.exchange.load import load_path, path_formats
except BaseException as E:
    # save a traceback to see why path didn't import
    load_path = ExceptionWrapper(E)

    # no path formats available
    def path_formats() -> set:
        return set()


def mesh_formats() -> Set[str]:
    """
    Get a list of mesh formats available to load.

    Returns
    -----------
    loaders
      Extensions of available mesh loaders
      i.e. `{'stl', 'ply'}`
    """
    # filter out exceptionmodule loaders
    return {k for k, v in mesh_loaders.items() if not isinstance(v, ExceptionWrapper)}


def available_formats() -> Set[str]:
    """
    Get a list of all available loaders


    Returns
    -----------
    loaders
      Extensions of all available loaders
      i.e. `{'stl', 'ply', 'dxf'}`
    """
    loaders = mesh_formats()
    loaders.update(path_formats())
    loaders.update(compressed_loaders.keys())

    return loaders


def load(
    file_obj: Loadable,
    file_type: Optional[str] = None,
    resolver: Optional[resolvers.ResolverLike] = None,
    force: Optional[str] = None,
    allow_remote: bool = False,
    **kwargs,
) -> Geometry:
    """
    THIS FUNCTION IS DEPRECATED but there are no current plans for it to be removed.

    For new code the typed load functions `trimesh.load_scene` or `trimesh.load_mesh`
    are recommended over `trimesh.load` which is a backwards-compatibility wrapper
    that mimics the behavior of the old function and can return any geometry type.

    Parameters
    -----------
    file_obj : str, or file- like object
      The source of the data to be loadeded
    file_type: str
      What kind of file type do we have (eg: 'stl')
    resolver : trimesh.visual.Resolver
      Object to load referenced assets like materials and textures
    force : None or str
      For 'mesh': try to coerce scenes into a single mesh
      For 'scene': try to coerce everything into a scene
    allow_remote
      If True allow this load call to work on a remote URL.
    kwargs : dict
      Passed to geometry __init__

    Returns
    ---------
    geometry : Trimesh, Path2D, Path3D, Scene
      Loaded geometry as trimesh classes
    """

    # call the most general loading case into a `Scene`.
    loaded = load_scene(
        file_obj=file_obj,
        file_type=file_type,
        resolver=resolver,
        allow_remote=allow_remote,
        **kwargs,
    )

    if force == "mesh":
        # new code should use `load_mesh` for this
        log.debug(
            "`trimesh.load(force='mesh')` is a compatibility wrapper for `trimesh.load_mesh`"
        )
        return loaded.to_mesh()
    elif force == "scene":
        # new code should use `load_scene` for this
        log.debug(
            "`trimesh.load(force='scene')` is a compatibility wrapper for `trimesh.load_scene`"
        )
        return loaded

    ###########################################
    # we are matching old, deprecated behavior here!
    kind = loaded.source.file_type
    always_scene = {"glb", "gltf", "zip", "3dxml", "tar.gz"}

    if kind not in always_scene and len(loaded.geometry) == 1:
        geom = next(iter(loaded.geometry.values()))
        geom.metadata.update(loaded.metadata)

        if isinstance(geom, PointCloud) or kind in {
            "obj",
            "stl",
            "ply",
            "svg",
            "binvox",
            "xaml",
            "dxf",
            "off",
            "msh",
        }:
            return geom

    return loaded


def load_scene(
    file_obj: Loadable,
    file_type: Optional[str] = None,
    resolver: Optional[resolvers.ResolverLike] = None,
    allow_remote: bool = False,
    metadata: Optional[Dict] = None,
    **kwargs,
) -> Scene:
    """
    Load geometry into the `trimesh.Scene` container. This may contain
    any `parent.Geometry` object, including `Trimesh`, `Path2D`, `Path3D`,
    or a `PointCloud`.

    Parameters
    -----------
    file_obj : str, or file- like object
      The source of the data to be loadeded
    file_type: str
      What kind of file type do we have (eg: 'stl')
    resolver : trimesh.visual.Resolver
      Object to load referenced assets like materials and textures
    force : None or str
      For 'mesh': try to coerce scenes into a single mesh
      For 'scene': try to coerce everything into a scene
    allow_remote
      If True allow this load call to work on a remote URL.
    kwargs : dict
      Passed to geometry __init__

    Returns
    ---------
    geometry : Trimesh, Path2D, Path3D, Scene
      Loaded geometry as trimesh classes
    """

    # parse all possible values of file objects into simple types
    arg = _parse_file_args(
        file_obj=file_obj,
        file_type=file_type,
        resolver=resolver,
        allow_remote=allow_remote,
    )

    try:
        if isinstance(file_obj, dict):
            # we've been passed a dictionary so treat them as keyword arguments
            loaded = _load_kwargs(file_obj)
        elif arg.file_type in path_formats():
            # use path loader
            loaded = load_path(
                file_obj=arg.file_obj,
                file_type=arg.file_type,
                metadata=metadata,
                **kwargs,
            )
        elif arg.file_type in mesh_loaders:
            # use mesh loader
            parsed = deepcopy(kwargs)
            parsed.update(
                mesh_loaders[arg.file_type](
                    file_obj=arg.file_obj,
                    file_type=arg.file_type,
                    resolver=arg.resolver,
                    metadata=metadata,
                    **kwargs,
                )
            )
            loaded = _load_kwargs(**parsed)

        elif arg.file_type in compressed_loaders:
            # for archives, like ZIP files
            loaded = _load_compressed(arg.file_obj, file_type=arg.file_type, **kwargs)
        elif arg.file_type in voxel_loaders:
            loaded = voxel_loaders[arg.file_type](
                file_obj=arg.file_obj,
                file_type=arg.file_type,
                resolver=arg.resolver,
                **kwargs,
            )
        else:
            raise NotImplementedError(f"file_type '{arg.file_type}' not supported")

    finally:
        # if we opened the file ourselves from a file name
        # close any opened files even if we crashed out
        if arg.was_opened:
            arg.file_obj.close()

    if not isinstance(loaded, Scene):
        # file name may be used for nodes
        loaded._source = arg
        loaded = Scene(loaded)

    # add on the loading information
    loaded._source = arg
    for g in loaded.geometry.values():
        g._source = arg

    return loaded


def load_mesh(*args, **kwargs) -> Trimesh:
    """
    Load a file into a Trimesh object.

    Parameters
    -----------
    file_obj : str or file object
      File name or file with mesh data
    file_type : str or None
      Which file type, e.g. 'stl'
    kwargs : dict
      Passed to Trimesh constructor

    Returns
    ----------
    mesh
      Loaded geometry data.
    """
    return load_scene(*args, **kwargs).to_mesh()


def _load_compressed(file_obj, file_type=None, resolver=None, mixed=False, **kwargs):
    """
    Given a compressed archive load all the geometry that
    we can from it.

    Parameters
    ----------
    file_obj : open file-like object
      Containing compressed data
    file_type : str
      Type of the archive file
    mixed : bool
      If False, for archives containing both 2D and 3D
      data will only load the 3D data into the Scene.

    Returns
    ----------
    scene : trimesh.Scene
      Geometry loaded in to a Scene object
    """

    # parse the file arguments into clean loadable form
    arg = _parse_file_args(file_obj=file_obj, file_type=file_type, resolver=resolver)

    # store loaded geometries as a list
    geometries = []

    # so loaders can access textures/etc
    archive = util.decompress(file_obj=arg.file_obj, file_type=arg.file_type)
    resolver = resolvers.ZipResolver(archive)

    # try to save the files with meaningful metadata
    # archive_name = arg.file_path or "archive"
    meta_archive = {}

    # populate our available formats
    if mixed:
        available = available_formats()
    else:
        # all types contained in ZIP archive
        contains = {util.split_extension(n).lower() for n in resolver.keys()}
        # if there are no mesh formats available
        if contains.isdisjoint(mesh_formats()):
            available = path_formats()
        else:
            available = mesh_formats()

    for file_name, file_obj in archive.items():
        try:
            # only load formats that we support
            compressed_type = util.split_extension(file_name).lower()

            # if file has metadata type include it
            if compressed_type in ("yaml", "yml"):
                import yaml

                continue
                meta_archive[file_name] = yaml.safe_load(file_obj)
            elif compressed_type == "json":
                import json

                meta_archive[file_name] = json.load(file_obj)
                continue
            elif compressed_type not in available:
                # don't raise an exception, just try the next one
                continue

            # load the individual geometry
            geometries.append(
                load_scene(
                    file_obj=file_obj,
                    file_type=compressed_type,
                    resolver=resolver,
                    **kwargs,
                )
            )

        except BaseException:
            log.debug("failed to load file in zip", exc_info=True)

    # if we opened the file in this function
    # clean up after ourselves
    if arg.was_opened:
        arg.file_obj.close()

    # append meshes or scenes into a single Scene object
    result = append_scenes(geometries)

    # append any archive metadata files
    if isinstance(result, Scene):
        result.metadata.update(meta_archive)

    return result


def load_remote(url: str, **kwargs) -> Scene:
    """
    Load a mesh at a remote URL into a local trimesh object.

    This is a thin wrapper around:
      `trimesh.load_scene(file_obj=url, allow_remote=True, **kwargs)`

    Parameters
    ------------
    url
      URL containing mesh file
    **kwargs
      Passed to `load_scene`

    Returns
    ------------
    loaded : Trimesh, Path, Scene
      Loaded result
    """
    return load_scene(file_obj=url, allow_remote=True, **kwargs)


def _load_kwargs(*args, **kwargs) -> Geometry:
    """
    Load geometry from a properly formatted dict or kwargs
    """

    def handle_scene() -> Scene:
        """
        Load a scene from our kwargs.

        class:      Scene
        geometry:   dict, name: Trimesh kwargs
        graph:      list of dict, kwargs for scene.graph.update
        base_frame: str, base frame of graph
        """
        graph = kwargs.get("graph", None)
        geometry = {k: _load_kwargs(v) for k, v in kwargs["geometry"].items()}

        if graph is not None:
            scene = Scene()
            scene.geometry.update(geometry)
            for k in graph:
                if isinstance(k, dict):
                    scene.graph.update(**k)
                elif util.is_sequence(k) and len(k) == 3:
                    scene.graph.update(k[1], k[0], **k[2])
        else:
            scene = Scene(geometry)

        # camera, if it exists
        camera = kwargs.get("camera")
        if camera:
            scene.camera = camera
            scene.camera_transform = kwargs.get("camera_transform")

        if "base_frame" in kwargs:
            scene.graph.base_frame = kwargs["base_frame"]
        metadata = kwargs.get("metadata")
        if isinstance(metadata, dict):
            scene.metadata.update(kwargs["metadata"])
        elif isinstance(metadata, str):
            # some ways someone might have encoded a string
            # note that these aren't evaluated until we
            # actually call the lambda in the loop
            candidates = [
                lambda: json.loads(metadata),
                lambda: json.loads(metadata.replace("'", '"')),
            ]
            for c in candidates:
                try:
                    scene.metadata.update(c())
                    break
                except BaseException:
                    pass
        elif metadata is not None:
            log.warning("unloadable metadata")

        return scene

    def handle_mesh() -> Trimesh:
        """
        Handle the keyword arguments for a Trimesh object
        """
        # if they've been serialized as a dict
        if isinstance(kwargs["vertices"], dict) or isinstance(kwargs["faces"], dict):
            return Trimesh(**misc.load_dict(kwargs))
        # otherwise just load that puppy
        return Trimesh(**kwargs)

    def handle_export():
        """
        Handle an exported mesh.
        """
        data, file_type = kwargs["data"], kwargs["file_type"]
        if isinstance(data, dict):
            return _load_kwargs(data)
        elif file_type in mesh_loaders:
            return Trimesh(**mesh_loaders[file_type](data, file_type=file_type))

        raise NotImplementedError(f"`{file_type}` is not supported")

    def handle_path():
        from ..path import Path2D, Path3D

        shape = np.shape(kwargs["vertices"])
        if len(shape) < 2:
            return Path2D()
        if shape[1] == 2:
            return Path2D(**kwargs)
        elif shape[1] == 3:
            return Path3D(**kwargs)
        else:
            raise ValueError("Vertices must be 2D or 3D!")

    def handle_pointcloud():
        return PointCloud(**kwargs)

    # if we've been passed a single dict instead of kwargs
    # substitute the dict for kwargs
    if len(kwargs) == 0 and len(args) == 1 and isinstance(args[0], dict):
        kwargs = args[0]

    # (function, tuple of expected keys)
    # order is important
    handlers = (
        (handle_scene, ("geometry",)),
        (handle_mesh, ("vertices", "faces")),
        (handle_path, ("entities", "vertices")),
        (handle_pointcloud, ("vertices",)),
        (handle_export, ("file_type", "data")),
    )

    # filter out keys with a value of None
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    # loop through handler functions and expected key
    for func, expected in handlers:
        if all(i in kwargs for i in expected):
            # all expected kwargs exist
            return func()

    raise ValueError(f"unable to determine type: {kwargs.keys()}")


def _parse_file_args(
    file_obj,
    file_type: Optional[str],
    resolver: Optional[resolvers.ResolverLike] = None,
    allow_remote: bool = False,
    **kwargs,
) -> LoadSource:
    """
    Given a file_obj and a file_type try to magically convert
    arguments to a file-like object and a lowercase string of
    file type.

    Parameters
    -----------
    file_obj : str
      if string represents a file path, returns:
        file_obj:   an 'rb' opened file object of the path
        file_type:  the extension from the file path

     if string is NOT a path, but has JSON-like special characters:
        file_obj:   the same string passed as file_obj
        file_type:  set to 'json'

     if string is a valid-looking URL
        file_obj: an open 'rb' file object with retrieved data
        file_type: from the extension

     if string is none of those:
        raise ValueError as we can't do anything with input

     if file like object:
        ValueError will be raised if file_type is None
        file_obj:  same as input
        file_type: same as input

     if other object: like a shapely.geometry.Polygon, etc:
        file_obj:  same as input
        file_type: if None initially, set to the class name
                    (in lower case), otherwise passed through

    file_type : str
         type of file and handled according to above

    Returns
    -----------
    args
      Populated `_FileArg` message
    """
    # try to save a file path from various inputs
    file_path = None

    # keep track if we opened a file ourselves and thus are
    # responsible for closing it at the end of loading
    was_opened = False

    if util.is_pathlib(file_obj):
        # convert pathlib objects to string
        file_obj = str(file_obj.absolute())

    if util.is_file(file_obj) and file_type is None:
        raise ValueError("`file_type` must be set for file objects!")

    if isinstance(file_obj, str):
        try:
            # clean up file path to an absolute location
            file_path = os.path.abspath(os.path.expanduser(file_obj))
            # check to see if this path exists
            exists = os.path.isfile(file_path)
        except BaseException:
            exists = False
            file_path = None

        # file obj is a string which exists on filesystm
        if exists:
            # if not passed create a resolver to find other files
            if resolver is None:
                resolver = resolvers.FilePathResolver(file_path)
            # save the file name and path to metadata
            # if file_obj is a path that exists use extension as file_type
            if file_type is None:
                file_type = util.split_extension(file_path, special=["tar.gz", "tar.bz2"])
            # actually open the file
            file_obj = open(file_path, "rb")
            # save that we opened it so we can cleanup later
            was_opened = True
        else:
            if "{" in file_obj:
                # if a bracket is in the string it's probably straight JSON
                file_type = "json"
                file_obj = util.wrap_as_stream(file_obj)
            elif "https://" in file_obj or "http://" in file_obj:
                if not allow_remote:
                    raise ValueError("unable to load URL with `allow_remote=False`")

                import urllib

                # remove the url-safe encoding and query params
                file_type = util.split_extension(
                    urllib.parse.unquote(file_obj).split("?", 1)[0].split("/")[-1].strip()
                )
                # create a web resolver to do the fetching and whatnot
                resolver = resolvers.WebResolver(url=file_obj)
                # fetch the base file
                file_obj = util.wrap_as_stream(resolver.get_base())

            elif file_type is None:
                raise ValueError(f"string is not a file: `{file_obj}`")

    if isinstance(file_type, str) and "." in file_type:
        # if someone has passed the whole filename as the file_type
        # use the file extension as the file_type
        path = os.path.abspath(os.path.expanduser(file_type))
        file_type = util.split_extension(file_type)
        if os.path.exists(path):
            file_path = path
            if resolver is None:
                resolver = resolvers.FilePathResolver(file_path)

    # all our stored extensions reference in lower case
    if file_type is not None:
        file_type = file_type.lower()

    # if we still have no resolver try using file_obj name
    if (
        resolver is None
        and hasattr(file_obj, "name")
        and file_obj.name is not None
        and len(file_obj.name) > 0
    ):
        resolver = resolvers.FilePathResolver(file_obj.name)

    return LoadSource(
        file_obj=file_obj,
        file_type=file_type,
        file_path=file_path,
        was_opened=was_opened,
        resolver=resolver,
    )


# loader functions for compressed extensions
compressed_loaders = {
    "zip": _load_compressed,
    "tar.bz2": _load_compressed,
    "tar.gz": _load_compressed,
    "bz2": _load_compressed,
}

# map file_type to loader function
mesh_loaders = {}
mesh_loaders.update(_misc_loaders)
mesh_loaders.update(_stl_loaders)
mesh_loaders.update(_ply_loaders)
mesh_loaders.update(_obj_loaders)
mesh_loaders.update(_off_loaders)
mesh_loaders.update(_collada_loaders)
mesh_loaders.update(_gltf_loaders)
mesh_loaders.update(_xaml_loaders)
mesh_loaders.update(_threedxml_loaders)
mesh_loaders.update(_three_loaders)
mesh_loaders.update(_xyz_loaders)
mesh_loaders.update(_cascade_loaders)

# collect loaders which return voxel types
voxel_loaders = {}
voxel_loaders.update(_binvox_loaders)
