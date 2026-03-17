"""
gltf/__init__.py
------------

Provides GLTF 2.0 exports of trimesh.Trimesh objects
as GL_TRIANGLES, and trimesh.Path2D/Path3D as GL_LINES
"""

import base64
import json
from collections import OrderedDict, defaultdict, deque
from copy import deepcopy

import numpy as np

from ... import rendering, resources, transformations, util, visual
from ...caching import hash_fast
from ...constants import log, tol
from ...resolvers import ResolverLike, ZipResolver
from ...scene.cameras import Camera
from ...typed import Dict, List, NDArray, Optional, Stream
from ...util import triangle_strips_to_faces, unique_name
from .extensions import handle_extensions

# magic numbers which have meaning in GLTF
# most are uint32's of UTF-8 text
_magic = {"gltf": 1179937895, "json": 1313821514, "bin": 5130562}

# GLTF data type codes: little endian numpy dtypes
_dtypes = {5120: "<i1", 5121: "<u1", 5122: "<i2", 5123: "<u2", 5125: "<u4", 5126: "<f4"}
# a string we can use to look up numpy dtype : GLTF dtype
_dtypes_lookup = {v[1:]: k for k, v in _dtypes.items()}


# GLTF data formats: numpy shapes
_shapes = {
    "SCALAR": 1,
    "VEC2": (2),
    "VEC3": (3),
    "VEC4": (4),
    "MAT2": (2, 2),
    "MAT3": (3, 3),
    "MAT4": (4, 4),
}

# a default PBR metallic material
_default_material = {
    "pbrMetallicRoughness": {
        "baseColorFactor": [1, 1, 1, 1],
        "metallicFactor": 0,
        "roughnessFactor": 0,
    }
}

# GL geometry modes
_GL_LINES = 1
_GL_POINTS = 0
_GL_TRIANGLES = 4
_GL_STRIP = 5

_EYE = np.eye(4)
_EYE.flags.writeable = False

# specify dtypes with forced little endian
float32 = np.dtype("<f4")
uint32 = np.dtype("<u4")
uint8 = np.dtype("<u1")


def export_gltf(
    scene,
    include_normals=None,
    merge_buffers=False,
    unitize_normals=True,
    tree_postprocessor=None,
    embed_buffers=False,
    extension_webp=False,
    extension_draco=False,
):
    """
    Export a scene object as a GLTF directory.

    This puts each mesh into a separate file (i.e. a `buffer`)
    as opposed to one larger file.

    Parameters
    -----------
    scene : trimesh.Scene
      Scene to be exported
    include_normals : None or bool
      Include vertex normals
    merge_buffers : bool
      Merge buffers into one blob.
    unitize_normals
      GLTF requires unit normals, however sometimes people
      want to include non-unit normals for shading reasons.
    resolver : trimesh.resolvers.Resolver
      If passed will use to write each file.
    tree_postprocesser : None or callable
      Run this on the header tree before exiting.
    embed_buffers : bool
      Embed the buffer into JSON file as a base64 string in the URI
    extension_webp : bool
      Export textures as webP (using glTF's EXT_texture_webp extension).
    extension_draco : bool
      Compress mesh data using Draco (KHR_draco_mesh_compression).
      Requires the `dracox` package to be installed.

    Returns
    ----------
    export : dict
      Format: {file name : file data}
    """
    # if we were passed a bare Trimesh or Path3D object
    if not util.is_instance_named(scene, "Scene") and hasattr(scene, "scene"):
        scene = scene.scene()

    # create the header and buffer data
    tree, buffer_items = _create_gltf_structure(
        scene=scene,
        unitize_normals=unitize_normals,
        include_normals=include_normals,
        extension_webp=extension_webp,
        extension_draco=extension_draco,
    )

    # allow custom postprocessing
    if tree_postprocessor is not None:
        tree_postprocessor(tree)

    # store files as {name : data}
    files = {}

    base64_buffer_format = "data:application/octet-stream;base64,{}"
    if merge_buffers:
        views = _build_views(buffer_items)
        buffer_data = b"".join(buffer_items.values())
        if embed_buffers:
            buffer_name = base64_buffer_format.format(
                base64.b64encode(buffer_data).decode()
            )
        else:
            buffer_name = "gltf_buffer.bin"
            files[buffer_name] = buffer_data
        buffers = [{"uri": buffer_name, "byteLength": len(buffer_data)}]
    else:
        # make one buffer per buffer_items
        buffers = [None] * len(buffer_items)
        # A bufferView is a slice of a file
        views = [None] * len(buffer_items)
        # create the buffer views
        for i, item in enumerate(buffer_items.values()):
            views[i] = {"buffer": i, "byteOffset": 0, "byteLength": len(item)}
            if embed_buffers:
                buffer_name = base64_buffer_format.format(base64.b64encode(item).decode())
            else:
                buffer_name = f"gltf_buffer_{i}.bin"
                files[buffer_name] = item
            buffers[i] = {"uri": buffer_name, "byteLength": len(item)}

    if len(buffers) > 0:
        tree["buffers"] = buffers
        tree["bufferViews"] = views
    # dump tree with compact separators
    files["model.gltf"] = util.jsonify(tree, separators=(",", ":")).encode("utf-8")

    if tol.strict:
        validate(tree)

    return files


def export_glb(
    scene,
    include_normals=None,
    unitize_normals=True,
    tree_postprocessor=None,
    buffer_postprocessor=None,
    extension_webp=False,
    extension_draco=False,
):
    """
    Export a scene as a binary GLTF (GLB) file.

    Parameters
    ------------
    scene: trimesh.Scene
      Input geometry
    extras : JSON serializable
      Will be stored in the extras field.
    include_normals : bool
      Include vertex normals in output file?
    tree_postprocessor : func
      Custom function to (in-place) post-process the tree
      before exporting.
    extension_webp : bool
      Export textures as webP using EXT_texture_webp extension.
    extension_draco : bool
      Compress mesh data using Draco (KHR_draco_mesh_compression).
      Requires the `dracox` package to be installed.

    Returns
    ----------
    exported : bytes
      Exported result in GLB 2.0
    """
    # if we were passed a bare Trimesh or Path3D object
    if not util.is_instance_named(scene, "Scene") and hasattr(scene, "scene"):
        # generate a scene with just that mesh in it
        scene = scene.scene()

    tree, buffer_items = _create_gltf_structure(
        scene=scene,
        unitize_normals=unitize_normals,
        include_normals=include_normals,
        buffer_postprocessor=buffer_postprocessor,
        extension_webp=extension_webp,
        extension_draco=extension_draco,
    )

    # A bufferView is a slice of a file
    views = _build_views(buffer_items)

    # combine bytes into a single blob
    buffer_data = b"".join(buffer_items.values())

    # add the information about the buffer data
    if len(buffer_data) > 0:
        tree["buffers"] = [{"byteLength": len(buffer_data)}]
        tree["bufferViews"] = views

    # allow custom postprocessing
    if tree_postprocessor is not None:
        tree_postprocessor(tree)

    # export the tree to JSON for the header
    content = util.jsonify(tree, separators=(",", ":"))
    # add spaces to content, so the start of the data
    # is 4 byte aligned as per spec
    content += (4 - ((len(content) + 20) % 4)) * " "
    content = content.encode("utf-8")
    # make sure we didn't screw it up
    assert (len(content) % 4) == 0

    # the initial header of the file
    header = _byte_pad(
        np.array(
            [
                _magic["gltf"],  # magic, turns into glTF
                2,  # GLTF version
                # length is the total length of the Binary glTF
                # including Header and all Chunks, in bytes.
                len(content) + len(buffer_data) + 28,
                # contentLength is the length, in bytes,
                # of the glTF content (JSON)
                len(content),
                # magic number which is 'JSON'
                _magic["json"],
            ],
            dtype="<u4",
        ).tobytes()
    )

    # the header of the binary data section
    bin_header = _byte_pad(
        np.array([len(buffer_data), 0x004E4942], dtype="<u4").tobytes()
    )

    exported = b"".join([header, content, bin_header, buffer_data])

    if tol.strict:
        validate(tree)

    return exported


def load_gltf(
    file_obj: Optional[Stream] = None,
    resolver: Optional[ResolverLike] = None,
    ignore_broken: bool = False,
    merge_primitives: bool = False,
    skip_materials: bool = False,
    **mesh_kwargs,
):
    """
    Load a GLTF file, which consists of a directory structure
    with multiple files.

    Parameters
    -------------
    file_obj : None or file-like
      Object containing header JSON, or None
    resolver : trimesh.visual.Resolver
      Object which can be used to load other files by name
    ignore_broken : bool
      If there is a mesh we can't load and this
      is True don't raise an exception but return
      a partial result
    merge_primitives : bool
      If True, each GLTF 'mesh' will correspond
      to a single Trimesh object
    skip_materials : bool
      If true, will not load materials (if present).
    **mesh_kwargs : dict
      Passed to mesh constructor

    Returns
    --------------
    kwargs : dict
      Arguments to create scene
    """
    try:
        # see if we've been passed the GLTF header file
        tree = json.loads(util.decode_text(file_obj.read()))
    except BaseException:
        # otherwise header should be in 'model.gltf'
        data = resolver["model.gltf"]
        # old versions of python/json need strings
        tree = json.loads(util.decode_text(data))

    # gltf 1.0 is a totally different format
    # that wasn't widely deployed before they fixed it
    version = tree.get("asset", {}).get("version", "2.0")
    if isinstance(version, str):
        # parse semver like '1.0.1' into just a major integer
        major = int(version.split(".", 1)[0])
    else:
        major = int(float(version))

    if major < 2:
        raise NotImplementedError(f"only GLTF 2 is supported not `{version}`")

    # use the URI and resolver to get data from file names
    buffers = [
        _uri_to_bytes(uri=b["uri"], resolver=resolver) for b in tree.get("buffers", [])
    ]

    # turn the layout header and data into kwargs
    # that can be used to instantiate a trimesh.Scene object
    kwargs = _read_buffers(
        header=tree,
        buffers=buffers,
        ignore_broken=ignore_broken,
        merge_primitives=merge_primitives,
        mesh_kwargs=mesh_kwargs,
        skip_materials=skip_materials,
        resolver=resolver,
    )
    return kwargs


def load_glb(
    file_obj: Stream,
    resolver: Optional[ResolverLike] = None,
    ignore_broken: bool = False,
    merge_primitives: bool = False,
    skip_materials: bool = False,
    **mesh_kwargs,
):
    """
    Load a GLTF file in the binary GLB format into a trimesh.Scene.

    Implemented from specification:
    https://github.com/KhronosGroup/glTF/tree/master/specification/2.0

    Parameters
    ------------
    file_obj : file- like object
      Containing GLB data
    resolver : trimesh.visual.Resolver
      Object which can be used to load other files by name
    ignore_broken : bool
      If there is a mesh we can't load and this
      is True don't raise an exception but return
      a partial result
    merge_primitives : bool
      If True, each GLTF 'mesh' will correspond to a
      single Trimesh object.
    skip_materials : bool
      If true, will not load materials (if present).

    Returns
    ------------
    kwargs : dict
      Kwargs to instantiate a trimesh.Scene
    """
    # read the first 20 bytes which contain section lengths
    head_data = file_obj.read(20)
    head = np.frombuffer(head_data, dtype="<u4")

    # check to make sure first index is gltf magic header
    if head[0] != _magic["gltf"]:
        raise ValueError("incorrect header on GLB file")

    # and second value is version: should be 2 for GLTF 2.0
    if head[1] != 2:
        raise NotImplementedError(f"only GLTF 2 is supported not `{head[1]}`")

    # overall file length
    # first chunk length
    # first chunk type
    length, chunk_length, chunk_type = head[2:]

    # first chunk should be JSON header
    if chunk_type != _magic["json"]:
        raise ValueError("no initial JSON header!")

    # uint32 causes an error in read, so we convert to native int
    # for the length passed to read, for the JSON header
    json_data = file_obj.read(int(chunk_length))
    # convert to text
    if hasattr(json_data, "decode"):
        json_data = util.decode_text(json_data)
    # load the json header to native dict
    header = json.loads(json_data)

    # read the binary data referred to by GLTF as 'buffers'
    buffers = []
    start = file_obj.tell()

    # header can contain base64 encoded data in the URI field
    info = header.get("buffers", []).copy()

    while (file_obj.tell() - start) < length:
        # if we have buffer infos with URI check it here
        try:
            # if they have interleaved URI data with GLB data handle it here
            uri = info.pop(0)["uri"]
            buffers.append(_uri_to_bytes(uri=uri, resolver=resolver))
            continue
        except (IndexError, KeyError):
            # if there was no buffer info or URI we still need to read
            pass

        # the last read put us past the JSON chunk
        # we now read the chunk header, which is 8 bytes
        chunk_head = file_obj.read(8)
        if len(chunk_head) != 8:
            # double check to make sure we didn't
            # read the whole file
            break
        chunk_length, chunk_type = np.frombuffer(chunk_head, dtype="<u4")
        # make sure we have the right data type
        if chunk_type != _magic["bin"]:
            raise ValueError("not binary GLTF!")
        # read the chunk
        chunk_data = file_obj.read(int(chunk_length))
        if len(chunk_data) != chunk_length:
            raise ValueError("chunk was not expected length!")
        buffers.append(chunk_data)

    # turn the layout header and data into kwargs
    # that can be used to instantiate a trimesh.Scene object
    kwargs = _read_buffers(
        header=header,
        buffers=buffers,
        ignore_broken=ignore_broken,
        merge_primitives=merge_primitives,
        skip_materials=skip_materials,
        mesh_kwargs=mesh_kwargs,
        resolver=resolver,
    )

    return kwargs


def _uri_to_bytes(uri: str, resolver: ResolverLike) -> bytes:
    """
    Take a URI string and load it as a
    a filename or as base64.

    Parameters
    --------------
    uri
      Usually a filename or something like:
      "data:object/stuff,base64,AABA112A..."
    resolver
      A resolver to load referenced assets

    Returns
    ---------------
    data
      Loaded data from URI
    """
    # see if the URI has base64 data
    index = uri.find("base64,")
    if index < 0:
        # string didn't contain the base64 header
        # so return the result from the resolver
        return resolver[uri]
    # we have a base64 header so strip off
    # leading index and then decode into bytes
    return base64.b64decode(uri[index + 7 :])


def _buffer_append(ordered, data):
    """
    Append data to an existing OrderedDict and
    pad it to a 4-byte boundary.

    Parameters
    ----------
    od : OrderedDict
      Keyed like { hash : data }
    data : bytes
      To be stored

    Returns
    ----------
    index : int
      Index of buffer_items stored in
    """
    # hash the data to see if we have it already
    hashed = hash_fast(data)
    if hashed in ordered:
        # apparently they never implemented keys().index -_-
        return list(ordered.keys()).index(hashed)
    # not in buffer items so append and then return index
    ordered[hashed] = _byte_pad(data)

    return len(ordered) - 1


def _data_append(acc: OrderedDict, buff: OrderedDict, blob: dict, data: NDArray):
    """
    Append a new accessor to an OrderedDict.

    Parameters
    ------------
    acc
      Collection of accessors, will be mutated in-place
    buff
      Collection of buffer bytes, will be mutated in-place
    blob
      Candidate accessor
    data
      Data to fill in details to blob

    Returns
    ----------
    index : int
      Index of accessor that was added or reused.
    """
    # if we have data include that in the key
    as_bytes = data.tobytes()
    if hasattr(data, "hash_fast"):
        # passed a TrackedArray object
        hashed = data.hash_fast()
    else:
        # someone passed a vanilla numpy array
        hashed = hash_fast(as_bytes)

    if hashed in buff:
        blob["bufferView"] = list(buff.keys()).index(hashed)
    else:
        # not in buffer items so append and then return index
        buff[hashed] = _byte_pad(as_bytes)
        blob["bufferView"] = len(buff) - 1

    # start by hashing the dict blob
    # note that this will not work if a value is a list
    try:
        # simple keys can be hashed as tuples without JSON
        key = hash(tuple(blob.items()))
    except BaseException:
        # if there are list keys that break the simple hash
        key = hash(json.dumps(blob, sort_keys=True))

    # xor the hash for the blob to the key
    key ^= hashed

    # if key exists return the index in the OrderedDict
    if key in acc:
        return list(acc.keys()).index(key)

    # get a numpy dtype for our components
    dtype = np.dtype(_dtypes[blob["componentType"]])
    # see if we're an array, matrix, etc
    kind = blob["type"]

    if tol.strict:
        # in unit tests make sure everything we're trying to export
        # is finite, which also checks for accidental NaN values
        assert np.isfinite(data).all()

    if kind == "SCALAR":
        # is probably (n, 1)
        blob["count"] = int(np.prod(data.shape))
        blob["max"] = np.array([data.max()], dtype=dtype).tolist()
        blob["min"] = np.array([data.min()], dtype=dtype).tolist()
    elif kind.startswith("MAT"):
        # i.e. (n, 4, 4) matrices
        blob["count"] = len(data)
    else:
        # reshape the data into what we're actually exporting
        resh = data.reshape((-1, _shapes[kind]))
        blob["count"] = len(resh)
        blob["max"] = resh.max(axis=0).astype(dtype).tolist()
        blob["min"] = resh.min(axis=0).astype(dtype).tolist()

    # store the accessor and return the index
    acc[key] = blob
    return len(acc) - 1


def _jsonify(blob):
    """
    Roundtrip a blob through json export-import cycle
    skipping any internal keys.
    """
    return json.loads(
        util.jsonify({k: v for k, v in blob.items() if not k.startswith("_")})
    )


def _create_gltf_structure(
    scene,
    include_normals=None,
    include_metadata=True,
    unitize_normals=None,
    buffer_postprocessor=None,
    extension_webp=False,
    extension_draco=False,
):
    """
    Generate a GLTF header.

    Parameters
    -------------
    scene : trimesh.Scene
      Input scene data
    include_metadata : bool
      Include `scene.metadata` as `scenes/{idx}/extras/metadata`
    include_normals : bool
      Include vertex normals in output file?
    unitize_normals : bool
      Unitize all exported normals so as to pass GLTF validation
    extension_webp : bool
      Export textures as webP using EXT_texture_webp extension.
    extension_draco : bool
      Compress mesh data using Draco (KHR_draco_mesh_compression).

    Returns
    ---------------
    tree : dict
      Contains required keys for a GLTF scene
    buffer_items : list
      Contains bytes of data
    """
    # we are defining a single scene, and will be setting the
    # world node to the 0-index
    tree = {
        "scene": 0,
        "scenes": [{"nodes": [0]}],
        "asset": {"version": "2.0", "generator": "https://github.com/mikedh/trimesh"},
        "accessors": OrderedDict(),
        "meshes": [],
        "images": [],
        "textures": [],
        "materials": [],
    }

    if scene.has_camera:
        tree["cameras"] = [_convert_camera(scene.camera)]

    if include_metadata and len(scene.metadata) > 0:
        try:
            # fail here if data isn't json compatible
            # only export the extras if there is something there
            tree["scenes"][0]["extras"] = _jsonify(scene.metadata)
            extensions = tree["scenes"][0]["extras"].pop("gltf_extensions", None)
            if isinstance(extensions, dict):
                tree["extensions"] = extensions
        except BaseException:
            log.debug("failed to export scene metadata!", exc_info=True)

    # store materials as {hash : index} to avoid duplicates
    mat_hashes = {}
    # store data from geometries
    buffer_items = OrderedDict()

    # map the name of each mesh to the index in tree['meshes']
    mesh_index = {}
    previous = len(tree["meshes"])

    # loop through every geometry
    for name, geometry in scene.geometry.items():
        if util.is_instance_named(geometry, "Trimesh"):
            # add the mesh
            _append_mesh(
                mesh=geometry,
                name=name,
                tree=tree,
                buffer_items=buffer_items,
                include_normals=include_normals,
                unitize_normals=unitize_normals,
                mat_hashes=mat_hashes,
                extension_webp=extension_webp,
                extension_draco=extension_draco,
            )
        elif util.is_instance_named(geometry, "Path"):
            # add Path2D and Path3D objects
            _append_path(path=geometry, name=name, tree=tree, buffer_items=buffer_items)
        elif util.is_instance_named(geometry, "PointCloud"):
            # add PointCloud objects
            _append_point(
                points=geometry, name=name, tree=tree, buffer_items=buffer_items
            )

        # only store the index if the append did anything
        if len(tree["meshes"]) != previous:
            previous = len(tree["meshes"])
            mesh_index[name] = previous - 1

    # grab the flattened scene graph in GLTF's format
    nodes = scene.graph.to_gltf(scene=scene, mesh_index=mesh_index)
    tree.update(nodes)

    extensions_used = set()
    extensions_required = set()
    # Add any scene extensions used
    if "extensions" in tree:
        extensions_used = extensions_used.union(set(tree["extensions"].keys()))
    # Add any mesh extensions used
    for mesh in tree["meshes"]:
        if "extensions" in mesh:
            extensions_used = extensions_used.union(set(mesh["extensions"].keys()))
        # Check primitives for extensions too
        for prim in mesh.get("primitives", []):
            if "extensions" in prim:
                extensions_used = extensions_used.union(set(prim["extensions"].keys()))
    # Add any extensions already in the tree (e.g. node extensions)
    if "extensionsUsed" in tree:
        extensions_used = extensions_used.union(set(tree["extensionsUsed"]))
    # Add WebP if used
    if extension_webp:
        extensions_used.add("EXT_texture_webp")
        extensions_required.add("EXT_texture_webp")
    # Add Draco if used (no fallback, so required)
    if extension_draco:
        extensions_used.add("KHR_draco_mesh_compression")
        extensions_required.add("KHR_draco_mesh_compression")
    if len(extensions_used) > 0:
        tree["extensionsUsed"] = list(extensions_used)
    if len(extensions_required) > 0:
        tree["extensionsRequired"] = list(extensions_required)

    if buffer_postprocessor is not None:
        buffer_postprocessor(buffer_items, tree)

    # convert accessors back to a flat list
    tree["accessors"] = list(tree["accessors"].values())

    # cull empty or unpopulated fields
    # check keys that might be empty so we can remove them
    check = ["textures", "materials", "images", "accessors", "meshes"]
    # remove the keys with nothing stored in them
    [tree.pop(key) for key in check if len(tree[key]) == 0]

    return tree, buffer_items


def _append_mesh(
    mesh,
    name,
    tree,
    buffer_items,
    include_normals: Optional[bool],
    unitize_normals: bool,
    mat_hashes: dict,
    extension_webp: bool,
    extension_draco: bool = False,
):
    """
    Append a mesh to the scene structure and put the
    data into buffer_items.

    Parameters
    -------------
    mesh : trimesh.Trimesh
      Source geometry
    name : str
      Name of geometry
    tree : dict
      Will be updated with data from mesh
    buffer_items
      Will have buffer appended with mesh data
    include_normals : bool
      Include vertex normals in export or not
    unitize_normals : bool
      Transform normals into unit vectors.
      May be undesirable but will fail validators without this.

    mat_hashes : dict
      Which materials have already been added
    extension_webp : bool
      Export textures as webP (using glTF's EXT_texture_webp extension).
    extension_draco : bool
      Compress mesh data using Draco (KHR_draco_mesh_compression).
    """
    # return early from empty meshes to avoid crashing later
    if len(mesh.faces) == 0 or len(mesh.vertices) == 0:
        log.debug("skipping empty mesh!")
        return
    # convert mesh data to the correct dtypes
    # faces: 5125 is an unsigned 32 bit integer
    # accessors refer to data locations
    # mesh faces are stored as flat list of integers
    acc_face = _data_append(
        acc=tree["accessors"],
        buff=buffer_items,
        blob={"componentType": 5125, "type": "SCALAR"},
        data=mesh.faces.astype(uint32),
    )

    # vertices: 5126 is a float32
    # create or reuse an accessor for these vertices
    acc_vertex = _data_append(
        acc=tree["accessors"],
        buff=buffer_items,
        blob={"componentType": 5126, "type": "VEC3", "byteOffset": 0},
        data=mesh.vertices.astype(float32),
    )

    # meshes reference accessor indexes
    current = {
        "name": name,
        "extras": {},
        "primitives": [
            {
                "attributes": {"POSITION": acc_vertex},
                "indices": acc_face,
                "mode": _GL_TRIANGLES,
            }
        ],
    }
    # if units are defined, store them as an extra
    # the GLTF spec says everything is implicit meters
    # we're not doing that as our unit conversions are expensive
    # although that might be better, implicit works for 3DXML
    # https://github.com/KhronosGroup/glTF/tree/master/extensions
    try:
        # skip jsonify any metadata, skipping internal keys
        current["extras"] = _jsonify(mesh.metadata)

        # extract extensions if any
        extensions = current["extras"].pop("gltf_extensions", None)
        if isinstance(extensions, dict):
            current["extensions"] = extensions

        if mesh.units not in [None, "m", "meters", "meter"]:
            current["extras"]["units"] = str(mesh.units)
    except BaseException:
        log.debug("metadata not serializable, dropping!", exc_info=True)

    # check to see if we have vertex or face colors
    # or if a TextureVisual has colors included as an attribute
    if mesh.visual.kind in ["vertex", "face"]:
        vertex_colors = mesh.visual.vertex_colors
    elif (
        hasattr(mesh.visual, "vertex_attributes")
        and "color" in mesh.visual.vertex_attributes
    ):
        vertex_colors = mesh.visual.vertex_attributes["color"]
    else:
        vertex_colors = None

    if vertex_colors is not None:
        if len(vertex_colors) == len(mesh.vertices):
            # convert color data to bytes and append
            acc_color = _data_append(
                acc=tree["accessors"],
                buff=buffer_items,
                blob={
                    "componentType": 5121,
                    "normalized": True,
                    "type": "VEC4",
                    "byteOffset": 0,
                },
                data=vertex_colors.astype(uint8),
            )

            # add the reference for vertex color
            current["primitives"][0]["attributes"]["COLOR_0"] = acc_color
        else:
            log.warning(
                "Vertex colors have different length than mesh vertices, dropping!"
            )

    if hasattr(mesh.visual, "material"):
        # append the material and then set from returned index
        current_material = _append_material(
            mat=mesh.visual.material,
            tree=tree,
            buffer_items=buffer_items,
            mat_hashes=mat_hashes,
            extension_webp=extension_webp,
        )

        # if mesh has UV coordinates defined export them
        has_uv = (
            hasattr(mesh.visual, "uv")
            and mesh.visual.uv is not None
            and len(mesh.visual.uv) == len(mesh.vertices)
        )
        if has_uv:
            # slice off W if passed
            uv = mesh.visual.uv.copy()[:, :2]
            # reverse the Y for GLTF
            uv[:, 1] = 1.0 - uv[:, 1]
            # add an accessor describing the blob of UV's
            acc_uv = _data_append(
                acc=tree["accessors"],
                buff=buffer_items,
                blob={"componentType": 5126, "type": "VEC2", "byteOffset": 0},
                data=uv.astype(float32),
            )
            # add the reference for UV coordinates
            current["primitives"][0]["attributes"]["TEXCOORD_0"] = acc_uv

        # reference the material
        current["primitives"][0]["material"] = current_material

    if include_normals or (
        include_normals is None and "vertex_normals" in mesh._cache.cache
    ):
        # store vertex normals if requested
        if unitize_normals:
            normals = util.unitize(mesh.vertex_normals)
        else:
            # we don't have to copy them since
            # they aren't being altered
            normals = mesh.vertex_normals

        acc_norm = _data_append(
            acc=tree["accessors"],
            buff=buffer_items,
            blob={
                "componentType": 5126,
                "count": len(mesh.vertices),
                "type": "VEC3",
                "byteOffset": 0,
            },
            data=normals.astype(float32),
        )
        # add the reference for vertex color
        current["primitives"][0]["attributes"]["NORMAL"] = acc_norm

    # for each attribute with a leading underscore, assign them to trimesh
    # vertex_attributes
    for key, attrib in mesh.vertex_attributes.items():
        # make sure vertex attribute length matches vertices
        if len(attrib) != len(mesh.vertices):
            log.warning(
                f"Vertex attribute `{key}` has different length than mesh vertices skipping!"
            )
            continue

        # application specific attributes must be prefixed with an underscore
        if not key.startswith("_"):
            key = "_" + key

        # GLTF has no floating point type larger than 32 bits so clip
        # any float64 or larger to float32
        if attrib.dtype.kind == "f" and attrib.dtype.itemsize > 4:
            data = attrib.astype(np.float32)
        else:
            data = attrib

        if len(data.shape) == 1:
            data = data[:, np.newaxis]

        # every accessor VALUE must be 4-byte aligned
        row_mod = (data.shape[1] * data.dtype.itemsize) % 4
        # if the row size is not a multiple of 4, pad it
        if row_mod != 0:
            # how many columns of padding for this value
            pad_columns = (4 - row_mod) // data.dtype.itemsize
            # pad this custom attribute with zeros -_-
            data = np.pad(data, ((0, 0), (0, pad_columns)), mode="constant")

        # store custom vertex attributes
        current["primitives"][0]["attributes"][key] = _data_append(
            acc=tree["accessors"],
            buff=buffer_items,
            blob=_build_accessor(data),
            data=data,
        )

    # Handle Draco compression via extension handler
    if extension_draco:
        # Determine if normals should be included
        should_include_normals = include_normals or (
            include_normals is None and "vertex_normals" in mesh._cache.cache
        )
        # Call primitive_export handlers
        handle_extensions(
            extensions={"KHR_draco_mesh_compression": {}},
            scope="primitive_export",
            mesh=mesh,
            name=name,
            tree=tree,
            buffer_items=buffer_items,
            primitive=current["primitives"][0],
            include_normals=should_include_normals,
        )

    tree["meshes"].append(current)


def _build_views(buffer_items):
    """
    Create views for buffers that are simply
    based on how many bytes they are long.

    Parameters
    --------------
    buffer_items : OrderedDict
      Buffers to build views for

    Returns
    ----------
    views : (n,) list of dict
      GLTF views
    """
    views = []
    # create the buffer views
    current_pos = 0
    for current_item in buffer_items.values():
        views.append(
            {"buffer": 0, "byteOffset": current_pos, "byteLength": len(current_item)}
        )
        assert (current_pos % 4) == 0
        assert (len(current_item) % 4) == 0
        current_pos += len(current_item)
    return views


def _build_accessor(array):
    """
    Build an accessor for an arbitrary array.

    Parameters
    -----------
    array : numpy array
      The array to build an accessor for

    Returns
    ----------
    accessor : dict
      The accessor for array.
    """
    shape = array.shape
    data_type = "SCALAR"
    if len(shape) == 2:
        vec_length = shape[1]
        if vec_length > 4:
            raise ValueError("The GLTF spec does not support vectors larger than 4")
        if vec_length > 1:
            data_type = f"VEC{int(vec_length)}"
        else:
            data_type = "SCALAR"

    if len(shape) == 3:
        if shape[2] not in [2, 3, 4]:
            raise ValueError("Matrix types must have 4, 9 or 16 components")
        data_type = f"MAT{int(shape[2])}"

    # get the array data type as a str stripping off endian
    lookup = array.dtype.str.lstrip("<>|")

    if lookup == "u4":
        # spec: UNSIGNED_INT is only allowed when the accessor
        # contains indices i.e. the accessor is only referenced
        # by `primitive.indices`
        log.debug("custom uint32 may cause validation failures")

    # map the numpy dtype to a GLTF code (i.e. 5121)
    componentType = _dtypes_lookup[lookup]
    accessor = {"componentType": componentType, "type": data_type, "byteOffset": 0}

    if len(shape) < 3:
        accessor["max"] = array.max(axis=0).tolist()
        accessor["min"] = array.min(axis=0).tolist()

    return accessor


def _byte_pad(data, bound=4):
    """
    GLTF wants chunks aligned with 4 byte boundaries.
    This function will add padding to the end of a
    chunk of bytes so that it aligns with the passed
    boundary size.

    Parameters
    --------------
    data : bytes
      Data to be padded
    bound : int
      Length of desired boundary

    Returns
    --------------
    padded : bytes
      Result where: (len(padded) % bound) == 0
    """
    assert isinstance(data, bytes)
    if len(data) % bound != 0:
        # extra bytes to pad with
        count = bound - (len(data) % bound)
        # bytes(count) only works on Python 3
        pad = (" " * count).encode("utf-8")
        # combine the padding and data
        result = b"".join([data, pad])
        # we should always divide evenly
        if tol.strict and (len(result) % bound) != 0:
            raise ValueError("byte_pad failed!")
        return result
    return data


def _append_path(path, name, tree, buffer_items):
    """
    Append a 2D or 3D path to the scene structure and put the
    data into buffer_items.

    Parameters
    -------------
    path : trimesh.Path2D or trimesh.Path3D
      Source geometry
    name : str
      Name of geometry
    tree : dict
      Will be updated with data from path
    buffer_items
      Will have buffer appended with path data
    """

    # convert the path to the unnamed args for
    # a pyglet vertex list
    vxlist = rendering.path_to_vertexlist(path)

    # of the count of things to export is zero exit early
    if vxlist[0] == 0:
        return

    # TODO add color support to Path object
    # this is just exporting everying as black
    try:
        material_idx = tree["materials"].index(_default_material)
    except ValueError:
        material_idx = len(tree["materials"])
        tree["materials"].append(_default_material)

    # data is the second value of the fifth field
    # which is a (data type, data) tuple
    acc_vertex = _data_append(
        acc=tree["accessors"],
        buff=buffer_items,
        blob={"componentType": 5126, "type": "VEC3", "byteOffset": 0},
        data=vxlist[4][1].astype(float32),
    )

    current = {
        "name": name,
        "primitives": [
            {
                "attributes": {"POSITION": acc_vertex},
                "mode": _GL_LINES,  # i.e. 1
                "material": material_idx,
            }
        ],
    }

    # if units are defined, store them as an extra:
    # https://github.com/KhronosGroup/glTF/tree/master/extensions
    try:
        current["extras"] = _jsonify(path.metadata)
    except BaseException:
        log.debug("failed to serialize metadata, dropping!", exc_info=True)

    if path.colors is not None:
        acc_color = _data_append(
            acc=tree["accessors"],
            buff=buffer_items,
            blob={
                "componentType": 5121,
                "normalized": True,
                "type": "VEC4",
                "byteOffset": 0,
            },
            data=np.array(vxlist[5][1]).astype(uint8),
        )
        # add color to attributes
        current["primitives"][0]["attributes"]["COLOR_0"] = acc_color

    # for each attribute with a leading underscore, assign them to path
    # vertex_attributes
    for key, attrib in path.vertex_attributes.items():
        # Application specific attributes must be
        # prefixed with an underscore
        if not key.startswith("_"):
            key = "_" + key

        # GLTF has no floating point type larger than 32 bits so clip
        # any float64 or larger to float32
        if attrib.dtype.kind == "f" and attrib.dtype.itemsize > 4:
            data = attrib.astype(np.float32)
        else:
            data = attrib

        if not all(util.is_instance_named(e, "Line") for e in path.entities):
            log.warning(
                f"Vertex attributes are only supported for Line entities, skipping `{key}`"
            )
            continue

        data_discretized = np.array(
            [util.stack_lines(e.discrete(data)) for e in path.entities]
        )
        stacked_data = data_discretized.reshape((-1,))

        # store custom vertex attributes
        current["primitives"][0]["attributes"][key] = _data_append(
            acc=tree["accessors"],
            buff=buffer_items,
            blob=_build_accessor(stacked_data),
            data=stacked_data,
        )

    tree["meshes"].append(current)


def _append_point(points, name, tree, buffer_items):
    """
    Append a 2D or 3D pointCloud to the scene structure and
    put the data into buffer_items.

    Parameters
    -------------
    points : trimesh.PointCloud
      Source geometry
    name : str
      Name of geometry
    tree : dict
      Will be updated with data from points
    buffer_items
      Will have buffer appended with points data
    """

    # convert the points to the unnamed args for
    # a pyglet vertex list
    vxlist = rendering.points_to_vertexlist(points=points.vertices, colors=points.colors)

    # data is the second value of the fifth field
    # which is a (data type, data) tuple
    acc_vertex = _data_append(
        acc=tree["accessors"],
        buff=buffer_items,
        blob={"componentType": 5126, "type": "VEC3", "byteOffset": 0},
        data=vxlist[4][1].astype(float32),
    )
    current = {
        "name": name,
        "primitives": [
            {
                "attributes": {"POSITION": acc_vertex},
                "mode": _GL_POINTS,
                "material": len(tree["materials"]),
            }
        ],
    }

    # TODO add color support to Points object
    # this is just exporting everying as black
    tree["materials"].append(_default_material)

    if len(np.shape(points.colors)) == 2:
        # colors may be returned as "c3f" or other RGBA
        color_type, color_data = vxlist[5]
        if "3" in color_type:
            kind = "VEC3"
        elif "4" in color_type:
            kind = "VEC4"
        else:
            raise ValueError("unknown color: %s", color_type)
        acc_color = _data_append(
            acc=tree["accessors"],
            buff=buffer_items,
            blob={
                "componentType": 5121,
                "count": vxlist[0],
                "normalized": True,
                "type": kind,
                "byteOffset": 0,
            },
            data=np.array(color_data).astype(uint8),
        )
        # add color to attributes
        current["primitives"][0]["attributes"]["COLOR_0"] = acc_color
    tree["meshes"].append(current)


def _parse_textures(header, views, resolver=None):
    try:
        import PIL.Image
    except ImportError:
        log.debug("unable to load textures without pillow!")
        return None

    # load any images
    images = None
    if "images" in header:
        # images are referenced by index
        images = [None] * len(header["images"])
        # loop through images
        for i, img in enumerate(header["images"]):
            if img.get("mimeType", "") == "image/ktx2":
                log.debug("`image/ktx2` textures are unsupported, skipping!")
                continue
            # get the bytes representing an image
            if "bufferView" in img:
                blob = views[img["bufferView"]]
            elif "uri" in img:
                try:
                    # will get bytes from filesystem or base64 URI
                    blob = _uri_to_bytes(uri=img["uri"], resolver=resolver)
                except BaseException:
                    log.debug(f"unable to load image from: {img.keys()}", exc_info=True)
                    continue
            else:
                log.debug(f"unable to load image from: {img.keys()}")
                continue
            # i.e. 'image/jpeg'
            # mime = img['mimeType']
            try:
                # load the buffer into a PIL image
                images[i] = PIL.Image.open(util.wrap_as_stream(blob))
            except BaseException:
                log.debug("failed to load image!", exc_info=True)
    return images


def _parse_materials(header, views, resolver=None):
    """
    Convert materials and images stored in a GLTF header
    and buffer views to PBRMaterial objects.

    Parameters
    ------------
    header : dict
      Contains layout of file
    views : (n,) bytes
      Raw data

    Returns
    ------------
    materials : list
      List of trimesh.visual.texture.Material objects
    """

    def parse_textures(*, data):
        result = {}
        for k, v in data.items():
            if isinstance(v, (list, tuple)):
                # colors are always float 0.0 - 1.0 in GLTF
                result[k] = np.array(v, dtype=np.float64)
            elif not isinstance(v, dict):
                result[k] = v
            elif images is not None and "index" in v:
                try:
                    index = None
                    texture = header["textures"][v["index"]]
                    # Handle texture extensions through registry
                    if tex_ext := texture.get("extensions"):
                        index = handle_extensions(
                            extensions=tex_ext, scope="texture_source"
                        )

                    if index is None:
                        # fall back to standard source key
                        index = texture.get("source")
                    if index is not None:
                        result[k] = images[index]
                except BaseException:
                    log.debug("unable to store texture", exc_info=True)
        return result

    images = _parse_textures(header, views, resolver)

    # store materials which reference images
    materials = []
    if "materials" in header:
        for mat in header["materials"]:
            # flatten key structure so we can loop it
            loopable = mat.copy()
            # this key stores another dict of crap
            if "pbrMetallicRoughness" in loopable:
                # add keys of keys to top level dict
                loopable.update(loopable.pop("pbrMetallicRoughness"))

            # Handle material extensions through registry
            if mat_extensions := mat.get("extensions"):
                ext_results = handle_extensions(
                    extensions=mat_extensions,
                    scope="material",
                    parse_textures=parse_textures,
                    images=images,
                )
                # Flatten extension results into the material parameters
                for ext_result in ext_results.values():
                    if isinstance(ext_result, dict):
                        loopable.update(ext_result)

            # save flattened keys we can use for kwargs
            pbr = parse_textures(data=loopable)
            # create a PBR material object for the GLTF material
            materials.append(visual.material.PBRMaterial(**pbr))

    return materials


def _read_buffers(
    header: Dict,
    buffers: List[bytes],
    mesh_kwargs: Dict,
    resolver: Optional[ResolverLike],
    ignore_broken: bool = False,
    merge_primitives: bool = False,
    skip_materials: bool = False,
):
    """
    Given binary data and a layout return the
    kwargs to create a scene object.

    Parameters
    -----------
    header : dict
      With GLTF keys
    buffers : list of bytes
      Stored data
    mesh_kwargs : dict
      To be passed to the mesh constructor.
    ignore_broken : bool
      If there is a mesh we can't load and this
      is True don't raise an exception but return
      a partial result
    merge_primitives : bool
      If true, combine primitives into a single mesh.
    skip_materials : bool
      If true, will not load materials (if present).
    resolver : trimesh.resolvers.Resolver
      Resolver to load referenced assets

    Returns
    -----------
    kwargs : dict
      Can be passed to load_kwargs for a trimesh.Scene
    """

    if "bufferViews" in header:
        # split buffer data into buffer views
        views = [None] * len(header["bufferViews"])
        for i, view in enumerate(header["bufferViews"]):
            if "byteOffset" in view:
                start = view["byteOffset"]
            else:
                start = 0
            end = start + view["byteLength"]
            views[i] = buffers[view["buffer"]][start:end]
            assert len(views[i]) == view["byteLength"]
        # load data from buffers into numpy arrays
        # using the layout described by accessors
        access = [None] * len(header["accessors"])
        for index, a in enumerate(header["accessors"]):
            # number of items
            count = a["count"]
            # what is the datatype
            dtype = np.dtype(_dtypes[a["componentType"]])
            # basically how many columns
            # for types like (4, 4)
            per_item = _shapes[a["type"]]
            # use reported count to generate shape
            shape = np.append(count, per_item)
            # number of items when flattened
            # i.e. a (4, 4) MAT4 has 16
            per_count = np.abs(np.prod(per_item))
            if "bufferView" in a:
                # data was stored in a buffer view so get raw bytes

                # load the bytes data into correct dtype and shape
                buffer_view = header["bufferViews"][a["bufferView"]]

                # is the accessor offset in a buffer
                # will include the start, length, and offset
                # but not the bytestride as that is easier to do
                # in numpy rather than in python looping
                data = views[a["bufferView"]]

                # both bufferView *and* accessors are allowed
                # to have a byteOffset
                start = a.get("byteOffset", 0)

                if "byteStride" in buffer_view:
                    # how many bytes for each chunk
                    stride = buffer_view["byteStride"]
                    # we want to get the bytes for every row
                    per_row = per_count * dtype.itemsize
                    # the total block we're looking at
                    length = (count - 1) * stride + per_row
                    # apply as_strided for fast construction of strided array
                    # and copy to ensure contiguous layout
                    assert stride > 0, "byteStride should be positive"
                    assert 0 <= start <= start + length <= len(data)
                    access[index] = np.array(
                        np.lib.stride_tricks.as_strided(
                            np.frombuffer(
                                data, dtype=np.uint8, offset=start, count=length
                            ),
                            [count, per_row],
                            [stride, 1],
                        )
                        .view(dtype)
                        .reshape(shape)
                    )
                else:
                    # length is the number of bytes per item times total
                    length = dtype.itemsize * count * per_count
                    access[index] = np.frombuffer(
                        data[start : start + length], dtype=dtype
                    ).reshape(shape)
            else:
                # a "sparse" accessor should be initialized as zeros
                access[index] = np.zeros(count * per_count, dtype=dtype).reshape(shape)

        # possibly load images and textures into material objects
        if skip_materials:
            materials = []
        else:
            materials = _parse_materials(header, views=views, resolver=resolver)

    mesh_prim = defaultdict(list)
    # load data from accessors into Trimesh objects
    meshes = OrderedDict()

    # keep track of how many times each name has been attempted to
    # be inserted to avoid a potentially slow search through our
    # dict of names
    name_counts = {}
    for index, m in enumerate(header.get("meshes", [])):
        try:
            # GLTF spec indicates implicit units are meters
            metadata = {
                "units": "meters",
                "from_gltf_primitive": len(m["primitives"]) > 1,
            }

            # try to load all mesh metadata
            if isinstance(m.get("extras"), dict):
                metadata.update(m["extras"])

            # put any mesh extensions in a field of the metadata
            if "extensions" in m:
                metadata["gltf_extensions"] = m["extensions"]

            for p in m["primitives"]:
                # Handle primitive preprocessing extensions (e.g. Draco decompression)
                # These run before reading accessors since they may modify them
                if prim_extensions := p.get("extensions"):
                    handle_extensions(
                        extensions=prim_extensions,
                        scope="primitive_preprocess",
                        primitive=p,
                        accessors=access,
                        views=views,
                    )

                # if we don't have a triangular mesh continue
                # if not specified assume it is a mesh
                kwargs = deepcopy(mesh_kwargs)
                if kwargs.get("metadata", None) is None:
                    kwargs["metadata"] = {}
                if "process" not in kwargs:
                    kwargs["process"] = False
                kwargs["metadata"].update(metadata)
                # i.e. GL_LINES, GL_TRIANGLES, etc
                # specification says the default mode is GL_TRIANGLES
                mode = p.get("mode", _GL_TRIANGLES)
                # colors, normals, etc
                attr = p["attributes"]
                # create a unique mesh name per- primitive
                name = m.get("name", "GLTF")
                # make name unique across multiple meshes
                name = unique_name(name, meshes, counts=name_counts)

                if mode == _GL_LINES:
                    # load GL_LINES into a Path object
                    from ...path.entities import Line

                    kwargs["vertices"] = access[attr["POSITION"]]
                    kwargs["entities"] = [Line(points=np.arange(len(kwargs["vertices"])))]

                    # custom attributes starting with a `_`
                    custom = {
                        a: access[attr[a]] for a in attr.keys() if a.startswith("_")
                    }
                    if len(custom) > 0:
                        kwargs["vertex_attributes"] = custom
                elif mode == _GL_POINTS:
                    kwargs["vertices"] = access[attr["POSITION"]]
                    visuals = None
                    if "COLOR_0" in attr:
                        try:
                            # try to load vertex colors from the accessors
                            colors = access[attr["COLOR_0"]]
                            if len(colors) == len(kwargs["vertices"]):
                                if visuals is None:
                                    # just pass to mesh as vertex color
                                    kwargs["vertex_colors"] = colors.copy()
                                else:
                                    # we ALSO have texture so save as vertex
                                    # attribute
                                    visuals.vertex_attributes["color"] = colors.copy()
                        except BaseException:
                            # survive failed colors
                            log.debug("failed to load colors", exc_info=True)
                    if visuals is not None:
                        kwargs["visual"] = visuals
                elif mode in (_GL_TRIANGLES, _GL_STRIP):
                    # get vertices from accessors
                    kwargs["vertices"] = access[attr["POSITION"]]
                    # get faces from accessors
                    if "indices" in p:
                        if mode == _GL_STRIP:
                            # this is triangle strips
                            flat = access[p["indices"]].reshape(-1)
                            kwargs["faces"] = triangle_strips_to_faces([flat])
                        else:
                            kwargs["faces"] = access[p["indices"]].reshape((-1, 3))
                    else:
                        # indices are apparently optional and we are supposed to
                        # do the same thing as webGL drawArrays?
                        if mode == _GL_STRIP:
                            kwargs["faces"] = triangle_strips_to_faces(
                                np.array([np.arange(len(kwargs["vertices"]))])
                            )
                        else:
                            # GL_TRIANGLES
                            kwargs["faces"] = np.arange(
                                len(kwargs["vertices"]), dtype=np.int64
                            ).reshape((-1, 3))

                    if "NORMAL" in attr:
                        # vertex normals are specified
                        kwargs["vertex_normals"] = access[attr["NORMAL"]]
                        # do we have UV coordinates
                    visuals = None
                    if "material" in p and not skip_materials:
                        if materials is None:
                            log.debug("no materials! `pip install pillow`")
                        else:
                            uv = None
                            if "TEXCOORD_0" in attr:
                                # flip UV's top- bottom to move origin to lower-left:
                                # https://github.com/KhronosGroup/glTF/issues/1021
                                uv = access[attr["TEXCOORD_0"]].copy()
                                uv[:, 1] = 1.0 - uv[:, 1]
                                # create a texture visual
                            visuals = visual.texture.TextureVisuals(
                                uv=uv, material=materials[p["material"]]
                            )

                    if "COLOR_0" in attr:
                        try:
                            # try to load vertex colors from the accessors
                            colors = access[attr["COLOR_0"]]
                            if len(colors) == len(kwargs["vertices"]):
                                if visuals is None:
                                    # just pass to mesh as vertex color
                                    kwargs["vertex_colors"] = colors.copy()
                                else:
                                    # we ALSO have texture so save as vertex
                                    # attribute
                                    visuals.vertex_attributes["color"] = colors.copy()
                        except BaseException:
                            # survive failed colors
                            log.debug("failed to load colors", exc_info=True)
                    if visuals is not None:
                        kwargs["visual"] = visuals

                    # custom attributes starting with a `_`
                    custom = {
                        a: access[attr[a]] for a in attr.keys() if a.startswith("_")
                    }
                    if len(custom) > 0:
                        kwargs["vertex_attributes"] = custom

                    # Process primitive-level extensions through registry
                    if prim_extensions := p.get("extensions"):
                        handle_extensions(
                            extensions=prim_extensions,
                            scope="primitive",
                            primitive=p,
                            mesh_kwargs=kwargs,
                            accessors=access,
                        )
                else:
                    log.debug("skipping primitive with mode %s!", mode)
                    continue
                # this should absolutely not be stomping on itself
                assert name not in meshes
                meshes[name] = kwargs
                mesh_prim[index].append(name)
        except BaseException as E:
            if ignore_broken:
                log.debug("failed to load mesh", exc_info=True)
            else:
                raise E

    # sometimes GLTF "meshes" come with multiple "primitives"
    # by default we return one Trimesh object per "primitive"
    # but if merge_primitives is True we combine the primitives
    # for the "mesh" into a single Trimesh object
    if merge_primitives:
        # if we are only returning one Trimesh object
        # replace `mesh_prim` with updated values
        mesh_prim_replace = {}
        # these are the names of meshes we need to remove
        mesh_pop = set()
        for mesh_index, names in mesh_prim.items():
            if len(names) <= 1:
                mesh_prim_replace[mesh_index] = names
                continue

            # just take the shortest name option available
            name = min(names)
            # remove the other meshes after we're done looping
            # since we're reusing the shortest one don't pop
            # that as we'll be overwriting it with the combined
            mesh_pop.update(set(names).difference([name]))

            # get all meshes for this group
            current = [meshes[n] for n in names]
            v_seq = [p["vertices"] for p in current]
            f_seq = [p["faces"] for p in current]
            v, f = util.append_faces(v_seq, f_seq)
            materials = [p["visual"].material for p in current]
            face_materials = []
            for i, p in enumerate(current):
                face_materials += [i] * len(p["faces"])
            visuals = visual.texture.TextureVisuals(
                material=visual.material.MultiMaterial(materials=materials),
                face_materials=face_materials,
            )
            if "metadata" in meshes[names[0]]:
                metadata = meshes[names[0]]["metadata"]
            else:
                metadata = {}
            meshes[name] = {
                "vertices": v,
                "faces": f,
                "visual": visuals,
                "metadata": metadata,
                "process": False,
            }
            mesh_prim_replace[mesh_index] = [name]
        # avoid altering inside loop
        mesh_prim = mesh_prim_replace
        # remove outdated meshes
        [meshes.pop(p, None) for p in mesh_pop]

    # make it easier to reference nodes
    nodes = header.get("nodes", [])
    # nodes are referenced by index
    # save their string names if they have one
    # we have to accumulate in a for loop opposed
    # to a dict comprehension as it will be checking
    # the mutated dict in every loop
    name_index = {}
    name_counts = {}

    # store the mapping of node name to index and the inverse
    # name_index: {name: index}
    for i, n in enumerate(nodes):
        name_index[unique_name(n.get("name", str(i)), name_index, counts=name_counts)] = i
    # names: {index: name}
    names = {v: k for k, v in name_index.items()}

    # the GLTF is allowed to declare a base frame, should we have used that?
    base_frame = "world"
    if base_frame in name_index:
        # todo : handle this?
        log.debug("file contains a `world` node, we may stomp on it")
    names[base_frame] = base_frame

    # visited, kwargs for scene.graph.update
    graph = deque()
    # unvisited, pairs of node indexes
    queue = deque()

    # camera(s), if they exist
    camera = None
    camera_transform = None

    if "scene" in header:
        # specify the index of scenes if specified
        scene_index = header["scene"]
    else:
        # otherwise just use the first index
        scene_index = 0

    base_frame = "world"
    if "scenes" in header:
        # start the traversal from the base frame to the roots
        for root in header["scenes"][scene_index].get("nodes", []):
            # add transform from base frame to these root nodes
            queue.append((base_frame, root))

    # make sure we don't process an edge multiple times
    consumed = set()

    # go through the nodes tree to populate
    # kwargs for scene graph loader
    while len(queue) > 0:
        # (int, int) pair of node indexes
        edge = queue.pop()

        # avoid looping forever if someone specified
        # recursive nodes
        if edge in consumed:
            continue

        consumed.add(edge)
        a, b = edge

        # dict of child node
        # parent = nodes[a]
        child = nodes[b]
        # add edges of children to be processed
        if "children" in child:
            queue.extend([(b, i) for i in child["children"]])

        # kwargs to be passed to scene.graph.update
        kwargs = {"frame_from": names[a], "frame_to": names[b]}

        # grab matrix from child
        # parent -> child relationships have matrix stored in child
        # for the transform from parent to child
        if "matrix" in child:
            kwargs["matrix"] = (
                np.array(child["matrix"], dtype=np.float64).reshape((4, 4)).T
            )
        else:
            # if no matrix set identity
            kwargs["matrix"] = _EYE

        # Now apply keyword translations
        # GLTF applies these in order: T * R * S
        if "translation" in child:
            kwargs["matrix"] = np.dot(
                kwargs["matrix"], transformations.translation_matrix(child["translation"])
            )
        if "rotation" in child:
            # GLTF rotations are stored as (4,) XYZW unit quaternions
            # we need to re- order to our quaternion style, WXYZ
            quat = np.reshape(child["rotation"], 4)[[3, 0, 1, 2]]
            # add the rotation to the matrix
            kwargs["matrix"] = np.dot(
                kwargs["matrix"], transformations.quaternion_matrix(quat)
            )
        if "scale" in child:
            # add scale to the matrix
            kwargs["matrix"] = np.dot(
                kwargs["matrix"], np.diag(np.concatenate((child["scale"], [1.0])))
            )

        # If a camera exists, create the camera and dont add the node to the graph
        # TODO only process the first camera, ignore the rest
        # TODO assumes the camera node is child of the world frame
        # TODO will only read perspective camera
        if "camera" in child and camera is None:
            cam_idx = child["camera"]
            try:
                camera = _cam_from_gltf(header["cameras"][cam_idx])
            except KeyError:
                log.debug("GLTF camera is not fully-defined")
            if camera:
                camera_transform = kwargs["matrix"]
            continue

        # treat node metadata similarly to mesh metadata
        if isinstance(child.get("extras"), dict):
            kwargs["metadata"] = child["extras"]

        # put any node extensions in a field of the metadata
        if "extensions" in child:
            if "metadata" not in kwargs:
                kwargs["metadata"] = {}
            kwargs["metadata"]["gltf_extensions"] = child["extensions"]

        if "mesh" in child:
            geometries = mesh_prim[child["mesh"]]

            # if the node has a mesh associated with it
            if len(geometries) > 1:
                # append root node
                graph.append(kwargs.copy())
                # put primitives as children
                for geom_name in geometries:
                    # save the name of the geometry
                    kwargs["geometry"] = geom_name
                    # no transformations
                    kwargs["matrix"] = _EYE
                    kwargs["frame_from"] = names[b]
                    # if we have more than one primitive assign a new UUID
                    # frame name for the primitives after the first one
                    frame_to = f"{names[b]}_{util.unique_id(length=6)}"
                    kwargs["frame_to"] = frame_to
                    # append the edge with the mesh frame
                    graph.append(kwargs.copy())
            elif len(geometries) == 1:
                kwargs["geometry"] = geometries[0]
                if "name" in child:
                    kwargs["frame_to"] = names[b]
                graph.append(kwargs.copy())
        else:
            # if the node doesn't have any geometry just add
            graph.append(kwargs)

    # kwargs for load_kwargs
    result = {
        "class": "Scene",
        "geometry": meshes,
        "graph": graph,
        "base_frame": base_frame,
        "camera": camera,
        "camera_transform": camera_transform,
        "metadata": {},
    }

    try:
        # load any scene extras into scene.metadata
        # use a try except to avoid nested key checks
        result["metadata"].update(header["scenes"][header["scene"]]["extras"])
    except BaseException:
        pass
    try:
        # load any scene extensions into a field of scene.metadata
        # use a try except to avoid nested key checks
        result["metadata"]["gltf_extensions"] = header["extensions"]
    except BaseException:
        pass

    return result


def _cam_from_gltf(cam):
    """
    Convert a gltf perspective camera to trimesh.

    The retrieved camera will have default resolution, since the gltf specification
    does not contain it.

    If the camera is not perspective will return None.
    If the camera is perspective but is missing fields, will raise `KeyError`

    Parameters
    ------------
    cam : dict
      Camera represented as a dictionary according to glTF

    Returns
    -------------
    camera : trimesh.scene.cameras.Camera
      Trimesh camera object
    """
    if "perspective" not in cam:
        return
    name = cam.get("name")
    znear = cam["perspective"]["znear"]
    aspect_ratio = cam["perspective"]["aspectRatio"]
    yfov = np.degrees(cam["perspective"]["yfov"])

    fov = (aspect_ratio * yfov, yfov)

    return Camera(name=name, fov=fov, z_near=znear)


def _convert_camera(camera):
    """
    Convert a trimesh camera to a GLTF camera.

    Parameters
    ------------
    camera : trimesh.scene.cameras.Camera
      Trimesh camera object

    Returns
    -------------
    gltf_camera : dict
      Camera represented as a GLTF dict
    """
    result = {
        "name": camera.name,
        "type": "perspective",
        "perspective": {
            "aspectRatio": camera.fov[0] / camera.fov[1],
            "yfov": np.radians(camera.fov[1]),
            "znear": float(camera.z_near),
        },
    }
    return result


def _append_image(img, tree, buffer_items, extension_webp):
    """
    Append a PIL image to a GLTF2.0 tree.

    Parameters
    ------------
    img : PIL.Image
      Image object
    tree : dict
      GLTF 2.0 format tree
    buffer_items : (n,) bytes
      Binary blobs containing data
    extension_webp : bool
      Export textures as webP (using glTF's EXT_texture_webp extension).

    Returns
    -----------
    index : int or None
      The index of the image in the tree
      None if image append failed for any reason
    """
    # probably not a PIL image so exit
    if not hasattr(img, "format"):
        return None

    if extension_webp:
        # support WebP if extension is specified
        save_as = "WEBP"
    elif img.format == "JPEG":
        # don't re-encode JPEGs
        save_as = "JPEG"
    else:
        # for everything else just use PNG
        save_as = "png"

    # get the image data into a bytes object
    with util.BytesIO() as f:
        img.save(f, format=save_as)
        f.seek(0)
        data = f.read()

    index = _buffer_append(buffer_items, data)
    # append buffer index and the GLTF-acceptable mimetype
    tree["images"].append({"bufferView": index, "mimeType": f"image/{save_as.lower()}"})

    # index is length minus one
    return len(tree["images"]) - 1


def _append_material(mat, tree, buffer_items, mat_hashes, extension_webp):
    """
    Add passed PBRMaterial as GLTF 2.0 specification JSON
    serializable data:
    - images are added to `tree['images']`
    - texture is added to `tree['texture']`
    - material is added to `tree['materials']`

    Parameters
    ------------
    mat : trimesh.visual.materials.PBRMaterials
      Source material to convert
    tree : dict
      GLTF header blob
    buffer_items : (n,) bytes
      Binary blobs with various data
    mat_hashes : dict
      Which materials have already been added
      Stored as { hashed : material index }
    extension_webp : bool
      Export textures as webP using EXT_texture_webp extension.

    Returns
    -------------
    index : int
      Index at which material was added
    """
    # materials are hashable
    hashed = hash(mat)
    # check stored material indexes to see if material
    # has already been added
    if mat_hashes is not None and hashed in mat_hashes:
        return mat_hashes[hashed]

    # convert passed input to PBR if necessary
    if hasattr(mat, "to_pbr"):
        as_pbr = mat.to_pbr()
    else:
        as_pbr = mat

    # a default PBR metallic material
    result = {"pbrMetallicRoughness": {}}
    try:
        # try to convert base color to (4,) float color
        result["baseColorFactor"] = (
            visual.color.to_float(as_pbr.baseColorFactor).reshape(4).tolist()
        )
    except BaseException:
        pass

    try:
        result["emissiveFactor"] = as_pbr.emissiveFactor.reshape(3).tolist()
    except BaseException:
        pass

    # if name is defined, export
    if isinstance(as_pbr.name, str):
        result["name"] = as_pbr.name

    # if alphaMode is defined, export
    if isinstance(as_pbr.alphaMode, str):
        result["alphaMode"] = as_pbr.alphaMode

    # if alphaCutoff is defined, export
    if isinstance(as_pbr.alphaCutoff, float):
        result["alphaCutoff"] = as_pbr.alphaCutoff

    # if doubleSided is defined, export
    if isinstance(as_pbr.doubleSided, bool):
        result["doubleSided"] = as_pbr.doubleSided

    # if scalars are defined correctly export
    if isinstance(as_pbr.metallicFactor, float):
        result["metallicFactor"] = as_pbr.metallicFactor
    if isinstance(as_pbr.roughnessFactor, float):
        result["roughnessFactor"] = as_pbr.roughnessFactor

    # which keys of the PBRMaterial are images
    image_mapping = {
        "baseColorTexture": as_pbr.baseColorTexture,
        "emissiveTexture": as_pbr.emissiveTexture,
        "normalTexture": as_pbr.normalTexture,
        "occlusionTexture": as_pbr.occlusionTexture,
        "metallicRoughnessTexture": as_pbr.metallicRoughnessTexture,
    }

    for key, img in image_mapping.items():
        if img is None:
            continue
        # try adding the base image to the export object
        index = _append_image(
            img=img, tree=tree, buffer_items=buffer_items, extension_webp=extension_webp
        )
        # if the image was added successfully it will return index
        # if it failed for any reason, it will return None
        if index is not None:
            # add a reference to the base color texture
            result[key] = {"index": len(tree["textures"])}

            # add texture object, optionally using EXT_texture_webp
            if extension_webp:
                tree["textures"].append(
                    {"extensions": {"EXT_texture_webp": {"source": index}}}
                )
            else:
                tree["textures"].append({"source": index})

    # for our PBRMaterial object we flatten all keys
    # however GLTF would like some of them under the
    # "pbrMetallicRoughness" key
    pbr_subset = [
        "baseColorTexture",
        "baseColorFactor",
        "roughnessFactor",
        "metallicFactor",
        "metallicRoughnessTexture",
    ]
    # move keys down a level
    for key in pbr_subset:
        if key in result:
            result["pbrMetallicRoughness"][key] = result.pop(key)

    # if we didn't have any PBR keys remove the empty key
    if len(result["pbrMetallicRoughness"]) == 0:
        result.pop("pbrMetallicRoughness")

    # which index are we inserting material at
    index = len(tree["materials"])
    # add the material to the data structure
    tree["materials"].append(result)
    # add the material index in-place
    mat_hashes[hashed] = index

    return index


def validate(header):
    """
    Validate a GLTF 2.0 header against the schema.

    Returns result from:
    `jsonschema.validate(header, schema=get_schema())`

    Parameters
    -------------
    header : dict
      Populated GLTF 2.0 header

    Raises
    --------------
    err : jsonschema.exceptions.ValidationError
      If the tree is an invalid GLTF2.0 header
    """
    # a soft dependency
    import jsonschema

    # will do the reference replacement
    schema = get_schema()
    # validate the passed header against the schema
    valid = jsonschema.validate(header, schema=schema)

    return valid


def get_schema():
    """
    Get a copy of the GLTF 2.0 schema with references resolved.

    Returns
    ------------
    schema : dict
      A copy of the GLTF 2.0 schema without external references.
    """
    # replace references
    # get zip resolver to access referenced assets
    from ...schemas import resolve

    # get a blob of a zip file including the GLTF 2.0 schema
    stream = resources.get_stream("schema/gltf2.schema.zip")
    # get the zip file as a dict keyed by file name
    archive = util.decompress(stream, "zip")
    # get a resolver object for accessing the schema
    resolver = ZipResolver(archive)
    # get a loaded dict from the base file
    unresolved = json.loads(util.decode_text(resolver.get("glTF.schema.json")))
    # resolve `$ref` references to other files in the schema
    schema = resolve(unresolved, resolver=resolver)

    return schema


# exporters
_gltf_loaders = {"glb": load_glb, "gltf": load_gltf}
