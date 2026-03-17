[![trimesh](https://trimesh.org/_static/images/logotype-a.svg)](http://trimesh.org)

-----------
[![Github Actions](https://github.com/mikedh/trimesh/workflows/Release%20Trimesh/badge.svg)](https://github.com/mikedh/trimesh/actions) [![codecov](https://codecov.io/gh/mikedh/trimesh/branch/main/graph/badge.svg?token=4PVRQXyl2h)](https://codecov.io/gh/mikedh/trimesh)  [![Docker Image Version (latest by date)](https://img.shields.io/docker/v/trimesh/trimesh?label=docker&sort=semver)](https://hub.docker.com/r/trimesh/trimesh/tags) [![PyPI version](https://badge.fury.io/py/trimesh.svg)](https://badge.fury.io/py/trimesh)


Trimesh is a pure Python 3.8+ library for loading and using [triangular meshes](https://en.wikipedia.org/wiki/Triangle_mesh) with an emphasis on watertight surfaces. The goal of the library is to provide a full featured and well tested Trimesh object which allows for easy manipulation and analysis, in the style of the Polygon object in the [Shapely library](https://github.com/Toblerity/Shapely).

The API is mostly stable, but this should not be relied on and is not guaranteed: install a specific version if you plan on deploying something using `trimesh`.

Pull requests are appreciated and responded to promptly! If you'd like to contribute, here a quick [development and contributing guide.](https://trimesh.org/contributing.html)


## Basic Installation

Keeping `trimesh` easy to install is a core goal, thus the *only* hard dependency is [`numpy`](http://www.numpy.org/). Installing other packages adds functionality but is not required. For the easiest install with just `numpy`:

```bash
pip install trimesh
```

The minimal install can load many supported formats (STL, PLY, OBJ, GLTF/GLB) into `numpy.ndarray` values. More functionality is available when [soft dependencies are installed](https://trimesh.org/install#dependency-overview), including convex hulls (`scipy`), graph operations (`networkx`), fast ray queries (`embreex`), vector path handling (`shapely` and `rtree`), XML formats like 3DXML/XAML/3MF (`lxml`), preview windows (`pyglet`), faster cache checks (`xxhash`), etc.

To install `trimesh` with the soft dependencies that generally install cleanly from binaries on Linux x86_64, MacOS ARM, and Windows x86_64 using `pip`:
```bash
pip install trimesh[easy]
```

If you are supporting a different platform or are freezing dependencies for an application we recommend you do not use extras, i.e. depend on `trimesh scipy` versus `trimesh[easy]`. Further information is available in the [advanced installation documentation](https://trimesh.org/install.html).

## Quick Start

Here is an example of loading a mesh from file and colorizing its faces ([nicely formatted notebook version](https://trimesh.org/quick_start.html) of this example.

```python
import numpy as np
import trimesh

# attach to logger so trimesh messages will be printed to console
trimesh.util.attach_to_log()

# mesh objects can be created from existing faces and vertex data
mesh = trimesh.Trimesh(vertices=[[0, 0, 0], [0, 0, 1], [0, 1, 0]],
                       faces=[[0, 1, 2]])

# by default, Trimesh will do a light processing, which will
# remove any NaN values and merge vertices that share position
# if you want to not do this on load, you can pass `process=False`
mesh = trimesh.Trimesh(vertices=[[0, 0, 0], [0, 0, 1], [0, 1, 0]],
                       faces=[[0, 1, 2]],
                       process=False)

# some formats like `glb` represent multiple meshes with multiple instances
# and `load_mesh` will concatenate irreversibly, load it as a Scene
# if you need instance information:
#   `scene = trimesh.load_scene('models/CesiumMilkTruck.glb')`
mesh = trimesh.load_mesh('models/CesiumMilkTruck.glb')

# is the current mesh watertight?
mesh.is_watertight

# what's the euler number for the mesh?
mesh.euler_number

# the convex hull is another Trimesh object that is available as a property
# lets compare the volume of our mesh with the volume of its convex hull
print(mesh.volume / mesh.convex_hull.volume)

# since the mesh is watertight it means there is a volume
# with a center of mass calculated from a surface integral approach
# which we can set as the origin for our mesh. It's perfectly fine to
# alter the vertices directly:
#   mesh.vertices -= mesh.center_mass
# although this will completely clear the cache including face normals
# as we don't know that they're still valid. Using the translation
# method will try to save cached values that are still valid:
mesh.apply_translation(-mesh.center_mass)


# what's the (3, 3) moment of inertia for the mesh?
mesh.moment_inertia

# if there are multiple bodies in the mesh we can split the mesh by
# connected components of face adjacency
# since this example mesh is a single watertight body we get a list of one mesh
mesh.split()

# facets are groups of coplanar adjacent faces
# set each facet to a random color
# colors are 8 bit RGBA by default (n, 4) np.uint8
for facet in mesh.facets:
    mesh.visual.face_colors[facet] = trimesh.visual.random_color()

# preview mesh in an opengl window if you installed pyglet and scipy with pip
mesh.show()

# transform method can be passed a (4, 4) matrix and will cleanly apply the transform
mesh.apply_transform(trimesh.transformations.random_rotation_matrix())

# axis aligned bounding box is available
mesh.bounding_box.extents

# a minimum volume oriented bounding box also available
# primitives are subclasses of Trimesh objects which automatically generate
# faces and vertices from data stored in the 'primitive' attribute
mesh.bounding_box_oriented.primitive.extents
mesh.bounding_box_oriented.primitive.transform

# show the mesh appended with its oriented bounding box
# the bounding box is a trimesh.primitives.Box object, which subclasses
# Trimesh and lazily evaluates to fill in vertices and faces when requested
# (press w in viewer to see triangles)
(mesh + mesh.bounding_box_oriented).show()

# bounding spheres and bounding cylinders of meshes are also
# available, and will be the minimum volume version of each
# except in certain degenerate cases, where they will be no worse
# than a least squares fit version of the primitive.
print(mesh.bounding_box_oriented.volume,
      mesh.bounding_cylinder.volume,
      mesh.bounding_sphere.volume)

```

## Features

* Import meshes from binary/ASCII STL, Wavefront OBJ, ASCII OFF, binary/ASCII PLY, GLTF/GLB 2.0, 3MF, XAML, 3DXML, etc.
* Export meshes as GLB/GLTF, binary STL, binary PLY, ASCII OFF, OBJ, COLLADA, etc.
* Import and export 2D or 3D vector paths with DXF or SVG files
* Preview meshes using an OpenGL `pyglet` window, or in-line in jupyter or marimo notebooks using `three.js`
* Automatic hashing from a subclassed numpy array for change tracking using MD5, zlib CRC, or xxhash, and internal caching of expensive values.
* Calculate face adjacencies, face angles, vertex defects, convex hulls, etc.
* Calculate cross sections for a 2D outline, or slice a mesh for a 3D remainder mesh, i.e. slicing for 3D-printing.
* Split mesh based on face connectivity using networkx, or scipy.sparse
* Calculate mass properties, including volume, center of mass, moment of inertia, principal components of inertia, etc. 
* Repair simple problems with triangle winding, normals, and quad/triangle holes
* Compute rotation/translation/tessellation invariant identifier and find duplicate meshes
* Check if a mesh is watertight, convex, etc.
* Sample the surface of a mesh
* Ray-mesh queries including location, triangle index, etc.
* Boolean operations on meshes (intersection, union, difference) using Manifold3D or Blender.
* Voxelize watertight meshes
* Smooth watertight meshes using Laplacian smoothing algorithms (Classic, Taubin, Humphrey)
* Subdivide faces of a mesh
* Approximate minimum volume oriented bounding boxes and spheres for meshes.
* Calculate nearest point on mesh surface and signed distance
* Primitive objects (Box, Cylinder, Sphere, Extrusion) which are subclassed Trimesh objects and have all the same features (inertia, viewers, etc)
* Simple scene graph and transform tree which can be rendered (pyglet window, three.js in a jupyter/marimo notebook or exported.
* Many utility functions, like transforming points, unitizing vectors, aligning vectors, tracking numpy arrays for changes, grouping rows, etc.

## Additional Notes

- Check out some cool stuff people have done in the [GitHub network](https://github.com/mikedh/trimesh/network/dependents).
- Generally `trimesh` API changes should have a one-year period of [printing a `warnings.DeprecationWarning`](https://trimesh.org/contributing.html#deprecations) although that's not always possible (i.e. the pyglet2 viewer rewrite that's been back-burnered for several years.) 
- Docker containers are available on Docker Hub as [`trimesh/trimesh`](https://hub.docker.com/r/trimesh/trimesh/tags) and there's a [container guide](https://trimesh.org/docker.html) in the docs.
- If you're choosing which format to use, you may want to try [GLB](https://trimesh.org/formats.html) as a fast modern option.
