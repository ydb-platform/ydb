#  Copyright (c) 2022, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Union, Iterable, Sequence, Optional
from pathlib import Path
import enum
import platform
import shutil
import subprocess
from uuid import uuid4
import tempfile

import ezdxf
from ezdxf.math import Matrix44, UVec, Vec3, Vec2
from ezdxf.render import MeshBuilder, MeshTransformer
from ezdxf.addons import meshex


class Operation(enum.Enum):
    union = 0
    difference = 1
    intersection = 2


CMD = "openscad"
UNION = Operation.union
DIFFERENCE = Operation.difference
INTERSECTION = Operation.intersection


def get_openscad_path() -> str:
    if platform.system() in ("Linux", "Darwin"):
        return CMD
    else:
        return ezdxf.options.get("openscad-addon", "win_exec_path").strip('"')


def is_installed() -> bool:
    """Returns ``True`` if OpenSCAD is installed.

    Searches on Windows the path stored in the options as "win_exec_path" in section
    "[openscad-addon]" which is "C:\\Program Files\\OpenSCAD\\openscad.exe" by default.

    Searches the "openscad" command on Linux and macOS.

    """
    if platform.system() in ("Linux", "Darwin"):
        return shutil.which(CMD) is not None
    return Path(get_openscad_path()).exists()


def run(script: str, exec_path: Optional[str] = None) -> MeshTransformer:
    """Executes the given `script` by OpenSCAD and returns the result mesh as
    :class:`~ezdxf.render.MeshTransformer`.

    Args:
        script: the OpenSCAD script as string
        exec_path: path to the executable as string or ``None`` to use the
            default installation path

    """
    if exec_path is None:
        exec_path = get_openscad_path()

    workdir = Path(tempfile.gettempdir())
    uuid = str(uuid4())
    # The OFF format is more compact than the default STL format
    off_path = workdir / f"ezdxf_{uuid}.off"
    scad_path = workdir / f"ezdxf_{uuid}.scad"

    scad_path.write_text(script)
    subprocess.call(
        [
            exec_path,
            "--quiet",
            "-o",
            str(off_path),
            str(scad_path),
        ]
    )
    # Remove the OpenSCAD temp file:
    scad_path.unlink(missing_ok=True)

    new_mesh = MeshTransformer()
    # Import the OpenSCAD result from OFF file:
    if off_path.exists():
        new_mesh = meshex.off_loads(off_path.read_text())

    # Remove the OFF temp file:
    off_path.unlink(missing_ok=True)
    return new_mesh


def str_matrix44(m: Matrix44) -> str:
    # OpenSCAD uses column major order!
    import numpy as np

    def cleanup(values: Iterable) -> Iterable:
        for value in values:
            if isinstance(value, np.float64):
                yield float(value)
            else:
                yield value

    s = ", ".join([str(list(cleanup(c))) for c in m.columns()])
    return f"[{s}]"


def str_polygon(
    path: Iterable[UVec],
    holes: Optional[Sequence[Iterable[UVec]]] = None,
) -> str:
    """Returns a ``polygon()`` command as string. This is a 2D command, all
    z-axis values of the input vertices are ignored and all paths and holes
    are closed automatically.

    OpenSCAD docs: https://en.wikibooks.org/wiki/OpenSCAD_User_Manual/Using_the_2D_Subsystem#polygon

    Args:
        path: exterior path
        holes: a sequences of one or more holes as vertices

    """

    def add_vertices(vertices):
        index = len(points)
        indices = []
        vlist = Vec2.list(vertices)
        if not vlist[0].isclose(vlist[-1]):
            vlist.append(vlist[0])

        for v in vlist:
            indices.append(index)
            points.append(f"  [{v.x:g}, {v.y:g}],")
            index += 1
        return indices

    points: list[str] = []
    paths = [add_vertices(path)]
    if holes is not None:
        for hole in holes:
            paths.append(add_vertices(hole))
    lines = ["polygon(points = ["]
    lines.extend(points)
    lines.append("],")
    if holes is not None:
        lines.append("paths = [")
        for indices in paths:
            lines.append(f"  {str(indices)},")
        lines.append("],")
    lines.append("convexity = 10);")
    return "\n".join(lines)


class Script:
    def __init__(self) -> None:
        self.data: list[str] = []

    def add(self, data: str) -> None:
        """Add a string."""
        self.data.append(data)

    def add_polyhedron(self, mesh: MeshBuilder) -> None:
        """Add `mesh` as ``polyhedron()`` command.

        OpenSCAD docs: https://en.wikibooks.org/wiki/OpenSCAD_User_Manual/Primitive_Solids#polyhedron

        """
        self.add(meshex.scad_dumps(mesh))

    def add_polygon(
        self,
        path: Iterable[UVec],
        holes: Optional[Sequence[Iterable[UVec]]] = None,
    ) -> None:
        """Add a ``polygon()`` command. This is a 2D command, all
        z-axis values of the input vertices are ignored and all paths and holes
        are closed automatically.

        OpenSCAD docs: https://en.wikibooks.org/wiki/OpenSCAD_User_Manual/Using_the_2D_Subsystem#polygon

        Args:
            path: exterior path
            holes: a sequence of one or more holes as vertices, or ``None`` for no holes

        """
        self.add(str_polygon(path, holes))

    def add_multmatrix(self, m: Matrix44) -> None:
        """Add a transformation matrix of type :class:`~ezdxf.math.Matrix44` as
        ``multmatrix()`` operation.

        OpenSCAD docs: https://en.wikibooks.org/wiki/OpenSCAD_User_Manual/Transformations#multmatrix

        """
        self.add(f"multmatrix(m = {str_matrix44(m)})")  # no pending ";"

    def add_translate(self, v: UVec) -> None:
        """Add a ``translate()`` operation.

        OpenSCAD docs: https://en.wikibooks.org/wiki/OpenSCAD_User_Manual/Transformations#translate

        Args:
            v: translation vector

        """
        vec = Vec3(v)
        self.add(f"translate(v = [{vec.x:g}, {vec.y:g}, {vec.z:g}])")

    def add_rotate(self, ax: float, ay: float, az: float) -> None:
        """Add a ``rotation()`` operation.

        OpenSCAD docs: https://en.wikibooks.org/wiki/OpenSCAD_User_Manual/Transformations#rotate

        Args:
            ax: rotation about the x-axis in degrees
            ay: rotation about the y-axis in degrees
            az: rotation about the z-axis in degrees

        """
        self.add(f"rotate(a = [{ax:g}, {ay:g}, {az:g}])")

    def add_rotate_about_axis(self, a: float, v: UVec) -> None:
        """Add a ``rotation()`` operation about the given axis `v`.

        OpenSCAD docs: https://en.wikibooks.org/wiki/OpenSCAD_User_Manual/Transformations#rotate

        Args:
            a: rotation angle about axis `v` in degrees
            v: rotation axis as :class:`ezdxf.math.UVec` object

        """
        vec = Vec3(v)
        self.add(f"rotate(a = {a:g}, v = [{vec.x:g}, {vec.y:g}, {vec.z:g}])")

    def add_scale(self, sx: float, sy: float, sz: float) -> None:
        """Add a ``scale()`` operation.

        OpenSCAD docs: https://en.wikibooks.org/wiki/OpenSCAD_User_Manual/Transformations#scale

        Args:
            sx: scaling factor for the x-axis
            sy: scaling factor for the y-axis
            sz: scaling factor for the z-axis

        """
        self.add(f"scale(v = [{sx:g}, {sy:g}, {sz:g}])")

    def add_resize(
        self,
        nx: float,
        ny: float,
        nz: float,
        auto: Optional[Union[bool, tuple[bool, bool, bool]]] = None,
    ) -> None:
        """Add a ``resize()`` operation.

        OpenSCAD docs: https://en.wikibooks.org/wiki/OpenSCAD_User_Manual/Transformations#resize

        Args:
            nx: new size in x-axis
            ny: new size in y-axis
            nz: new size in z-axis
            auto: If the `auto` argument is set to ``True``, the operation
                auto-scales any 0-dimensions to match. Set the `auto` argument
                as a  3-tuple of bool values to auto-scale individual axis.

        """
        main = f"resize(newsize = [{nx:g}, {ny:g}, {nz:g}]"
        if auto is None:
            self.add(main + ")")
            return
        elif isinstance(auto, bool):
            flags = str(auto).lower()
        else:
            flags = ", ".join([str(a).lower() for a in auto])
            flags = f"[{flags}]"
        self.add(main + f", auto = {flags})")

    def add_mirror(self, v: UVec) -> None:
        """Add a ``mirror()`` operation.

        OpenSCAD docs: https://en.wikibooks.org/wiki/OpenSCAD_User_Manual/Transformations#mirror

        Args:
            v: the normal vector of a plane intersecting the origin through
                which to mirror the object

        """
        n = Vec3(v).normalize()
        self.add(f"mirror(v = [{n.x:g}, {n.y:g}, {n.z:g}])")

    def get_string(self) -> str:
        """Returns the OpenSCAD build script."""
        return "\n".join(self.data)


def boolean_operation(op: Operation, mesh1: MeshBuilder, mesh2: MeshBuilder) -> str:
    """Returns an `OpenSCAD`_ script to apply the given boolean operation to the
    given meshes.

    The supported operations are:

        - UNION
        - DIFFERENCE
        - INTERSECTION

    """
    assert isinstance(op, Operation), "enum of type Operation expected"
    script = Script()
    script.add(f"{op.name}() {{")
    script.add_polyhedron(mesh1)
    script.add_polyhedron(mesh2)
    script.add("}")
    return script.get_string()
