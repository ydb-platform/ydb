# Purpose: menger sponge addon for ezdxf
# Copyright (c) 2016-2022 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterator, Sequence, Optional
from ezdxf.math import Vec3, UVec, Matrix44, UCS
from ezdxf.render.mesh import MeshVertexMerger, MeshTransformer, MeshBuilder

if TYPE_CHECKING:
    from ezdxf.eztypes import GenericLayoutType

# fmt: off
all_cubes_size_3_template = [
    (0, 0, 0), (1, 0, 0), (2, 0, 0), (0, 1, 0), (1, 1, 0), (2, 1, 0), (0, 2, 0), (1, 2, 0), (2, 2, 0),
    (0, 0, 1), (1, 0, 1), (2, 0, 1), (0, 1, 1), (1, 1, 1), (2, 1, 1), (0, 2, 1), (1, 2, 1), (2, 2, 1),
    (0, 0, 2), (1, 0, 2), (2, 0, 2), (0, 1, 2), (1, 1, 2), (2, 1, 2), (0, 2, 2), (1, 2, 2), (2, 2, 2),
]

original_menger_cubes = [
    (0, 0, 0), (1, 0, 0), (2, 0, 0), (0, 1, 0), (2, 1, 0), (0, 2, 0), (1, 2, 0), (2, 2, 0),
    (0, 0, 1), (2, 0, 1), (0, 2, 1), (2, 2, 1),
    (0, 0, 2), (1, 0, 2), (2, 0, 2), (0, 1, 2), (2, 1, 2), (0, 2, 2), (1, 2, 2), (2, 2, 2),
]

menger_v1 = [
    (0, 0, 0), (2, 0, 0), (1, 1, 0), (0, 2, 0), (2, 2, 0),
    (1, 0, 1), (0, 1, 1), (2, 1, 1), (1, 2, 1),
    (0, 0, 2), (2, 0, 2), (1, 1, 2), (0, 2, 2), (2, 2, 2),
]

menger_v2 = [
    (1, 0, 0), (0, 1, 0), (2, 1, 0), (1, 2, 0),
    (0, 0, 1), (2, 0, 1), (1, 1, 1), (0, 2, 1), (2, 2, 1),
    (1, 0, 2), (0, 1, 2), (2, 1, 2), (1, 2, 2),
]

jerusalem_cube = [
    (0, 0, 0), (1, 0, 0), (2, 0, 0), (3, 0, 0), (4, 0, 0), (0, 1, 0), (1, 1, 0), (3, 1, 0), (4, 1, 0), (0, 2, 0),
    (4, 2, 0), (0, 3, 0), (1, 3, 0), (3, 3, 0), (4, 3, 0), (0, 4, 0), (1, 4, 0), (2, 4, 0), (3, 4, 0), (4, 4, 0),
    (0, 0, 1), (1, 0, 1), (3, 0, 1), (4, 0, 1), (0, 1, 1), (1, 1, 1), (3, 1, 1), (4, 1, 1), (0, 3, 1), (1, 3, 1),
    (3, 3, 1), (4, 3, 1), (0, 4, 1), (1, 4, 1), (3, 4, 1), (4, 4, 1), (0, 0, 2), (4, 0, 2), (0, 4, 2), (4, 4, 2),
    (0, 0, 3), (1, 0, 3), (3, 0, 3), (4, 0, 3), (0, 1, 3), (1, 1, 3), (3, 1, 3), (4, 1, 3), (0, 3, 3), (1, 3, 3),
    (3, 3, 3), (4, 3, 3), (0, 4, 3), (1, 4, 3), (3, 4, 3), (4, 4, 3), (0, 0, 4), (1, 0, 4), (2, 0, 4), (3, 0, 4),
    (4, 0, 4), (0, 1, 4), (1, 1, 4), (3, 1, 4), (4, 1, 4), (0, 2, 4), (4, 2, 4), (0, 3, 4), (1, 3, 4), (3, 3, 4),
    (4, 3, 4), (0, 4, 4), (1, 4, 4), (2, 4, 4), (3, 4, 4), (4, 4, 4),
]

building_schemas = [
    original_menger_cubes,
    menger_v1,
    menger_v2,
    jerusalem_cube,
]

# subdivide level in order of building_schemas
cube_sizes = [3., 3., 3., 5.]

# 8 corner vertices
_cube_vertices = [
    (0, 0, 0),
    (1, 0, 0),
    (1, 1, 0),
    (0, 1, 0),
    (0, 0, 1),
    (1, 0, 1),
    (1, 1, 1),
    (0, 1, 1),
]

# 6 cube faces
cube_faces = [
    [0, 3, 2, 1],
    [4, 5, 6, 7],
    [0, 1, 5, 4],
    [1, 2, 6, 5],
    [3, 7, 6, 2],
    [0, 4, 7, 3],
]

# fmt: on


class MengerSponge:
    """

    Args:
        location: location of lower left corner as (x, y, z) tuple
        length: side length
        level: subdivide level
        kind: type of menger sponge

    === ===========================
    0   Original Menger Sponge
    1   Variant XOX
    2   Variant OXO
    3   Jerusalem Cube
    === ===========================

    """

    def __init__(
        self,
        location: UVec = (0.0, 0.0, 0.0),
        length: float = 1.0,
        level: int = 1,
        kind: int = 0,
    ):
        self.cube_definitions = _menger_sponge(
            location=location, length=length, level=level, kind=kind
        )

    def vertices(self) -> Iterator[list[Vec3]]:
        """Yields the cube vertices as list of (x, y, z) tuples."""
        for location, length in self.cube_definitions:
            x, y, z = location
            yield [
                Vec3(x + xf * length, y + yf * length, z + zf * length)
                for xf, yf, zf in _cube_vertices
            ]

    __iter__ = vertices

    @staticmethod
    def faces() -> list[list[int]]:
        """Returns list of cube faces. All cube vertices have the same order, so
        one faces list fits them all.

        """
        return cube_faces

    def render(
        self,
        layout: GenericLayoutType,
        merge: bool = False,
        dxfattribs=None,
        matrix: Optional[Matrix44] = None,
        ucs: Optional[UCS] = None,
    ) -> None:
        """Renders the menger sponge into layout, set `merge` to ``True`` for
        rendering the whole menger sponge into one MESH entity, set `merge` to
        ``False`` for rendering the individual cubes of the menger sponge as
        MESH entities.

        Args:
            layout: DXF target layout
            merge: ``True`` for one MESH entity, ``False`` for individual MESH
                entities per cube
            dxfattribs: DXF attributes for the MESH entities
            matrix: apply transformation matrix at rendering
            ucs: apply UCS transformation at rendering

        """
        if merge:
            mesh = self.mesh()
            mesh.render_mesh(
                layout, dxfattribs=dxfattribs, matrix=matrix, ucs=ucs
            )
        else:
            for cube in self.cubes():
                cube.render_mesh(layout, dxfattribs, matrix=matrix, ucs=ucs)

    def cubes(self) -> Iterator[MeshTransformer]:
        """Yields all cubes of the menger sponge as individual
        :class:`MeshTransformer` objects.
        """
        faces = self.faces()
        for vertices in self:
            mesh = MeshVertexMerger()
            mesh.add_mesh(vertices=vertices, faces=faces)  # type: ignore
            yield MeshTransformer.from_builder(mesh)

    def mesh(self) -> MeshTransformer:
        """Returns geometry as one :class:`MeshTransformer` object."""
        faces = self.faces()
        mesh = MeshVertexMerger()
        for vertices in self:
            mesh.add_mesh(vertices=vertices, faces=faces)  # type: ignore
        return remove_duplicate_inner_faces(mesh)


def remove_duplicate_inner_faces(mesh: MeshBuilder) -> MeshTransformer:
    new_mesh = MeshTransformer()
    new_mesh.vertices = mesh.vertices
    new_mesh.faces = list(manifold_faces(mesh.faces))
    return new_mesh


def manifold_faces(faces: list[Sequence[int]]) -> Iterator[Sequence[int]]:
    ledger: dict[tuple[int, ...], list[Sequence[int]]] = {}
    for face in faces:
        key = tuple(sorted(face))
        try:
            ledger[key].append(face)
        except KeyError:
            ledger[key] = [face]
    for faces in ledger.values():
        if len(faces) == 1:
            yield faces[0]


def _subdivide(
    location: UVec = (0.0, 0.0, 0.0), length: float = 1.0, kind: int = 0
) -> list[tuple[Vec3, float]]:
    """Divides a cube in sub-cubes and keeps only cubes determined by the
    building schema.

    All sides are parallel to x-, y- and z-axis, location is a (x, y, z) tuple
    and represents the coordinates of the lower left corner (nearest to the axis
    origin) of the cube, length is the side-length of the cube

    Args:
        location: (x, y, z) tuple, coordinates of the lower left corner of the cube
        length: side length of the cube
        kind: int for 0: original menger sponge; 1: Variant XOX; 2: Variant OXO;
            3: Jerusalem Cube;

    Returns: list of sub-cubes (location, length)

    """

    init_x, init_y, init_z = location
    step_size = float(length) / cube_sizes[kind]
    remaining_cubes = building_schemas[kind]

    def sub_location(indices) -> Vec3:
        x, y, z = indices
        return Vec3(
            init_x + x * step_size,
            init_y + y * step_size,
            init_z + z * step_size,
        )

    return [(sub_location(indices), step_size) for indices in remaining_cubes]


def _menger_sponge(
    location: UVec = (0.0, 0.0, 0.0),
    length: float = 1.0,
    level: int = 1,
    kind: int = 0,
) -> list[tuple[Vec3, float]]:
    """Builds a menger sponge for given level.

    Args:
        location: (x, y, z) tuple, coordinates of the lower left corner of the cube
        length: side length of the cube
        level: level of menger sponge, has to be 1 or bigger
        kind: int for 0: original menger sponge; 1: Variant XOX; 2: Variant OXO;
            3: Jerusalem Cube;

    Returns: list of sub-cubes (location, length)

    """
    kind = int(kind)
    if kind not in (0, 1, 2, 3):
        raise ValueError("kind has to be 0, 1, 2 or 3.")
    level = int(level)
    if level < 1:
        raise ValueError("level has to be 1 or bigger.")
    cubes = _subdivide(location, length, kind=kind)
    for _ in range(level - 1):
        next_level_cubes = []
        for location, length in cubes:
            next_level_cubes.extend(_subdivide(location, length, kind=kind))
        cubes = next_level_cubes
    return cubes
