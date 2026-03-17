# Copyright (c) 2018-2022 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, Sequence, Iterator, Callable, Optional
import math
from enum import IntEnum
from ezdxf.math import (
    Vec2,
    Vec3,
    UVec,
    Matrix44,
    global_bspline_interpolation,
    EulerSpiral,
    arc_angle_span_rad,
    NULLVEC,
    Z_AXIS,
    X_AXIS,
    UCS,
    intersection_ray_ray_3d,
)
from ezdxf.render.mesh import MeshVertexMerger, MeshTransformer


__all__ = [
    "circle",
    "ellipse",
    "euler_spiral",
    "square",
    "box",
    "open_arrow",
    "arrow2",
    "ngon",
    "star",
    "gear",
    "turtle",
    "translate",
    "rotate",
    "scale",
    "close_polygon",
    "helix",
    "cube",
    "extrude",
    "extrude_twist_scale",
    "sweep",
    "cylinder",
    "cylinder_2p",
    "from_profiles_linear",
    "from_profiles_spline",
    "spline_interpolation",
    "spline_interpolated_profiles",
    "cone",
    "cone_2p",
    "rotation_form",
    "sphere",
    "torus",
    "reference_frame_z",
    "reference_frame_ext",
    "make_next_reference_frame",
]


def circle(
    count: int, radius: float = 1, elevation: float = 0, close: bool = False
) -> Iterable[Vec3]:
    """Create polygon vertices for a `circle <https://en.wikipedia.org/wiki/Circle>`_
    with the given `radius` and approximated by `count` vertices, `elevation`
    is the z-axis for all vertices.

    Args:
        count: count of polygon vertices
        radius: circle radius
        elevation: z-axis for all vertices
        close: yields first vertex also as last vertex if ``True``.

    Returns:
        vertices in counter-clockwise orientation as :class:`~ezdxf.math.Vec3`
        objects

    """
    radius = float(radius)
    delta = math.tau / count
    alpha = 0.0
    for index in range(count):
        x = math.cos(alpha) * radius
        y = math.sin(alpha) * radius
        yield Vec3(x, y, elevation)
        alpha += delta

    if close:
        yield Vec3(radius, 0, elevation)


def ellipse(
    count: int,
    rx: float = 1,
    ry: float = 1,
    start_param: float = 0,
    end_param: float = math.tau,
    elevation: float = 0,
) -> Iterable[Vec3]:
    """Create polygon vertices for an `ellipse <https://en.wikipedia.org/wiki/Ellipse>`_
    with given `rx` as x-axis radius and `ry` as y-axis radius approximated by
    `count` vertices, `elevation` is the z-axis for all vertices.
    The ellipse goes from `start_param` to `end_param` in counter clockwise
    orientation.

    Args:
        count: count of polygon vertices
        rx: ellipse x-axis radius
        ry: ellipse y-axis radius
        start_param: start of ellipse in range [0, 2π]
        end_param: end of ellipse in range [0, 2π]
        elevation: z-axis for all vertices

    Returns:
        vertices in counter clockwise orientation as :class:`~ezdxf.math.Vec3`
        objects

    """
    rx = float(rx)
    ry = float(ry)
    start_param = float(start_param)
    end_param = float(end_param)
    count = int(count)
    delta = (end_param - start_param) / (count - 1)
    cos = math.cos
    sin = math.sin
    for param in range(count):
        alpha = start_param + param * delta
        yield Vec3(cos(alpha) * rx, sin(alpha) * ry, elevation)


def euler_spiral(
    count: int, length: float = 1, curvature: float = 1, elevation: float = 0
) -> Iterable[Vec3]:
    """Create polygon vertices for an `euler spiral <https://en.wikipedia.org/wiki/Euler_spiral>`_
    of a given `length` and radius of curvature. This is a parametric curve,
    which always starts at the origin (0, 0).

    Args:
        count: count of polygon vertices
        length: length of curve in drawing units
        curvature: radius of curvature
        elevation: z-axis for all vertices

    Returns:
        vertices as :class:`~ezdxf.math.Vec3` objects

    """
    spiral = EulerSpiral(curvature=curvature)
    for vertex in spiral.approximate(length, count - 1):
        yield vertex.replace(z=elevation)


def square(size: float = 1.0, center=False) -> tuple[Vec3, Vec3, Vec3, Vec3]:
    """Returns 4 vertices for a square with a side length of the given `size`.
    The center of the square in (0, 0) if `center` is ``True`` otherwise
    the lower left corner is (0, 0), upper right corner is (`size`, `size`).
    """
    if center:
        a = size / 2.0
        return Vec3(-a, -a), Vec3(a, -a), Vec3(a, a), Vec3(-a, a)
    else:
        return Vec3(0, 0), Vec3(size, 0), Vec3(size, size), Vec3(0, size)


def box(
    sx: float = 1.0, sy: float = 1.0, center=False
) -> tuple[Vec3, Vec3, Vec3, Vec3]:
    """Returns 4 vertices for a box with a width of `sx` by and a height of
    `sy`. The center of the box in (0, 0) if `center` is ``True`` otherwise
    the lower left corner is (0, 0), upper right corner is (`sx`, `sy`).
    """
    if center:
        a = sx / 2.0
        b = sy / 2.0
        return Vec3(-a, -b), Vec3(a, -b), Vec3(a, b), Vec3(-a, b)
    else:
        return Vec3(0, 0), Vec3(sx, 0), Vec3(sx, sy), Vec3(0, sy)


def open_arrow(size: float = 1.0, angle: float = 30.0) -> tuple[Vec3, Vec3, Vec3]:
    """Returns 3 vertices for an open arrow `<` with a length of the given
    `size`, argument `angle` defines the enclosing angle in degrees.
    Vertex order: upward end vertex, tip (0, 0) , downward end vertex (counter-
    clockwise order)

    Args:
        size: length of arrow
        angle: enclosing angle in degrees

    """
    h = math.sin(math.radians(angle / 2.0)) * size
    return Vec3(-size, h), Vec3(0, 0), Vec3(-size, -h)


def arrow2(
    size: float = 1.0, angle: float = 30.0, beta: float = 45.0
) -> tuple[Vec3, Vec3, Vec3, Vec3]:
    """Returns 4 vertices for an arrow with a length of the given `size`, and
    an enclosing `angle` in degrees and a slanted back side defined by angle
    `beta`::

                    ****
                ****  *
            ****     *
        **** angle   X********************
            ****     * +beta
                ****  *
                    ****

                    ****
                ****    *
            ****         *
        **** angle        X***************
            ****         * -beta
                ****    *
                    ****

    Vertex order: upward end vertex, tip (0, 0), downward end vertex, bottom
    vertex `X` (anti clockwise order).

    Bottom vertex `X` is also the connection point to a continuation line.

    Args:
        size: length of arrow
        angle: enclosing angle in degrees
        beta: angle if back side in degrees

    """
    h = math.sin(math.radians(angle / 2.0)) * size
    back_step = math.tan(math.radians(beta)) * h
    return (
        Vec3(-size, h),
        Vec3(0, 0),
        Vec3(-size, -h),
        Vec3(-size + back_step, 0),
    )


def ngon(
    count: int,
    length: Optional[float] = None,
    radius: Optional[float] = None,
    rotation: float = 0.0,
    elevation: float = 0.0,
    close: bool = False,
) -> Iterable[Vec3]:
    """Returns the corner vertices of a `regular polygon <https://en.wikipedia.org/wiki/Regular_polygon>`_.
    The polygon size is determined by the edge `length` or the circum `radius`
    argument. If both are given `length` has the higher priority.

    Args:
        count: count of polygon corners >= 3
        length: length of polygon side
        radius: circum radius
        rotation: rotation angle in radians
        elevation: z-axis for all vertices
        close: yields first vertex also as last vertex if ``True``.

    Returns:
        vertices as :class:`~ezdxf.math.Vec3` objects

    """
    if count < 3:
        raise ValueError("Argument `count` has to be greater than 2.")
    if length is not None:
        if length <= 0.0:
            raise ValueError("Argument `length` has to be greater than 0.")
        radius = length / 2.0 / math.sin(math.pi / count)
    elif radius is not None:
        if radius <= 0.0:
            raise ValueError("Argument `radius` has to be greater than 0.")
    else:
        raise ValueError("Argument `length` or `radius` required.")

    delta = math.tau / count
    angle = rotation
    first = None
    cos = math.cos
    sin = math.sin
    for _ in range(count):
        v = Vec3(radius * cos(angle), radius * sin(angle), elevation)
        if first is None:
            first = v
        yield v
        angle += delta

    if close:
        yield first


def star(
    count: int,
    r1: float,
    r2: float,
    rotation: float = 0.0,
    elevation: float = 0.0,
    close: bool = False,
) -> Iterable[Vec3]:
    """Returns the corner vertices for a `star shape <https://en.wikipedia.org/wiki/Star_polygon>`_.

    The shape has `count` spikes, `r1` defines the radius of the "outer"
    vertices and `r2` defines the radius of the "inner" vertices,
    but this does not mean that `r1` has to be greater than `r2`.

    Args:
        count: spike count >= 3
        r1: radius 1
        r2: radius 2
        rotation: rotation angle in radians
        elevation: z-axis for all vertices
        close: yields first vertex also as last vertex if ``True``.

    Returns:
        vertices as :class:`~ezdxf.math.Vec3` objects

    """
    if count < 3:
        raise ValueError("Argument `count` has to be greater than 2.")
    if r1 <= 0.0:
        raise ValueError("Argument `r1` has to be greater than 0.")
    if r2 <= 0.0:
        raise ValueError("Argument `r2` has to be greater than 0.")

    corners1 = ngon(
        count, radius=r1, rotation=rotation, elevation=elevation, close=False
    )
    corners2 = ngon(
        count,
        radius=r2,
        rotation=math.pi / count + rotation,
        elevation=elevation,
        close=False,
    )
    first = None
    for s1, s2 in zip(corners1, corners2):
        if first is None:
            first = s1
        yield s1
        yield s2

    if close:
        yield first


class _Gear(IntEnum):
    TOP_START = 0
    TOP_END = 1
    BOTTOM_START = 2
    BOTTOM_END = 3


def gear(
    count: int,
    top_width: float,
    bottom_width: float,
    height: float,
    outside_radius: float,
    elevation: float = 0,
    close: bool = False,
) -> Iterable[Vec3]:
    """Returns the corner vertices of a `gear shape <https://en.wikipedia.org/wiki/Gear>`_
    (cogwheel).

    .. warning::

        This function does not create correct gears for mechanical engineering!

    Args:
        count: teeth count >= 3
        top_width: teeth width at outside radius
        bottom_width: teeth width at base radius
        height: teeth height; base radius = outside radius - height
        outside_radius: outside radius
        elevation: z-axis for all vertices
        close: yields first vertex also as last vertex if True.

    Returns:
        vertices in counter clockwise orientation as :class:`~ezdxf.math.Vec3`
        objects

    """
    if count < 3:
        raise ValueError("Argument `count` has to be greater than 2.")
    if outside_radius <= 0.0:
        raise ValueError("Argument `radius` has to be greater than 0.")
    if top_width <= 0.0:
        raise ValueError("Argument `width` has to be greater than 0.")
    if bottom_width <= 0.0:
        raise ValueError("Argument `width` has to be greater than 0.")
    if height <= 0.0:
        raise ValueError("Argument `height` has to be greater than 0.")
    if height >= outside_radius:
        raise ValueError("Argument `height` has to be smaller than `radius`")

    base_radius = outside_radius - height
    alpha_top = math.asin(top_width / 2.0 / outside_radius)  # angle at tooth top
    alpha_bottom = math.asin(bottom_width / 2.0 / base_radius)  # angle at tooth bottom
    alpha_difference = (
        alpha_bottom - alpha_top
    ) / 2.0  # alpha difference at start and end of tooth
    beta = (math.tau - count * alpha_bottom) / count
    angle = -alpha_top / 2.0  # center of first tooth is in x-axis direction
    state = _Gear.TOP_START
    first = None
    cos = math.cos
    sin = math.sin
    for _ in range(4 * count):
        if state == _Gear.TOP_START or state == _Gear.TOP_END:
            radius = outside_radius
        else:
            radius = base_radius
        v = Vec3(radius * cos(angle), radius * sin(angle), elevation)

        if state == _Gear.TOP_START:
            angle += alpha_top
        elif state == _Gear.TOP_END:
            angle += alpha_difference
        elif state == _Gear.BOTTOM_START:
            angle += beta
        elif state == _Gear.BOTTOM_END:
            angle += alpha_difference

        if first is None:
            first = v
        yield v

        state += 1  # type: ignore
        if state > _Gear.BOTTOM_END:
            state = _Gear.TOP_START

    if close:
        yield first


def turtle(commands: str, start=Vec2(0, 0), angle: float = 0) -> Iterator[Vec2]:
    """Returns the 2D vertices of a polyline created by turtle-graphic like
    commands:

    - ``<length>`` - go <length> units forward in current direction and yield vertex
    - ``r<angle>`` - turn right <angle> in degrees, a missing angle is 90 deg
    - ``l<angle>`` - turn left <angle> in degrees, a missing angle is 90 deg
    - ``@<x>,<y>`` - go relative <x>,<y> and yield vertex

    The command string ``"10 l 10 l 10"`` returns the 4 corner vertices of a
    square with a side length of 10 drawing units.

    Args:
        commands: command string, commands are separated by spaces
        start: starting point, default is (0, 0)
        angle: starting direction, default is 0 deg

    """

    cursor = start
    yield cursor
    for cmd in commands.split(" "):
        cmd = cmd.strip()
        if cmd[0] == "l":
            if len(cmd) == 1:
                angle += 90
            else:
                angle += float(cmd[1:])
        elif cmd[0] == "r":
            if len(cmd) == 1:
                angle -= 90
            else:
                angle -= float(cmd[1:])
        elif cmd[0] == "@":
            x, y = cmd[1:].split(",")
            cursor += Vec2(float(x), float(y))
            yield cursor
        else:
            cursor += Vec2.from_deg_angle(angle, float(cmd))
            yield cursor


def translate(vertices: Iterable[UVec], vec: UVec = (0, 0, 0)) -> Iterable[Vec3]:
    """Translate `vertices` along `vec`, faster than a Matrix44 transformation.

    Args:
        vertices: iterable of vertices
        vec: translation vector

    Returns: yields transformed vertices

    """
    _vec = Vec3(vec)
    for p in vertices:
        yield _vec + p


def rotate(
    vertices: Iterable[UVec], angle: float = 0.0, deg: bool = True
) -> Iterable[Vec3]:
    """Rotate `vertices` about to z-axis at to origin (0, 0), faster than a
    Matrix44 transformation.

    Args:
        vertices: iterable of vertices
        angle: rotation angle
        deg: True if angle in degrees, False if angle in radians

    Returns: yields transformed vertices

    """
    if deg:
        return (Vec3(v).rotate_deg(angle) for v in vertices)
    else:
        return (Vec3(v).rotate(angle) for v in vertices)


def scale(vertices: Iterable[UVec], scaling=(1.0, 1.0, 1.0)) -> Iterable[Vec3]:
    """Scale `vertices` around the origin (0, 0), faster than a Matrix44
    transformation.

    Args:
        vertices: iterable of vertices
        scaling: scale factors as tuple of floats for x-, y- and z-axis

    Returns: yields scaled vertices

    """
    sx, sy, sz = scaling
    for v in Vec3.generate(vertices):
        yield Vec3(v.x * sx, v.y * sy, v.z * sz)


def close_polygon(
    vertices: Iterable[Vec3], rel_tol: float = 1e-9, abs_tol: float = 1e-12
) -> list[Vec3]:
    """Returns list of :class:`~ezdxf.math.Vec3`, where the first vertex is
    equal to the last vertex.
    """
    polygon: list[Vec3] = list(vertices)
    if not polygon[0].isclose(polygon[-1], rel_tol=rel_tol, abs_tol=abs_tol):
        polygon.append(polygon[0])
    return polygon


def helix(
    radius: float,
    pitch: float,
    turns: float,
    resolution: int = 16,
    ccw=True,
) -> Iterator[Vec3]:
    """Yields the vertices of a `helix <https://en.wikipedia.org/wiki/Helix>`_.
    The center of the helix is always (0, 0), a positive `pitch` value
    creates a helix along the +z-axis, a negative value along the -z-axis.

    Args:
        radius: helix radius
        pitch: the height of one complete helix turn
        turns: count of turns
        resolution: vertices per turn
        ccw: creates a counter-clockwise turning (right-handed) helix if ``True``

    """
    step: float = 1.0 / max(resolution, 1)
    if not ccw:
        step = -step
    total_step_count = int(turns / abs(step))
    for index in range(total_step_count + 1):
        t = step * index
        angle = t * math.tau
        x = math.cos(angle) * radius
        y = math.sin(angle) * radius
        yield Vec3(x, y, abs(t) * pitch)


# 8 corner vertices
_cube_vertices = [
    Vec3(0, 0, 0),
    Vec3(1, 0, 0),
    Vec3(1, 1, 0),
    Vec3(0, 1, 0),
    Vec3(0, 0, 1),
    Vec3(1, 0, 1),
    Vec3(1, 1, 1),
    Vec3(0, 1, 1),
]

# 8 corner vertices, 'mass' center in (0, 0, 0)
_cube0_vertices = [
    Vec3(-0.5, -0.5, -0.5),
    Vec3(+0.5, -0.5, -0.5),
    Vec3(+0.5, +0.5, -0.5),
    Vec3(-0.5, +0.5, -0.5),
    Vec3(-0.5, -0.5, +0.5),
    Vec3(+0.5, -0.5, +0.5),
    Vec3(+0.5, +0.5, +0.5),
    Vec3(-0.5, +0.5, +0.5),
]

# 6 cube faces
cube_faces = [
    [0, 3, 2, 1],  # bottom, normal = -Z
    [1, 2, 6, 5],  # right, normal = +X
    [3, 7, 6, 2],  # back, normal = +Y
    [0, 4, 7, 3],  # left, normal = -X
    [0, 1, 5, 4],  # front, normal = -Y
    [4, 5, 6, 7],  # top, normal = +Z
]


def cube(center: bool = True) -> MeshTransformer:
    """Create a `cube <https://en.wikipedia.org/wiki/Cube>`_ as
    :class:`~ezdxf.render.MeshTransformer` object.

    Args:
        center: 'mass' center of cube, ``(0, 0, 0)`` if ``True``, else first
            corner at ``(0, 0, 0)``

    Returns: :class:`~ezdxf.render.MeshTransformer`

    """
    mesh = MeshTransformer()
    vertices = _cube0_vertices if center else _cube_vertices
    mesh.add_mesh(vertices=vertices, faces=cube_faces)  # type: ignore
    return mesh


def extrude(
    profile: Iterable[UVec], path: Iterable[UVec], close=True, caps=False
) -> MeshTransformer:
    """Extrude a `profile` polygon along a `path` polyline, the vertices of
    `profile` should be in counter-clockwise order.
    The sweeping profile will not be rotated at extrusion!

    Args:
        profile: sweeping profile as list of (x, y, z) tuples in
            counter-clockwise order
        path:  extrusion path as list of (x, y, z) tuples
        close: close profile polygon if ``True``
        caps: close hull with top- and bottom faces (ngons)

    Returns: :class:`~ezdxf.render.MeshTransformer`

    """
    mesh = MeshVertexMerger()
    sweeping_profile = Vec3.list(profile)
    if close:
        sweeping_profile = close_polygon(sweeping_profile)

    extrusion_path = Vec3.list(path)
    if caps:
        mesh.add_face(reversed(sweeping_profile[:-1]))
    start_point = extrusion_path[0]
    for target_point in extrusion_path[1:]:
        translation_vector = target_point - start_point
        target_profile = [vec + translation_vector for vec in sweeping_profile]
        for face in _quad_connection_faces(sweeping_profile, target_profile):
            mesh.add_face(face)
        sweeping_profile = target_profile
        start_point = target_point
    if caps:
        mesh.add_face(sweeping_profile[:-1])
    return MeshTransformer.from_builder(mesh)


def _partial_path_factors(path: list[Vec3]) -> list[float]:
    partial_lengths = [v1.distance(v2) for v1, v2 in zip(path, path[1:])]
    total_length = sum(partial_lengths)
    factors = [0.0]
    partial_sum = 0.0
    for pl in partial_lengths:
        partial_sum += pl
        factors.append(partial_sum / total_length)
    return factors


def _divide_path_into_steps(path: Sequence[Vec3], max_step_size: float) -> list[Vec3]:
    new_path: list[Vec3] = [path[0]]
    for v0, v1 in zip(path, path[1:]):
        segment_vec = v1 - v0
        length = segment_vec.magnitude
        if length > max_step_size:
            parts = int(math.ceil(length / max_step_size))
            step = segment_vec * (1.0 / parts)
            for _ in range(parts - 1):
                v0 += step
                new_path.append(v0)
        new_path.append(v1)
    return new_path


def extrude_twist_scale(
    profile: Iterable[UVec],
    path: Iterable[UVec],
    *,
    twist: float = 0.0,
    scale: float = 1.0,
    step_size: float = 1.0,
    close=True,
    caps=False,
    quads=True,
) -> MeshTransformer:
    """Extrude a `profile` polygon along a `path` polyline, the vertices of
    `profile` should be in counter-clockwise order.
    This implementation can scale and twist the sweeping profile along the
    extrusion path. The `path` segment points are fix points, the
    `max_step_size` is used to create intermediate profiles between this
    fix points. The `max_step_size` is adapted for each
    segment to create equally spaced distances.
    The twist angle is the rotation angle in radians and the scale `argument`
    defines the scale factor of the final profile.
    The twist angle and scaling factor of the intermediate profiles will be
    linear interpolated between the start and end values.

    Args:
        profile: sweeping profile as list of (x, y, z) tuples in
            counter-clockwise order
        path:  extrusion path as list of (x, y, z) tuples
        twist: rotate sweeping profile up to the given end rotation angle in
            radians
        scale: scale sweeping profile gradually from 1.0 to given value
        step_size: rough distance between automatically created intermediate
            profiles, the step size is adapted to the distances between the
            path segment points, a value od 0.0 disables creating intermediate
            profiles
        close: close profile polygon if ``True``
        caps: close hull with top- and  bottom faces (ngons)
        quads: use quads for "sweeping" faces if ``True`` else triangles,
            the top and bottom faces are always ngons

    Returns: :class:`~ezdxf.render.MeshTransformer`

    """

    def matrix(fac: float) -> Matrix44:
        current_scale = 1.0 + (scale - 1.0) * fac
        current_rotation = twist * fac
        translation = target_point - start_point
        scale_cos_a = current_scale * math.cos(current_rotation)
        scale_sin_a = current_scale * math.sin(current_rotation)
        # fmt: off
        return Matrix44([
            scale_cos_a, scale_sin_a, 0.0, 0.0,
            -scale_sin_a, scale_cos_a, 0.0, 0.0,
            0.0, 0.0, current_scale, 0.0,
            translation.x, translation.y, translation.z, 1.0
        ])
        # fmt: on

    mesh = MeshVertexMerger()
    sweeping_profile = Vec3.list(profile)
    if close:
        sweeping_profile = close_polygon(sweeping_profile)
    if caps:
        mesh.add_face(reversed(sweeping_profile[:-1]))
    # create extrusion path with intermediate points
    extrusion_path = Vec3.list(path)
    if step_size != 0.0:
        extrusion_path = _divide_path_into_steps(extrusion_path, step_size)
    # create progress factors for each step along the extrusion path
    factors = _partial_path_factors(extrusion_path)
    start_point = extrusion_path[0]
    prev_profile = sweeping_profile
    face_generator = _quad_connection_faces if quads else _tri_connection_faces
    for target_point, factor in zip(extrusion_path[1:], factors[1:]):
        target_profile = list(matrix(factor).transform_vertices(sweeping_profile))
        for face in face_generator(prev_profile, target_profile):
            mesh.add_face(face)
        prev_profile = target_profile
    if caps:
        mesh.add_face(prev_profile[:-1])
    return MeshTransformer.from_builder(mesh)


def cylinder(
    count: int = 16,
    radius: float = 1.0,
    top_radius: Optional[float] = None,
    top_center: UVec = (0, 0, 1),
    *,
    caps=True,
) -> MeshTransformer:
    """Create a `cylinder <https://en.wikipedia.org/wiki/Cylinder>`_ as
    :class:`~ezdxf.render.MeshTransformer` object, the base center is fixed in
    the origin (0, 0, 0).

    Args:
        count: profiles edge count
        radius: radius for bottom profile
        top_radius: radius for top profile, if ``None`` top_radius == radius
        top_center: location vector for the center of the top profile
        caps: close hull with top- and  bottom faces (ngons)

    """
    if top_radius is None:
        top_radius = radius

    if math.isclose(top_radius, 0.0):  # pyramid/cone
        return cone(count=count, radius=radius, apex=top_center)

    base_profile = list(circle(count, radius, close=True))
    top_profile = list(translate(circle(count, top_radius, close=True), top_center))
    return from_profiles_linear(
        [base_profile, top_profile],
        close=False,
        quads=True,
        caps=caps,
    )


def cylinder_2p(
    count: int = 16,
    radius: float = 1,
    base_center: UVec = (0, 0, 0),
    top_center: UVec = (0, 0, 1),
    *,
    caps=True,
) -> MeshTransformer:
    """Creates a `cylinder <https://en.wikipedia.org/wiki/Cylinder>`_ as
    :class:`~ezdxf.render.MeshTransformer` object from two points,
    `base_center` is the center of the base circle and, `top_center` the center
    of the top circle.

    Args:
        count: cylinder profile edge count
        radius: radius for bottom profile
        base_center: center of base circle
        top_center: center of top circle
        caps: close hull with top- and  bottom faces (ngons)

    Raises:
        ValueError: the cylinder orientation cannot be detected (base center == top center)

    """
    origin = Vec3(base_center)
    heading = Vec3(top_center) - origin
    if heading.is_null:
        raise ValueError(
            "cylinder orientation cannot be detected (base center == top center)"
        )
    mesh = cylinder(count, radius, top_center=(0, 0, heading.magnitude), caps=caps)
    try:
        ucs = UCS(origin=origin, uy=Z_AXIS.cross(heading), uz=heading)
    except ZeroDivisionError:
        # heading vector is parallel to the z-axis
        ucs = UCS(origin=origin, ux=X_AXIS, uz=heading)
    mesh.transform(ucs.matrix)
    return mesh


def from_profiles_linear(
    profiles: Sequence[Sequence[Vec3]],
    *,
    close=True,
    quads=True,
    caps=False,
) -> MeshTransformer:
    """Returns a :class:`~ezdxf.render.MeshTransformer` instance from linear
    connected `profiles`.

    Args:
        profiles: list of profiles
        close: close profile polygon if ``True``
        quads: use quadrilaterals as connection faces if ``True`` else triangles
        caps: close hull with top- and bottom faces (ngons)

    """
    mesh = MeshVertexMerger()
    if close:
        profiles = [close_polygon(p) for p in profiles]
    if caps:
        mesh.add_face(reversed(profiles[0]))  # for correct outside pointing normals
    face_generator = _quad_connection_faces if quads else _tri_connection_faces
    for p0, p1 in zip(profiles, profiles[1:]):
        for face in face_generator(p0, p1):
            mesh.add_face(face)
    if caps:
        mesh.add_face(profiles[-1])
    return MeshTransformer.from_builder(mesh)


def spline_interpolation(
    vertices: Iterable[UVec],
    degree: int = 3,
    method: str = "chord",
    subdivide: int = 4,
) -> list[Vec3]:
    """B-spline interpolation, vertices are fit points for the spline
    definition.

    Only method 'uniform', yields vertices at fit points.

    Args:
        vertices: fit points
        degree: degree of B-spline
        method: "uniform", "chord"/"distance", "centripetal"/"sqrt_chord" or
            "arc" calculation method for parameter t
        subdivide: count of sub vertices + 1, e.g. 4 creates 3 sub-vertices

    Returns: list of vertices

    """
    vertices = list(vertices)
    spline = global_bspline_interpolation(vertices, degree=degree, method=method)
    return list(spline.approximate(segments=(len(vertices) - 1) * subdivide))


def spline_interpolated_profiles(
    profiles: Sequence[Sequence[Vec3]], subdivide: int = 4
) -> Iterable[list[Vec3]]:
    """Profile interpolation by cubic B-spline interpolation.

    Args:
        profiles: list of profiles
        subdivide: count of interpolated profiles + 1, e.g. 4 creates 3
            sub-profiles between two main profiles (4 face loops)

    Returns: yields profiles as list of vertices

    """

    if len(set(len(p) for p in profiles)) != 1:
        raise ValueError("All profiles have to have the same vertex count")

    vertex_count = len(profiles[0])
    edges = []  # interpolated spline vertices, where profile vertices are fit points
    for index in range(vertex_count):
        edge_vertices = [p[index] for p in profiles]
        edges.append(spline_interpolation(edge_vertices, subdivide=subdivide))

    profile_count = len(edges[0])
    for profile_index in range(profile_count):
        yield [edge[profile_index] for edge in edges]


def from_profiles_spline(
    profiles: Sequence[Sequence[Vec3]],
    subdivide: int = 4,
    *,
    close=True,
    quads=True,
    caps=False,
) -> MeshTransformer:
    """Returns a :class:`~ezdxf.render.MeshTransformer` instance by spline
    interpolation between given `profiles`.
    Requires at least 4 profiles. A `subdivide` value of 4, means, create 4 face
    loops between two profiles, without interpolation two profiles create one
    face loop.

    Args:
        profiles: list of profiles
        subdivide: count of face loops
        close: close profile polygon if ``True``
        quads: use quadrilaterals as connection faces if ``True`` else triangles
        caps: close hull with top- and bottom faces (ngons)

    """
    if len(profiles) > 3:
        profiles = list(spline_interpolated_profiles(profiles, subdivide))
    else:
        raise ValueError("Spline interpolation requires at least 4 profiles")
    return from_profiles_linear(
        profiles,
        close=close,
        quads=quads,
        caps=caps,
    )


def cone(
    count: int = 16,
    radius: float = 1.0,
    apex: UVec = (0, 0, 1),
    *,
    caps=True,
) -> MeshTransformer:
    """Create a `cone <https://en.wikipedia.org/wiki/Cone>`_ as
    :class:`~ezdxf.render.MeshTransformer` object, the base center is fixed in
    the origin (0, 0, 0).

    Args:
        count: edge count of basis_vector
        radius: radius of basis_vector
        apex: tip of the cone
        caps: add a bottom face as ngon if ``True``

    """
    mesh = MeshVertexMerger()
    base_circle = list(circle(count, radius, close=True))
    for p1, p2 in zip(base_circle, base_circle[1:]):
        mesh.add_face([p1, p2, apex])
    if caps:
        # reversed for correct outside pointing normals
        mesh.add_face(reversed(base_circle))
    return MeshTransformer.from_builder(mesh)


def cone_2p(
    count: int = 16,
    radius: float = 1.0,
    base_center: UVec = (0, 0, 0),
    apex: UVec = (0, 0, 1),
    *,
    caps=True,
) -> MeshTransformer:
    """Create a `cone <https://en.wikipedia.org/wiki/Cone>`_ as
    :class:`~ezdxf.render.MeshTransformer` object from two points, `base_center`
    is the center of the base circle and `apex` as the tip of the cone.

    Args:
        count: edge count of basis_vector
        radius: radius of basis_vector
        base_center: center point of base circle
        apex: tip of the cone
        caps: add a bottom face as ngon if ``True``

    Raises:
        ValueError: the cone orientation cannot be detected (base center == apex)

    """
    origin = Vec3(base_center)
    heading = Vec3(apex) - origin
    if heading.is_null:
        raise ValueError(
            "the cone orientation cannot be detected (base center == apex)"
        )

    mesh = cone(count, radius, apex=(0, 0, heading.magnitude), caps=caps)
    try:
        ucs = UCS(origin=origin, uy=Z_AXIS.cross(heading), uz=heading)
    except ZeroDivisionError:
        # heading vector is parallel to the z-axis
        ucs = UCS(origin=origin, ux=X_AXIS, uz=heading)
    mesh.transform(ucs.matrix)
    return mesh


def rotation_form(
    count: int,
    profile: Iterable[UVec],
    angle: float = math.tau,
    axis: UVec = (1, 0, 0),
    *,
    caps=False,
) -> MeshTransformer:
    """Returns a :class:`~ezdxf.render.MeshTransformer` instance created by
    rotating a `profile` around an `axis`.

    Args:
        count: count of rotated profiles
        profile: profile to rotate as list of vertices
        angle: rotation angle in radians
        axis: rotation axis
        caps: close hull with start- and end faces (ngons)

    """
    if count < 3:
        raise ValueError("count >= 2")
    delta = float(angle) / count
    m = Matrix44.axis_rotate(Vec3(axis), delta)
    profile = [Vec3(p) for p in profile]
    profiles = [profile]
    for _ in range(int(count)):
        profile = list(m.transform_vertices(profile))
        profiles.append(profile)
    mesh = from_profiles_linear(
        profiles,
        close=False,
        quads=True,
        caps=caps,
    )
    return mesh


def sphere(
    count: int = 16, stacks: int = 8, radius: float = 1, *, quads=True
) -> MeshTransformer:
    """Create a `sphere <https://en.wikipedia.org/wiki/Sphere>`_ as
    :class:`~ezdxf.render.MeshTransformer` object, the center of the sphere is
    always at (0, 0, 0).

    Args:
        count: longitudinal slices
        stacks: latitude slices
        radius: radius of sphere
        quads: use quadrilaterals as faces if ``True`` else triangles

    """
    radius = float(radius)
    slices = int(count)
    stacks_2 = int(stacks) // 2  # stacks from -stack/2 to +stack/2
    delta_theta = math.tau / float(slices)
    delta_phi = math.pi / float(stacks)
    mesh = MeshVertexMerger()

    def radius_of_stack(stack: float) -> float:
        return radius * math.cos(delta_phi * stack)

    def vertex(slice_: float, r: float, z: float) -> Vec3:
        actual_theta = delta_theta * slice_
        return Vec3(math.cos(actual_theta) * r, math.sin(actual_theta) * r, z)

    def cap_triangles(stack, top=False):
        z = math.sin(stack * delta_phi) * radius
        cap_vertex = Vec3(0, 0, radius) if top else Vec3(0, 0, -radius)
        r1 = radius_of_stack(stack)
        for slice_ in range(slices):
            v1 = vertex(slice_, r1, z)
            v2 = vertex(slice_ + 1, r1, z)
            if top:
                mesh.add_face((v1, v2, cap_vertex))
            else:
                mesh.add_face((cap_vertex, v2, v1))

    # bottom triangle faces
    cap_triangles(-stacks_2 + 1, top=False)

    # add body faces
    for actual_stack in range(-stacks_2 + 1, stacks_2 - 1):
        next_stack = actual_stack + 1
        r1 = radius_of_stack(actual_stack)
        r2 = radius_of_stack(next_stack)
        z1 = math.sin(delta_phi * actual_stack) * radius
        z2 = math.sin(delta_phi * next_stack) * radius
        for i in range(slices):
            v1 = vertex(i, r1, z1)
            v2 = vertex(i + 1, r1, z1)
            v3 = vertex(i + 1, r2, z2)
            v4 = vertex(i, r2, z2)
            if quads:
                mesh.add_face([v1, v2, v3, v4])
            else:
                center = vertex(
                    i + 0.5,
                    radius_of_stack(actual_stack + 0.5),
                    math.sin(delta_phi * (actual_stack + 0.5)) * radius,
                )
                mesh.add_face([v1, v2, center])
                mesh.add_face([v2, v3, center])
                mesh.add_face([v3, v4, center])
                mesh.add_face([v4, v1, center])

    # top triangle faces
    cap_triangles(stacks_2 - 1, top=True)

    return MeshTransformer.from_builder(mesh)


def torus(
    major_count: int = 16,
    minor_count: int = 8,
    major_radius=1.0,
    minor_radius=0.1,
    start_angle: float = 0.0,
    end_angle: float = math.tau,
    *,
    caps=True,
) -> MeshTransformer:
    """Create a `torus <https://en.wikipedia.org/wiki/Torus>`_ as
    :class:`~ezdxf.render.MeshTransformer` object, the center of the torus is
    always at (0, 0, 0). The `major_radius` has to be bigger than the
    `minor_radius`.

    Args:
        major_count: count of circles
        minor_count: count of circle vertices
        major_radius: radius of the circle center
        minor_radius: radius of circle
        start_angle: start angle of torus in radians
        end_angle: end angle of torus in radians
        caps: close hull with start- and end faces (ngons) if the torus is open

    """
    if major_count < 1:
        raise ValueError(f"major_count < 1")
    if minor_count < 3:
        raise ValueError(f"minor_count < 3")
    major_radius = math.fabs(float(major_radius))
    minor_radius = math.fabs(float(minor_radius))
    if major_radius < 1e-9:
        raise ValueError("major_radius is 0")
    if minor_radius < 1e-9:
        raise ValueError("minor_radius is 0")
    if minor_radius >= major_radius:
        raise ValueError("minor_radius >= major_radius")
    start_angle = float(start_angle) % math.tau
    if end_angle != math.tau:
        end_angle = float(end_angle) % math.tau
    angle_span = arc_angle_span_rad(start_angle, end_angle)
    closed_torus = math.isclose(angle_span, math.tau)
    step_angle = angle_span / major_count
    circle_profile = [
        Vec3(v.x, 0, v.y)
        for v in translate(
            circle(minor_count, minor_radius, close=True),
            Vec3(major_radius, 0, 0),
        )
    ]
    # required for outwards pointing normals:
    circle_profile.reverse()
    if start_angle > 1e-9:
        circle_profile = [v.rotate(start_angle) for v in circle_profile]
    mesh = MeshVertexMerger()
    start_profile = circle_profile
    end_profile = [v.rotate(step_angle) for v in circle_profile]

    if not closed_torus and caps:  # add start cap
        mesh.add_face(reversed(start_profile))

    for _ in range(major_count):
        for face in _quad_connection_faces(start_profile, end_profile):
            mesh.add_face(face)
        start_profile = end_profile
        end_profile = [v.rotate(step_angle) for v in end_profile]

    if not closed_torus and caps:  # add end cap
        # end_profile is rotated to the next profile!
        mesh.add_face(start_profile)

    return MeshTransformer.from_builder(mesh)


def connection_faces(
    start_profile: list[Vec3], end_profile: list[Vec3], quad: bool
) -> Iterator[Sequence[Vec3]]:
    assert len(start_profile) == len(end_profile), "profiles differ in vertex count"
    if quad:
        yield from _quad_connection_faces(start_profile, end_profile)
    else:
        yield from _tri_connection_faces(start_profile, end_profile)


def _quad_connection_faces(
    start_profile: Sequence[Vec3], end_profile: Sequence[Vec3]
) -> Iterator[Sequence[Vec3]]:
    v0_prev = start_profile[0]
    v1_prev = end_profile[0]
    for v0, v1 in zip(start_profile[1:], end_profile[1:]):
        yield v0_prev, v0, v1, v1_prev
        v0_prev = v0
        v1_prev = v1


def _tri_connection_faces(
    start_profile: Sequence[Vec3], end_profile: Sequence[Vec3]
) -> Iterator[Sequence[Vec3]]:
    v0_prev = start_profile[0]
    v1_prev = end_profile[0]
    for v0, v1 in zip(start_profile[1:], end_profile[1:]):
        yield v1, v1_prev, v0_prev,
        yield v0_prev, v0, v1
        v0_prev = v0
        v1_prev = v1


def reference_frame_z(heading: Vec3, origin: Vec3 = NULLVEC) -> UCS:
    """Returns a reference frame as UCS from the given heading and the
    WCS z-axis as reference "up" direction.
    The z-axis of the reference frame is pointing in heading direction, the
    x-axis is pointing right and the y-axis is pointing upwards.

    The reference frame is used to project vertices in xy-plane
    (construction plane) onto the normal plane of the given heading.

    Use the :func:`reference_frame_ext` if heading is parallel to the WCS
    Z_AXIS.

    Args:
        heading: WCS direction of the reference frame z-axis
        origin: new UCS origin

    Raises:
        ZeroDivisionError: heading is parallel to WCS Z_AXIS

    """
    return UCS(uy=Z_AXIS.cross(heading), uz=heading, origin=origin)


def reference_frame_ext(frame: UCS, origin: Vec3 = NULLVEC) -> UCS:
    """Reference frame calculation if heading vector is parallel to the WCS
    Z_AXIS.

    Args:
        frame: previous reference frame
        origin: new UCS origin

    """
    try:  # preserve x-axis
        return UCS(uy=Z_AXIS.cross(frame.ux), uz=Z_AXIS, origin=origin)
    except ZeroDivisionError:  # preserve y-axis
        return UCS(ux=frame.uy.cross(Z_AXIS), uz=Z_AXIS, origin=origin)


def _intersect_rays(
    prev_rays: Sequence[Sequence[Vec3]], next_rays: Sequence[Sequence[Vec3]]
) -> Iterator[Vec3]:
    for ray1, ray2 in zip(prev_rays, next_rays):
        ip = intersection_ray_ray_3d(ray1, ray2)
        count = len(ip)
        if count == 1:  # exact intersection
            yield ip[0]
        elif count == 2:  # imprecise intersection
            yield ip[0].lerp(ip[1])
        else:  # parallel rays
            yield ray1[-1].lerp(ray2[0])


def _intersection_profiles(
    start_profiles: Sequence[Sequence[Vec3]],
    end_profiles: Sequence[Sequence[Vec3]],
) -> list[Sequence[Vec3]]:
    profiles: list[Sequence[Vec3]] = [start_profiles[0]]
    rays = [
        [(v0, v1) for v0, v1 in zip(p0, p1)]
        for p0, p1 in zip(start_profiles, end_profiles)
    ]
    for prev_rays, next_rays in zip(rays, rays[1:]):
        profiles.append(list(_intersect_rays(prev_rays, next_rays)))
    profiles.append(end_profiles[-1])
    return profiles


def make_next_reference_frame(frame: UCS, heading: Vec3, origin: Vec3) -> UCS:
    """Returns the following reference frame next to the current reference
    `frame`.

    Args:
        frame: the current reference frame
        heading: z-axis direction of the following frame in WCS
        origin: origin of the following reference frame

    """
    try:
        next_frame = reference_frame_z(heading, origin)
        # reverse y-axis if the next frame y-axis has opposite orientation to
        # the previous frame y-axis:
        if next_frame.uy.dot(frame.uy) < 0:
            next_frame = UCS(origin=origin, uz=next_frame.uz, uy=-next_frame.uy)
        return next_frame
    except ZeroDivisionError:
        # heading vector is parallel to the Z_AXIS
        return reference_frame_ext(frame, origin)


def _make_sweep_start_and_end_profiles(
    profile: Iterable[UVec],
    sweeping_path: Iterable[UVec],
    next_ref_frame: Callable[[UCS, Vec3, Vec3], UCS],
) -> tuple[list[list[Vec3]], list[list[Vec3]]]:
    spath = Vec3.list(sweeping_path)
    reference_profile = Vec3.list(profile)
    start_profiles = []
    end_profiles = []
    ref_frame = UCS()
    for origin, target in zip(spath, spath[1:]):
        heading = target - origin
        ref_frame = next_ref_frame(ref_frame, heading, origin)
        start_profile = list(ref_frame.points_to_wcs(reference_profile))
        start_profiles.append(start_profile)
        end_profiles.append([v + heading for v in start_profile])
    return start_profiles, end_profiles


def sweep_profile(
    profile: Iterable[UVec],
    sweeping_path: Iterable[UVec],
) -> list[Sequence[Vec3]]:
    """Returns the intermediate profiles of sweeping a profile along a 3D path
    where the sweeping path defines the final location in the `WCS`.

    The profile is defined in a reference system. The origin of this reference
    system will be moved along the sweeping path where the z-axis of the
    reference system is pointing into the moving direction.

    Returns the start-, end- and all intermediate profiles along the sweeping
    path.

    """
    return _intersection_profiles(
        *_make_sweep_start_and_end_profiles(
            profile, sweeping_path, make_next_reference_frame
        )
    )


def debug_sweep_profiles(
    profile: Iterable[UVec],
    sweeping_path: Iterable[UVec],
    close=True,
) -> list[Sequence[Vec3]]:
    if close:
        profile = close_polygon(profile)
    profiles: list[Sequence[Vec3]] = []
    for sp, ep in zip(
        *_make_sweep_start_and_end_profiles(
            profile, sweeping_path, make_next_reference_frame
        )
    ):
        profiles.append(sp)
        profiles.append(ep)
    return profiles


def sweep(
    profile: Iterable[UVec],
    sweeping_path: Iterable[UVec],
    *,
    close=True,
    quads=True,
    caps=True,
) -> MeshTransformer:
    """Returns the mesh from sweeping a profile along a 3D path, where the
    sweeping path defines the final location in the `WCS`.

    The profile is defined in a reference system. The origin of this reference
    system will be moved along the sweeping path where the z-axis of the
    reference system is pointing into the moving direction.

    Returns the mesh as :class:`ezdxf.render.MeshTransformer` object.

    Args:
        profile: sweeping profile defined in the reference system as
            iterable of (x, y, z) coordinates in counter-clockwise order
        sweeping_path: the sweeping path defined in the WCS as iterable of
            (x, y, z) coordinates
        close: close sweeping profile if ``True``
        quads: use quadrilaterals as connection faces if ``True`` else triangles
        caps: close hull with top- and bottom faces (ngons)

    """
    profiles = sweep_profile(profile, sweeping_path)
    return from_profiles_linear(
        profiles,
        close=close,
        quads=quads,
        caps=caps,
    )
