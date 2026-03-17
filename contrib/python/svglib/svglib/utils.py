"""Utility functions for SVG processing and path manipulation.

This module provides low-level utility functions used throughout the svglib
package for processing SVG content. It includes functions for parsing SVG path
data, converting between different curve representations, and handling
geometric transformations.

The module includes:
- SVG path parsing and normalization
- Bezier curve conversion utilities
- Elliptical arc processing functions
- Vector mathematics helpers
- String parsing utilities for SVG attributes
"""

import re
from math import acos, ceil, copysign, cos, degrees, fabs, hypot, radians, sin, sqrt
from typing import List, Tuple, Union, cast

from reportlab.graphics.shapes import mmult, rotate, transformPoint, translate


def split_floats(op: str, min_num: int, value: str) -> List[Union[str, List[float]]]:
    """Parse SVG coordinate string into alternating operators and coordinate lists.

    Splits a string of numeric values into groups and pairs each group with the
    appropriate SVG path operation. Automatically converts 'M' to 'L' for subsequent
    coordinate pairs to handle move-to followed by line-to sequences.

    Args:
        op: SVG path operation character (e.g., 'M', 'L', 'm', 'l').
        min_num: Minimum number of coordinates expected per operation.
        value: String containing comma/whitespace-separated numeric values.

    Returns:
        List alternating between operation strings and coordinate lists.
        Example: ['M', [10.0, 20.0], 'L', [30.0, 40.0]]

    Examples:
        >>> split_floats('M', 2, '10,20 30,40')
        ['M', [10.0, 20.0], 'L', [30.0, 40.0]]

        >>> split_floats('L', 2, '100 200')
        ['L', [100.0, 200.0]]

    Note:
        Supports scientific notation (e.g., '1.23e-4') and handles various
        whitespace and comma separators automatically.
    """
    floats = [
        float(seq)
        for seq in re.findall(r"(-?\d*\.?\d*(?:[eE][+-]?\d+)?)", value)
        if seq
    ]
    res: List[Union[str, List[float]]] = []
    for i in range(0, len(floats), min_num):
        if i > 0 and op in {"m", "M"}:
            op = "l" if op == "m" else "L"
        res.extend([op, cast(List[float], list(floats[i : i + min_num]))])
    return res


def split_arc_values(op: str, value: str) -> List[Union[str, List[float]]]:
    """Parse SVG elliptical arc parameters into structured format.

    Parses SVG arc command parameters which consist of: rx, ry, x-axis-rotation,
    large-arc-flag, sweep-flag, x, y. Each complete parameter set is paired with
    the operation character.

    Args:
        op: SVG arc operation character ('A' or 'a').
        value: String containing arc parameters in the format:
            "rx,ry x-axis-rotation large-arc-flag,sweep-flag x,y"

    Returns:
        List alternating between operation strings and parameter lists.
        Each parameter list contains [rx, ry, x_axis_rotation, large_arc_flag,
        sweep_flag, x, y] as floats.

    Examples:
        >>> split_arc_values('A', '50,50 0 1,0 100,100')
        ['A', [50.0, 50.0, 0.0, 1.0, 0.0, 100.0, 100.0]]

        >>> split_arc_values('a', '25 25 30 0 1 50 75')
        ['a', [25.0, 25.0, 30.0, 0.0, 1.0, 50.0, 75.0]]

    Note:
        The large-arc-flag and sweep-flag are converted to float but should
        be treated as boolean values (0 or 1).
    """
    float_re = r"(-?\d*\.?\d*(?:[eE][+-]?\d+)?)"
    flag_re = r"([1|0])"
    # 3 numb, 2 flags, 1 coord pair
    a_seq_re = (
        r"[\s,]*".join(
            [float_re, float_re, float_re, flag_re, flag_re, float_re, float_re]
        )
        + r"[\s,]*"
    )
    res: List[Union[str, List[float]]] = []
    for seq in re.finditer(a_seq_re, value.strip()):
        res.extend([op, cast(List[float], list(float(num) for num in seq.groups()))])
    return res


def normalise_svg_path(attr: str) -> List[Union[str, List[float]]]:
    """Normalize SVG path data into structured format.

    Parses raw SVG path string and converts it into a standardized list format
    where each path command is paired with its coordinate parameters. Automatically
    handles command sequences and converts implicit line commands after move commands.

    Args:
        attr: Raw SVG path string (e.g., "M 10 20 L 30 40 Z").

    Returns:
        Normalized list alternating between command strings and coordinate lists.
        Close path commands ('Z', 'z') are paired with empty lists for consistency.

    Examples:
        >>> normalise_svg_path("M 10 20 L 30 40 Z")
        ['M', [10.0, 20.0], 'L', [30.0, 40.0], 'Z', []]

        >>> normalise_svg_path("M 0 0 L 10 0 10 10 Z")
        ['M', [0.0, 0.0], 'L', [10.0, 0.0], 'L', [10.0, 10.0], 'Z', []]

        >>> normalise_svg_path("m 100,200 300,400")
        ['m', [100.0, 200.0], 'l', [300.0, 400.0]]

    Note:
        - Converts sequences of M/m commands to M/m followed by L/l commands
        - Handles all SVG path commands: M, L, H, V, C, c, S, s, Q, q, T, t, A, a, Z, z
        - Supports various whitespace and comma separators
        - All coordinates are converted to float values
    """

    # operator codes mapped to the minimum number of expected arguments
    ops = {
        "A": 7,
        "a": 7,
        "Q": 4,
        "q": 4,
        "T": 2,
        "t": 2,
        "S": 4,
        "s": 4,
        "M": 2,
        "L": 2,
        "m": 2,
        "l": 2,
        "H": 1,
        "V": 1,
        "h": 1,
        "v": 1,
        "C": 6,
        "c": 6,
        "Z": 0,
        "z": 0,
    }
    op_keys = ops.keys()

    # do some preprocessing
    result: List[Union[str, List[float]]] = []
    groups = re.split("([achlmqstvz])", attr.strip(), flags=re.I)
    op = ""
    for item in groups:
        if item.strip() == "":
            continue
        if item in op_keys:
            # fix sequences of M to one M plus a sequence of L operators,
            # same for m and l.
            if item == "M" and op == "M":
                op = "L"
            elif item == "m" and op == "m":
                op = "l"
            else:
                op = item
            if ops[op] == 0:  # Z, z
                result.extend([op, []])
        else:
            if op.lower() == "a":
                result.extend(split_arc_values(op, item))
            else:
                result.extend(split_floats(op, ops[op], item))
            op = cast(str, result[-2])  # Remember last op

    return result


def convert_quadratic_to_cubic_path(
    q0: Tuple[float, float], q1: Tuple[float, float], q2: Tuple[float, float]
) -> Tuple[
    Tuple[float, float], Tuple[float, float], Tuple[float, float], Tuple[float, float]
]:
    """Convert quadratic Bezier curve to cubic Bezier curve.

    Converts a quadratic Bezier curve defined by control points q0, q1, q2
    into an equivalent cubic Bezier curve. This is useful for SVG processing
    since cubic curves are more commonly supported than quadratic curves.

    Args:
        q0: Starting point as (x, y) tuple.
        q1: Control point as (x, y) tuple.
        q2: End point as (x, y) tuple.

    Returns:
        Tuple of four (x, y) points defining the equivalent cubic Bezier curve:
        (start_point, control_point1, control_point2, end_point).

    Examples:
        >>> convert_quadratic_to_cubic_path((0, 0), (5, 10), (10, 0))
        ((0, 0), (3.3333333333, 6.6666666666), (6.6666666666, 6.6666666666), (10, 0))

        >>> # Simple case: straight line becomes straight line
        >>> convert_quadratic_to_cubic_path((0, 0), (5, 5), (10, 10))
        ((0, 0), (3.3333333333, 3.3333333333), (6.6666666666, 6.6666666666), (10, 10))

    Note:
        The conversion uses the standard formula where the cubic control points
        are calculated as: c1 = q0 + (2/3)*(q1 - q0), c2 = c1 + (1/3)*(q2 - q0).
    """
    c0 = q0
    c1 = (q0[0] + 2 / 3 * (q1[0] - q0[0]), q0[1] + 2 / 3 * (q1[1] - q0[1]))
    c2 = (c1[0] + 1 / 3 * (q2[0] - q0[0]), c1[1] + 1 / 3 * (q2[1] - q0[1]))
    c3 = q2
    return c0, c1, c2, c3


# ***********************************************
# Helper functions for elliptical arc conversion.
# ***********************************************


def vector_angle(u: Tuple[float, float], v: Tuple[float, float]) -> float:
    """Calculate the signed angle between two 2D vectors.

    Computes the angle between vectors u and v using the atan2 method to
    determine the correct quadrant and sign. Returns angle in degrees.

    Args:
        u: First vector as (x, y) tuple.
        v: Second vector as (x, y) tuple.

    Returns:
        Signed angle in degrees between the vectors, ranging from -180 to 180.

    Examples:
        >>> vector_angle((1, 0), (0, 1))  # 90 degrees counterclockwise
        90.0

        >>> vector_angle((1, 0), (0, -1))  # 90 degrees clockwise
        -90.0

        >>> vector_angle((1, 0), (1, 0))  # Same direction
        0.0

        >>> vector_angle((1, 0), (-1, 0))  # Opposite direction
        180.0

    Note:
        - Handles zero-length vectors by returning 0
        - Uses numerical stability checks to avoid domain errors in acos
        - Result is always in the range [-180, 180] degrees
    """
    d = hypot(*u) * hypot(*v)
    if d == 0:
        return 0
    c = (u[0] * v[0] + u[1] * v[1]) / d
    if c < -1:
        c = -1
    elif c > 1:
        c = 1
    s = u[0] * v[1] - u[1] * v[0]
    return degrees(copysign(acos(c), s))


def end_point_to_center_parameters(
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    fA: int,
    fS: int,
    rx: float,
    ry: float,
    phi: float = 0,
) -> Tuple[float, float, float, float, float, float]:
    """Convert SVG arc endpoint parameters to center-based representation.

    Implements the algorithm from W3C SVG specification for converting
    elliptical arc parameters from endpoint format to center format.
    This is needed for proper arc rendering in ReportLab.

    Args:
        x1: X-coordinate of arc start point.
        y1: Y-coordinate of arc start point.
        x2: X-coordinate of arc end point.
        y2: Y-coordinate of arc end point.
        fA: Large arc flag (0 or 1).
        fS: Sweep flag (0 or 1).
        rx: Arc radius in X direction.
        ry: Arc radius in Y direction.
        phi: Rotation angle of the arc in degrees (default 0).

    Returns:
        Tuple of (cx, cy, rx, ry, start_angle, sweep_angle):
        - cx, cy: Center point coordinates
        - rx, ry: Adjusted radii (may be scaled up if too small)
        - start_angle: Starting angle in degrees
        - sweep_angle: Sweep angle in degrees

    Raises:
        This function handles all edge cases internally and doesn't raise exceptions.

    Examples:
        >>> end_point_to_center_parameters(0, 0, 10, 0, 0, 1, 5, 5)
        (5.0, 0.0, 5.0, 5.0, 180.0, 180.0)

        >>> # Degenerate case - identical points
        >>> end_point_to_center_parameters(5, 5, 5, 5, 0, 0, 10, 10)
        (5.0, 5.0, 10.0, 10.0, 0.0, 0.0)

    See Also:
        W3C SVG 1.1 Implementation Notes, Section F.6.5:
        http://www.w3.org/TR/SVG/implnote.html#ArcImplementationNotes

    Note:
        The rotation angle phi is reduced to zero by coordinate transformation
        outside this function for simplicity.
    """
    rx = fabs(rx)
    ry = fabs(ry)

    # step 1
    if phi:
        phi_rad = radians(phi)
        sin_phi = sin(phi_rad)
        cos_phi = cos(phi_rad)
        tx = 0.5 * (x1 - x2)
        ty = 0.5 * (y1 - y2)
        x1d = cos_phi * tx - sin_phi * ty
        y1d = sin_phi * tx + cos_phi * ty
    else:
        x1d = 0.5 * (x1 - x2)
        y1d = 0.5 * (y1 - y2)

    # step 2
    # we need to calculate
    # (rx*rx*ry*ry-rx*rx*y1d*y1d-ry*ry*x1d*x1d)
    # -----------------------------------------
    #     (rx*rx*y1d*y1d+ry*ry*x1d*x1d)
    #
    # that is equivalent to
    #
    #          rx*rx*ry*ry
    # = -----------------------------  -    1
    #   (rx*rx*y1d*y1d+ry*ry*x1d*x1d)
    #
    #              1
    # = -------------------------------- - 1
    #   x1d*x1d/(rx*rx) + y1d*y1d/(ry*ry)
    #
    # = 1/r - 1
    #
    # it turns out r is what they recommend checking
    # for the negative radicand case
    r = x1d * x1d / (rx * rx) + y1d * y1d / (ry * ry)
    if r > 1:
        rr = sqrt(r)
        rx *= rr
        ry *= rr
        r = x1d * x1d / (rx * rx) + y1d * y1d / (ry * ry)
        r = 1 / r - 1
    elif r != 0:
        r = 1 / r - 1
    if -1e-10 < r < 0:
        r = 0
    r = sqrt(r)
    if fA == fS:
        r = -r
    cxd = (r * rx * y1d) / ry
    cyd = -(r * ry * x1d) / rx

    # step 3
    if phi:
        cx = cos_phi * cxd - sin_phi * cyd + 0.5 * (x1 + x2)
        cy = sin_phi * cxd + cos_phi * cyd + 0.5 * (y1 + y2)
    else:
        cx = cxd + 0.5 * (x1 + x2)
        cy = cyd + 0.5 * (y1 + y2)

    # step 4
    theta1 = vector_angle((1, 0), ((x1d - cxd) / rx, (y1d - cyd) / ry))
    dtheta = (
        vector_angle(
            ((x1d - cxd) / rx, (y1d - cyd) / ry), ((-x1d - cxd) / rx, (-y1d - cyd) / ry)
        )
        % 360
    )
    if fS == 0 and dtheta > 0:
        dtheta -= 360
    elif fS == 1 and dtheta < 0:
        dtheta += 360
    return cx, cy, rx, ry, -theta1, -dtheta


def bezier_arc_from_centre(
    cx: float, cy: float, rx: float, ry: float, start_ang: float = 0, extent: float = 90
) -> List[Tuple[float, float, float, float, float, float, float, float]]:
    """Convert elliptical arc to cubic Bezier curve segments.

    Approximates an elliptical arc with cubic Bezier curves using the kappa
    constant method. The arc is divided into segments of at most 90 degrees
    each for accurate approximation.

    Args:
        cx: X-coordinate of ellipse center.
        cy: Y-coordinate of ellipse center.
        rx: Radius in X direction.
        ry: Radius in Y direction.
        start_ang: Starting angle in degrees (default 0).
        extent: Angular extent in degrees (default 90).

    Returns:
        List of Bezier curve segments, each as an 8-tuple:
        (x1, y1, x2, y2, x3, y3, x4, y4) where:
        - (x1, y1): Start point
        - (x2, y2): First control point
        - (x3, y3): Second control point
        - (x4, y4): End point

    Examples:
        >>> # Quarter circle (90 degrees)
        >>> curves = bezier_arc_from_centre(0, 0, 10, 10, 0, 90)
        >>> len(curves)  # One segment for 90 degrees
        1

        >>> # Half circle (180 degrees) - split into two 90-degree segments
        >>> curves = bezier_arc_from_centre(0, 0, 10, 10, 0, 180)
        >>> len(curves)
        2

        >>> # Full circle (360 degrees) - split into four 90-degree segments
        >>> curves = bezier_arc_from_centre(0, 0, 10, 10, 0, 360)
        >>> len(curves)
        4

    Note:
        - Arcs are automatically subdivided into segments ≤ 90° for accuracy
        - Uses the standard kappa = 4/3 * (1 - cos(θ/2)) / sin(θ/2) formula
        - Handles both clockwise and counterclockwise arcs
        - Returns empty list for zero-extent arcs
    """
    if abs(extent) <= 90:
        nfrag = 1
        frag_angle = extent
    else:
        nfrag = ceil(abs(extent) / 90)
        frag_angle = extent / nfrag
    if frag_angle == 0:
        return []

    frag_rad = radians(frag_angle)
    half_rad = frag_rad * 0.5
    kappa = abs(4 / 3 * (1 - cos(half_rad)) / sin(half_rad))

    if frag_angle < 0:
        kappa = -kappa

    point_list = []
    theta1 = radians(start_ang)
    start_rad = theta1 + frag_rad

    c1 = cos(theta1)
    s1 = sin(theta1)
    for i in range(nfrag):
        c0 = c1
        s0 = s1
        theta1 = start_rad + i * frag_rad
        c1 = cos(theta1)
        s1 = sin(theta1)
        point_list.append(
            (
                cx + rx * c0,
                cy - ry * s0,
                cx + rx * (c0 - kappa * s0),
                cy - ry * (s0 + kappa * c0),
                cx + rx * (c1 + kappa * s1),
                cy - ry * (s1 - kappa * c1),
                cx + rx * c1,
                cy - ry * s1,
            )
        )
    return point_list


def bezier_arc_from_end_points(
    x1: float,
    y1: float,
    rx: float,
    ry: float,
    phi: float,
    fA: int,
    fS: int,
    x2: float,
    y2: float,
) -> List[Tuple[float, float, float, float, float, float, float, float]]:
    """Convert SVG elliptical arc to cubic Bezier curve segments.

    High-level function that converts SVG elliptical arc parameters (endpoint format)
    to a series of cubic Bezier curves. Handles rotation, scaling, and all SVG arc
    flags. This is the main entry point for arc-to-Bezier conversion in SVG processing.

    Args:
        x1: X-coordinate of arc start point.
        y1: Y-coordinate of arc start point.
        rx: Arc radius in X direction.
        ry: Arc radius in Y direction.
        phi: Rotation angle of the arc in degrees.
        fA: Large arc flag (0 or 1) - chooses larger or smaller arc.
        fS: Sweep flag (0 or 1) - chooses clockwise or counterclockwise.
        x2: X-coordinate of arc end point.
        y2: Y-coordinate of arc end point.

    Returns:
        List of Bezier curve segments, each as an 8-tuple:
        (x1, y1, x2, y2, x3, y3, x4, y4) representing:
        - (x1, y1): Start point of segment
        - (x2, y2): First control point
        - (x3, y3): Second control point
        - (x4, y4): End point of segment

    Examples:
        >>> # Simple 180-degree arc
        >>> curves = bezier_arc_from_end_points(0, 0, 10, 10, 0, 0, 1, 20, 0)
        >>> len(curves)  # Split into segments
        2

        >>> # Degenerate case - identical points (returns empty list)
        >>> curves = bezier_arc_from_end_points(10, 10, 5, 5, 0, 0, 0, 10, 10)
        >>> len(curves)
        0

        >>> # Rotated ellipse arc
        >>> curves = bezier_arc_from_end_points(0, 0, 10, 5, 45, 1, 0, 7, 7)
        >>> len(curves)  # Will be split into multiple segments
        2

    Note:
        - Returns empty list if start and end points are identical
        - Automatically handles coordinate transformations for rotation
        - Splits arcs into ≤90° segments for accurate Bezier approximation
        - Follows W3C SVG 1.1 specification for arc parameter interpretation
    """
    if x1 == x2 and y1 == y2:
        # From https://www.w3.org/TR/SVG/implnote.html#ArcImplementationNotes:
        # If the endpoints (x1, y1) and (x2, y2) are identical, then this is
        # equivalent to omitting the elliptical arc segment entirely.
        return []
    if phi:
        # Our box bezier arcs can't handle rotations directly
        # move to a well known point, eliminate phi and transform the other point
        mx = mmult(rotate(-phi), translate(-x1, -y1))
        tx2, ty2 = transformPoint(mx, (x2, y2))
        # Convert to box form in unrotated coords
        cx, cy, rx, ry, start_ang, extent = end_point_to_center_parameters(
            0, 0, tx2, ty2, fA, fS, rx, ry
        )
        bp = bezier_arc_from_centre(cx, cy, rx, ry, start_ang, extent)
        # Re-rotate by the desired angle and add back the translation
        mx = mmult(translate(x1, y1), rotate(phi))
        res = []
        for x1, y1, x2, y2, x3, y3, x4, y4 in bp:
            res.append(
                transformPoint(mx, (x1, y1))
                + transformPoint(mx, (x2, y2))
                + transformPoint(mx, (x3, y3))
                + transformPoint(mx, (x4, y4))
            )
        return res
    else:
        cx, cy, rx, ry, start_ang, extent = end_point_to_center_parameters(
            x1, y1, x2, y2, fA, fS, rx, ry
        )
        return bezier_arc_from_centre(cx, cy, rx, ry, start_ang, extent)
