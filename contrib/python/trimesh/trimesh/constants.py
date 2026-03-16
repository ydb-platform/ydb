from dataclasses import dataclass

import numpy as np

from .util import decimal_to_digits, log, now


@dataclass
class ToleranceMesh:
    """
    ToleranceMesh objects hold tolerance information about meshes.

    Parameters
    ----------------
    tol.zero : float
      Floating point numbers smaller than this are considered zero
    tol.merge : float
      When merging vertices, consider vertices closer than this
      to be the same vertex. Here we use the same value (1e-8)
      as SolidWorks uses, according to their documentation.
    tol.planar : float
      The maximum distance from a plane a point can be and
      still be considered to be on the plane
    tol.facet_threshold : float
      Threshold for two facets to be considered coplanar
    tol.strict : bool
      If True, run additional in- process checks (slower)
    """

    # set our zero for floating point comparison to 100x
    # the resolution of float64 which works out to 1e-13
    zero: float = np.finfo(np.float64).resolution * 100

    # vertices closer than this should be merged
    merge: float = 1e-8

    # peak to valley flatness to be considered planar
    planar: float = 1e-5

    # coplanar threshold: ratio of (radius / span) ** 2
    facet_threshold: int = 5000

    # should additional slow checks be run inside functions
    strict: bool = False


@dataclass
class TolerancePath:
    """
    TolerancePath objects contain tolerance information used in
    Path objects.

    Parameters
    ---------------
    tol.zero : float
      Floating point numbers smaller than this are considered zero
    tol.merge : float
      When merging vertices, consider vertices closer than this
      to be the same vertex. Here we use the same value (1e-8)
      as SolidWorks uses, according to their documentation.
    tol.planar : float
      The maximum distance from a plane a point can be and
      still be considered to be on the plane
    tol.seg_frac : float
      When simplifying line segments what percentage of the drawing
       scale can a segment be and have a curve fitted
    tol.seg_angle : float
      When simplifying line segments to arcs, what angle
      can a segment span to be acceptable.
    tol.aspect_frac : float
      When simplifying line segments to closed arcs (circles)
      what percentage can the aspect ratio differfrom 1:1
      before escaping the fit early
    tol.radius_frac : float
      When simplifying line segments to arcs, what percentage
      of the fit radius can vertices deviate to be acceptable
    tol.radius_min :
       When simplifying line segments to arcs, what is the minimum
       radius multiplied by document scale for an acceptable fit
    tol.radius_max :
       When simplifying line segments to arcs, what is the maximum
       radius multiplied by document scale for an acceptable fit
    tol.tangent :
       When simplifying line segments to curves, what is the maximum
       angle the end sections can deviate from tangent that is
       acceptable.
    """

    zero: float = 1e-12
    merge: float = 1e-5

    planar: float = 1e-5
    seg_frac: float = 0.125
    seg_angle: float = float(np.radians(50))
    seg_angle_min: float = float(np.radians(1))
    seg_angle_frac: float = 0.5
    aspect_frac: float = 0.1
    radius_frac: float = 0.02
    radius_min: float = 1e-4
    radius_max: float = 50.0
    tangent: float = float(np.radians(20))
    strict: bool = False

    @property
    def merge_digits(self) -> int:
        return decimal_to_digits(self.merge)


@dataclass
class ResolutionPath:
    """
    res.seg_frac : float
      When discretizing curves, what percentage of the drawing
      scale should we aim to make a single segment
    res.seg_angle : float
      When discretizing curves, what angle should a section span
    res.max_sections : int
      When discretizing splines, what is the maximum number
      of segments per control point
    res.min_sections : int
      When discretizing splines, what is the minimum number
      of segments per control point
    res.export : str
      Format string to use when exporting floating point vertices
    """

    seg_frac: float = 0.05
    seg_angle: float = 0.08
    max_sections: float = 500.0
    min_sections: float = 20.0
    export: str = "0.10f"


# instantiate mesh tolerances with defaults
tol = ToleranceMesh()

# instantiate path tolerances with defaults
tol_path = TolerancePath()
res_path = ResolutionPath()


def log_time(method):
    """
    A decorator for methods which will time the method
    and then emit a log.debug message with the method name
    and how long it took to execute.
    """

    def timed(*args, **kwargs):
        tic = now()
        result = method(*args, **kwargs)
        log.debug("%s executed in %.4f seconds.", method.__name__, now() - tic)

        return result

    timed.__name__ = method.__name__
    timed.__doc__ = method.__doc__
    return timed
