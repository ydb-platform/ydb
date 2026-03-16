# distutils: language=c++
# cython: language_level=2
"""
Cython wrapper for the C++ translation of the Angus Johnson's Clipper
library (ver. 6.2.1) (http://www.angusj.com/delphi/clipper.php)

This wrapper was written by Maxime Chalton, Lukas Treyer and Gregor Ratajc.

"""

SILENT = True


def log_action(description):
    if not SILENT:
        print description

log_action("Python binding clipper library")

import sys as _sys
import struct
import copy as _copy
import unicodedata as _unicodedata
import time as _time
import warnings as _warnings
import numbers as _numbers

from cython.operator cimport dereference as deref

cdef extern from "Python.h":
    Py_INCREF(object o)
    object Py_BuildValue(char *format, ...)
    object PyBuffer_FromMemory(void *ptr, int size)
    #int PyArg_ParseTuple(object struct,void* ptr)
    char*PyString_AsString(object string)
    int PyArg_VaParse(object args, char *format, ...)
    int PyArg_Parse(object args, char *format, ...)
    int PyObject_AsReadBuffer(object obj, void*buffer, int*buffer_len)
    object PyBuffer_FromObject(object base, int offset, int size)
    object PyBuffer_FromReadWriteObject(object base, int offset, int size)
    PyBuffer_New(object o)


cdef extern from "stdio.h":
    cdef void printf(char*, ...)

cdef extern from "stdlib.h":
    cdef void*malloc(unsigned int size)
    cdef void*free(void*p)
    char *strdup(char *str)
    int strcpy(void*str, void*src)
    int memcpy(void*str, void*src, int size)

from libcpp.vector cimport vector

cdef extern from "extra_defines.hpp":
    cdef int _USE_XYZ

cdef extern from "clipper.hpp" namespace "ClipperLib":

    # enum ClipType { ctIntersection, ctUnion, ctDifference, ctXor };
    cdef enum ClipType:
        ctIntersection = 1,
        ctUnion = 2,
        ctDifference = 3,
        ctXor = 4

    # enum PolyType { ptSubject, ptClip };
    cdef enum PolyType:
        ptSubject = 1,
        ptClip = 2

    # By far the most widely used winding rules for polygon filling are
    # EvenOdd & NonZero (GDI, GDI+, XLib, OpenGL, Cairo, AGG, Quartz, SVG, Gr32)
    # Others rules include Positive, Negative and ABS_GTR_EQ_TWO (only in OpenGL)
    # see http://glprogramming.com/red/chapter11.html
    # enum PolyFillType { pftEvenOdd, pftNonZero, pftPositive, pftNegative };
    cdef enum PolyFillType:
        pftEvenOdd = 1,
        pftNonZero = 2,
        pftPositive = 3,
        pftNegative = 4

    # The correct type definition is taken from cpp source, so
    # the use_int32 is handled correctly.
    # If you need 32 bit ints, just uncomment //#define use_int32 in clipper.hpp
    # and recompile
    ctypedef signed long long cInt
    ctypedef signed long long long64
    ctypedef unsigned long long ulong64

    ctypedef char bool

    # TODO: handle "use_xyz" that adds Z coordinate
    cdef struct IntPoint:
        cInt X
        cInt Y

    #typedef std::vector< IntPoint > Path;
    cdef cppclass Path:
        Path()
        void push_back(IntPoint &)
        IntPoint& operator[](int)
        IntPoint& at(int)
        int size()

    #typedef std::vector< Path > Paths;
    cdef cppclass Paths:
        Paths()
        void push_back(Path &)
        Path& operator[](int)
        Path& at(int)
        int size()

    cdef cppclass PolyNode:
        PolyNode()
        Path Contour
        PolyNodes Childs
        PolyNode*Parent
        PolyNode*GetNext()
        bool IsHole()
        bool IsOpen()
        int ChildCount()

    cdef cppclass PolyNodes:
        PolyNodes()
        void push_back(PolyNode &)
        PolyNode*operator[](int)
        PolyNode*at(int)
        int size()

    cdef cppclass PolyTree(PolyNode):
        PolyTree()
        PolyNode& GetFirst()
        void Clear()
        int Total()

    #enum InitOptions {ioReverseSolution = 1, ioStrictlySimple = 2, ioPreserveCollinear = 4};
    cdef enum InitOptions:
        ioReverseSolution = 1,
        ioStrictlySimple = 2,
        ioPreserveCollinear = 4

    #enum JoinType { jtSquare, jtRound, jtMiter };
    cdef enum JoinType:
        jtSquare = 1,
        jtRound = 2,
        jtMiter = 3

    #enum EndType {etClosedPolygon, etClosedLine, etOpenButt, etOpenSquare, etOpenRound};
    cdef enum EndType:
        etClosedPolygon = 1,
        etClosedLine = 2,
        etOpenButt = 3,
        etOpenSquare = 4,
        etOpenRound = 5

    cdef struct IntRect:
        cInt left
        cInt top
        cInt right
        cInt bottom

    cdef cppclass Clipper:
        Clipper(int initOptions)
        Clipper()
        #~Clipper()
        void Clear()
        bool Execute(ClipType clipType, Paths & solution, PolyFillType subjFillType, PolyFillType clipFillType) nogil
        bool Execute(ClipType clipType, PolyTree & solution, PolyFillType subjFillType, PolyFillType clipFillType) nogil
        bool ReverseSolution()
        void ReverseSolution(bool value)
        bool StrictlySimple()
        void StrictlySimple(bool value)
        bool PreserveCollinear()
        void PreserveCollinear(bool value)
        bool AddPath(Path & path, PolyType polyType, bool closed)
        bool AddPaths(Paths & paths, PolyType polyType, bool closed)
        IntRect GetBounds() nogil

    cdef cppclass ClipperOffset:
        ClipperOffset(double miterLimit, double roundPrecision)
        ClipperOffset(double miterLimit)
        ClipperOffset()
        #~ClipperOffset()
        void AddPath(Path & path, JoinType joinType, EndType endType)
        void AddPaths(Paths & paths, JoinType joinType, EndType endType)
        void Execute(Paths & solution, double delta) nogil
        void Execute(PolyTree & solution, double delta) nogil
        void Clear()
        double MiterLimit
        double ArcTolerance

    # prefixes are added to original functions to prevent naming collisions
    bool c_Orientation "Orientation"(const Path & poly) nogil
    double c_Area "Area"(const Path & poly) nogil
    int c_PointInPolygon "PointInPolygon"(const IntPoint & pt, const Path & path) nogil

    # In the following 4 functions default values for fillType and distance were removed
    # because it caused a bug in C++ code generated by Cython.
    # See Cython bug report: http://trac.cython.org/ticket/816
    void c_SimplifyPolygon "SimplifyPolygon"(const Path & in_poly, Paths & out_polys, PolyFillType fillType) nogil
    void c_SimplifyPolygons "SimplifyPolygons"(const Paths & in_polys, Paths & out_polys, PolyFillType fillType) nogil
    void c_CleanPolygon "CleanPolygon"(const Path& in_poly, Path& out_poly, double distance) nogil
    void c_CleanPolygons "CleanPolygons"(Paths& polys, double distance) nogil

    void c_MinkowskiSum "MinkowskiSum"(const Path& pattern, const Path& path, Paths& solution, bool pathIsClosed) nogil
    void c_MinkowskiSum "MinkowskiSum"(const Path& pattern, const Paths& paths, Paths& solution, bool pathIsClosed) nogil
    void c_MinkowskiDiff "MinkowskiDiff"(const Path& poly1, const Path& poly2, Paths& solution) nogil

    void c_PolyTreeToPaths "PolyTreeToPaths"(const PolyTree& polytree, Paths& paths)
    void c_ClosedPathsFromPolyTree "ClosedPathsFromPolyTree"(const PolyTree& polytree, Paths& paths)
    void c_OpenPathsFromPolyTree "OpenPathsFromPolyTree"(PolyTree& polytree, Paths& paths)

    void c_ReversePath "ReversePath"(Path& p) nogil
    void c_ReversePaths "ReversePaths"(Paths& p) nogil

#============================= Enum mapping ================

JT_SQUARE = jtSquare
JT_ROUND = jtRound
JT_MITER = jtMiter

ET_CLOSEDPOLYGON = etClosedPolygon
ET_CLOSEDLINE = etClosedLine
ET_OPENBUTT = etOpenButt
ET_OPENSQUARE = etOpenSquare
ET_OPENROUND = etOpenRound

CT_INTERSECTION = ctIntersection
CT_UNION = ctUnion
CT_DIFFERENCE = ctDifference
CT_XOR = ctXor

PT_SUBJECT = ptSubject
PT_CLIP = ptClip

PFT_EVENODD = pftEvenOdd
PFT_NONZERO = pftNonZero
PFT_POSITIVE = pftPositive
PFT_NEGATIVE = pftNegative

#=============================  PyPolyNode =================
class PyPolyNode:
    """
    Represents ClipperLibs' PolyTree and PolyNode data structures.
    """
    def __init__(self):
        self.Contour = []
        self.Childs = []
        self.Parent = None
        self.IsHole = False
        self.IsOpen = False
        self.depth = 0

#=============================  Other objects ==============
from collections import namedtuple
PyIntRect = namedtuple('PyIntRect', ['left', 'top', 'right', 'bottom'])

class ClipperException(Exception):
    pass

#============================= Namespace functions =========
def Orientation(poly):
    """ Get orientation of the supplied polygon.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/Orientation.htm

    Keyword arguments:
    poly -- closed polygon

    Returns:
    True  -- counter-clockwise orientation
    False -- clockwise orientation
    """
    cdef Path c_path = _to_clipper_path(poly)
    cdef bint result
    with nogil:
        result = <bint>c_Orientation(c_path)
    return result


def Area(poly):
    """ Get area of the supplied polygon.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/Area.htm

    Keyword arguments:
    poly -- closed polygon

    Returns:
    Positive number if orientation is True
    Negative number if orientation is False
    """

    cdef Path c_path = _to_clipper_path(poly)
    cdef double result
    with nogil:
        result = <double>c_Area(c_path)
    return result


def PointInPolygon(point, poly):
    """ Determine where does the point lie regarding the provided polygon.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/PointInPolygon.htm

    Keyword arguments:
    point -- point in question
    poly  -- closed polygon

    Returns:
    0  -- point is not in polygon
    -1 -- point is on polygon
    1  -- point is in polygon
    """

    cdef IntPoint c_point = _to_clipper_point(point)
    cdef Path c_path = _to_clipper_path(poly)
    cdef int result
    with nogil:
        result = <int>c_PointInPolygon(c_point, c_path)
    return result


def SimplifyPolygon(poly, PolyFillType fill_type=pftEvenOdd):
    """ Removes self-intersections from the supplied polygon.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/SimplifyPolygon.htm

    Keyword arguments:
    poly      -- polygon to be simplified
    fill_type -- PolyFillType used with the boolean union operation

    Returns:
    list of simplified polygons (containing one or more polygons)
    """
    cdef Paths out_polys
    cdef Path c_path = _to_clipper_path(poly)
    with nogil:
        c_SimplifyPolygon(c_path, out_polys, fill_type)
    return _from_clipper_paths(out_polys)


def SimplifyPolygons(polys, PolyFillType fill_type=pftEvenOdd):
    """ Removes self-intersections from the supplied polygons.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/SimplifyPolygons.htm

    Keyword arguments:
    polys     -- polygons to be simplified
    fill_type -- PolyFillType used with the boolean union operation

    Returns:
    list of simplified polygons
    """
    cdef Paths out_polys
    cdef Paths c_polys = _to_clipper_paths(polys)
    with nogil:
        c_SimplifyPolygons(c_polys, out_polys, fill_type)
    return _from_clipper_paths(out_polys)


def CleanPolygon(poly, double distance=1.415):
    """ Removes unnecessary vertices from the provided polygon.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/CleanPolygon.htm

    Keyword arguments:
    poly     -- polygon to be cleaned
    distance -- distance on which vertices are removed, see 'More info' (default: approx. sqrt of 2)

    Returns:
    cleaned polygon
    """
    cdef Path out_poly
    cdef Path c_path = _to_clipper_path(poly)
    with nogil:
        c_CleanPolygon(c_path, out_poly, distance)
    return _from_clipper_path(out_poly)


def CleanPolygons(polys, double distance=1.415):
    """ Removes unnecessary vertices from the provided polygons.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/CleanPolygons.htm

    Keyword arguments:
    polys    -- polygons to be cleaned
    distance -- distance on which vertices are removed, see 'More info' (default: approx. sqrt of 2)

    Returns:
    list of cleaned polygons
    """
    cdef Paths out_polys = _to_clipper_paths(polys)
    with nogil:
        c_CleanPolygons(out_polys, distance)
    return _from_clipper_paths(out_polys)


def MinkowskiSum(pattern, path, bint path_is_closed):
    """ Performs Minkowski Addition of the pattern and path.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/MinkowskiSum.htm

    Keyword arguments:
    pattern        -- polygon whose points are added to the path
    path           -- open or closed path
    path_is_closed -- set to True if passed path is closed, False if open

    Returns:
    list of polygons (containing one or more polygons)
    """
    cdef Paths solution
    cdef Path c_pat = _to_clipper_path(pattern)
    cdef Path c_path = _to_clipper_path(path)
    with nogil:
        c_MinkowskiSum(c_pat, c_path, solution, path_is_closed)
    return _from_clipper_paths(solution)


def MinkowskiSum2(pattern, paths, bint path_is_closed):
    """ Performs Minkowski Addition of the pattern and paths.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/MinkowskiSum.htm

    Keyword arguments:
    pattern        -- polygon whose points are added to the paths
    paths          -- open or closed paths
    path_is_closed -- set to True if passed paths are closed, False if open

    Returns:
    list of polygons
    """
    cdef Paths solution
    cdef Path c_pat = _to_clipper_path(pattern)
    cdef Paths c_paths = _to_clipper_paths(paths)
    with nogil:
        c_MinkowskiSum(c_pat, c_paths, solution, path_is_closed)
    return _from_clipper_paths(solution)


def MinkowskiDiff(poly1, poly2):
    """ Performs Minkowski Difference.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/MinkowskiDiff.htm

    Keyword arguments:
    poly1 -- polygon
    poly2 -- polygon

    Returns:
    list of polygons
    """
    cdef Paths solution
    cdef Path c_path1 = _to_clipper_path(poly1)
    cdef Path c_path2 = _to_clipper_path(poly2)
    with nogil:
        c_MinkowskiDiff(c_path1, c_path2, solution)
    return _from_clipper_paths(solution)


def PolyTreeToPaths(poly_node):
    """ Converts a PyPolyNode to a list of paths.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/PolyTreeToPaths.htm

    Keyword arguments:
    py_poly_node -- PyPolyNode to be filtered

    Returns:
    list of paths
    """
    paths = []
    _filter_polynode(poly_node, paths, filter_func=None)
    return paths


def ClosedPathsFromPolyTree(poly_node):
    """ Filters out open paths from the PyPolyNode and returns only closed paths.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/ClosedPathsFromPolyTree.htm

    Keyword arguments:
    py_poly_node -- PyPolyNode to be filtered

    Returns:
    list of closed paths
    """

    paths = []
    _filter_polynode(poly_node, paths, filter_func=lambda pn: not pn.IsOpen)
    return paths


def OpenPathsFromPolyTree(poly_node):
    """ Filters out closed paths from the PyPolyNode and returns only open paths.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/OpenPathsFromPolyTree.htm

    Keyword arguments:
    py_poly_node -- PyPolyNode to be filtered

    Returns:
    list of open paths
    """
    paths = []
    _filter_polynode(poly_node, paths, filter_func=lambda pn: pn.IsOpen)
    return paths


def ReversePath(path):
    """ Reverses the vertex order (and hence orientation) in the specified path.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/ReversePath.htm

    Note: Might be more effective to reverse the path outside of this package (eg. via [::-1] on a list)
    so there is no unneeded conversions to internal structures of this package.

    Keyword arguments:
    path -- path to be reversed

    Returns:
    reversed path
    """
    cdef Path c_path = _to_clipper_path(path)
    with nogil:
        c_ReversePath(c_path)
    return _from_clipper_path(c_path)


def ReversePaths(paths):
    """ Reverses the vertex order (and hence orientation) in each path.
    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Functions/ReversePaths.htm

    Note: Might be more effective to reverse each path outside of this package (eg. via [::-1] on a list)
    so there is no unneeded conversions to internal structures of this package.

    Keyword arguments:
    paths -- paths to be reversed

    Returns:
    list if reversed paths
    """
    cdef Paths c_paths = _to_clipper_paths(paths)
    with nogil:
        c_ReversePaths(c_paths)
    return _from_clipper_paths(c_paths)


def scale_to_clipper(path_or_paths, scale = 2 ** 31):
    """
    Take a path or list of paths with coordinates represented by floats and scale them using the specified factor.
    This function can be user to convert paths to a representation which is more appropriate for Clipper.

    Clipper, and thus Pyclipper, uses 64-bit integers to represent coordinates internally. The actual supported
    range (+/- 2 ** 62) is a bit smaller than the maximal values for this type. To operate on paths which use
    fractional coordinates, it is necessary to translate them from and to a representation which does not depend
    on floats. This can be done using this function and it's reverse, `scale_from_clipper()`.

    For details, see http://www.angusj.com/delphi/clipper/documentation/Docs/Overview/Rounding.htm.

    For example, to perform a clip operation on two polygons, the arguments to `Pyclipper.AddPath()` need to be wrapped
    in `scale_to_clipper()` while the return value needs to be converted back with `scale_from_clipper()`:

    >>> pc = Pyclipper()
    >>> path = [[0, 0], [1, 0], [1 / 2, (3 / 4) ** (1 / 2)]] # A triangle.
    >>> clip = [[0, 1 / 3], [1, 1 / 3], [1, 2 / 3], [0, 1 / 3]] # A rectangle.
    >>> pc.AddPath(scale_to_clipper(path), PT_SUBJECT)
    >>> pc.AddPath(scale_to_clipper(clip), PT_CLIP)
    >>> scale_from_clipper(pc.Execute(CT_INTERSECTION))
    [[[0.6772190444171429, 0.5590730146504939], [0.2383135547861457, 0.41277118446305394],
      [0.19245008938014507, 0.3333333330228925], [0.8075499106198549, 0.3333333330228925]]]

    :param path_or_paths: Either a list of paths or a path. A path is a list of tuples of numbers.
    :param scale: The factor with which to multiply coordinates before converting rounding them to ints. The default
    will give you a range of +/- 2 ** 31 with a precision of 2 ** -31.
    """

    def scale_value(x):
        if hasattr(x, "__len__"):
            return [scale_value(i) for i in x]
        else:
            return <cInt>(<double>x * scale)

    return scale_value(path_or_paths)


def scale_from_clipper(path_or_paths, scale = 2 ** 31):
    """
    Take a path or list of paths with coordinates represented by ints and scale them back to a fractional
    representation. This function does the inverse of `scale_to_clipper()`.

    :param path_or_paths: Either a list of paths or a path. A path is a list of tuples of numbers.
    :param scale: The factor by which to divide coordinates when converting them to floats.
    """

    def scale_value(x):
        if hasattr(x, "__len__"):
            return [scale_value(i) for i in x]
        else:
            return <double>x / scale

    return scale_value(path_or_paths)


cdef class Pyclipper:

    """Wraps the Clipper class.

    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/Clipper/_Body.htm
    """
    cdef Clipper *thisptr  # hold a C++ instance which we're wrapping
    def __cinit__(self):
        """ Creates an instance of the Clipper class. InitOptions from the Clipper class
        are substituted with separate properties.

        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/Clipper/Methods/Constructor.htm
        """

        log_action("Creating a Clipper instance")
        self.thisptr = new Clipper()

    def __dealloc__(self):
        log_action("Deleting the Clipper instance")
        del self.thisptr

    def AddPath(self, path, PolyType poly_type, closed=True):
        """ Add individual path.
        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/ClipperBase/Methods/AddPath.htm

        Keyword arguments:
        path      -- path to be added
        poly_type -- type of the added path - subject or clip
        closed    -- True if the added path is closed, False if open

        Returns:
        True -- path is valid for clipping and was added

        Raises:
        ClipperException -- if path is invalid for clipping
        """
        cdef Path c_path = _to_clipper_path(path)
        cdef bint result = <bint> self.thisptr.AddPath(c_path, poly_type, <bint> closed)
        if not result:
            raise ClipperException('The path is invalid for clipping')
        return result

    def AddPaths(self, paths, PolyType poly_type, closed=True):
        """ Add a list of paths.
        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/ClipperBase/Methods/AddPaths.htm

        Keyword arguments:
        paths     -- paths to be added
        poly_type -- type of added paths - subject or clip
        closed    -- True if added paths are closed, False if open

        Returns:
        True -- all or some paths are valid for clipping and were added

        Raises:
        ClipperException -- all paths are invalid for clipping
        """
        cdef Paths c_paths = _to_clipper_paths(paths)
        cdef bint result = <bint> self.thisptr.AddPaths(c_paths, poly_type, <bint> closed)
        if not result:
            raise ClipperException('All paths are invalid for clipping')
        return result

    def Clear(self):
        """ Removes all subject and clip polygons.
        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/ClipperBase/Methods/Clear.htm
        """
        self.thisptr.Clear()

    def GetBounds(self):
        """ Returns an axis-aligned bounding rectangle that bounds all added polygons.
        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/ClipperBase/Methods/GetBounds.htm

        Returns:
        PyIntRect with left, right, bottom, top vertices that define the axis-aligned bounding rectangle.
        """
        cdef IntRect rr
        with nogil:
            rr = <IntRect> self.thisptr.GetBounds()
        return PyIntRect(left=rr.left, top=rr.top,
                         right=rr.right, bottom=rr.bottom)

    def Execute(self, ClipType clip_type,
                PolyFillType subj_fill_type=pftEvenOdd, PolyFillType clip_fill_type=pftEvenOdd):
        """ Performs the clipping operation and returns a list of paths.
        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/Clipper/Methods/Execute.htm

        Keyword arguments:
        clip_type      -- type of the clipping operation
        subj_fill_type -- fill rule of subject paths
        clip_fill_type -- fill rule of clip paths

        Returns:
        list of resulting paths

        Raises:
        ClipperException -- operation did not succeed
        """

        cdef Paths solution
        cdef bint success
        with nogil:
            success = <bint> self.thisptr.Execute(clip_type, solution, subj_fill_type, clip_fill_type)
        if not success:
            raise ClipperException('Execution of clipper did not succeed!')
        return _from_clipper_paths(solution)

    def Execute2(self, ClipType clip_type,
                 PolyFillType subj_fill_type=pftEvenOdd, PolyFillType clip_fill_type=pftEvenOdd):
        """ Performs the clipping operation and returns a PyPolyNode.
        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/Clipper/Methods/Execute.htm

        Keyword arguments:
        clip_type      -- type of the clipping operation
        subj_fill_type -- fill rule of subject paths
        clip_fill_type -- fill rule of clip paths

        Returns:
        PyPolyNode

        Raises:
        ClipperException -- operation did not succeed
        """
        cdef PolyTree solution
        cdef bint success
        with nogil:
            success = <bint> self.thisptr.Execute(clip_type, solution, subj_fill_type, clip_fill_type)
        if not success:
            raise ClipperException('Execution of clipper did not succeed!')
        return _from_poly_tree(solution)

    property ReverseSolution:
        """ Should polygons returned from Execute/Execute2 have their orientations
        opposite to their normal orientations.
        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/Clipper/Properties/ReverseSolution.htm
        """
        def __get__(self):
            return <bint> self.thisptr.ReverseSolution()

        def __set__(self, value):
            self.thisptr.ReverseSolution(<bint> value)

    property PreserveCollinear:
        """ Should clipper preserve collinear vertices.
        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/Clipper/Properties/PreserveCollinear.htm
        """
        def __get__(self):
            return <bint> self.thisptr.PreserveCollinear()

        def __set__(self, value):
            self.thisptr.PreserveCollinear(<bint> value)

    property StrictlySimple:
        """ Should polygons returned from Execute/Execute2 be strictly simple (True) or may be weakly simple (False).
        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/Clipper/Properties/StrictlySimple.htm
        """
        def __get__(self):
            return <bint> self.thisptr.StrictlySimple()

        def __set__(self, value):
            self.thisptr.StrictlySimple(<bint> value)


cdef class PyclipperOffset:
    """ Wraps the ClipperOffset class.

    More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/ClipperOffset/_Body.htm
    """
    cdef ClipperOffset *thisptr

    def __cinit__(self, double miter_limit=2.0, double arc_tolerance=0.25):
        """ Creates an instance of the ClipperOffset class.

        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/ClipperOffset/Methods/Constructor.htm
        """
        log_action("Creating an ClipperOffset instance")
        self.thisptr = new ClipperOffset(miter_limit, arc_tolerance)

    def __dealloc__(self):
        log_action("Deleting the ClipperOffset instance")
        del self.thisptr

    def AddPath(self, path, JoinType join_type, EndType end_type):
        """ Add individual path.
        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/ClipperOffset/Methods/AddPath.htm

        Keyword arguments:
        path      -- path to be added
        join_type -- join type of added path
        end_type  -- end type of added path
        """
        cdef Path c_path = _to_clipper_path(path)
        self.thisptr.AddPath(c_path, join_type, end_type)

    def AddPaths(self, paths, JoinType join_type, EndType end_type):
        """ Add a list of paths.
        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/ClipperOffset/Methods/AddPaths.htm

        Keyword arguments:
        path      -- paths to be added
        join_type -- join type of added paths
        end_type  -- end type of added paths
        """
        cdef Paths c_paths = _to_clipper_paths(paths)
        self.thisptr.AddPaths(c_paths, join_type, end_type)

    def Execute(self, double delta):
        """ Performs the offset operation and returns a list of offset paths.
        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/ClipperOffset/Methods/Execute.htm

        Keyword arguments:
        delta -- amount to which the supplied paths will be offset - negative delta shrinks polygons,
                 positive delta expands them.

        Returns:
        list of offset paths
        """
        cdef Paths c_solution
        with nogil:
            self.thisptr.Execute(c_solution, delta)
        return _from_clipper_paths(c_solution)

    def Execute2(self, double delta):
        """ Performs the offset operation and returns a PyPolyNode with offset paths.
        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/ClipperOffset/Methods/Execute.htm

        Keyword arguments:
        delta -- amount to which the supplied paths will be offset - negative delta shrinks polygons,
                 positive delta expands them.

        Returns:
        PyPolyNode
        """
        cdef PolyTree solution
        with nogil:
            self.thisptr.Execute(solution, delta)
        return _from_poly_tree(solution)

    def Clear(self):
        """ Clears all paths.

        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/ClipperOffset/Methods/Clear.htm
        """
        self.thisptr.Clear()

    property MiterLimit:
        """ Maximum distance in multiples of delta that vertices can be offset from their
        original positions.

        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/ClipperOffset/Properties/MiterLimit.htm
        """
        def __get__(self):
            return <double> self.thisptr.MiterLimit

        def __set__(self, value):
            self.thisptr.MiterLimit = <double> value

    property ArcTolerance:
        """ Maximum acceptable imprecision when arcs are approximated in
        an offsetting operation.

        More info: http://www.angusj.com/delphi/clipper/documentation/Docs/Units/ClipperLib/Classes/ClipperOffset/Properties/ArcTolerance.htm
        """
        def __get__(self):
            return self.thisptr.ArcTolerance

        def __set__(self, value):
            self.thisptr.ArcTolerance = value


cdef _filter_polynode(pypolynode, result, filter_func=None):
    if (filter_func is None or filter_func(pypolynode)) and len(pypolynode.Contour) > 0:
        result.append(pypolynode.Contour)

    for child in pypolynode.Childs:
        _filter_polynode(child, result, filter_func)


cdef _from_poly_tree(PolyTree &c_polytree):
    poly_tree = PyPolyNode()
    depths = [0]
    for i in xrange(c_polytree.ChildCount()):
        c_child = c_polytree.Childs[i]
        py_child = _node_walk(c_child, poly_tree)
        poly_tree.Childs.append(py_child)
        depths.append(py_child.depth + 1)
    poly_tree.depth = max(depths)
    return poly_tree


cdef _node_walk(PolyNode *c_polynode, object parent):

    py_node = PyPolyNode()
    py_node.Parent = parent

    cdef object ishole = <bint>c_polynode.IsHole()
    py_node.IsHole = ishole

    cdef object isopen = <bint>c_polynode.IsOpen()
    py_node.IsOpen = isopen

    py_node.Contour = _from_clipper_path(c_polynode.Contour)

    # kids
    cdef PolyNode *cNode
    depths = [0]
    for i in range(c_polynode.ChildCount()):
        c_node = c_polynode.Childs[i]
        py_child = _node_walk(c_node, py_node)

        depths.append(py_child.depth + 1)
        py_node.Childs.append(py_child)

    py_node.depth = max(depths)

    return py_node


cdef Paths _to_clipper_paths(object polygons):
    cdef Paths paths = Paths()
    for poly in polygons:
        paths.push_back(_to_clipper_path(poly))
    return paths


cdef Path _to_clipper_path(object polygon):
    cdef Path path = Path()
    cdef IntPoint p
    for v in polygon:
        path.push_back(_to_clipper_point(v))
    return path


cdef IntPoint _to_clipper_point(object py_point):
    return IntPoint(py_point[0], py_point[1])


cdef object _from_clipper_paths(Paths paths):

    polys = []

    cdef Path path
    for i in xrange(paths.size()):
        path = paths[i]
        polys.append(_from_clipper_path(path))

    return polys


cdef object _from_clipper_path(Path path):
    poly = []
    cdef IntPoint point
    for i in xrange(path.size()):
        point = path[i]
        poly.append([point.X, point.Y])
    return poly
