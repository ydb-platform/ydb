PY2_LIBRARY()

LICENSE(PSF-2.0)

NO_COMPILER_WARNINGS()

PEERDIR(
    ADDINCL contrib/libs/freetype
    ADDINCL contrib/libs/libpng
    ADDINCL contrib/python/numpy
    contrib/libs/qhull
    contrib/python/matplotlib/py2/extern/agg24-svn
    contrib/python/matplotlib/py2/extern/ttconv
)

ADDINCL(
    contrib/libs/qhull/libqhull_r
    contrib/python/matplotlib/py2
    contrib/python/matplotlib/py2/extern
    contrib/python/matplotlib/py2/extern/agg24-svn/include
)

CFLAGS(
    -D_MULTIARRAYMODULE
    -DFREETYPE_BUILD_TYPE=local
    -DNPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
    -DMPL_DEVNULL=/dev/null
)

IF (OS_WINDOWS)
    LDFLAGS(
        Psapi.lib
    )
ENDIF()

PY_REGISTER(
    matplotlib._contour
    matplotlib._image # peerdir agg24-svn
    matplotlib._path # peerdir agg24-svn
    matplotlib._png
    matplotlib._qhull # peerdir libqhull
    matplotlib.backends._backend_agg # peerdir agg24-svn
    matplotlib.backends._tkagg
    matplotlib.ft2font
    matplotlib.ttconv # peerdir ttconv
)

SRCS(
    _backend_agg.cpp
    _backend_agg_wrapper.cpp
    _contour.cpp
    _contour_wrapper.cpp
    _image.cpp
    _image_wrapper.cpp
    _path_wrapper.cpp
    _png.cpp
    _tkagg.cpp
    _ttconv.cpp
    ft2font.cpp
    ft2font_wrapper.cpp
    mplutils.cpp
    py_converters.cpp
    qhull_wrap.c
)

END()
