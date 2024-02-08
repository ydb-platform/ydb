LIBRARY()

LICENSE(PSF-2.0)

ADDINCL(
    contrib/python/matplotlib/py2/extern/agg24-svn/include
)

NO_COMPILER_WARNINGS()

SRCS(
    src/agg_bezier_arc.cpp
    src/agg_curves.cpp
    src/agg_image_filters.cpp
    src/agg_trans_affine.cpp
    src/agg_vcgen_contour.cpp
    src/agg_vcgen_dash.cpp
    src/agg_vcgen_stroke.cpp
    src/agg_vpgen_segmentator.cpp
)

END()
