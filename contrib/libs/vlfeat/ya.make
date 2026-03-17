LIBRARY()

VERSION(0.9.20)

LICENSE(BSD-3-Clause)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

NO_COMPILER_WARNINGS()

IF (OS_WINDOWS)
    CFLAGS(-D__SSE2__)
ENDIF()

ENABLE(VL_DISABLE_AVX)

IF (VL_DISABLE_AVX)
    CFLAGS(-DVL_DISABLE_AVX)
ELSE()
    CFLAGS(-mavx)
ENDIF()

SRCDIR(contrib/libs/vlfeat/vl)

SRCS(
    aib.c
    array.c
    covdet.c
    dsift.c
    fisher.c
    generic.c
    getopt_long.c
    gmm.c
    hikmeans.c
    hog.c
    homkermap.c
    host.c
    ikmeans.c
    imopv.c
    kdtree.c
    kmeans.c
    lbp.c
    liop.c
    mathop.c
    mser.c
    pgm.c
    quickshift.c
    random.c
    rodrigues.c
    scalespace.c
    sift.c
    slic.c
    stringop.c
    svm.c
    svmdataset.c
    vlad.c
)

IF (MY_PLATFORM != arm)
    SRCS(
        imopv_sse2.c
        mathop_sse2.c
        mathop_avx.c
    )
ENDIF()

END()
