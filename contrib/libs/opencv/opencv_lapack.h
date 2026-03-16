// This file is auto-generated

extern "C" {

#ifdef _MSC_VER
#include <complex.h>
#define lapack_complex_float _Fcomplex
#define lapack_complex_double _Dcomplex
#endif

#include "contrib/libs/cblas/include/cblas.h"
#include "contrib/libs/eigen/Eigen/src/misc/lapacke.h"

#if defined(LAPACK_GLOBAL) || defined(LAPACK_NAME)
/*
 * Using netlib's reference LAPACK implementation version >= 3.4.0 (first with C interface).
 * Use LAPACK_xxxx to transparently (via predefined lapack macros) deal with pre and post 3.9.1 versions.
 * LAPACK 3.9.1 introduces LAPACK_FORTRAN_STRLEN_END and modifies (through preprocessing) the declarations of the following functions used in opencv
 *        sposv_, dposv_, spotrf_, dpotrf_, sgesdd_, dgesdd_, sgels_, dgels_
 * which end up with an extra parameter.
 * So we also need to preprocess the function calls in opencv coding by prefixing them with LAPACK_.
 * The good news is the preprocessing works fine whatever netlib's LAPACK version.
 */
#define OCV_LAPACK_FUNC(f) LAPACK_##f
#else
/* Using other LAPACK implementations so fall back to opencv's assumption until now */
#define OCV_LAPACK_FUNC(f) f##_
#endif

}