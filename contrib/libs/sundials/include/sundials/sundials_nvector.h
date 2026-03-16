/* -----------------------------------------------------------------
 * Programmer(s): Radu Serban and Aaron Collier @ LLNL
 * -----------------------------------------------------------------
 * SUNDIALS Copyright Start
 * Copyright (c) 2002-2019, Lawrence Livermore National Security
 * and Southern Methodist University.
 * All rights reserved.
 *
 * See the top-level LICENSE and NOTICE files for details.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 * SUNDIALS Copyright End
 * -----------------------------------------------------------------
 * This is the header file for a generic NVECTOR package.
 * It defines the N_Vector structure (_generic_N_Vector) which
 * contains the following fields:
 *   - an implementation-dependent 'content' field which contains
 *     the description and actual data of the vector
 *   - an 'ops' filed which contains a structure listing operations
 *     acting on such vectors
 * -----------------------------------------------------------------
 * This header file contains:
 *   - enumeration constants for all SUNDIALS-defined vector types,
 *     as well as a generic type for user-supplied vector types,
 *   - type declarations for the _generic_N_Vector and
 *     _generic_N_Vector_Ops structures, as well as references to
 *     pointers to such structures (N_Vector), and
 *   - prototypes for the vector functions which operate on
 *     N_Vector objects.
 * -----------------------------------------------------------------
 * At a minimum, a particular implementation of an NVECTOR must
 * do the following:
 *   - specify the 'content' field of N_Vector,
 *   - implement the operations on those N_Vector objects,
 *   - provide a constructor routine for new N_Vector objects
 *
 * Additionally, an NVECTOR implementation may provide the following:
 *   - macros to access the underlying N_Vector data
 *   - a constructor for an array of N_Vectors
 *   - a constructor for an empty N_Vector (i.e., a new N_Vector with
 *     a NULL data pointer).
 *   - a routine to print the content of an N_Vector
 * -----------------------------------------------------------------*/

#ifndef _NVECTOR_H
#define _NVECTOR_H

#include <sundials/sundials_types.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif


/* -----------------------------------------------------------------
 * Implemented N_Vector types
 * ----------------------------------------------------------------- */

typedef enum {
  SUNDIALS_NVEC_SERIAL,
  SUNDIALS_NVEC_PARALLEL,
  SUNDIALS_NVEC_OPENMP,
  SUNDIALS_NVEC_PTHREADS,
  SUNDIALS_NVEC_PARHYP,
  SUNDIALS_NVEC_PETSC,
  SUNDIALS_NVEC_CUDA,
  SUNDIALS_NVEC_RAJA,
  SUNDIALS_NVEC_OPENMPDEV,
  SUNDIALS_NVEC_TRILINOS,
  SUNDIALS_NVEC_CUSTOM
} N_Vector_ID;


/* -----------------------------------------------------------------
 * Generic definition of N_Vector
 * ----------------------------------------------------------------- */

/* Forward reference for pointer to N_Vector_Ops object */
typedef struct _generic_N_Vector_Ops *N_Vector_Ops;

/* Forward reference for pointer to N_Vector object */
typedef struct _generic_N_Vector *N_Vector;

/* Define array of N_Vectors */
typedef N_Vector *N_Vector_S;

/* Structure containing function pointers to vector operations  */
struct _generic_N_Vector_Ops {
  N_Vector_ID (*nvgetvectorid)(N_Vector);
  N_Vector    (*nvclone)(N_Vector);
  N_Vector    (*nvcloneempty)(N_Vector);
  void        (*nvdestroy)(N_Vector);
  void        (*nvspace)(N_Vector, sunindextype *, sunindextype *);
  realtype*   (*nvgetarraypointer)(N_Vector);
  void        (*nvsetarraypointer)(realtype *, N_Vector);

  /* standard vector operations */
  void        (*nvlinearsum)(realtype, N_Vector, realtype, N_Vector, N_Vector);
  void        (*nvconst)(realtype, N_Vector);
  void        (*nvprod)(N_Vector, N_Vector, N_Vector);
  void        (*nvdiv)(N_Vector, N_Vector, N_Vector);
  void        (*nvscale)(realtype, N_Vector, N_Vector);
  void        (*nvabs)(N_Vector, N_Vector);
  void        (*nvinv)(N_Vector, N_Vector);
  void        (*nvaddconst)(N_Vector, realtype, N_Vector);
  realtype    (*nvdotprod)(N_Vector, N_Vector);
  realtype    (*nvmaxnorm)(N_Vector);
  realtype    (*nvwrmsnorm)(N_Vector, N_Vector);
  realtype    (*nvwrmsnormmask)(N_Vector, N_Vector, N_Vector);
  realtype    (*nvmin)(N_Vector);
  realtype    (*nvwl2norm)(N_Vector, N_Vector);
  realtype    (*nvl1norm)(N_Vector);
  void        (*nvcompare)(realtype, N_Vector, N_Vector);
  booleantype (*nvinvtest)(N_Vector, N_Vector);
  booleantype (*nvconstrmask)(N_Vector, N_Vector, N_Vector);
  realtype    (*nvminquotient)(N_Vector, N_Vector);

  /* fused vector operations */
  int (*nvlinearcombination)(int, realtype*, N_Vector*, N_Vector);
  int (*nvscaleaddmulti)(int, realtype*, N_Vector, N_Vector*, N_Vector*);
  int (*nvdotprodmulti)(int, N_Vector, N_Vector*, realtype*);

  /* vector array operations */
  int (*nvlinearsumvectorarray)(int, realtype, N_Vector*, realtype, N_Vector*,
                                N_Vector*);
  int (*nvscalevectorarray)(int, realtype*, N_Vector*, N_Vector*);
  int (*nvconstvectorarray)(int, realtype, N_Vector*);
  int (*nvwrmsnormvectorarray)(int, N_Vector*, N_Vector*, realtype*);
  int (*nvwrmsnormmaskvectorarray)(int, N_Vector*, N_Vector*, N_Vector,
                                   realtype*);
  int (*nvscaleaddmultivectorarray)(int, int, realtype*, N_Vector*, N_Vector**,
                                    N_Vector**);
  int (*nvlinearcombinationvectorarray)(int, int, realtype*, N_Vector**,
                                        N_Vector*);
};

/* A vector is a structure with an implementation-dependent
   'content' field, and a pointer to a structure of vector
   operations corresponding to that implementation. */
struct _generic_N_Vector {
  void *content;
  struct _generic_N_Vector_Ops *ops;
};


/* -----------------------------------------------------------------
 * Functions exported by NVECTOR module
 * ----------------------------------------------------------------- */

SUNDIALS_EXPORT N_Vector_ID N_VGetVectorID(N_Vector w);
SUNDIALS_EXPORT N_Vector N_VClone(N_Vector w);
SUNDIALS_EXPORT N_Vector N_VCloneEmpty(N_Vector w);
SUNDIALS_EXPORT void N_VDestroy(N_Vector v);
SUNDIALS_EXPORT void N_VSpace(N_Vector v, sunindextype *lrw, sunindextype *liw);
SUNDIALS_EXPORT realtype *N_VGetArrayPointer(N_Vector v);
SUNDIALS_EXPORT void N_VSetArrayPointer(realtype *v_data, N_Vector v);

/* standard vector operations */
SUNDIALS_EXPORT void N_VLinearSum(realtype a, N_Vector x, realtype b,
                                  N_Vector y, N_Vector z);
SUNDIALS_EXPORT void N_VConst(realtype c, N_Vector z);
SUNDIALS_EXPORT void N_VProd(N_Vector x, N_Vector y, N_Vector z);
SUNDIALS_EXPORT void N_VDiv(N_Vector x, N_Vector y, N_Vector z);
SUNDIALS_EXPORT void N_VScale(realtype c, N_Vector x, N_Vector z);
SUNDIALS_EXPORT void N_VAbs(N_Vector x, N_Vector z);
SUNDIALS_EXPORT void N_VInv(N_Vector x, N_Vector z);
SUNDIALS_EXPORT void N_VAddConst(N_Vector x, realtype b, N_Vector z);
SUNDIALS_EXPORT realtype N_VDotProd(N_Vector x, N_Vector y);
SUNDIALS_EXPORT realtype N_VMaxNorm(N_Vector x);
SUNDIALS_EXPORT realtype N_VWrmsNorm(N_Vector x, N_Vector w);
SUNDIALS_EXPORT realtype N_VWrmsNormMask(N_Vector x, N_Vector w, N_Vector id);
SUNDIALS_EXPORT realtype N_VMin(N_Vector x);
SUNDIALS_EXPORT realtype N_VWL2Norm(N_Vector x, N_Vector w);
SUNDIALS_EXPORT realtype N_VL1Norm(N_Vector x);
SUNDIALS_EXPORT void N_VCompare(realtype c, N_Vector x, N_Vector z);
SUNDIALS_EXPORT booleantype N_VInvTest(N_Vector x, N_Vector z);
SUNDIALS_EXPORT booleantype N_VConstrMask(N_Vector c, N_Vector x, N_Vector m);
SUNDIALS_EXPORT realtype N_VMinQuotient(N_Vector num, N_Vector denom);

/* fused vector operations */
SUNDIALS_EXPORT int N_VLinearCombination(int nvec, realtype* c, N_Vector* X,
                                         N_Vector z);

SUNDIALS_EXPORT int N_VScaleAddMulti(int nvec, realtype* a, N_Vector x,
                                     N_Vector* Y, N_Vector* Z);

SUNDIALS_EXPORT int N_VDotProdMulti(int nvec, N_Vector x, N_Vector* Y,
                                    realtype* dotprods);

/* vector array operations */
SUNDIALS_EXPORT int N_VLinearSumVectorArray(int nvec,
                                            realtype a, N_Vector* X,
                                            realtype b, N_Vector* Y,
                                            N_Vector* Z);

SUNDIALS_EXPORT int N_VScaleVectorArray(int nvec, realtype* c, N_Vector* X,
                                        N_Vector* Z);

SUNDIALS_EXPORT int N_VConstVectorArray(int nvec, realtype c, N_Vector* Z);

SUNDIALS_EXPORT int N_VWrmsNormVectorArray(int nvec, N_Vector* X, N_Vector* W,
                                           realtype* nrm);

SUNDIALS_EXPORT int N_VWrmsNormMaskVectorArray(int nvec, N_Vector* X,
                                               N_Vector* W, N_Vector id,
                                               realtype* nrm);

SUNDIALS_EXPORT int N_VScaleAddMultiVectorArray(int nvec, int nsum,
                                                realtype* a, N_Vector* X,
                                                N_Vector** Y, N_Vector** Z);

SUNDIALS_EXPORT int N_VLinearCombinationVectorArray(int nvec, int nsum,
                                                    realtype* c, N_Vector** X,
                                                    N_Vector* Z);


/* -----------------------------------------------------------------
 * Additional functions exported by NVECTOR module
 * ----------------------------------------------------------------- */

SUNDIALS_EXPORT N_Vector *N_VCloneEmptyVectorArray(int count, N_Vector w);
SUNDIALS_EXPORT N_Vector *N_VCloneVectorArray(int count, N_Vector w);
SUNDIALS_EXPORT void N_VDestroyVectorArray(N_Vector *vs, int count);

#ifdef __cplusplus
}
#endif

#endif
