/* -----------------------------------------------------------------------------
 * Programmer(s): David J. Gardner @ LLNL
 * -----------------------------------------------------------------------------
 * SUNDIALS Copyright Start
 * Copyright (c) 2002-2019, Lawrence Livermore National Security
 * and Southern Methodist University.
 * All rights reserved.
 *
 * See the top-level LICENSE and NOTICE files for details.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 * SUNDIALS Copyright End
 * -----------------------------------------------------------------------------
 * This is the header file for the implementation of the NVECTOR SensWrapper.
 *
 * Part I contains declarations specific to the implementation of the
 * vector wrapper.
 *
 * Part II defines accessor macros that allow the user to efficiently access
 * the content of the vector wrapper data structure.
 *
 * Part III contains the prototype for the constructors N_VNewEmpty_SensWrapper
 * and N_VNew_SensWrapper, as well as wrappers to NVECTOR vector operations.
 * ---------------------------------------------------------------------------*/

#ifndef _NVECTOR_SENSWRAPPER_H
#define _NVECTOR_SENSWRAPPER_H

#include <stdio.h>
#include <sundials/sundials_nvector.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/*==============================================================================
  PART I: NVector wrapper content structure
  ============================================================================*/

struct _N_VectorContent_SensWrapper {
  N_Vector* vecs;        /* array of wrapped vectors                */
  int nvecs;             /* number of wrapped vectors               */
  booleantype own_vecs;  /* flag indicating if wrapper owns vectors */
};

typedef struct _N_VectorContent_SensWrapper *N_VectorContent_SensWrapper;

/*==============================================================================
  PART II: Macros to access wrapper content
  ============================================================================*/

#define NV_CONTENT_SW(v)  ( (N_VectorContent_SensWrapper)(v->content) )
#define NV_VECS_SW(v)     ( NV_CONTENT_SW(v)->vecs )
#define NV_NVECS_SW(v)    ( NV_CONTENT_SW(v)->nvecs )
#define NV_OWN_VECS_SW(v) ( NV_CONTENT_SW(v)->own_vecs )
#define NV_VEC_SW(v,i)    ( NV_VECS_SW(v)[i] )

/*==============================================================================
  PART III: Exported functions
  ============================================================================*/

/* constructor creates an empty vector wrapper */
SUNDIALS_EXPORT N_Vector N_VNewEmpty_SensWrapper(int nvecs);
SUNDIALS_EXPORT N_Vector N_VNew_SensWrapper(int count, N_Vector w);

/* clone operations */
SUNDIALS_EXPORT N_Vector N_VCloneEmpty_SensWrapper(N_Vector w);
SUNDIALS_EXPORT N_Vector N_VClone_SensWrapper(N_Vector w);

/* destructor */
SUNDIALS_EXPORT void N_VDestroy_SensWrapper(N_Vector v);

/* standard vector operations */
SUNDIALS_EXPORT void N_VLinearSum_SensWrapper(realtype a, N_Vector x,
                                              realtype b, N_Vector y,
                                              N_Vector z);
SUNDIALS_EXPORT void N_VConst_SensWrapper(realtype c, N_Vector z);
SUNDIALS_EXPORT void N_VProd_SensWrapper(N_Vector x, N_Vector y, N_Vector z);
SUNDIALS_EXPORT void N_VDiv_SensWrapper(N_Vector x, N_Vector y, N_Vector z);
SUNDIALS_EXPORT void N_VScale_SensWrapper(realtype c, N_Vector x, N_Vector z);
SUNDIALS_EXPORT void N_VAbs_SensWrapper(N_Vector x, N_Vector z);
SUNDIALS_EXPORT void N_VInv_SensWrapper(N_Vector x, N_Vector z);
SUNDIALS_EXPORT void N_VAddConst_SensWrapper(N_Vector x, realtype b,
                                             N_Vector z);
SUNDIALS_EXPORT realtype N_VDotProd_SensWrapper(N_Vector x, N_Vector y);
SUNDIALS_EXPORT realtype N_VMaxNorm_SensWrapper(N_Vector x);
SUNDIALS_EXPORT realtype N_VWrmsNorm_SensWrapper(N_Vector x, N_Vector w);
SUNDIALS_EXPORT realtype N_VWrmsNormMask_SensWrapper(N_Vector x, N_Vector w,
                                                     N_Vector id);
SUNDIALS_EXPORT realtype N_VMin_SensWrapper(N_Vector x);
SUNDIALS_EXPORT realtype N_VWL2Norm_SensWrapper(N_Vector x, N_Vector w);
SUNDIALS_EXPORT realtype N_VL1Norm_SensWrapper(N_Vector x);
SUNDIALS_EXPORT void N_VCompare_SensWrapper(realtype c, N_Vector x, N_Vector z);
SUNDIALS_EXPORT booleantype N_VInvTest_SensWrapper(N_Vector x, N_Vector z);
SUNDIALS_EXPORT booleantype N_VConstrMask_SensWrapper(N_Vector c, N_Vector x,
                                                      N_Vector m);
SUNDIALS_EXPORT realtype N_VMinQuotient_SensWrapper(N_Vector num,
                                                    N_Vector denom);

#ifdef __cplusplus
}
#endif

#endif
