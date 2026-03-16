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
 * This is the implementation file for a vector wrapper for an array of NVECTORS
 * ---------------------------------------------------------------------------*/

#include <stdio.h>
#include <stdlib.h>

#include <stdarg.h>
#include <string.h>

#include <sundials/sundials_nvector.h>
#include <sundials/sundials_nvector_senswrapper.h>

#define ZERO RCONST(0.0)

/*==============================================================================
  Constructors
  ============================================================================*/

/*------------------------------------------------------------------------------
  create a new empty vector wrapper with space for <nvecs> vectors
  ----------------------------------------------------------------------------*/
N_Vector N_VNewEmpty_SensWrapper(int nvecs)
{
  int i;
  N_Vector v;
  N_Vector_Ops ops;
  N_VectorContent_SensWrapper content;

  /* return if wrapper is empty */
  if (nvecs < 1) return(NULL);

  /* create vector */
  v = NULL;
  v = (N_Vector) malloc(sizeof *v);
  if (v == NULL) return(NULL);

  /* create vector operation structure */
  ops = NULL;
  ops = (N_Vector_Ops) malloc(sizeof *ops);
  if (ops == NULL) {free(v); return(NULL);}

  ops->nvgetvectorid     = NULL;
  ops->nvclone           = N_VClone_SensWrapper;
  ops->nvcloneempty      = N_VCloneEmpty_SensWrapper;
  ops->nvdestroy         = N_VDestroy_SensWrapper;
  ops->nvspace           = NULL;
  ops->nvgetarraypointer = NULL;
  ops->nvsetarraypointer = NULL;

  /* standard vector operations */
  ops->nvlinearsum    = N_VLinearSum_SensWrapper;
  ops->nvconst        = N_VConst_SensWrapper;
  ops->nvprod         = N_VProd_SensWrapper;
  ops->nvdiv          = N_VDiv_SensWrapper;
  ops->nvscale        = N_VScale_SensWrapper;
  ops->nvabs          = N_VAbs_SensWrapper;
  ops->nvinv          = N_VInv_SensWrapper;
  ops->nvaddconst     = N_VAddConst_SensWrapper;
  ops->nvdotprod      = N_VDotProd_SensWrapper;
  ops->nvmaxnorm      = N_VMaxNorm_SensWrapper;
  ops->nvwrmsnormmask = N_VWrmsNormMask_SensWrapper;
  ops->nvwrmsnorm     = N_VWrmsNorm_SensWrapper;
  ops->nvmin          = N_VMin_SensWrapper;
  ops->nvwl2norm      = N_VWL2Norm_SensWrapper;
  ops->nvl1norm       = N_VL1Norm_SensWrapper;
  ops->nvcompare      = N_VCompare_SensWrapper;
  ops->nvinvtest      = N_VInvTest_SensWrapper;
  ops->nvconstrmask   = N_VConstrMask_SensWrapper;
  ops->nvminquotient  = N_VMinQuotient_SensWrapper;

  /* fused vector operations */
  ops->nvlinearcombination = NULL;
  ops->nvscaleaddmulti     = NULL;
  ops->nvdotprodmulti      = NULL;

  /* vector array operations */
  ops->nvlinearsumvectorarray         = NULL;
  ops->nvscalevectorarray             = NULL;
  ops->nvconstvectorarray             = NULL;
  ops->nvwrmsnormvectorarray          = NULL;
  ops->nvwrmsnormmaskvectorarray      = NULL;
  ops->nvscaleaddmultivectorarray     = NULL;
  ops->nvlinearcombinationvectorarray = NULL;

  /* create content */
  content = NULL;
  content = (N_VectorContent_SensWrapper) malloc(sizeof *content);
  if (content == NULL) {free(ops); free(v); return(NULL);}

  content->nvecs    = nvecs;
  content->own_vecs = SUNFALSE;
  content->vecs     = NULL;
  content->vecs     = (N_Vector*) malloc(nvecs * sizeof(N_Vector));
  if (content->vecs == NULL) {free(ops); free(v); free(content); return(NULL);}

  /* initialize vector array to null */
  for (i=0; i < nvecs; i++)
    content->vecs[i] = NULL;

  /* attach content and ops */
  v->content = content;
  v->ops     = ops;

  return(v);
}


N_Vector N_VNew_SensWrapper(int count, N_Vector w)
{
  N_Vector v;
  int i;

  v = NULL;
  v = N_VNewEmpty_SensWrapper(count);
  if (v == NULL) return(NULL);

  for (i=0; i < NV_NVECS_SW(v); i++) {
    NV_VEC_SW(v,i) = N_VClone(w);
    if (NV_VEC_SW(v,i) == NULL) { N_VDestroy(v); return(NULL); }
  }

  /* update own vectors status */
  NV_OWN_VECS_SW(v) = SUNTRUE;

  return(v);
}


/*==============================================================================
  Clone operations
  ============================================================================*/

/*------------------------------------------------------------------------------
  create an empty clone of the vector wrapper w
  ----------------------------------------------------------------------------*/
N_Vector N_VCloneEmpty_SensWrapper(N_Vector w)
{
  int i;
  N_Vector v;
  N_Vector_Ops ops;
  N_VectorContent_SensWrapper content;

  if (w == NULL) return(NULL);

  if (NV_NVECS_SW(w) < 1) return(NULL);

  /* create vector */
  v = NULL;
  v = (N_Vector) malloc(sizeof *v);
  if (v == NULL) return(NULL);

  /* create vector operation structure */
  ops = NULL;
  ops = (N_Vector_Ops) malloc(sizeof *ops);
  if (ops == NULL) { free(v); return(NULL); }

  ops->nvgetvectorid     = w->ops->nvgetvectorid;
  ops->nvclone           = w->ops->nvclone;
  ops->nvcloneempty      = w->ops->nvcloneempty;
  ops->nvdestroy         = w->ops->nvdestroy;
  ops->nvspace           = w->ops->nvspace;
  ops->nvgetarraypointer = w->ops->nvgetarraypointer;
  ops->nvsetarraypointer = w->ops->nvsetarraypointer;

  /* standard vector operations */
  ops->nvlinearsum    = w->ops->nvlinearsum;
  ops->nvconst        = w->ops->nvconst;
  ops->nvprod         = w->ops->nvprod;
  ops->nvdiv          = w->ops->nvdiv;
  ops->nvscale        = w->ops->nvscale;
  ops->nvabs          = w->ops->nvabs;
  ops->nvinv          = w->ops->nvinv;
  ops->nvaddconst     = w->ops->nvaddconst;
  ops->nvdotprod      = w->ops->nvdotprod;
  ops->nvmaxnorm      = w->ops->nvmaxnorm;
  ops->nvwrmsnormmask = w->ops->nvwrmsnormmask;
  ops->nvwrmsnorm     = w->ops->nvwrmsnorm;
  ops->nvmin          = w->ops->nvmin;
  ops->nvwl2norm      = w->ops->nvwl2norm;
  ops->nvl1norm       = w->ops->nvl1norm;
  ops->nvcompare      = w->ops->nvcompare;
  ops->nvinvtest      = w->ops->nvinvtest;
  ops->nvconstrmask   = w->ops->nvconstrmask;
  ops->nvminquotient  = w->ops->nvminquotient;

  /* fused vector operations */
  ops->nvlinearcombination = w->ops->nvlinearcombination;
  ops->nvscaleaddmulti     = w->ops->nvscaleaddmulti;
  ops->nvdotprodmulti      = w->ops->nvdotprodmulti;

  /* vector array operations */
  ops->nvlinearsumvectorarray         = w->ops->nvlinearsumvectorarray;
  ops->nvscalevectorarray             = w->ops->nvscalevectorarray;
  ops->nvconstvectorarray             = w->ops->nvconstvectorarray;
  ops->nvwrmsnormvectorarray          = w->ops->nvwrmsnormvectorarray;
  ops->nvwrmsnormmaskvectorarray      = w->ops->nvwrmsnormmaskvectorarray;
  ops->nvscaleaddmultivectorarray     = w->ops->nvscaleaddmultivectorarray;
  ops->nvlinearcombinationvectorarray = w->ops->nvlinearcombinationvectorarray;

  /* Create content */
  content = NULL;
  content = (N_VectorContent_SensWrapper) malloc(sizeof *content);
  if (content == NULL) { free(ops); free(v); return(NULL); }

  content->nvecs    = NV_NVECS_SW(w);
  content->own_vecs = SUNFALSE;
  content->vecs     = NULL;
  content->vecs     = (N_Vector*) malloc(NV_NVECS_SW(w) * sizeof(N_Vector));
  if (content->vecs == NULL) {free(ops); free(v); free(content); return(NULL);}

  /* initialize vector array to null */
  for (i=0; i < NV_NVECS_SW(w); i++)
    content->vecs[i] = NULL;

  /* Attach content and ops */
  v->content = content;
  v->ops     = ops;

  return(v);
}


/*------------------------------------------------------------------------------
  create a clone of the vector wrapper w
  ----------------------------------------------------------------------------*/
N_Vector N_VClone_SensWrapper(N_Vector w)
{
  N_Vector v;
  int i;

  /* create empty wrapper */
  v = NULL;
  v = N_VCloneEmpty_SensWrapper(w);
  if (v == NULL) return(NULL);

  /* update own vectors status */
  NV_OWN_VECS_SW(v) = SUNTRUE;

  /* allocate arrays */
  for (i=0; i < NV_NVECS_SW(v); i++) {
    NV_VEC_SW(v,i) = N_VClone(NV_VEC_SW(w,i));
    if (NV_VEC_SW(v,i) == NULL) { N_VDestroy(v); return(NULL); }
  }

  return(v);
}


/*==============================================================================
  Destructor
  ============================================================================*/

void N_VDestroy_SensWrapper(N_Vector v)
{
  int i;

  if (NV_OWN_VECS_SW(v) == SUNTRUE) {
    for (i=0; i < NV_NVECS_SW(v); i++) {
      if (NV_VEC_SW(v,i)) N_VDestroy(NV_VEC_SW(v,i));
      NV_VEC_SW(v,i) = NULL;
    }
  }

  free(NV_VECS_SW(v)); NV_VECS_SW(v) = NULL;
  free(v->content); v->content = NULL;
  free(v->ops); v->ops = NULL;
  free(v); v = NULL;

  return;
}


/*==============================================================================
  Standard vector operations
  ============================================================================*/

void N_VLinearSum_SensWrapper(realtype a, N_Vector x, realtype b, N_Vector y, N_Vector z)
{
  int i;

  for (i=0; i < NV_NVECS_SW(x); i++)
    N_VLinearSum(a, NV_VEC_SW(x,i), b, NV_VEC_SW(y,i), NV_VEC_SW(z,i));

  return;
}


void N_VConst_SensWrapper(realtype c, N_Vector z)
{
  int i;

  for (i=0; i < NV_NVECS_SW(z); i++)
    N_VConst(c, NV_VEC_SW(z,i));

  return;
}


void N_VProd_SensWrapper(N_Vector x, N_Vector y, N_Vector z)
{
  int i;

  for (i=0; i < NV_NVECS_SW(x); i++)
    N_VProd(NV_VEC_SW(x,i), NV_VEC_SW(y,i), NV_VEC_SW(z,i));

  return;
}


void N_VDiv_SensWrapper(N_Vector x, N_Vector y, N_Vector z)
{
  int i;

  for (i=0; i < NV_NVECS_SW(x); i++)
    N_VDiv(NV_VEC_SW(x,i), NV_VEC_SW(y,i), NV_VEC_SW(z,i));

  return;
}


void N_VScale_SensWrapper(realtype c, N_Vector x, N_Vector z)
{
  int i;

  for (i=0; i < NV_NVECS_SW(x); i++)
    N_VScale(c, NV_VEC_SW(x,i), NV_VEC_SW(z,i));

  return;
}


void N_VAbs_SensWrapper(N_Vector x, N_Vector z)
{
  int i;

  for (i=0; i < NV_NVECS_SW(x); i++)
    N_VAbs(NV_VEC_SW(x,i), NV_VEC_SW(z,i));

  return;
}


void N_VInv_SensWrapper(N_Vector x, N_Vector z)
{
  int i;

  for (i=0; i < NV_NVECS_SW(x); i++)
    N_VInv(NV_VEC_SW(x,i), NV_VEC_SW(z,i));

  return;
}


void N_VAddConst_SensWrapper(N_Vector x, realtype b, N_Vector z)
{
  int i;

  for (i=0; i < NV_NVECS_SW(x); i++)
    N_VAddConst(NV_VEC_SW(x,i), b, NV_VEC_SW(z,i));

  return;
}


realtype N_VDotProd_SensWrapper(N_Vector x, N_Vector y)
{
  int i;
  realtype sum;

  sum = ZERO;

  for (i=0; i < NV_NVECS_SW(x); i++)
    sum += N_VDotProd(NV_VEC_SW(x,i), NV_VEC_SW(y,i));

  return(sum);
}


realtype N_VMaxNorm_SensWrapper(N_Vector x)
{
  int i;
  realtype max, tmp;

  max = ZERO;

  for (i=0; i < NV_NVECS_SW(x); i++) {
    tmp = N_VMaxNorm(NV_VEC_SW(x,i));
    if (tmp > max) max = tmp;
  }

  return(max);
}


realtype N_VWrmsNorm_SensWrapper(N_Vector x, N_Vector w)
{
  int i;
  realtype nrm, tmp;

  nrm = ZERO;

  for (i=0; i < NV_NVECS_SW(x); i++) {
    tmp = N_VWrmsNorm(NV_VEC_SW(x,i), NV_VEC_SW(w,i));
    if (tmp > nrm) nrm = tmp;
  }

  return(nrm);
}


realtype N_VWrmsNormMask_SensWrapper(N_Vector x, N_Vector w, N_Vector id)
{
  int i;
  realtype nrm, tmp;

  nrm = ZERO;

  for (i=0; i < NV_NVECS_SW(x); i++) {
    tmp = N_VWrmsNormMask(NV_VEC_SW(x,i), NV_VEC_SW(w,i), NV_VEC_SW(id,i));
    if (tmp > nrm) nrm = tmp;
  }

  return(nrm);
}


realtype N_VMin_SensWrapper(N_Vector x)
{
  int i;
  realtype min, tmp;

  min = N_VMin(NV_VEC_SW(x,0));

  for (i=1; i < NV_NVECS_SW(x); i++) {
    tmp = N_VMin(NV_VEC_SW(x,i));
    if (tmp < min) min = tmp;
  }

  return(min);
}


realtype N_VWL2Norm_SensWrapper(N_Vector x, N_Vector w)
{
  int i;
  realtype nrm, tmp;

  nrm = ZERO;

  for (i=0; i < NV_NVECS_SW(x); i++) {
    tmp = N_VWL2Norm(NV_VEC_SW(x,i), NV_VEC_SW(w,i));
    if (tmp > nrm) nrm = tmp;
  }

  return(nrm);
}


realtype N_VL1Norm_SensWrapper(N_Vector x)
{
  int i;
  realtype nrm, tmp;

  nrm = ZERO;

  for (i=0; i < NV_NVECS_SW(x); i++) {
    tmp = N_VL1Norm(NV_VEC_SW(x,i));
    if (tmp > nrm) nrm = tmp;
  }

  return(nrm);
}


void N_VCompare_SensWrapper(realtype c, N_Vector x, N_Vector z)
{
  int i;

  for (i=0; i < NV_NVECS_SW(x); i++)
    N_VCompare(c, NV_VEC_SW(x,i), NV_VEC_SW(z,i));

  return;
}


booleantype N_VInvTest_SensWrapper(N_Vector x, N_Vector z)
{
  int i;
  booleantype no_zero_found, tmp;

  no_zero_found = SUNTRUE;

  for (i=0; i < NV_NVECS_SW(x); i++) {
    tmp = N_VInvTest(NV_VEC_SW(x,i), NV_VEC_SW(z,i));
    if (tmp != SUNTRUE) no_zero_found = SUNFALSE;
  }

  return(no_zero_found);
}


booleantype N_VConstrMask_SensWrapper(N_Vector c, N_Vector x, N_Vector m)
{
  int i;
  booleantype test, tmp;

  test = SUNTRUE;

  for (i=0; i < NV_NVECS_SW(x); i++) {
    tmp = N_VConstrMask(c, NV_VEC_SW(x,i), NV_VEC_SW(m,i));
    if (tmp != SUNTRUE) test = SUNFALSE;
  }

  return(test);
}


realtype N_VMinQuotient_SensWrapper(N_Vector num, N_Vector denom)
{
  int i;
  realtype min, tmp;

  min = N_VMinQuotient(NV_VEC_SW(num,0), NV_VEC_SW(denom,0));

  for (i=1; i < NV_NVECS_SW(num); i++) {
    tmp = N_VMinQuotient(NV_VEC_SW(num,i), NV_VEC_SW(denom,i));
    if (tmp < min) min = tmp;
  }

  return(min);
}
