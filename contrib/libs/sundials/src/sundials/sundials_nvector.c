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
 * This is the implementation file for a generic NVECTOR package.
 * It contains the implementation of the N_Vector operations listed
 * in nvector.h.
 * -----------------------------------------------------------------*/

#include <stdlib.h>

#include <sundials/sundials_nvector.h>

/*
 * -----------------------------------------------------------------
 * Functions in the 'ops' structure
 * -----------------------------------------------------------------
 */

N_Vector_ID N_VGetVectorID(N_Vector w)
{
  N_Vector_ID id;
  id = w->ops->nvgetvectorid(w);
  return(id);
}

N_Vector N_VClone(N_Vector w)
{
  N_Vector v = NULL;
  v = w->ops->nvclone(w);
  return(v);
}

N_Vector N_VCloneEmpty(N_Vector w)
{
  N_Vector v = NULL;
  v = w->ops->nvcloneempty(w);
  return(v);
}

void N_VDestroy(N_Vector v)
{
  if (v==NULL) return;
  v->ops->nvdestroy(v);
  return;
}

void N_VSpace(N_Vector v, sunindextype *lrw, sunindextype *liw)
{
  v->ops->nvspace(v, lrw, liw);
  return;
}

realtype *N_VGetArrayPointer(N_Vector v)
{
  return((realtype *) v->ops->nvgetarraypointer(v));
}

void N_VSetArrayPointer(realtype *v_data, N_Vector v)
{
  v->ops->nvsetarraypointer(v_data, v);
  return;
}

/* -----------------------------------------------------------------
 * standard vector operations
 * ----------------------------------------------------------------- */

void N_VLinearSum(realtype a, N_Vector x, realtype b, N_Vector y, N_Vector z)
{
  z->ops->nvlinearsum(a, x, b, y, z);
  return;
}

void N_VConst(realtype c, N_Vector z)
{
  z->ops->nvconst(c, z);
  return;
}

void N_VProd(N_Vector x, N_Vector y, N_Vector z)
{
  z->ops->nvprod(x, y, z);
  return;
}

void N_VDiv(N_Vector x, N_Vector y, N_Vector z)
{
  z->ops->nvdiv(x, y, z);
  return;
}

void N_VScale(realtype c, N_Vector x, N_Vector z) 
{
  z->ops->nvscale(c, x, z);
  return;
}

void N_VAbs(N_Vector x, N_Vector z)
{
  z->ops->nvabs(x, z);
  return;
}

void N_VInv(N_Vector x, N_Vector z)
{
  z->ops->nvinv(x, z);
  return;
}

void N_VAddConst(N_Vector x, realtype b, N_Vector z)
{
  z->ops->nvaddconst(x, b, z);
  return;
}

realtype N_VDotProd(N_Vector x, N_Vector y)
{
  return((realtype) y->ops->nvdotprod(x, y));
}

realtype N_VMaxNorm(N_Vector x)
{
  return((realtype) x->ops->nvmaxnorm(x));
}

realtype N_VWrmsNorm(N_Vector x, N_Vector w)
{
  return((realtype) x->ops->nvwrmsnorm(x, w));
}

realtype N_VWrmsNormMask(N_Vector x, N_Vector w, N_Vector id)
{
  return((realtype) x->ops->nvwrmsnormmask(x, w, id));
}

realtype N_VMin(N_Vector x)
{
  return((realtype) x->ops->nvmin(x));
}

realtype N_VWL2Norm(N_Vector x, N_Vector w)
{
  return((realtype) x->ops->nvwl2norm(x, w));
}

realtype N_VL1Norm(N_Vector x)
{
  return((realtype) x->ops->nvl1norm(x));
}

void N_VCompare(realtype c, N_Vector x, N_Vector z)
{
  z->ops->nvcompare(c, x, z);
  return;
}

booleantype N_VInvTest(N_Vector x, N_Vector z)
{
  return((booleantype) z->ops->nvinvtest(x, z));
}

booleantype N_VConstrMask(N_Vector c, N_Vector x, N_Vector m)
{
  return((booleantype) x->ops->nvconstrmask(c, x, m));
}

realtype N_VMinQuotient(N_Vector num, N_Vector denom)
{
  return((realtype) num->ops->nvminquotient(num, denom));
}

/* -----------------------------------------------------------------
 * fused vector operations
 * ----------------------------------------------------------------- */

int N_VLinearCombination(int nvec, realtype* c, N_Vector* X, N_Vector z)
{
  int i;
  realtype ONE=RCONST(1.0);

  if (z->ops->nvlinearcombination != NULL) {

    return(z->ops->nvlinearcombination(nvec, c, X, z));

  } else {

    z->ops->nvscale(c[0], X[0], z);
    for (i=1; i<nvec; i++) {
      z->ops->nvlinearsum(c[i], X[i], ONE, z, z);
    }
    return(0);

  }
}

int N_VScaleAddMulti(int nvec, realtype* a, N_Vector x, N_Vector* Y, N_Vector* Z)
{
  int i;
  realtype ONE=RCONST(1.0);

  if (x->ops->nvscaleaddmulti != NULL) {
    
    return(x->ops->nvscaleaddmulti(nvec, a, x, Y, Z));

  } else {

    for (i=0; i<nvec; i++) {
      x->ops->nvlinearsum(a[i], x, ONE, Y[i], Z[i]);
    }
    return(0);

  }
}

int N_VDotProdMulti(int nvec, N_Vector x, N_Vector* Y, realtype* dotprods)
{
  int i;

  if (x->ops->nvdotprodmulti != NULL) {
    
    return(x->ops->nvdotprodmulti(nvec, x, Y, dotprods));

  } else {

    for (i=0; i<nvec; i++) {
      dotprods[i] = x->ops->nvdotprod(x, Y[i]);
    }
    return(0);

  }
}

/* -----------------------------------------------------------------
 * vector array operations
 * ----------------------------------------------------------------- */

int N_VLinearSumVectorArray(int nvec, realtype a, N_Vector* X,
                            realtype b, N_Vector* Y, N_Vector* Z)
{
  int i;

  if (Z[0]->ops->nvlinearsumvectorarray != NULL) {
    
    return(Z[0]->ops->nvlinearsumvectorarray(nvec, a, X, b, Y, Z));

  } else {

    for (i=0; i<nvec; i++) {
      Z[0]->ops->nvlinearsum(a, X[i], b, Y[i], Z[i]);
    }
    return(0);

  }
}

int N_VScaleVectorArray(int nvec, realtype* c, N_Vector* X, N_Vector* Z)
{
  int i;

  if (Z[0]->ops->nvscalevectorarray != NULL) {
    
    return(Z[0]->ops->nvscalevectorarray(nvec, c, X, Z));

  } else {

    for (i=0; i<nvec; i++) {
      Z[0]->ops->nvscale(c[i], X[i], Z[i]);
    }
    return(0);

  }
}

int N_VConstVectorArray(int nvec, realtype c, N_Vector* Z)
{
  int i, ier;

  if (Z[0]->ops->nvconstvectorarray != NULL) {
    
    ier = Z[0]->ops->nvconstvectorarray(nvec, c, Z);
    return(ier);

  } else {

    for (i=0; i<nvec; i++) {
      Z[0]->ops->nvconst(c, Z[i]);
    }
    return(0);

  }
}

int N_VWrmsNormVectorArray(int nvec, N_Vector* X, N_Vector* W, realtype* nrm)
{
  int i, ier;

  if (X[0]->ops->nvwrmsnormvectorarray != NULL) {
    
    ier = X[0]->ops->nvwrmsnormvectorarray(nvec, X, W, nrm);
    return(ier);

  } else {

    for (i=0; i<nvec; i++) {
      nrm[i] = X[0]->ops->nvwrmsnorm(X[i], W[i]);
    }
    return(0);

  }
}

int N_VWrmsNormMaskVectorArray(int nvec, N_Vector* X, N_Vector* W, N_Vector id,
                               realtype* nrm)
{
  int i, ier;

  if (id->ops->nvwrmsnormmaskvectorarray != NULL) {

    ier = id->ops->nvwrmsnormmaskvectorarray(nvec, X, W, id, nrm);
    return(ier);

  } else {

    for (i=0; i<nvec; i++) {
      nrm[i] = id->ops->nvwrmsnormmask(X[i], W[i], id);
    }
    return(0);

  }
}

int N_VScaleAddMultiVectorArray(int nvec, int nsum, realtype* a, N_Vector* X,
                                 N_Vector** Y, N_Vector** Z)
{
  int       i, j;
  int       ier=0;
  realtype  ONE=RCONST(1.0);
  N_Vector* YY=NULL;
  N_Vector* ZZ=NULL;

  if (X[0]->ops->nvscaleaddmultivectorarray != NULL) {

    ier = X[0]->ops->nvscaleaddmultivectorarray(nvec, nsum, a, X, Y, Z);
    return(ier);

  } else if (X[0]->ops->nvscaleaddmulti != NULL ) {

    /* allocate arrays of vectors */
    YY = (N_Vector *) malloc(nsum * sizeof(N_Vector));
    ZZ = (N_Vector *) malloc(nsum * sizeof(N_Vector));

    for (i=0; i<nvec; i++) {

      for (j=0; j<nsum; j++) {
        YY[j] = Y[j][i];
        ZZ[j] = Z[j][i];
      }

      ier = X[0]->ops->nvscaleaddmulti(nsum, a, X[i], YY, ZZ);
      if (ier != 0) break;
    }

    /* free array of vectors */
    free(YY);
    free(ZZ);

    return(ier);

  } else {

    for (i=0; i<nvec; i++) {
      for (j=0; j<nsum; j++) {
        X[0]->ops->nvlinearsum(a[j], X[i], ONE, Y[j][i], Z[j][i]);
      }
    }
    return(0);
  }
}

int N_VLinearCombinationVectorArray(int nvec, int nsum, realtype* c, N_Vector** X,
                                    N_Vector* Z)
{
  int       i, j;
  int       ier=0;
  realtype  ONE=RCONST(1.0);
  N_Vector* Y=NULL;

  if (Z[0]->ops->nvlinearcombinationvectorarray != NULL) {
    
    ier = Z[0]->ops->nvlinearcombinationvectorarray(nvec, nsum, c, X, Z);
    return(ier);

  } else if (Z[0]->ops->nvlinearcombination != NULL ) {
    
    /* allocate array of vectors */
    Y = (N_Vector *) malloc(nsum * sizeof(N_Vector));

    for (i=0; i<nvec; i++) {

      for (j=0; j<nsum; j++) {
        Y[j] = X[j][i];
      }

      ier = Z[0]->ops->nvlinearcombination(nsum, c, Y, Z[i]);
      if (ier != 0) break;
    }
    
    /* free array of vectors */
    free(Y);

    return(ier);

  } else {

    for (i=0; i<nvec; i++) {
      Z[0]->ops->nvscale(c[0], X[0][i], Z[i]);
      for (j=1; j<nsum; j++) {
        Z[0]->ops->nvlinearsum(c[j], X[j][i], ONE, Z[i], Z[i]);
      }
    }
    return(0);
  }
}

/*
 * -----------------------------------------------------------------
 * Additional functions exported by the generic NVECTOR:
 *   N_VCloneEmptyVectorArray
 *   N_VCloneVectorArray
 *   N_VDestroyVectorArray
 * -----------------------------------------------------------------
 */

N_Vector *N_VCloneEmptyVectorArray(int count, N_Vector w)
{
  N_Vector *vs = NULL;
  int j;

  if (count <= 0) return(NULL);

  vs = (N_Vector *) malloc(count * sizeof(N_Vector));
  if(vs == NULL) return(NULL);

  for (j = 0; j < count; j++) {
    vs[j] = N_VCloneEmpty(w);
    if (vs[j] == NULL) {
      N_VDestroyVectorArray(vs, j-1);
      return(NULL);
    }
  }

  return(vs);
}

N_Vector *N_VCloneVectorArray(int count, N_Vector w)
{
  N_Vector *vs = NULL;
  int j;

  if (count <= 0) return(NULL);

  vs = (N_Vector *) malloc(count * sizeof(N_Vector));
  if(vs == NULL) return(NULL);

  for (j = 0; j < count; j++) {
    vs[j] = N_VClone(w);
    if (vs[j] == NULL) {
      N_VDestroyVectorArray(vs, j-1);
      return(NULL);
    }
  }

  return(vs);
}

void N_VDestroyVectorArray(N_Vector *vs, int count)
{
  int j;

  if (vs==NULL) return;

  for (j = 0; j < count; j++) N_VDestroy(vs[j]);

  free(vs); vs = NULL;

  return;
}
