/* -----------------------------------------------------------------------------
 * Programmer(s): Daniel R. Reynolds @ SMU
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
 * This is the implementation file for the SUNNonlinearSolver module
 * implementation of the Anderson-accelerated Fixed-Point method.
 * ---------------------------------------------------------------------------*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <sunnonlinsol/sunnonlinsol_fixedpoint.h>
#include <sundials/sundials_math.h>
#include <sundials/sundials_nvector_senswrapper.h>

/* Internal utility routines */
static int AndersonAccelerate(SUNNonlinearSolver NLS, N_Vector gval, N_Vector x,
                              N_Vector xold, int iter);

static int AllocateContent(SUNNonlinearSolver NLS, N_Vector tmpl);
static void FreeContent(SUNNonlinearSolver NLS);

/* Content structure accessibility macros */
#define FP_CONTENT(S)  ( (SUNNonlinearSolverContent_FixedPoint)(S->content) )

/* Constant macros */
#define ONE  RCONST(1.0)
#define ZERO RCONST(0.0)

/*==============================================================================
  Constructor to create a new fixed point solver
  ============================================================================*/

SUNNonlinearSolver SUNNonlinSol_FixedPoint(N_Vector y, int m)
{
  SUNNonlinearSolver NLS;
  SUNNonlinearSolver_Ops ops;
  SUNNonlinearSolverContent_FixedPoint content;
  int retval;

  /* Check that the supplied N_Vector is non-NULL */
  if (y == NULL) return(NULL);

  /* Check that the supplied N_Vector supports all required operations */
  if ( (y->ops->nvclone     == NULL) ||
       (y->ops->nvdestroy   == NULL) ||
       (y->ops->nvscale     == NULL) ||
       (y->ops->nvlinearsum == NULL) ||
       (y->ops->nvdotprod   == NULL) )
    return(NULL);

  /* Create nonlinear linear solver */
  NLS = NULL;
  NLS = (SUNNonlinearSolver) malloc(sizeof *NLS);
  if (NLS == NULL) return(NULL);

  /* Create nonlinear solver operations structure */
  ops = NULL;
  ops = (SUNNonlinearSolver_Ops) malloc(sizeof *ops);
  if (ops == NULL) { free(NLS); return(NULL); }

  /* Create nonlinear solver content structure */
  content = NULL;
  content = (SUNNonlinearSolverContent_FixedPoint) malloc(sizeof *content);
  if (content == NULL) { free(ops); free(NLS); return(NULL); }

  /* Attach content and ops */
  NLS->content = content;
  NLS->ops     = ops;

  /* Attach operations */
  ops->gettype         = SUNNonlinSolGetType_FixedPoint;
  ops->initialize      = SUNNonlinSolInitialize_FixedPoint;
  ops->setup           = NULL;  /* no setup needed */
  ops->solve           = SUNNonlinSolSolve_FixedPoint;
  ops->free            = SUNNonlinSolFree_FixedPoint;
  ops->setsysfn        = SUNNonlinSolSetSysFn_FixedPoint;
  ops->setlsetupfn     = NULL;  /* no lsetup needed */
  ops->setlsolvefn     = NULL;  /* no lsolve needed */
  ops->setctestfn      = SUNNonlinSolSetConvTestFn_FixedPoint;
  ops->setmaxiters     = SUNNonlinSolSetMaxIters_FixedPoint;
  ops->getnumiters     = SUNNonlinSolGetNumIters_FixedPoint;
  ops->getcuriter      = SUNNonlinSolGetCurIter_FixedPoint;
  ops->getnumconvfails = SUNNonlinSolGetNumConvFails_FixedPoint;

  /* Initialize all components of content to 0/NULL */
  memset(content, 0, sizeof(struct _SUNNonlinearSolverContent_FixedPoint));

  /* Fill general content */
  content->Sys        = NULL;
  content->CTest      = NULL;
  content->m          = m;
  content->curiter    = 0;
  content->maxiters   = 3;
  content->niters     = 0;
  content->nconvfails = 0;

  /* Fill allocatable content */
  retval = AllocateContent(NLS, y);

  if (retval != SUN_NLS_SUCCESS) {
    NLS->content = NULL;
    NLS->ops = NULL;
    free(content);
    free(ops);
    free(NLS);
    return(NULL);
  }

  return(NLS);
}


/*==============================================================================
  Constructor wrapper to create a new fixed point solver for sensitivity solvers
  ============================================================================*/

SUNNonlinearSolver SUNNonlinSol_FixedPointSens(int count, N_Vector y, int m)
{
  SUNNonlinearSolver NLS;
  N_Vector w;

  /* create sensitivity vector wrapper */
  w = N_VNew_SensWrapper(count, y);

  /* create nonlinear solver using sensitivity vector wrapper */
  NLS = SUNNonlinSol_FixedPoint(w, m);

  /* free sensitivity vector wrapper */
  N_VDestroy(w);

  /* return NLS object */
  return(NLS);
}


/*==============================================================================
  GetType, Initialize, Setup, Solve, and Free operations
  ============================================================================*/

SUNNonlinearSolver_Type SUNNonlinSolGetType_FixedPoint(SUNNonlinearSolver NLS)
{
  return(SUNNONLINEARSOLVER_FIXEDPOINT);
}


int SUNNonlinSolInitialize_FixedPoint(SUNNonlinearSolver NLS)
{
  /* check that the nonlinear solver is non-null */
  if (NLS == NULL) return(SUN_NLS_MEM_NULL);

  /* check that all required function pointers have been set */
  if ( (FP_CONTENT(NLS)->Sys == NULL) || (FP_CONTENT(NLS)->CTest == NULL) )
    return(SUN_NLS_MEM_NULL);

  /* reset the total number of iterations and convergence failures */
  FP_CONTENT(NLS)->niters     = 0;
  FP_CONTENT(NLS)->nconvfails = 0;

  return(SUN_NLS_SUCCESS);
}


/*-----------------------------------------------------------------------------
  SUNNonlinSolSolve_FixedPoint: Performs the fixed-point solve g(y) = y

  Successful solve return code:
   SUN_NLS_SUCCESS = 0

  Recoverable failure return codes (positive):
    SUN_NLS_CONV_RECVR
    *_RHSFUNC_RECVR (ODEs) or *_RES_RECVR (DAEs)

  Unrecoverable failure return codes (negative):
    *_MEM_NULL
    *_RHSFUNC_FAIL (ODEs) or *_RES_FAIL (DAEs)

  Note that return values beginning with * are package specific values returned
  by the Sys function provided to the nonlinear solver.
  ---------------------------------------------------------------------------*/
int SUNNonlinSolSolve_FixedPoint(SUNNonlinearSolver NLS, N_Vector y0,
                                 N_Vector y, N_Vector w, realtype tol,
                                 booleantype callSetup, void* mem)
{
  /* local variables */
  int retval;
  N_Vector yprev, gy, delta;

  /* check that the inputs are non-null */
  if ( (NLS == NULL) || (y0 == NULL) || (y == NULL) || (w == NULL) || (mem == NULL) )
    return(SUN_NLS_MEM_NULL);

  /* set local shortcut variables */
  yprev = FP_CONTENT(NLS)->yprev;
  gy    = FP_CONTENT(NLS)->gy;
  delta = FP_CONTENT(NLS)->delta;

  /* load prediction into y */
  N_VScale(ONE, y0, y);

  /* Looping point for attempts at solution of the nonlinear system:
       Evaluate fixed-point function (store in gy).
       Performs the accelerated fixed-point iteration.
       Performs stopping tests. */
  for( FP_CONTENT(NLS)->curiter = 0;
       FP_CONTENT(NLS)->curiter < FP_CONTENT(NLS)->maxiters;
       FP_CONTENT(NLS)->curiter++ ) {

    /* update previous solution guess */
    N_VScale(ONE, y, yprev);

    /* compute fixed-point iteration function, store in gy */
    retval = FP_CONTENT(NLS)->Sys(y, gy, mem);
    if (retval != SUN_NLS_SUCCESS) break;

    /* perform fixed point update, based on choice of acceleration or not */
    if (FP_CONTENT(NLS)->m == 0) {    /* basic fixed-point solver */
      N_VScale(ONE, gy, y);
    } else {                          /* Anderson-accelerated solver */
      retval = AndersonAccelerate(NLS, gy, y, yprev, FP_CONTENT(NLS)->curiter);
    }

    /* increment nonlinear solver iteration counter */
    FP_CONTENT(NLS)->niters++;

    /* compute change in solution, and call the convergence test function */
    N_VLinearSum(ONE, y, -ONE, yprev, delta);

    /* test for convergence */
    retval = FP_CONTENT(NLS)->CTest(NLS, y, delta, tol, w, mem);

    /* return if successful */
    if (retval == SUN_NLS_SUCCESS)  return(SUN_NLS_SUCCESS);

    /* check if the iterations should continue; otherwise increment the
       convergence failure count and return error flag */
    if (retval != SUN_NLS_CONTINUE) {
      FP_CONTENT(NLS)->nconvfails++;
      return(retval);
    }

  }

  /* if we've reached this point, then we exhausted the iteration limit;
     increment the convergence failure count and return */
  FP_CONTENT(NLS)->nconvfails++;
  return(SUN_NLS_CONV_RECVR);
}


int SUNNonlinSolFree_FixedPoint(SUNNonlinearSolver NLS)
{
  /* return if NLS is already free */
  if (NLS == NULL)  return(SUN_NLS_SUCCESS);

  /* free items from content structure, then the structure itself */
  if (NLS->content) {
    FreeContent(NLS);
    free(NLS->content);
    NLS->content = NULL;
  }

  /* free the ops structure */
  if (NLS->ops) {
    free(NLS->ops);
    NLS->ops = NULL;
  }

  /* free the overall NLS structure */
  free(NLS);

  return(SUN_NLS_SUCCESS);
}


/*==============================================================================
  Set functions
  ============================================================================*/

int SUNNonlinSolSetSysFn_FixedPoint(SUNNonlinearSolver NLS, SUNNonlinSolSysFn SysFn)
{
  /* check that the nonlinear solver is non-null */
  if (NLS == NULL)
    return(SUN_NLS_MEM_NULL);

  /* check that the nonlinear system function is non-null */
  if (SysFn == NULL)
    return(SUN_NLS_ILL_INPUT);

  FP_CONTENT(NLS)->Sys = SysFn;
  return(SUN_NLS_SUCCESS);
}

int SUNNonlinSolSetConvTestFn_FixedPoint(SUNNonlinearSolver NLS, SUNNonlinSolConvTestFn CTestFn)
{
  /* check that the nonlinear solver is non-null */
  if (NLS == NULL)
    return(SUN_NLS_MEM_NULL);

  /* check that the convergence test function is non-null */
  if (CTestFn == NULL)
    return(SUN_NLS_ILL_INPUT);

  FP_CONTENT(NLS)->CTest = CTestFn;
  return(SUN_NLS_SUCCESS);
}

int SUNNonlinSolSetMaxIters_FixedPoint(SUNNonlinearSolver NLS, int maxiters)
{
  /* check that the nonlinear solver is non-null */
  if (NLS == NULL)
    return(SUN_NLS_MEM_NULL);

  /* check that maxiters is a vaild */
  if (maxiters < 1)
    return(SUN_NLS_ILL_INPUT);

  FP_CONTENT(NLS)->maxiters = maxiters;
  return(SUN_NLS_SUCCESS);
}


/*==============================================================================
  Get functions
  ============================================================================*/

int SUNNonlinSolGetNumIters_FixedPoint(SUNNonlinearSolver NLS, long int *niters)
{
  /* check that the nonlinear solver is non-null */
  if (NLS == NULL)
    return(SUN_NLS_MEM_NULL);

  /* return the total number of nonlinear iterations */
  *niters = FP_CONTENT(NLS)->niters;
  return(SUN_NLS_SUCCESS);
}


int SUNNonlinSolGetCurIter_FixedPoint(SUNNonlinearSolver NLS, int *iter)
{
  /* check that the nonlinear solver is non-null */
  if (NLS == NULL)
    return(SUN_NLS_MEM_NULL);

  /* return the current nonlinear solver iteration count */
  *iter = FP_CONTENT(NLS)->curiter;
  return(SUN_NLS_SUCCESS);
}


int SUNNonlinSolGetNumConvFails_FixedPoint(SUNNonlinearSolver NLS, long int *nconvfails)
{
  /* check that the nonlinear solver is non-null */
  if (NLS == NULL)
    return(SUN_NLS_MEM_NULL);

  /* return the total number of nonlinear convergence failures */
  *nconvfails = FP_CONTENT(NLS)->nconvfails;
  return(SUN_NLS_SUCCESS);
}


int SUNNonlinSolGetSysFn_FixedPoint(SUNNonlinearSolver NLS, SUNNonlinSolSysFn *SysFn)
{
  /* check that the nonlinear solver is non-null */
  if (NLS == NULL)
    return(SUN_NLS_MEM_NULL);

  /* return the nonlinear system defining function */
  *SysFn = FP_CONTENT(NLS)->Sys;
  return(SUN_NLS_SUCCESS);
}


/*=============================================================================
  Utility routines
  ===========================================================================*/

/*---------------------------------------------------------------
  AndersonAccelerate

  This routine computes the Anderson-accelerated fixed point
  iterate.  Upon entry, the predicted solution is held in xold;
  this array is never changed throughout this routine.

  The result of the routine is held in x.

  Possible return values:
    SUN_NLS_MEM_NULL --> a required item was missing from memory
    SUN_NLS_SUCCESS  --> successful completion
  -------------------------------------------------------------*/
static int AndersonAccelerate(SUNNonlinearSolver NLS, N_Vector gval,
                              N_Vector x, N_Vector xold, int iter)
{
  /* local variables */
  int       nvec, retval, i_pt, i, j, lAA, maa, *ipt_map;
  realtype  a, b, rtemp, c, s, *cvals, *R, *gamma;
  N_Vector  fv, vtemp, gold, fold, *df, *dg, *Q, *Xvecs;

  /* local shortcut variables */
  vtemp   = x;    /* use result as temporary vector */
  ipt_map = FP_CONTENT(NLS)->imap;
  maa     = FP_CONTENT(NLS)->m;
  gold    = FP_CONTENT(NLS)->gold;
  fold    = FP_CONTENT(NLS)->fold;
  df      = FP_CONTENT(NLS)->df;
  dg      = FP_CONTENT(NLS)->dg;
  Q       = FP_CONTENT(NLS)->q;
  cvals   = FP_CONTENT(NLS)->cvals;
  Xvecs   = FP_CONTENT(NLS)->Xvecs;
  R       = FP_CONTENT(NLS)->R;
  gamma   = FP_CONTENT(NLS)->gamma;
  fv      = FP_CONTENT(NLS)->delta;

  /* reset ipt_map, i_pt */
  for (i = 0; i < maa; i++)  ipt_map[i]=0;
  i_pt = iter-1 - ((iter-1)/maa)*maa;

  /* update dg[i_pt], df[i_pt], fv, gold and fold*/
  N_VLinearSum(ONE, gval, -ONE, xold, fv);
  if (iter > 0) {
    N_VLinearSum(ONE, gval, -ONE, gold, dg[i_pt]);  /* dg_new = gval - gold */
    N_VLinearSum(ONE, fv, -ONE, fold, df[i_pt]);    /* df_new = fv - fold */
  }
  N_VScale(ONE, gval, gold);
  N_VScale(ONE, fv, fold);

  /* on first iteration, just do basic fixed-point update */
  if (iter == 0) {
    N_VScale(ONE, gval, x);
    return(SUN_NLS_SUCCESS);
  }

  /* update data structures based on current iteration index */

  if (iter == 1) {   /* second iteration */

    R[0] = SUNRsqrt( N_VDotProd(df[i_pt], df[i_pt]) );
    N_VScale(ONE/R[0], df[i_pt], Q[i_pt]);
    ipt_map[0] = 0;

  } else if (iter <= maa) {   /* another iteration before we've reached maa */

    N_VScale(ONE, df[i_pt], vtemp);
    for (j = 0; j < iter-1; j++) {
      ipt_map[j] = j;
      R[(iter-1)*maa+j] = N_VDotProd(Q[j], vtemp);
      N_VLinearSum(ONE, vtemp, -R[(iter-1)*maa+j], Q[j], vtemp);
    }
    R[(iter-1)*maa+iter-1] = SUNRsqrt( N_VDotProd(vtemp, vtemp) );
    if (R[(iter-1)*maa+iter-1] == ZERO) {
      N_VScale(ZERO, vtemp, Q[i_pt]);
    } else {
      N_VScale((ONE/R[(iter-1)*maa+iter-1]), vtemp, Q[i_pt]);
    }
    ipt_map[iter-1] = iter-1;

  } else {   /* we've filled the acceleration subspace, so start recycling */

    /* delete left-most column vector from QR factorization */
    for (i = 0; i < maa-1; i++) {
      a = R[(i+1)*maa + i];
      b = R[(i+1)*maa + i+1];
      rtemp = SUNRsqrt(a*a + b*b);
      c = a / rtemp;
      s = b / rtemp;
      R[(i+1)*maa + i] = rtemp;
      R[(i+1)*maa + i+1] = 0.0;
      if (i < maa-1) {
        for (j = i+2; j < maa; j++) {
          a = R[j*maa + i];
          b = R[j*maa + i+1];
          rtemp = c * a + s * b;
          R[j*maa + i+1] = -s*a + c*b;
          R[j*maa + i] = rtemp;
        }
      }
      N_VLinearSum(c, Q[i], s, Q[i+1], vtemp);
      N_VLinearSum(-s, Q[i], c, Q[i+1], Q[i+1]);
      N_VScale(ONE, vtemp, Q[i]);
    }

    /* ahift R to the left by one */
    for (i = 1; i < maa; i++)
      for (j = 0; j < maa-1; j++)
        R[(i-1)*maa + j] = R[i*maa + j];

    /* add the new df vector */
    N_VScale(ONE, df[i_pt], vtemp);
    for (j = 0; j < maa-1; j++) {
      R[(maa-1)*maa+j] = N_VDotProd(Q[j], vtemp);
      N_VLinearSum(ONE, vtemp, -R[(maa-1)*maa+j], Q[j], vtemp);
    }
    R[(maa-1)*maa+maa-1] = SUNRsqrt( N_VDotProd(vtemp, vtemp) );
    N_VScale((ONE/R[(maa-1)*maa+maa-1]), vtemp, Q[maa-1]);

    /* update the iteration map */
    j = 0;
    for (i = i_pt+1; i < maa; i++)
      ipt_map[j++] = i;
    for (i = 0; i < i_pt+1; i++)
      ipt_map[j++] = i;
  }

  /* solve least squares problem and update solution */
  lAA = iter;
  if (maa < iter)  lAA = maa;
  retval = N_VDotProdMulti(lAA, fv, Q, gamma);
  if (retval != 0)  return(SUN_NLS_VECTOROP_ERR);

  /* set arrays for fused vector operation */
  cvals[0] = ONE;
  Xvecs[0] = gval;
  nvec = 1;
  for (i = lAA-1; i > -1; i--) {
    for (j = i+1; j < lAA; j++)
      gamma[i] -= R[j*maa+i]*gamma[j];
    if (gamma[i] == ZERO) {
      gamma[i] = ZERO;
    } else {
      gamma[i] /= R[i*maa+i];
    }
    cvals[nvec] = -gamma[i];
    Xvecs[nvec] = dg[ipt_map[i]];
    nvec += 1;
  }

  /* update solution */
  retval = N_VLinearCombination(nvec, cvals, Xvecs, x);
  if (retval != 0)  return(SUN_NLS_VECTOROP_ERR);

  return(SUN_NLS_SUCCESS);
}

static int AllocateContent(SUNNonlinearSolver NLS, N_Vector y)
{
  int m = FP_CONTENT(NLS)->m;

  FP_CONTENT(NLS)->yprev = N_VClone(y);
  if (FP_CONTENT(NLS)->yprev == NULL) { FreeContent(NLS); return(SUN_NLS_MEM_FAIL); }

  FP_CONTENT(NLS)->gy = N_VClone(y);
  if (FP_CONTENT(NLS)->gy == NULL) { FreeContent(NLS); return(SUN_NLS_MEM_FAIL); }

  FP_CONTENT(NLS)->delta = N_VClone(y);
  if (FP_CONTENT(NLS)->delta == NULL) { FreeContent(NLS); return(SUN_NLS_MEM_FAIL); }

  /* Allocate all m-dependent content */
  if (m > 0) {

    FP_CONTENT(NLS)->fold = N_VClone(y);
    if (FP_CONTENT(NLS)->fold == NULL) {
      FreeContent(NLS); return(SUN_NLS_MEM_FAIL); }

    FP_CONTENT(NLS)->gold = N_VClone(y);
    if (FP_CONTENT(NLS)->gold == NULL) {
      FreeContent(NLS); return(SUN_NLS_MEM_FAIL); }

    FP_CONTENT(NLS)->imap = (int *) malloc(m * sizeof(int));
    if (FP_CONTENT(NLS)->imap == NULL) {
      FreeContent(NLS); return(SUN_NLS_MEM_FAIL); }

    FP_CONTENT(NLS)->R = (realtype *) malloc((m*m) * sizeof(realtype));
    if (FP_CONTENT(NLS)->R == NULL) {
      FreeContent(NLS); return(SUN_NLS_MEM_FAIL); }

    FP_CONTENT(NLS)->gamma = (realtype *) malloc(m * sizeof(realtype));
    if (FP_CONTENT(NLS)->gamma == NULL) {
      FreeContent(NLS); return(SUN_NLS_MEM_FAIL); }

    FP_CONTENT(NLS)->cvals = (realtype *) malloc((m+1) * sizeof(realtype));
    if (FP_CONTENT(NLS)->cvals == NULL) {
      FreeContent(NLS); return(SUN_NLS_MEM_FAIL); }

    FP_CONTENT(NLS)->df = N_VCloneVectorArray(m, y);
    if (FP_CONTENT(NLS)->df == NULL) {
      FreeContent(NLS); return(SUN_NLS_MEM_FAIL); }

    FP_CONTENT(NLS)->dg = N_VCloneVectorArray(m, y);
    if (FP_CONTENT(NLS)->dg == NULL) {
      FreeContent(NLS); return(SUN_NLS_MEM_FAIL); }

    FP_CONTENT(NLS)->q = N_VCloneVectorArray(m, y);
    if (FP_CONTENT(NLS)->q == NULL) {
      FreeContent(NLS); return(SUN_NLS_MEM_FAIL); }

    FP_CONTENT(NLS)->Xvecs = (N_Vector *) malloc((m+1) * sizeof(N_Vector));
    if (FP_CONTENT(NLS)->Xvecs == NULL) {
      FreeContent(NLS); return(SUN_NLS_MEM_FAIL); }
  }

  return(SUN_NLS_SUCCESS);
}

static void FreeContent(SUNNonlinearSolver NLS)
{
  if (FP_CONTENT(NLS)->yprev) {
    N_VDestroy(FP_CONTENT(NLS)->yprev);
    FP_CONTENT(NLS)->yprev = NULL; }

  if (FP_CONTENT(NLS)->gy) {
    N_VDestroy(FP_CONTENT(NLS)->gy);
    FP_CONTENT(NLS)->gy = NULL; }

  if (FP_CONTENT(NLS)->fold) {
    N_VDestroy(FP_CONTENT(NLS)->fold);
    FP_CONTENT(NLS)->fold = NULL; }

  if (FP_CONTENT(NLS)->gold) {
    N_VDestroy(FP_CONTENT(NLS)->gold);
    FP_CONTENT(NLS)->gold = NULL; }

  if (FP_CONTENT(NLS)->delta) {
    N_VDestroy(FP_CONTENT(NLS)->delta);
    FP_CONTENT(NLS)->delta = NULL; }

  if (FP_CONTENT(NLS)->imap) {
    free(FP_CONTENT(NLS)->imap);
    FP_CONTENT(NLS)->imap = NULL; }

  if (FP_CONTENT(NLS)->R) {
    free(FP_CONTENT(NLS)->R);
    FP_CONTENT(NLS)->R = NULL; }

  if (FP_CONTENT(NLS)->gamma) {
    free(FP_CONTENT(NLS)->gamma);
    FP_CONTENT(NLS)->gamma = NULL; }

  if (FP_CONTENT(NLS)->cvals) {
    free(FP_CONTENT(NLS)->cvals);
    FP_CONTENT(NLS)->cvals = NULL; }

  if (FP_CONTENT(NLS)->df) {
    N_VDestroyVectorArray(FP_CONTENT(NLS)->df, FP_CONTENT(NLS)->m);
    FP_CONTENT(NLS)->df = NULL; }

  if (FP_CONTENT(NLS)->dg) {
    N_VDestroyVectorArray(FP_CONTENT(NLS)->dg, FP_CONTENT(NLS)->m);
    FP_CONTENT(NLS)->dg = NULL; }

  if (FP_CONTENT(NLS)->q) {
    N_VDestroyVectorArray(FP_CONTENT(NLS)->q, FP_CONTENT(NLS)->m);
    FP_CONTENT(NLS)->q = NULL; }

  if (FP_CONTENT(NLS)->Xvecs) {
    free(FP_CONTENT(NLS)->Xvecs);
    FP_CONTENT(NLS)->Xvecs = NULL; }

  return;
}
