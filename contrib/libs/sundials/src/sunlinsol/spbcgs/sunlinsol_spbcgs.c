/*
 * -----------------------------------------------------------------
 * Programmer(s): Daniel Reynolds @ SMU
 * Based on sundials_spbcgs.c code, written by Peter Brown and 
 *                Aaron Collier @ LLNL
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
 * This is the implementation file for the SPBCGS implementation of 
 * the SUNLINSOL package.
 * -----------------------------------------------------------------
 */ 

#include <stdio.h>
#include <stdlib.h>

#include <sunlinsol/sunlinsol_spbcgs.h>
#include <sundials/sundials_math.h>

#define ZERO RCONST(0.0)
#define ONE  RCONST(1.0)

/*
 * -----------------------------------------------------------------
 * SPBCGS solver structure accessibility macros: 
 * -----------------------------------------------------------------
 */

#define SPBCGS_CONTENT(S)  ( (SUNLinearSolverContent_SPBCGS)(S->content) )
#define PRETYPE(S)         ( SPBCGS_CONTENT(S)->pretype )
#define LASTFLAG(S)        ( SPBCGS_CONTENT(S)->last_flag )

/*
 * -----------------------------------------------------------------
 * deprecated wrapper functions
 * -----------------------------------------------------------------
 */

SUNLinearSolver SUNSPBCGS(N_Vector y, int pretype, int maxl)
{ return(SUNLinSol_SPBCGS(y, pretype, maxl)); }

int SUNSPBCGSSetPrecType(SUNLinearSolver S, int pretype)
{ return(SUNLinSol_SPBCGSSetPrecType(S, pretype)); }

int SUNSPBCGSSetMaxl(SUNLinearSolver S, int maxl)
{ return(SUNLinSol_SPBCGSSetMaxl(S, maxl)); }

/*
 * -----------------------------------------------------------------
 * exported functions
 * -----------------------------------------------------------------
 */

/* ----------------------------------------------------------------------------
 * Function to create a new SPBCGS linear solver
 */

SUNLinearSolver SUNLinSol_SPBCGS(N_Vector y, int pretype, int maxl)
{
  SUNLinearSolver S;
  SUNLinearSolver_Ops ops;
  SUNLinearSolverContent_SPBCGS content;
  
  /* check for legal pretype and maxl values; if illegal use defaults */
  if ((pretype != PREC_NONE)  && (pretype != PREC_LEFT) &&
      (pretype != PREC_RIGHT) && (pretype != PREC_BOTH))
    pretype = PREC_NONE;
  if (maxl <= 0)
    maxl = SUNSPBCGS_MAXL_DEFAULT;

  /* check that the supplied N_Vector supports all requisite operations */
  if ( (y->ops->nvclone == NULL) || (y->ops->nvdestroy == NULL) ||
       (y->ops->nvlinearsum == NULL) || (y->ops->nvprod == NULL) ||
       (y->ops->nvdiv == NULL) || (y->ops->nvscale == NULL) ||
       (y->ops->nvdotprod == NULL) )
    return(NULL);

  /* Create linear solver */
  S = NULL;
  S = (SUNLinearSolver) malloc(sizeof *S);
  if (S == NULL) return(NULL);
  
  /* Create linear solver operation structure */
  ops = NULL;
  ops = (SUNLinearSolver_Ops) malloc(sizeof(struct _generic_SUNLinearSolver_Ops));
  if (ops == NULL) { free(S); return(NULL); }

  /* Attach operations */
  ops->gettype           = SUNLinSolGetType_SPBCGS;
  ops->setatimes         = SUNLinSolSetATimes_SPBCGS;
  ops->setpreconditioner = SUNLinSolSetPreconditioner_SPBCGS;
  ops->setscalingvectors = SUNLinSolSetScalingVectors_SPBCGS;
  ops->initialize        = SUNLinSolInitialize_SPBCGS;
  ops->setup             = SUNLinSolSetup_SPBCGS;
  ops->solve             = SUNLinSolSolve_SPBCGS;
  ops->numiters          = SUNLinSolNumIters_SPBCGS;
  ops->resnorm           = SUNLinSolResNorm_SPBCGS;
  ops->resid             = SUNLinSolResid_SPBCGS;
  ops->lastflag          = SUNLinSolLastFlag_SPBCGS;  
  ops->space             = SUNLinSolSpace_SPBCGS;  
  ops->free              = SUNLinSolFree_SPBCGS;

  /* Create content */
  content = NULL;
  content = (SUNLinearSolverContent_SPBCGS) malloc(sizeof(struct _SUNLinearSolverContent_SPBCGS));
  if (content == NULL) { free(ops); free(S); return(NULL); }

  /* Fill content */
  content->last_flag = 0;
  content->maxl = maxl;
  content->pretype = pretype;
  content->numiters = 0;
  content->resnorm = ZERO;
  content->r_star = N_VClone(y);
  if (content->r_star == NULL)  return(NULL);
  content->r = N_VClone(y);
  if (content->r == NULL)  return(NULL);
  content->p = N_VClone(y);
  if (content->p == NULL)  return(NULL);
  content->q = N_VClone(y);
  if (content->q == NULL)  return(NULL);
  content->u = N_VClone(y);
  if (content->u == NULL)  return(NULL);
  content->Ap = N_VClone(y);
  if (content->Ap == NULL)  return(NULL);
  content->vtemp = N_VClone(y);
  if (content->vtemp == NULL)  return(NULL);
  content->s1 = NULL;
  content->s2 = NULL;
  content->ATimes = NULL;
  content->ATData = NULL;
  content->Psetup = NULL;
  content->Psolve = NULL;
  content->PData = NULL;

  /* Attach content and ops */
  S->content = content;
  S->ops     = ops;

  return(S);
}


/* ----------------------------------------------------------------------------
 * Function to set the type of preconditioning for SPBCGS to use 
 */

SUNDIALS_EXPORT int SUNLinSol_SPBCGSSetPrecType(SUNLinearSolver S, int pretype) 
{
  /* Check for legal pretype */ 
  if ((pretype != PREC_NONE)  && (pretype != PREC_LEFT) &&
      (pretype != PREC_RIGHT) && (pretype != PREC_BOTH)) {
    return(SUNLS_ILL_INPUT);
  }

  /* Check for non-NULL SUNLinearSolver */
  if (S == NULL) return(SUNLS_MEM_NULL);

  /* Set pretype */
  PRETYPE(S) = pretype;
  return(SUNLS_SUCCESS);
}


/* ----------------------------------------------------------------------------
 * Function to set the maximum number of iterations for SPBCGS to use 
 */

SUNDIALS_EXPORT int SUNLinSol_SPBCGSSetMaxl(SUNLinearSolver S, int maxl) 
{
  /* Check for non-NULL SUNLinearSolver */
  if (S == NULL) return(SUNLS_MEM_NULL);

  /* Check for legal pretype */ 
  if (maxl <= 0)
    maxl = SUNSPBCGS_MAXL_DEFAULT;

  /* Set pretype */
  SPBCGS_CONTENT(S)->maxl = maxl;
  return(SUNLS_SUCCESS);
}


/*
 * -----------------------------------------------------------------
 * implementation of linear solver operations
 * -----------------------------------------------------------------
 */

SUNLinearSolver_Type SUNLinSolGetType_SPBCGS(SUNLinearSolver S)
{
  return(SUNLINEARSOLVER_ITERATIVE);
}


int SUNLinSolInitialize_SPBCGS(SUNLinearSolver S)
{
  /* ensure valid options */
  if (S == NULL) return(SUNLS_MEM_NULL);  
  if ( (PRETYPE(S) != PREC_LEFT) && 
       (PRETYPE(S) != PREC_RIGHT) && 
       (PRETYPE(S) != PREC_BOTH) )
    PRETYPE(S) = PREC_NONE;
  if (SPBCGS_CONTENT(S)->maxl <= 0) 
    SPBCGS_CONTENT(S)->maxl = SUNSPBCGS_MAXL_DEFAULT;

  /* no additional memory to allocate */
  
  /* return with success */
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetATimes_SPBCGS(SUNLinearSolver S, void* ATData, 
                              ATimesFn ATimes)
{
  /* set function pointers to integrator-supplied ATimes routine
     and data, and return with success */
  if (S == NULL) return(SUNLS_MEM_NULL);
  SPBCGS_CONTENT(S)->ATimes = ATimes;
  SPBCGS_CONTENT(S)->ATData = ATData;
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetPreconditioner_SPBCGS(SUNLinearSolver S, void* PData,
                                      PSetupFn Psetup, PSolveFn Psolve)
{
  /* set function pointers to integrator-supplied Psetup and PSolve
     routines and data, and return with success */
  if (S == NULL) return(SUNLS_MEM_NULL);
  SPBCGS_CONTENT(S)->Psetup = Psetup;
  SPBCGS_CONTENT(S)->Psolve = Psolve;
  SPBCGS_CONTENT(S)->PData = PData;
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetScalingVectors_SPBCGS(SUNLinearSolver S, N_Vector s1,
                                      N_Vector s2)
{
  /* set N_Vector pointers to integrator-supplied scaling vectors, 
     and return with success */
  if (S == NULL) return(SUNLS_MEM_NULL);
  SPBCGS_CONTENT(S)->s1 = s1;
  SPBCGS_CONTENT(S)->s2 = s2;
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetup_SPBCGS(SUNLinearSolver S, SUNMatrix A)
{
  int ier;
  PSetupFn Psetup;
  void* PData;

  /* Set shortcuts to SPBCGS memory structures */
  if (S == NULL) return(SUNLS_MEM_NULL);
  Psetup = SPBCGS_CONTENT(S)->Psetup;
  PData = SPBCGS_CONTENT(S)->PData;
  
  /* no solver-specific setup is required, but if user-supplied 
     Psetup routine exists, call that here */
  if (Psetup != NULL) {
    ier = Psetup(PData);
    if (ier != 0) {
      LASTFLAG(S) = (ier < 0) ? 
	SUNLS_PSET_FAIL_UNREC : SUNLS_PSET_FAIL_REC;
      return(LASTFLAG(S));
    }
  }
  
  /* return with success */ 
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSolve_SPBCGS(SUNLinearSolver S, SUNMatrix A, N_Vector x, 
                          N_Vector b, realtype delta)
{
  /* local data and shortcut variables */
  realtype alpha, beta, omega, omega_denom, beta_num, beta_denom, r_norm, rho;
  N_Vector r_star, r, p, q, u, Ap, vtemp;
  booleantype preOnLeft, preOnRight, scale_x, scale_b, converged;
  int l, l_max, ier;
  void *A_data, *P_data;
  N_Vector sx, sb;
  ATimesFn atimes;
  PSolveFn psolve;
  realtype *res_norm;
  int *nli;

  /* local variables for fused vector operations */
  realtype cv[3];
  N_Vector Xv[3];
  
  /* Make local shorcuts to solver variables. */
  if (S == NULL) return(SUNLS_MEM_NULL);
  l_max        = SPBCGS_CONTENT(S)->maxl;
  r_star       = SPBCGS_CONTENT(S)->r_star;
  r            = SPBCGS_CONTENT(S)->r;
  p            = SPBCGS_CONTENT(S)->p;
  q            = SPBCGS_CONTENT(S)->q;
  u            = SPBCGS_CONTENT(S)->u;
  Ap           = SPBCGS_CONTENT(S)->Ap;
  vtemp        = SPBCGS_CONTENT(S)->vtemp;
  sb           = SPBCGS_CONTENT(S)->s1;
  sx           = SPBCGS_CONTENT(S)->s2;
  A_data       = SPBCGS_CONTENT(S)->ATData;
  P_data       = SPBCGS_CONTENT(S)->PData;
  atimes       = SPBCGS_CONTENT(S)->ATimes;
  psolve       = SPBCGS_CONTENT(S)->Psolve;
  nli          = &(SPBCGS_CONTENT(S)->numiters);
  res_norm     = &(SPBCGS_CONTENT(S)->resnorm);

  /* Initialize counters and convergence flag */
  *nli = 0;
  converged = SUNFALSE;

  /* set booleantype flags for internal solver options */
  preOnLeft  = ( (PRETYPE(S) == PREC_LEFT) || 
                 (PRETYPE(S) == PREC_BOTH) );
  preOnRight = ( (PRETYPE(S) == PREC_RIGHT) || 
                 (PRETYPE(S) == PREC_BOTH) );
  scale_x = (sx != NULL);
  scale_b = (sb != NULL);

  /* Set r_star to initial (unscaled) residual r_0 = b - A*x_0 */

  if (N_VDotProd(x, x) == ZERO) N_VScale(ONE, b, r_star);
  else {
    ier = atimes(A_data, x, r_star);
    if (ier != 0) {
      LASTFLAG(S) = (ier < 0) ?
        SUNLS_ATIMES_FAIL_UNREC : SUNLS_ATIMES_FAIL_REC;
      return(LASTFLAG(S));
    }
    N_VLinearSum(ONE, b, -ONE, r_star, r_star);
  }

  /* Apply left preconditioner and b-scaling to r_star = r_0 */

  if (preOnLeft) {
    ier = psolve(P_data, r_star, r, delta, PREC_LEFT);
    if (ier != 0) {
      LASTFLAG(S) = (ier < 0) ?
        SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
      return(LASTFLAG(S));
    }
  }
  else N_VScale(ONE, r_star, r);

  if (scale_b) N_VProd(sb, r, r_star);
  else N_VScale(ONE, r, r_star);

  /* Initialize beta_denom to the dot product of r0 with r0 */

  beta_denom = N_VDotProd(r_star, r_star);

  /* Set r_norm to L2 norm of r_star = sb P1_inv r_0, and
     return if small */

  *res_norm = r_norm = rho = SUNRsqrt(beta_denom);
  if (r_norm <= delta) {
    LASTFLAG(S) = SUNLS_SUCCESS;
    return(LASTFLAG(S));
  }

  /* Copy r_star to r and p */

  N_VScale(ONE, r_star, r);
  N_VScale(ONE, r_star, p);

  /* Begin main iteration loop */

  for(l = 0; l < l_max; l++) {

    (*nli)++;

    /* Generate Ap = A-tilde p, where A-tilde = sb P1_inv A P2_inv sx_inv */

    /*   Apply x-scaling: vtemp = sx_inv p */

    if (scale_x) N_VDiv(p, sx, vtemp);
    else N_VScale(ONE, p, vtemp);

    /*   Apply right preconditioner: vtemp = P2_inv sx_inv p */

    if (preOnRight) {
      N_VScale(ONE, vtemp, Ap);
      ier = psolve(P_data, Ap, vtemp, delta, PREC_RIGHT);
      if (ier != 0) {
        LASTFLAG(S) = (ier < 0) ?
          SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
        return(LASTFLAG(S));
      }
    }

    /*   Apply A: Ap = A P2_inv sx_inv p */

    ier = atimes(A_data, vtemp, Ap );
    if (ier != 0) {
      LASTFLAG(S) = (ier < 0) ?
        SUNLS_ATIMES_FAIL_UNREC : SUNLS_ATIMES_FAIL_REC;
      return(LASTFLAG(S));
    }

    /*   Apply left preconditioner: vtemp = P1_inv A P2_inv sx_inv p */

    if (preOnLeft) {
      ier = psolve(P_data, Ap, vtemp, delta, PREC_LEFT);
      if (ier != 0) {
        LASTFLAG(S) = (ier < 0) ?
          SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
        return(LASTFLAG(S));
      }
    }
    else N_VScale(ONE, Ap, vtemp);

    /*   Apply b-scaling: Ap = sb P1_inv A P2_inv sx_inv p */

    if (scale_b) N_VProd(sb, vtemp, Ap);
    else N_VScale(ONE, vtemp, Ap);


    /* Calculate alpha = <r,r_star>/<Ap,r_star> */

    alpha = ((beta_denom / N_VDotProd(Ap, r_star)));

    /* Update q = r - alpha*Ap = r - alpha*(sb P1_inv A P2_inv sx_inv p) */

    N_VLinearSum(ONE, r, -alpha, Ap, q);

    /* Generate u = A-tilde q */

    /*   Apply x-scaling: vtemp = sx_inv q */

    if (scale_x) N_VDiv(q, sx, vtemp);
    else N_VScale(ONE, q, vtemp);

    /*   Apply right preconditioner: vtemp = P2_inv sx_inv q */

    if (preOnRight) {
      N_VScale(ONE, vtemp, u);
      ier = psolve(P_data, u, vtemp, delta, PREC_RIGHT);
      if (ier != 0) {
        LASTFLAG(S) = (ier < 0) ?
          SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
        return(LASTFLAG(S));
      }
    }

    /*   Apply A: u = A P2_inv sx_inv u */

    ier = atimes(A_data, vtemp, u );
    if (ier != 0) {
      LASTFLAG(S) = (ier < 0) ?
        SUNLS_ATIMES_FAIL_UNREC : SUNLS_ATIMES_FAIL_REC;
      return(LASTFLAG(S));
    }

    /*   Apply left preconditioner: vtemp = P1_inv A P2_inv sx_inv p */

    if (preOnLeft) {
      ier = psolve(P_data, u, vtemp, delta, PREC_LEFT);
      if (ier != 0) {
        LASTFLAG(S) = (ier < 0) ?
          SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
        return(LASTFLAG(S));
      }
    }
    else N_VScale(ONE, u, vtemp);

    /*   Apply b-scaling: u = sb P1_inv A P2_inv sx_inv u */

    if (scale_b) N_VProd(sb, vtemp, u);
    else N_VScale(ONE, vtemp, u);


    /* Calculate omega = <u,q>/<u,u> */

    omega_denom = N_VDotProd(u, u);
    if (omega_denom == ZERO) omega_denom = ONE;
    omega = (N_VDotProd(u, q) / omega_denom);

    /* Update x = x + alpha*p + omega*q */
    cv[0] = ONE;
    Xv[0] = x;

    cv[1] = alpha;
    Xv[1] = p;

    cv[2] = omega;
    Xv[2] = q;

    ier = N_VLinearCombination(3, cv, Xv, x);
    if (ier != SUNLS_SUCCESS) return(SUNLS_VECTOROP_ERR);

    /* Update the residual r = q - omega*u */

    N_VLinearSum(ONE, q, -omega, u, r);

    /* Set rho = norm(r) and check convergence */

    *res_norm = rho = SUNRsqrt(N_VDotProd(r, r));
    if (rho <= delta) {
      converged = SUNTRUE;
      break;
    }

    /* Not yet converged, continue iteration */
    /* Update beta = <rnew,r_star> / <rold,r_start> * alpha / omega */

    beta_num = N_VDotProd(r, r_star);
    beta = ((beta_num / beta_denom) * (alpha / omega));

    /* Update p = r + beta*(p - omega*Ap) = beta*p - beta*omega*Ap + r */
    cv[0] = beta;
    Xv[0] = p;

    cv[1] = -alpha*(beta_num / beta_denom);
    Xv[1] = Ap;

    cv[2] = ONE;
    Xv[2] = r;

    ier = N_VLinearCombination(3, cv, Xv, p);
    if (ier != SUNLS_SUCCESS) return(SUNLS_VECTOROP_ERR);

    /* udpate beta_denom for next iteration */
    beta_denom = beta_num;
  }

  /* Main loop finished */

  if ((converged == SUNTRUE) || (rho < r_norm)) {

    /* Apply the x-scaling and right preconditioner: x = P2_inv sx_inv x */

    if (scale_x) N_VDiv(x, sx, x);
    if (preOnRight) {
      ier = psolve(P_data, x, vtemp, delta, PREC_RIGHT);
      if (ier != 0) {
        LASTFLAG(S) = (ier < 0) ?
          SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
        return(LASTFLAG(S));
      }
      N_VScale(ONE, vtemp, x);
    }

    if (converged == SUNTRUE) 
      LASTFLAG(S) = SUNLS_SUCCESS;
    else 
      LASTFLAG(S) = SUNLS_RES_REDUCED;
    return(LASTFLAG(S));
    
  }
  else {
    LASTFLAG(S) = SUNLS_CONV_FAIL;
    return(LASTFLAG(S));
  }
}


int SUNLinSolNumIters_SPBCGS(SUNLinearSolver S)
{
  /* return the stored 'numiters' value */
  if (S == NULL) return(-1);
  return (SPBCGS_CONTENT(S)->numiters);
}


realtype SUNLinSolResNorm_SPBCGS(SUNLinearSolver S)
{
  /* return the stored 'resnorm' value */
  if (S == NULL) return(-ONE);
  return (SPBCGS_CONTENT(S)->resnorm);
}


N_Vector SUNLinSolResid_SPBCGS(SUNLinearSolver S)
{
  /* return the stored 'r' vector */
  return (SPBCGS_CONTENT(S)->r);
}


long int SUNLinSolLastFlag_SPBCGS(SUNLinearSolver S)
{
  /* return the stored 'last_flag' value */
  if (S == NULL) return(-1);
  return (LASTFLAG(S));
}


int SUNLinSolSpace_SPBCGS(SUNLinearSolver S, 
                          long int *lenrwLS, 
                          long int *leniwLS)
{
  sunindextype liw1, lrw1;
  if (SPBCGS_CONTENT(S)->vtemp->ops->nvspace)
    N_VSpace(SPBCGS_CONTENT(S)->vtemp, &lrw1, &liw1);
  else
    lrw1 = liw1 = 0;
  *lenrwLS = lrw1*9;
  *leniwLS = liw1*9;
  return(SUNLS_SUCCESS);
}


int SUNLinSolFree_SPBCGS(SUNLinearSolver S)
{
  if (S == NULL) return(SUNLS_SUCCESS);

  /* delete items from within the content structure */
  if (SPBCGS_CONTENT(S)->r_star)
    N_VDestroy(SPBCGS_CONTENT(S)->r_star);
  if (SPBCGS_CONTENT(S)->r)
    N_VDestroy(SPBCGS_CONTENT(S)->r);
  if (SPBCGS_CONTENT(S)->p)
    N_VDestroy(SPBCGS_CONTENT(S)->p);
  if (SPBCGS_CONTENT(S)->q)
    N_VDestroy(SPBCGS_CONTENT(S)->q);
  if (SPBCGS_CONTENT(S)->u)
    N_VDestroy(SPBCGS_CONTENT(S)->u);
  if (SPBCGS_CONTENT(S)->Ap)
    N_VDestroy(SPBCGS_CONTENT(S)->Ap);
  if (SPBCGS_CONTENT(S)->vtemp)
    N_VDestroy(SPBCGS_CONTENT(S)->vtemp);

  /* delete generic structures */
  free(S->content);  S->content = NULL;
  free(S->ops);  S->ops = NULL;
  free(S); S = NULL;
  return(SUNLS_SUCCESS);
}
