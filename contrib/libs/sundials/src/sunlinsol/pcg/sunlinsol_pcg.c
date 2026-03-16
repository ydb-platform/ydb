/*
 * -----------------------------------------------------------------
 * Programmer(s): Daniel Reynolds, Ashley Crawford @ SMU
 * Based on sundials_pcg.c code, written by Daniel Reynolds @ SMU
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
 * This is the implementation file for the PCG implementation of 
 * the SUNLINSOL package.
 * -----------------------------------------------------------------
 */ 

#include <stdio.h>
#include <stdlib.h>

#include <sunlinsol/sunlinsol_pcg.h>
#include <sundials/sundials_math.h>

#define ZERO RCONST(0.0)
#define ONE  RCONST(1.0)

/*
 * -----------------------------------------------------------------
 * PCG solver structure accessibility macros: 
 * -----------------------------------------------------------------
 */

#define PCG_CONTENT(S)  ( (SUNLinearSolverContent_PCG)(S->content) )
#define PRETYPE(S)      ( PCG_CONTENT(S)->pretype )
#define LASTFLAG(S)     ( PCG_CONTENT(S)->last_flag )

/*
 * -----------------------------------------------------------------
 * deprecated wrapper functions
 * -----------------------------------------------------------------
 */

SUNLinearSolver SUNPCG(N_Vector y, int pretype, int maxl)
{ return(SUNLinSol_PCG(y, pretype, maxl)); }

int SUNPCGSetPrecType(SUNLinearSolver S, int pretype)
{ return(SUNLinSol_PCGSetPrecType(S, pretype)); }

int SUNPCGSetMaxl(SUNLinearSolver S, int maxl)
{ return(SUNLinSol_PCGSetMaxl(S, maxl)); }

/*
 * -----------------------------------------------------------------
 * exported functions
 * -----------------------------------------------------------------
 */

/* ----------------------------------------------------------------------------
 * Function to create a new PCG linear solver
 */

SUNLinearSolver SUNLinSol_PCG(N_Vector y, int pretype, int maxl)
{
  SUNLinearSolver S;
  SUNLinearSolver_Ops ops;
  SUNLinearSolverContent_PCG content;
  
  /* check for legal pretype and maxl values; if illegal use defaults */
  if ((pretype != PREC_NONE)  && (pretype != PREC_LEFT) &&
      (pretype != PREC_RIGHT) && (pretype != PREC_BOTH))
    pretype = PREC_NONE;
  if (maxl <= 0)
    maxl = SUNPCG_MAXL_DEFAULT;

  /* Create linear solver */
  S = NULL;
  S = (SUNLinearSolver) malloc(sizeof *S);
  if (S == NULL) return(NULL);
  
  /* Create linear solver operation structure */
  ops = NULL;
  ops = (SUNLinearSolver_Ops) malloc(sizeof(struct _generic_SUNLinearSolver_Ops));
  if (ops == NULL) { free(S); return(NULL); }

  /* Attach operations */
  ops->gettype           = SUNLinSolGetType_PCG;
  ops->setatimes         = SUNLinSolSetATimes_PCG;
  ops->setpreconditioner = SUNLinSolSetPreconditioner_PCG;
  ops->setscalingvectors = SUNLinSolSetScalingVectors_PCG;
  ops->initialize        = SUNLinSolInitialize_PCG;
  ops->setup             = SUNLinSolSetup_PCG;
  ops->solve             = SUNLinSolSolve_PCG;
  ops->numiters          = SUNLinSolNumIters_PCG;
  ops->resnorm           = SUNLinSolResNorm_PCG;
  ops->resid             = SUNLinSolResid_PCG;
  ops->lastflag          = SUNLinSolLastFlag_PCG;  
  ops->space             = SUNLinSolSpace_PCG;  
  ops->free              = SUNLinSolFree_PCG;

  /* Create content */
  content = NULL;
  content = (SUNLinearSolverContent_PCG) malloc(sizeof(struct _SUNLinearSolverContent_PCG));
  if (content == NULL) { free(ops); free(S); return(NULL); }

  /* Fill content */
  content->last_flag = 0;
  content->maxl = maxl;
  content->pretype = pretype;
  content->numiters = 0;
  content->resnorm = ZERO;
  content->r = N_VClone(y);
  if (content->r == NULL)  return NULL;
  content->p = N_VClone(y);
  if (content->p == NULL)  return NULL;
  content->z = N_VClone(y);
  if (content->z == NULL)  return NULL;
  content->Ap = N_VClone(y);
  if (content->Ap == NULL)  return NULL;
  content->s = NULL;
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
 * Function to set the type of preconditioning for PCG to use 
 */

SUNDIALS_EXPORT int SUNLinSol_PCGSetPrecType(SUNLinearSolver S, int pretype) 
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
 * Function to set the maximum number of iterations for PCG to use 
 */

SUNDIALS_EXPORT int SUNLinSol_PCGSetMaxl(SUNLinearSolver S, int maxl) 
{
  /* Check for non-NULL SUNLinearSolver */
  if (S == NULL) return(SUNLS_MEM_NULL);

  /* Check for legal pretype */ 
  if (maxl <= 0)
    maxl = SUNPCG_MAXL_DEFAULT;

  /* Set pretype */
  PCG_CONTENT(S)->maxl = maxl;
  return(SUNLS_SUCCESS);
}


/*
 * -----------------------------------------------------------------
 * implementation of linear solver operations
 * -----------------------------------------------------------------
 */

SUNLinearSolver_Type SUNLinSolGetType_PCG(SUNLinearSolver S)
{
  return(SUNLINEARSOLVER_ITERATIVE);
}

int SUNLinSolInitialize_PCG(SUNLinearSolver S)
{
  /* ensure valid options */
  if (S == NULL) return(SUNLS_MEM_NULL);  
  if ( (PRETYPE(S) != PREC_LEFT) && 
       (PRETYPE(S) != PREC_RIGHT) && 
       (PRETYPE(S) != PREC_BOTH) )
    PRETYPE(S) = PREC_NONE;
  if (PCG_CONTENT(S)->maxl <= 0) 
    PCG_CONTENT(S)->maxl = SUNPCG_MAXL_DEFAULT;

  /* no additional memory to allocate */

  /* return with success */
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetATimes_PCG(SUNLinearSolver S, void* ATData, 
                           ATimesFn ATimes)
{
  /* set function pointers to integrator-supplied ATimes routine
     and data, and return with success */
  if (S == NULL) return(SUNLS_MEM_NULL);
  PCG_CONTENT(S)->ATimes = ATimes;
  PCG_CONTENT(S)->ATData = ATData;
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetPreconditioner_PCG(SUNLinearSolver S, void* PData,
                                   PSetupFn Psetup, PSolveFn Psolve)
{
  /* set function pointers to integrator-supplied Psetup and PSolve
     routines and data, and return with success */
  if (S == NULL) return(SUNLS_MEM_NULL);
  PCG_CONTENT(S)->Psetup = Psetup;
  PCG_CONTENT(S)->Psolve = Psolve;
  PCG_CONTENT(S)->PData = PData;
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetScalingVectors_PCG(SUNLinearSolver S, N_Vector s,
                                   N_Vector nul)
{
  /* set N_Vector pointer to integrator-supplied scaling vector
     (only use the first one), and return with success */
  if (S == NULL) return(SUNLS_MEM_NULL);
  PCG_CONTENT(S)->s = s;
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetup_PCG(SUNLinearSolver S, SUNMatrix nul)
{
  int ier;
  PSetupFn Psetup;
  void* PData;

  /* Set shortcuts to PCG memory structures */
  if (S == NULL) return(SUNLS_MEM_NULL);
  Psetup = PCG_CONTENT(S)->Psetup;
  PData = PCG_CONTENT(S)->PData;
  
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


int SUNLinSolSolve_PCG(SUNLinearSolver S, SUNMatrix nul, N_Vector x, 
                       N_Vector b, realtype delta)
{
  /* local data and shortcut variables */
  realtype alpha, beta, r0_norm, rho, rz, rz_old;
  N_Vector r, p, z, Ap, w;
  booleantype UsePrec, UseScaling, converged;
  int l, l_max, pretype, ier;
  void *A_data, *P_data;
  ATimesFn atimes;
  PSolveFn psolve;
  realtype *res_norm;
  int *nli;

   /* Make local shorcuts to solver variables. */
  if (S == NULL) return(SUNLS_MEM_NULL);
  l_max        = PCG_CONTENT(S)->maxl;
  r            = PCG_CONTENT(S)->r;
  p            = PCG_CONTENT(S)->p;
  z            = PCG_CONTENT(S)->z;
  Ap           = PCG_CONTENT(S)->Ap;
  w            = PCG_CONTENT(S)->s;
  A_data       = PCG_CONTENT(S)->ATData;
  P_data       = PCG_CONTENT(S)->PData;
  atimes       = PCG_CONTENT(S)->ATimes;
  psolve       = PCG_CONTENT(S)->Psolve;
  pretype      = PCG_CONTENT(S)->pretype;
  nli          = &(PCG_CONTENT(S)->numiters);
  res_norm     = &(PCG_CONTENT(S)->resnorm);

  /* Initialize counters and convergence flag */
  *nli = 0;
  converged = SUNFALSE;

  /* set booleantype flags for internal solver options */
  UsePrec = ( (pretype == PREC_BOTH) || 
              (pretype == PREC_LEFT) || 
              (pretype == PREC_RIGHT) );
  UseScaling = (w != NULL);

  /* Set r to initial residual r_0 = b - A*x_0 */
  if (N_VDotProd(x, x) == ZERO)  N_VScale(ONE, b, r);
  else {
    ier = atimes(A_data, x, r);
    if (ier != 0) {
      LASTFLAG(S) = (ier < 0) ? 
        SUNLS_ATIMES_FAIL_UNREC : SUNLS_ATIMES_FAIL_REC;
      return(LASTFLAG(S));
    }
    N_VLinearSum(ONE, b, -ONE, r, r);
  }

  /* Set rho to scaled L2 norm of r, and return if small */
  if (UseScaling)  N_VProd(r, w, Ap);
  else N_VScale(ONE, r, Ap);
  *res_norm = r0_norm = rho = SUNRsqrt(N_VDotProd(Ap, Ap));
  if (rho <= delta) {
    LASTFLAG(S) = SUNLS_SUCCESS;
    return(LASTFLAG(S));
  }

  /* Apply preconditioner and b-scaling to r = r_0 */
  if (UsePrec) {
    ier = psolve(P_data, r, z, delta, PREC_LEFT);   /* z = P^{-1}r */
    if (ier != 0) {
      LASTFLAG(S) = (ier < 0) ? 
        SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
      return(LASTFLAG(S));
    }
  }
  else N_VScale(ONE, r, z);

  /* Initialize rz to <r,z> */
  rz = N_VDotProd(r, z);

  /* Copy z to p */
  N_VScale(ONE, z, p);

  /* Begin main iteration loop */
  for(l=0; l<l_max; l++) {

    /* increment counter */
    (*nli)++;

    /* Generate Ap = A*p */
    ier = atimes(A_data, p, Ap);
    if (ier != 0) {
      LASTFLAG(S) = (ier < 0) ? 
        SUNLS_ATIMES_FAIL_UNREC : SUNLS_ATIMES_FAIL_REC;
      return(LASTFLAG(S));
    }

    /* Calculate alpha = <r,z> / <Ap,p> */
    alpha = rz / N_VDotProd(Ap, p);

    /* Update x = x + alpha*p */
    N_VLinearSum(ONE, x, alpha, p, x);

    /* Update r = r - alpha*Ap */
    N_VLinearSum(ONE, r, -alpha, Ap, r);

    /* Set rho and check convergence */
    if (UseScaling)  N_VProd(r, w, Ap);
    else N_VScale(ONE, r, Ap);
    *res_norm = rho = SUNRsqrt(N_VDotProd(Ap, Ap));
    if (rho <= delta) {
      converged = SUNTRUE;
      break;
    }

    /* Apply preconditioner:  z = P^{-1}*r */
    if (UsePrec) {
      ier = psolve(P_data, r, z, delta, PREC_LEFT);
      if (ier != 0) {
        LASTFLAG(S) = (ier < 0) ? 
          SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
        return(LASTFLAG(S));
      }
    }
    else N_VScale(ONE, r, z);

    /* update rz */
    rz_old = rz;
    rz = N_VDotProd(r, z);
    
    /* Calculate beta = <r,z> / <r_old,z_old> */
    beta = rz / rz_old;

    /* Update p = z + beta*p */
    N_VLinearSum(ONE, z, beta, p, p);
  }

  /* Main loop finished, return with result */
  if (converged == SUNTRUE) {
    LASTFLAG(S) = SUNLS_SUCCESS;
  } else if (rho < r0_norm) {
    LASTFLAG(S) = SUNLS_RES_REDUCED;
  } else {
    LASTFLAG(S) = SUNLS_CONV_FAIL;
  }
  return(LASTFLAG(S));
}




int SUNLinSolNumIters_PCG(SUNLinearSolver S)
{
  /* return the stored 'numiters' value */
  if (S == NULL) return(-1);
  return (PCG_CONTENT(S)->numiters);
}


realtype SUNLinSolResNorm_PCG(SUNLinearSolver S)
{
  /* return the stored 'resnorm' value */
  if (S == NULL) return(-ONE);
  return (PCG_CONTENT(S)->resnorm);
}


N_Vector SUNLinSolResid_PCG(SUNLinearSolver S)
{
  /* return the stored 'r' vector */
  return (PCG_CONTENT(S)->r);
}


long int SUNLinSolLastFlag_PCG(SUNLinearSolver S)
{
  /* return the stored 'last_flag' value */
  if (S == NULL) return(-1);
  return (LASTFLAG(S));
}


int SUNLinSolSpace_PCG(SUNLinearSolver S, 
                       long int *lenrwLS, 
                       long int *leniwLS)
{
  sunindextype liw1, lrw1;
  N_VSpace(PCG_CONTENT(S)->r, &lrw1, &liw1);
  *lenrwLS = 1 + lrw1*4;
  *leniwLS = 4 + liw1*4;
  return(SUNLS_SUCCESS);
}

int SUNLinSolFree_PCG(SUNLinearSolver S)
{
  if (S == NULL) return(SUNLS_SUCCESS);

  /* delete items from within the content structure */
  if (PCG_CONTENT(S)->r)
    N_VDestroy(PCG_CONTENT(S)->r);
  if (PCG_CONTENT(S)->p)
    N_VDestroy(PCG_CONTENT(S)->p);
  if (PCG_CONTENT(S)->z)
    N_VDestroy(PCG_CONTENT(S)->z);
  if (PCG_CONTENT(S)->Ap)
    N_VDestroy(PCG_CONTENT(S)->Ap);

  /* delete generic structures */
  free(S->content);  S->content = NULL;
  free(S->ops);  S->ops = NULL;
  free(S); S = NULL;
  return 0;
}
