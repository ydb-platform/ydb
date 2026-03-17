/*
 * -----------------------------------------------------------------
 * Programmer(s): Daniel Reynolds @ SMU
 * Based on sundials_spfgmr.c code, written by Daniel R. Reynolds 
 *                and Hilari C. Tiedeman @ SMU
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
 * This is the implementation file for the SPFGMR implementation of 
 * the SUNLINSOL package.
 * -----------------------------------------------------------------
 */ 

#include <stdio.h>
#include <stdlib.h>

#include <sunlinsol/sunlinsol_spfgmr.h>
#include <sundials/sundials_math.h>

#define ZERO RCONST(0.0)
#define ONE  RCONST(1.0)

/*
 * -----------------------------------------------------------------
 * SPFGMR solver structure accessibility macros: 
 * -----------------------------------------------------------------
 */

#define SPFGMR_CONTENT(S)  ( (SUNLinearSolverContent_SPFGMR)(S->content) )
#define LASTFLAG(S)        ( SPFGMR_CONTENT(S)->last_flag )

/*
 * -----------------------------------------------------------------
 * deprecated wrapper functions
 * -----------------------------------------------------------------
 */

SUNLinearSolver SUNSPFGMR(N_Vector y, int pretype, int maxl)
{ return(SUNLinSol_SPFGMR(y, pretype, maxl)); }

int SUNSPFGMRSetPrecType(SUNLinearSolver S, int pretype)
{ return(SUNLinSol_SPFGMRSetPrecType(S, pretype)); }

int SUNSPFGMRSetGSType(SUNLinearSolver S, int gstype)
{ return(SUNLinSol_SPFGMRSetGSType(S, gstype)); }

int SUNSPFGMRSetMaxRestarts(SUNLinearSolver S, int maxrs)
{ return(SUNLinSol_SPFGMRSetMaxRestarts(S, maxrs)); }

/*
 * -----------------------------------------------------------------
 * exported functions
 * -----------------------------------------------------------------
 */

/* ----------------------------------------------------------------------------
 * Function to create a new SPFGMR linear solver
 */

SUNLinearSolver SUNLinSol_SPFGMR(N_Vector y, int pretype, int maxl)
{
  SUNLinearSolver S;
  SUNLinearSolver_Ops ops;
  SUNLinearSolverContent_SPFGMR content;
  
  /* set preconditioning flag (enabling any preconditioner implies right 
     preconditioning, since SPFGMR does not support left preconditioning) */
  pretype = ( (pretype == PREC_LEFT)  ||
              (pretype == PREC_RIGHT) ||
              (pretype == PREC_BOTH) ) ? PREC_RIGHT : PREC_NONE;

  /* if maxl input is illegal, set to default */
  if (maxl <= 0)  maxl = SUNSPFGMR_MAXL_DEFAULT;

  /* check that the supplied N_Vector supports all requisite operations */
  if ( (y->ops->nvclone == NULL) || (y->ops->nvdestroy == NULL) ||
       (y->ops->nvlinearsum == NULL) || (y->ops->nvconst == NULL) ||
       (y->ops->nvprod == NULL) || (y->ops->nvdiv == NULL) ||
       (y->ops->nvscale == NULL) || (y->ops->nvdotprod == NULL) )
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
  ops->gettype           = SUNLinSolGetType_SPFGMR;
  ops->setatimes         = SUNLinSolSetATimes_SPFGMR;
  ops->setpreconditioner = SUNLinSolSetPreconditioner_SPFGMR;
  ops->setscalingvectors = SUNLinSolSetScalingVectors_SPFGMR;
  ops->initialize        = SUNLinSolInitialize_SPFGMR;
  ops->setup             = SUNLinSolSetup_SPFGMR;
  ops->solve             = SUNLinSolSolve_SPFGMR;
  ops->numiters          = SUNLinSolNumIters_SPFGMR;
  ops->resnorm           = SUNLinSolResNorm_SPFGMR;
  ops->resid             = SUNLinSolResid_SPFGMR;
  ops->lastflag          = SUNLinSolLastFlag_SPFGMR;  
  ops->space             = SUNLinSolSpace_SPFGMR;  
  ops->free              = SUNLinSolFree_SPFGMR;

  /* Create content */
  content = NULL;
  content = (SUNLinearSolverContent_SPFGMR) malloc(sizeof(struct _SUNLinearSolverContent_SPFGMR));
  if (content == NULL) { free(ops); free(S); return(NULL); }

  /* Fill content */
  content->last_flag = 0;
  content->maxl = maxl;
  content->pretype = pretype;
  content->gstype = SUNSPFGMR_GSTYPE_DEFAULT;
  content->max_restarts = SUNSPFGMR_MAXRS_DEFAULT;
  content->numiters = 0;
  content->resnorm = ZERO;
  content->xcor = N_VClone(y);
  if (content->xcor == NULL)  return(NULL);
  content->vtemp = N_VClone(y);
  if (content->vtemp == NULL)  return(NULL);
  content->s1 = NULL;
  content->s2 = NULL;
  content->ATimes = NULL;
  content->ATData = NULL;
  content->Psetup = NULL;
  content->Psolve = NULL;
  content->PData = NULL;
  content->V = NULL;
  content->Z = NULL;
  content->Hes = NULL;
  content->givens = NULL;
  content->yg = NULL;
  content->cv = NULL;
  content->Xv = NULL;
    
  /* Attach content and ops */
  S->content = content;
  S->ops     = ops;

  return(S);
}


/* ----------------------------------------------------------------------------
 * Function to toggle preconditioning on/off -- turns on if pretype is any 
 * one of PREC_LEFT, PREC_RIGHT or PREC_BOTH; otherwise turns off
 */

SUNDIALS_EXPORT int SUNLinSol_SPFGMRSetPrecType(SUNLinearSolver S, int pretype) 
{
  /* Check for legal pretype */ 
  pretype = ( (pretype == PREC_LEFT)  ||
              (pretype == PREC_RIGHT) ||
              (pretype == PREC_BOTH) ) ? PREC_RIGHT : PREC_NONE;

  /* Check for non-NULL SUNLinearSolver */
  if (S == NULL) return(SUNLS_MEM_NULL);

  /* Set pretype */
  SPFGMR_CONTENT(S)->pretype = pretype;
  return(SUNLS_SUCCESS);
}


/* ----------------------------------------------------------------------------
 * Function to set the type of Gram-Schmidt orthogonalization for SPFGMR to use
 */

SUNDIALS_EXPORT int SUNLinSol_SPFGMRSetGSType(SUNLinearSolver S, int gstype)
{
  /* Check for legal gstype */ 
  if ((gstype != MODIFIED_GS) && (gstype != CLASSICAL_GS)) {
    return(SUNLS_ILL_INPUT);
  }

  /* Check for non-NULL SUNLinearSolver */
  if (S == NULL) return(SUNLS_MEM_NULL);

  /* Set pretype */
  SPFGMR_CONTENT(S)->gstype = gstype;
  return(SUNLS_SUCCESS);
}


/* ----------------------------------------------------------------------------
 * Function to set the maximum number of FGMRES restarts to allow
 */

SUNDIALS_EXPORT int SUNLinSol_SPFGMRSetMaxRestarts(SUNLinearSolver S, int maxrs)
{
  /* Illegal maxrs implies use of default value */ 
  if (maxrs < 0)
    maxrs = SUNSPFGMR_MAXRS_DEFAULT;

  /* Check for non-NULL SUNLinearSolver */
  if (S == NULL) return(SUNLS_MEM_NULL);

  /* Set max_restarts */
  SPFGMR_CONTENT(S)->max_restarts = maxrs;
  return(SUNLS_SUCCESS);
}


/*
 * -----------------------------------------------------------------
 * implementation of linear solver operations
 * -----------------------------------------------------------------
 */

SUNLinearSolver_Type SUNLinSolGetType_SPFGMR(SUNLinearSolver S)
{
  return(SUNLINEARSOLVER_ITERATIVE);
}


int SUNLinSolInitialize_SPFGMR(SUNLinearSolver S)
{
  int k;
  SUNLinearSolverContent_SPFGMR content;

  /* set shortcut to SPFGMR memory structure */
  if (S == NULL) return(SUNLS_MEM_NULL);  
  content = SPFGMR_CONTENT(S);

  /* ensure valid options */
  if (content->max_restarts < 0) 
    content->max_restarts = SUNSPFGMR_MAXRS_DEFAULT;
  if ( (content->pretype != PREC_LEFT) && 
       (content->pretype != PREC_RIGHT) && 
       (content->pretype != PREC_BOTH) )
    content->pretype = PREC_NONE;


  /* allocate solver-specific memory (where the size depends on the
     choice of maxl) here */

  /*   Krylov subspace vectors */
  if (content->V == NULL) {
    content->V = N_VCloneVectorArray(content->maxl+1, content->vtemp);
    if (content->V == NULL) {
      SUNLinSolFree(S);
      content->last_flag = SUNLS_MEM_FAIL;
      return(SUNLS_MEM_FAIL);
    }
  }

  /*   Preconditioned basis vectors */
  if (content->Z == NULL) {
    content->Z = N_VCloneVectorArray(content->maxl+1, content->vtemp);
    if (content->Z == NULL) {
      SUNLinSolFree(S);
      content->last_flag = SUNLS_MEM_FAIL;
      return(SUNLS_MEM_FAIL);
    }
  }
  
  /*   Hessenberg matrix Hes */
  if (content->Hes == NULL) {
    content->Hes = (realtype **) malloc((content->maxl+1)*sizeof(realtype *));
    if (content->Hes == NULL) {
      SUNLinSolFree(S);
      content->last_flag = SUNLS_MEM_FAIL;
      return(SUNLS_MEM_FAIL);
    }

    for (k=0; k<=content->maxl; k++) {
      content->Hes[k] = NULL;
      content->Hes[k] = (realtype *) malloc(content->maxl*sizeof(realtype));
      if (content->Hes[k] == NULL) {
        SUNLinSolFree(S);
        content->last_flag = SUNLS_MEM_FAIL;
        return(SUNLS_MEM_FAIL);
      }
    }
  }
  
  /*   Givens rotation components */
  if (content->givens == NULL) {
    content->givens = (realtype *) malloc(2*content->maxl*sizeof(realtype));
    if (content->givens == NULL) {
      SUNLinSolFree(S);
      content->last_flag = SUNLS_MEM_FAIL;
      return(SUNLS_MEM_FAIL);
    }
  }
  
  /*    y and g vectors */
  if (content->yg == NULL) {
    content->yg = (realtype *) malloc((content->maxl+1)*sizeof(realtype));
    if (content->yg == NULL) {
      SUNLinSolFree(S);
      content->last_flag = SUNLS_MEM_FAIL;
      return(SUNLS_MEM_FAIL);
    }
  }

  /*    cv vector for fused vector ops */
  if (content->cv == NULL) {
    content->cv = (realtype *) malloc((content->maxl+1)*sizeof(realtype));
    if (content->cv == NULL) {
      SUNLinSolFree(S);
      content->last_flag = SUNLS_MEM_FAIL;
      return(SUNLS_MEM_FAIL);
    }
  }

  /*    Xv vector for fused vector ops */
  if (content->Xv == NULL) {
    content->Xv = (N_Vector *) malloc((content->maxl+1)*sizeof(N_Vector));
    if (content->Xv == NULL) {    
      SUNLinSolFree(S);
      content->last_flag = SUNLS_MEM_FAIL;
      return(SUNLS_MEM_FAIL);
    }
  }

  /* return with success */
  content->last_flag = SUNLS_SUCCESS;
  return(SUNLS_SUCCESS);
}


int SUNLinSolSetATimes_SPFGMR(SUNLinearSolver S, void* ATData, 
                              ATimesFn ATimes)
{
  /* set function pointers to integrator-supplied ATimes routine 
     and data, and return with success */
  if (S == NULL) return(SUNLS_MEM_NULL);
  SPFGMR_CONTENT(S)->ATimes = ATimes;
  SPFGMR_CONTENT(S)->ATData = ATData;
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetPreconditioner_SPFGMR(SUNLinearSolver S, void* PData,
                                    PSetupFn Psetup, PSolveFn Psolve)
{
  /* set function pointers to integrator-supplied Psetup and PSolve
     routines and data, and return with success */
  if (S == NULL) return(SUNLS_MEM_NULL);
  SPFGMR_CONTENT(S)->Psetup = Psetup;
  SPFGMR_CONTENT(S)->Psolve = Psolve;
  SPFGMR_CONTENT(S)->PData = PData;
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetScalingVectors_SPFGMR(SUNLinearSolver S, N_Vector s1,
                                     N_Vector s2)
{
  /* set N_Vector pointers to integrator-supplied scaling vectors, 
     and return with success */
  if (S == NULL) return(SUNLS_MEM_NULL);
  SPFGMR_CONTENT(S)->s1 = s1;
  SPFGMR_CONTENT(S)->s2 = s2;
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetup_SPFGMR(SUNLinearSolver S, SUNMatrix A)
{
  int ier;
  PSetupFn Psetup;
  void* PData;

  /* Set shortcuts to SPFGMR memory structures */
  if (S == NULL) return(SUNLS_MEM_NULL);
  Psetup = SPFGMR_CONTENT(S)->Psetup;
  PData = SPFGMR_CONTENT(S)->PData;
  
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
  return(SUNLS_SUCCESS);
}


int SUNLinSolSolve_SPFGMR(SUNLinearSolver S, SUNMatrix A, N_Vector x, 
                         N_Vector b, realtype delta)
{
  /* local data and shortcut variables */
  N_Vector *V, *Z, xcor, vtemp, s1, s2;
  realtype **Hes, *givens, *yg, *res_norm;
  realtype beta, rotation_product, r_norm, s_product, rho;
  booleantype preOnRight, scale1, scale2, converged;
  int i, j, k, l, l_max, krydim, ier, ntries, max_restarts, gstype;
  int *nli;
  void *A_data, *P_data;
  ATimesFn atimes;
  PSolveFn psolve;

  /* local shortcuts for fused vector operations */
  realtype* cv;
  N_Vector* Xv;

  /* Initialize some variables */
  krydim = 0;

  /* Make local shorcuts to solver variables. */
  if (S == NULL) return(SUNLS_MEM_NULL);
  l_max        = SPFGMR_CONTENT(S)->maxl;
  max_restarts = SPFGMR_CONTENT(S)->max_restarts;
  gstype       = SPFGMR_CONTENT(S)->gstype;
  V            = SPFGMR_CONTENT(S)->V;
  Z            = SPFGMR_CONTENT(S)->Z;
  Hes          = SPFGMR_CONTENT(S)->Hes;
  givens       = SPFGMR_CONTENT(S)->givens;
  xcor         = SPFGMR_CONTENT(S)->xcor;
  yg           = SPFGMR_CONTENT(S)->yg;
  vtemp        = SPFGMR_CONTENT(S)->vtemp;
  s1           = SPFGMR_CONTENT(S)->s1;
  s2           = SPFGMR_CONTENT(S)->s2;
  A_data       = SPFGMR_CONTENT(S)->ATData;
  P_data       = SPFGMR_CONTENT(S)->PData;
  atimes       = SPFGMR_CONTENT(S)->ATimes;
  psolve       = SPFGMR_CONTENT(S)->Psolve;
  nli          = &(SPFGMR_CONTENT(S)->numiters);
  res_norm     = &(SPFGMR_CONTENT(S)->resnorm);
  cv           = SPFGMR_CONTENT(S)->cv;
  Xv           = SPFGMR_CONTENT(S)->Xv;

  /* Initialize counters and convergence flag */
  *nli = 0;
  converged = SUNFALSE;

  /* set booleantype flags for internal solver options */
  preOnRight = ( (SPFGMR_CONTENT(S)->pretype == PREC_LEFT) ||
                 (SPFGMR_CONTENT(S)->pretype == PREC_RIGHT) || 
                 (SPFGMR_CONTENT(S)->pretype == PREC_BOTH) );
  scale1 = (s1 != NULL);
  scale2 = (s2 != NULL);

  /* Set vtemp and V[0] to initial (unscaled) residual r_0 = b - A*x_0 */
  if (N_VDotProd(x, x) == ZERO) {
    N_VScale(ONE, b, vtemp);
  } else {
    ier = atimes(A_data, x, vtemp);
    if (ier != 0) {
      LASTFLAG(S) = (ier < 0) ? 
          SUNLS_ATIMES_FAIL_UNREC : SUNLS_ATIMES_FAIL_REC;
      return(LASTFLAG(S));
    }
    N_VLinearSum(ONE, b, -ONE, vtemp, vtemp);
  }

  /* Apply left scaling to vtemp = r_0 to fill V[0]. */
  if (scale1) {
    N_VProd(s1, vtemp, V[0]);   
  } else {
    N_VScale(ONE, vtemp, V[0]);
  }

  /* Set r_norm = beta to L2 norm of V[0] = s1 r_0, and return if small */
  *res_norm = r_norm = beta = SUNRsqrt(N_VDotProd(V[0], V[0]));
  if (r_norm <= delta) {
    LASTFLAG(S) = SUNLS_SUCCESS;
    return(LASTFLAG(S));
  }

  /* Initialize rho to avoid compiler warning message */
  rho = beta;

  /* Set xcor = 0. */
  N_VConst(ZERO, xcor);

  /* Begin outer iterations: up to (max_restarts + 1) attempts. */
  for (ntries=0; ntries<=max_restarts; ntries++) {
    
    /* Initialize the Hessenberg matrix Hes and Givens rotation
       product.  Normalize the initial vector V[0].             */
    for (i=0; i<=l_max; i++)
      for (j=0; j<l_max; j++)
        Hes[i][j] = ZERO;
    rotation_product = ONE;
    N_VScale(ONE/r_norm, V[0], V[0]);
    
    /* Inner loop: generate Krylov sequence and Arnoldi basis. */
    for (l=0; l<l_max; l++) {
      
      (*nli)++;
      
      krydim = l + 1;
      
      /* Generate A-tilde V[l], where A-tilde = s1 A P_inv s2_inv. */

      /*   Apply right scaling: vtemp = s2_inv V[l]. */
      if (scale2) N_VDiv(V[l], s2, vtemp);
      else N_VScale(ONE, V[l], vtemp);
      
      /*   Apply right preconditioner: vtemp = Z[l] = P_inv s2_inv V[l]. */ 
      if (preOnRight) {
        N_VScale(ONE, vtemp, V[l+1]);
        ier = psolve(P_data, V[l+1], vtemp, delta, PREC_RIGHT);
        if (ier != 0) {
          LASTFLAG(S) = (ier < 0) ?
            SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
          return(LASTFLAG(S));
        }
      }
      N_VScale(ONE, vtemp, Z[l]);
      
      /*   Apply A: V[l+1] = A P_inv s2_inv V[l]. */
      ier = atimes(A_data, vtemp, V[l+1]);
      if (ier != 0) {
        LASTFLAG(S) = (ier < 0) ?
          SUNLS_ATIMES_FAIL_UNREC : SUNLS_ATIMES_FAIL_REC;
        return(LASTFLAG(S));
      }

      /*   Apply left scaling: V[l+1] = s1 A P_inv s2_inv V[l]. */
      if (scale1)  N_VProd(s1, V[l+1], V[l+1]);
      
      /* Orthogonalize V[l+1] against previous V[i]: V[l+1] = w_tilde. */
      if (gstype == CLASSICAL_GS) {
        if (ClassicalGS(V, Hes, l+1, l_max, &(Hes[l+1][l]), cv, Xv) != 0) {
          LASTFLAG(S) = SUNLS_GS_FAIL;
          return(LASTFLAG(S));
        }
      } else {
        if (ModifiedGS(V, Hes, l+1, l_max, &(Hes[l+1][l])) != 0) {
          LASTFLAG(S) = SUNLS_GS_FAIL;
          return(LASTFLAG(S));
        }
      }
      
      /* Update the QR factorization of Hes. */
      if(QRfact(krydim, Hes, givens, l) != 0 ) {
        LASTFLAG(S) = SUNLS_QRFACT_FAIL;
        return(LASTFLAG(S));
      }
      
      /* Update residual norm estimate; break if convergence test passes. */
      rotation_product *= givens[2*l+1];
      *res_norm = rho = SUNRabs(rotation_product*r_norm);
      if (rho <= delta) { converged = SUNTRUE; break; }
      
      /* Normalize V[l+1] with norm value from the Gram-Schmidt routine. */
      N_VScale(ONE/Hes[l+1][l], V[l+1], V[l+1]);
    }
    
    /* Inner loop is done.  Compute the new correction vector xcor. */
    
    /*   Construct g, then solve for y. */
    yg[0] = r_norm;
    for (i=1; i<=krydim; i++)  yg[i]=ZERO;
    if (QRsol(krydim, Hes, givens, yg) != 0) {
      LASTFLAG(S) = SUNLS_QRSOL_FAIL;
      return(LASTFLAG(S));
    }
    
    /*   Add correction vector Z_l y to xcor. */
    cv[0] = ONE;
    Xv[0] = xcor;

    for (k=0; k<krydim; k++) {
      cv[k+1] = yg[k];
      Xv[k+1] = Z[k];
    }
    ier = N_VLinearCombination(krydim+1, cv, Xv, xcor);
    if (ier != SUNLS_SUCCESS) return(SUNLS_VECTOROP_ERR);

    /* If converged, construct the final solution vector x and return. */
    if (converged) {
      N_VLinearSum(ONE, x, ONE, xcor, x); {
        LASTFLAG(S) = SUNLS_SUCCESS;
        return(LASTFLAG(S));
      }
    }
    
    /* Not yet converged; if allowed, prepare for restart. */
    if (ntries == max_restarts) break;
    
    /* Construct last column of Q in yg. */
    s_product = ONE;
    for (i=krydim; i>0; i--) {
      yg[i] = s_product*givens[2*i-2];
      s_product *= givens[2*i-1];
    }
    yg[0] = s_product;
    
    /* Scale r_norm and yg. */
    r_norm *= s_product;
    for (i=0; i<=krydim; i++)
      yg[i] *= r_norm;
    r_norm = SUNRabs(r_norm);
    
    /* Multiply yg by V_(krydim+1) to get last residual vector; restart. */
    for (k=0; k<=krydim; k++) {
      cv[k] = yg[k];
      Xv[k] = V[k];
    }
    ier = N_VLinearCombination(krydim+1, cv, Xv, V[0]);
    if (ier != SUNLS_SUCCESS) return(SUNLS_VECTOROP_ERR);
    
  }
  
  /* Failed to converge, even after allowed restarts.
     If the residual norm was reduced below its initial value, compute
     and return x anyway.  Otherwise return failure flag. */
  if (rho < beta) {
    N_VLinearSum(ONE, x, ONE, xcor, x); {
      LASTFLAG(S) = SUNLS_RES_REDUCED;
      return(LASTFLAG(S));
    }
  }

  LASTFLAG(S) = SUNLS_CONV_FAIL;
  return(LASTFLAG(S)); 
}


int SUNLinSolNumIters_SPFGMR(SUNLinearSolver S)
{
  /* return the stored 'numiters' value */
  if (S == NULL) return(-1);
  return (SPFGMR_CONTENT(S)->numiters);
}


realtype SUNLinSolResNorm_SPFGMR(SUNLinearSolver S)
{
  /* return the stored 'resnorm' value */
  if (S == NULL) return(-ONE);
  return (SPFGMR_CONTENT(S)->resnorm);
}


N_Vector SUNLinSolResid_SPFGMR(SUNLinearSolver S)
{
  /* return the stored 'vtemp' vector */
  return (SPFGMR_CONTENT(S)->vtemp);
}


long int SUNLinSolLastFlag_SPFGMR(SUNLinearSolver S)
{
  /* return the stored 'last_flag' value */
  if (S == NULL) return(-1);
  return (LASTFLAG(S));
}


int SUNLinSolSpace_SPFGMR(SUNLinearSolver S, 
                          long int *lenrwLS, 
                          long int *leniwLS)
{
  int maxl;
  sunindextype liw1, lrw1;
  maxl = SPFGMR_CONTENT(S)->maxl;
  if (SPFGMR_CONTENT(S)->vtemp->ops->nvspace)
    N_VSpace(SPFGMR_CONTENT(S)->vtemp, &lrw1, &liw1);
  else
    lrw1 = liw1 = 0;
  *lenrwLS = lrw1*(2*maxl + 4) + maxl*(maxl + 5) + 2;
  *leniwLS = liw1*(2*maxl + 4);
  return(SUNLS_SUCCESS);
}

int SUNLinSolFree_SPFGMR(SUNLinearSolver S)
{
  int k;

  if (S == NULL) return(SUNLS_SUCCESS);

  /* delete items from within the content structure */
  if (SPFGMR_CONTENT(S)->xcor)
    N_VDestroy(SPFGMR_CONTENT(S)->xcor);
  if (SPFGMR_CONTENT(S)->vtemp)
    N_VDestroy(SPFGMR_CONTENT(S)->vtemp);
  if (SPFGMR_CONTENT(S)->V)
    N_VDestroyVectorArray(SPFGMR_CONTENT(S)->V,
                          SPFGMR_CONTENT(S)->maxl+1);
  if (SPFGMR_CONTENT(S)->Z)
    N_VDestroyVectorArray(SPFGMR_CONTENT(S)->Z,
                          SPFGMR_CONTENT(S)->maxl+1);
  if (SPFGMR_CONTENT(S)->Hes) {
    for (k=0; k<=SPFGMR_CONTENT(S)->maxl; k++)
      if (SPFGMR_CONTENT(S)->Hes[k])
        free(SPFGMR_CONTENT(S)->Hes[k]);
    free(SPFGMR_CONTENT(S)->Hes);
  }
  if (SPFGMR_CONTENT(S)->givens)
    free(SPFGMR_CONTENT(S)->givens);
  if (SPFGMR_CONTENT(S)->yg)
    free(SPFGMR_CONTENT(S)->yg);
  if (SPFGMR_CONTENT(S)->cv)
    free(SPFGMR_CONTENT(S)->cv);
  if (SPFGMR_CONTENT(S)->Xv)
    free(SPFGMR_CONTENT(S)->Xv);

  /* delete generic structures */
  free(S->content);  S->content = NULL;
  free(S->ops);  S->ops = NULL;
  free(S); S = NULL;
  return(SUNLS_SUCCESS);
}
