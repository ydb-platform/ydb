/*
 * -----------------------------------------------------------------
 * Programmer(s): Daniel Reynolds @ SMU
 * Based on sundials_spgmr.c code, written by Scott D. Cohen, 
 *                Alan C. Hindmarsh and Radu Serban @ LLNL
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
 * This is the implementation file for the SPGMR implementation of 
 * the SUNLINSOL package.
 * -----------------------------------------------------------------
 */ 

#include <stdio.h>
#include <stdlib.h>

#include <sunlinsol/sunlinsol_spgmr.h>
#include <sundials/sundials_math.h>

#define ZERO RCONST(0.0)
#define ONE  RCONST(1.0)

/*
 * -----------------------------------------------------------------
 * SPGMR solver structure accessibility macros: 
 * -----------------------------------------------------------------
 */
  
#define SPGMR_CONTENT(S)  ( (SUNLinearSolverContent_SPGMR)(S->content) )
#define LASTFLAG(S)       ( SPGMR_CONTENT(S)->last_flag )

/*
 * -----------------------------------------------------------------
 * deprecated wrapper functions
 * -----------------------------------------------------------------
 */
SUNLinearSolver SUNSPGMR(N_Vector y, int pretype, int maxl)
{ return(SUNLinSol_SPGMR(y, pretype, maxl)); }

int SUNSPGMRSetPrecType(SUNLinearSolver S, int pretype)
{ return(SUNLinSol_SPGMRSetPrecType(S, pretype)); }

int SUNSPGMRSetGSType(SUNLinearSolver S, int gstype)
{ return(SUNLinSol_SPGMRSetGSType(S, gstype)); }

int SUNSPGMRSetMaxRestarts(SUNLinearSolver S, int maxrs)
{ return(SUNLinSol_SPGMRSetMaxRestarts(S, maxrs)); }

/*
 * -----------------------------------------------------------------
 * exported functions
 * -----------------------------------------------------------------
 */

/* ----------------------------------------------------------------------------
 * Function to create a new SPGMR linear solver
 */

SUNLinearSolver SUNLinSol_SPGMR(N_Vector y, int pretype, int maxl)
{
  SUNLinearSolver S;
  SUNLinearSolver_Ops ops;
  SUNLinearSolverContent_SPGMR content;
  
  /* check for legal pretype and maxl values; if illegal use defaults */
  if ((pretype != PREC_NONE)  && (pretype != PREC_LEFT) &&
      (pretype != PREC_RIGHT) && (pretype != PREC_BOTH))
    pretype = PREC_NONE;
  if (maxl <= 0)
    maxl = SUNSPGMR_MAXL_DEFAULT;

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
  ops->gettype           = SUNLinSolGetType_SPGMR;
  ops->setatimes         = SUNLinSolSetATimes_SPGMR;
  ops->setpreconditioner = SUNLinSolSetPreconditioner_SPGMR;
  ops->setscalingvectors = SUNLinSolSetScalingVectors_SPGMR;
  ops->initialize        = SUNLinSolInitialize_SPGMR;
  ops->setup             = SUNLinSolSetup_SPGMR;
  ops->solve             = SUNLinSolSolve_SPGMR;
  ops->numiters          = SUNLinSolNumIters_SPGMR;
  ops->resnorm           = SUNLinSolResNorm_SPGMR;
  ops->resid             = SUNLinSolResid_SPGMR;
  ops->lastflag          = SUNLinSolLastFlag_SPGMR;  
  ops->space             = SUNLinSolSpace_SPGMR;  
  ops->free              = SUNLinSolFree_SPGMR;

  /* Create content */
  content = NULL;
  content = (SUNLinearSolverContent_SPGMR) malloc(sizeof(struct _SUNLinearSolverContent_SPGMR));
  if (content == NULL) { free(ops); free(S); return(NULL); }

  /* Fill content */
  content->last_flag = 0;
  content->maxl = maxl;
  content->pretype = pretype;
  content->gstype = SUNSPGMR_GSTYPE_DEFAULT;
  content->max_restarts = SUNSPGMR_MAXRS_DEFAULT;
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
 * Function to set the type of preconditioning for SPGMR to use 
 */

SUNDIALS_EXPORT int SUNLinSol_SPGMRSetPrecType(SUNLinearSolver S, int pretype) 
{
  /* Check for legal pretype */ 
  if ((pretype != PREC_NONE)  && (pretype != PREC_LEFT) &&
      (pretype != PREC_RIGHT) && (pretype != PREC_BOTH)) {
    return(SUNLS_ILL_INPUT);
  }

  /* Check for non-NULL SUNLinearSolver */
  if (S == NULL) return(SUNLS_MEM_NULL);

  /* Set pretype */
  SPGMR_CONTENT(S)->pretype = pretype;
  return(SUNLS_SUCCESS);
}


/* ----------------------------------------------------------------------------
 * Function to set the type of Gram-Schmidt orthogonalization for SPGMR to use
 */

SUNDIALS_EXPORT int SUNLinSol_SPGMRSetGSType(SUNLinearSolver S, int gstype)
{
  /* Check for legal gstype */ 
  if ((gstype != MODIFIED_GS) && (gstype != CLASSICAL_GS)) {
    return(SUNLS_ILL_INPUT);
  }

  /* Check for non-NULL SUNLinearSolver */
  if (S == NULL) return(SUNLS_MEM_NULL);

  /* Set pretype */
  SPGMR_CONTENT(S)->gstype = gstype;
  return(SUNLS_SUCCESS);
}


/* ----------------------------------------------------------------------------
 * Function to set the maximum number of GMRES restarts to allow
 */

SUNDIALS_EXPORT int SUNLinSol_SPGMRSetMaxRestarts(SUNLinearSolver S, int maxrs)
{
  /* Illegal maxrs implies use of default value */ 
  if (maxrs < 0)
    maxrs = SUNSPGMR_MAXRS_DEFAULT;

  /* Check for non-NULL SUNLinearSolver */
  if (S == NULL) return(SUNLS_MEM_NULL);

  /* Set max_restarts */
  SPGMR_CONTENT(S)->max_restarts = maxrs;
  return(SUNLS_SUCCESS);
}


/*
 * -----------------------------------------------------------------
 * implementation of linear solver operations
 * -----------------------------------------------------------------
 */

SUNLinearSolver_Type SUNLinSolGetType_SPGMR(SUNLinearSolver S)
{
  return(SUNLINEARSOLVER_ITERATIVE);
}


int SUNLinSolInitialize_SPGMR(SUNLinearSolver S)
{
  int k;
  SUNLinearSolverContent_SPGMR content;

  /* set shortcut to SPGMR memory structure */
  if (S == NULL) return(SUNLS_MEM_NULL);  
  content = SPGMR_CONTENT(S);

  /* ensure valid options */
  if (content->max_restarts < 0) 
    content->max_restarts = SUNSPGMR_MAXRS_DEFAULT;
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


int SUNLinSolSetATimes_SPGMR(SUNLinearSolver S, void* ATData, 
                             ATimesFn ATimes)
{
  /* set function pointers to integrator-supplied ATimes routine 
     and data, and return with success */
  if (S == NULL) return(SUNLS_MEM_NULL);
  SPGMR_CONTENT(S)->ATimes = ATimes;
  SPGMR_CONTENT(S)->ATData = ATData;
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetPreconditioner_SPGMR(SUNLinearSolver S, void* PData,
                                    PSetupFn Psetup, PSolveFn Psolve)
{
  /* set function pointers to integrator-supplied Psetup and PSolve
     routines and data, and return with success */
  if (S == NULL) return(SUNLS_MEM_NULL);
  SPGMR_CONTENT(S)->Psetup = Psetup;
  SPGMR_CONTENT(S)->Psolve = Psolve;
  SPGMR_CONTENT(S)->PData = PData;
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetScalingVectors_SPGMR(SUNLinearSolver S, N_Vector s1,
                                     N_Vector s2)
{
  /* set N_Vector pointers to integrator-supplied scaling vectors, 
     and return with success */
  if (S == NULL) return(SUNLS_MEM_NULL);
  SPGMR_CONTENT(S)->s1 = s1;
  SPGMR_CONTENT(S)->s2 = s2;
  LASTFLAG(S) = SUNLS_SUCCESS;
  return(LASTFLAG(S));
}


int SUNLinSolSetup_SPGMR(SUNLinearSolver S, SUNMatrix A)
{
  int ier;
  PSetupFn Psetup;
  void* PData;

  /* Set shortcuts to SPGMR memory structures */
  if (S == NULL) return(SUNLS_MEM_NULL);
  Psetup = SPGMR_CONTENT(S)->Psetup;
  PData = SPGMR_CONTENT(S)->PData;
  
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


int SUNLinSolSolve_SPGMR(SUNLinearSolver S, SUNMatrix A, N_Vector x, 
                         N_Vector b, realtype delta)
{
  /* local data and shortcut variables */
  N_Vector *V, xcor, vtemp, s1, s2;
  realtype **Hes, *givens, *yg, *res_norm;
  realtype beta, rotation_product, r_norm, s_product, rho;
  booleantype preOnLeft, preOnRight, scale2, scale1, converged;
  int i, j, k, l, l_plus_1, l_max, krydim, ier, ntries, max_restarts, gstype;
  int *nli;
  void *A_data, *P_data;
  ATimesFn atimes;
  PSolveFn psolve;

  /* local shortcuts for fused vector operations */
  realtype* cv;
  N_Vector* Xv;

  /* Initialize some variables */
  l_plus_1 = 0;
  krydim = 0;

  /* Make local shorcuts to solver variables. */
  if (S == NULL) return(SUNLS_MEM_NULL);
  l_max        = SPGMR_CONTENT(S)->maxl;
  max_restarts = SPGMR_CONTENT(S)->max_restarts;
  gstype       = SPGMR_CONTENT(S)->gstype;
  V            = SPGMR_CONTENT(S)->V;
  Hes          = SPGMR_CONTENT(S)->Hes;
  givens       = SPGMR_CONTENT(S)->givens;
  xcor         = SPGMR_CONTENT(S)->xcor;
  yg           = SPGMR_CONTENT(S)->yg;
  vtemp        = SPGMR_CONTENT(S)->vtemp;
  s1           = SPGMR_CONTENT(S)->s1;
  s2           = SPGMR_CONTENT(S)->s2;
  A_data       = SPGMR_CONTENT(S)->ATData;
  P_data       = SPGMR_CONTENT(S)->PData;
  atimes       = SPGMR_CONTENT(S)->ATimes;
  psolve       = SPGMR_CONTENT(S)->Psolve;
  nli          = &(SPGMR_CONTENT(S)->numiters);
  res_norm     = &(SPGMR_CONTENT(S)->resnorm);
  cv           = SPGMR_CONTENT(S)->cv;
  Xv           = SPGMR_CONTENT(S)->Xv;

  /* Initialize counters and convergence flag */
  *nli = 0;
  converged = SUNFALSE;

  /* set booleantype flags for internal solver options */
  preOnLeft  = ( (SPGMR_CONTENT(S)->pretype == PREC_LEFT) || 
                 (SPGMR_CONTENT(S)->pretype == PREC_BOTH) );
  preOnRight = ( (SPGMR_CONTENT(S)->pretype == PREC_RIGHT) || 
                 (SPGMR_CONTENT(S)->pretype == PREC_BOTH) );
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
  N_VScale(ONE, vtemp, V[0]);

  /* Apply left preconditioner and left scaling to V[0] = r_0 */
  if (preOnLeft) {
    ier = psolve(P_data, V[0], vtemp, delta, PREC_LEFT);
    if (ier != 0) {
      LASTFLAG(S) = (ier < 0) ? 
          SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
      return(LASTFLAG(S));
    }
  } else {
    N_VScale(ONE, V[0], vtemp);
  }
  
  if (scale1) {
    N_VProd(s1, vtemp, V[0]);   
  } else {
    N_VScale(ONE, vtemp, V[0]);
  }

  /* Set r_norm = beta to L2 norm of V[0] = s1 P1_inv r_0, and
     return if small  */
  *res_norm = r_norm = beta = SUNRsqrt(N_VDotProd(V[0], V[0])); 
  if (r_norm <= delta) {
    LASTFLAG(S) = SUNLS_SUCCESS;
    return(LASTFLAG(S));
  }

  /* Initialize rho to avoid compiler warning message */
  rho = beta;

  /* Set xcor = 0 */
  N_VConst(ZERO, xcor);

  /* Begin outer iterations: up to (max_restarts + 1) attempts */  
  for (ntries=0; ntries<=max_restarts; ntries++) {
    
    /* Initialize the Hessenberg matrix Hes and Givens rotation
       product.  Normalize the initial vector V[0] */
    for (i=0; i<=l_max; i++)
      for (j=0; j<l_max; j++)
        Hes[i][j] = ZERO;

    rotation_product = ONE;    
    N_VScale(ONE/r_norm, V[0], V[0]);
    
    /* Inner loop: generate Krylov sequence and Arnoldi basis */    
    for (l=0; l<l_max; l++) {
      (*nli)++;      
      krydim = l_plus_1 = l + 1;
      
      /* Generate A-tilde V[l], where A-tilde = s1 P1_inv A P2_inv s2_inv */
      
      /*   Apply right scaling: vtemp = s2_inv V[l] */
      if (scale2) N_VDiv(V[l], s2, vtemp);
      else N_VScale(ONE, V[l], vtemp);
      
      /*   Apply right preconditioner: vtemp = P2_inv s2_inv V[l] */ 
      if (preOnRight) {
        N_VScale(ONE, vtemp, V[l_plus_1]);
        ier = psolve(P_data, V[l_plus_1], vtemp, delta, PREC_RIGHT);
        if (ier != 0) {
          LASTFLAG(S) = (ier < 0) ? 
              SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
          return(LASTFLAG(S));
        }
      }
      
      /* Apply A: V[l+1] = A P2_inv s2_inv V[l] */
      ier = atimes( A_data, vtemp, V[l_plus_1] );
      if (ier != 0) {
        LASTFLAG(S) = (ier < 0) ? 
            SUNLS_ATIMES_FAIL_UNREC : SUNLS_ATIMES_FAIL_REC;
        return(LASTFLAG(S));
      }
      
      /* Apply left preconditioning: vtemp = P1_inv A P2_inv s2_inv V[l] */
      if (preOnLeft) {
        ier = psolve(P_data, V[l_plus_1], vtemp, delta, PREC_LEFT);
        if (ier != 0) {
          LASTFLAG(S) = (ier < 0) ? 
              SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
          return(LASTFLAG(S));
        }
      } else {
        N_VScale(ONE, V[l_plus_1], vtemp);
      }
      
      /* Apply left scaling: V[l+1] = s1 P1_inv A P2_inv s2_inv V[l] */
      if (scale1) {
        N_VProd(s1, vtemp, V[l_plus_1]);
      } else {
        N_VScale(ONE, vtemp, V[l_plus_1]);
      }
      
      /*  Orthogonalize V[l+1] against previous V[i]: V[l+1] = w_tilde */
      if (gstype == CLASSICAL_GS) {
        if (ClassicalGS(V, Hes, l_plus_1, l_max, &(Hes[l_plus_1][l]),
                        cv, Xv) != 0) {
          LASTFLAG(S) = SUNLS_GS_FAIL;
          return(LASTFLAG(S));
        }
      } else {
        if (ModifiedGS(V, Hes, l_plus_1, l_max, &(Hes[l_plus_1][l])) != 0) {
          LASTFLAG(S) = SUNLS_GS_FAIL;
          return(LASTFLAG(S));
        }
      }
      
      /*  Update the QR factorization of Hes */
      if(QRfact(krydim, Hes, givens, l) != 0 ) {
        LASTFLAG(S) = SUNLS_QRFACT_FAIL;
        return(LASTFLAG(S));
      }
      
      /*  Update residual norm estimate; break if convergence test passes */
      rotation_product *= givens[2*l+1];
      *res_norm = rho = SUNRabs(rotation_product*r_norm);
      
      if (rho <= delta) { converged = SUNTRUE; break; }
      
      /* Normalize V[l+1] with norm value from the Gram-Schmidt routine */
      N_VScale(ONE/Hes[l_plus_1][l], V[l_plus_1], V[l_plus_1]);
    }
    
    /* Inner loop is done.  Compute the new correction vector xcor */
    
    /*   Construct g, then solve for y */
    yg[0] = r_norm;
    for (i=1; i<=krydim; i++) yg[i]=ZERO;
    if (QRsol(krydim, Hes, givens, yg) != 0) {
      LASTFLAG(S) = SUNLS_QRSOL_FAIL;
      return(LASTFLAG(S));
    }
    
    /*   Add correction vector V_l y to xcor */
    cv[0] = ONE;
    Xv[0] = xcor;

    for (k=0; k<krydim; k++) {
      cv[k+1] = yg[k];
      Xv[k+1] = V[k];
    }
    ier = N_VLinearCombination(krydim+1, cv, Xv, xcor);
    if (ier != SUNLS_SUCCESS) return(SUNLS_VECTOROP_ERR);
    
    /* If converged, construct the final solution vector x and return */
    if (converged) {
      
      /* Apply right scaling and right precond.: vtemp = P2_inv s2_inv xcor */
      if (scale2) N_VDiv(xcor, s2, xcor);
      if (preOnRight) {
        ier = psolve(P_data, xcor, vtemp, delta, PREC_RIGHT);
        if (ier != 0) {
          LASTFLAG(S) = (ier < 0) ? 
              SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
          return(LASTFLAG(S));
        }
      } else {
        N_VScale(ONE, xcor, vtemp);
      }
      
      /* Add vtemp to initial x to get final solution x, and return */
      N_VLinearSum(ONE, x, ONE, vtemp, x);
      
      LASTFLAG(S) = SUNLS_SUCCESS;
      return(LASTFLAG(S));
    }
    
    /* Not yet converged; if allowed, prepare for restart */    
    if (ntries == max_restarts) break;
    
    /* Construct last column of Q in yg */
    s_product = ONE;
    for (i=krydim; i>0; i--) {
      yg[i] = s_product*givens[2*i-2];
      s_product *= givens[2*i-1];
    }
    yg[0] = s_product;
    
    /* Scale r_norm and yg */
    r_norm *= s_product;
    for (i=0; i<=krydim; i++)
      yg[i] *= r_norm;
    r_norm = SUNRabs(r_norm);
    
    /* Multiply yg by V_(krydim+1) to get last residual vector; restart */
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
    
    /* Apply right scaling and right precond.: vtemp = P2_inv s2_inv xcor */    
    if (scale2) N_VDiv(xcor, s2, xcor);
    if (preOnRight) {
      ier = psolve(P_data, xcor, vtemp, delta, PREC_RIGHT);
      if (ier != 0) {
        LASTFLAG(S) = (ier < 0) ? 
            SUNLS_PSOLVE_FAIL_UNREC : SUNLS_PSOLVE_FAIL_REC;
        return(LASTFLAG(S));
      }
      } else {
      N_VScale(ONE, xcor, vtemp);
    }

    /* Add vtemp to initial x to get final solution x, and return */
    N_VLinearSum(ONE, x, ONE, vtemp, x);
    
    LASTFLAG(S) = SUNLS_RES_REDUCED;
    return(LASTFLAG(S));
  }

  LASTFLAG(S) = SUNLS_CONV_FAIL;
  return(LASTFLAG(S));
}


int SUNLinSolNumIters_SPGMR(SUNLinearSolver S)
{
  /* return the stored 'numiters' value */
  if (S == NULL) return(-1);
  return (SPGMR_CONTENT(S)->numiters);
}


realtype SUNLinSolResNorm_SPGMR(SUNLinearSolver S)
{
  /* return the stored 'resnorm' value */
  if (S == NULL) return(-ONE);
  return (SPGMR_CONTENT(S)->resnorm);
}


N_Vector SUNLinSolResid_SPGMR(SUNLinearSolver S)
{
  /* return the stored 'vtemp' vector */
  return (SPGMR_CONTENT(S)->vtemp);
}


long int SUNLinSolLastFlag_SPGMR(SUNLinearSolver S)
{
  /* return the stored 'last_flag' value */
  if (S == NULL) return(-1);
  return (LASTFLAG(S));
}


int SUNLinSolSpace_SPGMR(SUNLinearSolver S, 
                         long int *lenrwLS, 
                         long int *leniwLS)
{
  int maxl;
  sunindextype liw1, lrw1;
  maxl = SPGMR_CONTENT(S)->maxl;
  if (SPGMR_CONTENT(S)->vtemp->ops->nvspace)
    N_VSpace(SPGMR_CONTENT(S)->vtemp, &lrw1, &liw1);
  else
    lrw1 = liw1 = 0;
  *lenrwLS = lrw1*(maxl + 5) + maxl*(maxl + 5) + 2;
  *leniwLS = liw1*(maxl + 5);
  return(SUNLS_SUCCESS);
}


int SUNLinSolFree_SPGMR(SUNLinearSolver S)
{
  int k;

  if (S == NULL) return(SUNLS_SUCCESS);

  /* delete items from within the content structure */
  if (SPGMR_CONTENT(S)->xcor)
    N_VDestroy(SPGMR_CONTENT(S)->xcor);
  if (SPGMR_CONTENT(S)->vtemp)
    N_VDestroy(SPGMR_CONTENT(S)->vtemp);
  if (SPGMR_CONTENT(S)->V)
    N_VDestroyVectorArray(SPGMR_CONTENT(S)->V, 
                          SPGMR_CONTENT(S)->maxl+1);
  if (SPGMR_CONTENT(S)->Hes) {
    for (k=0; k<=SPGMR_CONTENT(S)->maxl; k++)
      if (SPGMR_CONTENT(S)->Hes[k])
        free(SPGMR_CONTENT(S)->Hes[k]);
    free(SPGMR_CONTENT(S)->Hes);
  }
  if (SPGMR_CONTENT(S)->givens)
    free(SPGMR_CONTENT(S)->givens);
  if (SPGMR_CONTENT(S)->yg)
    free(SPGMR_CONTENT(S)->yg);
  if (SPGMR_CONTENT(S)->cv)
    free(SPGMR_CONTENT(S)->cv);
  if (SPGMR_CONTENT(S)->Xv)
    free(SPGMR_CONTENT(S)->Xv);

  /* delete generic structures */
  free(S->content);  S->content = NULL;
  free(S->ops);  S->ops = NULL;
  free(S); S = NULL;
  return(SUNLS_SUCCESS);
}
