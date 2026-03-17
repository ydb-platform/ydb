/*
 * ----------------------------------------------------------------- 
 * Programmer(s): Daniel R. Reynolds @ SMU
 *                Radu Serban and Aaron Collier @ LLNL
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
 * This file contains implementations of the banded difference
 * quotient Jacobian-based preconditioner and solver routines for
 * use with the CVSLS linear solver interface.
 * -----------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>

#include "cvodes_impl.h"
#include "cvodes_bandpre_impl.h"
#include "cvodes_ls_impl.h"
#include <sundials/sundials_math.h>

#define MIN_INC_MULT RCONST(1000.0)
#define ZERO         RCONST(0.0)
#define ONE          RCONST(1.0)
#define TWO          RCONST(2.0)

/* Prototypes of cvBandPrecSetup and cvBandPrecSolve */
static int cvBandPrecSetup(realtype t, N_Vector y, N_Vector fy, 
                           booleantype jok, booleantype *jcurPtr, 
                           realtype gamma, void *bp_data);
static int cvBandPrecSolve(realtype t, N_Vector y, N_Vector fy, 
                           N_Vector r, N_Vector z, 
                           realtype gamma, realtype delta,
                           int lr, void *bp_data);

/* Prototype for cvBandPrecFree */
static int cvBandPrecFree(CVodeMem cv_mem);

/* Prototype for difference quotient Jacobian calculation routine */
static int cvBandPrecDQJac(CVBandPrecData pdata, realtype t, N_Vector y, 
                           N_Vector fy, N_Vector ftemp, N_Vector ytemp);


/*================================================================
  PART I - Forward Problems
  ================================================================*/

/*-----------------------------------------------------------------
  Initialization, Free, and Get Functions
  NOTE: The band linear solver assumes a serial/OpenMP/Pthreads 
        implementation of the NVECTOR package. Therefore, 
        CVBandPrecInit will first test for a compatible N_Vector 
        internal representation by checking that the function 
        N_VGetArrayPointer exists.
  -----------------------------------------------------------------*/
int CVBandPrecInit(void *cvode_mem, sunindextype N,
                   sunindextype mu, sunindextype ml)
{
  CVodeMem cv_mem;
  CVLsMem cvls_mem;
  CVBandPrecData pdata;
  sunindextype mup, mlp, storagemu;
  int flag;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CVLS_MEM_NULL, "CVSBANDPRE",
                   "CVBandPrecInit", MSGBP_MEM_NULL);
    return(CVLS_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Test if the CVSLS linear solver interface has been attached */
  if (cv_mem->cv_lmem == NULL) {
    cvProcessError(cv_mem, CVLS_LMEM_NULL, "CVSBANDPRE",
                   "CVBandPrecInit", MSGBP_LMEM_NULL);
    return(CVLS_LMEM_NULL);
  }
  cvls_mem = (CVLsMem) cv_mem->cv_lmem;

  /* Test compatibility of NVECTOR package with the BAND preconditioner */
  if(cv_mem->cv_tempv->ops->nvgetarraypointer == NULL) {
    cvProcessError(cv_mem, CVLS_ILL_INPUT, "CVSBANDPRE",
                   "CVBandPrecInit", MSGBP_BAD_NVECTOR);
    return(CVLS_ILL_INPUT);
  }

  /* Allocate data memory */
  pdata = NULL;
  pdata = (CVBandPrecData) malloc(sizeof *pdata);
  if (pdata == NULL) {
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBANDPRE",
                   "CVBandPrecInit", MSGBP_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }

  /* Load pointers and bandwidths into pdata block. */
  pdata->cvode_mem = cvode_mem;
  pdata->N = N;
  pdata->mu = mup = SUNMIN(N-1, SUNMAX(0,mu));
  pdata->ml = mlp = SUNMIN(N-1, SUNMAX(0,ml));

  /* Initialize nfeBP counter */
  pdata->nfeBP = 0;

  /* Allocate memory for saved banded Jacobian approximation. */
  pdata->savedJ = NULL;
  pdata->savedJ = SUNBandMatrixStorage(N, mup, mlp, mup);
  if (pdata->savedJ == NULL) {
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBANDPRE",
                   "CVBandPrecInit", MSGBP_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }

  /* Allocate memory for banded preconditioner. */
  storagemu = SUNMIN(N-1, mup+mlp);
  pdata->savedP = NULL;
  pdata->savedP = SUNBandMatrixStorage(N, mup, mlp, storagemu);
  if (pdata->savedP == NULL) {
    SUNMatDestroy(pdata->savedJ);
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBANDPRE",
                   "CVBandPrecInit", MSGBP_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }

  /* Allocate memory for banded linear solver */
  pdata->LS = NULL;
  pdata->LS = SUNLinSol_Band(cv_mem->cv_tempv, pdata->savedP);
  if (pdata->LS == NULL) {
    SUNMatDestroy(pdata->savedP);
    SUNMatDestroy(pdata->savedJ);
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBANDPRE",
                   "CVBandPrecInit", MSGBP_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }

  /* allocate memory for temporary N_Vectors */
  pdata->tmp1 = NULL;
  pdata->tmp1 = N_VClone(cv_mem->cv_tempv);
  if (pdata->tmp1 == NULL) {
    SUNLinSolFree(pdata->LS);
    SUNMatDestroy(pdata->savedP);
    SUNMatDestroy(pdata->savedJ);
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBANDPRE", 
                    "CVBandPrecInit", MSGBP_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }
  pdata->tmp2 = NULL;
  pdata->tmp2 = N_VClone(cv_mem->cv_tempv);
  if (pdata->tmp2 == NULL) {
    SUNLinSolFree(pdata->LS);
    SUNMatDestroy(pdata->savedP);
    SUNMatDestroy(pdata->savedJ);
    N_VDestroy(pdata->tmp1);
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBANDPRE", 
                    "CVBandPrecInit", MSGBP_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }

  /* initialize band linear solver object */
  flag = SUNLinSolInitialize(pdata->LS);
  if (flag != SUNLS_SUCCESS) {
    SUNLinSolFree(pdata->LS);
    SUNMatDestroy(pdata->savedP);
    SUNMatDestroy(pdata->savedJ);
    N_VDestroy(pdata->tmp1);
    N_VDestroy(pdata->tmp2);
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_SUNLS_FAIL, "CVSBANDPRE", 
                    "CVBandPrecInit", MSGBP_SUNLS_FAIL);
    return(CVLS_SUNLS_FAIL);
  }
  
  /* make sure P_data is free from any previous allocations */
  if (cvls_mem->pfree)
    cvls_mem->pfree(cv_mem);

  /* Point to the new P_data field in the LS memory */
  cvls_mem->P_data = pdata;

  /* Attach the pfree function */
  cvls_mem->pfree = cvBandPrecFree;

  /* Attach preconditioner solve and setup functions */
  flag = CVodeSetPreconditioner(cvode_mem, cvBandPrecSetup,
                                cvBandPrecSolve);
  return(flag);
}


int CVBandPrecGetWorkSpace(void *cvode_mem, long int *lenrwBP,
                           long int *leniwBP)
{
  CVodeMem cv_mem;
  CVLsMem cvls_mem;
  CVBandPrecData pdata;
  sunindextype lrw1, liw1;
  long int lrw, liw;
  int flag;
  
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CVLS_MEM_NULL, "CVSBANDPRE",
                   "CVBandPrecGetWorkSpace", MSGBP_MEM_NULL);
    return(CVLS_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_lmem == NULL) {
    cvProcessError(cv_mem, CVLS_LMEM_NULL, "CVSBANDPRE",
                   "CVBandPrecGetWorkSpace", MSGBP_LMEM_NULL);
    return(CVLS_LMEM_NULL);
  }
  cvls_mem = (CVLsMem) cv_mem->cv_lmem;

  if (cvls_mem->P_data == NULL) {
    cvProcessError(cv_mem, CVLS_PMEM_NULL, "CVSBANDPRE",
                   "CVBandPrecGetWorkSpace", MSGBP_PMEM_NULL);
    return(CVLS_PMEM_NULL);
  } 
  pdata = (CVBandPrecData) cvls_mem->P_data;

  /* sum space requirements for all objects in pdata */
  *leniwBP = 4;
  *lenrwBP = 0;
  if (cv_mem->cv_tempv->ops->nvspace) {
    N_VSpace(cv_mem->cv_tempv, &lrw1, &liw1);
    *leniwBP += 2*liw1;
    *lenrwBP += 2*lrw1;
  }
  if (pdata->savedJ->ops->space) {
    flag = SUNMatSpace(pdata->savedJ, &lrw, &liw);
    if (flag != 0) return(-1);
    *leniwBP += liw;
    *lenrwBP += lrw;
  }
  if (pdata->savedP->ops->space) {
    flag = SUNMatSpace(pdata->savedP, &lrw, &liw);
    if (flag != 0) return(-1);
    *leniwBP += liw;
    *lenrwBP += lrw;
  }
  if (pdata->LS->ops->space) {
    flag = SUNLinSolSpace(pdata->LS, &lrw, &liw);
    if (flag != 0) return(-1);
    *leniwBP += liw;
    *lenrwBP += lrw;
  }

  return(CVLS_SUCCESS);
}


int CVBandPrecGetNumRhsEvals(void *cvode_mem, long int *nfevalsBP)
{
  CVodeMem cv_mem;
  CVLsMem cvls_mem;
  CVBandPrecData pdata;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CVLS_MEM_NULL, "CVSBANDPRE",
                   "CVBandPrecGetNumRhsEvals", MSGBP_MEM_NULL);
    return(CVLS_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_lmem == NULL) {
    cvProcessError(cv_mem, CVLS_LMEM_NULL, "CVSBANDPRE",
                   "CVBandPrecGetNumRhsEvals", MSGBP_LMEM_NULL);
    return(CVLS_LMEM_NULL);
  }
  cvls_mem = (CVLsMem) cv_mem->cv_lmem;

  if (cvls_mem->P_data == NULL) {
    cvProcessError(cv_mem, CVLS_PMEM_NULL, "CVSBANDPRE",
                   "CVBandPrecGetNumRhsEvals", MSGBP_PMEM_NULL);
    return(CVLS_PMEM_NULL);
  } 
  pdata = (CVBandPrecData) cvls_mem->P_data;

  *nfevalsBP = pdata->nfeBP;

  return(CVLS_SUCCESS);
}


/*-----------------------------------------------------------------
  cvBandPrecSetup
  -----------------------------------------------------------------
  Together cvBandPrecSetup and cvBandPrecSolve use a banded
  difference quotient Jacobian to create a preconditioner.
  cvBandPrecSetup calculates a new J, if necessary, then
  calculates P = I - gamma*J, and does an LU factorization of P.
 
  The parameters of cvBandPrecSetup are as follows:
 
  t       is the current value of the independent variable.
 
  y       is the current value of the dependent variable vector,
          namely the predicted value of y(t).
 
  fy      is the vector f(t,y).
 
  jok     is an input flag indicating whether Jacobian-related
          data needs to be recomputed, as follows:
            jok == SUNFALSE means recompute Jacobian-related data
                   from scratch.
            jok == SUNTRUE means that Jacobian data from the
                   previous PrecSetup call will be reused
                   (with the current value of gamma).
          A cvBandPrecSetup call with jok == SUNTRUE should only
          occur after a call with jok == SUNFALSE.
 
  *jcurPtr is a pointer to an output integer flag which is
           set by cvBandPrecSetup as follows:
             *jcurPtr = SUNTRUE if Jacobian data was recomputed.
             *jcurPtr = SUNFALSE if Jacobian data was not recomputed,
                        but saved data was reused.
 
  gamma   is the scalar appearing in the Newton matrix.
 
  bp_data is a pointer to preconditoner data (set by cvBandPrecInit)
 
  The value to be returned by the cvBandPrecSetup function is
    0  if successful, or
    1  if the band factorization failed.
  -----------------------------------------------------------------*/
static int cvBandPrecSetup(realtype t, N_Vector y, N_Vector fy, 
                           booleantype jok, booleantype *jcurPtr, 
                           realtype gamma, void *bp_data)
{
  CVBandPrecData pdata;
  CVodeMem cv_mem;
  int retval;
  sunindextype ier;

  /* Assume matrix and lpivots have already been allocated. */
  pdata = (CVBandPrecData) bp_data;
  cv_mem = (CVodeMem) pdata->cvode_mem;

  if (jok) {

    /* If jok = SUNTRUE, use saved copy of J. */
    *jcurPtr = SUNFALSE;
    retval = SUNMatCopy(pdata->savedJ, pdata->savedP);
    if (retval < 0) {
      cvProcessError(cv_mem, -1, "CVBANDPRE", 
                     "cvBandPrecSetup", MSGBP_SUNMAT_FAIL);
      return(-1);
    }
    if (retval > 0) {
      return(1);
    }

  } else {

    /* If jok = SUNFALSE, call CVBandPDQJac for new J value. */
    *jcurPtr = SUNTRUE;
    retval = SUNMatZero(pdata->savedJ);
    if (retval < 0) {
      cvProcessError(cv_mem, -1, "CVBANDPRE", 
                     "cvBandPrecSetup", MSGBP_SUNMAT_FAIL);
      return(-1);
    }
    if (retval > 0) {
      return(1);
    }

    retval = cvBandPrecDQJac(pdata, t, y, fy, 
                             pdata->tmp1, pdata->tmp2);
    if (retval < 0) {
      cvProcessError(cv_mem, -1, "CVBANDPRE", 
                     "cvBandPrecSetup", MSGBP_RHSFUNC_FAILED);
      return(-1);
    }
    if (retval > 0) {
      return(1);
    }

    retval = SUNMatCopy(pdata->savedJ, pdata->savedP);
    if (retval < 0) {
      cvProcessError(cv_mem, -1, "CVBANDPRE", 
                     "cvBandPrecSetup", MSGBP_SUNMAT_FAIL);
      return(-1);
    }
    if (retval > 0) {
      return(1);
    }

  }
  
  /* Scale and add identity to get savedP = I - gamma*J. */
  retval = SUNMatScaleAddI(-gamma, pdata->savedP);
  if (retval) {
    cvProcessError(cv_mem, -1, "CVBANDPRE", 
                   "cvBandPrecSetup", MSGBP_SUNMAT_FAIL);
    return(-1);
  }

  /* Do LU factorization of matrix and return error flag */
  ier = SUNLinSolSetup_Band(pdata->LS, pdata->savedP);
  return(ier);
}


/*-----------------------------------------------------------------
  cvBandPrecSolve
  -----------------------------------------------------------------
  cvBandPrecSolve solves a linear system P z = r, where P is the
  matrix computed by cvBandPrecond.
 
  The parameters of cvBandPrecSolve used here are as follows:
 
  r       is the right-hand side vector of the linear system.
 
  bp_data is a pointer to preconditoner data (set by CVBandPrecInit)
 
  z       is the output vector computed by cvBandPrecSolve.
 
  The value returned by the cvBandPrecSolve function is always 0,
  indicating success.
  -----------------------------------------------------------------*/ 
static int cvBandPrecSolve(realtype t, N_Vector y, N_Vector fy, 
                           N_Vector r, N_Vector z, realtype gamma, 
                           realtype delta, int lr, void *bp_data)
{
  CVBandPrecData pdata;
  int retval;

  /* Assume matrix and lpivots have already been allocated. */
  pdata = (CVBandPrecData) bp_data;

  /* Call banded solver object to do the work */
  retval = SUNLinSolSolve(pdata->LS, pdata->savedP, z, r, ZERO);
  return(retval);
}


static int cvBandPrecFree(CVodeMem cv_mem)
{
  CVLsMem cvls_mem;
  CVBandPrecData pdata;

  if (cv_mem->cv_lmem == NULL) return(0);
  cvls_mem = (CVLsMem) cv_mem->cv_lmem;
  
  if (cvls_mem->P_data == NULL) return(0);
  pdata = (CVBandPrecData) cvls_mem->P_data;

  SUNLinSolFree(pdata->LS);
  SUNMatDestroy(pdata->savedP);
  SUNMatDestroy(pdata->savedJ);
  N_VDestroy(pdata->tmp1);
  N_VDestroy(pdata->tmp2);

  free(pdata);
  pdata = NULL;

  return(0);
}


/*-----------------------------------------------------------------
  cvBandPrecDQJac
  -----------------------------------------------------------------
  This routine generates a banded difference quotient approximation 
  to the Jacobian of f(t,y). It assumes that a band SUNMatrix is 
  stored column-wise, and that elements within each column are 
  contiguous. This makes it possible to get the address of a column
  of J via the accessor function SUNBandMatrix_Column() and to 
  write a simple for loop to set each of the elements of a column 
  in succession.
  -----------------------------------------------------------------*/
static int cvBandPrecDQJac(CVBandPrecData pdata, realtype t, N_Vector y,
                           N_Vector fy, N_Vector ftemp, N_Vector ytemp)
{
  CVodeMem cv_mem;
  realtype fnorm, minInc, inc, inc_inv, yj, srur, conj;
  sunindextype group, i, j, width, ngroups, i1, i2;
  realtype *col_j, *ewt_data, *fy_data, *ftemp_data;
  realtype *y_data, *ytemp_data, *cns_data;
  int retval;

  cv_mem = (CVodeMem) pdata->cvode_mem;

  /* Obtain pointers to the data for ewt, fy, ftemp, y, ytemp. */
  ewt_data   = N_VGetArrayPointer(cv_mem->cv_ewt);
  fy_data    = N_VGetArrayPointer(fy);
  ftemp_data = N_VGetArrayPointer(ftemp);
  y_data     = N_VGetArrayPointer(y);
  ytemp_data = N_VGetArrayPointer(ytemp);
  if (cv_mem->cv_constraints != NULL)
    cns_data  = N_VGetArrayPointer(cv_mem->cv_constraints);

  /* Load ytemp with y = predicted y vector. */
  N_VScale(ONE, y, ytemp);

  /* Set minimum increment based on uround and norm of f. */
  srur = SUNRsqrt(cv_mem->cv_uround);
  fnorm = N_VWrmsNorm(fy, cv_mem->cv_ewt);
  minInc = (fnorm != ZERO) ?
    (MIN_INC_MULT * SUNRabs(cv_mem->cv_h) * cv_mem->cv_uround * pdata->N * fnorm) : ONE;

  /* Set bandwidth and number of column groups for band differencing. */
  width = pdata->ml + pdata->mu + 1;
  ngroups = SUNMIN(width, pdata->N);
  
  for (group = 1; group <= ngroups; group++) {
    
    /* Increment all y_j in group. */
    for(j = group-1; j < pdata->N; j += width) {
      inc = SUNMAX(srur*SUNRabs(y_data[j]), minInc/ewt_data[j]);
      yj = y_data[j];

      /* Adjust sign(inc) again if yj has an inequality constraint. */
      if (cv_mem->cv_constraints != NULL) {
        conj = cns_data[j];
        if (SUNRabs(conj) == ONE)      {if ((yj+inc)*conj < ZERO)  inc = -inc;}
        else if (SUNRabs(conj) == TWO) {if ((yj+inc)*conj <= ZERO) inc = -inc;}
      }

      ytemp_data[j] += inc;
    }

    /* Evaluate f with incremented y. */
    retval = cv_mem->cv_f(t, ytemp, ftemp, cv_mem->cv_user_data);
    pdata->nfeBP++;
    if (retval != 0) return(retval);

    /* Restore ytemp, then form and load difference quotients. */
    for (j = group-1; j < pdata->N; j += width) {
      yj = y_data[j];
      ytemp_data[j] = y_data[j];
      col_j = SUNBandMatrix_Column(pdata->savedJ,j);
      inc = SUNMAX(srur*SUNRabs(y_data[j]), minInc/ewt_data[j]);

      /* Adjust sign(inc) as before. */
      if (cv_mem->cv_constraints != NULL) {
        conj = cns_data[j];
        if (SUNRabs(conj) == ONE)      {if ((yj+inc)*conj < ZERO)  inc = -inc;}
        else if (SUNRabs(conj) == TWO) {if ((yj+inc)*conj <= ZERO) inc = -inc;}
      }

      inc_inv = ONE/inc;
      i1 = SUNMAX(0, j-pdata->mu);
      i2 = SUNMIN(j + pdata->ml, pdata->N - 1);
      for (i=i1; i <= i2; i++)
        SM_COLUMN_ELEMENT_B(col_j,i,j) = inc_inv * (ftemp_data[i] - fy_data[i]);
    }
  }

  return(0);
}


/*================================================================
  PART II - Backward Problems
  ================================================================*/

/*---------------------------------------------------------------
  User-Callable initialization function: wrapper for the backward
  phase around the corresponding CVODES functions
  ---------------------------------------------------------------*/
int CVBandPrecInitB(void *cvode_mem, int which, sunindextype nB,
                    sunindextype muB, sunindextype mlB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CVLS_MEM_NULL, "CVSBANDPRE",
                   "CVBandPrecInitB", MSGBP_MEM_NULL);
    return(CVLS_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CVLS_NO_ADJ, "CVSBANDPRE",
                   "CVBandPrecInitB", MSGBP_NO_ADJ);
    return(CVLS_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CVLS_ILL_INPUT, "CVSBANDPRE",
                   "CVBandPrecInitB", MSGBP_BAD_WHICH);
    return(CVLS_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    /* advance */
    cvB_mem = cvB_mem->cv_next;
  }
  /* cv_mem corresponding to 'which' problem. */
  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  /* Set pfree */
  cvB_mem->cv_pfree = NULL;
 
  /* Initialize the band preconditioner for this backward problem. */
  flag = CVBandPrecInit(cvodeB_mem, nB, muB, mlB);
  return(flag);
}
