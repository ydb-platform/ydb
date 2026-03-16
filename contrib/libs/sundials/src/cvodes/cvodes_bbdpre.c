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
 * This file contains implementations of routines for a
 * band-block-diagonal preconditioner, i.e. a block-diagonal
 * matrix with banded blocks, for use with CVODE, the CVSLS linear
 * solver interface, and the MPI-parallel implementation of NVECTOR.
 * -----------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>

#include "cvodes_impl.h"
#include "cvodes_bbdpre_impl.h"
#include "cvodes_ls_impl.h"
#include <sundials/sundials_math.h>
#include <nvector/nvector_serial.h>

#define MIN_INC_MULT RCONST(1000.0)
#define ZERO         RCONST(0.0)
#define ONE          RCONST(1.0)
#define TWO          RCONST(2.0)

/* Prototypes of functions cvBBDPrecSetup and cvBBDPrecSolve */
static int cvBBDPrecSetup(realtype t, N_Vector y, N_Vector fy, 
                          booleantype jok, booleantype *jcurPtr, 
                          realtype gamma, void *bbd_data);
static int cvBBDPrecSolve(realtype t, N_Vector y, N_Vector fy, 
                          N_Vector r, N_Vector z, 
                          realtype gamma, realtype delta,
                          int lr, void *bbd_data);

/* Prototype for cvBBDPrecFree */
static int cvBBDPrecFree(CVodeMem cv_mem);

/* Wrapper functions for adjoint code */
static int cvGlocWrapper(sunindextype NlocalB, realtype t,
                         N_Vector yB, N_Vector gB, 
                         void *cvadj_mem);
static int cvCfnWrapper(sunindextype NlocalB, realtype t,
                        N_Vector yB, void *cvadj_mem);

/* Prototype for difference quotient Jacobian calculation routine */
static int cvBBDDQJac(CVBBDPrecData pdata, realtype t, 
                      N_Vector y, N_Vector gy, 
                      N_Vector ytemp, N_Vector gtemp);

/* Prototype for the backward pfree routine */
static int CVBBDPrecFreeB(CVodeBMem cvB_mem);


/*================================================================
  PART I - forward problems
  ================================================================*/

/*-----------------------------------------------------------------
  User-Callable Functions: initialization, reinit and free
  -----------------------------------------------------------------*/
int CVBBDPrecInit(void *cvode_mem, sunindextype Nlocal, 
                  sunindextype mudq, sunindextype mldq,
                  sunindextype mukeep, sunindextype mlkeep, 
                  realtype dqrely, CVLocalFn gloc, CVCommFn cfn)
{
  CVodeMem cv_mem;
  CVLsMem cvls_mem;
  CVBBDPrecData pdata;
  sunindextype muk, mlk, storage_mu, lrw1, liw1;
  long int lrw, liw;
  int flag;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CVLS_MEM_NULL, "CVSBBDPRE",
                   "CVBBDPrecInit", MSGBBD_MEM_NULL);
    return(CVLS_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Test if the CVSLS linear solver interface has been created */
  if (cv_mem->cv_lmem == NULL) {
    cvProcessError(cv_mem, CVLS_LMEM_NULL, "CVSBBDPRE",
                   "CVBBDPrecInit", MSGBBD_LMEM_NULL);
    return(CVLS_LMEM_NULL);
  }
  cvls_mem = (CVLsMem) cv_mem->cv_lmem;

  /* Test compatibility of NVECTOR package with the BBD preconditioner */
  if(cv_mem->cv_tempv->ops->nvgetarraypointer == NULL) {
    cvProcessError(cv_mem, CVLS_ILL_INPUT, "CVSBBDPRE",
                   "CVBBDPrecInit", MSGBBD_BAD_NVECTOR);
    return(CVLS_ILL_INPUT);
  }

  /* Allocate data memory */
  pdata = NULL;
  pdata = (CVBBDPrecData) malloc(sizeof *pdata);  
  if (pdata == NULL) {
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBBDPRE",
                   "CVBBDPrecInit", MSGBBD_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }

  /* Set pointers to gloc and cfn; load half-bandwidths */
  pdata->cvode_mem = cvode_mem;
  pdata->gloc = gloc;
  pdata->cfn = cfn;
  pdata->mudq = SUNMIN(Nlocal-1, SUNMAX(0,mudq));
  pdata->mldq = SUNMIN(Nlocal-1, SUNMAX(0,mldq));
  muk = SUNMIN(Nlocal-1, SUNMAX(0,mukeep));
  mlk = SUNMIN(Nlocal-1, SUNMAX(0,mlkeep));
  pdata->mukeep = muk;
  pdata->mlkeep = mlk;

  /* Allocate memory for saved Jacobian */
  pdata->savedJ = SUNBandMatrixStorage(Nlocal, muk, mlk, muk);
  if (pdata->savedJ == NULL) { 
    free(pdata); pdata = NULL; 
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBBDPRE",
                   "CVBBDPrecInit", MSGBBD_MEM_FAIL);
    return(CVLS_MEM_FAIL); 
  }

  /* Allocate memory for preconditioner matrix */
  storage_mu = SUNMIN(Nlocal-1, muk + mlk);
  pdata->savedP = NULL;
  pdata->savedP = SUNBandMatrixStorage(Nlocal, muk, mlk, storage_mu);
  if (pdata->savedP == NULL) {
    SUNMatDestroy(pdata->savedJ);
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBBDPRE",
                   "CVBBDPrecInit", MSGBBD_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }
  
  /* Allocate memory for temporary N_Vectors */
  pdata->zlocal = NULL;
  pdata->zlocal = N_VNewEmpty_Serial(Nlocal);
  if (pdata->zlocal == NULL) {
    SUNMatDestroy(pdata->savedP);
    SUNMatDestroy(pdata->savedJ);
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBBDPRE", 
                   "CVBBDPrecInit", MSGBBD_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }
  pdata->rlocal = NULL;
  pdata->rlocal = N_VNewEmpty_Serial(Nlocal);
  if (pdata->rlocal == NULL) {
    N_VDestroy(pdata->zlocal);
    SUNMatDestroy(pdata->savedP);
    SUNMatDestroy(pdata->savedJ);
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBBDPRE", 
                   "CVBBDPrecInit", MSGBBD_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }
  pdata->tmp1 = NULL;
  pdata->tmp1 = N_VClone(cv_mem->cv_tempv);
  if (pdata->tmp1 == NULL) {
    N_VDestroy(pdata->zlocal);
    N_VDestroy(pdata->rlocal);
    SUNMatDestroy(pdata->savedP);
    SUNMatDestroy(pdata->savedJ);
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBBDPRE", 
                   "CVBBDPrecInit", MSGBBD_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }
  pdata->tmp2 = NULL;
  pdata->tmp2 = N_VClone(cv_mem->cv_tempv);
  if (pdata->tmp2 == NULL) {
    N_VDestroy(pdata->tmp1);
    N_VDestroy(pdata->zlocal);
    N_VDestroy(pdata->rlocal);
    SUNMatDestroy(pdata->savedP);
    SUNMatDestroy(pdata->savedJ);
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBBDPRE", 
                   "CVBBDPrecInit", MSGBBD_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }
  pdata->tmp3 = NULL;
  pdata->tmp3 = N_VClone(cv_mem->cv_tempv);
  if (pdata->tmp3 == NULL) {
    N_VDestroy(pdata->tmp1);
    N_VDestroy(pdata->tmp2);
    N_VDestroy(pdata->zlocal);
    N_VDestroy(pdata->rlocal);
    SUNMatDestroy(pdata->savedP);
    SUNMatDestroy(pdata->savedJ);
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBBDPRE", 
                   "CVBBDPrecInit", MSGBBD_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }

  /* Allocate memory for banded linear solver */
  pdata->LS = NULL;
  pdata->LS = SUNLinSol_Band(pdata->rlocal, pdata->savedP);
  if (pdata->LS == NULL) {
    N_VDestroy(pdata->tmp1);
    N_VDestroy(pdata->tmp2);
    N_VDestroy(pdata->tmp3);
    N_VDestroy(pdata->zlocal);
    N_VDestroy(pdata->rlocal);
    SUNMatDestroy(pdata->savedP);
    SUNMatDestroy(pdata->savedJ);
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBBDPRE", 
                   "CVBBDPrecInit", MSGBBD_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }

  /* initialize band linear solver object */
  flag = SUNLinSolInitialize(pdata->LS);
  if (flag != SUNLS_SUCCESS) {
    N_VDestroy(pdata->tmp1);
    N_VDestroy(pdata->tmp2);
    N_VDestroy(pdata->tmp3);
    N_VDestroy(pdata->zlocal);
    N_VDestroy(pdata->rlocal);
    SUNMatDestroy(pdata->savedP);
    SUNMatDestroy(pdata->savedJ);
    SUNLinSolFree(pdata->LS);
    free(pdata); pdata = NULL;
    cvProcessError(cv_mem, CVLS_SUNLS_FAIL, "CVSBBDPRE", 
                   "CVBBDPrecInit", MSGBBD_SUNLS_FAIL);
    return(CVLS_SUNLS_FAIL);
  }

  /* Set pdata->dqrely based on input dqrely (0 implies default). */
  pdata->dqrely = (dqrely > ZERO) ?
    dqrely : SUNRsqrt(cv_mem->cv_uround);

  /* Store Nlocal to be used in CVBBDPrecSetup */
  pdata->n_local = Nlocal;

  /* Set work space sizes and initialize nge */
  pdata->rpwsize = 0;
  pdata->ipwsize = 0;
  if (cv_mem->cv_tempv->ops->nvspace) {
    N_VSpace(cv_mem->cv_tempv, &lrw1, &liw1);
    pdata->rpwsize += 3*lrw1;
    pdata->ipwsize += 3*liw1;
  }
  if (pdata->rlocal->ops->nvspace) {
    N_VSpace(pdata->rlocal, &lrw1, &liw1);
    pdata->rpwsize += 2*lrw1;
    pdata->ipwsize += 2*liw1;
  }
  if (pdata->savedJ->ops->space) {
    flag = SUNMatSpace(pdata->savedJ, &lrw, &liw);
    pdata->rpwsize += lrw;
    pdata->ipwsize += liw;
  }
  if (pdata->savedP->ops->space) {
    flag = SUNMatSpace(pdata->savedP, &lrw, &liw);
    pdata->rpwsize += lrw;
    pdata->ipwsize += liw;
  }
  if (pdata->LS->ops->space) {
    flag = SUNLinSolSpace(pdata->LS, &lrw, &liw);
    pdata->rpwsize += lrw;
    pdata->ipwsize += liw;
  }
  pdata->nge = 0;

  /* make sure s_P_data is free from any previous allocations */
  if (cvls_mem->pfree)
    cvls_mem->pfree(cv_mem);

  /* Point to the new P_data field in the LS memory */
  cvls_mem->P_data = pdata;

  /* Attach the pfree function */
  cvls_mem->pfree = cvBBDPrecFree;

  /* Attach preconditioner solve and setup functions */
  flag = CVodeSetPreconditioner(cvode_mem, cvBBDPrecSetup,
                                cvBBDPrecSolve);
  return(flag);
}


int CVBBDPrecReInit(void *cvode_mem, sunindextype mudq, 
                    sunindextype mldq, realtype dqrely)
{
  CVodeMem cv_mem;
  CVLsMem cvls_mem;
  CVBBDPrecData pdata;
  sunindextype Nlocal;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CVLS_MEM_NULL, "CVSBBDPRE",
                   "CVBBDPrecReInit", MSGBBD_MEM_NULL);
    return(CVLS_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Test if the LS linear solver interface has been created */
  if (cv_mem->cv_lmem == NULL) {
    cvProcessError(cv_mem, CVLS_LMEM_NULL, "CVSBBDPRE",
                   "CVBBDPrecReInit", MSGBBD_LMEM_NULL);
    return(CVLS_LMEM_NULL);
  }
  cvls_mem = (CVLsMem) cv_mem->cv_lmem;

  /* Test if the preconditioner data is non-NULL */
  if (cvls_mem->P_data == NULL) {
    cvProcessError(cv_mem, CVLS_PMEM_NULL, "CVSBBDPRE",
                   "CVBBDPrecReInit", MSGBBD_PMEM_NULL);
    return(CVLS_PMEM_NULL);
  } 
  pdata = (CVBBDPrecData) cvls_mem->P_data;

  /* Load half-bandwidths */
  Nlocal = pdata->n_local;
  pdata->mudq = SUNMIN(Nlocal-1, SUNMAX(0,mudq));
  pdata->mldq = SUNMIN(Nlocal-1, SUNMAX(0,mldq));

  /* Set pdata->dqrely based on input dqrely (0 implies default). */
  pdata->dqrely = (dqrely > ZERO) ?
    dqrely : SUNRsqrt(cv_mem->cv_uround);

  /* Re-initialize nge */
  pdata->nge = 0;

  return(CVLS_SUCCESS);
}


int CVBBDPrecGetWorkSpace(void *cvode_mem,
                          long int *lenrwBBDP,
                          long int *leniwBBDP)
{
  CVodeMem cv_mem;
  CVLsMem cvls_mem;
  CVBBDPrecData pdata;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CVLS_MEM_NULL, "CVSBBDPRE",
                   "CVBBDPrecGetWorkSpace", MSGBBD_MEM_NULL);
    return(CVLS_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_lmem == NULL) {
    cvProcessError(cv_mem, CVLS_LMEM_NULL, "CVSBBDPRE",
                   "CVBBDPrecGetWorkSpace", MSGBBD_LMEM_NULL);
    return(CVLS_LMEM_NULL);
  }
  cvls_mem = (CVLsMem) cv_mem->cv_lmem;

  if (cvls_mem->P_data == NULL) {
    cvProcessError(cv_mem, CVLS_PMEM_NULL, "CVSBBDPRE",
                   "CVBBDPrecGetWorkSpace", MSGBBD_PMEM_NULL);
    return(CVLS_PMEM_NULL);
  } 
  pdata = (CVBBDPrecData) cvls_mem->P_data;

  *lenrwBBDP = pdata->rpwsize;
  *leniwBBDP = pdata->ipwsize;

  return(CVLS_SUCCESS);
}


int CVBBDPrecGetNumGfnEvals(void *cvode_mem,
                            long int *ngevalsBBDP)
{
  CVodeMem cv_mem;
  CVLsMem cvls_mem;
  CVBBDPrecData pdata;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CVLS_MEM_NULL, "CVSBBDPRE",
                   "CVBBDPrecGetNumGfnEvals", MSGBBD_MEM_NULL);
    return(CVLS_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_lmem == NULL) {
    cvProcessError(cv_mem, CVLS_LMEM_NULL, "CVSBBDPRE",
                   "CVBBDPrecGetNumGfnEvals", MSGBBD_LMEM_NULL);
    return(CVLS_LMEM_NULL);
  }
  cvls_mem = (CVLsMem) cv_mem->cv_lmem;

  if (cvls_mem->P_data == NULL) {
    cvProcessError(cv_mem, CVLS_PMEM_NULL, "CVSBBDPRE",
                   "CVBBDPrecGetNumGfnEvals", MSGBBD_PMEM_NULL);
    return(CVLS_PMEM_NULL);
  } 
  pdata = (CVBBDPrecData) cvls_mem->P_data;

  *ngevalsBBDP = pdata->nge;

  return(CVLS_SUCCESS);
}


/*-----------------------------------------------------------------
  Function : cvBBDPrecSetup                                      
  -----------------------------------------------------------------
  cvBBDPrecSetup generates and factors a banded block of the
  preconditioner matrix on each processor, via calls to the
  user-supplied gloc and cfn functions. It uses difference
  quotient approximations to the Jacobian elements.
 
  cvBBDPrecSetup calculates a new J,if necessary, then calculates
  P = I - gamma*J, and does an LU factorization of P.
 
  The parameters of cvBBDPrecSetup used here are as follows:
 
  t       is the current value of the independent variable.
 
  y       is the current value of the dependent variable vector,
          namely the predicted value of y(t).
 
  fy      is the vector f(t,y).
 
  jok     is an input flag indicating whether Jacobian-related
          data needs to be recomputed, as follows:
            jok == SUNFALSE means recompute Jacobian-related data
                   from scratch.
            jok == SUNTRUE  means that Jacobian data from the
                   previous CVBBDPrecon call can be reused
                   (with the current value of gamma).
          A cvBBDPrecSetup call with jok == SUNTRUE should only occur
          after a call with jok == SUNFALSE.
 
  jcurPtr is a pointer to an output integer flag which is
          set by cvBBDPrecSetup as follows:
            *jcurPtr = SUNTRUE if Jacobian data was recomputed.
            *jcurPtr = SUNFALSE if Jacobian data was not recomputed,
                       but saved data was reused.
 
  gamma   is the scalar appearing in the Newton matrix.
 
  bbd_data is a pointer to the preconditioner data set by
           CVBBDPrecInit
 
  Return value:
  The value returned by this cvBBDPrecSetup function is the int
    0  if successful,
    1  for a recoverable error (step will be retried).
  -----------------------------------------------------------------*/
static int cvBBDPrecSetup(realtype t, N_Vector y, N_Vector fy, 
                          booleantype jok, booleantype *jcurPtr, 
                          realtype gamma, void *bbd_data)
{
  sunindextype ier;
  CVBBDPrecData pdata;
  CVodeMem cv_mem;
  int retval;

  pdata = (CVBBDPrecData) bbd_data;
  cv_mem = (CVodeMem) pdata->cvode_mem;

  /* If jok = SUNTRUE, use saved copy of J */
  if (jok) {
    *jcurPtr = SUNFALSE;
    retval = SUNMatCopy(pdata->savedJ, pdata->savedP);
    if (retval < 0) {
      cvProcessError(cv_mem, -1, "CVBBDPRE", 
                     "CVBBDPrecSetup", MSGBBD_SUNMAT_FAIL);
      return(-1);
    }
    if (retval > 0) {
      return(1);
    }

  /* Otherwise call cvBBDDQJac for new J value */
  } else {

    *jcurPtr = SUNTRUE;
    retval = SUNMatZero(pdata->savedJ);
    if (retval < 0) {
      cvProcessError(cv_mem, -1, "CVBBDPRE", 
                     "CVBBDPrecSetup", MSGBBD_SUNMAT_FAIL);
      return(-1);
    }
    if (retval > 0) {
      return(1);
    }

    retval = cvBBDDQJac(pdata, t, y, pdata->tmp1, 
                        pdata->tmp2, pdata->tmp3);
    if (retval < 0) {
      cvProcessError(cv_mem, -1, "CVBBDPRE", "CVBBDPrecSetup", 
                     MSGBBD_FUNC_FAILED);
      return(-1);
    }
    if (retval > 0) {
      return(1);
    }

    retval = SUNMatCopy(pdata->savedJ, pdata->savedP);
    if (retval < 0) {
      cvProcessError(cv_mem, -1, "CVBBDPRE", 
                     "CVBBDPrecSetup", MSGBBD_SUNMAT_FAIL);
      return(-1);
    }
    if (retval > 0) {
      return(1);
    }

  }
  
  /* Scale and add I to get P = I - gamma*J */
  retval = SUNMatScaleAddI(-gamma, pdata->savedP);
  if (retval) {
    cvProcessError(cv_mem, -1, "CVBBDPRE", 
                   "CVBBDPrecSetup", MSGBBD_SUNMAT_FAIL);
    return(-1);
  }
 
  /* Do LU factorization of matrix and return error flag */
  ier = SUNLinSolSetup_Band(pdata->LS, pdata->savedP);
  return(ier);
}


/*-----------------------------------------------------------------
  Function : cvBBDPrecSolve
  -----------------------------------------------------------------
  cvBBDPrecSolve solves a linear system P z = r, with the
  band-block-diagonal preconditioner matrix P generated and
  factored by cvBBDPrecSetup.
 
  The parameters of cvBBDPrecSolve used here are as follows:
 
  r is the right-hand side vector of the linear system.
 
  bbd_data is a pointer to the preconditioner data set by
    CVBBDPrecInit.
 
  z is the output vector computed by cvBBDPrecSolve.
 
  The value returned by the cvBBDPrecSolve function is always 0,
  indicating success.
  -----------------------------------------------------------------*/
static int cvBBDPrecSolve(realtype t, N_Vector y, N_Vector fy, 
                          N_Vector r, N_Vector z, 
                          realtype gamma, realtype delta,
                          int lr, void *bbd_data)
{
  int retval;
  CVBBDPrecData pdata;

  pdata = (CVBBDPrecData) bbd_data;

  /* Attach local data arrays for r and z to rlocal and zlocal */
  N_VSetArrayPointer(N_VGetArrayPointer(r), pdata->rlocal);
  N_VSetArrayPointer(N_VGetArrayPointer(z), pdata->zlocal);
  
  /* Call banded solver object to do the work */
  retval = SUNLinSolSolve(pdata->LS, pdata->savedP, pdata->zlocal, 
                          pdata->rlocal, ZERO);

  /* Detach local data arrays from rlocal and zlocal */
  N_VSetArrayPointer(NULL, pdata->rlocal);
  N_VSetArrayPointer(NULL, pdata->zlocal);

  return(retval);
}


static int cvBBDPrecFree(CVodeMem cv_mem)
{
  CVLsMem cvls_mem;
  CVBBDPrecData pdata;
  
  if (cv_mem->cv_lmem == NULL) return(0);
  cvls_mem = (CVLsMem) cv_mem->cv_lmem;
  
  if (cvls_mem->P_data == NULL) return(0);
  pdata = (CVBBDPrecData) cvls_mem->P_data;

  SUNLinSolFree(pdata->LS);
  N_VDestroy(pdata->tmp1);
  N_VDestroy(pdata->tmp2);
  N_VDestroy(pdata->tmp3);
  N_VDestroy(pdata->zlocal);
  N_VDestroy(pdata->rlocal);
  SUNMatDestroy(pdata->savedP);
  SUNMatDestroy(pdata->savedJ);

  free(pdata);
  pdata = NULL;
  
  return(0);
}


/*-----------------------------------------------------------------
  Function : cvBBDDQJac
  -----------------------------------------------------------------
  This routine generates a banded difference quotient approximation
  to the local block of the Jacobian of g(t,y). It assumes that a
  band SUNMatrix is stored columnwise, and that elements within each 
  column are contiguous. All matrix elements are generated as 
  difference quotients, by way of calls to the user routine gloc.
  By virtue of the band structure, the number of these calls is
  bandwidth + 1, where bandwidth = mldq + mudq + 1.
  But the band matrix kept has bandwidth = mlkeep + mukeep + 1.
  This routine also assumes that the local elements of a vector are
  stored contiguously.
  -----------------------------------------------------------------*/
static int cvBBDDQJac(CVBBDPrecData pdata, realtype t, N_Vector y, 
                      N_Vector gy, N_Vector ytemp, N_Vector gtemp)
{
  CVodeMem cv_mem;
  realtype gnorm, minInc, inc, inc_inv, yj, conj;
  sunindextype group, i, j, width, ngroups, i1, i2;
  realtype *y_data, *ewt_data, *gy_data, *gtemp_data;
  realtype *ytemp_data, *col_j, *cns_data;
  int retval;

  cv_mem = (CVodeMem) pdata->cvode_mem;

  /* Load ytemp with y = predicted solution vector */
  N_VScale(ONE, y, ytemp);

  /* Call cfn and gloc to get base value of g(t,y) */
  if (pdata->cfn != NULL) {
    retval = pdata->cfn(pdata->n_local, t, y, cv_mem->cv_user_data);
    if (retval != 0) return(retval);
  }

  retval = pdata->gloc(pdata->n_local, t, ytemp, gy,
                       cv_mem->cv_user_data);
  pdata->nge++;
  if (retval != 0) return(retval);

  /* Obtain pointers to the data for various vectors */
  y_data     =  N_VGetArrayPointer(y);
  gy_data    =  N_VGetArrayPointer(gy);
  ewt_data   =  N_VGetArrayPointer(cv_mem->cv_ewt);
  ytemp_data =  N_VGetArrayPointer(ytemp);
  gtemp_data =  N_VGetArrayPointer(gtemp);
  if (cv_mem->cv_constraints != NULL)
    cns_data  =  N_VGetArrayPointer(cv_mem->cv_constraints);

  /* Set minimum increment based on uround and norm of g */
  gnorm = N_VWrmsNorm(gy, cv_mem->cv_ewt);
  minInc = (gnorm != ZERO) ?
    (MIN_INC_MULT * SUNRabs(cv_mem->cv_h) *
     cv_mem->cv_uround * pdata->n_local * gnorm) : ONE;

  /* Set bandwidth and number of column groups for band differencing */
  width = pdata->mldq + pdata->mudq + 1;
  ngroups = SUNMIN(width, pdata->n_local);

  /* Loop over groups */  
  for (group=1; group <= ngroups; group++) {
    
    /* Increment all y_j in group */
    for(j=group-1; j < pdata->n_local; j+=width) {
      inc = SUNMAX(pdata->dqrely * SUNRabs(y_data[j]), minInc/ewt_data[j]);
      yj = y_data[j];

      /* Adjust sign(inc) again if yj has an inequality constraint. */
      if (cv_mem->cv_constraints != NULL) {
        conj = cns_data[j];
        if (SUNRabs(conj) == ONE)      {if ((yj+inc)*conj < ZERO)  inc = -inc;}
        else if (SUNRabs(conj) == TWO) {if ((yj+inc)*conj <= ZERO) inc = -inc;}
      }

      ytemp_data[j] += inc;
    }

    /* Evaluate g with incremented y */
    retval = pdata->gloc(pdata->n_local, t, ytemp, gtemp,
                         cv_mem->cv_user_data);
    pdata->nge++;
    if (retval != 0) return(retval);

    /* Restore ytemp, then form and load difference quotients */
    for (j=group-1; j < pdata->n_local; j+=width) {
      yj = ytemp_data[j] = y_data[j];
      col_j = SUNBandMatrix_Column(pdata->savedJ,j);
      inc = SUNMAX(pdata->dqrely * SUNRabs(y_data[j]), minInc/ewt_data[j]);

      /* Adjust sign(inc) as before. */
      if (cv_mem->cv_constraints != NULL) {
        conj = cns_data[j];
        if (SUNRabs(conj) == ONE)      {if ((yj+inc)*conj < ZERO)  inc = -inc;}
        else if (SUNRabs(conj) == TWO) {if ((yj+inc)*conj <= ZERO) inc = -inc;}
      }

      inc_inv = ONE/inc;
      i1 = SUNMAX(0, j-pdata->mukeep);
      i2 = SUNMIN(j + pdata->mlkeep, pdata->n_local-1);
      for (i=i1; i <= i2; i++)
        SM_COLUMN_ELEMENT_B(col_j,i,j) =
          inc_inv * (gtemp_data[i] - gy_data[i]);
    }
  }

  return(0);
}


/*================================================================
  PART II - Backward Problems
  ================================================================*/

/*---------------------------------------------------------------
  User-Callable Functions: initialization, reinit and free
  ---------------------------------------------------------------*/
int CVBBDPrecInitB(void *cvode_mem, int which, sunindextype NlocalB, 
                   sunindextype mudqB, sunindextype mldqB,
                   sunindextype mukeepB, sunindextype mlkeepB, 
                   realtype dqrelyB, CVLocalFnB glocB, CVCommFnB cfnB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  CVBBDPrecDataB cvbbdB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CVLS_MEM_NULL, "CVSBBDPRE",
                   "CVBBDPrecInitB", MSGBBD_MEM_NULL);
    return(CVLS_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CVLS_NO_ADJ, "CVSBBDPRE",
                   "CVBBDPrecInitB", MSGBBD_NO_ADJ);
    return(CVLS_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CVLS_ILL_INPUT, "CVSBBDPRE",
                   "CVBBDPrecInitB", MSGBBD_BAD_WHICH);
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

  /* Initialize the BBD preconditioner for this backward problem. */
  flag = CVBBDPrecInit(cvodeB_mem, NlocalB, mudqB, mldqB, mukeepB, 
                       mlkeepB, dqrelyB, cvGlocWrapper, cvCfnWrapper);
  if (flag != CV_SUCCESS) return(flag);

  /* Allocate memory for CVBBDPrecDataB to store the user-provided
     functions which will be called from the wrappers */
  cvbbdB_mem = NULL;
  cvbbdB_mem = (CVBBDPrecDataB) malloc(sizeof(* cvbbdB_mem));
  if (cvbbdB_mem == NULL) {
    cvProcessError(cv_mem, CVLS_MEM_FAIL, "CVSBBDPRE",
                   "CVBBDPrecInitB", MSGBBD_MEM_FAIL);
    return(CVLS_MEM_FAIL);
  }

  /* set pointers to user-provided functions */
  cvbbdB_mem->glocB = glocB;
  cvbbdB_mem->cfnB  = cfnB;

  /* Attach pmem and pfree */
  cvB_mem->cv_pmem  = cvbbdB_mem;
  cvB_mem->cv_pfree = CVBBDPrecFreeB;

  return(CVLS_SUCCESS);
}


int CVBBDPrecReInitB(void *cvode_mem, int which, sunindextype mudqB, 
                     sunindextype mldqB, realtype dqrelyB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CVLS_MEM_NULL, "CVSBBDPRE",
                   "CVBBDPrecReInitB", MSGBBD_MEM_NULL);
    return(CVLS_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CVLS_NO_ADJ, "CVSBBDPRE",
                   "CVBBDPrecReInitB", MSGBBD_NO_ADJ);
    return(CVLS_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CVLS_ILL_INPUT, "CVSBBDPRE",
                   "CVBBDPrecReInitB", MSGBBD_BAD_WHICH);
    return(CVLS_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    /* advance */
    cvB_mem = cvB_mem->cv_next;
  }
  /* cv_mem corresponding to 'which' backward problem. */
  cvodeB_mem = (void *) (cvB_mem->cv_mem);
  
  /* ReInitialize the BBD preconditioner for this backward problem. */
  flag = CVBBDPrecReInit(cvodeB_mem, mudqB, mldqB, dqrelyB);
  return(flag);
}


static int CVBBDPrecFreeB(CVodeBMem cvB_mem)
{
  free(cvB_mem->cv_pmem); 
  cvB_mem->cv_pmem = NULL;
  return(0);
}


/*----------------------------------------------------------------
  Wrapper functions
  ----------------------------------------------------------------*/

/* cvGlocWrapper interfaces to the CVLocalFnB routine provided by the user */
static int cvGlocWrapper(sunindextype NlocalB, realtype t, N_Vector yB,
                         N_Vector gB, void *cvode_mem)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  CVBBDPrecDataB cvbbdB_mem;
  int flag;

  cv_mem = (CVodeMem) cvode_mem;
  ca_mem = cv_mem->cv_adj_mem;
  cvB_mem = ca_mem->ca_bckpbCrt;
  cvbbdB_mem = (CVBBDPrecDataB) (cvB_mem->cv_pmem);

  /* Get forward solution from interpolation */
  flag = ca_mem->ca_IMget(cv_mem, t, ca_mem->ca_ytmp, NULL);
  if (flag != CV_SUCCESS) {
    cvProcessError(cv_mem, -1, "CVSBBDPRE", "cvGlocWrapper",
                   MSGBBD_BAD_TINTERP);
    return(-1);
  } 

  /* Call user's adjoint glocB routine */
  return cvbbdB_mem->glocB(NlocalB, t, ca_mem->ca_ytmp, yB,
                           gB, cvB_mem->cv_user_data);
}


/* cvCfnWrapper interfaces to the CVCommFnB routine provided by the user */
static int cvCfnWrapper(sunindextype NlocalB, realtype t,
                        N_Vector yB, void *cvode_mem)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  CVBBDPrecDataB cvbbdB_mem;
  int flag;

  cv_mem = (CVodeMem) cvode_mem;
  ca_mem = cv_mem->cv_adj_mem;
  cvB_mem = ca_mem->ca_bckpbCrt;
  cvbbdB_mem = (CVBBDPrecDataB) (cvB_mem->cv_pmem);
  if (cvbbdB_mem->cfnB == NULL) return(0);

  /* Get forward solution from interpolation */
  flag = ca_mem->ca_IMget(cv_mem, t, ca_mem->ca_ytmp, NULL);
  if (flag != CV_SUCCESS) {
    cvProcessError(cv_mem, -1, "CVSBBDPRE", "cvCfnWrapper",
                   MSGBBD_BAD_TINTERP);
    return(-1);
  } 

  /* Call user's adjoint cfnB routine */
  return cvbbdB_mem->cfnB(NlocalB, t, ca_mem->ca_ytmp,
                          yB, cvB_mem->cv_user_data);
}
