/*
 * ----------------------------------------------------------------- 
 * Programmer(s): Daniel R. Reynolds @ SMU
 *        Alan C. Hindmarsh and Radu Serban @ LLNL
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
 * matrix with banded blocks, for use with IDA, the IDASLS 
 * linear solver interface.
 *
 * NOTE: With only one processor in use, a banded matrix results
 * rather than a block-diagonal matrix with banded blocks.
 * Diagonal blocking occurs at the processor level.
 * -----------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>

#include "idas_impl.h"
#include "idas_ls_impl.h"
#include "idas_bbdpre_impl.h"
#include <sundials/sundials_math.h>
#include <nvector/nvector_serial.h>


#define ZERO RCONST(0.0)
#define ONE  RCONST(1.0)
#define TWO  RCONST(2.0)

/* Prototypes of IDABBDPrecSetup and IDABBDPrecSolve */
static int IDABBDPrecSetup(realtype tt, N_Vector yy, N_Vector yp,
                           N_Vector rr, realtype c_j, void *prec_data);
static int IDABBDPrecSolve(realtype tt, N_Vector yy, N_Vector yp,
                           N_Vector rr, N_Vector rvec, N_Vector zvec,
                           realtype c_j, realtype delta, void *prec_data);

/* Prototype for IDABBDPrecFree */
static int IDABBDPrecFree(IDAMem ida_mem);

/* Prototype for difference quotient Jacobian calculation routine */
static int IBBDDQJac(IBBDPrecData pdata, realtype tt, realtype cj,
                     N_Vector yy, N_Vector yp, N_Vector gref, 
                     N_Vector ytemp, N_Vector yptemp, N_Vector gtemp);

/* Wrapper functions for adjoint code */
static int IDAAglocal(sunindextype NlocalB, realtype tt, N_Vector yyB, 
                      N_Vector ypB, N_Vector gvalB, void *user_dataB);

static int IDAAgcomm(sunindextype NlocalB, realtype tt, N_Vector yyB,
                     N_Vector ypB, void *user_dataB);

/* Prototype for the pfree routine for backward problems. */
static int IDABBDPrecFreeB(IDABMem IDAB_mem);


/*================================================================
  PART I - forward problems
  ================================================================*/

/*---------------------------------------------------------------
  User-Callable Functions: initialization, reinit and free
  ---------------------------------------------------------------*/
int IDABBDPrecInit(void *ida_mem, sunindextype Nlocal, 
                   sunindextype mudq, sunindextype mldq, 
                   sunindextype mukeep, sunindextype mlkeep, 
                   realtype dq_rel_yy, 
                   IDABBDLocalFn Gres, IDABBDCommFn Gcomm)
{
  IDAMem IDA_mem;
  IDALsMem idals_mem;
  IBBDPrecData pdata;
  sunindextype muk, mlk, storage_mu, lrw1, liw1;
  long int lrw, liw;
  int flag;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDALS_MEM_NULL, "IDASBBDPRE",
                    "IDABBDPrecInit", MSGBBD_MEM_NULL);
    return(IDALS_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Test if the LS linear solver interface has been created */
  if (IDA_mem->ida_lmem == NULL) {
    IDAProcessError(IDA_mem, IDALS_LMEM_NULL, "IDASBBDPRE",
                    "IDABBDPrecInit", MSGBBD_LMEM_NULL);
    return(IDALS_LMEM_NULL);
  }
  idals_mem = (IDALsMem) IDA_mem->ida_lmem;

  /* Test compatibility of NVECTOR package with the BBD preconditioner */
  if(IDA_mem->ida_tempv1->ops->nvgetarraypointer == NULL) {
    IDAProcessError(IDA_mem, IDALS_ILL_INPUT, "IDASBBDPRE",
                    "IDABBDPrecInit", MSGBBD_BAD_NVECTOR);
    return(IDALS_ILL_INPUT);
  }

  /* Allocate data memory. */
  pdata = NULL;
  pdata = (IBBDPrecData) malloc(sizeof *pdata);
  if (pdata == NULL) {
    IDAProcessError(IDA_mem, IDALS_MEM_FAIL, "IDASBBDPRE",
                    "IDABBDPrecInit", MSGBBD_MEM_FAIL);
    return(IDALS_MEM_FAIL);
  }

  /* Set pointers to glocal and gcomm; load half-bandwidths. */
  pdata->ida_mem = IDA_mem;
  pdata->glocal = Gres;
  pdata->gcomm = Gcomm;
  pdata->mudq = SUNMIN(Nlocal-1, SUNMAX(0, mudq));
  pdata->mldq = SUNMIN(Nlocal-1, SUNMAX(0, mldq));
  muk = SUNMIN(Nlocal-1, SUNMAX(0, mukeep));
  mlk = SUNMIN(Nlocal-1, SUNMAX(0, mlkeep));
  pdata->mukeep = muk;
  pdata->mlkeep = mlk;

  /* Set extended upper half-bandwidth for PP (required for pivoting). */
  storage_mu = SUNMIN(Nlocal-1, muk+mlk);

  /* Allocate memory for preconditioner matrix. */
  pdata->PP = NULL;
  pdata->PP = SUNBandMatrixStorage(Nlocal, muk, mlk, storage_mu);
  if (pdata->PP == NULL) { 
    free(pdata); pdata = NULL;
    IDAProcessError(IDA_mem, IDALS_MEM_FAIL, "IDASBBDPRE",
                    "IDABBDPrecInit", MSGBBD_MEM_FAIL);
    return(IDALS_MEM_FAIL); 
  }

  /* Allocate memory for temporary N_Vectors */
  pdata->zlocal = NULL;
  pdata->zlocal = N_VNewEmpty_Serial(Nlocal);
  if (pdata->zlocal == NULL) {
    SUNMatDestroy(pdata->PP);
    free(pdata); pdata = NULL;
    IDAProcessError(IDA_mem, IDALS_MEM_FAIL, "IDASBBDPRE", 
                    "IDABBDPrecInit", MSGBBD_MEM_FAIL);
    return(IDALS_MEM_FAIL);
  }
  pdata->rlocal = NULL;
  pdata->rlocal = N_VNewEmpty_Serial(Nlocal);
  if (pdata->rlocal == NULL) {
    N_VDestroy(pdata->zlocal);
    SUNMatDestroy(pdata->PP);
    free(pdata); pdata = NULL;
    IDAProcessError(IDA_mem, IDALS_MEM_FAIL, "IDASBBDPRE", 
                    "IDABBDPrecInit", MSGBBD_MEM_FAIL);
    return(IDALS_MEM_FAIL);
  }
  pdata->tempv1 = NULL;
  pdata->tempv1 = N_VClone(IDA_mem->ida_tempv1); 
  if (pdata->tempv1 == NULL){
    N_VDestroy(pdata->rlocal);
    N_VDestroy(pdata->zlocal);
    SUNMatDestroy(pdata->PP);
    free(pdata); pdata = NULL;
    IDAProcessError(IDA_mem, IDALS_MEM_FAIL, "IDASBBDPRE",
                    "IDABBDPrecInit", MSGBBD_MEM_FAIL);
    return(IDALS_MEM_FAIL);
  }
  pdata->tempv2 = NULL;
  pdata->tempv2 = N_VClone(IDA_mem->ida_tempv1); 
  if (pdata->tempv2 == NULL){
    N_VDestroy(pdata->rlocal);
    N_VDestroy(pdata->zlocal);
    N_VDestroy(pdata->tempv1);
    SUNMatDestroy(pdata->PP);
    free(pdata); pdata = NULL;
    IDAProcessError(IDA_mem, IDALS_MEM_FAIL, "IDASBBDPRE",
                    "IDABBDPrecInit", MSGBBD_MEM_FAIL);
    return(IDALS_MEM_FAIL);
  }
  pdata->tempv3 = NULL;
  pdata->tempv3 = N_VClone(IDA_mem->ida_tempv1); 
  if (pdata->tempv3 == NULL){
    N_VDestroy(pdata->rlocal);
    N_VDestroy(pdata->zlocal);
    N_VDestroy(pdata->tempv1);
    N_VDestroy(pdata->tempv2);
    SUNMatDestroy(pdata->PP);
    free(pdata); pdata = NULL;
    IDAProcessError(IDA_mem, IDALS_MEM_FAIL, "IDASBBDPRE",
                    "IDABBDPrecInit", MSGBBD_MEM_FAIL);
    return(IDALS_MEM_FAIL);
  }
  pdata->tempv4 = NULL;
  pdata->tempv4 = N_VClone(IDA_mem->ida_tempv1); 
  if (pdata->tempv4 == NULL){
    N_VDestroy(pdata->rlocal);
    N_VDestroy(pdata->zlocal);
    N_VDestroy(pdata->tempv1);
    N_VDestroy(pdata->tempv2);
    N_VDestroy(pdata->tempv3);
    SUNMatDestroy(pdata->PP);
    free(pdata); pdata = NULL;
    IDAProcessError(IDA_mem, IDALS_MEM_FAIL, "IDASBBDPRE",
                    "IDABBDPrecInit", MSGBBD_MEM_FAIL);
    return(IDALS_MEM_FAIL);
  }

  /* Allocate memory for banded linear solver */
  pdata->LS = NULL;
  pdata->LS = SUNLinSol_Band(pdata->rlocal, pdata->PP);
  if (pdata->LS == NULL) {
    N_VDestroy(pdata->zlocal);
    N_VDestroy(pdata->rlocal);
    N_VDestroy(pdata->tempv1);
    N_VDestroy(pdata->tempv2);
    N_VDestroy(pdata->tempv3);
    N_VDestroy(pdata->tempv4);
    SUNMatDestroy(pdata->PP);
    free(pdata); pdata = NULL;
    IDAProcessError(IDA_mem, IDALS_MEM_FAIL, "IDASBBDPRE",
                    "IDABBDPrecInit", MSGBBD_MEM_FAIL);
    return(IDALS_MEM_FAIL);
  }

  /* initialize band linear solver object */
  flag = SUNLinSolInitialize(pdata->LS);
  if (flag != SUNLS_SUCCESS) {
    N_VDestroy(pdata->zlocal);
    N_VDestroy(pdata->rlocal);
    N_VDestroy(pdata->tempv1);
    N_VDestroy(pdata->tempv2);
    N_VDestroy(pdata->tempv3);
    N_VDestroy(pdata->tempv4);
    SUNMatDestroy(pdata->PP);
    SUNLinSolFree(pdata->LS);
    free(pdata); pdata = NULL;
    IDAProcessError(IDA_mem, IDALS_SUNLS_FAIL, "IDASBBDPRE",
                    "IDABBDPrecInit", MSGBBD_SUNLS_FAIL);
    return(IDALS_SUNLS_FAIL);
  }
 
  /* Set rel_yy based on input value dq_rel_yy (0 implies default). */
  pdata->rel_yy = (dq_rel_yy > ZERO) ?
    dq_rel_yy : SUNRsqrt(IDA_mem->ida_uround); 

  /* Store Nlocal to be used in IDABBDPrecSetup */
  pdata->n_local = Nlocal;
  
  /* Set work space sizes and initialize nge. */
  pdata->rpwsize = 0;
  pdata->ipwsize = 0;
  if (IDA_mem->ida_tempv1->ops->nvspace) {
    N_VSpace(IDA_mem->ida_tempv1, &lrw1, &liw1);
    pdata->rpwsize += 4*lrw1;
    pdata->ipwsize += 4*liw1;
  }
  if (pdata->rlocal->ops->nvspace) {
    N_VSpace(pdata->rlocal, &lrw1, &liw1);
    pdata->rpwsize += 2*lrw1;
    pdata->ipwsize += 2*liw1;
  }
  if (pdata->PP->ops->space) {
    flag = SUNMatSpace(pdata->PP, &lrw, &liw);
    pdata->rpwsize += lrw;
    pdata->ipwsize += liw;
  }
  if (pdata->LS->ops->space) {
    flag = SUNLinSolSpace(pdata->LS, &lrw, &liw);
    pdata->rpwsize += lrw;
    pdata->ipwsize += liw;
  }
  pdata->nge = 0;

  /* make sure pdata is free from any previous allocations */
  if (idals_mem->pfree) 
    idals_mem->pfree(IDA_mem);

  /* Point to the new pdata field in the LS memory */
  idals_mem->pdata = pdata;

  /* Attach the pfree function */
  idals_mem->pfree = IDABBDPrecFree;

  /* Attach preconditioner solve and setup functions */
  flag = IDASetPreconditioner(ida_mem, IDABBDPrecSetup,
                              IDABBDPrecSolve);

  return(flag);
}


/*-------------------------------------------------------------*/
int IDABBDPrecReInit(void *ida_mem, sunindextype mudq,
                     sunindextype mldq, realtype dq_rel_yy)
{
  IDAMem IDA_mem;
  IDALsMem idals_mem;
  IBBDPrecData pdata;
  sunindextype Nlocal;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDALS_MEM_NULL, "IDASBBDPRE",
                    "IDABBDPrecReInit", MSGBBD_MEM_NULL);
    return(IDALS_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Test if the LS linear solver interface has been created */
  if (IDA_mem->ida_lmem == NULL) {
    IDAProcessError(IDA_mem, IDALS_LMEM_NULL, "IDASBBDPRE",
                    "IDABBDPrecReInit", MSGBBD_LMEM_NULL);
    return(IDALS_LMEM_NULL);
  }
  idals_mem = (IDALsMem) IDA_mem->ida_lmem;

  /* Test if the preconditioner data is non-NULL */
  if (idals_mem->pdata == NULL) {
    IDAProcessError(IDA_mem, IDALS_PMEM_NULL, "IDASBBDPRE",
                    "IDABBDPrecReInit", MSGBBD_PMEM_NULL);
    return(IDALS_PMEM_NULL);
  } 
  pdata = (IBBDPrecData) idals_mem->pdata;

  /* Load half-bandwidths. */
  Nlocal = pdata->n_local;
  pdata->mudq = SUNMIN(Nlocal-1, SUNMAX(0, mudq));
  pdata->mldq = SUNMIN(Nlocal-1, SUNMAX(0, mldq));

  /* Set rel_yy based on input value dq_rel_yy (0 implies default). */
  pdata->rel_yy = (dq_rel_yy > ZERO) ?
    dq_rel_yy : SUNRsqrt(IDA_mem->ida_uround); 

  /* Re-initialize nge */
  pdata->nge = 0;

  return(IDALS_SUCCESS);
}


/*-------------------------------------------------------------*/
int IDABBDPrecGetWorkSpace(void *ida_mem,
                           long int *lenrwBBDP,
                           long int *leniwBBDP)
{
  IDAMem IDA_mem;
  IDALsMem idals_mem;
  IBBDPrecData pdata;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDALS_MEM_NULL, "IDASBBDPRE",
                    "IDABBDPrecGetWorkSpace", MSGBBD_MEM_NULL);
    return(IDALS_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_lmem == NULL) {
    IDAProcessError(IDA_mem, IDALS_LMEM_NULL, "IDASBBDPRE",
                    "IDABBDPrecGetWorkSpace", MSGBBD_LMEM_NULL);
    return(IDALS_LMEM_NULL);
  }
  idals_mem = (IDALsMem) IDA_mem->ida_lmem;

  if (idals_mem->pdata == NULL) {
    IDAProcessError(IDA_mem, IDALS_PMEM_NULL, "IDASBBDPRE",
                    "IDABBDPrecGetWorkSpace", MSGBBD_PMEM_NULL);
    return(IDALS_PMEM_NULL);
  } 
  pdata = (IBBDPrecData) idals_mem->pdata;

  *lenrwBBDP = pdata->rpwsize;
  *leniwBBDP = pdata->ipwsize;

  return(IDALS_SUCCESS);
}


/*-------------------------------------------------------------*/
int IDABBDPrecGetNumGfnEvals(void *ida_mem,
                             long int *ngevalsBBDP)
{
  IDAMem IDA_mem;
  IDALsMem idals_mem;
  IBBDPrecData pdata;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDALS_MEM_NULL, "IDASBBDPRE",
                    "IDABBDPrecGetNumGfnEvals", MSGBBD_MEM_NULL);
    return(IDALS_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  if (IDA_mem->ida_lmem == NULL) {
    IDAProcessError(IDA_mem, IDALS_LMEM_NULL, "IDASBBDPRE",
                    "IDABBDPrecGetNumGfnEvals", MSGBBD_LMEM_NULL);
    return(IDALS_LMEM_NULL);
  }
  idals_mem = (IDALsMem) IDA_mem->ida_lmem;

  if (idals_mem->pdata == NULL) {
    IDAProcessError(IDA_mem, IDALS_PMEM_NULL, "IDASBBDPRE",
                    "IDABBDPrecGetNumGfnEvals", MSGBBD_PMEM_NULL);
    return(IDALS_PMEM_NULL);
  } 
  pdata = (IBBDPrecData) idals_mem->pdata;

  *ngevalsBBDP = pdata->nge;

  return(IDALS_SUCCESS);
}




/*---------------------------------------------------------------
  IDABBDPrecSetup:

  IDABBDPrecSetup generates a band-block-diagonal preconditioner
  matrix, where the local block (on this processor) is a band
  matrix. Each local block is computed by a difference quotient
  scheme via calls to the user-supplied routines glocal, gcomm.
  After generating the block in the band matrix PP, this routine
  does an LU factorization in place in PP.
 
  The IDABBDPrecSetup parameters used here are as follows:
 
  tt is the current value of the independent variable t.
 
  yy is the current value of the dependent variable vector,
     namely the predicted value of y(t).
 
  yp is the current value of the derivative vector y',
     namely the predicted value of y'(t).
 
  c_j is the scalar in the system Jacobian, proportional to 1/hh.
 
  bbd_data is the pointer to BBD memory set by IDABBDInit
 
  The argument rr is not used.
 
  Return value:
  The value returned by this IDABBDPrecSetup function is a int
  flag indicating whether it was successful. This value is
     0    if successful,
   > 0    for a recoverable error (step will be retried), or
   < 0    for a nonrecoverable error (step fails).
 ----------------------------------------------------------------*/
static int IDABBDPrecSetup(realtype tt, N_Vector yy, N_Vector yp,
                           N_Vector rr, realtype c_j, void *bbd_data)
{
  sunindextype ier;
  IBBDPrecData pdata;
  IDAMem IDA_mem;
  int retval;

  pdata =(IBBDPrecData) bbd_data;

  IDA_mem = (IDAMem) pdata->ida_mem;

  /* Call IBBDDQJac for a new Jacobian calculation and store in PP. */
  retval = SUNMatZero(pdata->PP);
  retval = IBBDDQJac(pdata, tt, c_j, yy, yp, pdata->tempv1,
                     pdata->tempv2, pdata->tempv3, pdata->tempv4);
  if (retval < 0) {
    IDAProcessError(IDA_mem, -1, "IDASBBDPRE", "IDABBDPrecSetup",
                    MSGBBD_FUNC_FAILED);
    return(-1);
  }
  if (retval > 0) {
    return(+1);
  } 
 
  /* Do LU factorization of matrix and return error flag */
  ier = SUNLinSolSetup_Band(pdata->LS, pdata->PP);
  return(ier);
}


/*---------------------------------------------------------------
  IDABBDPrecSolve

  The function IDABBDPrecSolve computes a solution to the linear
  system P z = r, where P is the left preconditioner defined by
  the routine IDABBDPrecSetup.
 
  The IDABBDPrecSolve parameters used here are as follows:
 
  rvec is the input right-hand side vector r.
 
  zvec is the computed solution vector z.
 
  bbd_data is the pointer to BBD data set by IDABBDInit.
 
  The arguments tt, yy, yp, rr, c_j and delta are NOT used.
 
  IDABBDPrecSolve returns the value returned from the linear 
  solver object.
  ---------------------------------------------------------------*/
static int IDABBDPrecSolve(realtype tt, N_Vector yy, N_Vector yp,
                           N_Vector rr, N_Vector rvec, N_Vector zvec,
                           realtype c_j, realtype delta, void *bbd_data)
{
  IBBDPrecData pdata;
  int retval;

  pdata = (IBBDPrecData) bbd_data;

  /* Attach local data arrays for rvec and zvec to rlocal and zlocal */
  N_VSetArrayPointer(N_VGetArrayPointer(rvec), pdata->rlocal);
  N_VSetArrayPointer(N_VGetArrayPointer(zvec), pdata->zlocal);
  
  /* Call banded solver object to do the work */
  retval = SUNLinSolSolve(pdata->LS, pdata->PP, pdata->zlocal, 
                          pdata->rlocal, ZERO);

  /* Detach local data arrays from rlocal and zlocal */
  N_VSetArrayPointer(NULL, pdata->rlocal);
  N_VSetArrayPointer(NULL, pdata->zlocal);

  return(retval);
}


/*-------------------------------------------------------------*/
static int IDABBDPrecFree(IDAMem IDA_mem)
{
  IDALsMem idals_mem;
  IBBDPrecData pdata;
  
  if (IDA_mem->ida_lmem == NULL) return(0);
  idals_mem = (IDALsMem) IDA_mem->ida_lmem;
  
  if (idals_mem->pdata == NULL) return(0);
  pdata = (IBBDPrecData) idals_mem->pdata;

  SUNLinSolFree(pdata->LS);
  N_VDestroy(pdata->rlocal);
  N_VDestroy(pdata->zlocal);
  N_VDestroy(pdata->tempv1);
  N_VDestroy(pdata->tempv2);
  N_VDestroy(pdata->tempv3);
  N_VDestroy(pdata->tempv4);
  SUNMatDestroy(pdata->PP);

  free(pdata);
  pdata = NULL;

  return(0);
}


/*---------------------------------------------------------------
  IBBDDQJac

  This routine generates a banded difference quotient approximation
  to the local block of the Jacobian of G(t,y,y'). It assumes that
  a band matrix of type SUNMatrix is stored column-wise, and that
  elements within each column are contiguous.
 
  All matrix elements are generated as difference quotients, by way
  of calls to the user routine glocal. By virtue of the band
  structure, the number of these calls is bandwidth + 1, where
  bandwidth = mldq + mudq + 1. But the band matrix kept has
  bandwidth = mlkeep + mukeep + 1. This routine also assumes that
  the local elements of a vector are stored contiguously.
 
  Return values are: 0 (success), > 0 (recoverable error),
  or < 0 (nonrecoverable error).
  ----------------------------------------------------------------*/
static int IBBDDQJac(IBBDPrecData pdata, realtype tt, realtype cj,
                     N_Vector yy, N_Vector yp, N_Vector gref, 
                     N_Vector ytemp, N_Vector yptemp, N_Vector gtemp)
{
  IDAMem IDA_mem;
  realtype inc, inc_inv;
  int retval;
  sunindextype group, i, j, width, ngroups, i1, i2;
  realtype *ydata, *ypdata, *ytempdata, *yptempdata, *grefdata, *gtempdata;
  realtype *cnsdata = NULL, *ewtdata;
  realtype *col_j, conj, yj, ypj, ewtj;

  IDA_mem = (IDAMem) pdata->ida_mem;

  /* Initialize ytemp and yptemp. */
  N_VScale(ONE, yy, ytemp);
  N_VScale(ONE, yp, yptemp);

  /* Obtain pointers as required to the data array of vectors. */
  ydata     = N_VGetArrayPointer(yy);
  ypdata    = N_VGetArrayPointer(yp);
  gtempdata = N_VGetArrayPointer(gtemp);
  ewtdata   = N_VGetArrayPointer(IDA_mem->ida_ewt);
  if (IDA_mem->ida_constraints != NULL) 
    cnsdata = N_VGetArrayPointer(IDA_mem->ida_constraints);
  ytempdata = N_VGetArrayPointer(ytemp);
  yptempdata= N_VGetArrayPointer(yptemp);
  grefdata = N_VGetArrayPointer(gref);

  /* Call gcomm and glocal to get base value of G(t,y,y'). */
  if (pdata->gcomm != NULL) {
    retval = pdata->gcomm(pdata->n_local, tt, yy, yp, IDA_mem->ida_user_data);
    if (retval != 0) return(retval);
  }

  retval = pdata->glocal(pdata->n_local, tt, yy, yp, gref, IDA_mem->ida_user_data); 
  pdata->nge++;
  if (retval != 0) return(retval);

  /* Set bandwidth and number of column groups for band differencing. */
  width = pdata->mldq + pdata->mudq + 1;
  ngroups = SUNMIN(width, pdata->n_local);

  /* Loop over groups. */
  for(group = 1; group <= ngroups; group++) {
    
    /* Loop over the components in this group. */
    for(j = group-1; j < pdata->n_local; j += width) {
      yj = ydata[j];
      ypj = ypdata[j];
      ewtj = ewtdata[j];
      
      /* Set increment inc to yj based on rel_yy*abs(yj), with
         adjustments using ypj and ewtj if this is small, and a further
         adjustment to give it the same sign as hh*ypj. */
      inc = pdata->rel_yy *
        SUNMAX(SUNRabs(yj), SUNMAX( SUNRabs(IDA_mem->ida_hh*ypj), ONE/ewtj));
      if (IDA_mem->ida_hh*ypj < ZERO)  inc = -inc;
      inc = (yj + inc) - yj;
      
      /* Adjust sign(inc) again if yj has an inequality constraint. */
      if (IDA_mem->ida_constraints != NULL) {
        conj = cnsdata[j];
        if (SUNRabs(conj) == ONE)      {if ((yj+inc)*conj <  ZERO) inc = -inc;}
        else if (SUNRabs(conj) == TWO) {if ((yj+inc)*conj <= ZERO) inc = -inc;}
      }

      /* Increment yj and ypj. */
      ytempdata[j] += inc;
      yptempdata[j] += cj*inc;
      
    }

    /* Evaluate G with incremented y and yp arguments. */
    retval = pdata->glocal(pdata->n_local, tt, ytemp, yptemp,
                           gtemp, IDA_mem->ida_user_data); 
    pdata->nge++;
    if (retval != 0) return(retval);

    /* Loop over components of the group again; restore ytemp and yptemp. */
    for(j = group-1; j < pdata->n_local; j += width) {
      yj  = ytempdata[j]  = ydata[j];
      ypj = yptempdata[j] = ypdata[j];
      ewtj = ewtdata[j];

      /* Set increment inc as before .*/
      inc = pdata->rel_yy *
        SUNMAX(SUNRabs(yj), SUNMAX( SUNRabs(IDA_mem->ida_hh*ypj), ONE/ewtj));
      if (IDA_mem->ida_hh*ypj < ZERO)  inc = -inc;
      inc = (yj + inc) - yj;
      if (IDA_mem->ida_constraints != NULL) {
        conj = cnsdata[j];
        if (SUNRabs(conj) == ONE)      {if ((yj+inc)*conj <  ZERO) inc = -inc;}
        else if (SUNRabs(conj) == TWO) {if ((yj+inc)*conj <= ZERO) inc = -inc;}
      }

      /* Form difference quotients and load into PP. */
      inc_inv = ONE/inc;
      col_j = SUNBandMatrix_Column(pdata->PP,j);
      i1 = SUNMAX(0, j-pdata->mukeep);
      i2 = SUNMIN(j + pdata->mlkeep, pdata->n_local-1);
      for(i = i1; i <= i2; i++)
        SM_COLUMN_ELEMENT_B(col_j,i,j) =
          inc_inv * (gtempdata[i] - grefdata[i]);
    }
  }
  
  return(0);
}


/*================================================================
  PART II - backward problems
  ================================================================*/

/*---------------------------------------------------------------
  User-Callable Functions: initialization, reinit and free
  ---------------------------------------------------------------*/
int IDABBDPrecInitB(void *ida_mem, int which, sunindextype NlocalB,
                    sunindextype mudqB, sunindextype mldqB,
                    sunindextype mukeepB, sunindextype mlkeepB,
                    realtype dq_rel_yyB, IDABBDLocalFnB glocalB, 
                    IDABBDCommFnB gcommB)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  IDABBDPrecDataB idabbdB_mem;
  void *ida_memB;
  int flag;
  
  /* Check if ida_mem is allright. */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDALS_MEM_NULL, "IDASBBDPRE",
                    "IDABBDPrecInitB", MSG_LS_IDAMEM_NULL);
    return(IDALS_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDALS_NO_ADJ, "IDASBBDPRE",
                    "IDABBDPrecInitB", MSG_LS_NO_ADJ);
    return(IDALS_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDALS_ILL_INPUT, "IDASBBDPRE",
                    "IDABBDPrecInitB", MSG_LS_BAD_WHICH);
    return(IDALS_ILL_INPUT);
  }

  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }
  /* ida_mem corresponding to 'which' problem. */
  ida_memB = (void *) IDAB_mem->IDA_mem;

  /* Initialize the BBD preconditioner for this backward problem. */
  flag = IDABBDPrecInit(ida_memB, NlocalB, mudqB, mldqB, mukeepB,
                        mlkeepB, dq_rel_yyB, IDAAglocal, IDAAgcomm);
  if (flag != IDA_SUCCESS) return(flag);

  /* Allocate memory for IDABBDPrecDataB to store the user-provided
     functions which will be called from the wrappers */
  idabbdB_mem = NULL;
  idabbdB_mem = (IDABBDPrecDataB) malloc(sizeof(* idabbdB_mem));
  if (idabbdB_mem == NULL) {
    IDAProcessError(IDA_mem, IDALS_MEM_FAIL, "IDASBBDPRE",
                    "IDABBDPrecInitB", MSGBBD_MEM_FAIL);
    return(IDALS_MEM_FAIL);
  }

  /* set pointers to user-provided functions */
  idabbdB_mem->glocalB = glocalB;
  idabbdB_mem->gcommB  = gcommB;

  /* Attach pmem and pfree */
  IDAB_mem->ida_pmem  = idabbdB_mem;
  IDAB_mem->ida_pfree = IDABBDPrecFreeB;

  return(IDALS_SUCCESS);
}


/*-------------------------------------------------------------*/
int IDABBDPrecReInitB(void *ida_mem, int which, sunindextype mudqB,
                      sunindextype mldqB, realtype dq_rel_yyB)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  void *ida_memB;
  int flag;
  
  /* Check if ida_mem is allright. */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDALS_MEM_NULL, "IDASBBDPRE",
                    "IDABBDPrecReInitB", MSG_LS_IDAMEM_NULL);
    return(IDALS_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* Is ASA initialized? */
  if (IDA_mem->ida_adjMallocDone == SUNFALSE) {
    IDAProcessError(IDA_mem, IDALS_NO_ADJ, "IDASBBDPRE",
                    "IDABBDPrecReInitB",  MSG_LS_NO_ADJ);
    return(IDALS_NO_ADJ);
  }
  IDAADJ_mem = IDA_mem->ida_adj_mem;

  /* Check the value of which */
  if ( which >= IDAADJ_mem->ia_nbckpbs ) {
    IDAProcessError(IDA_mem, IDALS_ILL_INPUT, "IDASBBDPRE",
                    "IDABBDPrecReInitB", MSG_LS_BAD_WHICH);
    return(IDALS_ILL_INPUT);
  }

  /* Find the IDABMem entry in the linked list corresponding to 'which'. */
  IDAB_mem = IDAADJ_mem->IDAB_mem;
  while (IDAB_mem != NULL) {
    if( which == IDAB_mem->ida_index ) break;
    /* advance */
    IDAB_mem = IDAB_mem->ida_next;
  }
  /* ida_mem corresponding to 'which' backward problem. */
  ida_memB = (void *) IDAB_mem->IDA_mem;

  /* ReInitialize the BBD preconditioner for this backward problem. */
  flag = IDABBDPrecReInit(ida_memB, mudqB, mldqB, dq_rel_yyB);
  return(flag);
}


/*-------------------------------------------------------------*/
static int IDABBDPrecFreeB(IDABMem IDAB_mem)
{
  free(IDAB_mem->ida_pmem);
  IDAB_mem->ida_pmem = NULL;
  return(0);
}


/*----------------------------------------------------------------
  Wrapper functions
  ----------------------------------------------------------------*/

/*----------------------------------------------------------------
  IDAAglocal
 
  This routine interfaces to the IDALocalFnB routine 
  provided by the user.
  ----------------------------------------------------------------*/
static int IDAAglocal(sunindextype NlocalB, realtype tt, N_Vector yyB,
                      N_Vector ypB, N_Vector gvalB, void *ida_mem)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  IDABBDPrecDataB idabbdB_mem;
  int flag;

  IDA_mem = (IDAMem) ida_mem;
  IDAADJ_mem = IDA_mem->ida_adj_mem;
  
  /* Get current backward problem. */
  IDAB_mem = IDAADJ_mem->ia_bckpbCrt;

  /* Get the preconditioner's memory. */
  idabbdB_mem = (IDABBDPrecDataB) IDAB_mem->ida_pmem;

  /* Get forward solution from interpolation. */
  if (IDAADJ_mem->ia_noInterp == SUNFALSE) {
    flag = IDAADJ_mem->ia_getY(IDA_mem, tt, IDAADJ_mem->ia_yyTmp,
                               IDAADJ_mem->ia_ypTmp, NULL, NULL);
    if (flag != IDA_SUCCESS) {
      IDAProcessError(IDA_mem, -1, "IDASBBDPRE", "IDAAglocal",
                      MSGBBD_BAD_T);
      return(-1);
    } 
  }
  /* Call user's adjoint LocalFnB function. */
  return idabbdB_mem->glocalB(NlocalB, tt, IDAADJ_mem->ia_yyTmp,
                              IDAADJ_mem->ia_ypTmp, yyB, ypB,
                              gvalB, IDAB_mem->ida_user_data);
}


/*----------------------------------------------------------------
  IDAAgcomm
 
  This routine interfaces to the IDACommFnB routine 
  provided by the user.
  ----------------------------------------------------------------*/
static int IDAAgcomm(sunindextype NlocalB, realtype tt,
                     N_Vector yyB, N_Vector ypB, void *ida_mem)
{
  IDAMem IDA_mem;
  IDAadjMem IDAADJ_mem;
  IDABMem IDAB_mem;
  IDABBDPrecDataB idabbdB_mem;
  int flag;

  IDA_mem = (IDAMem) ida_mem;
  IDAADJ_mem = IDA_mem->ida_adj_mem;
  
  /* Get current backward problem. */
  IDAB_mem = IDAADJ_mem->ia_bckpbCrt;

  /* Get the preconditioner's memory. */
  idabbdB_mem = (IDABBDPrecDataB) IDAB_mem->ida_pmem;
  if (idabbdB_mem->gcommB == NULL) return(0);

  /* Get forward solution from interpolation. */
  if (IDAADJ_mem->ia_noInterp == SUNFALSE) {
    flag = IDAADJ_mem->ia_getY(IDA_mem, tt, IDAADJ_mem->ia_yyTmp,
                               IDAADJ_mem->ia_ypTmp, NULL, NULL);
    if (flag != IDA_SUCCESS) {
      IDAProcessError(IDA_mem, -1, "IDASBBDPRE", "IDAAgcomm",
                      MSGBBD_BAD_T);
      return(-1);
    } 
  }

  /* Call user's adjoint CommFnB routine */
  return idabbdB_mem->gcommB(NlocalB, tt, IDAADJ_mem->ia_yyTmp, 
                             IDAADJ_mem->ia_ypTmp, yyB, ypB, 
                             IDAB_mem->ida_user_data);
}
