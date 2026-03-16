/*
 * -----------------------------------------------------------------
 * $Revision$
 * $Date$
 * -----------------------------------------------------------------
 * Programmer: Radu Serban @ LLNL
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
 * This is the implementation file for the optional input and output
 * functions for the adjoint module in the CVODES solver.
 * -----------------------------------------------------------------
 */

/* 
 * =================================================================
 * IMPORTED HEADER FILES
 * =================================================================
 */

#include <stdio.h>
#include <stdlib.h>

#include "cvodes_impl.h"
#include <sundials/sundials_types.h>

/* 
 * =================================================================
 * CVODEA PRIVATE CONSTANTS
 * =================================================================
 */

#define ONE         RCONST(1.0) 

/* 
 * =================================================================
 * EXPORTED FUNCTIONS IMPLEMENTATION
 * =================================================================
 */

/* 
 * -----------------------------------------------------------------
 * Optional input functions for ASA
 * -----------------------------------------------------------------
 */

int CVodeSetAdjNoSensi(void *cvode_mem)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeSetAdjNoSensi", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeSetAdjNoSensi", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  ca_mem->ca_IMstoreSensi = SUNFALSE;

  return(CV_SUCCESS);
}

/* 
 * -----------------------------------------------------------------
 * Optional input functions for backward integration
 * -----------------------------------------------------------------
 */

int CVodeSetNonlinearSolverB(void *cvode_mem, int which, SUNNonlinearSolver NLS)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA",
                   "CVodeSetNonlinearSolverB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA",
                   "CVodeSetNonlinearSolverB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  }
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA",
                   "CVodeSetNonlinearSolverB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  return(CVodeSetNonlinearSolver(cvodeB_mem, NLS));
}

int CVodeSetUserDataB(void *cvode_mem, int which, void *user_dataB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeSetUserDataB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeSetUserDataB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeSetUserDataB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvB_mem->cv_user_data = user_dataB;

  return(CV_SUCCESS);
}

int CVodeSetMaxOrdB(void *cvode_mem, int which, int maxordB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;


  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeSetMaxOrdB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeSetMaxOrdB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeSetMaxOrdB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  flag = CVodeSetMaxOrd(cvodeB_mem, maxordB);

  return(flag);
}


int CVodeSetMaxNumStepsB(void *cvode_mem, int which, long int mxstepsB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeSetMaxNumStepsB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeSetMaxNumStepsB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeSetMaxNumStepsB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  flag = CVodeSetMaxNumSteps(cvodeB_mem, mxstepsB);

  return(flag);
}

int CVodeSetStabLimDetB(void *cvode_mem, int which, booleantype stldetB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeSetStabLimDetB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeSetStabLimDetB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeSetStabLimDetB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  flag = CVodeSetStabLimDet(cvodeB_mem, stldetB);

  return(flag);
}

int CVodeSetInitStepB(void *cvode_mem, int which, realtype hinB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeSetInitStepB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeSetInitStepB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeSetInitStepB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  flag = CVodeSetInitStep(cvodeB_mem, hinB);

  return(flag);
}

int CVodeSetMinStepB(void *cvode_mem, int which, realtype hminB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeSetMinStepB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeSetMinStepB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeSetMinStepB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  flag = CVodeSetMinStep(cvodeB_mem, hminB);

  return(flag);
}

int CVodeSetMaxStepB(void *cvode_mem, int which, realtype hmaxB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeSetMaxStepB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeSetMaxStepB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeSetMaxStepB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  flag = CVodeSetMaxStep(cvodeB_mem, hmaxB);

  return(flag);
}

int CVodeSetConstraintsB(void *cvode_mem, int which, N_Vector constraintsB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Is cvode_mem valid? */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeSetConstraintsB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Is ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeSetConstraintsB", MSGCV_NO_ADJ);
  }
  ca_mem = cv_mem->cv_adj_mem;

  /* Check the value of which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeSetConstraintsB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to 'which'. */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index) break;
    /* advance */
    cvB_mem = cvB_mem->cv_next;
  }
  cvodeB_mem = (void *) cvB_mem->cv_mem;

  flag = CVodeSetConstraints(cvodeB_mem, constraintsB);
  return(flag);
}

/*
 * CVodeSetQuad*B
 *
 * Wrappers for the backward phase around the corresponding 
 * CVODES quadrature optional input functions
 */

int CVodeSetQuadErrConB(void *cvode_mem, int which, booleantype errconQB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeSetQuadErrConB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeSetQuadErrConB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeSetQuadErrConB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  flag = CVodeSetQuadErrCon(cvodeB_mem, errconQB);

  return(flag);
}

/* 
 * -----------------------------------------------------------------
 * Optional output functions for backward integration
 * -----------------------------------------------------------------
 */

/*
 * CVodeGetAdjCVodeBmem
 *
 * This function returns a (void *) pointer to the CVODES     
 * memory allocated for the backward problem. This pointer can    
 * then be used to call any of the CVodeGet* CVODES routines to  
 * extract optional output for the backward integration phase.
 */

void *CVodeGetAdjCVodeBmem(void *cvode_mem, int which)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, 0, "CVODEA", "CVodeGetAdjCVodeBmem", MSGCV_NO_MEM);
    return(NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, 0, "CVODEA", "CVodeGetAdjCVodeBmem", MSGCV_NO_ADJ);
    return(NULL);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, 0, "CVODEA", "CVodeGetAdjCVodeBmem", MSGCV_BAD_WHICH);
    return(NULL);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  return(cvodeB_mem);
}

/*
 * CVodeGetAdjCheckPointsInfo
 *
 * This routine loads an array of nckpnts structures of type CVadjCheckPointRec.
 * The user must allocate space for ckpnt.
 */

int CVodeGetAdjCheckPointsInfo(void *cvode_mem, CVadjCheckPointRec *ckpnt)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CkpntMem ck_mem;
  int i;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeGetAdjCheckPointsInfo", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeGetAdjCheckPointsInfo", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  ck_mem = ca_mem->ck_mem;

  i = 0;

  while (ck_mem != NULL) {

    ckpnt[i].my_addr = (void *) ck_mem;
    ckpnt[i].next_addr = (void *) ck_mem->ck_next;
    ckpnt[i].t0 = ck_mem->ck_t0;
    ckpnt[i].t1 = ck_mem->ck_t1;
    ckpnt[i].nstep = ck_mem->ck_nst;
    ckpnt[i].order = ck_mem->ck_q;
    ckpnt[i].step = ck_mem->ck_h;

    ck_mem = ck_mem->ck_next;
    i++;

  }

  return(CV_SUCCESS);

}


/* 
 * -----------------------------------------------------------------
 * Undocumented Development User-Callable Functions
 * -----------------------------------------------------------------
 */


/*
 * CVodeGetAdjDataPointHermite
 *
 * This routine returns the solution stored in the data structure
 * at the 'which' data point. Cubic Hermite interpolation.
 */

int CVodeGetAdjDataPointHermite(void *cvode_mem, int which, 
                                realtype *t, N_Vector y, N_Vector yd)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  DtpntMem *dt_mem;
  HermiteDataMem content;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeGetAdjDataPointHermite", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeGetAdjDataPointHermite", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  dt_mem = ca_mem->dt_mem;

  if (ca_mem->ca_IMtype != CV_HERMITE) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVadjGetDataPointHermite", MSGCV_WRONG_INTERP);
    return(CV_ILL_INPUT);
  }

  *t = dt_mem[which]->t;

  content = (HermiteDataMem) (dt_mem[which]->content);

  if (y != NULL)
    N_VScale(ONE, content->y, y);

  if (yd != NULL)
    N_VScale(ONE, content->yd, yd);

  return(CV_SUCCESS);
}

/*
 * CVodeGetAdjDataPointPolynomial
 *
 * This routine returns the solution stored in the data structure
 * at the 'which' data point. Polynomial interpolation.
 */

int CVodeGetAdjDataPointPolynomial(void *cvode_mem, int which, 
                                   realtype *t, int *order, N_Vector y)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  DtpntMem *dt_mem;
  PolynomialDataMem content;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeGetAdjDataPointPolynomial", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeGetAdjDataPointPolynomial", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  dt_mem = ca_mem->dt_mem;

  if (ca_mem->ca_IMtype != CV_POLYNOMIAL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVadjGetDataPointPolynomial", MSGCV_WRONG_INTERP);
    return(CV_ILL_INPUT);
  }

  *t = dt_mem[which]->t;

  content = (PolynomialDataMem) (dt_mem[which]->content);

  if (y != NULL)
    N_VScale(ONE, content->y, y);

  *order = content->order;

  return(CV_SUCCESS);
}


/*
 * CVodeGetAdjCurrentCheckPoint
 *
 * Returns the address of the 'active' check point.
 */

int CVodeGetAdjCurrentCheckPoint(void *cvode_mem, void **addr)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeGetAdjCurrentCheckPoint", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeGetAdjCurrentCheckPoint", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  *addr = (void *) ca_mem->ca_ckpntData;

  return(CV_SUCCESS);
}
