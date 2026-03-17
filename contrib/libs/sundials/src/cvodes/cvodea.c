/*
 * -----------------------------------------------------------------
 * $Revision$
 * $Date$
 * ----------------------------------------------------------------- 
 * Programmer(s): Radu Serban @ LLNL
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
 * This is the implementation file for the CVODEA adjoint integrator.
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

#include <sundials/sundials_math.h>
#include <sundials/sundials_types.h>

/* 
 * =================================================================
 * CVODEA PRIVATE CONSTANTS
 * =================================================================
 */

#define ZERO        RCONST(0.0)        /* real 0.0   */
#define ONE         RCONST(1.0)        /* real 1.0   */
#define TWO         RCONST(2.0)        /* real 2.0   */
#define HUNDRED     RCONST(100.0)      /* real 100.0 */
#define FUZZ_FACTOR RCONST(1000000.0)  /* fuzz factor for IMget */

/* 
 * =================================================================
 * PRIVATE FUNCTION PROTOTYPES
 * =================================================================
 */

static CkpntMem CVAckpntInit(CVodeMem cv_mem);
static CkpntMem CVAckpntNew(CVodeMem cv_mem);
static void CVAckpntDelete(CkpntMem *ck_memPtr);

static void CVAbckpbDelete(CVodeBMem *cvB_memPtr);

static int  CVAdataStore(CVodeMem cv_mem, CkpntMem ck_mem);
static int  CVAckpntGet(CVodeMem cv_mem, CkpntMem ck_mem); 

static int CVAfindIndex(CVodeMem cv_mem, realtype t, 
                        long int *indx, booleantype *newpoint);

static booleantype CVAhermiteMalloc(CVodeMem cv_mem);
static void CVAhermiteFree(CVodeMem cv_mem);
static int CVAhermiteGetY(CVodeMem cv_mem, realtype t, N_Vector y, N_Vector *yS);
static int CVAhermiteStorePnt(CVodeMem cv_mem, DtpntMem d);

static booleantype CVApolynomialMalloc(CVodeMem cv_mem);
static void CVApolynomialFree(CVodeMem cv_mem);
static int CVApolynomialGetY(CVodeMem cv_mem, realtype t, N_Vector y, N_Vector *yS);
static int CVApolynomialStorePnt(CVodeMem cv_mem, DtpntMem d);

/* Wrappers */

static int CVArhs(realtype t, N_Vector yB, 
                  N_Vector yBdot, void *cvode_mem);

static int CVArhsQ(realtype t, N_Vector yB, 
                   N_Vector qBdot, void *cvode_mem);

/* 
 * =================================================================
 * EXPORTED FUNCTIONS IMPLEMENTATION
 * =================================================================
 */

/*
 * CVodeAdjInit
 *
 * This routine initializes ASA and allocates space for the adjoint 
 * memory structure.
 */

int CVodeAdjInit(void *cvode_mem, long int steps, int interp)
{
  CVadjMem ca_mem;
  CVodeMem cv_mem;
  long int i, ii;

  /* ---------------
   * Check arguments
   * --------------- */

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeAdjInit", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem)cvode_mem;

  if (steps <= 0) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeAdjInit", MSGCV_BAD_STEPS);
    return(CV_ILL_INPUT);
  }

  if ( (interp != CV_HERMITE) && (interp != CV_POLYNOMIAL) ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeAdjInit", MSGCV_BAD_INTERP);
    return(CV_ILL_INPUT);
  } 

  /* ----------------------------
   * Allocate CVODEA memory block
   * ---------------------------- */

  ca_mem = NULL;
  ca_mem = (CVadjMem) malloc(sizeof(struct CVadjMemRec));
  if (ca_mem == NULL) {
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODEA", "CVodeAdjInit", MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }

  /* Attach ca_mem to CVodeMem structure */

  cv_mem->cv_adj_mem = ca_mem;

  /* ------------------------------
   * Initialization of check points
   * ------------------------------ */

  /* Set Check Points linked list to NULL */
  ca_mem->ck_mem = NULL;

  /* Initialize nckpnts to ZERO */
  ca_mem->ca_nckpnts = 0;

  /* No interpolation data is available */
  ca_mem->ca_ckpntData = NULL;

  /* ------------------------------------
   * Initialization of interpolation data
   * ------------------------------------ */

  /* Interpolation type */

  ca_mem->ca_IMtype = interp;

  /* Number of steps between check points */

  ca_mem->ca_nsteps = steps;

  /* Last index used in CVAfindIndex, initailize to invalid value */
  ca_mem->ca_ilast = -1;

  /* Allocate space for the array of Data Point structures */

  ca_mem->dt_mem = NULL;
  ca_mem->dt_mem = (DtpntMem *) malloc((steps+1)*sizeof(struct DtpntMemRec *));
  if (ca_mem->dt_mem == NULL) {
    free(ca_mem); ca_mem = NULL;
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODEA", "CVodeAdjInit", MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }

  for (i=0; i<=steps; i++) { 
    ca_mem->dt_mem[i] = NULL;
    ca_mem->dt_mem[i] = (DtpntMem) malloc(sizeof(struct DtpntMemRec));
    if (ca_mem->dt_mem[i] == NULL) {
      for(ii=0; ii<i; ii++) {free(ca_mem->dt_mem[ii]); ca_mem->dt_mem[ii] = NULL;}
      free(ca_mem->dt_mem); ca_mem->dt_mem = NULL;
      free(ca_mem); ca_mem = NULL;
      cvProcessError(cv_mem, CV_MEM_FAIL, "CVODEA", "CVodeAdjInit", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }
  }

  /* Attach functions for the appropriate interpolation module */
  
  switch(interp) {

  case CV_HERMITE:
    
    ca_mem->ca_IMmalloc = CVAhermiteMalloc;
    ca_mem->ca_IMfree   = CVAhermiteFree;
    ca_mem->ca_IMget    = CVAhermiteGetY;
    ca_mem->ca_IMstore  = CVAhermiteStorePnt;

    break;
    
  case CV_POLYNOMIAL:
  
    ca_mem->ca_IMmalloc = CVApolynomialMalloc;
    ca_mem->ca_IMfree   = CVApolynomialFree;
    ca_mem->ca_IMget    = CVApolynomialGetY;
    ca_mem->ca_IMstore  = CVApolynomialStorePnt;

    break;

  }

  /* The interpolation module has not been initialized yet */

  ca_mem->ca_IMmallocDone = SUNFALSE;

  /* By default we will store but not interpolate sensitivities
   *  - IMstoreSensi will be set in CVodeF to SUNFALSE if FSA is not enabled
   *    or if the user can force this through CVodeSetAdjNoSensi 
   *  - IMinterpSensi will be set in CVodeB to SUNTRUE if IMstoreSensi is
   *    SUNTRUE and if at least one backward problem requires sensitivities */

  ca_mem->ca_IMstoreSensi = SUNTRUE;
  ca_mem->ca_IMinterpSensi = SUNFALSE;

  /* ------------------------------------
   * Initialize list of backward problems
   * ------------------------------------ */

  ca_mem->cvB_mem = NULL;
  ca_mem->ca_bckpbCrt = NULL;
  ca_mem->ca_nbckpbs = 0;

  /* --------------------------------
   * CVodeF and CVodeB not called yet
   * -------------------------------- */

  ca_mem->ca_firstCVodeFcall = SUNTRUE;
  ca_mem->ca_tstopCVodeFcall = SUNFALSE;

  ca_mem->ca_firstCVodeBcall = SUNTRUE;

  /* ---------------------------------------------
   * ASA initialized and allocated
   * --------------------------------------------- */

  cv_mem->cv_adj = SUNTRUE;
  cv_mem->cv_adjMallocDone = SUNTRUE;

  return(CV_SUCCESS);
} 

/* CVodeAdjReInit
 *
 * This routine reinitializes the CVODEA memory structure assuming that the
 * the number of steps between check points and the type of interpolation
 * remain unchanged.
 * The list of check points (and associated memory) is deleted.
 * The list of backward problems is kept (however, new backward problems can 
 * be added to this list by calling CVodeCreateB).
 * The CVODES memory for the forward and backward problems can be reinitialized
 * separately by calling CVodeReInit and CVodeReInitB, respectively.
 * NOTE: if a completely new list of backward problems is also needed, then
 *       simply free the adjoint memory (by calling CVodeAdjFree) and reinitialize
 *       ASA with CVodeAdjInit.
 */

int CVodeAdjReInit(void *cvode_mem)
{
  CVadjMem ca_mem;
  CVodeMem cv_mem;

  /* Check cvode_mem */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeAdjReInit", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeAdjReInit", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 

  ca_mem = cv_mem->cv_adj_mem;

  /* Free current list of Check Points */

  while (ca_mem->ck_mem != NULL) CVAckpntDelete(&(ca_mem->ck_mem));

  /* Initialization of check points */
  
  ca_mem->ck_mem = NULL;
  ca_mem->ca_nckpnts = 0;
  ca_mem->ca_ckpntData = NULL;

  /* CVodeF and CVodeB not called yet */
 
  ca_mem->ca_firstCVodeFcall = SUNTRUE;
  ca_mem->ca_tstopCVodeFcall = SUNFALSE;
  ca_mem->ca_firstCVodeBcall = SUNTRUE;

  return(CV_SUCCESS);
}

/*
 * CVodeAdjFree
 *
 * This routine frees the memory allocated by CVodeAdjInit.
 */

void CVodeAdjFree(void *cvode_mem)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  long int i;
  
  if (cvode_mem == NULL) return;
  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_adjMallocDone) {

    ca_mem = cv_mem->cv_adj_mem;

    /* Delete check points one by one */
    while (ca_mem->ck_mem != NULL) CVAckpntDelete(&(ca_mem->ck_mem));

    /* Free vectors at all data points */
    if (ca_mem->ca_IMmallocDone) {
      ca_mem->ca_IMfree(cv_mem);
    }
    for(i=0; i<=ca_mem->ca_nsteps; i++) {
      free(ca_mem->dt_mem[i]);
      ca_mem->dt_mem[i] = NULL;
    }
    free(ca_mem->dt_mem);
    ca_mem->dt_mem = NULL;

    /* Delete backward problems one by one */
    while (ca_mem->cvB_mem != NULL) CVAbckpbDelete(&(ca_mem->cvB_mem));

    /* Free CVODEA memory */
    free(ca_mem);
    cv_mem->cv_adj_mem = NULL;

  }

}

/*
 * CVodeF
 *
 * This routine integrates to tout and returns solution into yout.
 * In the same time, it stores check point data every 'steps' steps. 
 * 
 * CVodeF can be called repeatedly by the user.
 *
 * ncheckPtr points to the number of check points stored so far.
 */

int CVodeF(void *cvode_mem, realtype tout, N_Vector yout, 
           realtype *tret, int itask, int *ncheckPtr)
{
  CVadjMem ca_mem;
  CVodeMem cv_mem;
  CkpntMem tmp;
  DtpntMem *dt_mem;
  int flag, i;
  booleantype iret, allocOK;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeF", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeF", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 

  ca_mem = cv_mem->cv_adj_mem;

  /* Check for yout != NULL */
  if (yout == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeF", MSGCV_YOUT_NULL);
    return(CV_ILL_INPUT);
  }
  
  /* Check for tret != NULL */
  if (tret == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeF", MSGCV_TRET_NULL);
    return(CV_ILL_INPUT);
  }

  /* Check for valid itask */
  if ( (itask != CV_NORMAL) && (itask != CV_ONE_STEP) ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeF", MSGCV_BAD_ITASK);
    return(CV_ILL_INPUT);
  }

  /* All error checking done */

  dt_mem = ca_mem->dt_mem;

  /* If tstop is enabled, store some info */
  if (cv_mem->cv_tstopset) {
    ca_mem->ca_tstopCVodeFcall = SUNTRUE;
    ca_mem->ca_tstopCVodeF = cv_mem->cv_tstop;
  }

  /* We will call CVode in CV_ONE_STEP mode, regardless
   * of what itask is, so flag if we need to return */
  if (itask == CV_ONE_STEP) iret = SUNTRUE;
  else                      iret = SUNFALSE;

  /* On the first step:
   *   - set tinitial
   *   - initialize list of check points
   *   - if needed, initialize the interpolation module
   *   - load dt_mem[0]
   * On subsequent steps, test if taking a new step is necessary. 
   */
  if ( ca_mem->ca_firstCVodeFcall ) {

    ca_mem->ca_tinitial = cv_mem->cv_tn;

    ca_mem->ck_mem = CVAckpntInit(cv_mem);
    if (ca_mem->ck_mem == NULL) {
      cvProcessError(cv_mem, CV_MEM_FAIL, "CVODEA", "CVodeF", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    if ( !ca_mem->ca_IMmallocDone ) {

      /* Do we need to store sensitivities? */
      if (!cv_mem->cv_sensi) ca_mem->ca_IMstoreSensi = SUNFALSE;

      /* Allocate space for interpolation data */
      allocOK = ca_mem->ca_IMmalloc(cv_mem);
      if (!allocOK) {
        cvProcessError(cv_mem, CV_MEM_FAIL, "CVODEA", "CVodeF", MSGCV_MEM_FAIL);
        return(CV_MEM_FAIL);
      }

      /* Rename zn and, if needed, znS for use in interpolation */
      for (i=0;i<L_MAX;i++) ca_mem->ca_Y[i] = cv_mem->cv_zn[i];
      if (ca_mem->ca_IMstoreSensi) {
        for (i=0;i<L_MAX;i++) ca_mem->ca_YS[i] = cv_mem->cv_znS[i];
      }

      ca_mem->ca_IMmallocDone = SUNTRUE;

    }

    dt_mem[0]->t = ca_mem->ck_mem->ck_t0;
    ca_mem->ca_IMstore(cv_mem, dt_mem[0]);

    ca_mem->ca_firstCVodeFcall = SUNFALSE;

  } else if ( (cv_mem->cv_tn - tout)*cv_mem->cv_h >= ZERO ) {

    /* If tout was passed, return interpolated solution. 
       No changes to ck_mem or dt_mem are needed. */
    *tret = tout;
    flag = CVodeGetDky(cv_mem, tout, 0, yout);
    *ncheckPtr = ca_mem->ca_nckpnts;
    ca_mem->ca_IMnewData = SUNTRUE;
    ca_mem->ca_ckpntData = ca_mem->ck_mem;
    ca_mem->ca_np = cv_mem->cv_nst % ca_mem->ca_nsteps + 1;

    return(flag);

  }

  /* Integrate to tout (in CV_ONE_STEP mode) while loading check points */
  for(;;) {

    /* Perform one step of the integration */

    flag = CVode(cv_mem, tout, yout, tret, CV_ONE_STEP);
    if (flag < 0) break;

    /* Test if a new check point is needed */

    if ( cv_mem->cv_nst % ca_mem->ca_nsteps == 0 ) {

      ca_mem->ck_mem->ck_t1 = *tret;

      /* Create a new check point, load it, and append it to the list */
      tmp = CVAckpntNew(cv_mem);
      if (tmp == NULL) {
        cvProcessError(cv_mem, CV_MEM_FAIL, "CVODEA", "CVodeF", MSGCV_MEM_FAIL);
        flag = CV_MEM_FAIL;
        break;
      }
      tmp->ck_next = ca_mem->ck_mem;
      ca_mem->ck_mem = tmp;
      ca_mem->ca_nckpnts++;
      cv_mem->cv_forceSetup = SUNTRUE;
      
      /* Reset i=0 and load dt_mem[0] */
      dt_mem[0]->t = ca_mem->ck_mem->ck_t0;
      ca_mem->ca_IMstore(cv_mem, dt_mem[0]);

    } else {

      /* Load next point in dt_mem */
      dt_mem[cv_mem->cv_nst % ca_mem->ca_nsteps]->t = *tret;
      ca_mem->ca_IMstore(cv_mem, dt_mem[cv_mem->cv_nst % ca_mem->ca_nsteps]);

    }

    /* Set t1 field of the current ckeck point structure
       for the case in which there will be no future
       check points */
    ca_mem->ck_mem->ck_t1 = *tret;

    /* tfinal is now set to *tret */
    ca_mem->ca_tfinal = *tret;

    /* Return if in CV_ONE_STEP mode */
    if (iret) break;

    /* Return if tout reached */
    if ( (*tret - tout)*cv_mem->cv_h >= ZERO ) {
      *tret = tout;
      CVodeGetDky(cv_mem, tout, 0, yout);
      /* Reset tretlast in cv_mem so that CVodeGetQuad and CVodeGetSens 
       * evaluate quadratures and/or sensitivities at the proper time */
      cv_mem->cv_tretlast = tout;
      break;
    }

  } /* end of for(;;)() */

  /* Get ncheck from ca_mem */ 
  *ncheckPtr = ca_mem->ca_nckpnts;

  /* Data is available for the last interval */
  ca_mem->ca_IMnewData = SUNTRUE;
  ca_mem->ca_ckpntData = ca_mem->ck_mem;
  ca_mem->ca_np = cv_mem->cv_nst % ca_mem->ca_nsteps + 1;

  return(flag);
}



/* 
 * =================================================================
 * FUNCTIONS FOR BACKWARD PROBLEMS
 * =================================================================
 */


int CVodeCreateB(void *cvode_mem, int lmmB, int *which)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem new_cvB_mem;
  void *cvodeB_mem;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeCreateB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeCreateB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  }
  ca_mem = cv_mem->cv_adj_mem;

  /* Allocate space for new CVodeBMem object */

  new_cvB_mem = NULL;
  new_cvB_mem = (CVodeBMem) malloc(sizeof(struct CVodeBMemRec));
  if (new_cvB_mem == NULL) {
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODEA", "CVodeCreateB", MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }

  /* Create and set a new CVODES object for the backward problem */

  cvodeB_mem = CVodeCreate(lmmB);
  if (cvodeB_mem == NULL) {
    cvProcessError(cv_mem, CV_MEM_FAIL, "CVODEA", "CVodeCreateB", MSGCV_MEM_FAIL);
    return(CV_MEM_FAIL);
  }

  CVodeSetUserData(cvodeB_mem, cvode_mem);

  CVodeSetMaxHnilWarns(cvodeB_mem, -1);

  CVodeSetErrHandlerFn(cvodeB_mem, cv_mem->cv_ehfun, cv_mem->cv_eh_data);
  CVodeSetErrFile(cvodeB_mem, cv_mem->cv_errfp);

  /* Set/initialize fields in the new CVodeBMem object, new_cvB_mem */

  new_cvB_mem->cv_index   = ca_mem->ca_nbckpbs;

  new_cvB_mem->cv_mem     = (CVodeMem) cvodeB_mem;

  new_cvB_mem->cv_f       = NULL;
  new_cvB_mem->cv_fs      = NULL;

  new_cvB_mem->cv_fQ      = NULL;
  new_cvB_mem->cv_fQs     = NULL;

  new_cvB_mem->cv_user_data  = NULL;

  new_cvB_mem->cv_lmem    = NULL;
  new_cvB_mem->cv_lfree   = NULL;
  new_cvB_mem->cv_pmem    = NULL;
  new_cvB_mem->cv_pfree   = NULL;

  new_cvB_mem->cv_y       = NULL;

  new_cvB_mem->cv_f_withSensi = SUNFALSE;
  new_cvB_mem->cv_fQ_withSensi = SUNFALSE;

  /* Attach the new object to the linked list cvB_mem */

  new_cvB_mem->cv_next = ca_mem->cvB_mem;
  ca_mem->cvB_mem = new_cvB_mem;
  
  /* Return the index of the newly created CVodeBMem object.
   * This must be passed to CVodeInitB and to other ***B 
   * functions to set optional inputs for this backward problem */

  *which = ca_mem->ca_nbckpbs;

  ca_mem->ca_nbckpbs++;

  return(CV_SUCCESS);
}

int CVodeInitB(void *cvode_mem, int which, 
               CVRhsFnB fB,
               realtype tB0, N_Vector yB0)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeInitB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */

  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeInitB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check the value of which */

  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeInitB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */

  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);
  
  /* Allocate and set the CVODES object */

  flag = CVodeInit(cvodeB_mem, CVArhs, tB0, yB0);

  if (flag != CV_SUCCESS) return(flag);

  /* Copy fB function in cvB_mem */

  cvB_mem->cv_f_withSensi = SUNFALSE;
  cvB_mem->cv_f = fB;

  /* Allocate space and initialize the y Nvector in cvB_mem */

  cvB_mem->cv_t0 = tB0;
  cvB_mem->cv_y = N_VClone(yB0);
  N_VScale(ONE, yB0, cvB_mem->cv_y);

  return(CV_SUCCESS);
}

int CVodeInitBS(void *cvode_mem, int which, 
                CVRhsFnBS fBs,
                realtype tB0, N_Vector yB0)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeInitBS", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */

  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeInitBS", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check the value of which */

  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeInitBS", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */

  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);
  
  /* Allocate and set the CVODES object */

  flag = CVodeInit(cvodeB_mem, CVArhs, tB0, yB0);

  if (flag != CV_SUCCESS) return(flag);

  /* Copy fBs function in cvB_mem */

  cvB_mem->cv_f_withSensi = SUNTRUE;
  cvB_mem->cv_fs = fBs;

  /* Allocate space and initialize the y Nvector in cvB_mem */

  cvB_mem->cv_t0 = tB0;
  cvB_mem->cv_y = N_VClone(yB0);
  N_VScale(ONE, yB0, cvB_mem->cv_y);

  return(CV_SUCCESS);
}


int CVodeReInitB(void *cvode_mem, int which,
                 realtype tB0, N_Vector yB0)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeReInitB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeReInitB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  }
  ca_mem = cv_mem->cv_adj_mem;

  /* Check the value of which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeReInitB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  /* Reinitialize CVODES object */

  flag = CVodeReInit(cvodeB_mem, tB0, yB0);

  return(flag);
}


int CVodeSStolerancesB(void *cvode_mem, int which, realtype reltolB, realtype abstolB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeSStolerancesB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */

  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeSStolerancesB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check the value of which */

  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeSStolerancesB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */

  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  /* Set tolerances */

  flag = CVodeSStolerances(cvodeB_mem, reltolB, abstolB);

  return(flag);
}


int CVodeSVtolerancesB(void *cvode_mem, int which, realtype reltolB, N_Vector abstolB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeSVtolerancesB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */

  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeSVtolerancesB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check the value of which */

  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeSVtolerancesB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */

  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  /* Set tolerances */

  flag = CVodeSVtolerances(cvodeB_mem, reltolB, abstolB);

  return(flag);
}


int CVodeQuadInitB(void *cvode_mem, int which,
                     CVQuadRhsFnB fQB, N_Vector yQB0)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeQuadInitB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeQuadInitB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeQuadInitB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  flag = CVodeQuadInit(cvodeB_mem, CVArhsQ, yQB0);
  if (flag != CV_SUCCESS) return(flag);

  cvB_mem->cv_fQ_withSensi = SUNFALSE;
  cvB_mem->cv_fQ = fQB;

  return(CV_SUCCESS);
}

int CVodeQuadInitBS(void *cvode_mem, int which,
                      CVQuadRhsFnBS fQBs, N_Vector yQB0)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeQuadInitBS", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeQuadInitBS", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeQuadInitBS", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  flag = CVodeQuadInit(cvodeB_mem, CVArhsQ, yQB0);
  if (flag != CV_SUCCESS) return(flag);

  cvB_mem->cv_fQ_withSensi = SUNTRUE;
  cvB_mem->cv_fQs = fQBs;

  return(CV_SUCCESS);
}

int CVodeQuadReInitB(void *cvode_mem, int which, N_Vector yQB0)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeQuadReInitB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeQuadReInitB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check the value of which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeQuadReInitB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  flag = CVodeQuadReInit(cvodeB_mem, yQB0);
  if (flag != CV_SUCCESS) return(flag);

  return(CV_SUCCESS);
}

int CVodeQuadSStolerancesB(void *cvode_mem, int which, realtype reltolQB, realtype abstolQB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeQuadSStolerancesB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeQuadSStolerancesB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeQuadSStolerancesB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  flag = CVodeQuadSStolerances(cvodeB_mem, reltolQB, abstolQB);

  return(flag);
}

int CVodeQuadSVtolerancesB(void *cvode_mem, int which, realtype reltolQB, N_Vector abstolQB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeQuadSStolerancesB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeQuadSStolerancesB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 
  ca_mem = cv_mem->cv_adj_mem;

  /* Check which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeQuadSStolerancesB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  }

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  flag = CVodeQuadSVtolerances(cvodeB_mem, reltolQB, abstolQB);

  return(flag);
}

/*
 * CVodeB
 *
 * This routine performs the backward integration towards tBout
 * of all backward problems that were defined.
 * When necessary, it performs a forward integration between two 
 * consecutive check points to update interpolation data.
 *
 * On a successful return, CVodeB returns CV_SUCCESS.
 *
 * NOTE that CVodeB DOES NOT return the solution for the backward
 * problem(s). Use CVodeGetB to extract the solution at tBret
 * for any given backward problem.
 *
 * If there are multiple backward problems and multiple check points,
 * CVodeB may not succeed in getting all problems to take one step
 * when called in ONE_STEP mode.
 */

int CVodeB(void *cvode_mem, realtype tBout, int itaskB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem, tmp_cvB_mem;
  CkpntMem ck_mem;
  int sign, flag=0;
  realtype tfuzz, tBret, tBn;
  booleantype gotCheckpoint, isActive, reachedTBout;
  
  /* Check if cvode_mem exists */

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */

  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  }
  ca_mem = cv_mem->cv_adj_mem;

  /* Check if any backward problem has been defined */

  if ( ca_mem->ca_nbckpbs == 0 ) {
    cvProcessError(cv_mem, CV_NO_BCK, "CVODEA", "CVodeB", MSGCV_NO_BCK);
    return(CV_NO_BCK);
  }
  cvB_mem = ca_mem->cvB_mem;

  /* Check whether CVodeF has been called */

  if ( ca_mem->ca_firstCVodeFcall ) {
    cvProcessError(cv_mem, CV_NO_FWD, "CVODEA", "CVodeB", MSGCV_NO_FWD);
    return(CV_NO_FWD);
  }
  sign = (ca_mem->ca_tfinal - ca_mem->ca_tinitial > ZERO) ? 1 : -1;

  /* If this is the first call, loop over all backward problems and
   *   - check that tB0 is valid
   *   - check that tBout is ahead of tB0 in the backward direction
   *   - check whether we need to interpolate forward sensitivities
   */

  if ( ca_mem->ca_firstCVodeBcall ) {

    tmp_cvB_mem = cvB_mem;

    while(tmp_cvB_mem != NULL) {

      tBn = tmp_cvB_mem->cv_mem->cv_tn;

      if ( (sign*(tBn-ca_mem->ca_tinitial) < ZERO) || (sign*(ca_mem->ca_tfinal-tBn) < ZERO) ) {
        cvProcessError(cv_mem, CV_BAD_TB0, "CVODEA", "CVodeB", MSGCV_BAD_TB0,
                       tmp_cvB_mem->cv_index);
        return(CV_BAD_TB0);
      }

      if (sign*(tBn-tBout) <= ZERO) {
        cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeB", MSGCV_BAD_TBOUT,
                       tmp_cvB_mem->cv_index);
        return(CV_ILL_INPUT);
      }

      if ( tmp_cvB_mem->cv_f_withSensi || tmp_cvB_mem->cv_fQ_withSensi )
          ca_mem->ca_IMinterpSensi = SUNTRUE;

      tmp_cvB_mem = tmp_cvB_mem->cv_next;

    }

    if ( ca_mem->ca_IMinterpSensi && !ca_mem->ca_IMstoreSensi) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeB", MSGCV_BAD_SENSI);
      return(CV_ILL_INPUT);
    }

    ca_mem->ca_firstCVodeBcall = SUNFALSE;
  }

  /* Check if itaskB is legal */

  if ( (itaskB != CV_NORMAL) && (itaskB != CV_ONE_STEP) ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeB", MSGCV_BAD_ITASKB);
    return(CV_ILL_INPUT);
  }

  /* Check if tBout is legal */

  if ( (sign*(tBout-ca_mem->ca_tinitial) < ZERO) || (sign*(ca_mem->ca_tfinal-tBout) < ZERO) ) {
    tfuzz = HUNDRED*cv_mem->cv_uround*(SUNRabs(ca_mem->ca_tinitial) + SUNRabs(ca_mem->ca_tfinal));
    if ( (sign*(tBout-ca_mem->ca_tinitial) < ZERO) && (SUNRabs(tBout-ca_mem->ca_tinitial) < tfuzz) ) {
      tBout = ca_mem->ca_tinitial;
    } else {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeB", MSGCV_BAD_TBOUT);
      return(CV_ILL_INPUT);
    }
  }

  /* Loop through the check points and stop as soon as a backward
   * problem has its tn value behind the current check point's t0_
   * value (in the backward direction) */

  ck_mem = ca_mem->ck_mem;

  gotCheckpoint = SUNFALSE;

  for(;;) {

    tmp_cvB_mem = cvB_mem;
    while(tmp_cvB_mem != NULL) {
      tBn = tmp_cvB_mem->cv_mem->cv_tn;

      if ( sign*(tBn-ck_mem->ck_t0) > ZERO ) {
        gotCheckpoint = SUNTRUE;
        break;
      }

      if ( (itaskB==CV_NORMAL) && (tBn == ck_mem->ck_t0) && (sign*(tBout-ck_mem->ck_t0) >= ZERO) ) {
        gotCheckpoint = SUNTRUE;
        break;
      }

      tmp_cvB_mem = tmp_cvB_mem->cv_next;
    }

    if (gotCheckpoint) break;

    if (ck_mem->ck_next == NULL) break;

    ck_mem = ck_mem->ck_next;
  }

  /* Starting with the current check point from above, loop over check points
     while propagating backward problems */

  for(;;) {

    /* Store interpolation data if not available.
       This is the 2nd forward integration pass */

    if (ck_mem != ca_mem->ca_ckpntData) {
      flag = CVAdataStore(cv_mem, ck_mem);
      if (flag != CV_SUCCESS) break;
    }

    /* Loop through all backward problems and, if needed,
     * propagate their solution towards tBout */

    tmp_cvB_mem = cvB_mem;
    while (tmp_cvB_mem != NULL) {

      /* Decide if current backward problem is "active" in this check point */

      isActive = SUNTRUE;

      tBn = tmp_cvB_mem->cv_mem->cv_tn;

      if ( (tBn == ck_mem->ck_t0) && (sign*(tBout-ck_mem->ck_t0) < ZERO ) ) isActive = SUNFALSE;
      if ( (tBn == ck_mem->ck_t0) && (itaskB==CV_ONE_STEP) ) isActive = SUNFALSE;

      if ( sign * (tBn - ck_mem->ck_t0) < ZERO ) isActive = SUNFALSE;

      if ( isActive ) {

        /* Store the address of current backward problem memory 
         * in ca_mem to be used in the wrapper functions */
        ca_mem->ca_bckpbCrt = tmp_cvB_mem;

        /* Integrate current backward problem */
        CVodeSetStopTime(tmp_cvB_mem->cv_mem, ck_mem->ck_t0);
        flag = CVode(tmp_cvB_mem->cv_mem, tBout, tmp_cvB_mem->cv_y, &tBret, itaskB);

        /* Set the time at which we will report solution and/or quadratures */
        tmp_cvB_mem->cv_tout = tBret;

        /* If an error occurred, exit while loop */
        if (flag < 0) break;

      } else {
        flag = CV_SUCCESS;
        tmp_cvB_mem->cv_tout = tBn;
      }

      /* Move to next backward problem */

      tmp_cvB_mem = tmp_cvB_mem->cv_next;
    }

    /* If an error occurred, return now */

    if (flag <0) {
      cvProcessError(cv_mem, flag, "CVODEA", "CVodeB", MSGCV_BACK_ERROR,
                     tmp_cvB_mem->cv_index);
      return(flag);
    }

    /* If in CV_ONE_STEP mode, return now (flag = CV_SUCCESS) */

    if (itaskB == CV_ONE_STEP) break;

    /* If all backward problems have succesfully reached tBout, return now */

    reachedTBout = SUNTRUE;

    tmp_cvB_mem = cvB_mem;
    while(tmp_cvB_mem != NULL) {
      if ( sign*(tmp_cvB_mem->cv_tout - tBout) > ZERO ) {
        reachedTBout = SUNFALSE;
        break;
      }
      tmp_cvB_mem = tmp_cvB_mem->cv_next;
    }

    if ( reachedTBout ) break;

    /* Move check point in linked list to next one */

    ck_mem = ck_mem->ck_next;

  } 

  return(flag);
}


int CVodeGetB(void *cvode_mem, int which, realtype *tret, N_Vector yB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeGetB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeGetB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 

  ca_mem = cv_mem->cv_adj_mem;

  /* Check the value of which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeGetB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  } 

  N_VScale(ONE, cvB_mem->cv_y, yB);
  *tret = cvB_mem->cv_tout;

  return(CV_SUCCESS);
}


/*
 * CVodeGetQuadB
 */

int CVodeGetQuadB(void *cvode_mem, int which, realtype *tret, N_Vector qB)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  void *cvodeB_mem;
  long int nstB;
  int flag;

  /* Check if cvode_mem exists */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeGetQuadB", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Was ASA initialized? */
  if (cv_mem->cv_adjMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_ADJ, "CVODEA", "CVodeGetQuadB", MSGCV_NO_ADJ);
    return(CV_NO_ADJ);
  } 

  ca_mem = cv_mem->cv_adj_mem;

  /* Check the value of which */
  if ( which >= ca_mem->ca_nbckpbs ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODEA", "CVodeGetQuadB", MSGCV_BAD_WHICH);
    return(CV_ILL_INPUT);
  }

  /* Find the CVodeBMem entry in the linked list corresponding to which */
  cvB_mem = ca_mem->cvB_mem;
  while (cvB_mem != NULL) {
    if ( which == cvB_mem->cv_index ) break;
    cvB_mem = cvB_mem->cv_next;
  } 

  cvodeB_mem = (void *) (cvB_mem->cv_mem);

  /* If the integration for this backward problem has not started yet,
   * simply return the current value of qB (i.e. the final conditions) */

  flag = CVodeGetNumSteps(cvodeB_mem, &nstB);
  
  if (nstB == 0) {
    N_VScale(ONE, cvB_mem->cv_mem->cv_znQ[0], qB);
    *tret = cvB_mem->cv_tout;
  } else {
    flag = CVodeGetQuad(cvodeB_mem, tret, qB);
  }

  return(flag);
}


/* 
 * =================================================================
 * PRIVATE FUNCTIONS FOR CHECK POINTS
 * =================================================================
 */

/*
 * CVAckpntInit
 *
 * This routine initializes the check point linked list with 
 * information from the initial time.
 */

static CkpntMem CVAckpntInit(CVodeMem cv_mem)
{
  CkpntMem ck_mem;
  int is;

  /* Allocate space for ckdata */
  ck_mem = NULL;
  ck_mem = (CkpntMem) malloc(sizeof(struct CkpntMemRec));
  if (ck_mem == NULL) return(NULL);

  ck_mem->ck_zn[0] = N_VClone(cv_mem->cv_tempv);
  if (ck_mem->ck_zn[0] == NULL) {
    free(ck_mem); ck_mem = NULL;
    return(NULL);
  }
  
  ck_mem->ck_zn[1] = N_VClone(cv_mem->cv_tempv);
  if (ck_mem->ck_zn[1] == NULL) {
    N_VDestroy(ck_mem->ck_zn[0]);
    free(ck_mem); ck_mem = NULL;
    return(NULL);
  }

  /* ck_mem->ck_zn[qmax] was not allocated */
  ck_mem->ck_zqm = 0;

  /* Load ckdata from cv_mem */
  N_VScale(ONE, cv_mem->cv_zn[0], ck_mem->ck_zn[0]);
  ck_mem->ck_t0    = cv_mem->cv_tn;
  ck_mem->ck_nst   = 0;
  ck_mem->ck_q     = 1;
  ck_mem->ck_h     = 0.0;
  
  /* Do we need to carry quadratures */
  ck_mem->ck_quadr = cv_mem->cv_quadr && cv_mem->cv_errconQ;

  if (ck_mem->ck_quadr) {

    ck_mem->ck_znQ[0] = N_VClone(cv_mem->cv_tempvQ);
    if (ck_mem->ck_znQ[0] == NULL) {
      N_VDestroy(ck_mem->ck_zn[0]);
      N_VDestroy(ck_mem->ck_zn[1]);
      free(ck_mem); ck_mem = NULL;
      return(NULL);
    }

    N_VScale(ONE, cv_mem->cv_znQ[0], ck_mem->ck_znQ[0]);

  }

  /* Do we need to carry sensitivities? */
  ck_mem->ck_sensi = cv_mem->cv_sensi;

  if (ck_mem->ck_sensi) {

    ck_mem->ck_Ns = cv_mem->cv_Ns;

    ck_mem->ck_znS[0] = N_VCloneVectorArray(cv_mem->cv_Ns, cv_mem->cv_tempv);
    if (ck_mem->ck_znS[0] == NULL) {
      N_VDestroy(ck_mem->ck_zn[0]);
      N_VDestroy(ck_mem->ck_zn[1]);
      if (ck_mem->ck_quadr) N_VDestroy(ck_mem->ck_znQ[0]);
      free(ck_mem); ck_mem = NULL;
      return(NULL);
    }

    for (is=0; is<cv_mem->cv_Ns; is++)
      cv_mem->cv_cvals[is] = ONE;

    (void) N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                               cv_mem->cv_znS[0], ck_mem->ck_znS[0]);
  }

  /* Do we need to carry quadrature sensitivities? */
  ck_mem->ck_quadr_sensi = cv_mem->cv_quadr_sensi && cv_mem->cv_errconQS;

  if (ck_mem->ck_quadr_sensi) {
    ck_mem->ck_znQS[0] = N_VCloneVectorArray(cv_mem->cv_Ns, cv_mem->cv_tempvQ);
    if (ck_mem->ck_znQS[0] == NULL) {
      N_VDestroy(ck_mem->ck_zn[0]);
      N_VDestroy(ck_mem->ck_zn[1]);
      if (ck_mem->ck_quadr) N_VDestroy(ck_mem->ck_znQ[0]);
      N_VDestroyVectorArray(ck_mem->ck_znS[0], cv_mem->cv_Ns);
      free(ck_mem); ck_mem = NULL;
      return(NULL);
    }
    
    for (is=0; is<cv_mem->cv_Ns; is++)
      cv_mem->cv_cvals[is] = ONE;

    (void) N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                               cv_mem->cv_znQS[0], ck_mem->ck_znQS[0]);
  }

  /* Next in list */
  ck_mem->ck_next  = NULL;

  return(ck_mem);
}

/*
 * CVAckpntNew
 *
 * This routine allocates space for a new check point and sets 
 * its data from current values in cv_mem.
 */

static CkpntMem CVAckpntNew(CVodeMem cv_mem)
{
  CkpntMem ck_mem;
  int j, jj, is, qmax;

  /* Allocate space for ckdata */
  ck_mem = NULL;
  ck_mem = (CkpntMem) malloc(sizeof(struct CkpntMemRec));
  if (ck_mem == NULL) return(NULL);

  /* Set cv_next to NULL */
  ck_mem->ck_next = NULL;

  /* Test if we need to allocate space for the last zn.
   * NOTE: zn(qmax) may be needed for a hot restart, if an order
   * increase is deemed necessary at the first step after a check point */
  qmax = cv_mem->cv_qmax;
  ck_mem->ck_zqm = (cv_mem->cv_q < qmax) ? qmax : 0;

  for (j=0; j<=cv_mem->cv_q; j++) {
    ck_mem->ck_zn[j] = N_VClone(cv_mem->cv_tempv);
    if (ck_mem->ck_zn[j] == NULL) {
      for (jj=0; jj<j; jj++) N_VDestroy(ck_mem->ck_zn[jj]);
      free(ck_mem); ck_mem = NULL;
      return(NULL);
    }
  }

  if (cv_mem->cv_q < qmax) {
    ck_mem->ck_zn[qmax] = N_VClone(cv_mem->cv_tempv);
    if (ck_mem->ck_zn[qmax] == NULL) {
      for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroy(ck_mem->ck_zn[jj]);
      free(ck_mem); ck_mem = NULL;
      return(NULL);
    }
  }

  /* Test if we need to carry quadratures */
  ck_mem->ck_quadr = cv_mem->cv_quadr && cv_mem->cv_errconQ;

  if (ck_mem->ck_quadr) {

    for (j=0; j<=cv_mem->cv_q; j++) {
      ck_mem->ck_znQ[j] = N_VClone(cv_mem->cv_tempvQ);
      if(ck_mem->ck_znQ[j] == NULL) {
        for (jj=0; jj<j; jj++) N_VDestroy(ck_mem->ck_znQ[jj]);
        if (cv_mem->cv_q < qmax) N_VDestroy(ck_mem->ck_zn[qmax]);
        for (jj=0; jj<=cv_mem->cv_q; j++) N_VDestroy(ck_mem->ck_zn[jj]);
        free(ck_mem); ck_mem = NULL;
        return(NULL);
      }
    }

    if (cv_mem->cv_q < qmax) {
      ck_mem->ck_znQ[qmax] = N_VClone(cv_mem->cv_tempvQ);
      if (ck_mem->ck_znQ[qmax] == NULL) {
        for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroy(ck_mem->ck_znQ[jj]);
        N_VDestroy(ck_mem->ck_zn[qmax]);
        for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroy(ck_mem->ck_zn[jj]);
        free(ck_mem); ck_mem = NULL;
        return(NULL);
      }
    }

  }

  /* Test if we need to carry sensitivities */
  ck_mem->ck_sensi = cv_mem->cv_sensi;

  if (ck_mem->ck_sensi) {

    ck_mem->ck_Ns = cv_mem->cv_Ns;

    for (j=0; j<=cv_mem->cv_q; j++) {
      ck_mem->ck_znS[j] = N_VCloneVectorArray(cv_mem->cv_Ns, cv_mem->cv_tempv);
      if (ck_mem->ck_znS[j] == NULL) {
        for (jj=0; jj<j; jj++) N_VDestroyVectorArray(ck_mem->ck_znS[jj], cv_mem->cv_Ns);
        if (ck_mem->ck_quadr) {
          if (cv_mem->cv_q < qmax) N_VDestroy(ck_mem->ck_znQ[qmax]);
          for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroy(ck_mem->ck_znQ[jj]);
        }
        if (cv_mem->cv_q < qmax) N_VDestroy(ck_mem->ck_zn[qmax]);
        for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroy(ck_mem->ck_zn[jj]);
        free(ck_mem); ck_mem = NULL;
        return(NULL);
      }
    }

    if ( cv_mem->cv_q < qmax) {
      ck_mem->ck_znS[qmax] = N_VCloneVectorArray(cv_mem->cv_Ns, cv_mem->cv_tempv);
      if (ck_mem->ck_znS[qmax] == NULL) {
        for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroyVectorArray(ck_mem->ck_znS[jj], cv_mem->cv_Ns);
        if (ck_mem->ck_quadr) {
          N_VDestroy(ck_mem->ck_znQ[qmax]);
          for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroy(ck_mem->ck_znQ[jj]);
        }
        N_VDestroy(ck_mem->ck_zn[qmax]);
        for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroy(ck_mem->ck_zn[jj]);
        free(ck_mem); ck_mem = NULL;
        return(NULL);
      }
    }

  }

  /* Test if we need to carry quadrature sensitivities */
  ck_mem->ck_quadr_sensi = cv_mem->cv_quadr_sensi && cv_mem->cv_errconQS;

  if (ck_mem->ck_quadr_sensi) {

    for (j=0; j<=cv_mem->cv_q; j++) {
      ck_mem->ck_znQS[j] = N_VCloneVectorArray(cv_mem->cv_Ns, cv_mem->cv_tempvQ);
      if (ck_mem->ck_znQS[j] == NULL) {
        for (jj=0; jj<j; jj++) N_VDestroyVectorArray(ck_mem->ck_znQS[jj], cv_mem->cv_Ns);
        if (cv_mem->cv_q < qmax) N_VDestroyVectorArray(ck_mem->ck_znS[qmax], cv_mem->cv_Ns);
        for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroyVectorArray(ck_mem->ck_znS[jj], cv_mem->cv_Ns);
        if (ck_mem->ck_quadr) {
          if (cv_mem->cv_q < qmax) N_VDestroy(ck_mem->ck_znQ[qmax]);
          for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroy(ck_mem->ck_znQ[jj]);
        }
        if (cv_mem->cv_q < qmax) N_VDestroy(ck_mem->ck_zn[qmax]);
        for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroy(ck_mem->ck_zn[jj]);
        free(ck_mem); ck_mem = NULL;
        return(NULL);
      }
    }

    if ( cv_mem->cv_q < qmax) {
      ck_mem->ck_znQS[qmax] = N_VCloneVectorArray(cv_mem->cv_Ns, cv_mem->cv_tempvQ);
      if (ck_mem->ck_znQS[qmax] == NULL) {
        for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroyVectorArray(ck_mem->ck_znQS[jj], cv_mem->cv_Ns);
        N_VDestroyVectorArray(ck_mem->ck_znS[qmax], cv_mem->cv_Ns);
        for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroyVectorArray(ck_mem->ck_znS[jj], cv_mem->cv_Ns);
        if (ck_mem->ck_quadr) {
          N_VDestroy(ck_mem->ck_znQ[qmax]);
          for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroy(ck_mem->ck_zn[jj]);
        }
        N_VDestroy(ck_mem->ck_zn[qmax]);
        for (jj=0; jj<=cv_mem->cv_q; jj++) N_VDestroy(ck_mem->ck_zn[jj]);
        free(ck_mem); ck_mem = NULL;
        return(NULL);
      }
    }

  }

  /* Load check point data from cv_mem */

  for (j=0; j<=cv_mem->cv_q; j++)
    cv_mem->cv_cvals[j] = ONE;

  (void) N_VScaleVectorArray(cv_mem->cv_q+1, cv_mem->cv_cvals,
                             cv_mem->cv_zn, ck_mem->ck_zn);

  if ( cv_mem->cv_q < qmax )
    N_VScale(ONE, cv_mem->cv_zn[qmax], ck_mem->ck_zn[qmax]);

  if (ck_mem->ck_quadr) {
    for (j=0; j<=cv_mem->cv_q; j++)
      cv_mem->cv_cvals[j] = ONE;

    (void) N_VScaleVectorArray(cv_mem->cv_q+1, cv_mem->cv_cvals,
                               cv_mem->cv_znQ, ck_mem->ck_znQ);

    if ( cv_mem->cv_q < qmax )
      N_VScale(ONE, cv_mem->cv_znQ[qmax], ck_mem->ck_znQ[qmax]);
  }

  if (ck_mem->ck_sensi) {
    for (j=0; j<=cv_mem->cv_q; j++) {
      for (is=0; is<cv_mem->cv_Ns; is++) {
        cv_mem->cv_cvals[j*cv_mem->cv_Ns+is] = ONE;
        cv_mem->cv_Xvecs[j*cv_mem->cv_Ns+is] = cv_mem->cv_znS[j][is];
        cv_mem->cv_Zvecs[j*cv_mem->cv_Ns+is] = ck_mem->ck_znS[j][is];
      }
    }
    
    (void) N_VScaleVectorArray(cv_mem->cv_Ns*(cv_mem->cv_q+1),
                               cv_mem->cv_cvals,
                               cv_mem->cv_Xvecs, cv_mem->cv_Zvecs);

    if ( cv_mem->cv_q < qmax ) {
      for (is=0; is<cv_mem->cv_Ns; is++)
        cv_mem->cv_cvals[is] = ONE;

      (void) N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                 cv_mem->cv_znS[qmax], ck_mem->ck_znS[qmax]);
    }
  }
  
  if (ck_mem->ck_quadr_sensi) {
    for (j=0; j<=cv_mem->cv_q; j++) {
      for (is=0; is<cv_mem->cv_Ns; is++) {
        cv_mem->cv_cvals[j*cv_mem->cv_Ns+is] = ONE;
        cv_mem->cv_Xvecs[j*cv_mem->cv_Ns+is] = cv_mem->cv_znQS[j][is];
        cv_mem->cv_Zvecs[j*cv_mem->cv_Ns+is] = ck_mem->ck_znQS[j][is];
      }
    }

    (void) N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                               cv_mem->cv_Xvecs, cv_mem->cv_Zvecs);

    if ( cv_mem->cv_q < qmax ) {
      for (is=0; is<cv_mem->cv_Ns; is++)
        cv_mem->cv_cvals[is] = ONE;

      (void) N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                 cv_mem->cv_znQS[qmax], ck_mem->ck_znQS[qmax]);
    }
  }

  for (j=0; j<=L_MAX; j++)        ck_mem->ck_tau[j] = cv_mem->cv_tau[j];
  for (j=0; j<=NUM_TESTS; j++)    ck_mem->ck_tq[j] = cv_mem->cv_tq[j];
  for (j=0; j<=cv_mem->cv_q; j++) ck_mem->ck_l[j] = cv_mem->cv_l[j];
  ck_mem->ck_nst       = cv_mem->cv_nst;
  ck_mem->ck_tretlast  = cv_mem->cv_tretlast;
  ck_mem->ck_q         = cv_mem->cv_q;
  ck_mem->ck_qprime    = cv_mem->cv_qprime;
  ck_mem->ck_qwait     = cv_mem->cv_qwait;
  ck_mem->ck_L         = cv_mem->cv_L;
  ck_mem->ck_gammap    = cv_mem->cv_gammap;
  ck_mem->ck_h         = cv_mem->cv_h;
  ck_mem->ck_hprime    = cv_mem->cv_hprime;
  ck_mem->ck_hscale    = cv_mem->cv_hscale;
  ck_mem->ck_eta       = cv_mem->cv_eta;
  ck_mem->ck_etamax    = cv_mem->cv_etamax;
  ck_mem->ck_t0        = cv_mem->cv_tn;
  ck_mem->ck_saved_tq5 = cv_mem->cv_saved_tq5;

  return(ck_mem);
}

/*
 * CVAckpntDelete
 *
 * This routine deletes the first check point in list and returns
 * the new list head
 */

static void CVAckpntDelete(CkpntMem *ck_memPtr)
{
  CkpntMem tmp;
  int j;

  if (*ck_memPtr == NULL) return;

  /* store head of list */
  tmp = *ck_memPtr;

  /* move head of list */
  *ck_memPtr = (*ck_memPtr)->ck_next;

  /* free N_Vectors in tmp */
  for (j=0;j<=tmp->ck_q;j++) N_VDestroy(tmp->ck_zn[j]);
  if (tmp->ck_zqm != 0) N_VDestroy(tmp->ck_zn[tmp->ck_zqm]);

  /* free N_Vectors for quadratures in tmp 
   * Note that at the check point at t_initial, only znQ_[0] 
   * was allocated */
  if (tmp->ck_quadr) {

    if (tmp->ck_next != NULL) {
      for (j=0;j<=tmp->ck_q;j++) N_VDestroy(tmp->ck_znQ[j]);
      if (tmp->ck_zqm != 0) N_VDestroy(tmp->ck_znQ[tmp->ck_zqm]);
    } else {
      N_VDestroy(tmp->ck_znQ[0]);
    }
    
  }

  /* free N_Vectors for sensitivities in tmp
   * Note that at the check point at t_initial, only znS_[0] 
   * was allocated */
  if (tmp->ck_sensi) {
    
    if (tmp->ck_next != NULL) {
      for (j=0;j<=tmp->ck_q;j++) N_VDestroyVectorArray(tmp->ck_znS[j], tmp->ck_Ns);
      if (tmp->ck_zqm != 0) N_VDestroyVectorArray(tmp->ck_znS[tmp->ck_zqm], tmp->ck_Ns);
    } else {
      N_VDestroyVectorArray(tmp->ck_znS[0], tmp->ck_Ns);
    }
    
  }

  /* free N_Vectors for quadrature sensitivities in tmp
   * Note that at the check point at t_initial, only znQS_[0] 
   * was allocated */
  if (tmp->ck_quadr_sensi) {
    
    if (tmp->ck_next != NULL) {
      for (j=0;j<=tmp->ck_q;j++) N_VDestroyVectorArray(tmp->ck_znQS[j], tmp->ck_Ns);
      if (tmp->ck_zqm != 0) N_VDestroyVectorArray(tmp->ck_znQS[tmp->ck_zqm], tmp->ck_Ns);
    } else {
      N_VDestroyVectorArray(tmp->ck_znQS[0], tmp->ck_Ns);
    }
    
  }

  free(tmp); tmp = NULL;

}

/* 
 * =================================================================
 * PRIVATE FUNCTIONS FOR BACKWARD PROBLEMS
 * =================================================================
 */

static void CVAbckpbDelete(CVodeBMem *cvB_memPtr)
{
  CVodeBMem tmp;
  void *cvode_mem;

  if (*cvB_memPtr != NULL) {

    /* Save head of the list */
    tmp = *cvB_memPtr;

    /* Move head of the list */
    *cvB_memPtr = (*cvB_memPtr)->cv_next;

    /* Free CVODES memory in tmp */
    cvode_mem = (void *)(tmp->cv_mem);
    CVodeFree(&cvode_mem);

    /* Free linear solver memory */
    if (tmp->cv_lfree != NULL) tmp->cv_lfree(tmp);

    /* Free preconditioner memory */
    if (tmp->cv_pfree != NULL) tmp->cv_pfree(tmp);

    /* Free workspace Nvector */
    N_VDestroy(tmp->cv_y);

    free(tmp); tmp = NULL;

  }

}

/* 
 * =================================================================
 * PRIVATE FUNCTIONS FOR INTERPOLATION
 * =================================================================
 */

/*
 * CVAdataStore
 *
 * This routine integrates the forward model starting at the check
 * point ck_mem and stores y and yprime at all intermediate steps.
 *
 * Return values:
 * CV_SUCCESS
 * CV_REIFWD_FAIL
 * CV_FWD_FAIL
 */

static int CVAdataStore(CVodeMem cv_mem, CkpntMem ck_mem)
{
  CVadjMem ca_mem;
  DtpntMem *dt_mem;
  realtype t;
  long int i;
  int flag, sign;

  ca_mem = cv_mem->cv_adj_mem;
  dt_mem = ca_mem->dt_mem;

  /* Initialize cv_mem with data from ck_mem */
  flag = CVAckpntGet(cv_mem, ck_mem);
  if (flag != CV_SUCCESS)
    return(CV_REIFWD_FAIL);

  /* Set first structure in dt_mem[0] */
  dt_mem[0]->t = ck_mem->ck_t0;
  ca_mem->ca_IMstore(cv_mem, dt_mem[0]);

  /* Decide whether TSTOP must be activated */
  if (ca_mem->ca_tstopCVodeFcall) {
    CVodeSetStopTime(cv_mem, ca_mem->ca_tstopCVodeF);
  }

  sign = (ca_mem->ca_tfinal - ca_mem->ca_tinitial > ZERO) ? 1 : -1;


  /* Run CVode to set following structures in dt_mem[i] */
  i = 1;
  do {

    flag = CVode(cv_mem, ck_mem->ck_t1, ca_mem->ca_ytmp, &t, CV_ONE_STEP);
    if (flag < 0) return(CV_FWD_FAIL);

    dt_mem[i]->t = t;
    ca_mem->ca_IMstore(cv_mem, dt_mem[i]);
    i++;

  } while ( sign*(ck_mem->ck_t1 - t) > ZERO );


  ca_mem->ca_IMnewData = SUNTRUE;     /* New data is now available    */
  ca_mem->ca_ckpntData = ck_mem;   /* starting at this check point */
  ca_mem->ca_np = i;               /* and we have this many points */

  return(CV_SUCCESS);
}

/*
 * CVAckpntGet
 *
 * This routine prepares CVODES for a hot restart from
 * the check point ck_mem
 */

static int CVAckpntGet(CVodeMem cv_mem, CkpntMem ck_mem) 
{
  int flag, j, is, qmax, retval;

  if (ck_mem->ck_next == NULL) {

    /* In this case, we just call the reinitialization routine,
     * but make sure we use the same initial stepsize as on 
     * the first run. */

    CVodeSetInitStep(cv_mem, cv_mem->cv_h0u);

    flag = CVodeReInit(cv_mem, ck_mem->ck_t0, ck_mem->ck_zn[0]);
    if (flag != CV_SUCCESS) return(flag);

    if (ck_mem->ck_quadr) {
      flag = CVodeQuadReInit(cv_mem, ck_mem->ck_znQ[0]);
      if (flag != CV_SUCCESS) return(flag);
    }

    if (ck_mem->ck_sensi) {
      flag = CVodeSensReInit(cv_mem, cv_mem->cv_ism, ck_mem->ck_znS[0]);
      if (flag != CV_SUCCESS) return(flag);
    }

    if (ck_mem->ck_quadr_sensi) {
      flag = CVodeQuadSensReInit(cv_mem, ck_mem->ck_znQS[0]);
      if (flag != CV_SUCCESS) return(flag);
    }

  } else {
    
    qmax = cv_mem->cv_qmax;

    /* Copy parameters from check point data structure */

    cv_mem->cv_nst       = ck_mem->ck_nst;
    cv_mem->cv_tretlast  = ck_mem->ck_tretlast;
    cv_mem->cv_q         = ck_mem->ck_q;
    cv_mem->cv_qprime    = ck_mem->ck_qprime;
    cv_mem->cv_qwait     = ck_mem->ck_qwait;
    cv_mem->cv_L         = ck_mem->ck_L;
    cv_mem->cv_gammap    = ck_mem->ck_gammap;
    cv_mem->cv_h         = ck_mem->ck_h;
    cv_mem->cv_hprime    = ck_mem->ck_hprime;
    cv_mem->cv_hscale    = ck_mem->ck_hscale;
    cv_mem->cv_eta       = ck_mem->ck_eta;
    cv_mem->cv_etamax    = ck_mem->ck_etamax;
    cv_mem->cv_tn        = ck_mem->ck_t0;
    cv_mem->cv_saved_tq5 = ck_mem->ck_saved_tq5;
    
    /* Copy the arrays from check point data structure */

    for (j=0; j<=cv_mem->cv_q; j++)
      cv_mem->cv_cvals[j] = ONE;

    retval = N_VScaleVectorArray(cv_mem->cv_q+1, cv_mem->cv_cvals,
                                 ck_mem->ck_zn, cv_mem->cv_zn);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

    if ( cv_mem->cv_q < qmax )
      N_VScale(ONE, ck_mem->ck_zn[qmax], cv_mem->cv_zn[qmax]);

    if (ck_mem->ck_quadr) {
      for (j=0; j<=cv_mem->cv_q; j++)
        cv_mem->cv_cvals[j] = ONE;

      retval = N_VScaleVectorArray(cv_mem->cv_q+1, cv_mem->cv_cvals,
                                   ck_mem->ck_znQ, cv_mem->cv_znQ);
      if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

      if ( cv_mem->cv_q < qmax )
        N_VScale(ONE, ck_mem->ck_znQ[qmax], cv_mem->cv_znQ[qmax]);
    }

    if (ck_mem->ck_sensi) {
      for (j=0; j<=cv_mem->cv_q; j++) {
        for (is=0; is<cv_mem->cv_Ns; is++) {
          cv_mem->cv_cvals[j*cv_mem->cv_Ns+is] = ONE;
          cv_mem->cv_Xvecs[j*cv_mem->cv_Ns+is] = ck_mem->ck_znS[j][is];
          cv_mem->cv_Zvecs[j*cv_mem->cv_Ns+is] = cv_mem->cv_znS[j][is];
        }
      }

      retval = N_VScaleVectorArray(cv_mem->cv_Ns*(cv_mem->cv_q+1),
                                   cv_mem->cv_cvals,
                                   cv_mem->cv_Xvecs, cv_mem->cv_Zvecs);
      if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

      if ( cv_mem->cv_q < qmax ) {
        for (is=0; is<cv_mem->cv_Ns; is++)
          cv_mem->cv_cvals[is] = ONE;

        retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                     ck_mem->ck_znS[qmax], cv_mem->cv_znS[qmax]);
        if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
      }
    }

    if (ck_mem->ck_quadr_sensi) {
      for (j=0; j<=cv_mem->cv_q; j++) {
        for (is=0; is<cv_mem->cv_Ns; is++) {
          cv_mem->cv_cvals[j*cv_mem->cv_Ns+is] = ONE;
          cv_mem->cv_Xvecs[j*cv_mem->cv_Ns+is] = ck_mem->ck_znQS[j][is];
          cv_mem->cv_Zvecs[j*cv_mem->cv_Ns+is] = cv_mem->cv_znQS[j][is];
        }
      }
      
      retval = N_VScaleVectorArray(cv_mem->cv_Ns*(cv_mem->cv_q+1),
                                   cv_mem->cv_cvals,
                                   cv_mem->cv_Xvecs, cv_mem->cv_Zvecs);
      if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

      if ( cv_mem->cv_q < qmax ) {
        for (is=0; is<cv_mem->cv_Ns; is++)
          cv_mem->cv_cvals[is] = ONE;

        retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                     ck_mem->ck_znQS[qmax], cv_mem->cv_znQS[qmax]);
        if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
      }
    }

    for (j=0; j<=L_MAX; j++)        cv_mem->cv_tau[j] = ck_mem->ck_tau[j];
    for (j=0; j<=NUM_TESTS; j++)    cv_mem->cv_tq[j] = ck_mem->ck_tq[j];
    for (j=0; j<=cv_mem->cv_q; j++) cv_mem->cv_l[j] = ck_mem->ck_l[j];
    
    /* Force a call to setup */

    cv_mem->cv_forceSetup = SUNTRUE;

  }

  return(CV_SUCCESS);
}

/* 
 * -----------------------------------------------------------------
 * Functions for interpolation
 * -----------------------------------------------------------------
 */

/*
 * CVAfindIndex
 *
 * Finds the index in the array of data point strctures such that
 *     dt_mem[indx-1].t <= t < dt_mem[indx].t
 * If indx is changed from the previous invocation, then newpoint = SUNTRUE
 *
 * If t is beyond the leftmost limit, but close enough, indx=0.
 *
 * Returns CV_SUCCESS if successful and CV_GETY_BADT if unable to
 * find indx (t is too far beyond limits).
 */

static int CVAfindIndex(CVodeMem cv_mem, realtype t, 
                        long int *indx, booleantype *newpoint)
{
  CVadjMem ca_mem;
  DtpntMem *dt_mem;
  int sign;
  booleantype to_left, to_right;

  ca_mem = cv_mem->cv_adj_mem;
  dt_mem = ca_mem->dt_mem;

  *newpoint = SUNFALSE;

  /* Find the direction of integration */
  sign = (ca_mem->ca_tfinal - ca_mem->ca_tinitial > ZERO) ? 1 : -1;

  /* If this is the first time we use new data */
  if (ca_mem->ca_IMnewData) {
    ca_mem->ca_ilast = ca_mem->ca_np-1;
    *newpoint = SUNTRUE;
    ca_mem->ca_IMnewData = SUNFALSE;
  }

  /* Search for indx starting from ilast */
  to_left  = ( sign*(t - dt_mem[ca_mem->ca_ilast-1]->t) < ZERO);
  to_right = ( sign*(t - dt_mem[ca_mem->ca_ilast]->t)   > ZERO);

  if ( to_left ) {
    /* look for a new indx to the left */

    *newpoint = SUNTRUE;
    
    *indx = ca_mem->ca_ilast;
    for(;;) {
      if ( *indx == 0 ) break;
      if ( sign*(t - dt_mem[*indx-1]->t) <= ZERO ) (*indx)--;
      else                                         break;
    }

    if ( *indx == 0 )
      ca_mem->ca_ilast = 1;
    else
      ca_mem->ca_ilast = *indx;

    if ( *indx == 0 ) {
      /* t is beyond leftmost limit. Is it too far? */  
      if ( SUNRabs(t - dt_mem[0]->t) > FUZZ_FACTOR * cv_mem->cv_uround ) {
        return(CV_GETY_BADT);
      }
    }

  } else if ( to_right ) {
    /* look for a new indx to the right */

    *newpoint = SUNTRUE;

    *indx = ca_mem->ca_ilast;
    for(;;) {
      if ( sign*(t - dt_mem[*indx]->t) > ZERO) (*indx)++;
      else                                     break;
    }

    ca_mem->ca_ilast = *indx;


  } else {
    /* ilast is still OK */

    *indx = ca_mem->ca_ilast;

  }

  return(CV_SUCCESS);


}

/*
 * CVodeGetAdjY
 *
 * This routine returns the interpolated forward solution at time t.
 * The user must allocate space for y.
 */

int CVodeGetAdjY(void *cvode_mem, realtype t, N_Vector y)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  int flag;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODEA", "CVodeGetAdjY", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  ca_mem = cv_mem->cv_adj_mem;

  flag = ca_mem->ca_IMget(cv_mem, t, y, NULL);

  return(flag);
}

/* 
 * -----------------------------------------------------------------
 * Functions specific to cubic Hermite interpolation
 * -----------------------------------------------------------------
 */

/*
 * CVAhermiteMalloc
 *
 * This routine allocates memory for storing information at all
 * intermediate points between two consecutive check points. 
 * This data is then used to interpolate the forward solution 
 * at any other time.
 */

static booleantype CVAhermiteMalloc(CVodeMem cv_mem)
{
  CVadjMem ca_mem;
  DtpntMem *dt_mem;
  HermiteDataMem content;
  long int i, ii=0;
  booleantype allocOK;

  allocOK = SUNTRUE;

  ca_mem = cv_mem->cv_adj_mem;

  /* Allocate space for the vectors ytmp and yStmp */

  ca_mem->ca_ytmp = N_VClone(cv_mem->cv_tempv);
  if (ca_mem->ca_ytmp == NULL) {
    return(SUNFALSE);
  }

  if (ca_mem->ca_IMstoreSensi) {
    ca_mem->ca_yStmp = N_VCloneVectorArray(cv_mem->cv_Ns, cv_mem->cv_tempv);
    if (ca_mem->ca_yStmp == NULL) {
      N_VDestroy(ca_mem->ca_ytmp);
      return(SUNFALSE);
    }
  }

  /* Allocate space for the content field of the dt structures */

  dt_mem = ca_mem->dt_mem;

  for (i=0; i<=ca_mem->ca_nsteps; i++) {

    content = NULL;
    content = (HermiteDataMem) malloc(sizeof(struct HermiteDataMemRec));
    if (content == NULL) {
      ii = i;
      allocOK = SUNFALSE;
      break;
    }

    content->y = N_VClone(cv_mem->cv_tempv);
    if (content->y == NULL) {
      free(content); content = NULL;
      ii = i;
      allocOK = SUNFALSE;
      break;
    }

    content->yd = N_VClone(cv_mem->cv_tempv);
    if (content->yd == NULL) {
      N_VDestroy(content->y);
      free(content); content = NULL;
      ii = i;
      allocOK = SUNFALSE;
      break;
    }

    if (ca_mem->ca_IMstoreSensi) {

      content->yS = N_VCloneVectorArray(cv_mem->cv_Ns, cv_mem->cv_tempv);
      if (content->yS == NULL) {
        N_VDestroy(content->y);
        N_VDestroy(content->yd);
        free(content); content = NULL;
        ii = i;
        allocOK = SUNFALSE;
        break;
      }

      content->ySd = N_VCloneVectorArray(cv_mem->cv_Ns, cv_mem->cv_tempv);
      if (content->ySd == NULL) {
        N_VDestroy(content->y);
        N_VDestroy(content->yd);
        N_VDestroyVectorArray(content->yS, cv_mem->cv_Ns);
        free(content); content = NULL;
        ii = i;
        allocOK = SUNFALSE;
        break;
      }
      
    }
    
    dt_mem[i]->content = content;

  } 

  /* If an error occurred, deallocate and return */

  if (!allocOK) {

    N_VDestroy(ca_mem->ca_ytmp);

    if (ca_mem->ca_IMstoreSensi) {
      N_VDestroyVectorArray(ca_mem->ca_yStmp, cv_mem->cv_Ns);
    }

    for (i=0; i<ii; i++) {
      content = (HermiteDataMem) (dt_mem[i]->content);
      N_VDestroy(content->y);
      N_VDestroy(content->yd);
      if (ca_mem->ca_IMstoreSensi) {
        N_VDestroyVectorArray(content->yS, cv_mem->cv_Ns);
        N_VDestroyVectorArray(content->ySd, cv_mem->cv_Ns);
      }
      free(dt_mem[i]->content); dt_mem[i]->content = NULL;
    }

  }

  return(allocOK);
}

/*
 * CVAhermiteFree
 *
 * This routine frees the memory allocated for data storage.
 */

static void CVAhermiteFree(CVodeMem cv_mem)
{  
  CVadjMem ca_mem;
  DtpntMem *dt_mem;
  HermiteDataMem content;
  long int i;

  ca_mem = cv_mem->cv_adj_mem;

  N_VDestroy(ca_mem->ca_ytmp);

  if (ca_mem->ca_IMstoreSensi) {
    N_VDestroyVectorArray(ca_mem->ca_yStmp, cv_mem->cv_Ns);
  }

  dt_mem = ca_mem->dt_mem;

  for (i=0; i<=ca_mem->ca_nsteps; i++) {
    content = (HermiteDataMem) (dt_mem[i]->content);
    N_VDestroy(content->y);
    N_VDestroy(content->yd);
    if (ca_mem->ca_IMstoreSensi) {
      N_VDestroyVectorArray(content->yS, cv_mem->cv_Ns);
      N_VDestroyVectorArray(content->ySd, cv_mem->cv_Ns);
    }
    free(dt_mem[i]->content); dt_mem[i]->content = NULL;
  }
}

/*
 * CVAhermiteStorePnt ( -> IMstore )
 *
 * This routine stores a new point (y,yd) in the structure d for use
 * in the cubic Hermite interpolation.
 * Note that the time is already stored.
 */

static int CVAhermiteStorePnt(CVodeMem cv_mem, DtpntMem d)
{
  CVadjMem ca_mem;
  HermiteDataMem content;
  int is, retval;

  ca_mem = cv_mem->cv_adj_mem;

  content = (HermiteDataMem) d->content;

  /* Load solution */

  N_VScale(ONE, cv_mem->cv_zn[0], content->y);
  
  if (ca_mem->ca_IMstoreSensi) {
    for (is=0; is<cv_mem->cv_Ns; is++)
      cv_mem->cv_cvals[is] = ONE;

    retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                 cv_mem->cv_znS[0], content->yS);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
  }

  /* Load derivative */

  if (cv_mem->cv_nst == 0) {

    /* retval = */ cv_mem->cv_f(cv_mem->cv_tn, content->y, content->yd, cv_mem->cv_user_data);

    if (ca_mem->ca_IMstoreSensi) {
      /* retval = */ cvSensRhsWrapper(cv_mem, cv_mem->cv_tn, content->y, content->yd,
                                content->yS, content->ySd,
                                cv_mem->cv_tempv, cv_mem->cv_ftemp);
    }

  } else {

    N_VScale(ONE/cv_mem->cv_h, cv_mem->cv_zn[1], content->yd);

    if (ca_mem->ca_IMstoreSensi) {
      for (is=0; is<cv_mem->cv_Ns; is++)
        cv_mem->cv_cvals[is] = ONE/cv_mem->cv_h;

      retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                   cv_mem->cv_znS[1], content->ySd);
      if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
    }

  }

  return(0);
}

/*
 * CVAhermiteGetY ( -> IMget )
 *
 * This routine uses cubic piece-wise Hermite interpolation for 
 * the forward solution vector. 
 * It is typically called by the wrapper routines before calling
 * user provided routines (fB, djacB, bjacB, jtimesB, psolB) but
 * can be directly called by the user through CVodeGetAdjY
 */

static int CVAhermiteGetY(CVodeMem cv_mem, realtype t,
                          N_Vector y, N_Vector *yS)
{
  CVadjMem ca_mem;
  DtpntMem *dt_mem;
  HermiteDataMem content0, content1;

  realtype t0, t1, delta;
  realtype factor1, factor2, factor3;

  N_Vector y0, yd0, y1, yd1;
  N_Vector *yS0=NULL, *ySd0=NULL, *yS1, *ySd1;

  int flag, is, NS;
  long int indx;
  booleantype newpoint;

  /* local variables for fused vector oerations */
  int retval;
  realtype  cvals[4];
  N_Vector  Xvecs[4];
  N_Vector* XXvecs[4];
 
  ca_mem = cv_mem->cv_adj_mem;
  dt_mem = ca_mem->dt_mem;
 
  /* Local value of Ns */
 
  NS = (ca_mem->ca_IMinterpSensi && (yS != NULL)) ? cv_mem->cv_Ns : 0;

  /* Get the index in dt_mem */

  flag = CVAfindIndex(cv_mem, t, &indx, &newpoint);
  if (flag != CV_SUCCESS) return(flag);

  /* If we are beyond the left limit but close enough,
     then return y at the left limit. */

  if (indx == 0) {
    content0 = (HermiteDataMem) (dt_mem[0]->content);
    N_VScale(ONE, content0->y, y);

    if (NS > 0) {
      for (is=0; is<NS; is++)
        cv_mem->cv_cvals[is] = ONE;

      retval = N_VScaleVectorArray(NS, cv_mem->cv_cvals,
                                   content0->yS, yS);
      if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
    }
    
    return(CV_SUCCESS);
  }

  /* Extract stuff from the appropriate data points */

  t0 = dt_mem[indx-1]->t;
  t1 = dt_mem[indx]->t;
  delta = t1 - t0;

  content0 = (HermiteDataMem) (dt_mem[indx-1]->content);
  y0  = content0->y;
  yd0 = content0->yd;
  if (ca_mem->ca_IMinterpSensi) {
    yS0  = content0->yS;
    ySd0 = content0->ySd;
  }

  if (newpoint) {
    
    /* Recompute Y0 and Y1 */

    content1 = (HermiteDataMem) (dt_mem[indx]->content);

    y1  = content1->y;
    yd1 = content1->yd;

    /* Y1 = delta (yd1 + yd0) - 2 (y1 - y0) */
    cvals[0] = -TWO;   Xvecs[0] = y1;
    cvals[1] = TWO;    Xvecs[1] = y0;
    cvals[2] = delta;  Xvecs[2] = yd1;
    cvals[3] = delta;  Xvecs[3] = yd0;

    retval = N_VLinearCombination(4, cvals, Xvecs, ca_mem->ca_Y[1]);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

    /* Y0 = y1 - y0 - delta * yd0 */
    cvals[0] = ONE;     Xvecs[0] = y1;
    cvals[1] = -ONE;    Xvecs[1] = y0;
    cvals[2] = -delta;  Xvecs[2] = yd0;

    retval = N_VLinearCombination(3, cvals, Xvecs, ca_mem->ca_Y[0]);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

    /* Recompute YS0 and YS1, if needed */

    if (NS > 0) {

      yS1  = content1->yS;
      ySd1 = content1->ySd;

      /* YS1 = delta (ySd1 + ySd0) - 2 (yS1 - yS0) */
      cvals[0] = -TWO;   XXvecs[0] = yS1;
      cvals[1] = TWO;    XXvecs[1] = yS0;
      cvals[2] = delta;  XXvecs[2] = ySd1;
      cvals[3] = delta;  XXvecs[3] = ySd0;

      retval = N_VLinearCombinationVectorArray(NS, 4, cvals, XXvecs, ca_mem->ca_YS[1]);
      if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

      /* YS0 = yS1 - yS0 - delta * ySd0 */
      cvals[0] = ONE;     XXvecs[0] = yS1;
      cvals[1] = -ONE;    XXvecs[1] = yS0;
      cvals[2] = -delta;  XXvecs[2] = ySd0;

      retval = N_VLinearCombinationVectorArray(NS, 3, cvals, XXvecs, ca_mem->ca_YS[0]);
      if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

    }

  }

  /* Perform the actual interpolation. */

  factor1 = t - t0;

  factor2 = factor1/delta;
  factor2 = factor2*factor2;

  factor3 = factor2*(t-t1)/delta;

  cvals[0] = ONE;
  cvals[1] = factor1;
  cvals[2] = factor2;
  cvals[3] = factor3;

  /* y = y0 + factor1 yd0 + factor2 * Y[0] + factor3 Y[1] */
  Xvecs[0] = y0;
  Xvecs[1] = yd0;
  Xvecs[2] = ca_mem->ca_Y[0];
  Xvecs[3] = ca_mem->ca_Y[1];

  retval = N_VLinearCombination(4, cvals, Xvecs, y);
  if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

  /* yS = yS0 + factor1 ySd0 + factor2 * YS[0] + factor3 YS[1], if needed */
  if (NS > 0) {

    XXvecs[0] = yS0;
    XXvecs[1] = ySd0;
    XXvecs[2] = ca_mem->ca_YS[0];
    XXvecs[3] = ca_mem->ca_YS[1];

    retval = N_VLinearCombinationVectorArray(NS, 4, cvals, XXvecs, yS);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

  }

  return(CV_SUCCESS);
}

/* 
 * -----------------------------------------------------------------
 * Functions specific to Polynomial interpolation
 * -----------------------------------------------------------------
 */

/*
 * CVApolynomialMalloc
 *
 * This routine allocates memory for storing information at all
 * intermediate points between two consecutive check points. 
 * This data is then used to interpolate the forward solution 
 * at any other time.
 */

static booleantype CVApolynomialMalloc(CVodeMem cv_mem)
{
  CVadjMem ca_mem;
  DtpntMem *dt_mem;
  PolynomialDataMem content;
  long int i, ii=0;
  booleantype allocOK;

  allocOK = SUNTRUE;

  ca_mem = cv_mem->cv_adj_mem;

  /* Allocate space for the vectors ytmp and yStmp */

  ca_mem->ca_ytmp = N_VClone(cv_mem->cv_tempv);
  if (ca_mem->ca_ytmp == NULL) {
    return(SUNFALSE);
  }

  if (ca_mem->ca_IMstoreSensi) {
    ca_mem->ca_yStmp = N_VCloneVectorArray(cv_mem->cv_Ns, cv_mem->cv_tempv);
    if (ca_mem->ca_yStmp == NULL) {
      N_VDestroy(ca_mem->ca_ytmp);
      return(SUNFALSE);
    }
  }

  /* Allocate space for the content field of the dt structures */

  dt_mem = ca_mem->dt_mem;

  for (i=0; i<=ca_mem->ca_nsteps; i++) {

    content = NULL;
    content = (PolynomialDataMem) malloc(sizeof(struct PolynomialDataMemRec));
    if (content == NULL) {
      ii = i;
      allocOK = SUNFALSE;
      break;
    }

    content->y = N_VClone(cv_mem->cv_tempv);
    if (content->y == NULL) {
      free(content); content = NULL;
      ii = i;
      allocOK = SUNFALSE;
      break;
    }

    if (ca_mem->ca_IMstoreSensi) {

      content->yS = N_VCloneVectorArray(cv_mem->cv_Ns, cv_mem->cv_tempv);
      if (content->yS == NULL) {
        N_VDestroy(content->y);
        free(content); content = NULL;
        ii = i;
        allocOK = SUNFALSE;
        break;
      }

    }

    dt_mem[i]->content = content;

  } 

  /* If an error occurred, deallocate and return */

  if (!allocOK) {

    N_VDestroy(ca_mem->ca_ytmp);

    if (ca_mem->ca_IMstoreSensi) {
      N_VDestroyVectorArray(ca_mem->ca_yStmp, cv_mem->cv_Ns);
    }

    for (i=0; i<ii; i++) {
      content = (PolynomialDataMem) (dt_mem[i]->content);
      N_VDestroy(content->y);
      if (ca_mem->ca_IMstoreSensi) {
        N_VDestroyVectorArray(content->yS, cv_mem->cv_Ns);
      }
      free(dt_mem[i]->content); dt_mem[i]->content = NULL;
    }

  }

  return(allocOK);

}

/*
 * CVApolynomialFree
 *
 * This routine frees the memeory allocated for data storage.
 */

static void CVApolynomialFree(CVodeMem cv_mem)
{
  CVadjMem ca_mem;
  DtpntMem *dt_mem;
  PolynomialDataMem content;
  long int i;

  ca_mem = cv_mem->cv_adj_mem;

  N_VDestroy(ca_mem->ca_ytmp);

  if (ca_mem->ca_IMstoreSensi) {
    N_VDestroyVectorArray(ca_mem->ca_yStmp, cv_mem->cv_Ns);
  }

  dt_mem = ca_mem->dt_mem;

  for (i=0; i<=ca_mem->ca_nsteps; i++) {
    content = (PolynomialDataMem) (dt_mem[i]->content);
    N_VDestroy(content->y);
    if (ca_mem->ca_IMstoreSensi) {
      N_VDestroyVectorArray(content->yS, cv_mem->cv_Ns);
    }
    free(dt_mem[i]->content); dt_mem[i]->content = NULL;
  }
}

/*
 * CVApolynomialStorePnt ( -> IMstore )
 *
 * This routine stores a new point y in the structure d for use
 * in the Polynomial interpolation.
 * Note that the time is already stored.
 */

static int CVApolynomialStorePnt(CVodeMem cv_mem, DtpntMem d)
{
  CVadjMem ca_mem;
  PolynomialDataMem content;
  int is, retval;

  ca_mem = cv_mem->cv_adj_mem;

  content = (PolynomialDataMem) d->content;

  N_VScale(ONE, cv_mem->cv_zn[0], content->y);

  if (ca_mem->ca_IMstoreSensi) {
    for (is=0; is<cv_mem->cv_Ns; is++)
      cv_mem->cv_cvals[is] = ONE;
    retval = N_VScaleVectorArray(cv_mem->cv_Ns, cv_mem->cv_cvals,
                                 cv_mem->cv_znS[0], content->yS);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
  }

  content->order = cv_mem->cv_qu;

  return(0);
}

/*
 * CVApolynomialGetY ( -> IMget )
 *
 * This routine uses polynomial interpolation for the forward solution vector. 
 * It is typically called by the wrapper routines before calling
 * user provided routines (fB, djacB, bjacB, jtimesB, psolB)) but
 * can be directly called by the user through CVodeGetAdjY.
 */

static int CVApolynomialGetY(CVodeMem cv_mem, realtype t,
                             N_Vector y, N_Vector *yS)
{
  CVadjMem ca_mem;
  DtpntMem *dt_mem;
  PolynomialDataMem content;

  int flag, dir, order, i, j, is, NS, retval;
  long int indx, base;
  booleantype newpoint;
  realtype dt, factor;

  ca_mem = cv_mem->cv_adj_mem;
  dt_mem = ca_mem->dt_mem;
  
  /* Local value of Ns */
 
  NS = (ca_mem->ca_IMinterpSensi && (yS != NULL)) ? cv_mem->cv_Ns : 0;

  /* Get the index in dt_mem */

  flag = CVAfindIndex(cv_mem, t, &indx, &newpoint);
  if (flag != CV_SUCCESS) return(flag);

  /* If we are beyond the left limit but close enough,
     then return y at the left limit. */

  if (indx == 0) {
    content = (PolynomialDataMem) (dt_mem[0]->content);
    N_VScale(ONE, content->y, y);

    if (NS > 0) {
      for (is=0; is<NS; is++)
        cv_mem->cv_cvals[is] = ONE;
      retval = N_VScaleVectorArray(NS, cv_mem->cv_cvals, content->yS, yS);
      if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
    }

    return(CV_SUCCESS);
  }

  /* Scaling factor */

  dt = SUNRabs(dt_mem[indx]->t - dt_mem[indx-1]->t);

  /* Find the direction of the forward integration */

  dir = (ca_mem->ca_tfinal - ca_mem->ca_tinitial > ZERO) ? 1 : -1;

  /* Establish the base point depending on the integration direction.
     Modify the base if there are not enough points for the current order */

  if (dir == 1) {
    base = indx;
    content = (PolynomialDataMem) (dt_mem[base]->content);
    order = content->order;
    if(indx < order) base += order-indx;
  } else {
    base = indx-1;
    content = (PolynomialDataMem) (dt_mem[base]->content);
    order = content->order;
    if (ca_mem->ca_np-indx > order) base -= indx+order-ca_mem->ca_np;
  }

  /* Recompute Y (divided differences for Newton polynomial) if needed */

  if (newpoint) {

    /* Store 0-th order DD */
    if (dir == 1) {
      for(j=0;j<=order;j++) {
        ca_mem->ca_T[j] = dt_mem[base-j]->t;
        content = (PolynomialDataMem) (dt_mem[base-j]->content);
        N_VScale(ONE, content->y, ca_mem->ca_Y[j]);

        if (NS > 0) {
          for (is=0; is<NS; is++)
            cv_mem->cv_cvals[is] = ONE;
          retval = N_VScaleVectorArray(NS, cv_mem->cv_cvals,
                                       content->yS, ca_mem->ca_YS[j]);
          if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
        }
      }
    } else {
      for(j=0;j<=order;j++) {
        ca_mem->ca_T[j] = dt_mem[base-1+j]->t;
        content = (PolynomialDataMem) (dt_mem[base-1+j]->content);
        N_VScale(ONE, content->y, ca_mem->ca_Y[j]);
        if (NS > 0) {
          for (is=0; is<NS; is++)
            cv_mem->cv_cvals[is] = ONE;
          retval = N_VScaleVectorArray(NS, cv_mem->cv_cvals,
                                       content->yS, ca_mem->ca_YS[j]);
          if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
        }
      }
    }

    /* Compute higher-order DD */
    for(i=1;i<=order;i++) {
      for(j=order;j>=i;j--) {
        factor = dt/(ca_mem->ca_T[j]-ca_mem->ca_T[j-i]);
        N_VLinearSum(factor, ca_mem->ca_Y[j], -factor, ca_mem->ca_Y[j-1], ca_mem->ca_Y[j]);

        if (NS > 0) {
          retval = N_VLinearSumVectorArray(NS,
                                           factor,  ca_mem->ca_YS[j],
                                           -factor, ca_mem->ca_YS[j-1],
                                           ca_mem->ca_YS[j]);
          if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
        }
      }
    }
  }

  /* Perform the actual interpolation using nested multiplications */

  cv_mem->cv_cvals[0] = ONE;
  for (i=0; i<order; i++)
    cv_mem->cv_cvals[i+1] = cv_mem->cv_cvals[i] * (t-ca_mem->ca_T[i]) / dt;

  retval = N_VLinearCombination(order+1, cv_mem->cv_cvals, ca_mem->ca_Y, y);
  if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);

  if (NS > 0) {
    retval = N_VLinearCombinationVectorArray(NS, order+1, cv_mem->cv_cvals, ca_mem->ca_YS, yS);
    if (retval != CV_SUCCESS) return (CV_VECTOROP_ERR);
  }

  return(CV_SUCCESS);

}

/* 
 * =================================================================
 * WRAPPERS FOR ADJOINT SYSTEM
 * =================================================================
 */
/*
 * CVArhs
 *
 * This routine interfaces to the CVRhsFnB (or CVRhsFnBS) routine 
 * provided by the user.
 */

static int CVArhs(realtype t, N_Vector yB, 
                  N_Vector yBdot, void *cvode_mem)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  int flag, retval;

  cv_mem = (CVodeMem) cvode_mem;

  ca_mem = cv_mem->cv_adj_mem;

  cvB_mem = ca_mem->ca_bckpbCrt;

  /* Get forward solution from interpolation */

  if (ca_mem->ca_IMinterpSensi)
    flag = ca_mem->ca_IMget(cv_mem, t, ca_mem->ca_ytmp, ca_mem->ca_yStmp);
  else 
    flag = ca_mem->ca_IMget(cv_mem, t, ca_mem->ca_ytmp, NULL);

  if (flag != CV_SUCCESS) {
    cvProcessError(cv_mem, -1, "CVODEA", "CVArhs", MSGCV_BAD_TINTERP, t);
    return(-1);
  }

  /* Call the user's RHS function */

  if (cvB_mem->cv_f_withSensi)
    retval = (cvB_mem->cv_fs)(t, ca_mem->ca_ytmp, ca_mem->ca_yStmp, yB, yBdot, cvB_mem->cv_user_data);
  else
    retval = (cvB_mem->cv_f)(t, ca_mem->ca_ytmp, yB, yBdot, cvB_mem->cv_user_data);

  return(retval);
}

/*
 * CVArhsQ
 *
 * This routine interfaces to the CVQuadRhsFnB (or CVQuadRhsFnBS) routine
 * provided by the user.
 */

static int CVArhsQ(realtype t, N_Vector yB, 
                   N_Vector qBdot, void *cvode_mem)
{
  CVodeMem cv_mem;
  CVadjMem ca_mem;
  CVodeBMem cvB_mem;
  /* int flag; */
  int retval;

  cv_mem = (CVodeMem) cvode_mem;

  ca_mem = cv_mem->cv_adj_mem;

  cvB_mem = ca_mem->ca_bckpbCrt;

  /* Get forward solution from interpolation */

  if (ca_mem->ca_IMinterpSensi)
    /* flag = */ ca_mem->ca_IMget(cv_mem, t, ca_mem->ca_ytmp, ca_mem->ca_yStmp);
  else 
    /* flag = */ ca_mem->ca_IMget(cv_mem, t, ca_mem->ca_ytmp, NULL);

  /* Call the user's RHS function */

  if (cvB_mem->cv_fQ_withSensi)
    retval = (cvB_mem->cv_fQs)(t, ca_mem->ca_ytmp, ca_mem->ca_yStmp, yB, qBdot, cvB_mem->cv_user_data);
  else
    retval = (cvB_mem->cv_fQ)(t, ca_mem->ca_ytmp, yB, qBdot, cvB_mem->cv_user_data);

  return(retval);
}
