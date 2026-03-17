/*
 * -----------------------------------------------------------------
 * $Revision$
 * $Date$
 * -----------------------------------------------------------------
 * Programmer(s): Alan C. Hindmarsh and Radu Serban @ LLNL
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
 * functions for the CVODES solver.
 * -----------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>

#include "cvodes_impl.h"

#include <sundials/sundials_math.h>
#include <sundials/sundials_types.h>

#define ZERO   RCONST(0.0)
#define HALF   RCONST(0.5)
#define ONE    RCONST(1.0)
#define TWOPT5 RCONST(2.5)

/* 
 * =================================================================
 * CVODES optional input functions
 * =================================================================
 */

/* 
 * CVodeSetErrHandlerFn
 *
 * Specifies the error handler function
 */

int CVodeSetErrHandlerFn(void *cvode_mem, CVErrHandlerFn ehfun, void *eh_data)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetErrHandlerFn", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  cv_mem->cv_ehfun = ehfun;
  cv_mem->cv_eh_data = eh_data;

  return(CV_SUCCESS);
}

/* 
 * CVodeSetErrFile
 *
 * Specifies the FILE pointer for output (NULL means no messages)
 */

int CVodeSetErrFile(void *cvode_mem, FILE *errfp)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetErrFile", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  cv_mem->cv_errfp = errfp;

  return(CV_SUCCESS);
}

/* 
 * CVodeSetUserData
 *
 * Specifies the user data pointer for f
 */

int CVodeSetUserData(void *cvode_mem, void *user_data)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetUserData", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  cv_mem->cv_user_data = user_data;

  return(CV_SUCCESS);
}

/* 
 * CVodeSetMaxOrd
 *
 * Specifies the maximum method order
 */

int CVodeSetMaxOrd(void *cvode_mem, int maxord)
{
  CVodeMem cv_mem;
  int qmax_alloc;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetMaxOrd", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (maxord <= 0) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetMaxOrd", MSGCV_NEG_MAXORD);
    return(CV_ILL_INPUT);
  }
  
  /* Cannot increase maximum order beyond the value that
     was used when allocating memory */
  qmax_alloc = cv_mem->cv_qmax_alloc;
  qmax_alloc = SUNMIN(qmax_alloc, cv_mem->cv_qmax_allocQ);
  qmax_alloc = SUNMIN(qmax_alloc, cv_mem->cv_qmax_allocS);

  if (maxord > qmax_alloc) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetMaxOrd", MSGCV_BAD_MAXORD);
    return(CV_ILL_INPUT);
  }

  cv_mem->cv_qmax = maxord;

  return(CV_SUCCESS);
}

/* 
 * CVodeSetMaxNumSteps
 *
 * Specifies the maximum number of integration steps
 */

int CVodeSetMaxNumSteps(void *cvode_mem, long int mxsteps)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetMaxNumSteps", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  /* Passing mxsteps=0 sets the default. Passing mxsteps<0 disables the test. */
  if (mxsteps == 0)
    cv_mem->cv_mxstep = MXSTEP_DEFAULT;
  else
    cv_mem->cv_mxstep = mxsteps;

  return(CV_SUCCESS);
}

/* 
 * CVodeSetMaxHnilWarns
 *
 * Specifies the maximum number of warnings for small h
 */

int CVodeSetMaxHnilWarns(void *cvode_mem, int mxhnil)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetMaxHnilWarns", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  cv_mem->cv_mxhnil = mxhnil;

  return(CV_SUCCESS);
}

/* 
 *CVodeSetStabLimDet
 *
 * Turns on/off the stability limit detection algorithm
 */

int CVodeSetStabLimDet(void *cvode_mem, booleantype sldet)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetStabLimDet", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if( sldet && (cv_mem->cv_lmm != CV_BDF) ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetStabLimDet", MSGCV_SET_SLDET);
    return(CV_ILL_INPUT);
  }

  cv_mem->cv_sldeton = sldet;

  return(CV_SUCCESS);
}

/* 
 * CVodeSetInitStep
 *
 * Specifies the initial step size
 */

int CVodeSetInitStep(void *cvode_mem, realtype hin)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetInitStep", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  cv_mem->cv_hin = hin;

  return(CV_SUCCESS);
}

/* 
 * CVodeSetMinStep
 *
 * Specifies the minimum step size
 */

int CVodeSetMinStep(void *cvode_mem, realtype hmin)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetMinStep", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (hmin<0) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetMinStep", MSGCV_NEG_HMIN);
    return(CV_ILL_INPUT);
  }

  /* Passing 0 sets hmin = zero */
  if (hmin == ZERO) {
    cv_mem->cv_hmin = HMIN_DEFAULT;
    return(CV_SUCCESS);
  }

  if (hmin * cv_mem->cv_hmax_inv > ONE) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetMinStep", MSGCV_BAD_HMIN_HMAX);
    return(CV_ILL_INPUT);
  }

  cv_mem->cv_hmin = hmin;

  return(CV_SUCCESS);
}

/* 
 * CVodeSetMaxStep
 *
 * Specifies the maximum step size
 */

int CVodeSetMaxStep(void *cvode_mem, realtype hmax)
{
  realtype hmax_inv;
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetMaxStep", MSGCV_NO_MEM);
    return (CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (hmax < 0) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetMaxStep", MSGCV_NEG_HMAX);
    return(CV_ILL_INPUT);
  }

  /* Passing 0 sets hmax = infinity */
  if (hmax == ZERO) {
    cv_mem->cv_hmax_inv = HMAX_INV_DEFAULT;
    return(CV_SUCCESS);
  }

  hmax_inv = ONE/hmax;
  if (hmax_inv * cv_mem->cv_hmin > ONE) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetMaxStep", MSGCV_BAD_HMIN_HMAX);
    return(CV_ILL_INPUT);
  }

  cv_mem->cv_hmax_inv = hmax_inv;

  return(CV_SUCCESS);
}

/* 
 * CVodeSetStopTime
 *
 * Specifies the time beyond which the integration is not to proceed.
 */

int CVodeSetStopTime(void *cvode_mem, realtype tstop)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetStopTime", MSGCV_NO_MEM);
    return (CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* If CVode was called at least once, test if tstop is legal
   * (i.e. if it was not already passed).
   * If CVodeSetStopTime is called before the first call to CVode,
   * tstop will be checked in CVode. */
  if (cv_mem->cv_nst > 0) {

    if ( (tstop - cv_mem->cv_tn) * cv_mem->cv_h < ZERO ) {
      cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetStopTime", MSGCV_BAD_TSTOP, tstop, cv_mem->cv_tn);
      return(CV_ILL_INPUT);
    }

  }

  cv_mem->cv_tstop = tstop;
  cv_mem->cv_tstopset = SUNTRUE;

  return(CV_SUCCESS);
}

/* 
 * CVodeSetMaxErrTestFails
 *
 * Specifies the maximum number of error test failures during one
 * step try.
 */

int CVodeSetMaxErrTestFails(void *cvode_mem, int maxnef)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetMaxErrTestFails", MSGCV_NO_MEM);
    return (CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  cv_mem->cv_maxnef = maxnef;

  return(CV_SUCCESS);
}

/* 
 * CVodeSetMaxConvFails
 *
 * Specifies the maximum number of nonlinear convergence failures 
 * during one step try.
 */

int CVodeSetMaxConvFails(void *cvode_mem, int maxncf)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetMaxConvFails", MSGCV_NO_MEM);
    return (CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  cv_mem->cv_maxncf = maxncf;

  return(CV_SUCCESS);
}

/*
 * CVodeSetMaxNonlinIters
 *
 * Specifies the maximum number of nonlinear iterations during
 * one solve.
 */

int CVodeSetMaxNonlinIters(void *cvode_mem, int maxcor)
{
  CVodeMem cv_mem;
  booleantype sensi_sim;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeSetMaxNonlinIters", MSGCV_NO_MEM);
    return (CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  /* Are we computing sensitivities with the simultaneous approach? */
  sensi_sim = (cv_mem->cv_sensi && (cv_mem->cv_ism==CV_SIMULTANEOUS));

  if (sensi_sim) {

    /* check that the NLS is non-NULL */
    if (cv_mem->NLSsim == NULL) {
      cvProcessError(NULL, CV_MEM_FAIL, "CVODES",
                      "CVodeSetMaxNonlinIters", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    return(SUNNonlinSolSetMaxIters(cv_mem->NLSsim, maxcor));

  } else {

    /* check that the NLS is non-NULL */
    if (cv_mem->NLS == NULL) {
      cvProcessError(NULL, CV_MEM_FAIL, "CVODES",
                      "CVodeSetMaxNonlinIters", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    return(SUNNonlinSolSetMaxIters(cv_mem->NLS, maxcor));
  }

  return(CV_SUCCESS);
}

/* 
 * CVodeSetNonlinConvCoef
 *
 * Specifies the coeficient in the nonlinear solver convergence
 * test
 */

int CVodeSetNonlinConvCoef(void *cvode_mem, realtype nlscoef)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetNonlinConvCoef", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  cv_mem->cv_nlscoef = nlscoef;

  return(CV_SUCCESS);
}

/* 
 * CVodeSetRootDirection
 *
 * Specifies the direction of zero-crossings to be monitored.
 * The default is to monitor both crossings.
 */

int CVodeSetRootDirection(void *cvode_mem, int *rootdir)
{
  CVodeMem cv_mem;
  int i, nrt;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetRootDirection", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  nrt = cv_mem->cv_nrtfn;
  if (nrt==0) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetRootDirection", MSGCV_NO_ROOT);
    return(CV_ILL_INPUT);    
  }

  for(i=0; i<nrt; i++) cv_mem->cv_rootdir[i] = rootdir[i];

  return(CV_SUCCESS);
}


/*
 * CVodeSetNoInactiveRootWarn
 *
 * Disables issuing a warning if some root function appears
 * to be identically zero at the beginning of the integration
 */

int CVodeSetNoInactiveRootWarn(void *cvode_mem)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetNoInactiveRootWarn", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  cv_mem->cv_mxgnull = 0;
  
  return(CV_SUCCESS);
}

/*
 * CVodeSetConstraints
 *
 * Setup for constraint handling feature
 */

int CVodeSetConstraints(void *cvode_mem, N_Vector constraints)
{
  CVodeMem cv_mem;
  realtype temptest;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetConstraints", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  /* If there are no constraints, destroy data structures */
  if (constraints == NULL) {
    if (cv_mem->cv_constraintsMallocDone) {
      N_VDestroy(cv_mem->cv_constraints);
      cv_mem->cv_lrw -= cv_mem->cv_lrw1;
      cv_mem->cv_liw -= cv_mem->cv_liw1;
    }
    cv_mem->cv_constraintsMallocDone = SUNFALSE;
    cv_mem->cv_constraintsSet = SUNFALSE;
    return(CV_SUCCESS);
  }

  /* Test if required vector ops. are defined */

  if (constraints->ops->nvdiv         == NULL ||
      constraints->ops->nvmaxnorm     == NULL ||
      constraints->ops->nvcompare     == NULL ||
      constraints->ops->nvconstrmask  == NULL ||
      constraints->ops->nvminquotient == NULL) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetConstraints", MSGCV_BAD_NVECTOR);
    return(CV_ILL_INPUT);
  }

  /* Check the constraints vector */
  temptest = N_VMaxNorm(constraints);
  if ((temptest > TWOPT5) || (temptest < HALF)) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetConstraints", MSGCV_BAD_CONSTR);
    return(CV_ILL_INPUT);
  }

  if ( !(cv_mem->cv_constraintsMallocDone) ) {
    cv_mem->cv_constraints = N_VClone(constraints);
    cv_mem->cv_lrw += cv_mem->cv_lrw1;
    cv_mem->cv_liw += cv_mem->cv_liw1;
    cv_mem->cv_constraintsMallocDone = SUNTRUE;
  }

  /* Load the constraints vector */
  N_VScale(ONE, constraints, cv_mem->cv_constraints);

  cv_mem->cv_constraintsSet = SUNTRUE;

  return(CV_SUCCESS);
}

/* 
 * =================================================================
 * Quadrature optional input functions
 * =================================================================
 */

int CVodeSetQuadErrCon(void *cvode_mem, booleantype errconQ)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetQuadErrCon", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  cv_mem->cv_errconQ = errconQ;

  return(CV_SUCCESS);
}

/* 
 * =================================================================
 * FSA optional input functions
 * =================================================================
 */

int CVodeSetSensDQMethod(void *cvode_mem, int DQtype, realtype DQrhomax)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetSensDQMethod", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if ( (DQtype != CV_CENTERED) && (DQtype != CV_FORWARD) ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetSensDQMethod", MSGCV_BAD_DQTYPE);    
    return(CV_ILL_INPUT);
  }

  if (DQrhomax < ZERO ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetSensDQMethod", MSGCV_BAD_DQRHO);    
    return(CV_ILL_INPUT);
  }

  cv_mem->cv_DQtype = DQtype;
  cv_mem->cv_DQrhomax = DQrhomax;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeSetSensErrCon(void *cvode_mem, booleantype errconS)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetSensErrCon", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  cv_mem->cv_errconS = errconS;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeSetSensMaxNonlinIters(void *cvode_mem, int maxcorS)
{
  CVodeMem cv_mem;
  booleantype sensi_stg;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeSetSensMaxNonlinIters", MSGCV_NO_MEM);
    return (CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  /* Are we computing sensitivities with a staggered approach? */
  sensi_stg = (cv_mem->cv_sensi && (cv_mem->cv_ism==CV_STAGGERED));

  if (sensi_stg) {

    /* check that the NLS is non-NULL */
    if (cv_mem->NLSstg == NULL) {
      cvProcessError(NULL, CV_MEM_FAIL, "CVODES",
                      "CVodeSetSensMaxNonlinIters", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    return(SUNNonlinSolSetMaxIters(cv_mem->NLSstg, maxcorS));

  } else {

    /* check that the NLS is non-NULL */
    if (cv_mem->NLSstg1 == NULL) {
      cvProcessError(NULL, CV_MEM_FAIL, "CVODES",
                      "CVodeSetMaxNonlinIters", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    return(SUNNonlinSolSetMaxIters(cv_mem->NLSstg1, maxcorS));
  }

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeSetSensParams(void *cvode_mem, realtype *p, realtype *pbar, int *plist)
{
  CVodeMem cv_mem;
  int is, Ns;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetSensParams", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  /* Was sensitivity initialized? */

  if (cv_mem->cv_SensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeSetSensParams", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  Ns = cv_mem->cv_Ns;

  /* Parameters */

  cv_mem->cv_p = p;

  /* pbar */

  if (pbar != NULL)
    for (is=0; is<Ns; is++) {
      if (pbar[is] == ZERO) {
        cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetSensParams", MSGCV_BAD_PBAR);
        return(CV_ILL_INPUT);
      }
      cv_mem->cv_pbar[is] = SUNRabs(pbar[is]);
    }
  else
    for (is=0; is<Ns; is++)
      cv_mem->cv_pbar[is] = ONE;

  /* plist */

  if (plist != NULL)
    for (is=0; is<Ns; is++) {
      if ( plist[is] < 0 ) {
        cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES", "CVodeSetSensParams", MSGCV_BAD_PLIST);
        return(CV_ILL_INPUT);
      }
      cv_mem->cv_plist[is] = plist[is];
    }
  else
    for (is=0; is<Ns; is++)
      cv_mem->cv_plist[is] = is;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeSetQuadSensErrCon(void *cvode_mem, booleantype errconQS)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeSetQuadSensErrCon", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }  
  cv_mem = (CVodeMem) cvode_mem;

  /* Was sensitivity initialized? */

  if (cv_mem->cv_SensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeSetQuadSensTolerances", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  } 

  /* Ckeck if quadrature sensitivity was initialized? */

  if (cv_mem->cv_QuadSensMallocDone == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUADSENS, "CVODES", "CVodeSetQuadSensErrCon", MSGCV_NO_QUADSENSI); 
    return(CV_NO_QUAD);
  }

  cv_mem->cv_errconQS = errconQS;

  return(CV_SUCCESS);
}

/* 
 * =================================================================
 * CVODES optional output functions
 * =================================================================
 */

/*
 * CVodeGetNumSteps
 *
 * Returns the current number of integration steps
 */

int CVodeGetNumSteps(void *cvode_mem, long int *nsteps)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetNumSteps", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *nsteps = cv_mem->cv_nst;

  return(CV_SUCCESS);
}

/* 
 * CVodeGetNumRhsEvals
 *
 * Returns the current number of calls to f
 */

int CVodeGetNumRhsEvals(void *cvode_mem, long int *nfevals)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetNumRhsEvals", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *nfevals = cv_mem->cv_nfe;

  return(CV_SUCCESS);
}

/* 
 * CVodeGetNumLinSolvSetups
 *
 * Returns the current number of calls to the linear solver setup routine
 */

int CVodeGetNumLinSolvSetups(void *cvode_mem, long int *nlinsetups)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetNumLinSolvSetups", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *nlinsetups = cv_mem->cv_nsetups;

  return(CV_SUCCESS);
}

/*
 * CVodeGetNumErrTestFails
 *
 * Returns the current number of error test failures
 */

int CVodeGetNumErrTestFails(void *cvode_mem, long int *netfails)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetNumErrTestFails", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *netfails = cv_mem->cv_netf;

  return(CV_SUCCESS);
}

/* 
 * CVodeGetLastOrder
 *
 * Returns the order on the last succesful step
 */

int CVodeGetLastOrder(void *cvode_mem, int *qlast)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetLastOrder", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *qlast = cv_mem->cv_qu;

  return(CV_SUCCESS);
}

/* 
 * CVodeGetCurrentOrder
 *
 * Returns the order to be attempted on the next step
 */

int CVodeGetCurrentOrder(void *cvode_mem, int *qcur)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetCurrentOrder", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *qcur = cv_mem->cv_next_q;

  return(CV_SUCCESS);
}

/* 
 * CVodeGetNumStabLimOrderReds
 *
 * Returns the number of order reductions triggered by the stability
 * limit detection algorithm
 */

int CVodeGetNumStabLimOrderReds(void *cvode_mem, long int *nslred)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetNumStabLimOrderReds", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_sldeton==SUNFALSE)
    *nslred = 0;
  else
    *nslred = cv_mem->cv_nor;

  return(CV_SUCCESS);
}

/* 
 * CVodeGetActualInitStep
 *
 * Returns the step size used on the first step
 */

int CVodeGetActualInitStep(void *cvode_mem, realtype *hinused)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetActualInitStep", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *hinused = cv_mem->cv_h0u;

  return(CV_SUCCESS);
}

/*
 * CVodeGetLastStep
 *
 * Returns the step size used on the last successful step
 */

int CVodeGetLastStep(void *cvode_mem, realtype *hlast)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetLastStep", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *hlast = cv_mem->cv_hu;

  return(CV_SUCCESS);
}

/* 
 * CVodeGetCurrentStep
 *
 * Returns the step size to be attempted on the next step
 */

int CVodeGetCurrentStep(void *cvode_mem, realtype *hcur)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetCurrentStep", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;
  
  *hcur = cv_mem->cv_next_h;

  return(CV_SUCCESS);
}

/* 
 * CVodeGetCurrentTime
 *
 * Returns the current value of the independent variable
 */

int CVodeGetCurrentTime(void *cvode_mem, realtype *tcur)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetCurrentTime", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *tcur = cv_mem->cv_tn;

  return(CV_SUCCESS);
}

/* 
 * CVodeGetTolScaleFactor
 *
 * Returns a suggested factor for scaling tolerances
 */

int CVodeGetTolScaleFactor(void *cvode_mem, realtype *tolsfact)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetTolScaleFactor", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *tolsfact = cv_mem->cv_tolsf;

  return(CV_SUCCESS);
}

/* 
 * CVodeGetErrWeights
 *
 * This routine returns the current weight vector.
 */

int CVodeGetErrWeights(void *cvode_mem, N_Vector eweight)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetErrWeights", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  N_VScale(ONE, cv_mem->cv_ewt, eweight);

  return(CV_SUCCESS);
}

/*
 * CVodeGetEstLocalErrors
 *
 * Returns an estimate of the local error
 */

int CVodeGetEstLocalErrors(void *cvode_mem, N_Vector ele)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetEstLocalErrors", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  N_VScale(ONE, cv_mem->cv_acor, ele);

  return(CV_SUCCESS);
}

/* 
 * CVodeGetWorkSpace
 *
 * Returns integrator work space requirements
 */

int CVodeGetWorkSpace(void *cvode_mem, long int *lenrw, long int *leniw)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetWorkSpace", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *leniw = cv_mem->cv_liw;
  *lenrw = cv_mem->cv_lrw;

  return(CV_SUCCESS);
}

/* 
 * CVodeGetIntegratorStats
 *
 * Returns integrator statistics
 */

int CVodeGetIntegratorStats(void *cvode_mem, long int *nsteps, long int *nfevals, 
                            long int *nlinsetups, long int *netfails, int *qlast, 
                            int *qcur, realtype *hinused, realtype *hlast, 
                            realtype *hcur, realtype *tcur)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetIntegratorStats", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *nsteps = cv_mem->cv_nst;
  *nfevals = cv_mem->cv_nfe;
  *nlinsetups = cv_mem->cv_nsetups;
  *netfails = cv_mem->cv_netf;
  *qlast = cv_mem->cv_qu;
  *qcur = cv_mem->cv_next_q;
  *hinused = cv_mem->cv_h0u;
  *hlast = cv_mem->cv_hu;
  *hcur = cv_mem->cv_next_h;
  *tcur = cv_mem->cv_tn;

  return(CV_SUCCESS);
}

/* 
 * CVodeGetNumGEvals
 *
 * Returns the current number of calls to g (for rootfinding)
 */

int CVodeGetNumGEvals(void *cvode_mem, long int *ngevals)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetNumGEvals", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *ngevals = cv_mem->cv_nge;

  return(CV_SUCCESS);
}

/* 
 * CVodeGetRootInfo
 *
 * Returns pointer to array rootsfound showing roots found
 */

int CVodeGetRootInfo(void *cvode_mem, int *rootsfound)
{
  CVodeMem cv_mem;
  int i, nrt;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetRootInfo", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  nrt = cv_mem->cv_nrtfn;

  for (i=0; i<nrt; i++) rootsfound[i] = cv_mem->cv_iroots[i];

  return(CV_SUCCESS);
}


/*
 * CVodeGetNumNonlinSolvIters
 *
 * Returns the current number of iterations in the nonlinear solver
 */

int CVodeGetNumNonlinSolvIters(void *cvode_mem, long int *nniters)
{
  CVodeMem cv_mem;
  booleantype sensi_sim;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeGetNumNonlinSolvIters", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  /* are we computing sensitivities with the simultaneous approach? */
  sensi_sim = (cv_mem->cv_sensi && (cv_mem->cv_ism==CV_SIMULTANEOUS));

  /* get number of iterations from the NLS */
  if (sensi_sim) {

    /* check that the NLS is non-NULL */
    if (cv_mem->NLSsim == NULL) {
      cvProcessError(NULL, CV_MEM_FAIL, "CVODES",
                     "CVodeGetNumNonlinSolvIters", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    return(SUNNonlinSolGetNumIters(cv_mem->NLSsim, nniters));

  } else {

    /* check that the NLS is non-NULL */
    if (cv_mem->NLS == NULL) {
      cvProcessError(NULL, CV_MEM_FAIL, "CVODES",
                     "CVodeGetNumNonlinSolvIters", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    return(SUNNonlinSolGetNumIters(cv_mem->NLS, nniters));
  }

  return(CV_SUCCESS);
}

/* 
 * CVodeGetNumNonlinSolvConvFails
 *
 * Returns the current number of convergence failures in the
 * nonlinear solver
 */

int CVodeGetNumNonlinSolvConvFails(void *cvode_mem, long int *nncfails)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetNumNonlinSolvConvFails", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *nncfails = cv_mem->cv_ncfn;

  return(CV_SUCCESS);
}

/*
 * CVodeGetNonlinSolvStats
 *
 * Returns nonlinear solver statistics
 */

int CVodeGetNonlinSolvStats(void *cvode_mem, long int *nniters,
                            long int *nncfails)
{
  CVodeMem cv_mem;
  booleantype sensi_sim;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeGetNonlinSolvStats", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  *nncfails = cv_mem->cv_ncfn;

  /* are we computing sensitivities with the simultaneous approach? */
  sensi_sim = (cv_mem->cv_sensi && (cv_mem->cv_ism==CV_SIMULTANEOUS));

  /* get number of iterations from the NLS */
  if (sensi_sim) {

    /* check that the NLS is non-NULL */
    if (cv_mem->NLSsim == NULL) {
      cvProcessError(NULL, CV_MEM_FAIL, "CVODES",
                     "CVodeGetNumNonlinSolvIters", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    return(SUNNonlinSolGetNumIters(cv_mem->NLSsim, nniters));

  } else {

    /* check that the NLS is non-NULL */
    if (cv_mem->NLS == NULL) {
      cvProcessError(NULL, CV_MEM_FAIL, "CVODES",
                     "CVodeGetNumNonlinSolvIters", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    return(SUNNonlinSolGetNumIters(cv_mem->NLS, nniters));
  }

  return(CV_SUCCESS);
}


/* 
 * =================================================================
 * Quadrature optional output functions
 * =================================================================
 */

/*-----------------------------------------------------------------*/

int CVodeGetQuadNumRhsEvals(void *cvode_mem, long int *nfQevals)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuadNumRhsEvals", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_quadr==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUAD, "CVODES", "CVodeGetQuadNumRhsEvals", MSGCV_NO_QUAD); 
    return(CV_NO_QUAD);
  }

  *nfQevals = cv_mem->cv_nfQe;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetQuadNumErrTestFails(void *cvode_mem, long int *nQetfails)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuadNumErrTestFails", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_quadr==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUAD, "CVODES", "CVodeGetQuadNumErrTestFails", MSGCV_NO_QUAD); 
    return(CV_NO_QUAD);
  }

  *nQetfails = cv_mem->cv_netfQ;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetQuadErrWeights(void *cvode_mem, N_Vector eQweight)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuadErrWeights", MSGCV_NO_MEM); 
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_quadr==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUAD, "CVODES", "CVodeGetQuadErrWeights", MSGCV_NO_QUAD); 
    return(CV_NO_QUAD);
  }

  if(cv_mem->cv_errconQ) N_VScale(ONE, cv_mem->cv_ewtQ, eQweight);

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetQuadStats(void *cvode_mem, long int *nfQevals, long int *nQetfails)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuadStats", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_quadr==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUAD, "CVODES", "CVodeGetQuadStats", MSGCV_NO_QUAD); 
    return(CV_NO_QUAD);
  }

  *nfQevals = cv_mem->cv_nfQe;
  *nQetfails = cv_mem->cv_netfQ;

  return(CV_SUCCESS);
}

/* 
 * =================================================================
 * Quadrature FSA optional output functions
 * =================================================================
 */

/*-----------------------------------------------------------------*/

int CVodeGetQuadSensNumRhsEvals(void *cvode_mem, long int *nfQSevals)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuadSensNumRhsEvals", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_quadr_sensi == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUADSENS, "CVODES", "CVodeGetQuadSensNumRhsEvals", MSGCV_NO_QUADSENSI); 
    return(CV_NO_QUADSENS);
  }

  *nfQSevals = cv_mem->cv_nfQSe;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetQuadSensNumErrTestFails(void *cvode_mem, long int *nQSetfails)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuadSensNumErrTestFails", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_quadr_sensi == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUADSENS, "CVODES", "CVodeGetQuadSensNumErrTestFails", MSGCV_NO_QUADSENSI); 
    return(CV_NO_QUADSENS);
  }

  *nQSetfails = cv_mem->cv_netfQS;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetQuadSensErrWeights(void *cvode_mem, N_Vector *eQSweight)
{
  CVodeMem cv_mem;
  int is, Ns;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuadSensErrWeights", MSGCV_NO_MEM); 
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_quadr_sensi == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUADSENS, "CVODES", "CVodeGetQuadSensErrWeights", MSGCV_NO_QUADSENSI); 
    return(CV_NO_QUADSENS);
  }
  Ns = cv_mem->cv_Ns;

  if (cv_mem->cv_errconQS)
    for (is=0; is<Ns; is++)
      N_VScale(ONE, cv_mem->cv_ewtQS[is], eQSweight[is]);

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetQuadSensStats(void *cvode_mem, long int *nfQSevals, long int *nQSetfails)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetQuadSensStats", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_quadr_sensi == SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_QUADSENS, "CVODES", "CVodeGetQuadSensStats", MSGCV_NO_QUADSENSI); 
    return(CV_NO_QUADSENS);
  }

  *nfQSevals = cv_mem->cv_nfQSe;
  *nQSetfails = cv_mem->cv_netfQS;

  return(CV_SUCCESS);
}


/* 
 * =================================================================
 * FSA optional output functions
 * =================================================================
 */

/*-----------------------------------------------------------------*/

int CVodeGetSensNumRhsEvals(void *cvode_mem, long int *nfSevals)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetSensNumRhsEvals", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_sensi==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeGetSensNumRhsEvals", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  *nfSevals = cv_mem->cv_nfSe;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetNumRhsEvalsSens(void *cvode_mem, long int *nfevalsS)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetNumRhsEvalsSens", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_sensi==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeGetNumRhsEvalsSens", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  *nfevalsS = cv_mem->cv_nfeS;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetSensNumErrTestFails(void *cvode_mem, long int *nSetfails)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetSensNumErrTestFails", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_sensi==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeGetSensNumErrTestFails", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  *nSetfails = cv_mem->cv_netfS;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetSensNumLinSolvSetups(void *cvode_mem, long int *nlinsetupsS)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetSensNumLinSolvSetups", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_sensi==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeGetSensNumLinSolvSetups", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  *nlinsetupsS = cv_mem->cv_nsetupsS;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetSensErrWeights(void *cvode_mem, N_Vector *eSweight)
{
  CVodeMem cv_mem;
  int is, Ns;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetSensErrWeights", MSGCV_NO_MEM);    
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_sensi==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeGetSensErrWeights", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  Ns = cv_mem->cv_Ns;

  for (is=0; is<Ns; is++)
    N_VScale(ONE, cv_mem->cv_ewtS[is], eSweight[is]);

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetSensStats(void *cvode_mem, long int *nfSevals, long int *nfevalsS, 
                      long int *nSetfails, long int *nlinsetupsS)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetSensStats", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_sensi==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeGetSensStats", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  *nfSevals = cv_mem->cv_nfSe;
  *nfevalsS = cv_mem->cv_nfeS;
  *nSetfails = cv_mem->cv_netfS;
  *nlinsetupsS = cv_mem->cv_nsetupsS;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetSensNumNonlinSolvIters(void *cvode_mem, long int *nSniters)
{
  CVodeMem cv_mem;
  booleantype sensi_stg;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeGetSensNumNonlinSolvIters", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_sensi==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES",
                   "CVodeGetSensNumNonlinSolvIters", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  /* Are we computing sensitivities with a staggered approach? */
  sensi_stg = (cv_mem->cv_sensi && (cv_mem->cv_ism==CV_STAGGERED));

  if (sensi_stg) {

    /* check that the NLS is non-NULL */
    if (cv_mem->NLSstg == NULL) {
      cvProcessError(NULL, CV_MEM_FAIL, "CVODES",
                      "CVodeGetSensNumNonlinSolvIters", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    return(SUNNonlinSolGetNumIters(cv_mem->NLSstg, nSniters));

  } else {

    /* check that the NLS is non-NULL */
    if (cv_mem->NLSstg1 == NULL) {
      cvProcessError(NULL, CV_MEM_FAIL, "CVODES",
                      "CVodeGetSensNumNonlinSolvIters", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    return(SUNNonlinSolGetNumIters(cv_mem->NLSstg1, nSniters));
  }

}

/*-----------------------------------------------------------------*/

int CVodeGetSensNumNonlinSolvConvFails(void *cvode_mem, long int *nSncfails)
{
  CVodeMem cv_mem;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetSensNumNonlinSolvConvFails", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_sensi==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeGetSensNumNonlinSolvConvFails", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  *nSncfails = cv_mem->cv_ncfnS;

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetStgrSensNumNonlinSolvIters(void *cvode_mem, long int *nSTGR1niters)
{
  CVodeMem cv_mem;
  int is, Ns;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetStgrSensNumNonlinSolvIters", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  Ns = cv_mem->cv_Ns;

  if (cv_mem->cv_sensi==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeGetStgrSensNumNonlinSolvIters", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  if(cv_mem->cv_ism==CV_STAGGERED1) 
    for(is=0; is<Ns; is++) nSTGR1niters[is] = cv_mem->cv_nniS1[is];

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetStgrSensNumNonlinSolvConvFails(void *cvode_mem, long int *nSTGR1ncfails)
{
  CVodeMem cv_mem;
  int is, Ns;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES", "CVodeGetStgrSensNumNonlinSolvConvFails", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  Ns = cv_mem->cv_Ns;

  if (cv_mem->cv_sensi==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES", "CVodeGetStgrSensNumNonlinSolvConvFails", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  if(cv_mem->cv_ism==CV_STAGGERED1) 
    for(is=0; is<Ns; is++) nSTGR1ncfails[is] = cv_mem->cv_ncfnS1[is];

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

int CVodeGetSensNonlinSolvStats(void *cvode_mem, long int *nSniters, 
                                long int *nSncfails)
{
  CVodeMem cv_mem;
  booleantype sensi_stg;

  if (cvode_mem==NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeGetSensNonlinSolvstats", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }

  cv_mem = (CVodeMem) cvode_mem;

  if (cv_mem->cv_sensi==SUNFALSE) {
    cvProcessError(cv_mem, CV_NO_SENS, "CVODES",
                   "CVodeGetSensNonlinSolvStats", MSGCV_NO_SENSI);
    return(CV_NO_SENS);
  }

  *nSncfails = cv_mem->cv_ncfnS;

  /* Are we computing sensitivities with a staggered approach? */
  sensi_stg = (cv_mem->cv_sensi && (cv_mem->cv_ism==CV_STAGGERED));

  if (sensi_stg) {

    /* check that the NLS is non-NULL */
    if (cv_mem->NLSstg == NULL) {
      cvProcessError(NULL, CV_MEM_FAIL, "CVODES",
                      "CVodeGetSensNumNonlinSolvStats", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    return(SUNNonlinSolGetNumIters(cv_mem->NLSstg, nSniters));

  } else {

    /* check that the NLS is non-NULL */
    if (cv_mem->NLSstg1 == NULL) {
      cvProcessError(NULL, CV_MEM_FAIL, "CVODES",
                      "CVodeGetSensNumNonlinSolvStats", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    return(SUNNonlinSolGetNumIters(cv_mem->NLSstg1, nSniters));
  }

  return(CV_SUCCESS);
}

/*-----------------------------------------------------------------*/

char *CVodeGetReturnFlagName(long int flag)
{
  char *name;

  name = (char *)malloc(24*sizeof(char));

  switch(flag) {
  case CV_SUCCESS:
    sprintf(name,"CV_SUCCESS");
    break;
  case CV_TSTOP_RETURN:
    sprintf(name,"CV_TSTOP_RETURN");
    break;
  case CV_ROOT_RETURN:
    sprintf(name,"CV_ROOT_RETURN");
    break;
  case CV_TOO_MUCH_WORK:
    sprintf(name,"CV_TOO_MUCH_WORK");
    break;
  case CV_TOO_MUCH_ACC:
    sprintf(name,"CV_TOO_MUCH_ACC");
    break;
  case CV_ERR_FAILURE:
    sprintf(name,"CV_ERR_FAILURE");
    break;
  case CV_CONV_FAILURE:
    sprintf(name,"CV_CONV_FAILURE");
    break;
  case CV_LINIT_FAIL:
    sprintf(name,"CV_LINIT_FAIL");
    break;
  case CV_LSETUP_FAIL:
    sprintf(name,"CV_LSETUP_FAIL");
    break;
  case CV_LSOLVE_FAIL:
    sprintf(name,"CV_LSOLVE_FAIL");
    break;
  case CV_RHSFUNC_FAIL:
    sprintf(name,"CV_RHSFUNC_FAIL");
    break;
  case CV_FIRST_RHSFUNC_ERR:
    sprintf(name,"CV_FIRST_RHSFUNC_ERR");
    break;
  case CV_REPTD_RHSFUNC_ERR:
    sprintf(name,"CV_REPTD_RHSFUNC_ERR");
    break;
  case CV_UNREC_RHSFUNC_ERR:
    sprintf(name,"CV_UNREC_RHSFUNC_ERR");
    break;
  case CV_RTFUNC_FAIL:
    sprintf(name,"CV_RTFUNC_FAIL");
    break;
  case CV_MEM_FAIL:
    sprintf(name,"CV_MEM_FAIL");
    break;
  case CV_MEM_NULL:
    sprintf(name,"CV_MEM_NULL");
    break;
  case CV_ILL_INPUT:
    sprintf(name,"CV_ILL_INPUT");
    break;
  case CV_NO_MALLOC:
    sprintf(name,"CV_NO_MALLOC");
    break;
  case CV_BAD_K:
    sprintf(name,"CV_BAD_K");
    break;
  case CV_BAD_T:
    sprintf(name,"CV_BAD_T");
    break;
  case CV_BAD_DKY:
    sprintf(name,"CV_BAD_DKY");
    break;
  case CV_NO_QUAD:
    sprintf(name,"CV_NO_QUAD");
    break;
  case CV_QRHSFUNC_FAIL:
    sprintf(name,"CV_QRHSFUNC_FAIL");
    break;
  case CV_FIRST_QRHSFUNC_ERR:
    sprintf(name,"CV_FIRST_QRHSFUNC_ERR");
    break;
  case CV_REPTD_QRHSFUNC_ERR:
    sprintf(name,"CV_REPTD_QRHSFUNC_ERR");
    break;
  case CV_UNREC_QRHSFUNC_ERR:
    sprintf(name,"CV_UNREC_QRHSFUNC_ERR");
    break;
  case CV_BAD_IS:
    sprintf(name,"CV_BAD_IS");
    break;
  case CV_NO_SENS:
    sprintf(name,"CV_NO_SENS");
    break;
  case CV_SRHSFUNC_FAIL:
    sprintf(name,"CV_SRHSFUNC_FAIL");
    break;
  case CV_FIRST_SRHSFUNC_ERR:
    sprintf(name,"CV_FIRST_SRHSFUNC_ERR");
    break;
  case CV_REPTD_SRHSFUNC_ERR:
    sprintf(name,"CV_REPTD_SRHSFUNC_ERR");
    break;
  case CV_UNREC_SRHSFUNC_ERR:
    sprintf(name,"CV_UNREC_SRHSFUNC_ERR");
    break;
  case CV_TOO_CLOSE:
    sprintf(name,"CV_TOO_CLOSE");
    break; 
  case CV_NO_ADJ:
    sprintf(name,"CV_NO_ADJ");
    break;
  case CV_NO_FWD:
    sprintf(name,"CV_NO_FWD");
    break;
  case CV_NO_BCK:
    sprintf(name,"CV_NO_BCK");
    break;
  case CV_BAD_TB0:
    sprintf(name,"CV_BAD_TB0");
    break;
  case CV_REIFWD_FAIL:
    sprintf(name,"CV_REIFWD_FAIL");
    break;
  case CV_FWD_FAIL:
    sprintf(name,"CV_FWD_FAIL");
    break;
  case CV_GETY_BADT:
    sprintf(name,"CV_GETY_BADT");
    break;  
  default:
    sprintf(name,"NONE");
  }

  return(name);
}

