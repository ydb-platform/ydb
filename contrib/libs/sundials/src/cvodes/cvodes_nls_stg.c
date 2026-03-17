/* -----------------------------------------------------------------------------
 * Programmer(s): David J. Gardner @ LLNL
 * -----------------------------------------------------------------------------
 * SUNDIALS Copyright Start
 * Copyright (c) 2002-2019, Lawrence Livermore National Security
 * and Southern Methodist University.
 * All rights reserved.
 *
 * See the top-level LICENSE and NOTICE files for details.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 * SUNDIALS Copyright End
 * -----------------------------------------------------------------------------
 * This the implementation file for the CVODES nonlinear solver interface.
 * ---------------------------------------------------------------------------*/

#include "cvodes_impl.h"
#include "sundials/sundials_math.h"
#include "sundials/sundials_nvector_senswrapper.h"

/* constant macros */
#define ONE RCONST(1.0)

/* private functions */
static int cvNlsResidualSensStg(N_Vector ycorStg, N_Vector resStg,
                                void* cvode_mem);
static int cvNlsFPFunctionSensStg(N_Vector ycorStg, N_Vector resStg,
                                  void* cvode_mem);

static int cvNlsLSetupSensStg(N_Vector ycorStg, N_Vector resStg,
                              booleantype jbad, booleantype* jcur,
                              void* cvode_mem);
static int cvNlsLSolveSensStg(N_Vector ycorStg, N_Vector deltaStg,
                              void* cvode_mem);
static int cvNlsConvTestSensStg(SUNNonlinearSolver NLS,
                                N_Vector ycorStg, N_Vector delStg,
                                realtype tol, N_Vector ewtStg, void* cvode_mem);

/* -----------------------------------------------------------------------------
 * Exported functions
 * ---------------------------------------------------------------------------*/

int CVodeSetNonlinearSolverSensStg(void *cvode_mem, SUNNonlinearSolver NLS)
{
  CVodeMem cv_mem;
  int retval, is;

  /* Return immediately if CVode memory is NULL */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeSetNonlinearSolverSensStg", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Return immediately if NLS memory is NULL */
  if (NLS == NULL) {
    cvProcessError(NULL, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg",
                   "NLS must be non-NULL");
    return (CV_ILL_INPUT);
  }

  /* check for required nonlinear solver functions */
  if ( NLS->ops->gettype    == NULL ||
       NLS->ops->initialize == NULL ||
       NLS->ops->solve      == NULL ||
       NLS->ops->free       == NULL ||
       NLS->ops->setsysfn   == NULL ) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg",
                   "NLS does not support required operations");
    return(CV_ILL_INPUT);
  }

  /* check that sensitivities were initialized */
  if (!(cv_mem->cv_sensi)) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg",
                   MSGCV_NO_SENSI);
    return(CV_ILL_INPUT);
  }

  /* check that staggered corrector was selected */
  if (cv_mem->cv_ism != CV_STAGGERED) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg",
                   "Sensitivity solution method is not CV_STAGGERED");
    return(CV_ILL_INPUT);
  }

  /* free any existing nonlinear solver */
  if ((cv_mem->NLSstg != NULL) && (cv_mem->ownNLSstg))
    retval = SUNNonlinSolFree(cv_mem->NLSstg);

  /* set SUNNonlinearSolver pointer */
  cv_mem->NLSstg = NLS;

  /* Set NLS ownership flag. If this function was called to attach the default
     NLS, CVODE will set the flag to SUNTRUE after this function returns. */
  cv_mem->ownNLSstg = SUNFALSE;

  /* set the nonlinear system function */
  if (SUNNonlinSolGetType(NLS) == SUNNONLINEARSOLVER_ROOTFIND) {
    retval = SUNNonlinSolSetSysFn(cv_mem->NLSstg, cvNlsResidualSensStg);
  } else if (SUNNonlinSolGetType(NLS) ==  SUNNONLINEARSOLVER_FIXEDPOINT) {
    retval = SUNNonlinSolSetSysFn(cv_mem->NLSstg, cvNlsFPFunctionSensStg);
  } else {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg",
                   "Invalid nonlinear solver type");
    return(CV_ILL_INPUT);
  }

  if (retval != CV_SUCCESS) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg",
                   "Setting nonlinear system function failed");
    return(CV_ILL_INPUT);
  }

  /* set convergence test function */
  retval = SUNNonlinSolSetConvTestFn(cv_mem->NLSstg, cvNlsConvTestSensStg);
  if (retval != CV_SUCCESS) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg",
                   "Setting convergence test function failed");
    return(CV_ILL_INPUT);
  }

  /* set max allowed nonlinear iterations */
  retval = SUNNonlinSolSetMaxIters(cv_mem->NLSstg, NLS_MAXCOR);
  if (retval != CV_SUCCESS) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg",
                   "Setting maximum number of nonlinear iterations failed");
    return(CV_ILL_INPUT);
  }

  /* create vector wrappers if necessary */
  if (cv_mem->stgMallocDone == SUNFALSE) {

    cv_mem->ycor0Stg = N_VNewEmpty_SensWrapper(cv_mem->cv_Ns);
    if (cv_mem->ycor0Stg == NULL) {
      cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES",
                     "CVodeSetNonlinearSolverSensStg", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    cv_mem->ycorStg = N_VNewEmpty_SensWrapper(cv_mem->cv_Ns);
    if (cv_mem->ycorStg == NULL) {
      N_VDestroy(cv_mem->ycor0Stg);
      cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES",
                     "CVodeSetNonlinearSolverSensStg", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    cv_mem->ewtStg = N_VNewEmpty_SensWrapper(cv_mem->cv_Ns);
    if (cv_mem->ewtStg == NULL) {
      N_VDestroy(cv_mem->ycor0Stg);
      N_VDestroy(cv_mem->ycorStg);
      cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES",
                     "CVodeSetNonlinearSolverSensStg", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    cv_mem->stgMallocDone = SUNTRUE;
  }

  /* attach vectors to vector wrappers */
  for (is=0; is < cv_mem->cv_Ns; is++) {
    NV_VEC_SW(cv_mem->ycor0Stg, is) = cv_mem->cv_tempvS[is];
    NV_VEC_SW(cv_mem->ycorStg,  is) = cv_mem->cv_acorS[is];
    NV_VEC_SW(cv_mem->ewtStg,   is) = cv_mem->cv_ewtS[is];
  }

  return(CV_SUCCESS);
}


/* -----------------------------------------------------------------------------
 * Private functions
 * ---------------------------------------------------------------------------*/


int cvNlsInitSensStg(CVodeMem cvode_mem)
{
  int retval;

  /* set the linear solver setup wrapper function */
  if (cvode_mem->cv_lsetup)
    retval = SUNNonlinSolSetLSetupFn(cvode_mem->NLSstg, cvNlsLSetupSensStg);
  else
    retval = SUNNonlinSolSetLSetupFn(cvode_mem->NLSstg, NULL);

  if (retval != CV_SUCCESS) {
    cvProcessError(cvode_mem, CV_ILL_INPUT, "CVODES", "cvNlsInitSensStg",
                   "Setting the linear solver setup function failed");
    return(CV_NLS_INIT_FAIL);
  }

  /* set the linear solver solve wrapper function */
  if (cvode_mem->cv_lsolve)
    retval = SUNNonlinSolSetLSolveFn(cvode_mem->NLSstg, cvNlsLSolveSensStg);
  else
    retval = SUNNonlinSolSetLSolveFn(cvode_mem->NLSstg, NULL);

  if (retval != CV_SUCCESS) {
    cvProcessError(cvode_mem, CV_ILL_INPUT, "CVODES", "cvNlsInitSensStg",
                   "Setting linear solver solve function failed");
    return(CV_NLS_INIT_FAIL);
  }

  /* initialize nonlinear solver */
  retval = SUNNonlinSolInitialize(cvode_mem->NLSstg);

  if (retval != CV_SUCCESS) {
    cvProcessError(cvode_mem, CV_ILL_INPUT, "CVODES", "cvNlsInitSensStg",
                   MSGCV_NLS_INIT_FAIL);
    return(CV_NLS_INIT_FAIL);
  }

  return(CV_SUCCESS);
}


static int cvNlsLSetupSensStg(N_Vector ycorStg, N_Vector resStg,
                              booleantype jbad, booleantype* jcur,
                              void* cvode_mem)
{
  CVodeMem cv_mem;
  int retval;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsLSetupSensStg", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* if the nonlinear solver marked the Jacobian as bad update convfail */
  if (jbad)
    cv_mem->convfail = CV_FAIL_BAD_J;

  /* setup the linear solver */
  retval = cv_mem->cv_lsetup(cv_mem, cv_mem->convfail, cv_mem->cv_y,
                             cv_mem->cv_ftemp, &(cv_mem->cv_jcur),
                             cv_mem->cv_vtemp1, cv_mem->cv_vtemp2,
                             cv_mem->cv_vtemp3);
  cv_mem->cv_nsetups++;
  cv_mem->cv_nsetupsS++;

  /* update Jacobian status */
  *jcur = cv_mem->cv_jcur;

  cv_mem->cv_gamrat     = ONE;
  cv_mem->cv_gammap     = cv_mem->cv_gamma;
  cv_mem->cv_crate      = ONE;
  cv_mem->cv_crateS     = ONE;
  cv_mem->cv_nstlp      = cv_mem->cv_nst;

  if (retval < 0) return(CV_LSETUP_FAIL);
  if (retval > 0) return(SUN_NLS_CONV_RECVR);

  return(CV_SUCCESS);
}


static int cvNlsLSolveSensStg(N_Vector ycorStg, N_Vector deltaStg, void* cvode_mem)
{
  CVodeMem cv_mem;
  int retval, is;
  N_Vector *deltaS;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsLSolveSensStg", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* extract sensitivity deltas from the vector wrapper */
  deltaS = NV_VECS_SW(deltaStg);

  /* solve the sensitivity linear systems */
  for (is=0; is<cv_mem->cv_Ns; is++) {
    retval = cv_mem->cv_lsolve(cv_mem, deltaS[is], cv_mem->cv_ewtS[is],
                               cv_mem->cv_y, cv_mem->cv_ftemp);

    if (retval < 0) return(CV_LSOLVE_FAIL);
    if (retval > 0) return(SUN_NLS_CONV_RECVR);
  }

  return(CV_SUCCESS);
}


static int cvNlsConvTestSensStg(SUNNonlinearSolver NLS,
                                N_Vector ycorStg, N_Vector deltaStg,
                                realtype tol, N_Vector ewtStg, void* cvode_mem)
{
  CVodeMem cv_mem;
  int m, retval;
  realtype Del;
  realtype dcon;
  N_Vector *ycorS, *deltaS, *ewtS;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsConvTestSensStg", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* extract the current sensitivity corrections */
  ycorS = NV_VECS_SW(ycorStg);

  /* extract the sensitivity deltas */
  deltaS = NV_VECS_SW(deltaStg);

  /* extract the sensitivity error weights */
  ewtS = NV_VECS_SW(ewtStg);

  /* compute the norm of the state and sensitivity corrections */
  Del = cvSensNorm(cv_mem, deltaS, ewtS);

  /* get the current nonlinear solver iteration count */
  retval = SUNNonlinSolGetCurIter(NLS, &m);
  if (retval != CV_SUCCESS) return(CV_MEM_NULL);

  /* Test for convergence. If m > 0, an estimate of the convergence
     rate constant is stored in crate, and used in the test.

     Recall that, even when errconS=SUNFALSE, all variables are used in the
     convergence test. Hence, we use Del (and not del). However, acnrm is used
     in the error test and thus it has different forms depending on errconS
     (and this explains why we have to carry around del and delS).
  */
  if (m > 0) {
    cv_mem->cv_crateS = SUNMAX(CRDOWN * cv_mem->cv_crateS, Del/cv_mem->cv_delp);
  }
  dcon = Del * SUNMIN(ONE, cv_mem->cv_crateS) / tol;

  /* check if nonlinear system was solved successfully */
  if (dcon <= ONE) {
    if (cv_mem->cv_errconS)
      cv_mem->cv_acnrmS = (m==0) ? Del : cvSensNorm(cv_mem, ycorS, ewtS);
    return(CV_SUCCESS);
  }

  /* check if the iteration seems to be diverging */
  if ((m >= 1) && (Del > RDIV*cv_mem->cv_delp)) return(SUN_NLS_CONV_RECVR);

  /* Save norm of correction and loop again */
  cv_mem->cv_delp = Del;

  /* Not yet converged */
  return(SUN_NLS_CONTINUE);
}


static int cvNlsResidualSensStg(N_Vector ycorStg, N_Vector resStg, void* cvode_mem)
{
  CVodeMem cv_mem;
  int retval;
  N_Vector *ycorS, *resS;
  realtype cvals[3];
  N_Vector* XXvecs[3];

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsResidualSensStg", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* extract sensitivity and residual vectors from the vector wrapper */
  ycorS = NV_VECS_SW(ycorStg);
  resS  = NV_VECS_SW(resStg);

  /* update sensitivities based on the current correction */
  retval = N_VLinearSumVectorArray(cv_mem->cv_Ns,
                                   ONE, cv_mem->cv_znS[0],
                                   ONE, ycorS, cv_mem->cv_yS);
  if (retval != CV_SUCCESS) return(CV_VECTOROP_ERR);

  /* evaluate the sensitivity rhs function */
  retval = cvSensRhsWrapper(cv_mem, cv_mem->cv_tn,
                            cv_mem->cv_y, cv_mem->cv_ftemp,
                            cv_mem->cv_yS, cv_mem->cv_ftempS,
                            cv_mem->cv_vtemp1, cv_mem->cv_vtemp2);

  if (retval < 0) return(CV_SRHSFUNC_FAIL);
  if (retval > 0) return(SRHSFUNC_RECVR);

  /* compute the sensitivity resiudal */
  cvals[0] = cv_mem->cv_rl1;    XXvecs[0] = cv_mem->cv_znS[1];
  cvals[1] = ONE;               XXvecs[1] = ycorS;
  cvals[2] = -cv_mem->cv_gamma; XXvecs[2] = cv_mem->cv_ftempS;

  retval = N_VLinearCombinationVectorArray(cv_mem->cv_Ns,
                                           3, cvals, XXvecs, resS);
  if (retval != CV_SUCCESS) return(CV_VECTOROP_ERR);

  return(CV_SUCCESS);
}


static int cvNlsFPFunctionSensStg(N_Vector ycorStg, N_Vector resStg, void* cvode_mem)
{
 CVodeMem cv_mem;
 int retval, is;
 N_Vector *ycorS, *resS;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsFPFunctionSensStg", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* extract sensitivity and residual vectors from the vector wrapper */
  ycorS = NV_VECS_SW(ycorStg);
  resS  = NV_VECS_SW(resStg);

  /* update the sensitivities based on the current correction */
  retval = N_VLinearSumVectorArray(cv_mem->cv_Ns,
                                   ONE, cv_mem->cv_znS[0],
                                   ONE, ycorS, cv_mem->cv_yS);
  if (retval != CV_SUCCESS) return(CV_VECTOROP_ERR);

  /* evaluate the sensitivity rhs function */
  retval = cvSensRhsWrapper(cv_mem, cv_mem->cv_tn,
                            cv_mem->cv_y, cv_mem->cv_ftemp,
                            cv_mem->cv_yS, resS,
                            cv_mem->cv_vtemp1, cv_mem->cv_vtemp2);

  if (retval < 0) return(CV_SRHSFUNC_FAIL);
  if (retval > 0) return(SRHSFUNC_RECVR);

  /* evaluate sensitivity fixed point function */
  for (is=0; is<cv_mem->cv_Ns; is++) {
    N_VLinearSum(cv_mem->cv_h, resS[is], -ONE, cv_mem->cv_znS[1][is], resS[is]);
    N_VScale(cv_mem->cv_rl1, resS[is], resS[is]);
  }

  return(CV_SUCCESS);
}
