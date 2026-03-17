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

/*
 * When sensitivities are computed using the CV_SIMULTANEOUS approach and the
 * Newton solver is selected the iteraiton is a  quasi-Newton method on the
 * combined system (by approximating the Jacobian matrix by its block diagonal)
 * and thus only solve linear systems with multiple right hand sides (all
 * sharing the same coefficient matrix - whatever iteration matrix we decide on)
 * we set-up the linear solver to handle N equations at a time.
 */

#include "cvodes_impl.h"
#include "sundials/sundials_math.h"
#include "sundials/sundials_nvector_senswrapper.h"

/* constant macros */
#define ONE RCONST(1.0)

/* private functions */
static int cvNlsResidualSensSim(N_Vector ycorSim, N_Vector resSim,
                                void* cvode_mem);
static int cvNlsFPFunctionSensSim(N_Vector ycorSim, N_Vector resSim,
                                  void* cvode_mem);

static int cvNlsLSetupSensSim(N_Vector ycorSim, N_Vector resSim,
                              booleantype jbad, booleantype* jcur,
                              void* cvode_mem);
static int cvNlsLSolveSensSim(N_Vector ycorSim, N_Vector deltaSim,
                              void* cvode_mem);
static int cvNlsConvTestSensSim(SUNNonlinearSolver NLS,
                                N_Vector ycorSim, N_Vector delSim,
                                realtype tol, N_Vector ewtSim, void* cvode_mem);

/* -----------------------------------------------------------------------------
 * Exported functions
 * ---------------------------------------------------------------------------*/

int CVodeSetNonlinearSolverSensSim(void *cvode_mem, SUNNonlinearSolver NLS)
{
  CVodeMem cv_mem;
  int retval, is;

  /* Return immediately if CVode memory is NULL */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeSetNonlinearSolverSensSim", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Return immediately if NLS memory is NULL */
  if (NLS == NULL) {
    cvProcessError(NULL, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensSim",
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
                   "CVodeSetNonlinearSolverSensSim",
                   "NLS does not support required operations");
    return(CV_ILL_INPUT);
  }

  /* check that sensitivities were initialized */
  if (!(cv_mem->cv_sensi)) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensSim",
                   MSGCV_NO_SENSI);
    return(CV_ILL_INPUT);
  }

  /* check that simultaneous corrector was selected */
  if (cv_mem->cv_ism != CV_SIMULTANEOUS) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg",
                   "Sensitivity solution method is not CV_SIMULTANEOUS");
    return(CV_ILL_INPUT);
  }

  /* free any existing nonlinear solver */
  if ((cv_mem->NLSsim != NULL) && (cv_mem->ownNLSsim))
    retval = SUNNonlinSolFree(cv_mem->NLSsim);

  /* set SUNNonlinearSolver pointer */
  cv_mem->NLSsim = NLS;

  /* Set NLS ownership flag. If this function was called to attach the default
     NLS, CVODE will set the flag to SUNTRUE after this function returns. */
  cv_mem->ownNLSsim = SUNFALSE;

  /* set the nonlinear system function */
  if (SUNNonlinSolGetType(NLS) == SUNNONLINEARSOLVER_ROOTFIND) {
    retval = SUNNonlinSolSetSysFn(cv_mem->NLSsim, cvNlsResidualSensSim);
  } else if (SUNNonlinSolGetType(NLS) ==  SUNNONLINEARSOLVER_FIXEDPOINT) {
    retval = SUNNonlinSolSetSysFn(cv_mem->NLSsim, cvNlsFPFunctionSensSim);
  } else {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensSim",
                   "Invalid nonlinear solver type");
    return(CV_ILL_INPUT);
  }

  if (retval != CV_SUCCESS) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensSim",
                   "Setting nonlinear system function failed");
    return(CV_ILL_INPUT);
  }

  /* set convergence test function */
  retval = SUNNonlinSolSetConvTestFn(cv_mem->NLSsim, cvNlsConvTestSensSim);
  if (retval != CV_SUCCESS) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensSim",
                   "Setting convergence test function failed");
    return(CV_ILL_INPUT);
  }

  /* set max allowed nonlinear iterations */
  retval = SUNNonlinSolSetMaxIters(cv_mem->NLSsim, NLS_MAXCOR);
  if (retval != CV_SUCCESS) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensSim",
                   "Setting maximum number of nonlinear iterations failed");
    return(CV_ILL_INPUT);
  }

  /* create vector wrappers if necessary */
  if (cv_mem->simMallocDone == SUNFALSE) {

    cv_mem->ycor0Sim = N_VNewEmpty_SensWrapper(cv_mem->cv_Ns+1);
    if (cv_mem->ycor0Sim == NULL) {
      cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES",
                     "CVodeSetNonlinearSolverSensSim", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    cv_mem->ycorSim = N_VNewEmpty_SensWrapper(cv_mem->cv_Ns+1);
    if (cv_mem->ycorSim == NULL) {
      N_VDestroy(cv_mem->ycor0Sim);
      cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES",
                     "CVodeSetNonlinearSolverSensSim", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    cv_mem->ewtSim = N_VNewEmpty_SensWrapper(cv_mem->cv_Ns+1);
    if (cv_mem->ewtSim == NULL) {
      N_VDestroy(cv_mem->ycor0Sim);
      N_VDestroy(cv_mem->ycorSim);
      cvProcessError(cv_mem, CV_MEM_FAIL, "CVODES",
                     "CVodeSetNonlinearSolverSensSim", MSGCV_MEM_FAIL);
      return(CV_MEM_FAIL);
    }

    cv_mem->simMallocDone = SUNTRUE;
  }

  /* attach vectors to vector wrappers */
  NV_VEC_SW(cv_mem->ycor0Sim, 0) = cv_mem->cv_tempv;
  NV_VEC_SW(cv_mem->ycorSim,  0) = cv_mem->cv_acor;
  NV_VEC_SW(cv_mem->ewtSim,   0) = cv_mem->cv_ewt;

  for (is=0; is < cv_mem->cv_Ns; is++) {
    NV_VEC_SW(cv_mem->ycor0Sim, is+1) = cv_mem->cv_tempvS[is];
    NV_VEC_SW(cv_mem->ycorSim,  is+1) = cv_mem->cv_acorS[is];
    NV_VEC_SW(cv_mem->ewtSim,   is+1) = cv_mem->cv_ewtS[is];
  }

  return(CV_SUCCESS);
}


/* -----------------------------------------------------------------------------
 * Private functions
 * ---------------------------------------------------------------------------*/


int cvNlsInitSensSim(CVodeMem cvode_mem)
{
  int retval;

  /* set the linear solver setup wrapper function */
  if (cvode_mem->cv_lsetup)
    retval = SUNNonlinSolSetLSetupFn(cvode_mem->NLSsim, cvNlsLSetupSensSim);
  else
    retval = SUNNonlinSolSetLSetupFn(cvode_mem->NLSsim, NULL);

  if (retval != CV_SUCCESS) {
    cvProcessError(cvode_mem, CV_ILL_INPUT, "CVODES", "cvNlsInitSensSim",
                   "Setting the linear solver setup function failed");
    return(CV_NLS_INIT_FAIL);
  }

  /* set the linear solver solve wrapper function */
  if (cvode_mem->cv_lsolve)
    retval = SUNNonlinSolSetLSolveFn(cvode_mem->NLSsim, cvNlsLSolveSensSim);
  else
    retval = SUNNonlinSolSetLSolveFn(cvode_mem->NLSsim, NULL);

  if (retval != CV_SUCCESS) {
    cvProcessError(cvode_mem, CV_ILL_INPUT, "CVODES", "cvNlsInitSensSim",
                   "Setting linear solver solve function failed");
    return(CV_NLS_INIT_FAIL);
  }

  /* initialize nonlinear solver */
  retval = SUNNonlinSolInitialize(cvode_mem->NLSsim);

  if (retval != CV_SUCCESS) {
    cvProcessError(cvode_mem, CV_ILL_INPUT, "CVODES", "cvNlsInitSensSim",
                   MSGCV_NLS_INIT_FAIL);
    return(CV_NLS_INIT_FAIL);
  }

  return(CV_SUCCESS);
}


static int cvNlsLSetupSensSim(N_Vector ycorSim, N_Vector resSim,
                              booleantype jbad, booleantype* jcur,
                              void* cvode_mem)
{
  CVodeMem cv_mem;
  int retval;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsLSetupSensSim", MSGCV_NO_MEM);
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

  /* update Jacobian status */
  *jcur = cv_mem->cv_jcur;

  cv_mem->cv_forceSetup = SUNFALSE;
  cv_mem->cv_gamrat     = ONE;
  cv_mem->cv_gammap     = cv_mem->cv_gamma;
  cv_mem->cv_crate      = ONE;
  cv_mem->cv_crateS     = ONE;
  cv_mem->cv_nstlp      = cv_mem->cv_nst;

  if (retval < 0) return(CV_LSETUP_FAIL);
  if (retval > 0) return(SUN_NLS_CONV_RECVR);

  return(CV_SUCCESS);
}


static int cvNlsLSolveSensSim(N_Vector ycorSim, N_Vector deltaSim, void* cvode_mem)
{
  CVodeMem cv_mem;
  int retval, is;
  N_Vector delta;
  N_Vector *deltaS;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsLSolveSensSim", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* extract state delta from the vector wrapper */
  delta = NV_VEC_SW(deltaSim,0);

  /* solve the state linear system */
  retval = cv_mem->cv_lsolve(cv_mem, delta, cv_mem->cv_ewt, cv_mem->cv_y,
                             cv_mem->cv_ftemp);

  if (retval < 0) return(CV_LSOLVE_FAIL);
  if (retval > 0) return(SUN_NLS_CONV_RECVR);

  /* extract sensitivity deltas from the vector wrapper */
  deltaS = NV_VECS_SW(deltaSim)+1;

  /* solve the sensitivity linear systems */
  for (is=0; is<cv_mem->cv_Ns; is++) {
    retval = cv_mem->cv_lsolve(cv_mem, deltaS[is], cv_mem->cv_ewtS[is],
                               cv_mem->cv_y, cv_mem->cv_ftemp);

    if (retval < 0) return(CV_LSOLVE_FAIL);
    if (retval > 0) return(SUN_NLS_CONV_RECVR);
  }

  return(CV_SUCCESS);
}


static int cvNlsConvTestSensSim(SUNNonlinearSolver NLS,
                                N_Vector ycorSim, N_Vector deltaSim,
                                realtype tol, N_Vector ewtSim, void* cvode_mem)
{
  CVodeMem cv_mem;
  int m, retval;
  realtype del, delS, Del;
  realtype dcon;
  N_Vector ycor, delta, ewt;
  N_Vector *deltaS, *ewtS;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsConvTestSensSim", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* extract the current state and sensitivity corrections */
  ycor  = NV_VEC_SW(ycorSim,0);

  /* extract state and sensitivity deltas */
  delta  = NV_VEC_SW(deltaSim,0);
  deltaS = NV_VECS_SW(deltaSim)+1;

  /* extract state and sensitivity error weights */
  ewt  = NV_VEC_SW(ewtSim,0);
  ewtS = NV_VECS_SW(ewtSim)+1;

  /* compute the norm of the state and sensitivity corrections */
  del  = N_VWrmsNorm(delta, ewt);
  delS = cvSensUpdateNorm(cv_mem, del, deltaS, ewtS);

  /* norm used in error test */
  Del = delS;

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
    cv_mem->cv_crate = SUNMAX(CRDOWN * cv_mem->cv_crate, Del/cv_mem->cv_delp);
  }
  dcon = Del * SUNMIN(ONE, cv_mem->cv_crate) / tol;

  /* check if nonlinear system was solved successfully */
  if (dcon <= ONE) {
    if (m == 0) {
      cv_mem->cv_acnrm = (cv_mem->cv_errconS) ? delS : del;
    } else {
      cv_mem->cv_acnrm = (cv_mem->cv_errconS) ?
        N_VWrmsNorm(ycorSim, ewtSim) : N_VWrmsNorm(ycor, ewt);
    }
    return(CV_SUCCESS);
  }

  /* check if the iteration seems to be diverging */
  if ((m >= 1) && (Del > RDIV*cv_mem->cv_delp)) return(SUN_NLS_CONV_RECVR);

  /* Save norm of correction and loop again */
  cv_mem->cv_delp = Del;

  /* Not yet converged */
  return(SUN_NLS_CONTINUE);
}


static int cvNlsResidualSensSim(N_Vector ycorSim, N_Vector resSim, void* cvode_mem)
{
  CVodeMem cv_mem;
  int retval;
  N_Vector ycor, res;
  N_Vector *ycorS, *resS;
  realtype cvals[3];
  N_Vector* XXvecs[3];

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsResidualSensSim", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* extract state and residual vectors from the vector wrapper */
  ycor = NV_VEC_SW(ycorSim,0);
  res  = NV_VEC_SW(resSim,0);

  /* update the state based on the current correction */
  N_VLinearSum(ONE, cv_mem->cv_zn[0], ONE, ycor, cv_mem->cv_y);

  /* evaluate the rhs function */
  retval = cv_mem->cv_f(cv_mem->cv_tn, cv_mem->cv_y, cv_mem->cv_ftemp,
                        cv_mem->cv_user_data);
  cv_mem->cv_nfe++;
  if (retval < 0) return(CV_RHSFUNC_FAIL);
  if (retval > 0) return(RHSFUNC_RECVR);

  /* compute the resiudal */
  N_VLinearSum(cv_mem->cv_rl1, cv_mem->cv_zn[1], ONE, ycor, res);
  N_VLinearSum(-cv_mem->cv_gamma, cv_mem->cv_ftemp, ONE, res, res);

  /* extract sensitivity and residual vectors from the vector wrapper */
  ycorS = NV_VECS_SW(ycorSim)+1;
  resS  = NV_VECS_SW(resSim)+1;

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


static int cvNlsFPFunctionSensSim(N_Vector ycorSim, N_Vector resSim, void* cvode_mem)
{
 CVodeMem cv_mem;
 int retval, is;
 N_Vector ycor, res;
 N_Vector *ycorS, *resS;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsFPFunctionSensSim", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* extract state and residual vectors from the vector wrapper */
  ycor = NV_VEC_SW(ycorSim,0);
  res  = NV_VEC_SW(resSim,0);

  /* update the state based on the current correction */
  N_VLinearSum(ONE, cv_mem->cv_zn[0], ONE, ycor, cv_mem->cv_y);

  /* evaluate the rhs function */
  retval = cv_mem->cv_f(cv_mem->cv_tn, cv_mem->cv_y, res,
                        cv_mem->cv_user_data);
  cv_mem->cv_nfe++;
  if (retval < 0) return(CV_RHSFUNC_FAIL);
  if (retval > 0) return(RHSFUNC_RECVR);

  /* evaluate fixed point function */
  N_VLinearSum(cv_mem->cv_h, res, -ONE, cv_mem->cv_zn[1], res);
  N_VScale(cv_mem->cv_rl1, res, res);

  /* extract sensitivity and residual vectors from the vector wrapper */
  ycorS = NV_VECS_SW(ycorSim)+1;
  resS  = NV_VECS_SW(resSim)+1;

  /* update the sensitivities based on the current correction */
  N_VLinearSumVectorArray(cv_mem->cv_Ns,
                          ONE, cv_mem->cv_znS[0],
                          ONE, ycorS, cv_mem->cv_yS);

  /* evaluate the sensitivity rhs function */
  retval = cvSensRhsWrapper(cv_mem, cv_mem->cv_tn,
                            cv_mem->cv_y, res,
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
