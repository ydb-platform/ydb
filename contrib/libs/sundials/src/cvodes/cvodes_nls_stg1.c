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

/* constant macros */
#define ONE RCONST(1.0)

/* private functions */
static int cvNlsResidualSensStg1(N_Vector ycor, N_Vector res,
                                 void* cvode_mem);
static int cvNlsFPFunctionSensStg1(N_Vector ycor, N_Vector res,
                                   void* cvode_mem);

static int cvNlsLSetupSensStg1(N_Vector ycor, N_Vector res,
                               booleantype jbad, booleantype* jcur,
                               void* cvode_mem);
static int cvNlsLSolveSensStg1(N_Vector ycor, N_Vector delta,
                               void* cvode_mem);
static int cvNlsConvTestSensStg1(SUNNonlinearSolver NLS,
                                 N_Vector ycor, N_Vector del,
                                 realtype tol, N_Vector ewt, void* cvode_mem);

/* -----------------------------------------------------------------------------
 * Exported functions
 * ---------------------------------------------------------------------------*/

int CVodeSetNonlinearSolverSensStg1(void *cvode_mem, SUNNonlinearSolver NLS)
{
  CVodeMem cv_mem;
  int retval;

  /* Return immediately if CVode memory is NULL */
  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "CVodeSetNonlinearSolverSensStg1", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* Return immediately if NLS memory is NULL */
  if (NLS == NULL) {
    cvProcessError(NULL, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg1",
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
                   "CVodeSetNonlinearSolverSensStg1",
                   "NLS does not support required operations");
    return(CV_ILL_INPUT);
  }

  /* check that sensitivities were initialized */
  if (!(cv_mem->cv_sensi)) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg1",
                   MSGCV_NO_SENSI);
    return(CV_ILL_INPUT);
  }

  /* check that staggered corrector was selected */
  if (cv_mem->cv_ism != CV_STAGGERED1) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg1",
                   "Sensitivity solution method is not CV_STAGGERED1");
    return(CV_ILL_INPUT);
  }

  /* free any existing nonlinear solver */
  if ((cv_mem->NLSstg1 != NULL) && (cv_mem->ownNLSstg1))
      retval = SUNNonlinSolFree(cv_mem->NLSstg1);

  /* set SUNNonlinearSolver pointer */
  cv_mem->NLSstg1 = NLS;

  /* Set NLS ownership flag. If this function was called to attach the default
     NLS, CVODE will set the flag to SUNTRUE after this function returns. */
  cv_mem->ownNLSstg1 = SUNFALSE;

  /* set the nonlinear system function */
  if (SUNNonlinSolGetType(NLS) == SUNNONLINEARSOLVER_ROOTFIND) {
    retval = SUNNonlinSolSetSysFn(cv_mem->NLSstg1, cvNlsResidualSensStg1);
  } else if (SUNNonlinSolGetType(NLS) ==  SUNNONLINEARSOLVER_FIXEDPOINT) {
    retval = SUNNonlinSolSetSysFn(cv_mem->NLSstg1, cvNlsFPFunctionSensStg1);
  } else {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg1",
                   "Invalid nonlinear solver type");
    return(CV_ILL_INPUT);
  }

  if (retval != CV_SUCCESS) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg1",
                   "Setting nonlinear system function failed");
    return(CV_ILL_INPUT);
  }

  /* set convergence test function */
  retval = SUNNonlinSolSetConvTestFn(cv_mem->NLSstg1, cvNlsConvTestSensStg1);
  if (retval != CV_SUCCESS) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg1",
                   "Setting convergence test function failed");
    return(CV_ILL_INPUT);
  }

  /* set max allowed nonlinear iterations */
  retval = SUNNonlinSolSetMaxIters(cv_mem->NLSstg1, NLS_MAXCOR);
  if (retval != CV_SUCCESS) {
    cvProcessError(cv_mem, CV_ILL_INPUT, "CVODES",
                   "CVodeSetNonlinearSolverSensStg1",
                   "Setting maximum number of nonlinear iterations failed");
    return(CV_ILL_INPUT);
  }

  return(CV_SUCCESS);
}


/* -----------------------------------------------------------------------------
 * Private functions
 * ---------------------------------------------------------------------------*/


int cvNlsInitSensStg1(CVodeMem cvode_mem)
{
  int retval;

  /* set the linear solver setup wrapper function */
  if (cvode_mem->cv_lsetup)
    retval = SUNNonlinSolSetLSetupFn(cvode_mem->NLSstg1, cvNlsLSetupSensStg1);
  else
    retval = SUNNonlinSolSetLSetupFn(cvode_mem->NLSstg1, NULL);

  if (retval != CV_SUCCESS) {
    cvProcessError(cvode_mem, CV_ILL_INPUT, "CVODES", "cvNlsInitSensStg1",
                   "Setting the linear solver setup function failed");
    return(CV_NLS_INIT_FAIL);
  }

  /* set the linear solver solve wrapper function */
  if (cvode_mem->cv_lsolve)
    retval = SUNNonlinSolSetLSolveFn(cvode_mem->NLSstg1, cvNlsLSolveSensStg1);
  else
    retval = SUNNonlinSolSetLSolveFn(cvode_mem->NLSstg1, NULL);

  if (retval != CV_SUCCESS) {
    cvProcessError(cvode_mem, CV_ILL_INPUT, "CVODES", "cvNlsInitSensStg1",
                   "Setting linear solver solve function failed");
    return(CV_NLS_INIT_FAIL);
  }

  /* initialize nonlinear solver */
  retval = SUNNonlinSolInitialize(cvode_mem->NLSstg1); 

  if (retval != CV_SUCCESS) {
    cvProcessError(cvode_mem, CV_ILL_INPUT, "CVODES", "cvNlsInitSensStg1",
                   MSGCV_NLS_INIT_FAIL);
    return(CV_NLS_INIT_FAIL);
  }

  /* reset previous iteration count for updating nniS1 */
  cvode_mem->nnip = 0;

  return(CV_SUCCESS);
}


static int cvNlsLSetupSensStg1(N_Vector ycor, N_Vector res,
                               booleantype jbad, booleantype* jcur,
                               void* cvode_mem)
{
  CVodeMem cv_mem;
  int retval;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsLSetupSensStg1", MSGCV_NO_MEM);
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


static int cvNlsLSolveSensStg1(N_Vector ycor, N_Vector delta, void* cvode_mem)
{
  CVodeMem cv_mem;
  int retval, is;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsLSolveSensStg1", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* get index of current sensitivity solve */
  is = cv_mem->sens_solve_idx;

  /* solve the sensitivity linear systems */
  retval = cv_mem->cv_lsolve(cv_mem, delta, cv_mem->cv_ewtS[is],
                             cv_mem->cv_y, cv_mem->cv_ftemp);

  if (retval < 0) return(CV_LSOLVE_FAIL);
  if (retval > 0) return(SUN_NLS_CONV_RECVR);

  return(CV_SUCCESS);
}


static int cvNlsConvTestSensStg1(SUNNonlinearSolver NLS,
                                 N_Vector ycor, N_Vector delta,
                                 realtype tol, N_Vector ewt, void* cvode_mem)
{
  CVodeMem cv_mem;
  int m, retval;
  realtype del;
  realtype dcon;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsConvTestSensStg1", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* compute the norm of the state and sensitivity corrections */
  del = N_VWrmsNorm(delta, ewt);

  /* get the current nonlinear solver iteration count */
  retval = SUNNonlinSolGetCurIter(NLS, &m);
  if (retval != CV_SUCCESS) return(CV_MEM_NULL);

  /* Test for convergence. If m > 0, an estimate of the convergence
     rate constant is stored in crate, and used in the test.
  */
  if (m > 0) {
    cv_mem->cv_crateS = SUNMAX(CRDOWN * cv_mem->cv_crateS, del/cv_mem->cv_delp);
  }
  dcon = del * SUNMIN(ONE, cv_mem->cv_crateS) / tol;

  /* check if nonlinear system was solved successfully */
  if (dcon <= ONE) return(CV_SUCCESS);

  /* check if the iteration seems to be diverging */
  if ((m >= 1) && (del > RDIV*cv_mem->cv_delp)) return(SUN_NLS_CONV_RECVR);

  /* Save norm of correction and loop again */
  cv_mem->cv_delp = del;

  /* Not yet converged */
  return(SUN_NLS_CONTINUE);
}


static int cvNlsResidualSensStg1(N_Vector ycor, N_Vector res, void* cvode_mem)
{
  CVodeMem cv_mem;
  int retval, is;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsResidualSensStg1", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* get index of current sensitivity solve */
  is = cv_mem->sens_solve_idx;

  /* update sensitivity based on the current correction */
  N_VLinearSum(ONE, cv_mem->cv_znS[0][is], ONE, ycor, cv_mem->cv_yS[is]);

  /* evaluate the sensitivity rhs function */
  retval = cvSensRhs1Wrapper(cv_mem, cv_mem->cv_tn,
                             cv_mem->cv_y, cv_mem->cv_ftemp,
                             is, cv_mem->cv_yS[is], cv_mem->cv_ftempS[is],
                             cv_mem->cv_vtemp1, cv_mem->cv_vtemp2);

  if (retval < 0) return(CV_SRHSFUNC_FAIL);
  if (retval > 0) return(SRHSFUNC_RECVR);

  /* compute the sensitivity resiudal */
  N_VLinearSum(cv_mem->cv_rl1, cv_mem->cv_znS[1][is], ONE, ycor, res);
  N_VLinearSum(-cv_mem->cv_gamma, cv_mem->cv_ftempS[is], ONE, res, res);

  return(CV_SUCCESS);
}


static int cvNlsFPFunctionSensStg1(N_Vector ycor, N_Vector res, void* cvode_mem)
{
  CVodeMem cv_mem;
  int retval, is;

  if (cvode_mem == NULL) {
    cvProcessError(NULL, CV_MEM_NULL, "CVODES",
                   "cvNlsFPFunctionSensStg1", MSGCV_NO_MEM);
    return(CV_MEM_NULL);
  }
  cv_mem = (CVodeMem) cvode_mem;

  /* get index of current sensitivity solve */
  is = cv_mem->sens_solve_idx;

  /* update the sensitivities based on the current correction */
  N_VLinearSum(ONE, cv_mem->cv_znS[0][is], ONE, ycor, cv_mem->cv_yS[is]);

  /* evaluate the sensitivity rhs function */
  retval = cvSensRhs1Wrapper(cv_mem, cv_mem->cv_tn,
                             cv_mem->cv_y, cv_mem->cv_ftemp,
                             is, cv_mem->cv_yS[is], res,
                             cv_mem->cv_vtemp1, cv_mem->cv_vtemp2);

  if (retval < 0) return(CV_SRHSFUNC_FAIL);
  if (retval > 0) return(SRHSFUNC_RECVR);

  /* evaluate sensitivity fixed point function */
  N_VLinearSum(cv_mem->cv_h, res, -ONE, cv_mem->cv_znS[1][is], res);
  N_VScale(cv_mem->cv_rl1, res, res);

  return(CV_SUCCESS);
}
