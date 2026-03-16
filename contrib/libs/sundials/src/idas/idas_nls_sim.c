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
 * This the implementation file for the IDA nonlinear solver interface.
 * ---------------------------------------------------------------------------*/

#include "idas_impl.h"
#include "sundials/sundials_math.h"
#include "sundials/sundials_nvector_senswrapper.h"

/* constant macros */
#define PT0001  RCONST(0.0001) /* real 0.0001 */
#define ONE     RCONST(1.0)    /* real 1.0    */
#define TWENTY  RCONST(20.0)   /* real 20.0   */

/* nonlinear solver parameters */
#define MAXIT   4           /* default max number of nonlinear iterations    */
#define RATEMAX RCONST(0.9) /* max convergence rate used in divergence check */

/* private functions passed to nonlinear solver */
static int idaNlsResidualSensSim(N_Vector ycor, N_Vector res, void* ida_mem);
static int idaNlsLSetupSensSim(N_Vector ycor, N_Vector res, booleantype jbad,
                               booleantype* jcur, void* ida_mem);
static int idaNlsLSolveSensSim(N_Vector ycor, N_Vector delta, void* ida_mem);
static int idaNlsConvTestSensSim(SUNNonlinearSolver NLS, N_Vector ycor, N_Vector del,
                                 realtype tol, N_Vector ewt, void* ida_mem);

/* -----------------------------------------------------------------------------
 * Exported functions
 * ---------------------------------------------------------------------------*/

int IDASetNonlinearSolverSensSim(void *ida_mem, SUNNonlinearSolver NLS)
{
  IDAMem IDA_mem;
  int retval, is;

  /* return immediately if IDA memory is NULL */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "IDASetNonlinearSolverSensSim", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* return immediately if NLS memory is NULL */
  if (NLS == NULL) {
    IDAProcessError(NULL, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensSim",
                    "NLS must be non-NULL");
    return(IDA_ILL_INPUT);
  }

  /* check for required nonlinear solver functions */
  if ( NLS->ops->gettype    == NULL ||
       NLS->ops->initialize == NULL ||
       NLS->ops->solve      == NULL ||
       NLS->ops->free       == NULL ||
       NLS->ops->setsysfn   == NULL ) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensSim",
                    "NLS does not support required operations");
    return(IDA_ILL_INPUT);
  }

  /* check for allowed nonlinear solver types */
  if (SUNNonlinSolGetType(NLS) != SUNNONLINEARSOLVER_ROOTFIND) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensSim",
                    "NLS type must be SUNNONLINEARSOLVER_ROOTFIND");
    return(IDA_ILL_INPUT);
  }

  /* check that sensitivities were initialized */
  if (!(IDA_mem->ida_sensi)) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensSim",
                    MSG_NO_SENSI);
    return(IDA_ILL_INPUT);
  }

  /* check that the simultaneous corrector was selected */
  if (IDA_mem->ida_ism != IDA_SIMULTANEOUS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensSim",
                    "Sensitivity solution method is not IDA_SIMULTANEOUS");
    return(IDA_ILL_INPUT);
  }

  /* free any existing nonlinear solver */
  if ((IDA_mem->NLSsim != NULL) && (IDA_mem->ownNLSsim))
    retval = SUNNonlinSolFree(IDA_mem->NLSsim);

  /* set SUNNonlinearSolver pointer */
  IDA_mem->NLSsim = NLS;

  /* Set NLS ownership flag. If this function was called to attach the default
     NLS, IDA will set the flag to SUNTRUE after this function returns. */
  IDA_mem->ownNLSsim = SUNFALSE;

  /* set the nonlinear residual function */
  retval = SUNNonlinSolSetSysFn(IDA_mem->NLSsim, idaNlsResidualSensSim);
  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensSim",
                    "Setting nonlinear system function failed");
    return(IDA_ILL_INPUT);
  }

  /* set convergence test function */
  retval = SUNNonlinSolSetConvTestFn(IDA_mem->NLSsim, idaNlsConvTestSensSim);
  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensSim",
                    "Setting convergence test function failed");
    return(IDA_ILL_INPUT);
  }

  /* set max allowed nonlinear iterations */
  retval = SUNNonlinSolSetMaxIters(IDA_mem->NLSsim, MAXIT);
  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensSim",
                    "Setting maximum number of nonlinear iterations failed");
    return(IDA_ILL_INPUT);
  }

  /* create vector wrappers if necessary */
  if (IDA_mem->simMallocDone == SUNFALSE) {

    IDA_mem->ycor0Sim = N_VNewEmpty_SensWrapper(IDA_mem->ida_Ns+1);
    if (IDA_mem->ycor0Sim == NULL) {
      IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS",
                      "IDASetNonlinearSolverSensSim", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    IDA_mem->ycorSim = N_VNewEmpty_SensWrapper(IDA_mem->ida_Ns+1);
    if (IDA_mem->ycorSim == NULL) {
      N_VDestroy(IDA_mem->ycor0Sim);
      IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS",
                      "IDASetNonlinearSolverSensSim", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    IDA_mem->ewtSim = N_VNewEmpty_SensWrapper(IDA_mem->ida_Ns+1);
    if (IDA_mem->ewtSim == NULL) {
      N_VDestroy(IDA_mem->ycor0Sim);
      N_VDestroy(IDA_mem->ycorSim);
      IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS",
                      "IDASetNonlinearSolverSensSim", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    IDA_mem->simMallocDone = SUNTRUE;
  }

  /* attach vectors to vector wrappers */
  NV_VEC_SW(IDA_mem->ycor0Sim, 0) = IDA_mem->ida_delta;
  NV_VEC_SW(IDA_mem->ycorSim,  0) = IDA_mem->ida_ee;
  NV_VEC_SW(IDA_mem->ewtSim,   0) = IDA_mem->ida_ewt;

  for (is=0; is < IDA_mem->ida_Ns; is++) {
    NV_VEC_SW(IDA_mem->ycor0Sim, is+1) = IDA_mem->ida_deltaS[is];
    NV_VEC_SW(IDA_mem->ycorSim,  is+1) = IDA_mem->ida_eeS[is];
    NV_VEC_SW(IDA_mem->ewtSim,   is+1) = IDA_mem->ida_ewtS[is];
  }

  return(IDA_SUCCESS);
}


/* -----------------------------------------------------------------------------
 * Private functions
 * ---------------------------------------------------------------------------*/

int idaNlsInitSensSim(IDAMem IDA_mem)
{
  int retval;

  /* set the linear solver setup wrapper function */
  if (IDA_mem->ida_lsetup)
    retval = SUNNonlinSolSetLSetupFn(IDA_mem->NLSsim, idaNlsLSetupSensSim);
  else
    retval = SUNNonlinSolSetLSetupFn(IDA_mem->NLSsim, NULL);

  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "idaNlsInitSnesSim",
                    "Setting the linear solver setup function failed");
    return(IDA_NLS_INIT_FAIL);
  }

  /* set the linear solver solve wrapper function */
  if (IDA_mem->ida_lsolve)
    retval = SUNNonlinSolSetLSolveFn(IDA_mem->NLSsim, idaNlsLSolveSensSim);
  else
    retval = SUNNonlinSolSetLSolveFn(IDA_mem->NLSsim, NULL);

  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "idaNlsInitSnesSim",
                    "Setting linear solver solve function failed");
    return(IDA_NLS_INIT_FAIL);
  }

  /* initialize nonlinear solver */
  retval = SUNNonlinSolInitialize(IDA_mem->NLSsim);

  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "idaNlsInitSnesSim",
                    MSG_NLS_INIT_FAIL);
    return(IDA_NLS_INIT_FAIL);
  }

  return(IDA_SUCCESS);
}


static int idaNlsLSetupSensSim(N_Vector ycorSim, N_Vector resSim,
                               booleantype jbad, booleantype* jcur,
                               void* ida_mem)
{
  IDAMem IDA_mem;
  int retval;
  N_Vector res;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "idaNlsLSetupSensSim", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* extract residual vector from the vector wrapper */
  res = NV_VEC_SW(resSim,0);

  IDA_mem->ida_nsetups++;
  IDA_mem->ida_forceSetup = SUNFALSE;

  retval = IDA_mem->ida_lsetup(IDA_mem, IDA_mem->ida_yy, IDA_mem->ida_yp, res,
                               IDA_mem->ida_tempv1, IDA_mem->ida_tempv2, IDA_mem->ida_tempv3);

  /* update Jacobian status */
  *jcur = SUNTRUE;

  /* update convergence test constants */
  IDA_mem->ida_cjold = IDA_mem->ida_cj;
  IDA_mem->ida_cjratio = ONE;
  IDA_mem->ida_ss = TWENTY;
  IDA_mem->ida_ssS = TWENTY;

  if (retval < 0) return(IDA_LSETUP_FAIL);
  if (retval > 0) return(IDA_LSETUP_RECVR);

  return(IDA_SUCCESS);
}


static int idaNlsLSolveSensSim(N_Vector ycorSim, N_Vector deltaSim, void* ida_mem)
{
  IDAMem IDA_mem;
  int retval, is;
  N_Vector delta;
  N_Vector *deltaS;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "idaNlsLSolveSensSim", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* extract state update vector from the vector wrapper */
  delta = NV_VEC_SW(deltaSim,0);

  /* solve the state linear system */
  retval = IDA_mem->ida_lsolve(IDA_mem, delta, IDA_mem->ida_ewt,
                               IDA_mem->ida_yy, IDA_mem->ida_yp,
                               IDA_mem->ida_savres);

  if (retval < 0) return(IDA_LSOLVE_FAIL);
  if (retval > 0) return(IDA_LSOLVE_RECVR);

  /* extract sensitivity deltas from the vector wrapper */
  deltaS = NV_VECS_SW(deltaSim)+1;

  /* solve the sensitivity linear systems */
  for(is=0; is<IDA_mem->ida_Ns; is++) {
    retval = IDA_mem->ida_lsolve(IDA_mem, deltaS[is], IDA_mem->ida_ewtS[is],
                                 IDA_mem->ida_yy, IDA_mem->ida_yp,
                                 IDA_mem->ida_savres);

    if (retval < 0) return(IDA_LSOLVE_FAIL);
    if (retval > 0) return(IDA_LSOLVE_RECVR);
  }

  return(IDA_SUCCESS);
}


static int idaNlsResidualSensSim(N_Vector ycorSim, N_Vector resSim, void* ida_mem)
{
  IDAMem IDA_mem;
  int retval;
  N_Vector ycor, res;
  N_Vector *ycorS, *resS;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "idaNlsResidualSensSim", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* extract state and residual vectors from the vector wrapper */
  ycor = NV_VEC_SW(ycorSim,0);
  res  = NV_VEC_SW(resSim,0);

  /* update yy and yp based on the current correction */
  N_VLinearSum(ONE, IDA_mem->ida_yypredict, ONE, ycor, IDA_mem->ida_yy);
  N_VLinearSum(ONE, IDA_mem->ida_yppredict, IDA_mem->ida_cj, ycor, IDA_mem->ida_yp);

  /* evaluate residual */
  retval = IDA_mem->ida_res(IDA_mem->ida_tn, IDA_mem->ida_yy, IDA_mem->ida_yp,
                            res, IDA_mem->ida_user_data);

  /* increment the number of residual evaluations */
  IDA_mem->ida_nre++;

  /* save a copy of the residual vector in savres */
  N_VScale(ONE, res, IDA_mem->ida_savres);

  if (retval < 0) return(IDA_RES_FAIL);
  if (retval > 0) return(IDA_RES_RECVR);

  /* extract sensitivity and residual vectors from the vector wrapper */
  ycorS = NV_VECS_SW(ycorSim)+1;
  resS  = NV_VECS_SW(resSim)+1;

  /* update yS and ypS based on the current correction */
  N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                          ONE, IDA_mem->ida_yySpredict,
                          ONE, ycorS, IDA_mem->ida_yyS);
  N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                          ONE, IDA_mem->ida_ypSpredict,
                          IDA_mem->ida_cj, ycorS, IDA_mem->ida_ypS);

  /* evaluate sens residual */
  retval = IDA_mem->ida_resS(IDA_mem->ida_Ns, IDA_mem->ida_tn,
                             IDA_mem->ida_yy, IDA_mem->ida_yp, res,
                             IDA_mem->ida_yyS, IDA_mem->ida_ypS, resS,
                             IDA_mem->ida_user_dataS, IDA_mem->ida_tmpS1,
                             IDA_mem->ida_tmpS2, IDA_mem->ida_tmpS3);

  /* increment the number of sens residual evaluations */
  IDA_mem->ida_nrSe++;

  if (retval < 0) return(IDA_SRES_FAIL);
  if (retval > 0) return(IDA_SRES_RECVR);

  return(IDA_SUCCESS);
}


static int idaNlsConvTestSensSim(SUNNonlinearSolver NLS, N_Vector ycor, N_Vector del,
                                 realtype tol, N_Vector ewt, void* ida_mem)
{
  IDAMem IDA_mem;
  int m, retval;
  realtype delnrm;
  realtype rate;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "idaNlsConvTestSensSim", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* compute the norm of the correction */
  delnrm = N_VWrmsNorm(del, ewt);

  /* get the current nonlinear solver iteration count */
  retval = SUNNonlinSolGetCurIter(NLS, &m);
  if (retval != IDA_SUCCESS) return(IDA_MEM_NULL);

  /* test for convergence, first directly, then with rate estimate. */
  if (m == 0){
    IDA_mem->ida_oldnrm = delnrm;
    if (delnrm <= PT0001 * IDA_mem->ida_toldel) return(SUN_NLS_SUCCESS);
  } else {
    rate = SUNRpowerR( delnrm/IDA_mem->ida_oldnrm, ONE/m );
    if (rate > RATEMAX) return(SUN_NLS_CONV_RECVR);
    IDA_mem->ida_ss = rate/(ONE - rate);
  }

  if (IDA_mem->ida_ss*delnrm <= tol) return(SUN_NLS_SUCCESS);

  /* not yet converged */
  return(SUN_NLS_CONTINUE);
}
