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
static int idaNlsResidualSensStg(N_Vector ycor, N_Vector res, void* ida_mem);
static int idaNlsLSetupSensStg(N_Vector ycor, N_Vector res, booleantype jbad,
                               booleantype* jcur, void* ida_mem);
static int idaNlsLSolveSensStg(N_Vector ycor, N_Vector delta, void* ida_mem);
static int idaNlsConvTestSensStg(SUNNonlinearSolver NLS, N_Vector ycor, N_Vector del,
                                 realtype tol, N_Vector ewt, void* ida_mem);

/* -----------------------------------------------------------------------------
 * Exported functions
 * ---------------------------------------------------------------------------*/

int IDASetNonlinearSolverSensStg(void *ida_mem, SUNNonlinearSolver NLS)
{
  IDAMem IDA_mem;
  int retval, is;

  /* return immediately if IDA memory is NULL */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "IDASetNonlinearSolverSensStg", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* return immediately if NLS memory is NULL */
  if (NLS == NULL) {
    IDAProcessError(NULL, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensStg",
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
                    "IDASetNonlinearSolverSensStg",
                    "NLS does not support required operations");
    return(IDA_ILL_INPUT);
  }

  /* check for allowed nonlinear solver types */
  if (SUNNonlinSolGetType(NLS) != SUNNONLINEARSOLVER_ROOTFIND) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensStg",
                    "NLS type must be SUNNONLINEARSOLVER_ROOTFIND");
    return(IDA_ILL_INPUT);
  }

  /* check that sensitivities were initialized */
  if (!(IDA_mem->ida_sensi)) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensStg",
                    MSG_NO_SENSI);
    return(IDA_ILL_INPUT);
  }

  /* check that the staggered corrector was selected */
  if (IDA_mem->ida_ism != IDA_STAGGERED) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensStg",
                    "Sensitivity solution method is not IDA_STAGGERED");
    return(IDA_ILL_INPUT);
  }

  /* free any existing nonlinear solver */
  if ((IDA_mem->NLSstg != NULL) && (IDA_mem->ownNLSstg))
    retval = SUNNonlinSolFree(IDA_mem->NLSstg);

  /* set SUNNonlinearSolver pointer */
  IDA_mem->NLSstg = NLS;

  /* Set NLS ownership flag. If this function was called to attach the default
     NLS, IDA will set the flag to SUNTRUE after this function returns. */
  IDA_mem->ownNLSstg = SUNFALSE;

  /* set the nonlinear residual function */
  retval = SUNNonlinSolSetSysFn(IDA_mem->NLSstg, idaNlsResidualSensStg);
  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensStg",
                    "Setting nonlinear system function failed");
    return(IDA_ILL_INPUT);
  }

  /* set convergence test function */
  retval = SUNNonlinSolSetConvTestFn(IDA_mem->NLSstg, idaNlsConvTestSensStg);
  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensStg",
                    "Setting convergence test function failed");
    return(IDA_ILL_INPUT);
  }

  /* set max allowed nonlinear iterations */
  retval = SUNNonlinSolSetMaxIters(IDA_mem->NLSstg, MAXIT);
  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolverSensStg",
                    "Setting maximum number of nonlinear iterations failed");
    return(IDA_ILL_INPUT);
  }

  /* create vector wrappers if necessary */
  if (IDA_mem->stgMallocDone == SUNFALSE) {

    IDA_mem->ycor0Stg = N_VNewEmpty_SensWrapper(IDA_mem->ida_Ns);
    if (IDA_mem->ycor0Stg == NULL) {
      IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS",
                      "IDASetNonlinearSolverSensStg", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    IDA_mem->ycorStg = N_VNewEmpty_SensWrapper(IDA_mem->ida_Ns);
    if (IDA_mem->ycorStg == NULL) {
      N_VDestroy(IDA_mem->ycor0Stg);
      IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS",
                      "IDASetNonlinearSolverSensStg", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    IDA_mem->ewtStg = N_VNewEmpty_SensWrapper(IDA_mem->ida_Ns);
    if (IDA_mem->ewtStg == NULL) {
      N_VDestroy(IDA_mem->ycor0Stg);
      N_VDestroy(IDA_mem->ycorStg);
      IDAProcessError(IDA_mem, IDA_MEM_FAIL, "IDAS",
                      "IDASetNonlinearSolverSensStg", MSG_MEM_FAIL);
      return(IDA_MEM_FAIL);
    }

    IDA_mem->stgMallocDone = SUNTRUE;
  }

  /* attach vectors to vector wrappers */
  for (is=0; is < IDA_mem->ida_Ns; is++) {
    NV_VEC_SW(IDA_mem->ycor0Stg, is) = IDA_mem->ida_deltaS[is];
    NV_VEC_SW(IDA_mem->ycorStg,  is) = IDA_mem->ida_eeS[is];
    NV_VEC_SW(IDA_mem->ewtStg,   is) = IDA_mem->ida_ewtS[is];
  }

  return(IDA_SUCCESS);
}


/* -----------------------------------------------------------------------------
 * Private functions
 * ---------------------------------------------------------------------------*/

int idaNlsInitSensStg(IDAMem IDA_mem)
{
  int retval;

  /* set the linear solver setup wrapper function */
  if (IDA_mem->ida_lsetup)
    retval = SUNNonlinSolSetLSetupFn(IDA_mem->NLSstg, idaNlsLSetupSensStg);
  else
    retval = SUNNonlinSolSetLSetupFn(IDA_mem->NLSstg, NULL);

  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "idaNlsInitSensStg",
                    "Setting the linear solver setup function failed");
    return(IDA_NLS_INIT_FAIL);
  }

  /* set the linear solver solve wrapper function */
  if (IDA_mem->ida_lsolve)
    retval = SUNNonlinSolSetLSolveFn(IDA_mem->NLSstg, idaNlsLSolveSensStg);
  else
    retval = SUNNonlinSolSetLSolveFn(IDA_mem->NLSstg, NULL);

  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "idaNlsInitSensStg",
                    "Setting linear solver solve function failed");
    return(IDA_NLS_INIT_FAIL);
  }

  /* initialize nonlinear solver */
  retval = SUNNonlinSolInitialize(IDA_mem->NLSstg);

  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "idaNlsInitSensStg",
                    MSG_NLS_INIT_FAIL);
    return(IDA_NLS_INIT_FAIL);
  }

  return(IDA_SUCCESS);
}


static int idaNlsLSetupSensStg(N_Vector ycorStg, N_Vector resStg, booleantype jbad,
                               booleantype* jcur, void* ida_mem)
{
  IDAMem IDA_mem;
  int retval;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "idaNlsLSetupSensStg", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  IDA_mem->ida_nsetupsS++;

  retval = IDA_mem->ida_lsetup(IDA_mem, IDA_mem->ida_yy, IDA_mem->ida_yp, IDA_mem->ida_delta,
                               IDA_mem->ida_tmpS1, IDA_mem->ida_tmpS2, IDA_mem->ida_tmpS3);

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


static int idaNlsLSolveSensStg(N_Vector ycorStg, N_Vector deltaStg, void* ida_mem)
{
  IDAMem IDA_mem;
  int retval, is;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "idaNlsLSolveSensStg", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  for(is=0;is<IDA_mem->ida_Ns;is++) {
    retval = IDA_mem->ida_lsolve(IDA_mem, NV_VEC_SW(deltaStg,is),
                                 IDA_mem->ida_ewtS[is], IDA_mem->ida_yy,
                                 IDA_mem->ida_yp, IDA_mem->ida_delta);

    if (retval < 0) return(IDA_LSOLVE_FAIL);
    if (retval > 0) return(IDA_LSOLVE_RECVR);
  }

  return(IDA_SUCCESS);
}


static int idaNlsResidualSensStg(N_Vector ycorStg, N_Vector resStg, void* ida_mem)
{
  IDAMem IDA_mem;
  int retval;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "idaNlsResidualSensStg", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* update yS and ypS based on the current correction */
  N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                          ONE, IDA_mem->ida_yySpredict,
                          ONE, NV_VECS_SW(ycorStg), IDA_mem->ida_yyS);
  N_VLinearSumVectorArray(IDA_mem->ida_Ns,
                          ONE, IDA_mem->ida_ypSpredict,
                          IDA_mem->ida_cj, NV_VECS_SW(ycorStg), IDA_mem->ida_ypS);

  /* evaluate sens residual */
  retval = IDA_mem->ida_resS(IDA_mem->ida_Ns, IDA_mem->ida_tn,
                             IDA_mem->ida_yy, IDA_mem->ida_yp, IDA_mem->ida_delta,
                             IDA_mem->ida_yyS, IDA_mem->ida_ypS, NV_VECS_SW(resStg),
                             IDA_mem->ida_user_dataS, IDA_mem->ida_tmpS1,
                             IDA_mem->ida_tmpS2, IDA_mem->ida_tmpS3);

  /* increment the number of sens residual evaluations */
  IDA_mem->ida_nrSe++;

  if (retval < 0) return(IDA_SRES_FAIL);
  if (retval > 0) return(IDA_SRES_RECVR);

  return(IDA_SUCCESS);
}


static int idaNlsConvTestSensStg(SUNNonlinearSolver NLS, N_Vector ycor, N_Vector del,
                                 realtype tol, N_Vector ewt, void* ida_mem)
{
  IDAMem IDA_mem;
  int m, retval;
  realtype delnrm;
  realtype rate;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "idaNlsConvTestSensStg", MSG_NO_MEM);
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
    if (delnrm <= IDA_mem->ida_toldel) return(SUN_NLS_SUCCESS);
  } else {
    rate = SUNRpowerR( delnrm/IDA_mem->ida_oldnrm, ONE/m );
    if (rate > RATEMAX) return(SUN_NLS_CONV_RECVR);
    IDA_mem->ida_ssS = rate/(ONE - rate);
  }

  if (IDA_mem->ida_ssS*delnrm <= tol) return(SUN_NLS_SUCCESS);

  /* not yet converged */
  return(SUN_NLS_CONTINUE);
}
