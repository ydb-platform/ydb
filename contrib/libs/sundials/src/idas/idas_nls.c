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

/* constant macros */
#define PT0001  RCONST(0.0001) /* real 0.0001 */
#define ONE     RCONST(1.0)    /* real 1.0    */
#define TWENTY  RCONST(20.0)   /* real 20.0   */

/* nonlinear solver parameters */
#define MAXIT   4           /* default max number of nonlinear iterations    */
#define RATEMAX RCONST(0.9) /* max convergence rate used in divergence check */

/* private functions passed to nonlinear solver */
static int idaNlsResidual(N_Vector ycor, N_Vector res, void* ida_mem);
static int idaNlsLSetup(N_Vector ycor, N_Vector res, booleantype jbad,
                        booleantype* jcur, void* ida_mem);
static int idaNlsLSolve(N_Vector ycor, N_Vector delta, void* ida_mem);
static int idaNlsConvTest(SUNNonlinearSolver NLS, N_Vector ycor, N_Vector del,
                          realtype tol, N_Vector ewt, void* ida_mem);

/* -----------------------------------------------------------------------------
 * Exported functions
 * ---------------------------------------------------------------------------*/

int IDASetNonlinearSolver(void *ida_mem, SUNNonlinearSolver NLS)
{
  IDAMem IDA_mem;
  int retval;

  /* return immediately if IDA memory is NULL */
  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS",
                    "IDASetNonlinearSolver", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  /* return immediately if NLS memory is NULL */
  if (NLS == NULL) {
    IDAProcessError(NULL, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolver",
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
                    "IDASetNonlinearSolver",
                    "NLS does not support required operations");
    return(IDA_ILL_INPUT);
  }

  /* check for allowed nonlinear solver types */
  if (SUNNonlinSolGetType(NLS) != SUNNONLINEARSOLVER_ROOTFIND) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolver",
                    "NLS type must be SUNNONLINEARSOLVER_ROOTFIND");
    return(IDA_ILL_INPUT);
  }

  /* free any existing nonlinear solver */
  if ((IDA_mem->NLS != NULL) && (IDA_mem->ownNLS))
    retval = SUNNonlinSolFree(IDA_mem->NLS);

  /* set SUNNonlinearSolver pointer */
  IDA_mem->NLS = NLS;

  /* Set NLS ownership flag. If this function was called to attach the default
     NLS, IDA will set the flag to SUNTRUE after this function returns. */
  IDA_mem->ownNLS = SUNFALSE;

  /* set the nonlinear residual function */
  retval = SUNNonlinSolSetSysFn(IDA_mem->NLS, idaNlsResidual);
  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolver",
                    "Setting nonlinear system function failed");
    return(IDA_ILL_INPUT);
  }

  /* set convergence test function */
  retval = SUNNonlinSolSetConvTestFn(IDA_mem->NLS, idaNlsConvTest);
  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolver",
                    "Setting convergence test function failed");
    return(IDA_ILL_INPUT);
  }

  /* set max allowed nonlinear iterations */
  retval = SUNNonlinSolSetMaxIters(IDA_mem->NLS, MAXIT);
  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS",
                    "IDASetNonlinearSolver",
                    "Setting maximum number of nonlinear iterations failed");
    return(IDA_ILL_INPUT);
  }

  return(IDA_SUCCESS);
}


/* -----------------------------------------------------------------------------
 * Private functions
 * ---------------------------------------------------------------------------*/

int idaNlsInit(IDAMem IDA_mem)
{
  int retval;

  /* set the linear solver setup wrapper function */
  if (IDA_mem->ida_lsetup)
    retval = SUNNonlinSolSetLSetupFn(IDA_mem->NLS, idaNlsLSetup);
  else
    retval = SUNNonlinSolSetLSetupFn(IDA_mem->NLS, NULL);

  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "idaNlsInit",
                    "Setting the linear solver setup function failed");
    return(IDA_NLS_INIT_FAIL);
  }

  /* set the linear solver solve wrapper function */
  if (IDA_mem->ida_lsolve)
    retval = SUNNonlinSolSetLSolveFn(IDA_mem->NLS, idaNlsLSolve);
  else
    retval = SUNNonlinSolSetLSolveFn(IDA_mem->NLS, NULL);

  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "idaNlsInit",
                    "Setting linear solver solve function failed");
    return(IDA_NLS_INIT_FAIL);
  }

  /* initialize nonlinear solver */
  retval = SUNNonlinSolInitialize(IDA_mem->NLS);

  if (retval != IDA_SUCCESS) {
    IDAProcessError(IDA_mem, IDA_ILL_INPUT, "IDAS", "idaNlsInit",
                    MSG_NLS_INIT_FAIL);
    return(IDA_NLS_INIT_FAIL);
  }

  return(IDA_SUCCESS);
}


static int idaNlsLSetup(N_Vector ycor, N_Vector res, booleantype jbad,
                        booleantype* jcur, void* ida_mem)
{
  IDAMem IDA_mem;
  int retval;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "idaNlsLSetup", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

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


static int idaNlsLSolve(N_Vector ycor, N_Vector delta, void* ida_mem)
{
  IDAMem IDA_mem;
  int retval;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "idaNlsLSolve", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

  retval = IDA_mem->ida_lsolve(IDA_mem, delta, IDA_mem->ida_ewt, IDA_mem->ida_yy, IDA_mem->ida_yp,
                               IDA_mem->ida_savres);

  if (retval < 0) return(IDA_LSOLVE_FAIL);
  if (retval > 0) return(IDA_LSOLVE_RECVR);

  return(IDA_SUCCESS);
}


static int idaNlsResidual(N_Vector ycor, N_Vector res, void* ida_mem)
{
  IDAMem IDA_mem;
  int retval;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "idaNlsResidual", MSG_NO_MEM);
    return(IDA_MEM_NULL);
  }
  IDA_mem = (IDAMem) ida_mem;

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

  return(IDA_SUCCESS);
}


static int idaNlsConvTest(SUNNonlinearSolver NLS, N_Vector ycor, N_Vector del,
                          realtype tol, N_Vector ewt, void* ida_mem)
{
  IDAMem IDA_mem;
  int m, retval;
  realtype delnrm;
  realtype rate;

  if (ida_mem == NULL) {
    IDAProcessError(NULL, IDA_MEM_NULL, "IDAS", "idaNlsConvTest", MSG_NO_MEM);
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
