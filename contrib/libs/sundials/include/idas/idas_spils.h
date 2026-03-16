/* -----------------------------------------------------------------
 * Programmer(s): Daniel R. Reynolds @ SMU
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
 * Header file for the deprecated Scaled, Preconditioned Iterative
 * Linear Solver interface in IDAS; these routines now just wrap
 * the updated IDA generic linear solver interface in idas_ls.h.
 * -----------------------------------------------------------------*/

#ifndef _IDASSPILS_H
#define _IDASSPILS_H

#include <idas/idas_ls.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif


/*===============================================================
  Function Types (typedefs for equivalent types in idas_ls.h)
  ===============================================================*/

typedef IDALsPrecSetupFn IDASpilsPrecSetupFn;
typedef IDALsPrecSolveFn IDASpilsPrecSolveFn;
typedef IDALsJacTimesSetupFn IDASpilsJacTimesSetupFn;
typedef IDALsJacTimesVecFn IDASpilsJacTimesVecFn;
typedef IDALsPrecSetupFnB IDASpilsPrecSetupFnB;
typedef IDALsPrecSetupFnBS IDASpilsPrecSetupFnBS;
typedef IDALsPrecSolveFnB IDASpilsPrecSolveFnB;
typedef IDALsPrecSolveFnBS IDASpilsPrecSolveFnBS;
typedef IDALsJacTimesSetupFnB IDASpilsJacTimesSetupFnB;
typedef IDALsJacTimesSetupFnBS IDASpilsJacTimesSetupFnBS;
typedef IDALsJacTimesVecFnB IDASpilsJacTimesVecFnB;
typedef IDALsJacTimesVecFnBS IDASpilsJacTimesVecFnBS;

/*====================================================================
  Exported Functions (wrappers for equivalent routines in idas_ls.h)
  ====================================================================*/

int IDASpilsSetLinearSolver(void *ida_mem, SUNLinearSolver LS);

int IDASpilsSetPreconditioner(void *ida_mem, IDASpilsPrecSetupFn pset,
                              IDASpilsPrecSolveFn psolve);

int IDASpilsSetJacTimes(void *ida_mem, IDASpilsJacTimesSetupFn jtsetup,
                        IDASpilsJacTimesVecFn jtimes);

int IDASpilsSetEpsLin(void *ida_mem, realtype eplifac);

int IDASpilsSetIncrementFactor(void *ida_mem, realtype dqincfac);

int IDASpilsGetWorkSpace(void *ida_mem, long int *lenrwLS, long int *leniwLS);

int IDASpilsGetNumPrecEvals(void *ida_mem, long int *npevals);

int IDASpilsGetNumPrecSolves(void *ida_mem, long int *npsolves);

int IDASpilsGetNumLinIters(void *ida_mem, long int *nliters);

int IDASpilsGetNumConvFails(void *ida_mem, long int *nlcfails);

int IDASpilsGetNumJTSetupEvals(void *ida_mem, long int *njtsetups);

int IDASpilsGetNumJtimesEvals(void *ida_mem, long int *njvevals);

int IDASpilsGetNumResEvals(void *ida_mem, long int *nrevalsLS);

int IDASpilsGetLastFlag(void *ida_mem, long int *flag);

char *IDASpilsGetReturnFlagName(long int flag);

int IDASpilsSetLinearSolverB(void *ida_mem, int which,
                             SUNLinearSolver LS);

int IDASpilsSetEpsLinB(void *ida_mem, int which, realtype eplifacB);

int IDASpilsSetIncrementFactorB(void *ida_mem, int which,
                                realtype dqincfacB);

int IDASpilsSetPreconditionerB(void *ida_mem, int which,
                               IDASpilsPrecSetupFnB psetB,
                               IDASpilsPrecSolveFnB psolveB);

int IDASpilsSetPreconditionerBS(void *ida_mem, int which,
                                IDASpilsPrecSetupFnBS psetBS,
                                IDASpilsPrecSolveFnBS psolveBS);

int IDASpilsSetJacTimesB(void *ida_mem, int which,
                         IDASpilsJacTimesSetupFnB jtsetupB,
                         IDASpilsJacTimesVecFnB jtimesB);

int IDASpilsSetJacTimesBS(void *ida_mem, int which,
                          IDASpilsJacTimesSetupFnBS jtsetupBS,
                          IDASpilsJacTimesVecFnBS jtimesBS);


#ifdef __cplusplus
}
#endif

#endif
