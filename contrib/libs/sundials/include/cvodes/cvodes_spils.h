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
 * Linear Solver interface in CVODES; these routines now just wrap
 * the updated CVODES generic linear solver interface in cvodes_ls.h.
 * -----------------------------------------------------------------*/

#ifndef _CVSSPILS_H
#define _CVSSPILS_H

#include <cvodes/cvodes_ls.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif


/*===============================================================
  Function Types (typedefs for equivalent types in cvodes_ls.h)
  ===============================================================*/

typedef CVLsPrecSetupFn CVSpilsPrecSetupFn;
typedef CVLsPrecSolveFn CVSpilsPrecSolveFn;
typedef CVLsJacTimesSetupFn CVSpilsJacTimesSetupFn;
typedef CVLsJacTimesVecFn CVSpilsJacTimesVecFn;
typedef CVLsPrecSetupFnB CVSpilsPrecSetupFnB;
typedef CVLsPrecSetupFnBS CVSpilsPrecSetupFnBS;
typedef CVLsPrecSolveFnB CVSpilsPrecSolveFnB;
typedef CVLsPrecSolveFnBS CVSpilsPrecSolveFnBS;
typedef CVLsJacTimesSetupFnB CVSpilsJacTimesSetupFnB;
typedef CVLsJacTimesSetupFnBS CVSpilsJacTimesSetupFnBS;
typedef CVLsJacTimesVecFnB CVSpilsJacTimesVecFnB;
typedef CVLsJacTimesVecFnBS CVSpilsJacTimesVecFnBS;

/*====================================================================
  Exported Functions (wrappers for equivalent routines in cvodes_ls.h)
  ====================================================================*/

int CVSpilsSetLinearSolver(void *cvode_mem, SUNLinearSolver LS);

int CVSpilsSetEpsLin(void *cvode_mem, realtype eplifac);

int CVSpilsSetPreconditioner(void *cvode_mem, CVSpilsPrecSetupFn pset,
                             CVSpilsPrecSolveFn psolve);

int CVSpilsSetJacTimes(void *cvode_mem, CVSpilsJacTimesSetupFn jtsetup,
                       CVSpilsJacTimesVecFn jtimes);

int CVSpilsGetWorkSpace(void *cvode_mem, long int *lenrwLS,
                        long int *leniwLS);

int CVSpilsGetNumPrecEvals(void *cvode_mem, long int *npevals);

int CVSpilsGetNumPrecSolves(void *cvode_mem, long int *npsolves);

int CVSpilsGetNumLinIters(void *cvode_mem, long int *nliters);

int CVSpilsGetNumConvFails(void *cvode_mem, long int *nlcfails);

int CVSpilsGetNumJTSetupEvals(void *cvode_mem, long int *njtsetups);

int CVSpilsGetNumJtimesEvals(void *cvode_mem, long int *njvevals);

int CVSpilsGetNumRhsEvals(void *cvode_mem, long int *nfevalsLS);

int CVSpilsGetLastFlag(void *cvode_mem, long int *flag);

char *CVSpilsGetReturnFlagName(long int flag);

int CVSpilsSetLinearSolverB(void *cvode_mem, int which,
                            SUNLinearSolver LS);

int CVSpilsSetEpsLinB(void *cvode_mem, int which, realtype eplifacB);

int CVSpilsSetPreconditionerB(void *cvode_mem, int which,
                              CVSpilsPrecSetupFnB psetB,
                              CVSpilsPrecSolveFnB psolveB);

int CVSpilsSetPreconditionerBS(void *cvode_mem, int which,
                               CVSpilsPrecSetupFnBS psetBS,
                               CVSpilsPrecSolveFnBS psolveBS);

int CVSpilsSetJacTimesB(void *cvode_mem, int which,
                        CVSpilsJacTimesSetupFnB jtsetupB,
                        CVSpilsJacTimesVecFnB jtimesB);

int CVSpilsSetJacTimesBS(void *cvode_mem, int which,
                         CVSpilsJacTimesSetupFnBS jtsetupBS,
                         CVSpilsJacTimesVecFnBS jtimesBS);


#ifdef __cplusplus
}
#endif

#endif
