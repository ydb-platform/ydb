/* ----------------------------------------------------------------
 * Programmer(s): Daniel R. Reynolds @ SMU
 *                Radu Serban @ LLNL
 * ----------------------------------------------------------------
 * SUNDIALS Copyright Start
 * Copyright (c) 2002-2019, Lawrence Livermore National Security
 * and Southern Methodist University.
 * All rights reserved.
 *
 * See the top-level LICENSE and NOTICE files for details.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 * SUNDIALS Copyright End
 * ----------------------------------------------------------------
 * This is the header file for CVODES' linear solver interface.
 * ----------------------------------------------------------------*/

#ifndef _CVSLS_H
#define _CVSLS_H

#include <sundials/sundials_direct.h>
#include <sundials/sundials_iterative.h>
#include <sundials/sundials_linearsolver.h>
#include <sundials/sundials_matrix.h>
#include <sundials/sundials_nvector.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif


/*=================================================================
  CVLS Constants
  =================================================================*/

#define CVLS_SUCCESS          0
#define CVLS_MEM_NULL        -1
#define CVLS_LMEM_NULL       -2
#define CVLS_ILL_INPUT       -3
#define CVLS_MEM_FAIL        -4
#define CVLS_PMEM_NULL       -5
#define CVLS_JACFUNC_UNRECVR -6
#define CVLS_JACFUNC_RECVR   -7
#define CVLS_SUNMAT_FAIL     -8
#define CVLS_SUNLS_FAIL      -9

/* Return values for the adjoint module */

#define CVLS_NO_ADJ          -101
#define CVLS_LMEMB_NULL      -102


/*=================================================================
  Forward problems
  =================================================================*/

/*=================================================================
  CVLS user-supplied function prototypes
  =================================================================*/

typedef int (*CVLsJacFn)(realtype t, N_Vector y, N_Vector fy,
                         SUNMatrix Jac, void *user_data,
                         N_Vector tmp1, N_Vector tmp2, N_Vector tmp3);

typedef int (*CVLsPrecSetupFn)(realtype t, N_Vector y, N_Vector fy,
                               booleantype jok, booleantype *jcurPtr,
                               realtype gamma, void *user_data);

typedef int (*CVLsPrecSolveFn)(realtype t, N_Vector y, N_Vector fy,
                               N_Vector r, N_Vector z, realtype gamma,
                               realtype delta, int lr, void *user_data);

typedef int (*CVLsJacTimesSetupFn)(realtype t, N_Vector y,
                                   N_Vector fy, void *user_data);

typedef int (*CVLsJacTimesVecFn)(N_Vector v, N_Vector Jv, realtype t,
                                 N_Vector y, N_Vector fy,
                                 void *user_data, N_Vector tmp);


/*=================================================================
  CVLS Exported functions
  =================================================================*/

SUNDIALS_EXPORT int CVodeSetLinearSolver(void *cvode_mem,
                                         SUNLinearSolver LS,
                                         SUNMatrix A);


/*-----------------------------------------------------------------
  Optional inputs to the CVLS linear solver interface
  -----------------------------------------------------------------*/

SUNDIALS_EXPORT int CVodeSetJacFn(void *cvode_mem, CVLsJacFn jac);
SUNDIALS_EXPORT int CVodeSetMaxStepsBetweenJac(void *cvode_mem,
                                               long int msbj);
SUNDIALS_EXPORT int CVodeSetEpsLin(void *cvode_mem, realtype eplifac);
SUNDIALS_EXPORT int CVodeSetPreconditioner(void *cvode_mem,
                                           CVLsPrecSetupFn pset,
                                           CVLsPrecSolveFn psolve);
SUNDIALS_EXPORT int CVodeSetJacTimes(void *cvode_mem,
                                     CVLsJacTimesSetupFn jtsetup,
                                     CVLsJacTimesVecFn jtimes);

/*-----------------------------------------------------------------
  Optional outputs from the CVLS linear solver interface
  -----------------------------------------------------------------*/

SUNDIALS_EXPORT int CVodeGetLinWorkSpace(void *cvode_mem,
                                         long int *lenrwLS,
                                         long int *leniwLS);
SUNDIALS_EXPORT int CVodeGetNumJacEvals(void *cvode_mem,
                                        long int *njevals);
SUNDIALS_EXPORT int CVodeGetNumPrecEvals(void *cvode_mem,
                                         long int *npevals);
SUNDIALS_EXPORT int CVodeGetNumPrecSolves(void *cvode_mem,
                                          long int *npsolves);
SUNDIALS_EXPORT int CVodeGetNumLinIters(void *cvode_mem,
                                        long int *nliters);
SUNDIALS_EXPORT int CVodeGetNumLinConvFails(void *cvode_mem,
                                            long int *nlcfails);
SUNDIALS_EXPORT int CVodeGetNumJTSetupEvals(void *cvode_mem,
                                              long int *njtsetups);
SUNDIALS_EXPORT int CVodeGetNumJtimesEvals(void *cvode_mem,
                                           long int *njvevals);
SUNDIALS_EXPORT int CVodeGetNumLinRhsEvals(void *cvode_mem,
                                           long int *nfevalsLS);
SUNDIALS_EXPORT int CVodeGetLastLinFlag(void *cvode_mem,
                                        long int *flag);
SUNDIALS_EXPORT char *CVodeGetLinReturnFlagName(long int flag);


/*=================================================================
  Backward problems
  =================================================================*/

/*=================================================================
  CVLS user-supplied function prototypes
  =================================================================*/

typedef int (*CVLsJacFnB)(realtype t, N_Vector y, N_Vector yB,
                          N_Vector fyB, SUNMatrix JB,
                          void *user_dataB, N_Vector tmp1B,
                          N_Vector tmp2B, N_Vector tmp3B);

typedef int (*CVLsJacFnBS)(realtype t, N_Vector y, N_Vector *yS,
                           N_Vector yB, N_Vector fyB, SUNMatrix JB,
                           void *user_dataB, N_Vector tmp1B,
                           N_Vector tmp2B, N_Vector tmp3B);

typedef int (*CVLsPrecSetupFnB)(realtype t, N_Vector y, N_Vector yB,
                                N_Vector fyB, booleantype jokB,
                                booleantype *jcurPtrB,
                                realtype gammaB, void *user_dataB);

typedef int (*CVLsPrecSetupFnBS)(realtype t, N_Vector y,
                                 N_Vector *yS, N_Vector yB,
                                 N_Vector fyB, booleantype jokB,
                                 booleantype *jcurPtrB,
                                 realtype gammaB, void *user_dataB);

typedef int (*CVLsPrecSolveFnB)(realtype t, N_Vector y, N_Vector yB,
                                N_Vector fyB, N_Vector rB,
                                N_Vector zB, realtype gammaB,
                                realtype deltaB, int lrB,
                                void *user_dataB);

typedef int (*CVLsPrecSolveFnBS)(realtype t, N_Vector y, N_Vector *yS,
                                 N_Vector yB, N_Vector fyB,
                                 N_Vector rB, N_Vector zB,
                                 realtype gammaB, realtype deltaB,
                                 int lrB, void *user_dataB);

typedef int (*CVLsJacTimesSetupFnB)(realtype t, N_Vector y, N_Vector yB,
                                    N_Vector fyB, void *jac_dataB);

typedef int (*CVLsJacTimesSetupFnBS)(realtype t, N_Vector y,
                                     N_Vector *yS, N_Vector yB,
                                     N_Vector fyB, void *jac_dataB);

typedef int (*CVLsJacTimesVecFnB)(N_Vector vB, N_Vector JvB, realtype t,
                                  N_Vector y, N_Vector yB, N_Vector fyB,
                                  void *jac_dataB, N_Vector tmpB);

typedef int (*CVLsJacTimesVecFnBS)(N_Vector vB, N_Vector JvB,
                                   realtype t, N_Vector y, N_Vector *yS,
                                   N_Vector yB, N_Vector fyB,
                                   void *jac_dataB, N_Vector tmpB);


/*=================================================================
  CVLS Exported functions
  =================================================================*/

SUNDIALS_EXPORT int CVodeSetLinearSolverB(void *cvode_mem,
                                          int which,
                                          SUNLinearSolver LS,
                                          SUNMatrix A);

/*-----------------------------------------------------------------
  Each CVodeSet***B or CVodeSet***BS function below links the
  main CVODES integrator with the corresponding CVSLS
  optional input function for the backward integration.
  The 'which' argument is the int returned by CVodeCreateB.
  -----------------------------------------------------------------*/

SUNDIALS_EXPORT int CVodeSetJacFnB(void *cvode_mem, int which,
                                   CVLsJacFnB jacB);
SUNDIALS_EXPORT int CVodeSetJacFnBS(void *cvode_mem, int which,
                                    CVLsJacFnBS jacBS);

SUNDIALS_EXPORT int CVodeSetEpsLinB(void *cvode_mem, int which,
                                    realtype eplifacB);

SUNDIALS_EXPORT int CVodeSetPreconditionerB(void *cvode_mem, int which,
                                            CVLsPrecSetupFnB psetB,
                                            CVLsPrecSolveFnB psolveB);
SUNDIALS_EXPORT int CVodeSetPreconditionerBS(void *cvode_mem, int which,
                                             CVLsPrecSetupFnBS psetBS,
                                             CVLsPrecSolveFnBS psolveBS);

SUNDIALS_EXPORT int CVodeSetJacTimesB(void *cvode_mem, int which,
                                      CVLsJacTimesSetupFnB jtsetupB,
                                      CVLsJacTimesVecFnB jtimesB);
SUNDIALS_EXPORT int CVodeSetJacTimesBS(void *cvode_mem, int which,
                                       CVLsJacTimesSetupFnBS jtsetupBS,
                                       CVLsJacTimesVecFnBS jtimesBS);


#ifdef __cplusplus
}
#endif

#endif
