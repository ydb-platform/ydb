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
 * This is the header file for IDAS' linear solver interface.
 * ----------------------------------------------------------------*/

#ifndef _IDASLS_H
#define _IDASLS_H

#include <sundials/sundials_direct.h>
#include <sundials/sundials_iterative.h>
#include <sundials/sundials_linearsolver.h>
#include <sundials/sundials_matrix.h>
#include <sundials/sundials_nvector.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif


/*=================================================================
  IDALS Constants
  =================================================================*/

#define IDALS_SUCCESS           0
#define IDALS_MEM_NULL         -1
#define IDALS_LMEM_NULL        -2
#define IDALS_ILL_INPUT        -3
#define IDALS_MEM_FAIL         -4
#define IDALS_PMEM_NULL        -5
#define IDALS_JACFUNC_UNRECVR  -6
#define IDALS_JACFUNC_RECVR    -7
#define IDALS_SUNMAT_FAIL      -8
#define IDALS_SUNLS_FAIL       -9

/* Return values for the adjoint module */
#define IDALS_NO_ADJ          -101
#define IDALS_LMEMB_NULL      -102


/*=================================================================
  Forward problems
  =================================================================*/

/*=================================================================
  IDALS user-supplied function prototypes
  =================================================================*/

typedef int (*IDALsJacFn)(realtype t, realtype c_j, N_Vector y,
                          N_Vector yp, N_Vector r, SUNMatrix Jac,
                          void *user_data, N_Vector tmp1,
                          N_Vector tmp2, N_Vector tmp3);

typedef int (*IDALsPrecSetupFn)(realtype tt, N_Vector yy,
                                N_Vector yp, N_Vector rr,
                                realtype c_j, void *user_data);

typedef int (*IDALsPrecSolveFn)(realtype tt, N_Vector yy,
                                N_Vector yp, N_Vector rr,
                                N_Vector rvec, N_Vector zvec,
                                realtype c_j, realtype delta,
                                void *user_data);

typedef int (*IDALsJacTimesSetupFn)(realtype tt, N_Vector yy,
                                    N_Vector yp, N_Vector rr,
                                    realtype c_j, void *user_data);

typedef int (*IDALsJacTimesVecFn)(realtype tt, N_Vector yy,
                                  N_Vector yp, N_Vector rr,
                                  N_Vector v, N_Vector Jv,
                                  realtype c_j, void *user_data,
                                  N_Vector tmp1, N_Vector tmp2);


/*=================================================================
  IDALS Exported functions
  =================================================================*/

SUNDIALS_EXPORT int IDASetLinearSolver(void *ida_mem,
                                       SUNLinearSolver LS,
                                       SUNMatrix A);


/*-----------------------------------------------------------------
  Optional inputs to the IDALS linear solver interface
  -----------------------------------------------------------------*/

SUNDIALS_EXPORT int IDASetJacFn(void *ida_mem, IDALsJacFn jac);
SUNDIALS_EXPORT int IDASetPreconditioner(void *ida_mem,
                                         IDALsPrecSetupFn pset,
                                         IDALsPrecSolveFn psolve);
SUNDIALS_EXPORT int IDASetJacTimes(void *ida_mem,
                                   IDALsJacTimesSetupFn jtsetup,
                                   IDALsJacTimesVecFn jtimes);
SUNDIALS_EXPORT int IDASetEpsLin(void *ida_mem, realtype eplifac);
SUNDIALS_EXPORT int IDASetIncrementFactor(void *ida_mem,
                                          realtype dqincfac);

/*-----------------------------------------------------------------
  Optional outputs from the IDALS linear solver interface
  -----------------------------------------------------------------*/

SUNDIALS_EXPORT int IDAGetLinWorkSpace(void *ida_mem,
                                       long int *lenrwLS,
                                       long int *leniwLS);
SUNDIALS_EXPORT int IDAGetNumJacEvals(void *ida_mem,
                                      long int *njevals);
SUNDIALS_EXPORT int IDAGetNumPrecEvals(void *ida_mem,
                                       long int *npevals);
SUNDIALS_EXPORT int IDAGetNumPrecSolves(void *ida_mem,
                                        long int *npsolves);
SUNDIALS_EXPORT int IDAGetNumLinIters(void *ida_mem,
                                      long int *nliters);
SUNDIALS_EXPORT int IDAGetNumLinConvFails(void *ida_mem,
                                          long int *nlcfails);
SUNDIALS_EXPORT int IDAGetNumJTSetupEvals(void *ida_mem,
                                          long int *njtsetups);
SUNDIALS_EXPORT int IDAGetNumJtimesEvals(void *ida_mem,
                                         long int *njvevals);
SUNDIALS_EXPORT int IDAGetNumLinResEvals(void *ida_mem,
                                         long int *nrevalsLS);
SUNDIALS_EXPORT int IDAGetLastLinFlag(void *ida_mem,
                                      long int *flag);
SUNDIALS_EXPORT char *IDAGetLinReturnFlagName(long int flag);


/*=================================================================
  Backward problems
  =================================================================*/

/*=================================================================
  IDALS user-supplied function prototypes
  =================================================================*/

typedef int (*IDALsJacFnB)(realtype tt, realtype c_jB, N_Vector yy,
                           N_Vector yp, N_Vector yyB, N_Vector ypB,
                           N_Vector rrB, SUNMatrix JacB,
                           void *user_dataB, N_Vector tmp1B,
                           N_Vector tmp2B, N_Vector tmp3B);

typedef int (*IDALsJacFnBS)(realtype tt, realtype c_jB, N_Vector yy,
                            N_Vector yp, N_Vector *yS, N_Vector *ypS,
                            N_Vector yyB, N_Vector ypB, N_Vector rrB,
                            SUNMatrix JacB, void *user_dataB,
                            N_Vector tmp1B, N_Vector tmp2B,
                            N_Vector tmp3B);

typedef int (*IDALsPrecSetupFnB)(realtype tt, N_Vector yy,
                                 N_Vector yp, N_Vector yyB,
                                 N_Vector ypB, N_Vector rrB,
                                 realtype c_jB, void *user_dataB);

typedef int (*IDALsPrecSetupFnBS)(realtype tt, N_Vector yy,
                                  N_Vector yp, N_Vector *yyS,
                                  N_Vector *ypS, N_Vector yyB,
                                  N_Vector ypB, N_Vector rrB,
                                  realtype c_jB, void *user_dataB);

typedef int (*IDALsPrecSolveFnB)(realtype tt, N_Vector yy,
                                 N_Vector yp, N_Vector yyB,
                                 N_Vector ypB, N_Vector rrB,
                                 N_Vector rvecB, N_Vector zvecB,
                                 realtype c_jB, realtype deltaB,
                                 void *user_dataB);

typedef int (*IDALsPrecSolveFnBS)(realtype tt, N_Vector yy,
                                  N_Vector yp, N_Vector *yyS,
                                  N_Vector *ypS, N_Vector yyB,
                                  N_Vector ypB, N_Vector rrB,
                                  N_Vector rvecB, N_Vector zvecB,
                                  realtype c_jB, realtype deltaB,
                                  void *user_dataB);

typedef int (*IDALsJacTimesSetupFnB)(realtype t, N_Vector yy,
                                     N_Vector yp, N_Vector yyB,
                                     N_Vector ypB, N_Vector rrB,
                                     realtype c_jB, void *user_dataB);

typedef int (*IDALsJacTimesSetupFnBS)(realtype t, N_Vector yy,
                                      N_Vector yp, N_Vector *yyS,
                                      N_Vector *ypS, N_Vector yyB,
                                      N_Vector ypB, N_Vector rrB,
                                      realtype c_jB, void *user_dataB);

typedef int (*IDALsJacTimesVecFnB)(realtype t, N_Vector yy,
                                   N_Vector yp, N_Vector yyB,
                                   N_Vector ypB, N_Vector rrB,
                                   N_Vector vB, N_Vector JvB,
                                   realtype c_jB, void *user_dataB,
                                   N_Vector tmp1B, N_Vector tmp2B);

typedef int (*IDALsJacTimesVecFnBS)(realtype t, N_Vector yy,
                                    N_Vector yp, N_Vector *yyS,
                                    N_Vector *ypS, N_Vector yyB,
                                    N_Vector ypB, N_Vector rrB,
                                    N_Vector vB, N_Vector JvB,
                                    realtype c_jB, void *user_dataB,
                                    N_Vector tmp1B, N_Vector tmp2B);


/*=================================================================
  IDALS Exported functions
  =================================================================*/

SUNDIALS_EXPORT int IDASetLinearSolverB(void *ida_mem,
                                        int which,
                                        SUNLinearSolver LS,
                                        SUNMatrix A);

/*-----------------------------------------------------------------
  Each IDASet***B or IDASet***BS function below links the
  main IDAS integrator with the corresponding IDALS
  optional input function for the backward integration.
  The 'which' argument is the int returned by IDACreateB.
  -----------------------------------------------------------------*/

SUNDIALS_EXPORT int IDASetJacFnB(void *ida_mem, int which,
                                 IDALsJacFnB jacB);
SUNDIALS_EXPORT int IDASetJacFnBS(void *ida_mem, int which,
                                  IDALsJacFnBS jacBS);

SUNDIALS_EXPORT int IDASetEpsLinB(void *ida_mem, int which,
                                  realtype eplifacB);
SUNDIALS_EXPORT int IDASetIncrementFactorB(void *ida_mem, int which,
                                           realtype dqincfacB);
SUNDIALS_EXPORT int IDASetPreconditionerB(void *ida_mem, int which,
                                          IDALsPrecSetupFnB psetB,
                                          IDALsPrecSolveFnB psolveB);
SUNDIALS_EXPORT int IDASetPreconditionerBS(void *ida_mem, int which,
                                           IDALsPrecSetupFnBS psetBS,
                                           IDALsPrecSolveFnBS psolveBS);
SUNDIALS_EXPORT int IDASetJacTimesB(void *ida_mem, int which,
                                    IDALsJacTimesSetupFnB jtsetupB,
                                    IDALsJacTimesVecFnB jtimesB);
SUNDIALS_EXPORT int IDASetJacTimesBS(void *ida_mem, int which,
                                     IDALsJacTimesSetupFnBS jtsetupBS,
                                     IDALsJacTimesVecFnBS jtimesBS);


#ifdef __cplusplus
}
#endif

#endif
