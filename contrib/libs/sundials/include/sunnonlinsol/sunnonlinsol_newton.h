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
 * This is the header file for the SUNNonlinearSolver module implementation of
 * Newton's method.
 *
 * Part I defines the solver-specific content structure.
 *
 * Part II contains prototypes for the solver constructor and operations.
 * ---------------------------------------------------------------------------*/

#ifndef _SUNNONLINSOL_NEWTON_H
#define _SUNNONLINSOL_NEWTON_H

#include "sundials/sundials_types.h"
#include "sundials/sundials_nvector.h"
#include "sundials/sundials_nonlinearsolver.h"

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/* -----------------------------------------------------------------------------
 * I. Content structure
 * ---------------------------------------------------------------------------*/

struct _SUNNonlinearSolverContent_Newton {

  /* functions provided by the integrator */
  SUNNonlinSolSysFn      Sys;    /* nonlinear system residual function         */
  SUNNonlinSolLSetupFn   LSetup; /* linear solver setup function               */
  SUNNonlinSolLSolveFn   LSolve; /* linear solver solve function               */
  SUNNonlinSolConvTestFn CTest;  /* nonlinear solver convergence test function */

  /* nonlinear solver variables */
  N_Vector    delta;      /* Newton update vector                                   */
  booleantype jcur;       /* Jacobian status, current = SUNTRUE / stale = SUNFALSE  */
  int         curiter;    /* current number of iterations in a solve attempt        */
  int         maxiters;   /* maximum number of iterations in a solve attempt        */
  long int    niters;     /* total number of nonlinear iterations across all solves */
  long int    nconvfails; /* total number of convergence failures across all solves */
};

typedef struct _SUNNonlinearSolverContent_Newton *SUNNonlinearSolverContent_Newton;

/* -----------------------------------------------------------------------------
 * II: Exported functions
 * ---------------------------------------------------------------------------*/

/* Constructor to create solver and allocates memory */
SUNDIALS_EXPORT SUNNonlinearSolver SUNNonlinSol_Newton(N_Vector y);
SUNDIALS_EXPORT SUNNonlinearSolver SUNNonlinSol_NewtonSens(int count, N_Vector y);

/* core functions */
SUNDIALS_EXPORT SUNNonlinearSolver_Type SUNNonlinSolGetType_Newton(SUNNonlinearSolver NLS);

SUNDIALS_EXPORT int SUNNonlinSolInitialize_Newton(SUNNonlinearSolver NLS);

SUNDIALS_EXPORT int SUNNonlinSolSolve_Newton(SUNNonlinearSolver NLS,
                                             N_Vector y0, N_Vector y,
                                             N_Vector w, realtype tol,
                                             booleantype callLSetup, void *mem);

SUNDIALS_EXPORT int SUNNonlinSolFree_Newton(SUNNonlinearSolver NLS);

/* set functions */
SUNDIALS_EXPORT int SUNNonlinSolSetSysFn_Newton(SUNNonlinearSolver NLS,
                                                SUNNonlinSolSysFn SysFn);

SUNDIALS_EXPORT int SUNNonlinSolSetLSetupFn_Newton(SUNNonlinearSolver NLS,
                                                   SUNNonlinSolLSetupFn LSetupFn);

SUNDIALS_EXPORT int SUNNonlinSolSetLSolveFn_Newton(SUNNonlinearSolver NLS,
                                                   SUNNonlinSolLSolveFn LSolveFn);

SUNDIALS_EXPORT int SUNNonlinSolSetConvTestFn_Newton(SUNNonlinearSolver NLS,
                                                     SUNNonlinSolConvTestFn CTestFn);

SUNDIALS_EXPORT int SUNNonlinSolSetMaxIters_Newton(SUNNonlinearSolver NLS,
                                                   int maxiters);

/* get functions */
SUNDIALS_EXPORT int SUNNonlinSolGetNumIters_Newton(SUNNonlinearSolver NLS,
                                                   long int *niters);

SUNDIALS_EXPORT int SUNNonlinSolGetCurIter_Newton(SUNNonlinearSolver NLS,
                                                  int *iter);

SUNDIALS_EXPORT int SUNNonlinSolGetNumConvFails_Newton(SUNNonlinearSolver NLS,
                                                       long int *nconvfails);

SUNDIALS_EXPORT int SUNNonlinSolGetSysFn_Newton(SUNNonlinearSolver NLS,
                                                SUNNonlinSolSysFn *SysFn);

#ifdef __cplusplus
}
#endif

#endif
