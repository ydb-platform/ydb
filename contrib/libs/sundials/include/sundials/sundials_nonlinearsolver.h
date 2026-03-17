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
 * This is the header file for a generic nonlinear solver package. It defines
 * the SUNNonlinearSolver structure (_generic_SUNNonlinearSolver) which contains
 * the following fields:
 *   - an implementation-dependent 'content' field which contains any internal
 *     data required by the solver
 *   - an 'ops' filed which contains a structure listing operations acting on/by
 *     such solvers
 *
 * We consider iterative nonlinear solvers for systems in both root finding
 * (F(y) = 0) or fixed-point (G(y) = y) form. As a result, some of the routines
 * are applicable only to one type of nonlinear solver.
 * -----------------------------------------------------------------------------
 * This header file contains:
 *   - function types supplied to a SUNNonlinearSolver,
 *   - enumeration constants for SUNDIALS-defined nonlinear solver types,
 *   - type declarations for the _generic_SUNNonlinearSolver and
 *     _generic_SUNNonlinearSolver_Ops structures, as well as references to
 *     pointers to such structures (SUNNonlinearSolver),
 *   - prototypes for the nonlinear solver functions which operate
 *     on/by SUNNonlinearSolver objects, and
 *   - return codes for SUNLinearSolver objects.
 * -----------------------------------------------------------------------------
 * At a minimum, a particular implementation of a SUNNonlinearSolver must do the
 * following:
 *   - specify the 'content' field of a SUNNonlinearSolver,
 *   - implement the operations on/by the SUNNonlinearSovler objects,
 *   - provide a constructor routine for new SUNNonlinearSolver objects
 *
 * Additionally, a SUNNonlinearSolver implementation may provide the following:
 *   - "Set" routines to control solver-specific parameters/options
 *   - "Get" routines to access solver-specific performance metrics
 * ---------------------------------------------------------------------------*/

#ifndef _SUNNONLINEARSOLVER_H
#define _SUNNONLINEARSOLVER_H

#include <sundials/sundials_types.h>
#include <sundials/sundials_nvector.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif


/* -----------------------------------------------------------------------------
 *  Forward references for SUNNonlinearSolver types defined below
 * ---------------------------------------------------------------------------*/

/* Forward reference for pointer to SUNNonlinearSolver_Ops object */
typedef struct _generic_SUNNonlinearSolver_Ops *SUNNonlinearSolver_Ops;

/* Forward reference for pointer to SUNNonlinearSolver object */
typedef struct _generic_SUNNonlinearSolver *SUNNonlinearSolver;


/* -----------------------------------------------------------------------------
 * Integrator supplied function types
 * ---------------------------------------------------------------------------*/

typedef int (*SUNNonlinSolSysFn)(N_Vector y, N_Vector F, void* mem);

typedef int (*SUNNonlinSolLSetupFn)(N_Vector y, N_Vector F, booleantype jbad,
                                    booleantype* jcur, void* mem);

typedef int (*SUNNonlinSolLSolveFn)(N_Vector y, N_Vector b, void* mem);

typedef int (*SUNNonlinSolConvTestFn)(SUNNonlinearSolver NLS, N_Vector y,
                                      N_Vector del, realtype tol, N_Vector ewt,
                                      void* mem);


/* -----------------------------------------------------------------------------
 * SUNNonlinearSolver types
 * ---------------------------------------------------------------------------*/

typedef enum {
  SUNNONLINEARSOLVER_ROOTFIND,
  SUNNONLINEARSOLVER_FIXEDPOINT
} SUNNonlinearSolver_Type;


/* -----------------------------------------------------------------------------
 * Generic definition of SUNNonlinearSolver
 * ---------------------------------------------------------------------------*/

/* Structure containing function pointers to nonlinear solver operations */
struct _generic_SUNNonlinearSolver_Ops {
  SUNNonlinearSolver_Type (*gettype)(SUNNonlinearSolver);
  int (*initialize)(SUNNonlinearSolver);
  int (*setup)(SUNNonlinearSolver, N_Vector, void*);
  int (*solve)(SUNNonlinearSolver, N_Vector, N_Vector, N_Vector, realtype,
               booleantype, void*);
  int (*free)(SUNNonlinearSolver);
  int (*setsysfn)(SUNNonlinearSolver, SUNNonlinSolSysFn);
  int (*setlsetupfn)(SUNNonlinearSolver, SUNNonlinSolLSetupFn);
  int (*setlsolvefn)(SUNNonlinearSolver, SUNNonlinSolLSolveFn);
  int (*setctestfn)(SUNNonlinearSolver, SUNNonlinSolConvTestFn);
  int (*setmaxiters)(SUNNonlinearSolver, int);
  int (*getnumiters)(SUNNonlinearSolver, long int*);
  int (*getcuriter)(SUNNonlinearSolver, int*);
  int (*getnumconvfails)(SUNNonlinearSolver, long int*);
};

/* A nonlinear solver is a structure with an implementation-dependent 'content'
   field, and a pointer to a structure of solver nonlinear solver operations
   corresponding to that implementation. */
struct _generic_SUNNonlinearSolver {
  void *content;
  struct _generic_SUNNonlinearSolver_Ops *ops;
};


/* -----------------------------------------------------------------------------
 * Functions exported by SUNNonlinearSolver module
 * ---------------------------------------------------------------------------*/

/* core functions */
SUNDIALS_EXPORT SUNNonlinearSolver_Type SUNNonlinSolGetType(SUNNonlinearSolver NLS);

SUNDIALS_EXPORT int SUNNonlinSolInitialize(SUNNonlinearSolver NLS);

SUNDIALS_EXPORT int SUNNonlinSolSetup(SUNNonlinearSolver NLS,
                                      N_Vector y, void* mem);

SUNDIALS_EXPORT int SUNNonlinSolSolve(SUNNonlinearSolver NLS,
                                      N_Vector y0, N_Vector y,
                                      N_Vector w, realtype tol,
                                      booleantype callLSetup, void *mem);

SUNDIALS_EXPORT int SUNNonlinSolFree(SUNNonlinearSolver NLS);

/* set functions */
SUNDIALS_EXPORT int SUNNonlinSolSetSysFn(SUNNonlinearSolver NLS,
                                         SUNNonlinSolSysFn SysFn);

SUNDIALS_EXPORT int SUNNonlinSolSetLSetupFn(SUNNonlinearSolver NLS,
                                            SUNNonlinSolLSetupFn SetupFn);

SUNDIALS_EXPORT int SUNNonlinSolSetLSolveFn(SUNNonlinearSolver NLS,
                                            SUNNonlinSolLSolveFn SolveFn);

SUNDIALS_EXPORT int SUNNonlinSolSetConvTestFn(SUNNonlinearSolver NLS,
                                              SUNNonlinSolConvTestFn CTestFn);

SUNDIALS_EXPORT int SUNNonlinSolSetMaxIters(SUNNonlinearSolver NLS,
                                            int maxiters);
/* get functions */
SUNDIALS_EXPORT int SUNNonlinSolGetNumIters(SUNNonlinearSolver NLS,
                                            long int *niters);

SUNDIALS_EXPORT int SUNNonlinSolGetCurIter(SUNNonlinearSolver NLS,
                                           int *iter);

SUNDIALS_EXPORT int SUNNonlinSolGetNumConvFails(SUNNonlinearSolver NLS,
                                                long int *nconvfails);


/* -----------------------------------------------------------------------------
 * SUNNonlinearSolver return values
 * ---------------------------------------------------------------------------*/

#define SUN_NLS_SUCCESS        0  /* successful / converged */

/* Recoverable */
#define SUN_NLS_CONTINUE      +1  /* not converged, keep iterating      */
#define SUN_NLS_CONV_RECVR    +2  /* convergece failure, try to recover */

/* Unrecoverable */
#define SUN_NLS_MEM_NULL      -1  /* memory argument is NULL            */
#define SUN_NLS_MEM_FAIL      -2  /* failed memory access / allocation  */
#define SUN_NLS_ILL_INPUT     -3  /* illegal function input             */
#define SUN_NLS_VECTOROP_ERR  -4  /* failed NVector operation           */

#ifdef __cplusplus
}
#endif

#endif
