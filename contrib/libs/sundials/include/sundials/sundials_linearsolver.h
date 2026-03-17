/* -----------------------------------------------------------------
 * Programmer(s): Daniel Reynolds @ SMU
 *                David Gardner, Carol Woodward, Slaven Peles @ LLNL
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
 * This is the header file for a generic linear solver package.
 * It defines the SUNLinearSolver structure (_generic_SUNLinearSolver)
 * which contains the following fields:
 *   - an implementation-dependent 'content' field which contains
 *     any internal data required by the solver
 *   - an 'ops' filed which contains a structure listing operations
 *     acting on/by such solvers
 *
 * We consider both direct linear solvers and iterative linear solvers
 * as available implementations of this package.  Furthermore, iterative
 * linear solvers can either use a matrix or be matrix-free.  As a
 * result of these different solver characteristics, some of the
 * routines are applicable only to some types of linear solver.
 * -----------------------------------------------------------------
 * This header file contains:
 *   - enumeration constants for all SUNDIALS-defined linear solver
 *     types, as well as a generic type for user-supplied linear
 *     solver types,
 *   - type declarations for the _generic_SUNLinearSolver and
 *     _generic_SUNLinearSolver_Ops structures, as well as references
 *     to pointers to such structures (SUNLinearSolver),
 *   - prototypes for the linear solver functions which operate
 *     on/by SUNLinearSolver objects, and
 *   - return codes for SUNLinearSolver objects.
 * -----------------------------------------------------------------
 * At a minimum, a particular implementation of a SUNLinearSolver must
 * do the following:
 *   - specify the 'content' field of SUNLinearSolver,
 *   - implement the operations on/by those SUNLinearSolver objects,
 *   - provide a constructor routine for new SUNLinearSolver objects
 *
 * Additionally, a SUNLinearSolver implementation may provide the
 * following:
 *   - "Set" routines to control solver-specific parameters/options
 *   - "Get" routines to access solver-specific performance metrics
 * -----------------------------------------------------------------*/

#ifndef _SUNLINEARSOLVER_H
#define _SUNLINEARSOLVER_H

#include <sundials/sundials_types.h>
#include <sundials/sundials_iterative.h>
#include <sundials/sundials_matrix.h>
#include <sundials/sundials_nvector.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif


/* -----------------------------------------------------------------
 * Implemented SUNLinearSolver types:
 * ----------------------------------------------------------------- */

typedef enum {
  SUNLINEARSOLVER_DIRECT,
  SUNLINEARSOLVER_ITERATIVE,
  SUNLINEARSOLVER_MATRIX_ITERATIVE
} SUNLinearSolver_Type;


/* -----------------------------------------------------------------
 * Generic definition of SUNLinearSolver
 * ----------------------------------------------------------------- */

/* Forward reference for pointer to SUNLinearSolver_Ops object */
typedef struct _generic_SUNLinearSolver_Ops *SUNLinearSolver_Ops;

/* Forward reference for pointer to SUNLinearSolver object */
typedef struct _generic_SUNLinearSolver *SUNLinearSolver;

/* Structure containing function pointers to linear solver operations */
struct _generic_SUNLinearSolver_Ops {
  SUNLinearSolver_Type (*gettype)(SUNLinearSolver);
  int                  (*setatimes)(SUNLinearSolver, void*, ATimesFn);
  int                  (*setpreconditioner)(SUNLinearSolver, void*,
                                            PSetupFn, PSolveFn);
  int                  (*setscalingvectors)(SUNLinearSolver,
                                            N_Vector, N_Vector);
  int                  (*initialize)(SUNLinearSolver);
  int                  (*setup)(SUNLinearSolver, SUNMatrix);
  int                  (*solve)(SUNLinearSolver, SUNMatrix, N_Vector,
                                N_Vector, realtype);
  int                  (*numiters)(SUNLinearSolver);
  realtype             (*resnorm)(SUNLinearSolver);
  long int             (*lastflag)(SUNLinearSolver);
  int                  (*space)(SUNLinearSolver, long int*, long int*);
  N_Vector             (*resid)(SUNLinearSolver);
  int                  (*free)(SUNLinearSolver);
};

/* A linear solver is a structure with an implementation-dependent
   'content' field, and a pointer to a structure of linear solver
   operations corresponding to that implementation. */
struct _generic_SUNLinearSolver {
  void *content;
  struct _generic_SUNLinearSolver_Ops *ops;
};


/* -----------------------------------------------------------------
 * Functions exported by SUNLinearSolver module
 * ----------------------------------------------------------------- */

SUNDIALS_EXPORT SUNLinearSolver_Type SUNLinSolGetType(SUNLinearSolver S);

SUNDIALS_EXPORT int SUNLinSolSetATimes(SUNLinearSolver S, void* A_data,
                                       ATimesFn ATimes);

SUNDIALS_EXPORT int SUNLinSolSetPreconditioner(SUNLinearSolver S, void* P_data,
                                               PSetupFn Pset, PSolveFn Psol);

SUNDIALS_EXPORT int SUNLinSolSetScalingVectors(SUNLinearSolver S, N_Vector s1,
                                               N_Vector s2);

SUNDIALS_EXPORT int SUNLinSolInitialize(SUNLinearSolver S);

SUNDIALS_EXPORT int SUNLinSolSetup(SUNLinearSolver S, SUNMatrix A);

SUNDIALS_EXPORT int SUNLinSolSolve(SUNLinearSolver S, SUNMatrix A, N_Vector x,
                                   N_Vector b, realtype tol);

SUNDIALS_EXPORT int SUNLinSolNumIters(SUNLinearSolver S);

SUNDIALS_EXPORT realtype SUNLinSolResNorm(SUNLinearSolver S);

SUNDIALS_EXPORT N_Vector SUNLinSolResid(SUNLinearSolver S);

SUNDIALS_EXPORT long int SUNLinSolLastFlag(SUNLinearSolver S);

SUNDIALS_EXPORT int SUNLinSolSpace(SUNLinearSolver S, long int *lenrwLS,
                                   long int *leniwLS);

SUNDIALS_EXPORT int SUNLinSolFree(SUNLinearSolver S);


/* -----------------------------------------------------------------
 * SUNLinearSolver return values
 * ----------------------------------------------------------------- */

#define SUNLS_SUCCESS             0   /* successful/converged          */

#define SUNLS_MEM_NULL           -1   /* mem argument is NULL          */
#define SUNLS_ILL_INPUT          -2   /* illegal function input        */
#define SUNLS_MEM_FAIL           -3   /* failed memory access          */
#define SUNLS_ATIMES_FAIL_UNREC  -4   /* atimes unrecoverable failure  */
#define SUNLS_PSET_FAIL_UNREC    -5   /* pset unrecoverable failure    */
#define SUNLS_PSOLVE_FAIL_UNREC  -6   /* psolve unrecoverable failure  */
#define SUNLS_PACKAGE_FAIL_UNREC -7   /* external package unrec. fail  */
#define SUNLS_GS_FAIL            -8   /* Gram-Schmidt failure          */
#define SUNLS_QRSOL_FAIL         -9   /* QRsol found singular R        */
#define SUNLS_VECTOROP_ERR       -10  /* vector operation error        */

#define SUNLS_RES_REDUCED         1   /* nonconv. solve, resid reduced */
#define SUNLS_CONV_FAIL           2   /* nonconvergent solve           */
#define SUNLS_ATIMES_FAIL_REC     3   /* atimes failed recoverably     */
#define SUNLS_PSET_FAIL_REC       4   /* pset failed recoverably       */
#define SUNLS_PSOLVE_FAIL_REC     5   /* psolve failed recoverably     */
#define SUNLS_PACKAGE_FAIL_REC    6   /* external package recov. fail  */
#define SUNLS_QRFACT_FAIL         7   /* QRfact found singular matrix  */
#define SUNLS_LUFACT_FAIL         8   /* LUfact found singular matrix  */

#ifdef __cplusplus
}
#endif
#endif
