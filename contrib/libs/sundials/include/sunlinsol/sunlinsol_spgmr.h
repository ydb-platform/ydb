/*
 * -----------------------------------------------------------------
 * Programmer(s): Daniel Reynolds @ SMU
 * Based on code sundials_spgmr.h by: Scott D. Cohen, 
 *      Alan C. Hindmarsh and Radu Serban @ LLNL
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
 * This is the header file for the SPGMR implementation of the 
 * SUNLINSOL module, SUNLINSOL_SPGMR.  The SPGMR algorithm is based
 * on the Scaled Preconditioned GMRES (Generalized Minimal Residual)
 * method.
 *
 * Note:
 *   - The definition of the generic SUNLinearSolver structure can 
 *     be found in the header file sundials_linearsolver.h.
 * -----------------------------------------------------------------
 */

#ifndef _SUNLINSOL_SPGMR_H
#define _SUNLINSOL_SPGMR_H

#include <sundials/sundials_linearsolver.h>
#include <sundials/sundials_matrix.h>
#include <sundials/sundials_nvector.h>
#include <sundials/sundials_spgmr.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/* Default SPGMR solver parameters */
#define SUNSPGMR_MAXL_DEFAULT    5
#define SUNSPGMR_MAXRS_DEFAULT   0
#define SUNSPGMR_GSTYPE_DEFAULT  MODIFIED_GS

/* ----------------------------------------
 * SPGMR Implementation of SUNLinearSolver
 * ---------------------------------------- */
  
struct _SUNLinearSolverContent_SPGMR {
  int maxl;
  int pretype;
  int gstype;
  int max_restarts;
  int numiters;
  realtype resnorm;
  long int last_flag;

  ATimesFn ATimes;
  void* ATData;
  PSetupFn Psetup;
  PSolveFn Psolve;
  void* PData;

  N_Vector s1;
  N_Vector s2;
  N_Vector *V;
  realtype **Hes;
  realtype *givens;
  N_Vector xcor;
  realtype *yg;
  N_Vector vtemp;

  realtype *cv;
  N_Vector *Xv;
};

typedef struct _SUNLinearSolverContent_SPGMR *SUNLinearSolverContent_SPGMR;


/* ---------------------------------------
 * Exported Functions for SUNLINSOL_SPGMR
 * --------------------------------------- */

SUNDIALS_EXPORT SUNLinearSolver SUNLinSol_SPGMR(N_Vector y,
                                                int pretype,
                                                int maxl);
SUNDIALS_EXPORT int SUNLinSol_SPGMRSetPrecType(SUNLinearSolver S,
                                               int pretype);
SUNDIALS_EXPORT int SUNLinSol_SPGMRSetGSType(SUNLinearSolver S,
                                             int gstype);
SUNDIALS_EXPORT int SUNLinSol_SPGMRSetMaxRestarts(SUNLinearSolver S,
                                                  int maxrs);

/* deprecated */
SUNDIALS_EXPORT SUNLinearSolver SUNSPGMR(N_Vector y, int pretype, int maxl);
/* deprecated */
SUNDIALS_EXPORT int SUNSPGMRSetPrecType(SUNLinearSolver S, int pretype);
/* deprecated */
SUNDIALS_EXPORT int SUNSPGMRSetGSType(SUNLinearSolver S, int gstype);
/* deprecated */
SUNDIALS_EXPORT int SUNSPGMRSetMaxRestarts(SUNLinearSolver S, int maxrs);

SUNDIALS_EXPORT SUNLinearSolver_Type SUNLinSolGetType_SPGMR(SUNLinearSolver S);
SUNDIALS_EXPORT int SUNLinSolInitialize_SPGMR(SUNLinearSolver S);
SUNDIALS_EXPORT int SUNLinSolSetATimes_SPGMR(SUNLinearSolver S, void* A_data,
                                             ATimesFn ATimes);
SUNDIALS_EXPORT int SUNLinSolSetPreconditioner_SPGMR(SUNLinearSolver S,
                                                     void* P_data,
                                                     PSetupFn Pset,
                                                     PSolveFn Psol);
SUNDIALS_EXPORT int SUNLinSolSetScalingVectors_SPGMR(SUNLinearSolver S,
                                                     N_Vector s1,
                                                     N_Vector s2);
SUNDIALS_EXPORT int SUNLinSolSetup_SPGMR(SUNLinearSolver S, SUNMatrix A);
SUNDIALS_EXPORT int SUNLinSolSolve_SPGMR(SUNLinearSolver S, SUNMatrix A,
                                         N_Vector x, N_Vector b, realtype tol);
SUNDIALS_EXPORT int SUNLinSolNumIters_SPGMR(SUNLinearSolver S);
SUNDIALS_EXPORT realtype SUNLinSolResNorm_SPGMR(SUNLinearSolver S);
SUNDIALS_EXPORT N_Vector SUNLinSolResid_SPGMR(SUNLinearSolver S);
SUNDIALS_EXPORT long int SUNLinSolLastFlag_SPGMR(SUNLinearSolver S);
SUNDIALS_EXPORT int SUNLinSolSpace_SPGMR(SUNLinearSolver S, 
                                         long int *lenrwLS, 
                                         long int *leniwLS);
SUNDIALS_EXPORT int SUNLinSolFree_SPGMR(SUNLinearSolver S);


#ifdef __cplusplus
}
#endif

#endif
