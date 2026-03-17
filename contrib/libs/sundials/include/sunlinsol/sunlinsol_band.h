/*
 * -----------------------------------------------------------------
 * Programmer(s): Daniel Reynolds, Ashley Crawford @ SMU
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
 * This is the header file for the band implementation of the 
 * SUNLINSOL module, SUNLINSOL_BAND.
 *
 * Note:
 *   - The definition of the generic SUNLinearSolver structure can 
 *     be found in the header file sundials_linearsolver.h.
 * -----------------------------------------------------------------
 */

#ifndef _SUNLINSOL_BAND_H
#define _SUNLINSOL_BAND_H

#include <sundials/sundials_linearsolver.h>
#include <sundials/sundials_matrix.h>
#include <sundials/sundials_nvector.h>
#include <sundials/sundials_band.h>
#include <sunmatrix/sunmatrix_band.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/* ---------------------------------------
 * Band Implementation of SUNLinearSolver
 * --------------------------------------- */
  
struct _SUNLinearSolverContent_Band {
  sunindextype  N;
  sunindextype *pivots;
  long int last_flag;
};

typedef struct _SUNLinearSolverContent_Band *SUNLinearSolverContent_Band;

  
/* --------------------------------------
 * Exported Functions for SUNLINSOL_BAND
 * -------------------------------------- */

SUNDIALS_EXPORT SUNLinearSolver SUNLinSol_Band(N_Vector y, SUNMatrix A);

/* deprecated */
SUNDIALS_EXPORT SUNLinearSolver SUNBandLinearSolver(N_Vector y,
                                                    SUNMatrix A);

SUNDIALS_EXPORT SUNLinearSolver_Type SUNLinSolGetType_Band(SUNLinearSolver S);
SUNDIALS_EXPORT int SUNLinSolInitialize_Band(SUNLinearSolver S);
SUNDIALS_EXPORT int SUNLinSolSetup_Band(SUNLinearSolver S, SUNMatrix A);
SUNDIALS_EXPORT int SUNLinSolSolve_Band(SUNLinearSolver S, SUNMatrix A,
                                        N_Vector x, N_Vector b, realtype tol);
SUNDIALS_EXPORT long int SUNLinSolLastFlag_Band(SUNLinearSolver S);
SUNDIALS_EXPORT int SUNLinSolSpace_Band(SUNLinearSolver S,
                                        long int *lenrwLS,
                                        long int *leniwLS);
SUNDIALS_EXPORT int SUNLinSolFree_Band(SUNLinearSolver S);

#ifdef __cplusplus
}
#endif

#endif
