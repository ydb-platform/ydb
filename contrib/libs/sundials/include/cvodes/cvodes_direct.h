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
 * Header file for the deprecated direct linear solver interface in
 * CVODES; these routines now just wrap the updated CVODE generic
 * linear solver interface in cvodes_ls.h.
 * -----------------------------------------------------------------*/

#ifndef _CVSDLS_H
#define _CVSDLS_H

#include <cvodes/cvodes_ls.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif


/*=================================================================
  Function Types (typedefs for equivalent types in cvodes_ls.h)
  =================================================================*/

typedef CVLsJacFn CVDlsJacFn;
typedef CVLsJacFnB CVDlsJacFnB;
typedef CVLsJacFnBS CVDlsJacFnBS;

/*====================================================================
  Exported Functions (wrappers for equivalent routines in cvodes_ls.h)
  ====================================================================*/

int CVDlsSetLinearSolver(void *cvode_mem, SUNLinearSolver LS,
                         SUNMatrix A);

int CVDlsSetJacFn(void *cvode_mem, CVDlsJacFn jac);

int CVDlsGetWorkSpace(void *cvode_mem, long int *lenrwLS,
                      long int *leniwLS);

int CVDlsGetNumJacEvals(void *cvode_mem, long int *njevals);

int CVDlsGetNumRhsEvals(void *cvode_mem, long int *nfevalsLS);

int CVDlsGetLastFlag(void *cvode_mem, long int *flag);

char *CVDlsGetReturnFlagName(long int flag);

int CVDlsSetLinearSolverB(void *cvode_mem, int which,
                          SUNLinearSolver LS, SUNMatrix A);

int CVDlsSetJacFnB(void *cvode_mem, int which, CVDlsJacFnB jacB);

int CVDlsSetJacFnBS(void *cvode_mem, int which, CVDlsJacFnBS jacBS);


#ifdef __cplusplus
}
#endif

#endif
