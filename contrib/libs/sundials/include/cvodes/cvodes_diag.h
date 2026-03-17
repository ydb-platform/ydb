/* ---------------------------------------------------------------------
 * Programmer(s): Radu Serban @ LLNL
 * ---------------------------------------------------------------------
 * SUNDIALS Copyright Start
 * Copyright (c) 2002-2019, Lawrence Livermore National Security
 * and Southern Methodist University.
 * All rights reserved.
 *
 * See the top-level LICENSE and NOTICE files for details.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 * SUNDIALS Copyright End
 * ---------------------------------------------------------------------
 * This is the header file for the CVODE diagonal linear solver, CVDIAG.
 * ---------------------------------------------------------------------*/

#ifndef _CVSDIAG_H
#define _CVSDIAG_H

#include <sundials/sundials_nvector.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/* ---------------------
 * CVDIAG return values
 * --------------------- */

#define CVDIAG_SUCCESS          0
#define CVDIAG_MEM_NULL        -1
#define CVDIAG_LMEM_NULL       -2
#define CVDIAG_ILL_INPUT       -3
#define CVDIAG_MEM_FAIL        -4

/* Additional last_flag values */

#define CVDIAG_INV_FAIL        -5
#define CVDIAG_RHSFUNC_UNRECVR -6
#define CVDIAG_RHSFUNC_RECVR   -7

/* Return values for adjoint module */

#define CVDIAG_NO_ADJ          -101

/* -----------------
 * Forward Problems
 * ----------------- */

/* CVDiag initialization function */

SUNDIALS_EXPORT int CVDiag(void *cvode_mem);

/* Optional output functions */

SUNDIALS_EXPORT int CVDiagGetWorkSpace(void *cvode_mem, long int *lenrwLS,
                                       long int *leniwLS);
SUNDIALS_EXPORT int CVDiagGetNumRhsEvals(void *cvode_mem, long int *nfevalsLS);
SUNDIALS_EXPORT int CVDiagGetLastFlag(void *cvode_mem, long int *flag);
SUNDIALS_EXPORT char *CVDiagGetReturnFlagName(long int flag);

/* -------------------------------------
 * Backward Problems - Function CVDiagB
 * ------------------------------------- */

SUNDIALS_EXPORT int CVDiagB(void *cvode_mem, int which);


#ifdef __cplusplus
}
#endif

#endif
