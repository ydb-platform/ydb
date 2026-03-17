/*
 * ----------------------------------------------------------------- 
 * Programmer(s): Daniel R. Reynolds @ SMU
 *                Radu Serban @ LLNL
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
 * Implementation header file for the CVSBANDPRE module.
 * -----------------------------------------------------------------
 */

#ifndef _CVSBANDPRE_IMPL_H
#define _CVSBANDPRE_IMPL_H

#include <cvodes/cvodes_bandpre.h>
#include <sunmatrix/sunmatrix_band.h>
#include <sunlinsol/sunlinsol_band.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/*-----------------------------------------------------------------
  Type: CVBandPrecData
  -----------------------------------------------------------------*/

typedef struct CVBandPrecDataRec {

  /* Data set by user in CVBandPrecInit */
  sunindextype N;
  sunindextype ml, mu;

  /* Data set by CVBandPrecSetup */
  SUNMatrix savedJ;
  SUNMatrix savedP;
  SUNLinearSolver LS;
  N_Vector tmp1;
  N_Vector tmp2;

  /* Rhs calls */
  long int nfeBP;

  /* Pointer to cvode_mem */
  void *cvode_mem;

} *CVBandPrecData;

  
/*-----------------------------------------------------------------
  CVBANDPRE error messages
  -----------------------------------------------------------------*/

#define MSGBP_MEM_NULL       "Integrator memory is NULL."
#define MSGBP_LMEM_NULL      "Linear solver memory is NULL. One of the SPILS linear solvers must be attached."
#define MSGBP_MEM_FAIL       "A memory request failed."
#define MSGBP_BAD_NVECTOR    "A required vector operation is not implemented."
#define MSGBP_SUNMAT_FAIL    "An error arose from a SUNBandMatrix routine."
#define MSGBP_SUNLS_FAIL     "An error arose from a SUNBandLinearSolver routine."
#define MSGBP_PMEM_NULL      "Band preconditioner memory is NULL. CVBandPrecInit must be called."
#define MSGBP_RHSFUNC_FAILED "The right-hand side routine failed in an unrecoverable manner."

#define MSGBP_NO_ADJ         "Illegal attempt to call before calling CVodeAdjInit."
#define MSGBP_BAD_WHICH      "Illegal value for parameter which."

#ifdef __cplusplus
}
#endif

#endif
