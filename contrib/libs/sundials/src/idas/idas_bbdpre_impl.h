/*-----------------------------------------------------------------
 * Programmer(s): Daniel R. Reynolds @ SMU
 *                Alan C. Hindmarsh and Radu Serban @ LLNL
 *-----------------------------------------------------------------
 * SUNDIALS Copyright Start
 * Copyright (c) 2002-2019, Lawrence Livermore National Security
 * and Southern Methodist University.
 * All rights reserved.
 *
 * See the top-level LICENSE and NOTICE files for details.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 * SUNDIALS Copyright End
 *-----------------------------------------------------------------
 * This is the header file (private version) for the IDABBDPRE
 * module, for a band-block-diagonal preconditioner, i.e. a
 * block-diagonal matrix with banded blocks, for use with IDA
 * and an IDASPILS linear solver.
 *-----------------------------------------------------------------*/

#ifndef _IDASBBDPRE_IMPL_H
#define _IDASBBDPRE_IMPL_H

#include <idas/idas_bbdpre.h>
#include <sunmatrix/sunmatrix_band.h>
#include <sunlinsol/sunlinsol_band.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/*
 * -----------------------------------------------------------------
 * Definition of IBBDPrecData
 * -----------------------------------------------------------------
 */

typedef struct IBBDPrecDataRec {

  /* passed by user to IDABBDPrecAlloc and used by
     IDABBDPrecSetup/IDABBDPrecSolve functions */
  sunindextype mudq, mldq, mukeep, mlkeep;
  realtype rel_yy;
  IDABBDLocalFn glocal;
  IDABBDCommFn gcomm;

  /* set by IDABBDPrecSetup and used by IDABBDPrecSetup and 
     IDABBDPrecSolve functions */
  sunindextype n_local;
  SUNMatrix PP;
  SUNLinearSolver LS;
  N_Vector zlocal;
  N_Vector rlocal;
  N_Vector tempv1;
  N_Vector tempv2;
  N_Vector tempv3;
  N_Vector tempv4;

  /* available for optional output */
  long int rpwsize;
  long int ipwsize;
  long int nge;

  /* pointer to ida_mem */
  void *ida_mem;

} *IBBDPrecData;

/*
 * -----------------------------------------------------------------
 * Type: IDABBDPrecDataB
 * -----------------------------------------------------------------
 */

typedef struct IDABBDPrecDataRecB {

  /* BBD user functions (glocB and cfnB) for backward run */
  IDABBDLocalFnB glocalB;
  IDABBDCommFnB  gcommB;
    
} *IDABBDPrecDataB;


/*
 * -----------------------------------------------------------------
 * IDABBDPRE error messages
 * -----------------------------------------------------------------
 */

#define MSGBBD_MEM_NULL    "Integrator memory is NULL."
#define MSGBBD_LMEM_NULL   "Linear solver memory is NULL. One of the SPILS linear solvers must be attached."
#define MSGBBD_MEM_FAIL    "A memory request failed."
#define MSGBBD_BAD_NVECTOR "A required vector operation is not implemented."
#define MSGBBD_SUNMAT_FAIL "An error arose from a SUNBandMatrix routine."
#define MSGBBD_SUNLS_FAIL  "An error arose from a SUNBandLinearSolver routine."
#define MSGBBD_PMEM_NULL   "BBD peconditioner memory is NULL. IDABBDPrecInit must be called."
#define MSGBBD_FUNC_FAILED "The Glocal or Gcomm routine failed in an unrecoverable manner."

#define MSGBBD_AMEM_NULL   "idaadj_mem = NULL illegal."
#define MSGBBD_PDATAB_NULL "IDABBDPRE memory is NULL for the backward integration."
#define MSGBBD_BAD_T       "Bad t for interpolation."

#ifdef __cplusplus
}
#endif

#endif
