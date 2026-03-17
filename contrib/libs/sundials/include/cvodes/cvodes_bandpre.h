/* -----------------------------------------------------------------
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
 * This is the header file for the CVBANDPRE module, which provides
 * a banded difference quotient Jacobian-based preconditioner.
 * -----------------------------------------------------------------*/

#ifndef _CVSBANDPRE_H
#define _CVSBANDPRE_H

#include <sundials/sundials_nvector.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif


/*-----------------
  FORWARD PROBLEMS
  -----------------*/

/* BandPrec inititialization function */

SUNDIALS_EXPORT int CVBandPrecInit(void *cvode_mem, sunindextype N,
                                   sunindextype mu, sunindextype ml);

/* Optional output functions */

SUNDIALS_EXPORT int CVBandPrecGetWorkSpace(void *cvode_mem,
                                           long int *lenrwLS,
                                           long int *leniwLS);
SUNDIALS_EXPORT int CVBandPrecGetNumRhsEvals(void *cvode_mem,
                                             long int *nfevalsBP);


/*------------------
  BACKWARD PROBLEMS
  ------------------*/

SUNDIALS_EXPORT int CVBandPrecInitB(void *cvode_mem, int which,
                                    sunindextype nB, sunindextype muB,
                                    sunindextype mlB);


#ifdef __cplusplus
}
#endif

#endif
