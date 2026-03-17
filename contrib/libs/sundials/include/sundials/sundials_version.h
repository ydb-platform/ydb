/* -----------------------------------------------------------------
 * Programmer(s): David J. Gardner @ LLNL
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
 * This header file is for routines to get SUNDIALS version info
 * -----------------------------------------------------------------*/

#ifndef _SUNDIALS_VERSION_H
#define _SUNDIALS_VERSION_H

#include <sundials/sundials_config.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/* Fill a string with SUNDIALS version information */
SUNDIALS_EXPORT int SUNDIALSGetVersion(char *version, int len);

/* Fills integers with the major, minor, and patch release version numbers and a
   string with the release label.*/
SUNDIALS_EXPORT int SUNDIALSGetVersionNumber(int *major, int *minor, int *patch,
                                             char *label, int len);

#ifdef __cplusplus
}
#endif

#endif
