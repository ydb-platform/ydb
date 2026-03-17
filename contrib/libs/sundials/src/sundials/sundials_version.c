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
 * This file implements functions for getting SUNDIALS version
 * information.
 * -----------------------------------------------------------------*/

#include <string.h>

#include <sundials/sundials_version.h>

/* fill string with SUNDIALS version information */
int SUNDIALSGetVersion(char *version, int len)
{
  if (strlen(SUNDIALS_VERSION) > len) {
    return(-1);
  }
  
  strncpy(version, SUNDIALS_VERSION, len);
  return(0);
}

/* fill integers with SUNDIALS major, minor, and patch release 
   numbers and fill a string with the release label */
int SUNDIALSGetVersionNumber(int *major, int *minor, int *patch, 
                             char *label, int len)
{
  if (strlen(SUNDIALS_VERSION_LABEL) > len) {
    return(-1);
  }
  
  *major = SUNDIALS_VERSION_MAJOR;
  *minor = SUNDIALS_VERSION_MINOR;
  *patch = SUNDIALS_VERSION_PATCH;
  strncpy(label, SUNDIALS_VERSION_LABEL, len);

  return(0);
}
