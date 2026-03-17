/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */

#ifndef OPAL_NUMTOSTR_UTIL
#define OPAL_NUMTOSTR_UTIL

#include "opal_config.h"
/**
 * Convert a long integer to a char* string.  The returned buffer is
 * allocated by calling malloc() and must be freed by the caller.
 *
 *  @param num (IN)      Input number
 *  @return              String containing number (NULL on failure)
 */
OPAL_DECLSPEC char* opal_ltostr(long num);


/**
 * Convert a double to a char* string.  The returned buffer is allocated
 * by calling malloc() and must be freed by the caller.
 *
 * @param num (IN)       Input number
 * @return               String containing number (NULL on failure)
 */
OPAL_DECLSPEC char* opal_dtostr(double num);

#endif /* OPAL_NUMTOSTR_UTIL */
