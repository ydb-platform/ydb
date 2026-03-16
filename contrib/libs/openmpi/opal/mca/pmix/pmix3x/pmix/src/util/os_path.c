/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015-2018 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>


#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif  /* HAVE_SYS_PARAM_H */
#include <stdlib.h>
#include <stdarg.h>

#include "src/util/os_path.h"

static const char *path_sep = PMIX_PATH_SEP;

char *pmix_os_path(int relative, ...)
{
    va_list ap;
    char *element, *path;
    size_t num_elements, total_length;

    va_start(ap, relative);

    /* no way to protect ourselves from reading too far, so have to
       trust caller that they ended the list with the NULL */

    num_elements = 0;
    total_length = 0;
    while (NULL != (element = va_arg(ap, char*))) {
        num_elements++;
        total_length = total_length + strlen(element);
        if( path_sep[0] != element[0] ) total_length++;
    }
    va_end(ap);

    if (0 == num_elements) { /* must be looking for a simple answer */
        path = (char *)malloc(3);
        path[0] = '\0';
        if (relative) {
            strcpy(path, ".");
            strcat(path, path_sep);
        } else {
            strcpy(path, path_sep);
        }
        return(path);
    }

    /* setup path with enough room for the string terminator, the elements, and
       the separator between each of the elements */
    total_length = total_length + num_elements * strlen(path_sep) + 1;
    if(relative) {
        total_length++;
    }

    if (total_length > PMIX_PATH_MAX) {  /* path length is too long - reject it */
        return(NULL);
    }

    path = (char *)malloc(total_length);
    if (NULL == path) {
        return(NULL);
    }
    path[0] = 0;

    if (relative) {
        strcpy(path, ".");
    }

    va_start(ap, relative);
    if( NULL != (element = va_arg(ap, char*)) ) {
        if (path_sep[0] != element[0]) {
            strcat(path, path_sep);
        }
        strcat(path, element);
    }
    while (NULL != (element=va_arg(ap, char*))) {
        if (path_sep[0] != element[0]) {
            strcat(path, path_sep);
        }
        strcat(path, element);
    }

    va_end(ap);
    return pmix_make_filename_os_friendly(path);
}
