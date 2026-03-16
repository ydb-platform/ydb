/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2008 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2014 Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_BTL_BASE_ERROR_H
#define MCA_BTL_BASE_ERROR_H

#include "opal_config.h"

#include <errno.h>
#include <stdio.h>

#include "opal/util/proc.h"

OPAL_DECLSPEC extern int mca_btl_base_verbose;

OPAL_DECLSPEC extern int mca_btl_base_err(const char*, ...) __opal_attribute_format__(__printf__, 1, 2);
OPAL_DECLSPEC extern int mca_btl_base_out(const char*, ...) __opal_attribute_format__(__printf__, 1, 2);

#define BTL_OUTPUT(args)                                        \
    do {                                                        \
        mca_btl_base_out("[%s]%s[%s:%d:%s] ",                   \
                         opal_process_info.nodename,            \
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),    \
                         __FILE__, __LINE__, __func__);         \
        mca_btl_base_out args;                                  \
        mca_btl_base_out("\n");                                 \
    } while(0);


#define BTL_ERROR(args)                                         \
    do {                                                        \
        mca_btl_base_err("[%s]%s[%s:%d:%s] ",                   \
                         opal_process_info.nodename,            \
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),    \
                         __FILE__, __LINE__, __func__);         \
        mca_btl_base_err args;                                  \
        mca_btl_base_err("\n");                                 \
    } while(0);

#define BTL_PEER_ERROR(proc, args)                              \
    do {                                                        \
        mca_btl_base_err("%s[%s:%d:%s] from %s ",               \
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),    \
                         __FILE__, __LINE__, __func__,          \
                         opal_process_info.nodename);           \
        if (proc) {                                             \
            mca_btl_base_err("to: %s ",                         \
                             opal_get_proc_hostname(proc));     \
        }                                                       \
        mca_btl_base_err args;                                  \
        mca_btl_base_err("\n");                                 \
    } while(0);


#if OPAL_ENABLE_DEBUG
#define BTL_VERBOSE(args)                                               \
    do {                                                                \
        if(mca_btl_base_verbose > 0) {                                  \
            mca_btl_base_err("[%s]%s[%s:%d:%s] ",                       \
                             opal_process_info.nodename,                \
                             OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),        \
                             __FILE__, __LINE__, __func__);             \
            mca_btl_base_err args;                                      \
            mca_btl_base_err("\n");                                     \
        }                                                               \
    } while(0);
#else
#define BTL_VERBOSE(args)
#endif

#endif


BEGIN_C_DECLS

OPAL_DECLSPEC extern void mca_btl_base_error_no_nics(const char* transport,
                                                     const char* nic_name);

END_C_DECLS
