/* -*- C -*-
 *
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
 * Copyright (c) 2016-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2011 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_UTIL_HOSTFILE_LEX_H_
#define ORTE_UTIL_HOSTFILE_LEX_H_

#include "orte_config.h"

#ifdef malloc
#undef malloc
#endif
#ifdef realloc
#undef realloc
#endif
#ifdef free
#undef free
#endif

#include <stdio.h>

typedef union {
    int ival;
    char* sval;
} orte_hostfile_value_t;

extern int   orte_util_hostfile_lex(void);
extern FILE *orte_util_hostfile_in;
extern int   orte_util_hostfile_line;
extern bool  orte_util_hostfile_done;
extern orte_hostfile_value_t  orte_util_hostfile_value;
extern int orte_util_hostfile_lex_destroy (void );

/*
 * Make lex-generated files not issue compiler warnings
 */
#define YY_STACK_USED 0
#define YY_ALWAYS_INTERACTIVE 0
#define YY_NEVER_INTERACTIVE 0
#define YY_MAIN 0
#define YY_NO_UNPUT 1
#define YY_SKIP_YYWRAP 1

#define ORTE_HOSTFILE_DONE                   0
#define ORTE_HOSTFILE_ERROR                  1
#define ORTE_HOSTFILE_QUOTED_STRING          2
#define ORTE_HOSTFILE_EQUAL                  3
#define ORTE_HOSTFILE_INT                    4
#define ORTE_HOSTFILE_STRING                 5
#define ORTE_HOSTFILE_CPU                    6
#define ORTE_HOSTFILE_COUNT                  7
#define ORTE_HOSTFILE_SLOTS                  8
#define ORTE_HOSTFILE_SLOTS_MAX              9
#define ORTE_HOSTFILE_USERNAME              10
#define ORTE_HOSTFILE_IPV4                  11
#define ORTE_HOSTFILE_HOSTNAME              12
#define ORTE_HOSTFILE_NEWLINE               13
#define ORTE_HOSTFILE_IPV6                  14
#define ORTE_HOSTFILE_SLOT                  15
#define ORTE_HOSTFILE_RELATIVE              16
#define ORTE_HOSTFILE_BOARDS                17
#define ORTE_HOSTFILE_SOCKETS_PER_BOARD     18
#define ORTE_HOSTFILE_CORES_PER_SOCKET      19
/* ensure we can handle a rank_file input */
#define ORTE_HOSTFILE_RANK                  20
#define ORTE_HOSTFILE_PORT                  21

#endif
