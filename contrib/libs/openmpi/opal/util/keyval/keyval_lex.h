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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_UTIL_KEYVAL_LEX_H_
#define OPAL_UTIL_KEYVAL_LEX_H_

#include "opal_config.h"

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

int opal_util_keyval_yylex(void);
int opal_util_keyval_init_buffer(FILE *file);
int opal_util_keyval_yylex_destroy(void);

extern FILE *opal_util_keyval_yyin;
extern bool opal_util_keyval_parse_done;
extern char *opal_util_keyval_yytext;
extern int opal_util_keyval_yynewlines;
extern int opal_util_keyval_yylineno;

/*
 * Make lex-generated files not issue compiler warnings
 */
#define YY_STACK_USED 0
#define YY_ALWAYS_INTERACTIVE 0
#define YY_NEVER_INTERACTIVE 0
#define YY_MAIN 0
#define YY_NO_UNPUT 1
#define YY_SKIP_YYWRAP 1

enum opal_keyval_parse_state_t {
    OPAL_UTIL_KEYVAL_PARSE_DONE,
    OPAL_UTIL_KEYVAL_PARSE_ERROR,

    OPAL_UTIL_KEYVAL_PARSE_NEWLINE,
    OPAL_UTIL_KEYVAL_PARSE_EQUAL,
    OPAL_UTIL_KEYVAL_PARSE_SINGLE_WORD,
    OPAL_UTIL_KEYVAL_PARSE_VALUE,
    OPAL_UTIL_KEYVAL_PARSE_MCAVAR,
    OPAL_UTIL_KEYVAL_PARSE_ENVVAR,
    OPAL_UTIL_KEYVAL_PARSE_ENVEQL,

    OPAL_UTIL_KEYVAL_PARSE_MAX
};
typedef enum opal_keyval_parse_state_t opal_keyval_parse_state_t;

#endif
