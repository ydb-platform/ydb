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
 # Copyright (c) 2016      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_UTIL_KEYVAL_LEX_H_
#define PMIX_UTIL_KEYVAL_LEX_H_

#include <src/include/pmix_config.h>

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

int pmix_util_keyval_yylex(void);
int pmix_util_keyval_init_buffer(FILE *file);
int pmix_util_keyval_yylex_destroy(void);

extern FILE *pmix_util_keyval_yyin;
extern bool pmix_util_keyval_parse_done;
extern char *pmix_util_keyval_yytext;
extern int pmix_util_keyval_yynewlines;
extern int pmix_util_keyval_yylineno;

/*
 * Make lex-generated files not issue compiler warnings
 */
#define YY_STACK_USED 0
#define YY_ALWAYS_INTERACTIVE 0
#define YY_NEVER_INTERACTIVE 0
#define YY_MAIN 0
#define YY_NO_UNPUT 1
#define YY_SKIP_YYWRAP 1

enum pmix_keyval_parse_state_t {
    PMIX_UTIL_KEYVAL_PARSE_DONE,
    PMIX_UTIL_KEYVAL_PARSE_ERROR,

    PMIX_UTIL_KEYVAL_PARSE_NEWLINE,
    PMIX_UTIL_KEYVAL_PARSE_EQUAL,
    PMIX_UTIL_KEYVAL_PARSE_SINGLE_WORD,
    PMIX_UTIL_KEYVAL_PARSE_VALUE,
    PMIX_UTIL_KEYVAL_PARSE_MCAVAR,
    PMIX_UTIL_KEYVAL_PARSE_ENVVAR,
    PMIX_UTIL_KEYVAL_PARSE_ENVEQL,

    PMIX_UTIL_KEYVAL_PARSE_MAX
};
typedef enum pmix_keyval_parse_state_t pmix_keyval_parse_state_t;

#endif
