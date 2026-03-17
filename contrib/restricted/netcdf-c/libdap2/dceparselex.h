/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#ifndef DCEPARSELEX_H
#define DCEPARSELEX_H

#include "config.h"

/* Forward */
struct DCEparsestate;
typedef struct DCEparsestate DCEparsestate;

#include "dcetab.h"

#ifdef _WIN32


#endif

/* For consistency with Java parser */
#ifndef null
#define null NULL
#endif

typedef void* Object;

#define YYSTYPE Object

#define MAX_TOKEN_LENGTH 1024

/*! Specifies DCElexstate. */
typedef struct DCElexstate {
    char* input;
    char* next; /* next char in uri.query */
    NCbytes* yytext;
    /*! Specifies the Lasttoken. */
    int lasttoken;
    char lasttokentext[MAX_TOKEN_LENGTH+1]; /* leave room for trailing null */
    NClist* reclaim; /* reclaim SCAN_WORD instances */
} DCElexstate;

/*! Specifies DCEparsestate. */
struct DCEparsestate {
    DCEconstraint* constraint;
    char errorbuf[1024];
    int errorcode;
    DCElexstate* lexstate;
};

/* Define a generic object carrier; this serves
   essentially the same role as the typical bison %union
   declaration
*/
   

extern int ceerror(DCEparsestate*,char*);
extern void ce_parse_error(DCEparsestate*,const char *fmt, ...);

/* bison parse entry point */
extern int dceparse(DCEparsestate*);

extern int dceerror(DCEparsestate* state, char* msg);
extern void projections(DCEparsestate* state, Object list0);
extern void selections(DCEparsestate* state, Object list0);
extern Object projectionlist(DCEparsestate* state, Object list0, Object decl);
extern Object projection(DCEparsestate* state, Object segmentlist);
extern Object segmentlist(DCEparsestate* state, Object list0, Object decl);
extern Object segment(DCEparsestate* state, Object name, Object slices0);
extern Object array_indices(DCEparsestate* state, Object list0, Object decl);
extern Object dap2_range(DCEparsestate* state, Object, Object, Object);
extern Object selectionlist(DCEparsestate* state, Object list0, Object decl);
extern Object sel_clause(DCEparsestate* state, int selcase, Object path0, Object relop0, Object values);
extern Object selectionpath(DCEparsestate* state, Object list0, Object text);
extern Object arrayelement(DCEparsestate* state, Object name, Object index);
extern Object function(DCEparsestate* state, Object fcnname, Object args);
extern Object arg_list(DCEparsestate* state, Object list0, Object decl);
extern Object value_list(DCEparsestate* state, Object list0, Object decl);
extern Object value(DCEparsestate* state, Object value);
extern Object makeselectiontag(CEsort);
extern Object indexer(DCEparsestate* state, Object name, Object indices);
extern Object indexpath(DCEparsestate* state, Object list0, Object index);
extern Object var(DCEparsestate* state, Object indexpath);
extern Object constant(DCEparsestate* state, Object val, int tag);
extern Object clauselist(DCEparsestate* state, Object list0, Object decl);
extern Object range1(DCEparsestate* state, Object rangenumber);
extern Object rangelist(DCEparsestate* state, Object list0, Object decl);

/* lexer interface */
extern int dcelex(YYSTYPE*, DCEparsestate*);
extern void dcelexinit(char* input, DCElexstate** lexstatep);
extern void dcelexcleanup(DCElexstate** lexstatep);

extern int dcedebug;

#ifdef PARSEDEBUG
extern Object debugobject(Object);
#define checkobject(x) debugobject(x)
#else
#define checkobject(x) (x)
#endif

extern int dapceparse(char* input, DCEconstraint*, char**);

#endif /*DCEPARSELEX_H*/

