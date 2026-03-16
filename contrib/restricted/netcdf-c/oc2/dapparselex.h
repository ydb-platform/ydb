/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#ifndef DAPPARSELEX_H
#define DAPPARSELEX_H 1

#include "ocinternal.h"
#include "ocdebug.h"

/* For consistency with Java parser */
#define null NULL

typedef void* Object;

#define YYSTYPE Object

#define MAX_TOKEN_LENGTH 1024

/*! Specifies the Lexstate. */
typedef struct DAPlexstate {
    char* input;
    char* next; /* next char in uri.query*/
    NCbytes* yytext;
    int lineno;
    /*! Specifies the Lasttoken. */
    int lasttoken;
    char lasttokentext[MAX_TOKEN_LENGTH+1];
    const char* wordchars1;
    const char* wordcharsn;
    const char* worddelims;
    NClist* reclaim; /* reclaim WORD_WORD instances */
} DAPlexstate;

/*! Specifies the DAPparsestate. */
typedef struct DAPparsestate {
    struct OCnode* root;
    DAPlexstate* lexstate;
    NClist* ocnodes;
    struct OCstate* conn;
    /* Provide a flag for semantic failures during parse */
    OCerror error; /* OC_EDAPSVC=> we had a server failure; else we had a semantic error */
    char* code;
    char* message;
    char* progtype;
    char* progname;
    /* State for constraint expressions */
    struct CEstate* cestate;
} DAPparsestate;

extern int dapdebug; /* global state */

extern int daperror(DAPparsestate*, const char*);
extern int dapsemanticerror(DAPparsestate* state, OCerror, const char* msg);
extern void dap_parse_error(DAPparsestate*,const char *fmt, ...);
/* bison parse entry point */
extern int dapparse(DAPparsestate*);

extern Object dap_datasetbody(DAPparsestate*,Object decls, Object name);
extern Object dap_declarations(DAPparsestate*,Object decls, Object decl);
extern Object dap_arraydecls(DAPparsestate*,Object arraydecls, Object arraydecl);
extern Object dap_arraydecl(DAPparsestate*,Object name, Object size);

extern void dap_dassetup(DAPparsestate*);
extern Object dap_attributebody(DAPparsestate*,Object attrlist);
extern Object dap_attrlist(DAPparsestate*,Object attrlist, Object attrtuple);
extern Object dap_attribute(DAPparsestate*,Object name, Object value, Object etype);
extern Object dap_attrset(DAPparsestate*,Object name, Object attributes);
extern Object dap_attrvalue(DAPparsestate*,Object valuelist, Object value, Object etype);

extern Object dap_makebase(DAPparsestate*,Object name, Object etype, Object dimensions);
extern Object dap_makestructure(DAPparsestate*,Object name, Object dimensions, Object fields);
extern Object dap_makesequence(DAPparsestate*,Object name, Object members);
extern Object dap_makegrid(DAPparsestate*,Object name, Object arraydecl, Object mapdecls);

extern void dap_errorbody(DAPparsestate*, Object, Object, Object, Object);
extern void dap_unrecognizedresponse(DAPparsestate*);

extern void dap_tagparse(DAPparsestate*,int);

/* Lexer entry points */
extern int daplex(YYSTYPE*, DAPparsestate*);
extern void daplexinit(char* input, DAPlexstate** lexstatep);
extern void daplexcleanup(DAPlexstate** lexstatep);
extern void dapsetwordchars(DAPlexstate* lexstate, int kind);
extern char* dapdecode(DAPlexstate*,char*);

extern OCerror DAPparse(OCstate*, struct OCtree*, char*);
extern char* dimnameanon(char* basename, unsigned int index);

#endif /*DAPPARSELEX_H*/
