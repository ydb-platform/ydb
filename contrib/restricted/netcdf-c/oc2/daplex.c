/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#include "config.h"
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif

#include "oc.h"
#include "dapparselex.h"
#include "dapy.h"

#undef URLCVT /* NEVER turn this on */

/* Do we %xx decode all or part of a DAP Identifier: see dapdecode() */
#define DECODE_PARTIAL

#define DAP2ENCODE
#ifdef DAP2ENCODE
#define KEEPSLASH
#endif

/* Forward */
static void dumptoken(DAPlexstate* lexstate);
static void dapaddyytext(DAPlexstate* lex, int c);
#ifndef DAP2ENCODE
static int tohex(int c);
#endif

/****************************************************/

#ifdef INFORMATIONAL
/* Set of all ascii printable characters */
static const char ascii[] = " !\"#$%&'()*+,-./:;<=>?@[]\\^_`|{}~";

/* Define the set of legal nonalphanum characters as specified in the DAP2 spec. */
static const char* daplegal ="_!~*'-\"";
#endif

static const char* ddsworddelims =
  "{}[]:;=,";

/* Define 1 and > 1st legal characters */
/* Note: for some reason I added # and removed !~'"
   what was I thinking?
*/
static const char* ddswordchars1 =
  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  "-+_/%\\.*!~'\"";
static const char* ddswordcharsn =
  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  "-+_/%\\.*!~'\"";

/* This includes sharp and colon for historical reasons */
static const char* daswordcharsn =
  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  "-+_/%\\.*#:!~'\"";

/* Need to remove '.' to allow for fqns */
static const char* cewordchars1 =
  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  "-+_/%\\*!~'\"";
static const char* cewordcharsn =
  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  "-+_/%\\*!~'\"";

/* Current sets of legal characters */
/*
static char* wordchars1 = NULL;
static char* wordcharsn = NULL;
static char* worddelims = NULL;
*/

static const char* keywords[] = {
"alias",
"array",
"attributes",
"byte",
"dataset",
"error",
"float32",
"float64",
"grid",
"int16",
"int32",
"maps",
"sequence",
"string",
"structure",
"uint16",
"uint32",
"url",
"code",
"message",
"program_type",
"program",
NULL /* mark end of the keywords list */
};

static const int keytokens[] = {
SCAN_ALIAS,
SCAN_ARRAY,
SCAN_ATTR,
SCAN_BYTE,
SCAN_DATASET,
SCAN_ERROR,
SCAN_FLOAT32,
SCAN_FLOAT64,
SCAN_GRID,
SCAN_INT16,
SCAN_INT32,
SCAN_MAPS,
SCAN_SEQUENCE,
SCAN_STRING,
SCAN_STRUCTURE,
SCAN_UINT16,
SCAN_UINT32,
SCAN_URL,
SCAN_CODE,
SCAN_MESSAGE,
SCAN_PTYPE,
SCAN_PROG
};

/**************************************************/

int
daplex(YYSTYPE* lvalp, DAPparsestate* state)
{
    DAPlexstate* lexstate = state->lexstate;
    int token;
    int c;
    unsigned int i;
    char* p;
    char* tmp;
    YYSTYPE lval = NULL;

    token = 0;
    ncbytesclear(lexstate->yytext);
    /* invariant: p always points to current char */
    for(p=lexstate->next;token==0&&(c=*p);p++) {
	if(c == '\n') {
	    lexstate->lineno++;
	} else if(c <= ' ' || c == '\177') {
	    /* whitespace: ignore */
	} else if(c == '#') {
	    /* single line comment */
	    while((c=*(++p))) {if(c == '\n') break;}
	} else if(strchr(lexstate->worddelims,c) != NULL) {
	    /* don't put in lexstate->yytext to avoid memory leak */
	    token = c;
	} else if(c == '"') {
	    int more = 1;
	    /* We have a string token; will be reported as WORD_STRING */
	    while(more && (c=*(++p))) {
	        if(c == '"') {
		    more = 0;
		    continue;
		}
#ifdef DAP2ENCODE
		if(c == '\\') {
		    /* Resolve spec ambiguity about handling of \c:
			1. !KEEPSLASH: convert \c to c for any character c
			2. KEEPSLASH: convert \c to \c for any character c;
			   that is, keep the backslash.
			It is clear that the problem being addressed was \".
			But it is unclear what to to do about \n: convert to
                        Ascii LF or leave as \n.
                        This code will leave as \n and assume higher levels
                        of code will address the issue.
		    */
#ifdef KEEPSLASH
		    dapaddyytext(lexstate,c);		    
#endif
		    c=*(++p);
		    if(c == '\0') more = 0;
		}
#else /*Non-standard*/
		switch (c) {
		case '\\':
		    c=*(++p);
		    switch (c) {
		    case 'r': c = '\r'; break;
		    case 'n': c = '\n'; break;
		    case 'f': c = '\f'; break;
		    case 't': c = '\t'; break;
		    case 'x': {
			int d1,d2;
			c = '?';
			++p;
		        d1 = tohex(*p++);
			if(d1 < 0) {
			    daperror(state,"Illegal \\xDD in TOKEN_STRING");
			} else {
			    d2 = tohex(*p++);
			    if(d2 < 0) {
			        daperror(state,"Illegal \\xDD in TOKEN_STRING");
			    } else {
				c=(((unsigned int)d1)<<4) | (unsigned int)d2;
			    }
			}
		    } break;
		    default: break;
		    }
		    break;
		default: break;
		}
#endif /*!DAP2ENCODE*/
		if(more) dapaddyytext(lexstate,c);
	    }
	    token=WORD_STRING;
	} else if(strchr(lexstate->wordchars1,c) != NULL) {
	    int isdatamark = 0;
	    /* we have a WORD_WORD */
	    dapaddyytext(lexstate,c);
	    while((c=*(++p))) {
#ifdef URLCVT
		if(c == '%' && p[1] != 0 && p[2] != 0
			    && strchr(hexdigits,p[1]) != NULL
                            && strchr(hexdigits,p[2]) != NULL) {
		    int d1,d2;
		    d1 = tohex(p[1]);
		    d2 = tohex(p[2]);
		    if(d1 >= 0 || d2 >= 0) {
			c=(((unsigned int)d1)<<4) | (unsigned int)d2;
			p+=2;
		    }
		} else {
		    if(strchr(lexstate->wordcharsn,c) == NULL) {p--; break;}
		}
		dapaddyytext(lexstate,c);
#else
		if(strchr(lexstate->wordcharsn,c) == NULL) {p--; break;}
		dapaddyytext(lexstate,c);
#endif
	    }
	    /* Special check for Data: */
	    tmp = ncbytescontents(lexstate->yytext);
	    if(strcmp(tmp,"Data")==0 && *p == ':') {
		dapaddyytext(lexstate,*p); p++;
		if(p[0] == '\n') {
		    token = SCAN_DATA;
		    isdatamark = 1;
		    p++;
	        } else if(p[0] == '\r' && p[1] == '\n') {
		    token = SCAN_DATA;
		    isdatamark = 1;
		    p+=2;
		}
	    }
	    if(!isdatamark) {
	        /* check for keyword */
	        token=WORD_WORD; /* assume */
	        for(i=0;;i++) {
		    if(keywords[i] == NULL) break;
		    if(strcasecmp(keywords[i],tmp)==0) {
		        token=keytokens[i];
		        break;
		    }
		}
	    }
	} else { /* illegal */
	}
    }
    lexstate->next = p;
    strncpy(lexstate->lasttokentext,ncbytescontents(lexstate->yytext),MAX_TOKEN_LENGTH);
    lexstate->lasttoken = token;
    if(ocdebug >= 2)
	dumptoken(lexstate);

    /*Put return value onto Bison stack*/

    if(ncbyteslength(lexstate->yytext) == 0)
        lval = NULL;
    else {
        lval = ncbytesdup(lexstate->yytext);
	nclistpush(lexstate->reclaim,(void*)lval);
    }
    if(lvalp) *lvalp = lval;
    return token;      /* Return the type of the token.  */
}

static void
dapaddyytext(DAPlexstate* lex, int c)
{
    ncbytesappend(lex->yytext, (char)c);
    ncbytesnull(lex->yytext);
}

#ifndef DAP2ENCODE
static int
tohex(int c)
{
    if(c >= 'a' && c <= 'f') return (c - 'a') + 0xa;
    if(c >= 'A' && c <= 'F') return (c - 'A') + 0xa;
    if(c >= '0' && c <= '9') return (c - '0');
    return -1;
}
#endif

static void
dumptoken(DAPlexstate* lexstate)
{
    fprintf(stderr,"TOKEN = |%s|\n",ncbytescontents(lexstate->yytext));
}

/*
Simple lexer
*/

void
dapsetwordchars(DAPlexstate* lexstate, int kind)
{
    switch (kind) {
    case 0:
	lexstate->worddelims = ddsworddelims;
	lexstate->wordchars1 = ddswordchars1;
	lexstate->wordcharsn = ddswordcharsn;
	break;
    case 1:
	lexstate->worddelims = ddsworddelims;
	lexstate->wordchars1 = ddswordchars1;
	lexstate->wordcharsn = daswordcharsn;
	break;
    case 2:
	lexstate->worddelims = ddsworddelims;
	lexstate->wordchars1 = cewordchars1;
	lexstate->wordcharsn = cewordcharsn;
	break;
    default: break;
    }
}

void
daplexinit(char* input, DAPlexstate** lexstatep)
{
    DAPlexstate* lexstate;
    if(lexstatep == NULL) return; /* no point in building it */
    lexstate = (DAPlexstate*)malloc(sizeof(DAPlexstate));
    *lexstatep = lexstate;
    if(lexstate == NULL) return;
    memset((void*)lexstate,0,sizeof(DAPlexstate));
    lexstate->input = strdup(input);
    lexstate->next = lexstate->input;
    lexstate->yytext = ncbytesnew();
    lexstate->reclaim = nclistnew();
    dapsetwordchars(lexstate,0); /* Assume DDS */
}

void
daplexcleanup(DAPlexstate** lexstatep)
{
    DAPlexstate* lexstate = *lexstatep;
    if(lexstate == NULL) return;
    if(lexstate->input != NULL) ocfree(lexstate->input);
    if(lexstate->reclaim != NULL) {
	while(nclistlength(lexstate->reclaim) > 0) {
	    char* word = (char*)nclistpop(lexstate->reclaim);
	    if(word) free(word);
	}
	nclistfree(lexstate->reclaim);
    }
    ncbytesfree(lexstate->yytext);
    free(lexstate);
    *lexstatep = NULL;
}

/* Dap identifiers will come to us with some
   characters escaped using the URL notation of
   %HH. The assumption here is that any character
   that is encoded is left encoded, except as follows:
   1. if the encoded character is in fact a legal DAP2 character
      (alphanum+"_!~*'-\"") then it is decoded, otherwise not.
*/
#ifdef DECODE_PARTIAL
static const char* decodeset = /* Specify which characters are decoded */
  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_!~*'-\"@";
#endif

char*
dapdecode(DAPlexstate* lexstate, char* name)
{
    char* decoded = NULL;
#ifdef DECODE_PARTIAL
    decoded = ncuridecodepartial(name,decodeset); /* Decode selected */
#else
    decoded = ncuridecode(name); /* Decode everything */
#endif
    nclistpush(lexstate->reclaim,(void*)decoded);
    return decoded;
}
