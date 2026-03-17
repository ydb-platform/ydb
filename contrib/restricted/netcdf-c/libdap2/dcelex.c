/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#define URLDECODE

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "netcdf.h"

#include "nclist.h"
#include "ncbytes.h"
#include "ncuri.h"
#include "dceconstraints.h"
#include "dceparselex.h"

/* Forward */
static void dumptoken(DCElexstate* lexstate);
static int tohex(int c);
static void ceaddyytext(DCElexstate* lex, int c);

/****************************************************/
/* Define 1 and > 1st legal characters */
static const char* wordchars1 =
  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-+_/%\\";
static const char* wordcharsn =
  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-+_/%\\";

/* Number characters */
static const char* numchars1="+-0123456789";
static const char* numcharsn="Ee.+-0123456789";

/**************************************************/

int
dcelex(YYSTYPE* lvalp, DCEparsestate* state)
{
    DCElexstate* lexstate = state->lexstate;
    int token;
    int c;
    char* p=NULL;
    token = 0;
    ncbytesclear(lexstate->yytext);
    ncbytesnull(lexstate->yytext);
    p=lexstate->next;
    while(token == 0 && (c=*p)) {
	if(c <= ' ' || c >= '\177') {p++; continue;}
	if(c == '"') {
	    int more = 1;
	    ceaddyytext(lexstate,c);
	    /* We have a SCAN_STRINGCONST */
	    while(more && (c=*(++p))) {
		switch (c) {
		case '"': p++; more=0; break;
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
			    dceerror(state,"Illegal \\xDD in SCAN_STRING");
			} else {
			    d2 = tohex(*p++);
			    if(d2 < 0) {
			        dceerror(state,"Illegal \\xDD in SCAN_STRING");
			    } else {
			        c=(int)((((unsigned int)d1)<<4) | (unsigned int)d2);
			    }
			}
		    } break;
		    default: break;
		    }
		    break;
		default: break;
		}
		ceaddyytext(lexstate,c);
	    }
	    token=SCAN_STRINGCONST;
	} else if(strchr(numchars1,c) != NULL) {
	    /* we might have a SCAN_NUMBERCONST */
	    int isnumber = 0;
	    char* yytext;
	    char* endpoint;
	    ceaddyytext(lexstate,c);
	    for(p++;(c=*p);p++) {
		if(strchr(numcharsn,c) == NULL) break;
	        ceaddyytext(lexstate,c);
	    }
	    /* See if this is a number */
	    ncbytesnull(lexstate->yytext);
	    yytext = ncbytescontents(lexstate->yytext);
	    (void)strtoll(yytext,&endpoint,10);
	    if(*yytext != '\0' && *endpoint == '\0')
	        isnumber = 1;
	    else {
	        (void)strtod(yytext,&endpoint);
	        if(*yytext != '\0' && *endpoint == '\0')
	            isnumber = 1; /* maybe */
	    }
	    /* A number followed by an id char is assumed to just be
	       a funny id */
	    if(isnumber && (*p == '\0' || strchr(wordcharsn,*p) == NULL))  {
	        token = SCAN_NUMBERCONST;
	    } else {
		/* Now, if the funny word has a "." in it,
		   we have to back up to that dot */
		char* dotpoint = strchr(yytext,'.');
		if(dotpoint != NULL) {
		    p = dotpoint;
		    *dotpoint = '\0';
		}
		token = SCAN_WORD;
	    }
	} else if(strchr(wordchars1,c) != NULL) {
	    /* we have a SCAN_WORD */
	    ceaddyytext(lexstate,c);
	    for(p++;(c=*p);p++) {
		if(strchr(wordcharsn,c) == NULL) break;
	        ceaddyytext(lexstate,c);
	    }
	    token=SCAN_WORD;
	} else {
	    /* we have a single char token */
	    token = c;
	    ceaddyytext(lexstate,c);
	    p++;
	}
    }
    lexstate->next = p;
    size_t len = ncbyteslength(lexstate->yytext);
    if(len > MAX_TOKEN_LENGTH) len = MAX_TOKEN_LENGTH;
    strncpy(lexstate->lasttokentext,ncbytescontents(lexstate->yytext),len);
    lexstate->lasttokentext[len] = '\0';
    lexstate->lasttoken = token;
    if(dcedebug) dumptoken(lexstate);

    /*Put return value onto Bison stack*/

    if(ncbyteslength(lexstate->yytext) == 0)
        *lvalp = NULL;
    else {
        *lvalp = ncbytesdup(lexstate->yytext);
	nclistpush(lexstate->reclaim,(void*)*lvalp);
    }

    return token;
}

static void
ceaddyytext(DCElexstate* lex, int c)
{
    ncbytesappend(lex->yytext,(char)c);
}

static int
tohex(int c)
{
    if(c >= 'a' && c <= 'f') return (c - 'a') + 0xa;
    if(c >= 'A' && c <= 'F') return (c - 'A') + 0xa;
    if(c >= '0' && c <= '9') return (c - '0');
    return -1;
}

static void
dumptoken(DCElexstate* lexstate)
{
    switch (lexstate->lasttoken) {
    case SCAN_STRINGCONST:
        fprintf(stderr,"TOKEN = |\"%s\"|\n",lexstate->lasttokentext);
	break;
    case SCAN_WORD:
    case SCAN_NUMBERCONST:
    default:
        fprintf(stderr,"TOKEN = |%s|\n",lexstate->lasttokentext);
	break;
    }
}

void
dcelexinit(char* input, DCElexstate** lexstatep)
{
    DCElexstate* lexstate = (DCElexstate*)malloc(sizeof(DCElexstate));

    /* If lexstatep is NULL,
       we want to free lexstate and
       return to avoid a memory leak. */
    if(lexstatep) {
      *lexstatep = lexstate;
    } else {
      if(lexstate) free(lexstate);
      return;
    }

    if(lexstate == NULL) return;
    memset((void*)lexstate,0,sizeof(DCElexstate));



#ifdef URLDECODE
    lexstate->input = ncuridecode(input);
#else
    lexstate->input = strdup(input);
#endif
    lexstate->next = lexstate->input;
    lexstate->yytext = ncbytesnew();
    lexstate->reclaim = nclistnew();
}

void
dcelexcleanup(DCElexstate** lexstatep)
{
    DCElexstate* lexstate = *lexstatep;
    if(lexstate == NULL) return;
    if(lexstate->input != NULL) free(lexstate->input);
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
