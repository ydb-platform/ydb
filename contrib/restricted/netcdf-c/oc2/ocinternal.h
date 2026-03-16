/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#ifndef OCINTERNAL_H
#define OCINTERNAL_H

#include "config.h"

#ifdef _MSC_VER
#include <malloc.h>
#endif

/* Required for getcwd, other functions. */
#ifdef _MSC_VER
#include <direct.h>
#define getcwd _getcwd
#endif

#ifdef _AIX
#include <netinet/in.h>
#endif

#include <stdlib.h>
#include <assert.h>
#include <string.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif
#ifdef HAVE_STDARG_H
#include <stdarg.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#define CURL_DISABLE_TYPECHECK 1
#include <curl/curl.h>

#include "netcdf.h"
#include "ncauth.h"
#include "nclist.h"
#include "ncbytes.h"
#include "ncuri.h"

#ifndef HAVE_STRNDUP
/* Not all systems have strndup, so provide one*/
#define strndup ocstrndup
#endif

#define OCCACHEPOS

#ifndef HAVE_STRNDUP
/* Not all systems have strndup, so provide one*/
#define strndup ocstrndup
#endif

#define OCPATHMAX 8192

#ifndef nullfree
#define nullfree(x) {if((x)!=NULL) free(x);}
#endif

/* Forwards */
typedef struct OCstate OCstate;
typedef struct OCnode OCnode;
typedef struct OCdata OCdata;
struct OCTriplestore;

/* Define the internal node classification values */
#define OC_None  ((unsigned int)0)
#define OC_State ((unsigned int)1)
#define OC_Node  ((unsigned int)2)
#define OC_Data  ((unsigned int)3)

/* Define a magic number to mark externally visible oc objects */
#define OCMAGIC ((unsigned int)0x0c0c0c0c) /*clever, huh*/

/* Max rc file line size */
#define MAXRCLINESIZE 4096

/* Max number of triples from an rc file */
#define MAXRCLINES 4096

/* Define a struct that all oc objects must start with */
/* OCheader must be defined here to make it available in other headers */
typedef struct OCheader {
    unsigned int magic;
    unsigned int occlass;
} OCheader;

#include "oc.h"
#include "ocx.h"
#include "ocnode.h"
#include "ocdata.h"
#include "occonstraints.h"
#include "ocutil.h"
#include "nclog.h"
#include "xxdr.h"
#include "ocdatatypes.h"
#include "occompile.h"

#ifndef nulldup
#define nulldup(s) (s==NULL?NULL:strdup(s))
#endif

/*
 * Macros for dealing with flag bits.
 */
#define fset(t,f)       ((t) |= (f))
#define fclr(t,f)       ((t) &= ~(f))
#define fisset(t,f)     ((t) & (f))

#define nullstring(s) (s==NULL?"(null)":s)
#define PATHSEPARATOR "."

#define OCCOOKIEDIR "occookies"

/* Define infinity for memory size */
#if SIZEOF_SIZE_T == 4 
#define OCINFINITY ((size_t)0xffffffff)
#else
#define OCINFINITY ((size_t)0xffffffffffffffff)
#endif

/* Extend the OCdxd type for internal use */
#define OCVER 3

/* Default initial memory packet size */
#define DFALTPACKETSIZE 0x20000 /*approximately 100k bytes*/

/* Default maximum memory packet size */
#define DFALTMAXPACKETSIZE 0x3000000 /*approximately 50M bytes*/

/* Default user agent string (will have version appended)*/
#ifndef DFALTUSERAGENT
#define DFALTUSERAGENT "oc"
#endif

#if 0
/* Hold known curl flags */
enum OCCURLFLAGTYPE {CF_UNKNOWN=0,CF_OTHER=1,CF_STRING=2,CF_LONG=3};
struct OCCURLFLAG {
    const char* name;
    int flag;
    int value;
    enum OCCURLFLAGTYPE type;
};
#endif

struct OCTriplestore {
    int ntriples;
    struct OCTriple {
        char host[MAXRCLINESIZE]; /* includes port if specified */
        char key[MAXRCLINESIZE];
        char value[MAXRCLINESIZE];
   } triples[MAXRCLINES];
};

/*! Specifies the OCstate = non-opaque version of OClink */
struct OCstate {
    OCheader header; /* class=OC_State */
    NClist* trees; /* list<OCNODE*> ; all root objects */
    NCURI* uri; /* parsed base URI*/
    NCbytes* packet; /* shared by all trees during construction */
    struct OCerrdata {/* Hold info for an error return from server */
	char* code;
	char* message;
	long  httpcode;
	char  curlerrorbuf[CURL_ERROR_SIZE]; /* CURLOPT_ERRORBUFFER*/
    } error;
    CURL* curl; /* curl handle*/
    char curlerror[CURL_ERROR_SIZE];
    void* usercurldata;
    NCauth* auth; /* curl auth data */
    long ddslastmodified;
    long datalastmodified;
    long curlbuffersize;
    struct {
	int active; /* Activate keepalive? */
	long idle; /* KEEPIDLE value */
	long interval; /* KEEPINTVL value */
    } curlkeepalive; /* keepalive info */
};

/*! OCtree holds extra state info about trees */

typedef struct OCtree
{
    OCdxd  dxdclass;
    char* constraint;
    char* text;
    struct OCnode* root; /* cross link */
    struct OCstate* state; /* cross link */
    NClist* nodes; /* all nodes in tree*/
    /* when dxdclass == OCDATADDS */
    struct {
	char*   memory;   /* allocated memory if OC_ONDISK is not set */
        char*   filename; /* If OC_ONDISK is set */
        FILE*   file;
        off_t   datasize; /* xdr size on disk or in memory */
        off_t   bod;      /* offset of the beginning of packet data */
        off_t   ddslen;   /* length of ddslen (assert(ddslen <= bod)) */
        XXDR*   xdrs;		/* access either memory or file */
        OCdata* data;
    } data;
} OCtree;

/* (Almost) All shared procedure definitions are kept here
   except for: ocdebug.h ocutil.h
   The true external interface is defined in oc.h
*/

#if 0
/* Location: ceparselex.c*/
extern int cedebug;
extern NClist* CEparse(OCstate*,char* input);
#endif

extern int ocinitialized;


extern OCerror ocopen(OCstate** statep, const char* url);
extern void occlose(OCstate* state);
extern OCerror ocfetch(OCstate*, const char*, OCdxd, OCflags, OCnode**);
extern int oc_network_order;
extern int oc_invert_xdr_double;
extern OCerror ocinternalinitialize(void);

extern OCerror ocupdatelastmodifieddata(OCstate* state, OCflags);

extern OCerror ocset_useragent(OCstate* state, const char* agent);
extern OCerror ocset_netrc(OCstate* state, const char* path);

/* From ocrc.c */
extern OCerror ocrc_load(void); /* find, read, and compile */
extern OCerror ocrc_process(OCstate* state); /* extract relevant triples */
extern char* ocrc_lookup(char* key, char* url);
extern struct OCTriple* ocrc_triple_iterate(char* key, char* url, struct OCTriple* prevp);
extern int ocparseproxy(OCstate* state, char* v);

#endif /*COMMON_H*/
