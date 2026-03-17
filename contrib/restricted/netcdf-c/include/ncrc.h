/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

/*
Common functionality for reading
and accessing rc files (e.g. .daprc).
*/

#ifndef NCRC_H
#define NCRC_H

/* Need these support includes */
#include <stdio.h>
#include "ncuri.h"
#include "nclist.h"
#include "ncbytes.h"
#include "ncs3sdk.h"

/* getenv() keys */
#define NCRCENVIGNORE "NCRCENV_IGNORE"
#define NCRCENVRC "NCRCENV_RC"
#define NCRCENVHOME "NCRCENV_HOME"

typedef struct NCRCentry {
	char* host; /* combined host:port */
	char* urlpath; /* prefix to match or NULL */
        char* key;
        char* value;
} NCRCentry;

/* collect all the relevant info around the rc file and AWS */
typedef struct NCRCinfo {
	int ignore; /* if 1, then do not use any rc file */
	int loaded; /* 1 => already loaded */
        NClist* entries; /* the rc file entry store fields*/
        char* rcfile; /* specified rcfile; overrides anything else */
        char* rchome; /* Overrides $HOME when looking for .rc files */
	NClist* s3profiles; /* NClist<struct AWSprofile*> */
} NCRCinfo;

/* Opaque structures */

#if defined(__cplusplus)
extern "C" {
#endif

/* From drc.c */
EXTERNL void ncrc_initialize(void);
EXTERNL int NC_rcfile_insert(const char* key, const char* hostport, const char* path, const char* value);
EXTERNL char* NC_rclookup(const char* key, const char* hostport, const char* path);
EXTERNL char* NC_rclookupx(NCURI* uri, const char* key);

/* Following are primarily for debugging */
/* Obtain the count of number of entries */
EXTERNL size_t NC_rcfile_length(NCRCinfo*);
/* Obtain the ith entry; return NULL if out of range */
EXTERNL NCRCentry* NC_rcfile_ith(NCRCinfo*,size_t);

/* For internal use */
EXTERNL void NC_rcclear(NCRCinfo* info);
EXTERNL void NC_rcclear(NCRCinfo* info);

#if defined(__cplusplus)
}
#endif

#endif /*NCRC_H*/
