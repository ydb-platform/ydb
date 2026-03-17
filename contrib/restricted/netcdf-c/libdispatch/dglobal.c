/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See LICENSE.txt for license information.
*/

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "netcdf.h"
#include "ncglobal.h"
#include "nclist.h"
#include "ncuri.h"
#include "ncrc.h"
#include "ncs3sdk.h"

/**************************************************/
/* Global State constants and state */

/* The singleton global state object */
static NCglobalstate* nc_globalstate = NULL;

/* Forward */
static int NC_createglobalstate(void);

/** \defgroup global_state Global state functions. */
/** \{

\ingroup global_state
*/

/* NCglobal state management */

static int
NC_createglobalstate(void)
{
    int stat = NC_NOERR;
    const char* tmp = NULL;
    
    if(nc_globalstate == NULL) {
        nc_globalstate = calloc(1,sizeof(NCglobalstate));
	if(nc_globalstate == NULL) {stat = NC_ENOMEM; goto done;}
	/* Initialize struct pointers */
	if((nc_globalstate->rcinfo = calloc(1,sizeof(struct NCRCinfo)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	if((nc_globalstate->rcinfo->entries = nclistnew())==NULL)
	    {stat = NC_ENOMEM; goto done;}
	if((nc_globalstate->rcinfo->s3profiles = nclistnew())==NULL)
	    {stat = NC_ENOMEM; goto done;}
	memset(&nc_globalstate->chunkcache,0,sizeof(struct ChunkCache));
    }

    /* Get environment variables */
    if(getenv(NCRCENVIGNORE) != NULL)
        nc_globalstate->rcinfo->ignore = 1;
    tmp = getenv(NCRCENVRC);
    if(tmp != NULL && strlen(tmp) > 0)
        nc_globalstate->rcinfo->rcfile = strdup(tmp);
    /* Initialize chunk cache defaults */
    nc_globalstate->chunkcache.size = DEFAULT_CHUNK_CACHE_SIZE;		    /**< Default chunk cache size. */
    nc_globalstate->chunkcache.nelems = DEFAULT_CHUNKS_IN_CACHE;	    /**< Default chunk cache number of elements. */
    nc_globalstate->chunkcache.preemption = DEFAULT_CHUNK_CACHE_PREEMPTION; /**< Default chunk cache preemption. */
    
done:
    return stat;
}

/* Get global state */
NCglobalstate*
NC_getglobalstate(void)
{
    if(nc_globalstate == NULL)
        NC_createglobalstate();
    return nc_globalstate;
}

void
NC_freeglobalstate(void)
{
    NCglobalstate* gs = nc_globalstate;
    if(gs != NULL) {
        nullfree(gs->tempdir);
        nullfree(gs->home);
        nullfree(gs->cwd);
	memset(&gs->chunkcache,0,sizeof(struct ChunkCache));
	NC_clearawsparams(&gs->aws);
        if(gs->rcinfo) {
	    NC_rcclear(gs->rcinfo);
	    free(gs->rcinfo);
	}
	nclistfree(gs->pluginpaths);
	free(gs);
	nc_globalstate = NULL;
    }
}

/** \} */

void
NC_clearawsparams(struct GlobalAWS* aws)
{
    nullfree(aws->default_region);
    nullfree(aws->config_file);
    nullfree(aws->profile);
    nullfree(aws->access_key_id);
    nullfree(aws->secret_access_key);
    memset(aws,0,sizeof(struct GlobalAWS));
}

