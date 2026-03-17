/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "d4includes.h"
#include "d4chunk.h"

#define CHECKSUMFLAG

/**************************************************/

/**************************************************/

/*
Given a packet as read from the wire via http (or a file), convert in
place from chunked format to a single contiguous set of bytes. If an
error packet is recovered, then make that available to the caller and
return an error. Also return whether the data was big endian encoded
and whether it has checksums.
Notes:
*/

/* Forward */
static int processerrchunk(NCD4response*, void* errchunk, unsigned int count);

/**************************************************/
int
NCD4_dechunk(NCD4response* resp)
{
    unsigned char *praw, *pdmr, *phdr, *pdap, *pappend, *pchunk;
    NCD4HDR hdr;
    int firstchunk;

#ifdef D4DUMPRAW
    NCD4_tagdump(resp->serial.raw.size,resp->serial.raw.data,0,"RAW");
#endif

    /* Access the returned raw data */
    praw = (unsigned char*)resp->raw.memory;

    if(resp->mode == NCD4_DSR) {
	return THROW(NC_EDMR);
    } else if(resp->mode == NCD4_DMR) {
        /* Verify the mode; assume that the <?xml...?> is optional */
        if(memcmp(praw,"<?xml",strlen("<?xml"))==0
           || memcmp(praw,"<Dataset",strlen("<Dataset"))==0) {
	    size_t len = 0;
	    /* setup as dmr only */
            /* Avoid strdup since rawdata might contain nul chars */
	    len = resp->raw.size;
            if((resp->serial.dmr = malloc(len+1)) == NULL)
                return THROW(NC_ENOMEM);    
            memcpy(resp->serial.dmr,praw,len);
            resp->serial.dmr[len] = '\0';
            /* Suppress nuls */
            (void)NCD4_elidenuls(resp->serial.dmr,len);
            return THROW(NC_NOERR); 
	}
    } else if(resp->mode != NCD4_DAP)
    	return THROW(NC_EDAP);

    /* We must be processing a DAP mode packet */
    praw = resp->raw.memory;

    /* If the raw data looks like xml, then we almost certainly have an error */
    if(memcmp(praw,"<?xml",strlen("<?xml"))==0
           || memcmp(praw,"<!doctype",strlen("<!doctype"))==0) {
	/* Set up to report the error */
	int stat = NCD4_seterrormessage(resp, resp->raw.size, resp->raw.memory);
        return THROW(stat); /* slight lie */
    }

    /* Get the first header to get dmr content and endian flags*/
    pdmr = NCD4_getheader(praw,&hdr,resp->controller->platform.hostlittleendian);
    if(hdr.count == 0)
        return THROW(NC_EDMR);
    if(hdr.flags & NCD4_ERR_CHUNK)
        return processerrchunk(resp, (void*)pdmr, hdr.count);
    resp->remotelittleendian = ((hdr.flags & NCD4_LITTLE_ENDIAN_CHUNK) ? 1 : 0);

    /* avoid strxxx operations on dmr */
    if((resp->serial.dmr = malloc(hdr.count+1)) == NULL)
        return THROW(NC_ENOMEM);        
    memcpy(resp->serial.dmr,pdmr,hdr.count);
    resp->serial.dmr[hdr.count-1] = '\0';
    /* Suppress nuls */
    (void)NCD4_elidenuls(resp->serial.dmr,hdr.count);

    /* See if there is any data after the DMR */
    if(hdr.flags & NCD4_LAST_CHUNK)
        return THROW(NC_ENODATA);

    /* Read and concat together the data chunks */
    phdr = pdmr + hdr.count; /* point to data chunk header */
    /* Do a sanity check in case the server has shorted us with no data */
    if((hdr.count + CHUNKHDRSIZE) >= resp->raw.size) {
        /* Server only sent the DMR part */
        resp->serial.dapsize = 0;
        return THROW(NC_EDATADDS);
    }
    /* walk all the data chunks */
    /* invariants:
	praw    -- beginning of the raw response
	pdmr    -- beginning of the dmr in the raw data
	pdap    -- beginning of the dechunked dap data
	phdr    -- pointer to the hdr of the current chunk
	pchunk  -- pointer to the data part of the current chunk
	pappend -- where to append next chunk to the growing dechunked data
    */
    for(firstchunk=1;;firstchunk=0) {	
        pchunk = NCD4_getheader(phdr,&hdr,resp->controller->platform.hostlittleendian); /* Process first data chunk header */
	if(firstchunk) {
	    pdap = phdr; /* remember start point of the dechunked data */
	    pappend = phdr; /* start appending here */
        }
        if(hdr.flags & NCD4_ERR_CHUNK)
            return processerrchunk(resp, (void*)pchunk, hdr.count);
        /* data chunk; possibly last; possibly empty */
        if(hdr.count > 0)
            memmove(pappend,pchunk,hdr.count); /* overwrite the header; this the heart of dechunking */
	pappend += hdr.count; /* next append point */
        phdr = pchunk + hdr.count; /* point to header of next chunk */
        if(hdr.flags & NCD4_LAST_CHUNK) break;
    }
    resp->serial.dap = pdap; /* start of dechunked data */
    resp->serial.dapsize = (size_t)DELTA(pappend,pdap);

#ifdef D4DUMPDMR
    fprintf(stderr,"%s\n",resp->serial.dmr);
    fflush(stderr);
#endif
#ifdef D4DUMPDAP
    NCD4_tagdump(resp->serial.dapsize,resp->serial.dap,0,"DAP");
#endif
    return THROW(NC_NOERR);    
}

static int
processerrchunk(NCD4response* resp, void* errchunk, unsigned int count)
{
    resp->serial.errdata = (char*)d4alloc(count+1);
    if(resp->serial.errdata == NULL)
        return THROW(NC_ENOMEM);
    memcpy(resp->serial.errdata,errchunk,count);
    resp->serial.errdata[count] = '\0';
    return THROW(NC_ENODATA); /* slight lie */
}

/**
Given a raw response, attempt to infer the mode: DMR, DAP, DSR.
Since DSR is not standardizes, it becomes the default.
*/
int
NCD4_infermode(NCD4response* resp)
{
    d4size_t size = resp->raw.size;
    char* raw = resp->raw.memory;

    if(size < 16)
        return THROW(NC_EDAP); /* must have at least this to hold a hdr + partial dmr*/ 
    if(memcmp(raw,"<?xml",strlen("<?xml"))==0
       || memcmp(raw,"<Dataset",strlen("<Dataset"))==0) {
        resp->mode = NCD4_DMR;
        goto done;
    }
    raw += 4; /* Pretend we have a DAP hdr */
    if(memcmp(raw,"<?xml",strlen("<?xml"))==0
       || memcmp(raw,"<Dataset",strlen("<Dataset"))==0) {
        resp->mode = NCD4_DAP;
        goto done;
    }
    /* Default to DSR */
    resp->mode = NCD4_DSR;

done:
    return NC_NOERR;
}
