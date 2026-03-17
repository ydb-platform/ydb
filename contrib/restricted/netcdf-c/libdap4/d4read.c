/*! \file

Copyright 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002,
2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014,
2015, 2016, 2017, 2018
University Corporation for Atmospheric Research/Unidata.

See \ref copyright file for more info.

*/

#include "d4includes.h"
#include "d4curlfunctions.h"
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#include "ncpathmgr.h"

/* Do conversion if this code was compiled via Vis. Studio or Mingw */

/*Forward*/
static int readpacket(NCD4INFO* state, NCURI*, NCbytes*, NCD4mode, NCD4format, int*, long*);
static int readfile(NCD4INFO* state, const NCURI* uri, NCD4mode dxx, NCD4format fxx, NCbytes* packet);
static int readfileDAPDMR(NCD4INFO* state, const NCURI* uri, NCbytes* packet);

#ifdef HAVE_GETTIMEOFDAY
static double
deltatime(struct timeval time0,struct timeval time1)
{
    double t0, t1;
    t0 = ((double)time0.tv_sec);
    t0 += ((double)time0.tv_usec) / 1000000.0;
    t1 = ((double)time1.tv_sec);
    t1 += ((double)time1.tv_usec) / 1000000.0;
    return (t1 - t0);
}
#endif

int
NCD4_readDMR(NCD4INFO* state, NCURI* url, NCD4response* resp)
{
    int stat = NC_NOERR;
    ncbytesclear(state->curl->packet);
    stat = readpacket(state,url,state->curl->packet,NCD4_DMR,NCD4_FORMAT_XML,&resp->serial.httpcode,NULL);
    return THROW(stat);
}

int
NCD4_readDAP(NCD4INFO* state, NCURI* url, NCD4response* resp)
{
    int stat = NC_NOERR;
    
    ncbytesclear(state->curl->packet);
    stat = readpacket(state,url,state->curl->packet,NCD4_DAP,NCD4_FORMAT_NONE,&resp->serial.httpcode,NULL);
    if(stat) {
	NCD4_seterrormessage(resp, nclistlength(state->curl->packet), nclistcontents(state->curl->packet));
    } else {
        resp->raw.size = ncbyteslength(state->curl->packet);
        resp->raw.memory = ncbytesextract(state->curl->packet);
    }
    return THROW(stat);
}

static const char*
dxxextension(int dxx)
{
    switch(dxx) {
    case NCD4_DMR: return ".dmr";
    case NCD4_DAP: return ".dap";
    default: break;
    }
    return NULL;
}

static const char*
dxxformat(int fxx, int dxx)
{
    switch(dxx) {
    case NCD4_DMR: 
	switch(fxx) {
	case NCD4_FORMAT_XML:return ".xml";
        default: break;
        }
        break;
    case NCD4_DAP:
	switch(fxx) {
	case NCD4_FORMAT_NONE:return "";
        default: break;
        }
        break;
    default: break;
    }
    return NULL;
}

static int
readpacket(NCD4INFO* state, NCURI* url, NCbytes* packet, NCD4mode dxx, NCD4format fxx, int* httpcodep, long* lastmodified)
{
    int stat = NC_NOERR;
    int fileprotocol = 0;
    CURL* curl = state->curl->curl;
#ifdef HAVE_GETTIMEOFDAY
    struct timeval time0;
    struct timeval time1;
#endif
    char suffix[256];

    suffix[0] = '\0';
    strlcat(suffix,dxxextension(dxx),sizeof(suffix));
    strlcat(suffix,dxxformat(fxx,dxx),sizeof(suffix));

    fileprotocol = (strcmp(url->protocol,"file")==0);

    if(fileprotocol) {
	/* Short circuit file://... urls*/
	/* We do this because the test code always needs to read files*/
	stat = readfile(state, url, dxx, fxx, packet);
    } else {
        char* fetchurl = NULL;
	int flags = NCURIBASE;

	if(!fileprotocol) flags |= NCURIQUERY;
	flags |= NCURIENCODE;
        fetchurl = ncuribuild(url,NULL,suffix,flags);
	MEMCHECK(fetchurl);
	if(FLAGSET(state->controls.flags,NCF_SHOWFETCH)) {
	    nclog(NCLOGDEBUG,"fetch url=%s",fetchurl);
#ifdef HAVE_GETTIMEOFDAY
   	    gettimeofday(&time0,NULL);
#endif
	}
        stat = NCD4_fetchurl(curl,fetchurl,packet,lastmodified,httpcodep);
        nullfree(fetchurl);
	if(stat) goto fail;
	if(FLAGSET(state->controls.flags,NCF_SHOWFETCH)) {
            double secs = 0;
#ifdef HAVE_GETTIMEOFDAY
   	    gettimeofday(&time1,NULL);
	    secs = deltatime(time0,time1);
#endif
            nclog(NCLOGDEBUG,"fetch complete: %0.3f",secs);
	}
    }
#ifdef D4DEBUG
  {
fprintf(stderr,"readpacket: packet.size=%lu\n",
		(unsigned long)ncbyteslength(packet));
  }
#endif
fail:
    return THROW(stat);
}

#if 0
static int
readfromfile(NCD4INFO* state, const NCURI* uri, NCD4mode dxx, NCD4format fxx, d4size_t* sizep)
{
    int stat = NC_NOERR;
    size_t len;

    ncbytesclear(state->curl->packet);
    stat = readfile(state, uri, dxx, fxx, state->curl->packet);
#ifdef D4DEBUG
fprintf(stderr,"readfiletofile: packet.size=%lu\n",
		(unsigned long)ncbyteslength(state->curl->packet));
#endif
    if(stat != NC_NOERR) goto unwind;
    len = nclistlength(state->curl->packet);
    if(sizep != NULL) *sizep = len;
unwind:
    return THROW(stat);
}
#endif

static int
readfile(NCD4INFO* state, const NCURI* uri, NCD4mode dxx, NCD4format fxx, NCbytes* packet)
{
    int stat = NC_NOERR;
    NCbytes* tmp = ncbytesnew();
    char* filename = NULL;
    char suffix[256];
#ifdef HAVE_GETTIMEOFDAY
    struct timeval time0;
    struct timeval time1;
#endif

    suffix[0] = '\0';
    strlcat(suffix,dxxextension(dxx),sizeof(suffix));
#if 0
     not needed strlcat(suffix,dxxformat(fxx,dxx),sizeof(suffix));
#endif
    ncbytescat(tmp,uri->path);
    ncbytescat(tmp,suffix);
    ncbytesnull(tmp);
    filename = ncbytesextract(tmp);
    ncbytesfree(tmp);

    nullfree(state->fileproto.filename);
    state->fileproto.filename = filename; /* filename is alloc'd here anyway */

    if(FLAGSET(state->controls.flags,NCF_SHOWFETCH)) {
	char* surl = NULL;
#ifdef HAVE_GETTIMEOFDAY
	gettimeofday(&time0,NULL);
#endif
        surl = ncuribuild((NCURI*)uri,NULL,NULL,NCURIALL);
	nclog(NCLOGDEBUG,"fetch uri=%s file=%s",surl,filename);
    }
    switch (dxx) {
    case NCD4_DMR:
        if((stat = NC_readfile(filename,packet))) {
  	    /* See if we can get the same thing from the .dap file */
	    stat = readfileDAPDMR(state,uri,packet);
        }
	break;
    case NCD4_DAP:
    case NCD4_DSR:
        stat = NC_readfile(filename,packet);
	break;
    default: stat = NC_EDAP; break;
    }
    
    if(FLAGSET(state->controls.flags,NCF_SHOWFETCH)) {
	double secs;
#ifdef HAVE_GETTIMEOFDAY
   	gettimeofday(&time1,NULL);
	secs = deltatime(time0,time1);
#endif
        nclog(NCLOGDEBUG,"%s fetch complete: %0.3f",suffix,secs);
    }
    return THROW(stat);
}

/* Extract the DMR from a DAP file */
static int
readfileDAPDMR(NCD4INFO* state, const NCURI* uri, NCbytes* packet)
{
    int stat = NC_NOERR;
    NCbytes* tmp = ncbytesnew();
    char* filename = NULL;
    NCD4HDR hdr;
#ifdef HAVE_GETTIMEOFDAY
    struct timeval time0;
    struct timeval time1;
#endif

    ncbytescat(tmp,uri->path);
    ncbytescat(tmp,".dap");
    ncbytesnull(tmp);
    filename = ncbytesextract(tmp);
    ncbytesfree(tmp);

    nullfree(state->fileproto.filename);
    state->fileproto.filename = filename; /* filename is alloc'd here anyway */

    if(FLAGSET(state->controls.flags,NCF_SHOWFETCH)) {
	char* surl = NULL;
#ifdef HAVE_GETTIMEOFDAY
	gettimeofday(&time0,NULL);
#endif
        surl = ncuribuild((NCURI*)uri,NULL,".dap",NCURIALL);
	nclog(NCLOGDEBUG,"fetch uri=%s file=%s",surl,filename);
    }
    stat = NC_readfile(filename,packet);

    if(FLAGSET(state->controls.flags,NCF_SHOWFETCH)) {
	double secs;
#ifdef HAVE_GETTIMEOFDAY
   	gettimeofday(&time1,NULL);
	secs = deltatime(time0,time1);
#endif
        nclog(NCLOGDEBUG,"fetch complete: %0.3f",secs);
    }

    if(stat != NC_NOERR) goto done;
    /* Extract the DMR from the dap */
    NCD4_getheader(ncbytescontents(packet),&hdr,NCD4_isLittleEndian());
    if(hdr.count == 0 || (hdr.flags & NCD4_ERR_CHUNK))
        return THROW(NC_EDMR);
    /* patch up the packet */
    {
	int i;
	size_t newlen;
	for(i=0;i<4;i++)
	    ncbytesremove(packet,0); /* remove the hdr */
	/* null terminate */
	ncbytessetlength(packet,hdr.count-1);
	ncbytesnull(packet);
        /* Suppress nuls */
        newlen = NCD4_elidenuls(ncbytescontents(packet),ncbyteslength(packet));
	/* reset packet length */
	ncbytessetlength(packet,newlen);
    }

done:
    return THROW(stat);
}

/* Extract packet as error message; assume httpcode set */
int
NCD4_seterrormessage(NCD4response* resp, size_t len, char* msg)
{
    resp->error.message = (char*)d4alloc(len+1);
    if(resp->error.message == NULL)
        return THROW(NC_ENOMEM);
    memcpy(resp->error.message,msg,len);
    resp->error.message[len] = '\0';
    return THROW(NC_ENODATA); /* slight lie */
}
