/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#include "config.h"
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif
#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif
#ifdef _MSC_VER
#include <io.h>
#ifndef O_BINARY
#define O_BINARY _O_BINARY
#endif
#endif
#include "ncrc.h"
#include "ncutil.h"
#include "ocinternal.h"
#include "ocdebug.h"
#include "ochttp.h"
#include "ocread.h"
#include "occurlfunctions.h"
#include "ncpathmgr.h"
#include "ncutil.h"

/*Forward*/
static int readpacket(OCstate* state, NCURI*, NCbytes*, OCdxd, OCflags, long*);
static int readfile(const char* path, const char* suffix, NCbytes* packet);
static int readfiletofile(const char* path, const char* suffix, FILE* stream, off_t*);

int
readDDS(OCstate* state, OCtree* tree, OCflags flags)
{
    int stat = OC_NOERR;
    long lastmodified = -1;

    ncurisetquery(state->uri,tree->constraint);

#ifdef OCDEBUG
fprintf(stderr,"readDDS:\n");
#endif
    stat = readpacket(state,state->uri,state->packet,OCDDS, flags,
			&lastmodified);
    if(stat == OC_NOERR) state->ddslastmodified = lastmodified;

    return stat;
}

int
readDAS(OCstate* state, OCtree* tree, OCflags flags)
{
    int stat = OC_NOERR;

    ncurisetquery(state->uri,tree->constraint);
#ifdef OCDEBUG
fprintf(stderr,"readDAS:\n");
#endif
    stat = readpacket(state,state->uri,state->packet,OCDAS,flags,NULL);

    return stat;
}

#if 0
int
readversion(OCstate* state, NCURI* url, NCbytes* packet)
{
   return readpacket(state,url,packet,OCVER,NULL);
}
#endif

const char*
ocdxdextension(OCdxd dxd)
{
    switch(dxd) {
    case OCDDS: return ".dds";
    case OCDAS: return ".das";
    case OCDATADDS: return ".dods";
    default: break;
    }
    return NULL;
}

static int
readpacket(OCstate* state, NCURI* url, NCbytes* packet, OCdxd dxd, OCflags ocflags, long* lastmodified)
{
   int stat = OC_NOERR;
   int fileprotocol = 0;
   const char* suffix = ocdxdextension(dxd);
   char* fetchurl = NULL;
   CURL* curl = state->curl;

   fileprotocol = (strcmp(url->protocol,"file")==0);

   if(fileprotocol) {
        /* Short circuit file://... urls and read directly */
	fetchurl = ncuribuild(url,NULL,NULL,NCURIBASE);
	stat = readfile(fetchurl,suffix,packet);
    } else {
	int flags = NCURIBASE;
	if(ocflags & OCENCODEPATH)flags |= NCURIENCODEPATH;
	if(ocflags & OCENCODEQUERY) flags |= NCURIENCODEQUERY;
	if(!fileprotocol) flags |= NCURIQUERY;
        fetchurl = ncuribuild(url,NULL,suffix,flags);
	MEMCHECK(fetchurl,OC_ENOMEM);
	if(ocdebug > 0)
            {fprintf(stderr,"fetch url=%s\n",fetchurl); fflush(stderr);}
        stat = ocfetchurl(curl,fetchurl,packet,lastmodified);
	if(stat)
	    oc_curl_printerror(state);
	if(ocdebug > 0)
            {fprintf(stderr,"fetch complete\n"); fflush(stderr);}
    }
    free(fetchurl);
#ifdef OCDEBUG
  {
fprintf(stderr,"readpacket: packet.size=%lu\n",
		(unsigned long)ncbyteslength(packet));
  }
#endif
    return OCTHROW(stat);
}

int
readDATADDS(OCstate* state, OCtree* tree, OCflags ocflags)
{
    int stat = OC_NOERR;
    long lastmod = -1;

#ifdef OCDEBUG
fprintf(stderr,"readDATADDS:\n");
#endif
    if((ocflags & OCONDISK) == 0) {
        ncurisetquery(state->uri,tree->constraint);
        stat = readpacket(state,state->uri,state->packet,OCDATADDS,ocflags,&lastmod);
        if(stat == OC_NOERR)
            state->datalastmodified = lastmod;
        tree->data.datasize = (off_t)ncbyteslength(state->packet);
    } else { /*((flags & OCONDISK) != 0) */
        NCURI* url = state->uri;
        int fileprotocol = 0;
        char* readurl = NULL;

        fileprotocol = (strcmp(url->protocol,"file")==0);

        if(fileprotocol) {
            readurl = ncuribuild(url,NULL,NULL,NCURIBASE);
            stat = readfiletofile(readurl, ".dods", tree->data.file, &tree->data.datasize);
        } else {
            int flags = NCURIBASE;
	    if(ocflags & OCENCODEPATH)
	        flags |= NCURIENCODEPATH;
	    if(ocflags & OCENCODEQUERY)
	        flags |= NCURIENCODEQUERY;
            if(!fileprotocol) flags |= NCURIQUERY;
            ncurisetquery(url,tree->constraint);
            readurl = ncuribuild(url,NULL,".dods",flags);
            MEMCHECK(readurl,OC_ENOMEM);
            if (ocdebug > 0) 
                {fprintf(stderr, "fetch url=%s\n", readurl);fflush(stderr);}
            stat = ocfetchurl_file(state->curl, readurl, tree->data.file,
                                   &tree->data.datasize, &lastmod);
            if(stat == OC_NOERR)
                state->datalastmodified = lastmod;
            if (ocdebug > 0) 
                {fprintf(stderr,"fetch complete\n"); fflush(stderr);}
        }
        free(readurl);
    }
    return OCTHROW(stat);
}

static int
readfiletofile(const char* path, const char* suffix, FILE* stream, off_t* sizep)
{
    int stat = OC_NOERR;
    NCbytes* packet = ncbytesnew();
    size_t len;
    /* check for leading file:/// */
    if(ocstrncmp(path,"file:///",8)==0) path += 7; /* assume absolute path*/
    stat = readfile(path,suffix,packet);
#ifdef OCDEBUG
fprintf(stderr,"readfiletofile: packet.size=%lu\n",
		(unsigned long)ncbyteslength(packet));
#endif
    if(stat != OC_NOERR) goto unwind;
    len = nclistlength(packet);
    if(stat == OC_NOERR) {
	size_t written;
        fseek(stream,0,SEEK_SET);
	written = fwrite(ncbytescontents(packet),1,len,stream);
	if(written != len) {
#ifdef OCDEBUG
fprintf(stderr,"readfiletofile: written!=length: %lu :: %lu\n",
	(unsigned long)written,(unsigned long)len);
#endif
	    stat = OC_EIO;
	}
    }
    if(sizep != NULL) *sizep = (off_t)len;
unwind:
    ncbytesfree(packet);
    return OCTHROW(stat);
}

static int
readfile(const char* path, const char* suffix, NCbytes* packet)
{
    int stat = OC_NOERR;
    char filename[1024];
    /* check for leading file:/// */
    if(ocstrncmp(path,"file://",7)==0) path += 7; /* assume absolute path*/
    strncpy(filename,path,sizeof(filename));
    strlcat(filename,(suffix != NULL ? suffix : ""),sizeof(filename));
    stat = NC_readfile(filename,packet);
    return OCTHROW(stat);
}


