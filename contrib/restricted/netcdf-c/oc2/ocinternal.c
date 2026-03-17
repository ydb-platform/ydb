/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#include "config.h"
#include <stddef.h>
#include <stdio.h>

#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#if _MSC_VER
#include <io.h>
#include <direct.h>
#include <process.h>
#endif

#ifdef HAVE_FTW_H
#include <ftw.h>
#endif

#include <errno.h>

#include "nc4internal.h"
#include "ocinternal.h"
#include "ocdebug.h"
#include "occurlfunctions.h"
#include "ochttp.h"
#include "ocread.h"
#include "dapparselex.h"
#include "ncpathmgr.h"
#include "ncutil.h"

#define DATADDSFILE "datadds"

#define CLBRACE '{'
#define CRBRACE '}'

#define OCBUFFERSIZE "HTTP.READ.BUFFERSIZE"
#define OCKEEPALIVE "HTTP.KEEPALIVE"

#ifdef HAVE_CURLOPT_BUFFERSIZE
#ifndef CURL_MAX_READ_SIZE
#define CURL_MAX_READ_SIZE  (512*1024)
#endif
#endif

/*Forward*/
static OCerror ocextractddsinmemory(OCstate*,OCtree*,int);
static OCerror ocextractddsinfile(OCstate*,OCtree*,int);
static OCerror createtempfile(OCstate*,OCtree*);
static int dataError(XXDR* xdrs, OCstate*);
static void ocremovefile(const char* path);

static OCerror ocset_curlproperties(OCstate*);
static OCerror ocget_rcproperties(OCstate*);

extern OCnode* makeunlimiteddimension(void);

int ocdebug = 0;

int ocinitialized = 0;

OCerror
ocinternalinitialize(void)
{
    int stat = OC_NOERR;

    if(ocinitialized) return OC_NOERR;
    ocinitialized = 1;

#if 0
    if(sizeof(off_t) != sizeof(void*)) {
      fprintf(stderr,"OC xxdr depends on the assumption that sizeof(off_t) == sizeof(void*)\n");
      /*
	Commenting out for now, as this does not hold true on 32-bit linux systems.
      OCASSERT(sizeof(off_t) == sizeof(void*));
	*/
    }
#endif

    /* Compute some xdr related flags */
    xxdr_init();

    if(getenv("OCDEBUG") != NULL)
        ocdebug = atoi(getenv("OCDEBUG"));

    return OCTHROW(stat);
}


/**************************************************/
OCerror
ocopen(OCstate** statep, const char* url)
{
    int stat = OC_NOERR;
    OCstate * state = NULL;
    NCURI* tmpurl = NULL;
    CURL* curl = NULL; /* curl handle*/

    if(!ocinitialized)
        ocinternalinitialize();

    if(ncuriparse(url,&tmpurl)) {
	OCTHROWCHK(stat=OC_EBADURL);
	goto fail;
    }

    stat = occurlopen(&curl);
    if(stat != OC_NOERR) {OCTHROWCHK(stat); goto fail;}

    state = (OCstate*)ocmalloc(sizeof(OCstate)); /* ocmalloc zeros memory*/
    if(state == NULL) {OCTHROWCHK(stat=OC_ENOMEM); goto fail;}

    /* Setup DAP state*/
    state->header.magic = OCMAGIC;
    state->header.occlass = OC_State;
    state->curl = curl;
    state->trees = nclistnew();
    state->uri = tmpurl;

    state->packet = ncbytesnew();
    ncbytessetalloc(state->packet,DFALTPACKETSIZE); /*initial reasonable size*/

    /* Initialize auth info from rc file */
    stat = NC_authsetup(&state->auth, state->uri);

    /* Initialize misc info from rc file */
    stat = ocget_rcproperties(state);

    /* Apply curl properties for this link;
       assumes state has been initialized */
    stat = ocset_curlproperties(state);
    if(stat != OC_NOERR) goto fail;

    /* Set the one-time curl flags */
    if((stat=ocset_flags_perlink(state))!= OC_NOERR) goto fail;
#if 1 /* temporarily make per-link */
    if((stat=ocset_flags_perfetch(state))!= OC_NOERR) goto fail;
#endif

    oc_curl_protocols(state);
    if(statep) *statep = state;
    else {
      if(state != NULL) ocfree(state);
    }
    return OCTHROW(stat);

fail:
    ncurifree(tmpurl);
    if(state != NULL) ocfree(state);
    if(curl != NULL) occurlclose(curl);
    return OCTHROW(stat);
}

OCerror
ocfetch(OCstate* state, const char* constraint, OCdxd kind, OCflags flags,
        OCnode** rootp)
{
    OCtree* tree = NULL;
    OCnode* root = NULL;
    OCerror stat = OC_NOERR;

    tree = (OCtree*)ocmalloc(sizeof(OCtree));
    MEMCHECK(tree,OC_ENOMEM);
    memset((void*)tree,0,sizeof(OCtree));
    tree->dxdclass = kind;
    tree->state = state;
    tree->constraint = nulldup(constraint);

    /* Set per-fetch curl properties */
#if 0 /* temporarily make per-link */
    if((stat=ocset_flags_perfetch(state))!= OC_NOERR) goto fail;
#endif

    ncbytesclear(state->packet);

    switch (kind) {
    case OCDAS:
        stat = readDAS(state,tree,flags);
	if(stat == OC_NOERR) {
            tree->text = ncbytesdup(state->packet);
	    if(tree->text == NULL) stat = OC_EDAS;
	}
	break;
    case OCDDS:
        stat = readDDS(state,tree,flags);
	if(stat == OC_NOERR) {
            tree->text = ncbytesdup(state->packet);
	    if(tree->text == NULL) stat = OC_EDDS;
	}
	break;
    case OCDATADDS:
	if((flags & OCONDISK) != 0) {/* store in file */
	    /* Create the datadds file immediately
               so that DRNO can reference it*/
            /* Make the tmp file*/
            stat = createtempfile(state,tree);
            if(stat) {OCTHROWCHK(stat); goto fail;}
            stat = readDATADDS(state,tree,flags);
	    if(stat == OC_NOERR) {
                /* Separate the DDS from data and return the dds;
                   will modify packet */
                stat = ocextractddsinfile(state,tree,flags);
	    }
	} else { /*inmemory*/
            stat = readDATADDS(state,tree,flags);
	    if(stat == OC_NOERR) {
                /* Separate the DDS from data and return the dds;
               will modify packet */
            stat = ocextractddsinmemory(state,tree,flags);
	}
	}
	break;
    default:
	break;
    }/*switch*/
    /* Obtain any http code */
    state->error.httpcode = ocfetchhttpcode(state->curl);
    if(stat != OC_NOERR) {
	if(state->error.httpcode >= 400) {
	    nclog(NCLOGWARN,"oc_open: Could not read url (%s); http error = %l",
		  ncuribuild(state->uri,NULL,NULL,NCURIALL),state->error.httpcode);
	} else {
	    nclog(NCLOGWARN,"oc_open: Could not read url");
	}
	goto fail;
    }

    tree->nodes = NULL;
    stat = DAPparse(state,tree,tree->text);
    /* Check and report on an error return from the server */
    if(stat == OC_EDAPSVC  && state->error.code != NULL) {
	fprintf(stderr,"oc_open: server error retrieving url: code=%s message=\"%s\"",
		  state->error.code,
		  (state->error.message?state->error.message:""));
    }
    if(stat) {OCTHROWCHK(stat); goto fail;}
    root = tree->root;
    /* make sure */
    tree->root = root;
    root->tree = tree;

    /* Verify the parse */
    switch (kind) {
    case OCDAS:
        if(root->octype != OC_Attributeset)
	    {OCTHROWCHK(stat=OC_EDAS); goto fail;}
	break;
    case OCDDS:
        if(root->octype != OC_Dataset)
	    {OCTHROWCHK(stat=OC_EDDS); goto fail;}
	break;
    case OCDATADDS:
        if(root->octype != OC_Dataset)
	    {OCTHROWCHK(stat=OC_EDATADDS); goto fail;}
	/* Modify the tree kind */
	tree->dxdclass = OCDATADDS;
	break;
    default: return OC_EINVAL;
    }

    if(kind != OCDAS) {
        /* Process ocnodes to mark those that are cacheable */
        ocmarkcacheable(state,root);
        /* Process ocnodes to handle various semantic issues*/
        occomputesemantics(tree->nodes);
    }

    /* Process ocnodes to compute name info*/
    occomputefullnames(tree->root);

     if(kind == OCDATADDS) {
	if((flags & OCONDISK) != 0) {
            tree->data.xdrs = xxdr_filecreate(tree->data.file,tree->data.bod);
	} else {
#ifdef OCDEBUG
fprintf(stderr,"ocfetch.datadds.memory: datasize=%lu bod=%lu\n",
	(unsigned long)tree->data.datasize,(unsigned long)tree->data.bod);
#endif
	    /* Switch to zero based memory */
            tree->data.xdrs
		= xxdr_memcreate(tree->data.memory,tree->data.datasize,tree->data.bod);
	}
        MEMCHECK(tree->data.xdrs,OC_ENOMEM);
	/* Do a quick check to see if server returned an ERROR {}
           at the beginning of the data
         */
	if(dataError(tree->data.xdrs,state)) {
	    stat = OC_EDATADDS;
	    fprintf(stderr,"oc_open: server error retrieving url: code=%s message=\"%s\"",
		  state->error.code,
		  (state->error.message?state->error.message:""));
	    goto fail;
	}

	/* Compile the data into a more accessible format */
	stat = occompile(state,tree->root);
	if(stat != OC_NOERR)
	    goto fail;
    }

    /* Put root into the state->trees list */
    nclistpush(state->trees,(void*)root);

    if(rootp) *rootp = root;
    return stat;

fail:
    if(root != NULL)
	ocroot_free(root);
    else if(tree != NULL)
	octree_free(tree);
    return OCTHROW(stat);
}

static OCerror
createtempfile(OCstate* state, OCtree* tree)
{
    int stat = OC_NOERR;
    char basepath[8192];
    char* tmppath = NULL;
    size_t len;
    NCglobalstate* globalstate = NC_getglobalstate();

    snprintf(basepath,sizeof(basepath),"%s/%s",globalstate->tempdir,DATADDSFILE);
    tmppath = NULL;
    stat = NC_mktmp(basepath,&tmppath);
    if(stat || tmppath == NULL) {
        fprintf(stderr, "Cannot create %sfile\n",DATADDSFILE);
	stat = OC_EACCESS;
        goto fail;
    }

#ifdef OCDEBUG
    nclog(NCLOGNOTE,"oc_open: creating tmp file: %s",tmppath);
#endif
    tree->data.filename = tmppath; /* remember our tmp file name */
    tmppath = NULL;
    tree->data.file = NCfopen(tree->data.filename,"w+");
    if(tree->data.file == NULL) return OC_EOPEN;
    /* make the temp file so it will automatically be reclaimed on close */
    if(ocdebug == 0)
	ocremovefile(tree->data.filename);
    return stat;

fail:
    if(tmppath != NULL) {
        nclog(NCLOGERR,"oc_open: attempt to create tmp file failed: %s",tmppath);
	free(tmppath);
    } else {
        nclog(NCLOGERR,"oc_open: attempt to create tmp file failed: NULL");
    }
    return OCTHROW(stat);
}

void
occlose(OCstate* state)
{
    unsigned int i;
    if(state == NULL) return;

    /* Warning: ocfreeroot will attempt to remove the root from state->trees */
    /* Ok in this case because we are popping the root out of state->trees */
    for(i=0;i<nclistlength(state->trees);i++) {
	OCnode* root = (OCnode*)nclistpop(state->trees);
	ocroot_free(root);
    }
    nclistfree(state->trees);
    ncurifree(state->uri);
    ncbytesfree(state->packet);
    ocfree(state->error.code);
    ocfree(state->error.message);
    if(state->curl != NULL) occurlclose(state->curl);
    NC_authfree(state->auth);
    state->auth = NULL;
    ocfree(state);
}

static OCerror
ocextractddsinmemory(OCstate* state, OCtree* tree, OCflags flags)
{
    OCerror stat = OC_NOERR;
    size_t ddslen, bod;
    /* Read until we find the separator (or EOF)*/
    int bodfound = ocfindbod(state->packet,&bod,&ddslen);
    if(bodfound) {
        tree->data.bod = (off_t)bod;
        tree->data.ddslen = (off_t)ddslen;
    }
    /* copy out the dds */
    if(ddslen > 0) {
        tree->text = (char*)ocmalloc(ddslen+1);
        memcpy((void*)tree->text,(void*)ncbytescontents(state->packet),ddslen);
        tree->text[ddslen] = '\0';
    } else
	tree->text = NULL;
    /* Extract the inmemory contents */
    tree->data.memory = ncbytesextract(state->packet);
#ifdef OCIGNORE
    /* guarantee the data part is on an 8 byte boundary */
    if(tree->data.bod % 8 != 0) {
        unsigned long count = tree->data.datasize - tree->data.bod;
        memcpy(tree->xdrmemory,tree->xdrmemory+tree->data.bod,count);
        tree->data.datasize = count;
	tree->data.bod = 0;
	tree->data.ddslen = 0;
    }
#endif
    if(tree->text == NULL) stat = OC_EDATADDS;
    return OCTHROW(stat);
}

static OCerror
ocextractddsinfile(OCstate* state, OCtree* tree, OCflags flags)
{
    OCerror stat = OC_NOERR;

    size_t ddslen, bod, bodfound;
    int retVal;

    /* Read until we find the separator (or EOF)*/
    ncbytesclear(state->packet);
    retVal = fseek(tree->data.file, 0L, SEEK_SET);  
    if (retVal != 0) {
    	stat = OC_EDATADDS;
        return OCTHROW(stat);
    }

    bodfound = 0;
    do {
        char chunk[1024];
	size_t count;
	/* read chunks of the file until we find the separator*/
        count = fread(chunk,1,sizeof(chunk),tree->data.file);
	if(count <= 0) break; /* EOF;*/
        ncbytesappendn(state->packet,chunk,count);
	ncbytesnull(state->packet);
	bodfound = ocfindbod(state->packet,&bod,&ddslen);
    } while(!bodfound);
    if(bodfound) {
        tree->data.bod = (off_t)bod;
        tree->data.ddslen = (off_t)ddslen;
    }
    /* copy out the dds */
    if(ddslen > 0) {
        tree->text = (char*)ocmalloc(ddslen+1);
        memcpy((void*)tree->text,(void*)ncbytescontents(state->packet),ddslen);
        tree->text[ddslen] = '\0';
    } else
	tree->text = NULL;
    /* reset the position of the tmp file*/
    if(fseek(tree->data.file,(long)tree->data.bod,SEEK_SET) < 0
       || tree->text == NULL)
	stat = OC_EDATADDS;
    return OCTHROW(stat);
}

OCerror
ocupdatelastmodifieddata(OCstate* state, OCflags ocflags)
{
    OCerror status = OC_NOERR;
    long lastmodified;
    char* base = NULL;
    int flags = 0;
    if(ocflags & OCENCODEPATH) flags |= NCURIENCODEPATH;
    if(ocflags & OCENCODEQUERY) flags |= NCURIENCODEQUERY;
    base = ncuribuild(state->uri,NULL,NULL,flags);
    status = ocfetchlastmodified(state->curl, base, &lastmodified);
    free(base);
    if(status == OC_NOERR) {
	state->datalastmodified = lastmodified;
    }
    return OCTHROW(status);
}

/*
    Extract state values from .rc file
*/
static OCerror
ocget_rcproperties(OCstate* state)
{
    OCerror ocerr = OC_NOERR;
    char* option = NULL;
#ifdef HAVE_CURLOPT_BUFFERSIZE
    option = NC_rclookup(OCBUFFERSIZE,state->uri->uri,NULL);
    if(option != NULL && strlen(option) != 0) {
	long bufsize;
	if(strcasecmp(option,"max")==0) 
	    bufsize = CURL_MAX_READ_SIZE;
	else if(sscanf(option,"%ld",&bufsize) != 1 || bufsize <= 0)
            fprintf(stderr,"Illegal %s size\n",OCBUFFERSIZE);
	state->curlbuffersize = bufsize;
    }
#endif
#ifdef HAVE_CURLOPT_KEEPALIVE
    option = NC_rclookup(OCKEEPALIVE,state->uri->uri,NULL);
    if(option != NULL && strlen(option) != 0) {
	/* The keepalive value is of the form 0 or n/m,
           where n is the idle time and m is the interval time;
           setting either to zero will prevent that field being set. */
	if(strcasecmp(option,"on")==0) {
	    state->curlkeepalive.active = 1;
	} else {
	    unsigned long idle=0;
	    unsigned long interval=0;
	    if(sscanf(option,"%lu/%lu",&idle,&interval) != 2)
	        fprintf(stderr,"Illegal KEEPALIVE VALUE: %s\n",option);
	    state->curlkeepalive.idle = (long)idle;
	    state->curlkeepalive.interval = (long)interval;
	    state->curlkeepalive.active = 1;
	}
    }
#endif
    return ocerr;
}


/*
    Set curl properties for link based on fields in the state.
*/
static OCerror
ocset_curlproperties(OCstate* state)
{
    OCerror stat = OC_NOERR;
    NCglobalstate* globalstate = NC_getglobalstate();

    if(state->auth->curlflags.useragent == NULL) {
        size_t len = strlen(DFALTUSERAGENT) + strlen(VERSION) + 1;
	char* agent = (char*)malloc(len);
	strncpy(agent,DFALTUSERAGENT,len);
	strlcat(agent,VERSION,len);
        state->auth->curlflags.useragent = agent;
	agent = NULL;
    }

    /* Some servers (e.g. thredds and columbia) appear to require a place
       to put cookies in order for some security functions to work
    */
    if(state->auth->curlflags.cookiejar != NULL
       && strlen(state->auth->curlflags.cookiejar) == 0) {
	free(state->auth->curlflags.cookiejar);
	state->auth->curlflags.cookiejar = NULL;
    }

    if (state->auth->curlflags.cookiejar == NULL) {
        /* If no cookie file was defined, define a default */
        int stat = NC_NOERR;
        char basepath[8192];
        char* tmppath = NULL;
        errno = 0;
	NCglobalstate* globalstate = NC_getglobalstate();

        /* Create the unique cookie file name */
	snprintf(basepath,sizeof(basepath),"%s/occookies",globalstate->tempdir);
	tmppath = NULL;
	stat = NC_mktmp(basepath,&tmppath);
	if(stat || tmppath == NULL) {
            fprintf(stderr, "Cannot create cookie file\n");
	    stat = OC_EACCESS;
            goto fail;
        }
        state->auth->curlflags.cookiejar = tmppath; tmppath = NULL;
        state->auth->curlflags.cookiejarcreated = 1;
        errno = 0;
    }
    OCASSERT(state->auth->curlflags.cookiejar != NULL);

    /* Make sure the cookie jar exists and can be read and written */
    {
	FILE* f = NULL;
	const char* fname = state->auth->curlflags.cookiejar;
	/* See if the file exists already */
        f = NCfopen(fname,"r");
	if(f == NULL) {
	    /* Ok, create it */
	    f = NCfopen(fname,"w+");
	    if(f == NULL) {
	        fprintf(stderr,"Cookie file cannot be read and written: %s\n",fname);
	        {stat = OC_EPERM; goto fail;}
	    }
	} else { /* test if file can be written */
	    fclose(f);
	    f = NCfopen(fname,"r+");
	    if(f == NULL) {
	        fprintf(stderr,"Cookie file is cannot be written: %s\n",fname);
	        {stat = OC_EPERM; goto fail;}
	    }
	}
	if(f != NULL) fclose(f);
    }

#if 0
    /* Create a netrc file if specified  and required,
       where required => >1 NETRC triples exist */
    if(ocrc_netrc_required(state)) {
	/* WARNING: it appears that a user+pwd was specified specifically, then
           the netrc file will be completely disabled. */
	if(state->auth->creds.userpwd != NULL) {
  	    nclog(NCLOGWARN,"The rc file specifies both netrc and user+pwd; this will cause curl to ignore the netrc file");
	}
	stat = oc_build_netrc(state);
    }
#endif

    return stat;

fail:
    return OCTHROW(stat);
}

static const char* ERROR_TAG = "Error ";

static int
dataError(XXDR* xdrs, OCstate* state)
{
    int depth=0;
    int errfound = 0;
    off_t ckp=0,avail=0;
    int i=0;
    char* errmsg = NULL;
    char errortext[16]; /* bigger than |ERROR_TAG|*/
    avail = xxdr_getavail(xdrs);
    if(avail < strlen(ERROR_TAG))
	goto done; /* assume it is ok */
    ckp = xxdr_getpos(xdrs);
    /* Read enough characters to test for 'ERROR ' */
    errortext[0] = '\0';
    xxdr_getbytes(xdrs,errortext,(off_t)strlen(ERROR_TAG));
    if(ocstrncmp(errortext,ERROR_TAG,strlen(ERROR_TAG)) != 0)
	goto done; /* not an immediate error */
    /* Try to locate the whole error body */
    xxdr_setpos(xdrs,ckp);
    for(depth=0,i=0;i<avail;i++) {
	xxdr_getbytes(xdrs,errortext,(off_t)1);
	if(errortext[0] == CLBRACE) depth++;
	else if(errortext[0] == CRBRACE) {
	    depth--;
	    if(depth == 0) {i++; break;}
	}
    }
    errmsg = (char*)malloc((size_t)i+1);
    if(errmsg == NULL) {errfound = 1; goto done;}
    xxdr_setpos(xdrs,ckp);
    xxdr_getbytes(xdrs,errmsg,(off_t)i);
    errmsg[i] = '\0';
    state->error.message = errmsg;
    state->error.code = strdup("?");
    state->error.httpcode = 404;
    xxdr_setpos(xdrs,ckp);
    errfound = 1;
done:
    xxdr_setpos(xdrs,ckp);
    return errfound;
}

/**************************************************/
/* Curl option functions */
/*
Note that if we set the option in curlflags,
then we need to also invoke the ocset_curlopt
to update the curl flags in libcurl.
*/

OCerror
ocset_useragent(OCstate* state, const char* agent)
{
    OCerror stat = OC_NOERR;
    if(state->auth->curlflags.useragent != NULL)
	free(state->auth->curlflags.useragent);
    state->auth->curlflags.useragent = strdup(agent);
    if(state->auth->curlflags.useragent == NULL)
	return OCTHROW(OC_ENOMEM);
    stat = ocset_curlflag(state,CURLOPT_USERAGENT);
    return stat;
}

OCerror
ocset_netrc(OCstate* state, const char* path)
{
    OCerror stat = OC_NOERR;
    if(state->auth->curlflags.netrc != NULL)
	free(state->auth->curlflags.netrc);
    state->auth->curlflags.netrc = strdup(path);
    if(state->auth->curlflags.netrc == NULL)
	return OCTHROW(OC_ENOMEM);
    stat = ocset_curlflag(state,CURLOPT_NETRC);
    return stat;
}

static void
ocremovefile(const char* path)
{
#ifdef _MSC_VER
    DeleteFile(path);
#else
    remove(path);
#endif
}

