/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

#include "config.h"
#include <stddef.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_STDARG_H
#include <stdarg.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "netcdf.h"
#include "ncbytes.h"
#include "ncuri.h"
#include "ncrc.h"
#include "nclog.h"
#include "ncauth.h"
#include "ncpathmgr.h"
#include "nc4internal.h"
#include "ncs3sdk.h"
#include "ncdispatch.h"
#include "ncutil.h"

#undef NOREAD

#undef DRCDEBUG
#undef LEXDEBUG
#undef PARSEDEBUG

#define RTAG ']'
#define LTAG '['

#undef MEMCHECK
#define MEMCHECK(x) if((x)==NULL) {goto nomem;} else {}

/* Forward */
static int NC_rcload(void);
static char* rcreadline(char** nextlinep);
static void rctrim(char* text);
static void rcorder(NClist* rc);
static int rccompile(const char* path);
static int rcequal(NCRCentry* e1, NCRCentry* e2);
static int rclocatepos(const char* key, const char* hostport, const char* urlpath);
static struct NCRCentry* rclocate(const char* key, const char* hostport, const char* urlpath);
static int rcsearch(const char* prefix, const char* rcname, char** pathp);
static void rcfreeentries(NClist* rc);
static void rcfreeentry(NCRCentry* t);
#ifdef DRCDEBUG
static void storedump(char* msg, NClist* entries);
#endif

/* Define default rc files and aliases, also defines load order*/
static const char* rcfilenames[] = {".ncrc", ".daprc", ".dodsrc", NULL};

static int NCRCinitialized = 0;

/**************************************************/
/* User API */

/**
The most common case is to get the most general value for a key,
where most general means that the urlpath and hostport are null
So this function returns the value associated with the key
where the .rc entry has the simple form "key=value".
If that entry is not found, then return NULL.

@param key table entry key field
@param  table entry key field
@return value matching the key -- caller frees
@return NULL if no entry of the form key=value exists
*/
char*
nc_rc_get(const char* key)
{
    NCglobalstate* ncg = NULL;
    char* value = NULL;

    if(!NC_initialized) nc_initialize();

    ncg = NC_getglobalstate();
    assert(ncg != NULL && ncg->rcinfo != NULL && ncg->rcinfo->entries != NULL);
    if(ncg->rcinfo->ignore) goto done;
    value = NC_rclookup(key,NULL,NULL);
done:
    value = nulldup(value);   
    return value;
}

/**
Set simple key=value in .rc table.
Will overwrite any existing value.

@param key
@param value 
@return NC_NOERR if success
@return NC_EINVAL if fail
*/
int
nc_rc_set(const char* key, const char* value)
{
    int stat = NC_NOERR;
    NCglobalstate* ncg = NULL;

    if(!NC_initialized) nc_initialize();

    ncg = NC_getglobalstate();
    assert(ncg != NULL && ncg->rcinfo != NULL && ncg->rcinfo->entries != NULL);
    if(ncg->rcinfo->ignore) goto done;;
    stat = NC_rcfile_insert(key,NULL,NULL,value);
done:
    return stat;
}

/**************************************************/
/* External Entry Points */

/*
Initialize defaults and load:
* .ncrc
* .dodsrc
* ${HOME}/.aws/config
* ${HOME}/.aws/credentials

For debugging support, it is possible
to change where the code looks for the .aws directory.
This is set by the environment variable NC_TEST_AWS_DIR.

*/

void
ncrc_initialize(void)
{
    if(NCRCinitialized) return;
    NCRCinitialized = 1; /* prevent recursion */

#ifndef NOREAD
    {
    int stat = NC_NOERR;
    NCglobalstate* ncg = NC_getglobalstate();
    /* Load entries */
    if((stat = NC_rcload())) {
        nclog(NCLOGWARN,".rc loading failed");
    }
    /* Load .aws/config &/ credentials */
    if((stat = NC_aws_load_credentials(ncg))) {
        nclog(NCLOGWARN,"AWS config file not loaded");
    }
    }
#endif
}

static void
ncrc_setrchome(void)
{
    const char* tmp = NULL;
    NCglobalstate* ncg = NC_getglobalstate();
    assert(ncg && ncg->home);
    if(ncg->rcinfo->rchome) return;
    tmp = getenv(NCRCENVHOME);
    if(tmp == NULL || strlen(tmp) == 0)
	tmp = ncg->home;
    ncg->rcinfo->rchome = strdup(tmp);
#ifdef DRCDEBUG
    fprintf(stderr,"ncrc_setrchome: %s\n",ncg->rcinfo->rchome);
#endif
}

void
NC_rcclear(NCRCinfo* info)
{
    if(info == NULL) return;
    nullfree(info->rcfile);
    nullfree(info->rchome);
    rcfreeentries(info->entries);
    NC_s3freeprofilelist(info->s3profiles);
}

static void
rcfreeentry(NCRCentry* t)
{
	nullfree(t->host);
	nullfree(t->urlpath);
	nullfree(t->key);
	nullfree(t->value);
	free(t);
}

static void
rcfreeentries(NClist* rc)
{
    size_t i;
    for(i=0;i<nclistlength(rc);i++) {
	NCRCentry* t = (NCRCentry*)nclistget(rc,i);
	rcfreeentry(t);
    }
    nclistfree(rc);
}

/* locate, read and compile the rc files, if any */
static int
NC_rcload(void)
{
    size_t i;
    int ret = NC_NOERR;
    char* path = NULL;
    NCglobalstate* globalstate = NULL;
    NClist* rcfileorder = nclistnew();

    if(!NCRCinitialized) ncrc_initialize();
    globalstate = NC_getglobalstate();

    if(globalstate->rcinfo->ignore) {
        nclog(NCLOGNOTE,".rc file loading suppressed");
	goto done;
    }
    if(globalstate->rcinfo->loaded) goto done;

    /* locate the configuration files in order of use:
       1. Specified by NCRCENV_RC environment variable.
       2. If NCRCENV_RC is not set then merge the set of rc files in this order:
	  1. $HOME/.ncrc
	  2. $HOME/.dodsrc
	  3. $CWD/.ncrc
	  4. $CWD/.dodsrc
	  Entries in later files override any of the earlier files
    */
    if(globalstate->rcinfo->rcfile != NULL) { /* always use this */
	nclistpush(rcfileorder,strdup(globalstate->rcinfo->rcfile));
    } else {
	const char** rcname;
	const char* dirnames[3];
	const char** dir;

        /* Make sure rcinfo.rchome is defined */
	ncrc_setrchome();
	dirnames[0] = globalstate->rcinfo->rchome;
	dirnames[1] = globalstate->cwd;
	dirnames[2] = NULL;

        for(dir=dirnames;*dir;dir++) {
	    for(rcname=rcfilenames;*rcname;rcname++) {
	        ret = rcsearch(*dir,*rcname,&path);
		if(ret == NC_NOERR && path != NULL)
		    nclistpush(rcfileorder,path);
		path = NULL;
	    }
	}
    }
    for(i=0;i<nclistlength(rcfileorder);i++) {
	path = (char*)nclistget(rcfileorder,i);
	if((ret=rccompile(path))) {
	    nclog(NCLOGWARN, "Error parsing %s\n",path);
	    ret = NC_NOERR; /* ignore it */
	    goto done;
	}
    }

done:
    globalstate->rcinfo->loaded = 1; /* even if not exists */
    nclistfreeall(rcfileorder);
    return (ret);
}

/**
 * Locate a entry by property key.
 * If duplicate keys, first takes precedence.
 * @param key to lookup
 * @param hostport to use for lookup
 * @param urlpath to use for lookup
 * @return the value of the key or NULL if not found.
 */
char*
NC_rclookup(const char* key, const char* hostport, const char* urlpath)
{
    struct NCRCentry* entry = NULL;
    if(!NCRCinitialized) ncrc_initialize();
    entry = rclocate(key,hostport,urlpath);
    return (entry == NULL ? NULL : entry->value);
}

/**
 * Locate a entry by property key and uri.
 * If duplicate keys, first takes precedence.
 */
char*
NC_rclookupx(NCURI* uri, const char* key)
{
    char* hostport = NULL;
    char* result = NULL;

    hostport = NC_combinehostport(uri);
    result = NC_rclookup(key,hostport,uri->path);
    nullfree(hostport);
    return result;
}

#if 0
/*!
Set the absolute path to use for the rc file.
WARNING: this MUST be called before any other
call in order for this to take effect.

\param[in] rcfile The path to use. If NULL then do not use any rcfile.

\retval OC_NOERR if the request succeeded.
\retval OC_ERCFILE if the file failed to load
*/

int
NC_set_rcfile(const char* rcfile)
{
    int stat = NC_NOERR;
    FILE* f = NULL;
    NCglobalstate* globalstate = NC_getglobalstate();

    if(rcfile != NULL && strlen(rcfile) == 0)
	rcfile = NULL;
    f = NCfopen(rcfile,"r");
    if(f == NULL) {
	stat = NC_ERCFILE;
        goto done;
    }
    fclose(f);
    NC_rcclear(globalstate->rcinfo);
    globalstate->rcinfo->rcfile = strdup(rcfile);
    /* Clear globalstate->rcinfo */
    NC_rcclear(&globalstate->rcinfo);
    /* (re) load the rcfile and esp the entriestore*/
    stat = NC_rcload();
done:
    return stat;
}
#endif

/**************************************************/
/* RC processing functions */

static char*
rcreadline(char** nextlinep)
{
    char* line;
    char* p;

    line = (p = *nextlinep);
    if(*p == '\0') return NULL; /*signal done*/
    for(;*p;p++) {
	if(*p == '\r' && p[1] == '\n') *p = '\0';
	else if(*p == '\n') break;
    }
    *p++ = '\0'; /* null terminate line; overwrite newline */
    *nextlinep = p;
    return line;
}

/* Trim TRIMCHARS from both ends of text; */
static void
rctrim(char* text)
{
    char* p;
    char* q;
    size_t len = 0;

    if(text == NULL || *text == '\0') return;

    len = strlen(text);

    /* elide upto first non-trimchar */
    for(q=text,p=text;*p;p++) {
	if(*p != ' ' && *p != '\t' && *p != '\r') {*q++ = *p;}
    }
    len = strlen(p);
    /* locate last non-trimchar */
    if(len > 0) {
        for(size_t i = len; i-->0;) {
	    p = &text[i];
	    if(*p != ' ' && *p != '\t' && *p != '\r') {break;}
	    *p = '\0'; /* elide trailing trimchars */
        }
    }
}

/* Order the entries: those with urls must be first,
   but otherwise relative order does not matter.
*/
static void
rcorder(NClist* rc)
{
    size_t i;
    size_t len = nclistlength(rc);
    NClist* tmprc = NULL;
    if(rc == NULL || len == 0) return;
    tmprc = nclistnew();
    /* Two passes: 1) pull entries with host */
    for(i=0;i<len;i++) {
        NCRCentry* ti = nclistget(rc,i);
	if(ti->host == NULL) continue;
	nclistpush(tmprc,ti);
    }
    /* pass 2 pull entries without host*/
    for(i=0;i<len;i++) {
        NCRCentry* ti = nclistget(rc,i);
	if(ti->host != NULL) continue;
	nclistpush(tmprc,ti);
    }
    /* Move tmp to rc */
    nclistsetlength(rc,0);
    for(i=0;i<len;i++) {
        NCRCentry* ti = nclistget(tmprc,i);
	nclistpush(rc,ti);
    }
#ifdef DRCDEBUG
    storedump("reorder:",rc);
#endif
    nclistfree(tmprc);
}

/* Merge a entry store from a file*/
static int
rccompile(const char* filepath)
{
    int ret = NC_NOERR;
    NClist* rc = NULL;
    char* contents = NULL;
    NCbytes* tmp = ncbytesnew();
    NCURI* uri = NULL;
    char* nextline = NULL;
    NCglobalstate* globalstate = NC_getglobalstate();
    NCS3INFO s3;

    memset(&s3,0,sizeof(s3));

    if((ret=NC_readfile(filepath,tmp))) {
        nclog(NCLOGWARN, "Could not open configuration file: %s",filepath);
	goto done;
    }
    contents = ncbytesextract(tmp);
    if(contents == NULL) contents = strdup("");
    /* Either reuse or create new  */
    rc = globalstate->rcinfo->entries;
    if(rc == NULL) {
        rc = nclistnew();
        globalstate->rcinfo->entries = rc;
    }
    nextline = contents;
    for(;;) {
	char* line;
	char* key = NULL;
        char* value = NULL;
        char* host = NULL;
        char* urlpath = NULL;
	size_t llen;
        NCRCentry* entry;

	line = rcreadline(&nextline);
	if(line == NULL) break; /* done */
        rctrim(line);  /* trim leading and trailing blanks */
        if(line[0] == '#') continue; /* comment */
	if((llen=strlen(line)) == 0) continue; /* empty line */
	if(line[0] == LTAG) {
	    char* url = ++line;
            char* rtag = strchr(line,RTAG);
            if(rtag == NULL) {
                nclog(NCLOGERR, "Malformed [url] in %s entry: %s",filepath,line);
		continue;
            }
            line = rtag + 1;
            *rtag = '\0';
            /* compile the url and pull out the host, port, and path */
            if(uri) ncurifree(uri);
            if(ncuriparse(url,&uri)) {
                nclog(NCLOGERR, "Malformed [url] in %s entry: %s",filepath,line);
		continue;
            }
	    if(NC_iss3(uri,NULL)) {
	         NCURI* newuri = NULL;
	        /* Rebuild the url to S3 "path" format */
		NC_s3clear(&s3);
	        if((ret = NC_s3urlrebuild(uri,&s3,&newuri))) goto done;
		ncurifree(uri);
		uri = newuri;
		newuri = NULL;
	    }
	    /* Get the host+port */
            ncbytesclear(tmp);
            ncbytescat(tmp,uri->host);
            if(uri->port != NULL) {
		ncbytesappend(tmp,':');
                ncbytescat(tmp,uri->port);
            }
            ncbytesnull(tmp);
            host = ncbytesextract(tmp);
	    if(strlen(host)==0) /* nullify host */
		{free(host); host = NULL;}
	    /* Get the url path part */
	    urlpath = uri->path;
	    if(urlpath && strlen(urlpath)==0) urlpath = NULL; /* nullify */
	}
        /* split off key and value */
        key=line;
        value = strchr(line, '=');
        if(value == NULL)
            value = line + strlen(line);
        else {
            *value = '\0';
            value++;
        }
	/* See if key already exists */
	entry = rclocate(key,host,urlpath);
	if(entry == NULL) {
	    entry = (NCRCentry*)calloc(1,sizeof(NCRCentry));
	    if(entry == NULL) {ret = NC_ENOMEM; goto done;}
	    nclistpush(rc,entry);
	    entry->host = host; host = NULL;
	    entry->urlpath = nulldup(urlpath);
    	    entry->key = nulldup(key);
            rctrim(entry->host);
            rctrim(entry->urlpath);
            rctrim(entry->key);
	}
	nullfree(entry->value);
        entry->value = nulldup(value);
        rctrim(entry->value);

#ifdef DRCDEBUG
	fprintf(stderr,"rc: host=%s urlpath=%s key=%s value=%s\n",
		(entry->host != NULL ? entry->host : "<null>"),
		(entry->urlpath != NULL ? entry->urlpath : "<null>"),
		entry->key,entry->value);
#endif
	entry = NULL;
    }
#ifdef DRCDEBUG
    fprintf(stderr,"reorder.path=%s\n",filepath);
#endif
    rcorder(rc);

done:
    NC_s3clear(&s3);
    if(contents) free(contents);
    ncurifree(uri);
    ncbytesfree(tmp);
    return (ret);
}

/**
Encapsulate equality comparison: return 1|0
*/
static int
rcequal(NCRCentry* e1, NCRCentry* e2)
{
    int nulltest;
    if(e1->key == NULL || e2->key == NULL) return 0;
    if(strcmp(e1->key,e2->key) != 0) return 0;
    /* test hostport; take NULL into account*/
    nulltest = 0;
    if(e1->host == NULL) nulltest |= 1;
    if(e2->host == NULL) nulltest |= 2;
    /* Use host to decide if entry applies */
    switch (nulltest) {
    case 0: if(strcmp(e1->host,e2->host) != 0) {return 0;}  break;
    case 1: break;    /* .rc->host == NULL && candidate->host != NULL */
    case 2: return 0; /* .rc->host != NULL && candidate->host == NULL */
    case 3: break;    /* .rc->host == NULL && candidate->host == NULL */
    default: return 0;
    }
    /* test urlpath take NULL into account*/
    nulltest = 0;
    if(e1->urlpath == NULL) nulltest |= 1;
    if(e2->urlpath == NULL) nulltest |= 2;
    switch (nulltest) {
    case 0: if(strcmp(e1->urlpath,e2->urlpath) != 0) {return 0;} break;
    case 1: break;    /* .rc->urlpath == NULL && candidate->urlpath != NULL */
    case 2: return 0; /* .rc->urlpath != NULL && candidate->urlpath == NULL */
    case 3: break;    /* .rc->urlpath == NULL && candidate->urlpath == NULL */
    default: return 0;
    }
    return 1;
}

/**
 * (Internal) Locate a entry by property key and host+port (may be null) and urlpath (may be null)
 * If duplicate keys, first takes precedence.
 */
static int
rclocatepos(const char* key, const char* hostport, const char* urlpath)
{
    size_t i;
    NCglobalstate* globalstate = NC_getglobalstate();
    struct NCRCinfo* info = globalstate->rcinfo;
    NCRCentry* entry = NULL;
    NCRCentry candidate;
    NClist* rc = info->entries;

    if(info->ignore) return -1;

    candidate.key = (char*)key;
    candidate.value = (char*)NULL;
    candidate.host = (char*)hostport;
    candidate.urlpath = (char*)urlpath;

    for(i=0;i<nclistlength(rc);i++) {
      entry = (NCRCentry*)nclistget(rc,i);
      if(rcequal(entry,&candidate)) return (int)i;
    }
    return -1;
}

/**
 * (Internal) Locate a entry by property key and host+port (may be null or "").
 * If duplicate keys, first takes precedence.
 */
static struct NCRCentry*
rclocate(const char* key, const char* hostport, const char* urlpath)
{
    int pos;
    NCglobalstate* globalstate = NC_getglobalstate();
    struct NCRCinfo* info = globalstate->rcinfo;

    if(globalstate->rcinfo->ignore) return NULL;
    if(key == NULL || info == NULL) return NULL;
    pos = rclocatepos(key,hostport,urlpath);
    if(pos < 0) return NULL;
    return NC_rcfile_ith(info,(size_t)pos);
}

/**
 * Locate rc file by searching in directory prefix.
 */
static
int
rcsearch(const char* prefix, const char* rcname, char** pathp)
{
    char* path = NULL;
    FILE* f = NULL;
    size_t plen = (prefix?strlen(prefix):0);
    size_t rclen = strlen(rcname);
    int ret = NC_NOERR;

    size_t pathlen = plen+rclen+1+1; /*+1 for '/' +1 for nul */
    path = (char*)malloc(pathlen); /* +1 for nul*/
    if(path == NULL) {ret = NC_ENOMEM;	goto done;}
    snprintf(path, pathlen, "%s/%s", prefix, rcname);
    /* see if file is readable */
    f = NCfopen(path,"r");
    if(f != NULL)
        nclog(NCLOGNOTE, "Found rc file=%s",path);
done:
    if(f == NULL || ret != NC_NOERR) {
	nullfree(path);
	path = NULL;
    }
    if(f != NULL)
      fclose(f);
    if(pathp != NULL)
      *pathp = path;
    else {
      nullfree(path);
      path = NULL;
    }
    errno = 0; /* silently ignore errors */
    return (ret);
}

int
NC_rcfile_insert(const char* key, const char* hostport, const char* urlpath, const char* value)
{
    int ret = NC_NOERR;
    /* See if this key already defined */
    struct NCRCentry* entry = NULL;
    NCglobalstate* globalstate = NULL;
    NClist* rc = NULL;

    if(!NCRCinitialized) ncrc_initialize();

    if(key == NULL || value == NULL)
        {ret = NC_EINVAL; goto done;}

    globalstate = NC_getglobalstate();
    rc = globalstate->rcinfo->entries;

    if(rc == NULL) {
	rc = nclistnew();
        globalstate->rcinfo->entries = rc;
	if(rc == NULL) {ret = NC_ENOMEM; goto done;}
    }
    entry = rclocate(key,hostport,urlpath);
    if(entry == NULL) {
	entry = (NCRCentry*)calloc(1,sizeof(NCRCentry));
	if(entry == NULL) {ret = NC_ENOMEM; goto done;}
	entry->key = strdup(key);
	entry->value = NULL;
        rctrim(entry->key);
        entry->host = nulldup(hostport);
        rctrim(entry->host);
        entry->urlpath = nulldup(urlpath);
        rctrim(entry->urlpath);
	nclistpush(rc,entry);
    }
    if(entry->value != NULL) free(entry->value);
    entry->value = strdup(value);
    rctrim(entry->value);
#ifdef DRCDEBUG
    storedump("NC_rcfile_insert",rc);
#endif    
done:
    return ret;
}

/* Obtain the count of number of entries */
size_t
NC_rcfile_length(NCRCinfo* info)
{
    return nclistlength(info->entries);
}

/* Obtain the ith entry; return NULL if out of range */
NCRCentry*
NC_rcfile_ith(NCRCinfo* info, size_t i)
{
    if(i >= nclistlength(info->entries))
	return NULL;
    return (NCRCentry*)nclistget(info->entries,i);
}


#ifdef DRCDEBUG
static void
storedump(char* msg, NClist* entries)
{
    int i;

    if(msg != NULL) fprintf(stderr,"%s\n",msg);
    if(entries == NULL || nclistlength(entries)==0) {
        fprintf(stderr,"<EMPTY>\n");
        return;
    }
    for(i=0;i<nclistlength(entries);i++) {
	NCRCentry* t = (NCRCentry*)nclistget(entries,i);
        fprintf(stderr,"\t%s\t%s\t%s\n",
                ((t->host == NULL || strlen(t->host)==0)?"--":t->host),t->key,t->value);
    }
    fflush(stderr);
}
#endif
