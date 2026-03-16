/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "d4includes.h"
#include <stddef.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef _MSC_VER
#include <io.h>
#endif

#define LBRACKET '['
#define RBRACKET ']'

#define VERIFY(off,space) assert((off)->offset+(space) <= (off)->limit)

/**************************************************/
/* Forward */

static char* backslashEscape(const char* s);

/**************************************************/
/**
 * Provide a hidden interface to allow utilities
 * to check if a given path name is really an ncdap4 url.
 * If no, return null, else return basename of the url
 * minus any extension.
 */

int
ncd4__testurl(const char* path, char** basenamep)
{
    NCURI* uri;
    int ok = NC_NOERR;
    if(ncuriparse(path,&uri))
	ok = NC_EURL;
    else {
	char* slash = (uri->path == NULL ? NULL : strrchr(uri->path, '/'));
	char* dot;
	if(slash == NULL) slash = (char*)path; else slash++;
        slash = nulldup(slash);
        if(slash == NULL)
            dot = NULL;
        else
            dot = strrchr(slash, '.');
        if(dot != NULL &&  dot != slash) *dot = '\0';
        if(basenamep)
            *basenamep=slash;
        else if(slash)
            free(slash);
    }
    ncurifree(uri);
    return ok;
}

/* Return 1 if this machine is little endian */
int
NCD4_isLittleEndian(void)
{
    union {
        unsigned char bytes[SIZEOF_INT];
	int i;
    } u;
    u.i = 1;
    return (u.bytes[0] == 1 ? 1 : 0);
}

/* Compute the size of an atomic type, except opaque */
size_t
NCD4_typesize(nc_type tid)
{
    switch(tid) {
    case NC_BYTE: case NC_UBYTE: case NC_CHAR: return 1;
    case NC_SHORT: case NC_USHORT: return sizeof(short);
    case NC_INT: case NC_UINT: return sizeof(int);
    case NC_FLOAT: return  sizeof(float);
    case NC_DOUBLE: return  sizeof(double);
    case NC_INT64: case NC_UINT64: return sizeof(long long);
    case NC_STRING: return sizeof(char*);
    default: break;
    }
    return 0;
}

d4size_t
NCD4_dimproduct(NCD4node* node)
{
    size_t i;
    d4size_t product = 1;
    for(i=0;i<nclistlength(node->dims);i++) {
        NCD4node* dim = (NCD4node*)nclistget(node->dims,i);
        product *= dim->dim.size;
    }
    return product;
}

/* Caller must free return value */
char*
NCD4_makeFQN(NCD4node* node)
{
    char* fqn = NULL;
    char* escaped;
    NCbytes* buf = ncbytesnew();
    NClist* grps = nclistnew();
    NClist* parts = nclistnew();
    NCD4node* n;
    size_t i;

    /* collect all the non-groups */
    for(n=node;n;n=n->container) {
        if(ISGROUP(n->sort))
	    nclistinsert(grps,0,n); /* keep the correct order of groups */
	else
	    nclistinsert(parts,0,n);	
    }
    
    /* Build grp prefix of the fqn */
    for(i=1;i<nclistlength(grps);i++) {
        n = (NCD4node*)nclistget(grps,i);
	/* Add in the group name */
	escaped = backslashEscape(n->name);
	if(escaped == NULL) goto done;
	ncbytescat(buf,"/");
	ncbytescat(buf,escaped);
	free(escaped);
    }
    /* Add in the final name part (if not group) */
    for(i=0;i<nclistlength(parts);i++) {
        n = (NCD4node*)nclistget(parts,i);
	escaped = backslashEscape(n->name);
	if(escaped == NULL) goto done;
	ncbytescat(buf,(i==0?"/":"."));
	ncbytescat(buf,escaped);
	free(escaped);
    }
    fqn = ncbytesextract(buf);

done:
    ncbytesfree(buf);
    nclistfree(grps);
    nclistfree(parts);
    return fqn;
}

/*
create the last part of the fqn
(post groups)
*/
char*
NCD4_makeName(NCD4node* elem, const char* sep)
{
    size_t i;
    size_t estimate = 0;
    NCD4node* n;
    NClist* path = nclistnew();
    char* fqn = NULL;

    /* Collect the path up to, but not including, the first containing group */
    for(estimate=0,n=elem;n->sort != NCD4_GROUP;n=n->container) {
	nclistinsert(path,0,n);
	estimate += (1+(2*strlen(n->name)));
    }
    estimate++; /*strlcat nul*/
    fqn = (char*)malloc(estimate+1);
    if(fqn == NULL) goto done;
    fqn[0] = '\0';

    for(i=0;i<nclistlength(path);i++) {
	NCD4node* elem = (NCD4node*)nclistget(path,i);
	char* escaped = backslashEscape(elem->name);
	if(escaped == NULL) {free(fqn); fqn = NULL; goto done;}
	if(i > 0)
	    strlcat(fqn,sep,estimate);
	strlcat(fqn,escaped,estimate);
	free(escaped);
    }
done:
    nclistfree(path);
    return fqn;
}

static char*
backslashEscape(const char* s)
{
    const char* p;
    char* q;
    size_t len;
    char* escaped = NULL;

    len = strlen(s);
    escaped = (char*)malloc(1+(2*len)); /* max is everychar is escaped */
    if(escaped == NULL) return NULL;
    for(p=s,q=escaped;*p;p++) {
        char c = *p;
        switch (c) {
	case '\\':
	case '/':
	case '.':
	case '@':
	    *q++ = '\\'; *q++ = '\\';
	    break;
	default: *q++ = c; break;
        }
    }
    *q = '\0';
    return escaped;
}

/* Parse an fqn into a sequence of names;
   using '/', and then (conditionally) '.' */
int
NCD4_parseFQN(const char* fqn0, NClist* pieces)
{
    int ret = NC_NOERR;
    int count;
    char* p;
    char* start;
    char* fqn = NULL;

    if(fqn0 == NULL) fqn0 = "/";
    fqn = strdup(fqn0[0] == '/' ? fqn0+1 : fqn0);
    start = fqn;
    /* Step 0: insert rootname */
    nclistpush(pieces,strdup("/"));
    /* Step 1: Break fqn into pieces at occurrences of '/' */
    count = 0;
    for(p=start;*p;) {
	switch(*p) {
	case '\\': /* leave the escapes in place */
	    p+=2;
	    break;
	case '/': /*capture the piece name */
	    *p++ = '\0';
	    start = p; /* mark start of the next part */
	    count++;
	    break;
	default: /* ordinary char */
	    p++;
	    break;
	}
    }
#ifdef ALLOWFIELDMAPS
    /* Step 2, walk the final piece to break up based on '.' */
    for(p=start;*p;) {
	switch(*p) {
	case '\\': /* leave the escapes in place */
	    p+=2;
	    break;
	case '.': /*capture the piece name */
	    *p++ = '\0';
	    start = p;
	    count++;
	    break;
	default: /* ordinary char */
	    p++;
	    break;
	}
    }
#endif
    count++; /* acct for last piece */
    /* Step 3: capture and de-scape the pieces */
    for(p=fqn;count > 0;count--) {
	char* descaped = NCD4_deescape(p);
	nclistpush(pieces,descaped);
	p = p + strlen(p) + 1; /* skip past the terminating nul */
    }
    if(fqn != NULL) free(fqn);
    return THROW(ret);
}

char*
NCD4_deescape(const char* esc)
{
    size_t len;
    char* s;
    const char* p;
    char* q;

    if(esc == NULL) return NULL;
    len = strlen(esc);
    s = (char*)malloc(len+1);
    if(s == NULL) return NULL;
    for(p=esc,q=s;*p;) {
	switch (*p) {
	case '\\':
	     p++;
	     /* fall thru */
	default: *q++ = *p++; break;
	}
    }
    *q = '\0';
    return s;
}

char*
NCD4_entityescape(const char* s)
{
    const char* p;
    char* q;
    size_t len;
    char* escaped = NULL;
    const char* entity;

    len = strlen(s);
    escaped = (char*)malloc(1+(6*len)); /* 6 = |&apos;| */
    if(escaped == NULL) return NULL;
    for(p=s,q=escaped;*p;p++) {
	char c = *p;
	switch (c) {
	case '&':  entity = "&amp;"; break;
	case '<':  entity = "&lt;"; break;
	case '>':  entity = "&gt;"; break;
	case '"':  entity = "&quot;"; break;
	case '\'': entity = "&apos;"; break;
	default	 : entity = NULL; break;
	}
	if(entity == NULL)
	    *q++ = c;
	else {
	    len = strlen(entity);
	    memcpy(q,entity,len);
	    q+=len;
	}
    }
    *q = '\0';
    return escaped;
}

/* Elide all nul characters from an XML document as a precaution*/
size_t
NCD4_elidenuls(char* s, size_t slen)
{
    size_t i,j;
    for(j=0,i=0;i<slen;i++) {
        int c = s[i];
	if(c != 0)
	    s[j++] = (char)c;
    }       
    /* if we remove any nuls then nul term */
    if(j < i)
	s[j] = '\0';
    return j;
}

void
NCD4_hostport(NCURI* uri, char* space, size_t len)
{
    if(space != NULL && len >  0) {
	space[0] = '\0'; /* so we can use strlcat */
        if(uri->host != NULL) {
	    strlcat(space,uri->host,len);
            if(uri->port != NULL) {
	        strlcat(space,":",len);
	        strlcat(space,uri->port,len);
	    }
	}
    }
}

void
NCD4_userpwd(NCURI* uri, char* space, size_t len)
{
    if(space != NULL && len > 0) {
	space[0] = '\0'; /* so we can use strlcat */
        if(uri->user != NULL && uri->password != NULL) {
            strlcat(space,uri->user,len);
            strlcat(space,":",len);
            strlcat(space,uri->password,len);
	}
    }
}

/**************************************************/
/* Error reporting */

int
NCD4_error(int code, const int line, const char* file, const char* fmt, ...)
{
    va_list argv;
    fprintf(stderr,"(%s:%d) ",file,line);
    va_start(argv,fmt);
    ncvlog(NCLOGERR,fmt,argv);
    return code;
}

int
NCD4_errorNC(int code, const int line, const char* file)
{
    return NCD4_error(code,line,file,nc_strerror(code));
}

NCD4offset*
NCD4_buildoffset(void* base, d4size_t limit)
{
    NCD4offset* offset = (NCD4offset*)calloc(1,sizeof(NCD4offset));
    assert(offset != NULL);
    offset->base = base;
    offset->limit = ((char*)base)+limit;
    offset->offset = base;
    return offset;
}

d4size_t
NCD4_getcounter(NCD4offset* p)
{
    COUNTERTYPE v;
    VERIFY(p,sizeof(v));
    memcpy(&v,p->offset,sizeof(v));
    return (d4size_t)v;
}

void
NCD4_incr(NCD4offset* p, d4size_t size)
{
    VERIFY(p,size);    
    p->offset += size;
}

void
NCD4_decr(NCD4offset* p, d4size_t size)
{
    VERIFY(p,size);    
    p->offset -= size;
}

void*
NCD4_getheader(void* p, NCD4HDR* hdr, int hostlittleendian)
{
    unsigned char bytes[4];
    memcpy(bytes,p,sizeof(bytes));
    p = ((char*)p) + 4; /* on-the-wire hdr is 4 bytes */
    /* assume header is network (big) order */
    hdr->flags = bytes[0]; /* big endian => flags are in byte 0 */
    hdr->flags &= NCD4_ALL_CHUNK_FLAGS; /* Ignore extraneous flags */
    bytes[0] = 0; /* so we can do byte swap to get count */
    if(hostlittleendian)
        swapinline32(bytes); /* host is little endian */
    hdr->count = *(unsigned int*)bytes; /* get count */
    return p;
}

void
NCD4_reporterror(NCD4response* resp, NCURI* uri)
{
    char* u = NULL;
    u = ncuribuild(uri,NULL,NULL,NCURIALL);     
    fprintf(stderr,"***FAIL: url=%s httpcode=%d errmsg->\n%s\n",u,resp->serial.httpcode,resp->error.message);
}
