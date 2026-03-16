/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#include "config.h"
#include <errno.h>
#include <stddef.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#ifdef _MSC_VER
#include <io.h>
#define mode_t int
#endif

#include "ocinternal.h"
#include "ocdebug.h"

/* Order is important: longest first */
static const char* DDSdatamarks[3] = {"Data:\r\n","Data:\n",(char*)NULL};

/* Not all systems have strndup, so provide one*/
char*
ocstrndup(const char* s, size_t len)
{
    char* dup;
    if(s == NULL) return NULL;
    dup = (char*)ocmalloc(len+1);
    MEMCHECK(dup,NULL);
    memcpy((void*)dup,s,len);
    dup[len] = '\0';
    return dup;
}

/* Do not trust strncmp semantics; this one
   compares up to len chars or to null terminators */
int
ocstrncmp(const char* s1, const char* s2, size_t len)
{
    const char *p,*q;
    if(s1 == s2) return 0;
    if(s1 == NULL) return -1;
    if(s2 == NULL) return +1;
    for(p=s1,q=s2;len > 0;p++,q++,len--) {
	if(*p == 0 && *q == 0) return 0; /* *p == *q == 0 */
	if(*p != *q)
	    return (*p - *q);	
    }
    /* 1st len chars are same */
    return 0;
}


#if 0
void
makedimlist(Nclist* path, Nclist* dims)
{
    unsigned int i,j;
    for(i=0;i<nclistlength(path);i++) {
	OCnode* node = (OCnode*)nclistget(path,i);
        unsigned int rank = node->array.rank;
	for(j=0;j<rank;j++) {
	    OCnode* dim = (OCnode*)nclistget(node->array.dimensions,j);
	    nclistpush(dims,(void*)dim);
        }
    }
}
#endif

void
ocfreeprojectionclause(OCprojectionclause* clause)
{
    if(clause->target != NULL) free(clause->target);
    while(nclistlength(clause->indexsets) > 0) {
	NClist* slices = (NClist*)nclistpop(clause->indexsets);
        while(nclistlength(slices) > 0) {
	    OCslice* slice = (OCslice*)nclistpop(slices);
	    if(slice != NULL) free(slice);
	}
        nclistfree(slices);
    }
    nclistfree(clause->indexsets);
    free(clause);
}

#if 0
void
freeAttributes(NClist* attset)
{
    unsigned int i,j;
    for(i=0;i<nclistlength(attset);i++) {
	OCattribute* att = (OCattribute*)nclistget(attset,i);
	if(att->name != NULL) free(att->name);
	if(att->etype == OC_String || att->etype == OC_URL) {
	    for(j=0;j<att->nvalues;j++) {
		char* s = ((char**)att->values)[j];
		if(s != NULL) free(s);
	    }
	} else {
	    free(att->values);
	}
    }
}
#endif

#if 0
void
freeOCnode(OCnode* cdf, int deep)
{
    unsigned int i;
    if(cdf == NULL) return;
    if(cdf->name != NULL) free(cdf->name);
    if(cdf->fullname != NULL) free(cdf->fullname);
    if(cdf->attributes != NULL) freeAttributes(cdf->attributes);
    if(cdf->subnodes != NULL) {
	if(deep) {
            for(i=0;i<nclistlength(cdf->subnodes);i++) {
	        OCnode* node = (OCnode*)nclistget(cdf->subnodes,i);
		freeOCnode(node,deep);
	    }
	}
        nclistfree(cdf->subnodes);
    }
    free(cdf);
}
#endif

int
ocfindbod(NCbytes* buffer, size_t* bodp, size_t* ddslenp)
{
    char* content;
    size_t len = ncbyteslength(buffer);
    const char** marks;
    
    content = ncbytescontents(buffer);

    for(marks = DDSdatamarks;*marks;marks++) {
	const char* mark = *marks;
        size_t tlen = strlen(mark);
        for(size_t i=0;i<len;i++) {
	    if((i+tlen) <= len 
	        && (ocstrncmp(content+i,mark,tlen)==0)) {
	       *ddslenp = i;
	        i += tlen;
	        *bodp = i;
	        return 1;
	    }
	}
    }
    *ddslenp = 0;
    *bodp = 0;
    return 0; /* tag not found; not necessarily an error*/
}

/* Compute total # of elements if dimensioned*/
size_t
octotaldimsize(size_t rank, size_t* sizes)
{
    unsigned int i;
    size_t count = 1;
    for(i=0;i<rank;i++) {
        count *= sizes[i];
    }
    return count;
}

size_t
octypesize(OCtype etype)
{
    switch (etype) {
    case OC_Char:	return sizeof(char);
    case OC_Byte:	return sizeof(signed char);
    case OC_UByte:	return sizeof(unsigned char);
    case OC_Int16:	return sizeof(short);
    case OC_UInt16:	return sizeof(unsigned short);
    case OC_Int32:	return sizeof(int);
    case OC_UInt32:	return sizeof(unsigned int);
    case OC_Float32:	return sizeof(float);
    case OC_Float64:	return sizeof(double);
#ifdef HAVE_LONG_LONG_INT
    case OC_Int64:	return sizeof(long long);
    case OC_UInt64:	return sizeof(unsigned long long);
#endif
    case OC_String:	return sizeof(char*);
    case OC_URL:	return sizeof(char*);
    default: break;     /* Ignore all others */
    }
    return 0;
}

char*
octypetostring(OCtype octype)
{
    switch (octype) {
    case OC_NAT:          return "OC_NAT";
    case OC_Char:         return "OC_Char";
    case OC_Byte:         return "OC_Byte";
    case OC_UByte:	   return "OC_UByte";
    case OC_Int16:        return "OC_Int16";
    case OC_UInt16:       return "OC_UInt16";
    case OC_Int32:        return "OC_Int32";
    case OC_UInt32:       return "OC_UInt32";
    case OC_Int64:        return "OC_Int64";
    case OC_UInt64:       return "OC_UInt64";
    case OC_Float32:      return "OC_Float32";
    case OC_Float64:      return "OC_Float64";
    case OC_String:       return "OC_String";
    case OC_URL:          return "OC_URL";
    /* Non-primitives*/
    case OC_Dataset:      return "OC_Dataset";
    case OC_Sequence:     return "OC_Sequence";
    case OC_Grid:         return "OC_Grid";
    case OC_Structure:    return "OC_Structure";
    case OC_Dimension:    return "OC_Dimension";
    case OC_Attribute:    return "OC_Attribute";
    case OC_Attributeset: return "OC_Attributeset";
    case OC_Atomic:       return "OC_Atomic";
    default: break;
    }
    return NULL;
}

char*
octypetoddsstring(OCtype octype)
{
    switch (octype) {
    case OC_Byte:         return "Byte";
    case OC_Int16:        return "Int16";
    case OC_UInt16:       return "UInt16";
    case OC_Int32:        return "Int32";
    case OC_UInt32:       return "UInt32";
    case OC_Float32:      return "Float32";
    case OC_Float64:      return "Float64";
    case OC_String:       return "String";
    case OC_URL:          return "Url";
    /* Non-atomics*/
    case OC_Dataset:      return "Dataset";
    case OC_Sequence:     return "Sequence";
    case OC_Grid:         return "Grid";
    case OC_Structure:    return "Structure";
    case OC_Dimension:    return "Dimension";
    case OC_Attribute:    return "Attribute";
    case OC_Attributeset: return "Attributeset";
    case OC_Atomic:       return "Atomic";
    default: break;
    }
    return "<unknown>";
}


OCerror
octypeprint(OCtype etype, void* value, size_t bufsize, char* buf)
{
    if(buf == NULL || bufsize == 0 || value == NULL) return OC_EINVAL;
    buf[0] = '\0';
    switch (etype) {
    case OC_Char:
	snprintf(buf,bufsize,"'%c'",*(char*)value);
	break;
    case OC_Byte:
	snprintf(buf,bufsize,"%d",*(signed char*)value);
	break;
    case OC_UByte:
	snprintf(buf,bufsize,"%u",*(unsigned char*)value);
	break;
    case OC_Int16:
	snprintf(buf,bufsize,"%d",*(short*)value);
	break;
    case OC_UInt16:
	snprintf(buf,bufsize,"%u",*(unsigned short*)value);
	break;
    case OC_Int32:
	snprintf(buf,bufsize,"%d",*(int*)value);
	break;
    case OC_UInt32:
	snprintf(buf,bufsize,"%u",*(unsigned int*)value);
	break;
    case OC_Float32:
	snprintf(buf,bufsize,"%g",*(float*)value);
	break;
    case OC_Float64:
	snprintf(buf,bufsize,"%g",*(double*)value);
	break;
#ifdef HAVE_LONG_LONG_INT
    case OC_Int64:
	snprintf(buf,bufsize,"%lld",*(long long*)value);
	break;
    case OC_UInt64:
	snprintf(buf,bufsize,"%llu",*(unsigned long long*)value);
	break;
#endif
    case OC_String:
    case OC_URL: {
	char* s = *(char**)value;
	snprintf(buf,bufsize,"\"%s\"",s);
	} break;
    default: break;
    }
    return OC_NOERR;
}

size_t
xxdrsize(OCtype etype)
{
    switch (etype) {
    case OC_Char:
    case OC_Byte:
    case OC_UByte:
    case OC_Int16:
    case OC_UInt16:
    case OC_Int32:
    case OC_UInt32:
	return XDRUNIT;
    case OC_Int64:
    case OC_UInt64:
	return (2*XDRUNIT);
    case OC_Float32:
	return XDRUNIT;
    case OC_Float64:
	return (2*XDRUNIT);
    case OC_String:
    case OC_URL:
    default: break;
    }
    return 0;
}

/**************************************/

char*
ocerrstring(int err)
{
    if(err == 0) return "no error";
    if(err > 0) return strerror(err);
    switch (err) {
	case OC_EBADID:
	    return "OC_EBADID: Not a valid ID";
	case OC_EINVAL:
	    return "OC_EINVAL: Invalid argument";
	case OC_EPERM:
	    return "OC_EPERM: Write to read only";
	case OC_EINVALCOORDS:
	    return "OC_EINVALCOORDS: Index exceeds dimension bound";
	case OC_ENOTVAR:
	    return "OC_ENOTVAR: Variable not found";
	case OC_ECHAR:
	    return "OC_ECHAR: Attempt to convert between text & numbers";
	case OC_EEDGE:
	    return "OC_EEDGE: Start+count exceeds dimension bound";
	case OC_ESTRIDE:
	    return "OC_ESTRIDE: Illegal stride";
	case OC_ENOMEM:
	    return "OC_ENOMEM: Memory allocation (malloc) failure";
	case OC_EDIMSIZE:
	    return "OC_EDIMSIZE: Invalid dimension size";
	case OC_EDAP:
	    return "OC_EDAP: unspecified DAP failure";
	case OC_EXDR:
	    return "OC_EXDR: XDR failure";
	case OC_ECURL:
	    return "OC_ECURL: unspecified libcurl failure";
	case OC_EBADURL:
	    return "OC_EBADURL: malformed url";
	case OC_EBADVAR:
	    return "OC_EBADVAR: no such variable";
	case OC_EOPEN:
	    return "OC_EOPEN: temporary file open failed";	
	case OC_EIO:
	    return "OC_EIO: I/O failure";
	case OC_ENODATA:
	    return "OC_ENODATA: Variable has no data in DAP request";
	case OC_EDAPSVC:
	    return "OC_EDAPSVC: DAP Server error";
	case OC_ENAMEINUSE:
	    return "OC_ENAMEINUSE: Duplicate name in DDS";
	case OC_EDAS:
	    return "OC_EDAS: Malformed or unreadable DAS";
	case OC_EDDS:
	    return "OC_EDDS: Malformed or unreadable DDS";
	case OC_EDATADDS:
	    return "OC_EDATADDS: Malformed or unreadable DATADDS";
	case OC_ERCFILE:
	    return "OC_ERCFILE: Malformed,  unreadable, or bad value in the run-time configuration file";
	case OC_ENOFILE:
	    return "OC_ENOFILE: cannot read content of URL";

	/* oc_data related errors */
	case OC_EINDEX:
	    return "OC_EINDEX: index argument too large";
	case OC_EBADTYPE:
	    return "OC_EBADTYPE: argument of wrong OCtype";

	/* String concatenation overrun */
	case OC_EOVERRUN:
	    return "OC_EOVERRUN: internal concatenation failed";

	/* Authorization Error */
	case OC_EAUTH:
	    return "OC_EAUTH: authorization failure";

	/* Access Error */
	case OC_EACCESS:
	    return "OC_EACCESS: access failure";

	default: break;
    }
    return "<unknown error code>";
}

OCerror
ocsvcerrordata(OCstate* state, char** codep, char** msgp, long* httpp)
{
    if(codep) *codep = state->error.code;
    if(msgp) *msgp = state->error.message;
    if(httpp) *httpp = state->error.httpcode;
    return OC_NOERR;    
}

/* if we get OC_EDATADDS error, then try to capture any
   error message and log it; assumes that in this case,
   the datadds is not big.
*/
void
ocdataddsmsg(OCstate* state, OCtree* tree)
{
#define ERRCHUNK 1024
#define ERRFILL ' '
#define ERRTAG "Error {" 
    int i,j;
    size_t len;
    XXDR* xdrs;
    char* contents;
    off_t ckp;

    if(tree == NULL) return;
    /* get available space */
    xdrs = tree->data.xdrs;
    len = (size_t)xxdr_length(xdrs);
    if(len < strlen(ERRTAG))
	return; /* no room */
    ckp = xxdr_getpos(xdrs);
    xxdr_setpos(xdrs,(off_t)0);
    /* read the whole thing */
    contents = (char*)malloc(len+1);
    (void)xxdr_getbytes(xdrs,contents,(off_t)len);
    contents[len] = '\0';
    /* Look for error tag */
    for(i=0;i<len;i++) {
        if(ocstrncmp(contents+i,ERRTAG,strlen(ERRTAG))==0) {
	    /* log the error message */
	    /* Do a quick and dirty escape */
	    for(j=i;j<len;j++) {
	        int c = contents[i+j];
		if(c > 0 && (c < ' ' || c >= '\177'))
		    contents[i+j] = ERRFILL;
	    }
	    nclog(NCLOGERR,"DATADDS failure, possible message: '%s'\n",
			contents+i);
	    goto done;
	}
    }
    xxdr_setpos(xdrs,ckp);
done:
    return;
}

/* Given some set of indices [i0][i1]...[in] (where n == rank-1)
   and the maximum sizes, compute the linear offset
   for set of dimension indices.
*/
size_t
ocarrayoffset(size_t rank, size_t* sizes, const size_t* indices)
{
    unsigned int i;
    size_t count = 0;
    for(i=0;i<rank;i++) {
	count *= sizes[i];
	count += indices[i];
    }
    return count;
}

/* Inverse of ocarrayoffset: convert linear index to a set of indices */
void
ocarrayindices(size_t index, size_t rank, size_t* sizes, size_t* indices)
{
    for(size_t i = rank; i-->0;) {
        indices[i] = index % sizes[i];
        index = (index - indices[i]) / sizes[i];
    }
}

/* Given some set of edge counts [i0][i1]...[in] (where n == rank-1)
   and the maximum sizes, compute the linear offset
   for the last edge position
*/
size_t
ocedgeoffset(size_t rank, size_t* sizes, size_t* edges)
{
    unsigned int i;
    size_t count = 0;
    for(i=0;i<rank;i++) {
	count *= sizes[i];
	count += (edges[i]-1);
    }
    return count;
}

int
ocvalidateindices(size_t rank, size_t* sizes, size_t* indices)
{
    int i;
    for(i=0;i<rank;i++) {
	if(indices[i] >= sizes[i]) return 0;
    }
    return 1;
}

int
oc_ispacked(OCnode* node)
{
    OCtype octype = node->octype;
    OCtype etype = node->etype;
    int isscalar = (node->array.rank == 0);
    int packed;

    if(isscalar || octype != OC_Atomic)
	return 0; /* is not packed */
    packed = (etype == OC_Byte
	      || etype == OC_UByte
              || etype == OC_Char) ? 1 : 0;
    return packed;
}

/* Must be consistent with ocx.h.OCDT */
#define NMODES 6
#define MAXMODENAME 8 /*max (strlen(modestrings[i])) */
static const char* modestrings[NMODES+1] = {
"FIELD", /* ((OCDT)(1<<0)) field of a container */
"ELEMENT", /* ((OCDT)(1<<1)) element of a structure array */
"RECORD", /* ((OCDT)(1<<2)) record of a sequence */
"ARRAY", /* ((OCDT)(1<<3)) is structure array */
"SEQUENCE", /* ((OCDT)(1<<4)) is sequence */
"ATOMIC", /* ((OCDT)(1<<5)) is atomic leaf */
NULL,
};

char*
ocdtmodestring(OCDT mode,int compact)
{
    char* result = NULL;
    int i;
    char* p = NULL;
    size_t len = 1+(NMODES*(MAXMODENAME+1));

    result = malloc(len);
    if(result == NULL) return NULL;
    p = result;
    result[0] = '\0';
    if(mode == 0) {
	if(compact) *p++ = '-';
	else strlcat(result,"NONE",len);
    } else for(i=0;;i++) {
	const char* ms = modestrings[i];
	if(ms == NULL) break;
	if(!compact && i > 0)
	    strlcat(result,";",len);
        if(fisset(mode,(1<<i))) {
	    if(compact) *p++ = ms[0];
	    else strlcat(result,ms,len);
	}
    }
    /* pad compact list out to NMODES in length (+1 for null terminator) */
    if(compact) {
	while((p-result) < NMODES) *p++ = ' ';
	*p = '\0';
    }
    return result;
}


/*
Instead of using snprintf to concatenate
multiple strings into a given target,
provide a direct concatenator.
So, this function concats the n argument strings
and overwrites the contents of dst.
Care is taken to never overrun the available
space (the size parameter).
Note that size is assumed to include the null
terminator and that in the event of overrun,
the string will have a null at dst[size-1].
Return 0 if overrun, 1 otherwise.
*/
int
occopycat(char* dst, size_t size, size_t n, ...)
{
    va_list args;
    size_t avail = size - 1;
    int i; 
    int status = 1; /* assume ok */
    char* p = dst;

    if(n == 0) {
	if(size > 0)
	    dst[0] = '\0';
	return (size > 0 ? 1: 0);
    }
	
    va_start(args,n);
    for(i=0;i<n;i++) {
	char* q = va_arg(args, char*);
	for(;;) {
	    char c = *q++;
	    if(c == '\0') break;
	    if(avail == 0) {status = 0; goto done;}
	    *p++ = c;
	    avail--;
	}
    }
    /* make sure we null terminate;
       note that since avail was size-1, there
       will always be room
    */
    *p = '\0';    

done:
    va_end(args);
    return status;    
}

/*
Similar to occopycat, but
the n strings are, in effect,
concatenated and appended to the
current contents of dst.
The size parameter is the total size of dst,
including room for null terminator.
Return 0 if overrun, 1 otherwise.
*/
int
occoncat(char* dst, size_t size, size_t n, ...)
{
    va_list args;
    int status = 1; /* assume ok */
    size_t avail = 0;
    int i; 
    char* p;
    size_t dstused;
    dstused = strlen(dst);
    if(dstused >= size)
	return 0; /* There is no room to append */
    /* move to the end of the current contents of dst
       and act like we are doing copycat
    */
    p = dst + dstused;
    size -= dstused;
    avail = size - 1;
    if(n == 0) {
	if(size > 0)
	    p[0] = '\0';
	return (size > 0 ? 1: 0);
    }
	
    va_start(args,n);
    for(i=0;i<n;i++) {
	char* q = va_arg(args, char*);
	for(;;) {
	    char c = *q++;
	    if(c == '\0') break;
	    if(avail == 0) {status = 0; goto done;}
	    *p++ = c;
	    avail--;
	}
    }
    /* make sure we null terminate;
       note that since avail was size-1, there
       will always be room
    */
    *p = '\0';    

done:
    va_end(args);
    return status;    
}

/* merge two envv style lists */
char**
ocmerge(const char** list1, const char** list2)
{
    size_t l1, l2;
    char** merge;
    const char** p;
    for(l1=0,p=list1;*p;p++) {l1++;}
    for(l2=0,p=list2;*p;p++) {l2++;}
    merge = (char**)malloc(sizeof(char*)*(l1+l2+1));
    if(merge == NULL)
	return NULL;
    memcpy(merge,list1,sizeof(char*)*l1);
    memcpy(merge+l1,list2,sizeof(char*)*l2);
    merge[l1+l2] = NULL;
    return merge;    
}
