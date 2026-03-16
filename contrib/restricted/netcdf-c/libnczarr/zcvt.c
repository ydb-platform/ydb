/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"
#include <math.h>
#ifdef _MSC_VER
#include <crtdbg.h>
#endif

#include "isnan.h"

/*
Code taken directly from libdap4/d4cvt.c
*/

static const size_t ncz_type_size[NC_MAX_ATOMIC_TYPE+1] = {
0, /*NC_NAT*/
sizeof(char), /*NC_BYTE*/
sizeof(char), /*NC_CHAR*/
sizeof(short), /*NC_SHORT*/
sizeof(int), /*NC_INT*/
sizeof(float), /*NC_FLOAT*/
sizeof(double), /*NC_DOUBLE*/
sizeof(unsigned char), /*NC_UBYTE*/
sizeof(unsigned short), /*NC_USHORT*/
sizeof(unsigned int), /*NC_UINT*/
sizeof(long long), /*NC_INT64*/
sizeof(unsigned long long), /*NC_UINT64*/
sizeof(char *), /*NC_STRING*/
};

/* Forward */
static int typeid2jtype(nc_type typeid);
static int naninftest(const char* s, double* dcase, float* fcase);

#if 0
/* Convert a JSON value to a struct ZCVT value and also return the type */
int
NCZ_string2cvt(char* src, nc_type srctype, struct ZCVT* zcvt, nc_type* typeidp)
{
    int stat = NC_NOERR;
    nc_type dsttype = NC_NAT;

    assert(zcvt);
    
    /* Convert to a restricted set of values */
    switch (srctype) {
    case NC_BYTE: {
	zcvt->int64v = (signed long long)(*((signed char*)src));
	dsttype = NC_INT64;
	} break;
    case NC_UBYTE: {
	zcvt->uint64v = (unsigned long long)(*((unsigned char*)src));
	dsttype = NC_UINT64;
	} break;
    case NC_SHORT: {
	zcvt->int64v = (signed long long)(*((signed short*)src));
	dsttype = NC_INT64;
	} break;
    case NC_USHORT: {
	zcvt->uint64v = (unsigned long long)(*((unsigned short*)src));
	dsttype = NC_UINT64;
	} break;
    case NC_INT: {
	zcvt->int64v = (signed long long)(*((signed int*)src));
	dsttype = NC_INT64;
	} break;
    case NC_UINT: {
	zcvt->uint64v = (unsigned long long)(*((unsigned int*)src));
	dsttype = NC_UINT64;
	} break;
    case NC_INT64: {
	zcvt->int64v = (signed long long)(*((signed long long*)src));
	dsttype = NC_INT64;
	} break;
    case NC_UINT64: {
	zcvt->uint64v = (unsigned long long)(*((unsigned long long*)src));
	dsttype = NC_UINT64;
	} break;
    case NC_FLOAT: {
	zcvt->float64v = (double)(*((float*)src));
	dsttype = NC_DOUBLE;
	} break;
    case NC_DOUBLE: {
	dsttype = NC_DOUBLE;
	zcvt->float64v= (double)(*((double*)src));
	} break;
    case NC_STRING: {
	dsttype = NC_STRING;
	zcvt->strv= *((char**)src);
	} break;
    default: stat = NC_EINTERNAL; goto done;
    }
    if(typeidp) *typeidp = dsttype;
done:
    return stat;
}
#endif

/* Warning: not free returned zcvt.strv; it may point into a string in jsrc */
int
NCZ_json2cvt(const NCjson* jsrc, struct ZCVT* zcvt, nc_type* typeidp)
{
    int stat = NC_NOERR;
    nc_type srctype = NC_NAT;
    double naninf;
    float naninff;
    
    /* Convert the incoming jsrc to a restricted set of values */
    switch (NCJsort(jsrc)) {
    case NCJ_INT: /* convert to (u)int64 */
	if(NCJstring(jsrc)[0] == '-') {
	    if(sscanf(NCJstring(jsrc),"%lld",&zcvt->int64v) != 1)
		{stat = NC_EINVAL; goto done;}
	    srctype = NC_INT64;
	} else {
	    if(sscanf(NCJstring(jsrc),"%llu",&zcvt->uint64v) != 1)
		{stat = NC_EINVAL; goto done;}
	    srctype = NC_UINT64;
	}
	break;
    case NCJ_DOUBLE:
	switch (naninftest(NCJstring(jsrc),&naninf,&naninff)) {
	case NC_NAT:
	    if(sscanf(NCJstring(jsrc),"%lg",&zcvt->float64v) != 1)
	        {stat = NC_EINVAL; goto done;}
	    break;
	default:
	    zcvt->float64v = naninf;
	    break;
	}
	srctype = NC_DOUBLE;
	break;
    case NCJ_BOOLEAN:
	srctype = NC_UINT64;
	if(strcasecmp(NCJstring(jsrc),"false")==0)
	    zcvt->uint64v = 0;
	else
	    zcvt->uint64v = 1;
	break;
    case NCJ_STRING:
	srctype = NC_STRING;
	zcvt->strv = NCJstring(jsrc);
	break;
    default: stat = NC_EINTERNAL; goto done;
    }

    if(typeidp) *typeidp = srctype;
done:
    return stat;
}

/* Convert a singleton NCjson value to a memory equivalent value of specified dsttype; */
int
NCZ_convert1(const NCjson* jsrc, nc_type dsttype, NCbytes* buf)
{
    int stat = NC_NOERR;
    nc_type srctype;
    struct ZCVT zcvt = zcvt_empty;
    int outofrange = 0;
    size_t len = 0;
    double naninf;
    float naninff;

    assert(dsttype != NC_NAT && dsttype <= NC_MAX_ATOMIC_TYPE && buf);

    switch (NCJsort(jsrc)) {
    case NCJ_STRING: case NCJ_INT: case NCJ_DOUBLE: case NCJ_BOOLEAN:
        if((stat = NCZ_json2cvt(jsrc,&zcvt,&srctype))) goto done;
	break;
    default: stat = NC_EINVAL; goto done; /* Illegal JSON */
    }

    len = ncz_type_size[dsttype]; /* may change later */

    /* Now, do the down conversion */
    switch (dsttype) {
    case NC_BYTE: {
        signed char c = 0;
	switch (srctype) {
	case NC_DOUBLE:
	    zcvt.int64v = (long long)zcvt.float64v; /* Convert to int64 */
	    /* fall thru */
	case NC_INT64:
	    if(zcvt.int64v < NC_MIN_BYTE || zcvt.int64v > NC_MAX_BYTE) outofrange = 1;
	    c = (signed char)zcvt.int64v;
	    ncbytesappend(buf,(char)c);
	    break;
	case NC_UINT64:
	    if(zcvt.uint64v > NC_MAX_BYTE) outofrange = 1;
	    c = (signed char)zcvt.uint64v;
	    ncbytesappend(buf,(char)c);
	    break;
	default: abort();
	}
	} break;
    case NC_UBYTE: {
	unsigned char c = 0;
	switch (srctype) {
	case NC_DOUBLE:
	    zcvt.int64v = (long long)zcvt.float64v; /* Convert to int64 */
	    /* fall thru */
	case NC_INT64:
	    if(zcvt.int64v < 0 || zcvt.int64v > NC_MAX_BYTE) outofrange = 1;
	    c = (unsigned char)zcvt.int64v;
	    ncbytesappend(buf,(char)c);
	    break;
	case NC_UINT64:
	    if(zcvt.uint64v > NC_MAX_UBYTE) outofrange = 1;
	    c = (unsigned char)zcvt.uint64v;
	    ncbytesappend(buf,(char)c);
	    break;
	default: abort();
	}
	} break;
    case NC_SHORT: {
	signed short s = 0;
	switch (srctype) {
	case NC_DOUBLE:
	    zcvt.int64v = (long long)zcvt.float64v; /* Convert to int64 */
	    /* fall thru */
	case NC_INT64:
	    if(zcvt.int64v < NC_MIN_SHORT || zcvt.int64v > NC_MAX_SHORT) outofrange = 1;
	    s = (signed short)zcvt.int64v;
	    ncbytesappendn(buf,(char*)&s,sizeof(s));
	    break;
	case NC_UINT64:
	    if(zcvt.uint64v > NC_MAX_SHORT) outofrange = 1;
	    s = (signed short)zcvt.uint64v;
	    ncbytesappendn(buf,(char*)&s,sizeof(s));
	    break;
	default: abort();
	}
	} break;
    case NC_USHORT: {
	unsigned short s = 0;
	switch (srctype) {
	case NC_DOUBLE:
	    zcvt.int64v = (long long)zcvt.float64v; /* Convert to int64 */
	    /* fall thru */
	case NC_INT64:
	    if(zcvt.int64v < 0 || zcvt.int64v > NC_MAX_USHORT) outofrange = 1;
	    s = (unsigned short)zcvt.int64v;
	    ncbytesappendn(buf,(char*)&s,sizeof(s));
	    break;
	case NC_UINT64:
	    if(zcvt.uint64v > NC_MAX_USHORT) outofrange = 1;
	    s = (unsigned short)zcvt.uint64v;
	    ncbytesappendn(buf,(char*)&s,sizeof(s));
	    break;
	default: abort();
	}
	} break;
    case NC_INT: {
	signed int ii = 0;
	switch (srctype) {
	case NC_DOUBLE:
	    zcvt.int64v = (long long)zcvt.float64v; /* Convert to int64 */
	    /* fall thru */
	case NC_INT64:
	    if(zcvt.int64v < NC_MIN_INT || zcvt.int64v > NC_MAX_INT) outofrange = 1;
	    ii = (signed int)zcvt.int64v;
	    ncbytesappendn(buf,(char*)&ii,sizeof(ii));
	    break;
	case NC_UINT64:
	    if(zcvt.uint64v > NC_MAX_INT) outofrange = 1;
	    ii = (signed int)zcvt.uint64v;
	    ncbytesappendn(buf,(char*)&ii,sizeof(ii));
	    break;
	default: abort();
	}
	} break;
    case NC_UINT: {
	unsigned int ii = 0;
	switch (srctype) {
	case NC_DOUBLE:
	    zcvt.int64v = (long long)zcvt.float64v; /* Convert to int64 */
	    /* fall thru */
	case NC_INT64:
	    if(zcvt.int64v < 0 || zcvt.int64v > NC_MAX_UINT) outofrange = 1;
	    ii = (unsigned int)zcvt.int64v;
	    ncbytesappendn(buf,(char*)&ii,sizeof(ii));
	    break;
	case NC_UINT64:
	    if(zcvt.uint64v > NC_MAX_UINT) outofrange = 1;
	    ii = (unsigned int)zcvt.uint64v;
	    ncbytesappendn(buf,(char*)&ii,sizeof(ii));
	    break;
	default: abort();
	}
	} break;
    case NC_INT64: {
	signed long long ll = 0;
	switch (srctype) {
	case NC_DOUBLE:
	    zcvt.int64v = (long long)zcvt.float64v; /* Convert to int64 */
	    /* fall thru */
	case NC_INT64:
	    ll = (signed long long)zcvt.int64v;
	    ncbytesappendn(buf,(char*)&ll,sizeof(ll));
	    break;
	case NC_UINT64:
	    if(zcvt.uint64v > NC_MAX_INT64) outofrange = 1;
	    ll = (signed long long)zcvt.uint64v;
	    ncbytesappendn(buf,(char*)&ll,sizeof(ll));
	    break;
	default: abort();
	}
	} break;
    case NC_UINT64: {
	unsigned long long ll = 0;
	switch (srctype) {
	case NC_DOUBLE:
	    zcvt.int64v = (signed long long)zcvt.float64v;
	    /* fall thru */
	case NC_INT64:
	    if(zcvt.int64v < 0) outofrange = 1;
	    ll = (unsigned long long)zcvt.int64v;
	    ncbytesappendn(buf,(char*)&ll,sizeof(ll));
	    break;
	case NC_UINT64:
	    ll = (unsigned long long)zcvt.uint64v;
	    ncbytesappendn(buf,(char*)&ll,sizeof(ll));
	    break;
	default: abort();
	}
	} break;
    case NC_FLOAT: {
	float f = 0;
	switch (srctype) {
	case NC_DOUBLE:
	    f = (float)zcvt.float64v;
	    break;
	case NC_INT64:
	    f = (float)zcvt.int64v;
	    break;
	case NC_UINT64:
	    f = (float)zcvt.uint64v;
	    break;
	case NC_STRING: /* Detect special constants encoded as strings e.g. "Nan" */
	    switch (naninftest(zcvt.strv,&naninf,&naninff)) {
	    case NC_NAT: abort();
    	    case NC_FLOAT:
       	    case NC_DOUBLE:
	        f = naninff; break;
		break;
	    }
	    break;
	default: abort();
	}
        ncbytesappendn(buf,(char*)&f,sizeof(f));
	} break;
    case NC_DOUBLE: {
	double d = 0;
	switch (srctype) {
	case NC_DOUBLE:
	    d = (double)zcvt.float64v;
	    break;
	case NC_INT64:
	    d = (double)zcvt.int64v;
	    break;
    	case NC_UINT64:
	    d = (double)zcvt.uint64v;
	    break;
	case NC_STRING: /* NaN might be quoted */
	    switch (naninftest(zcvt.strv,&naninf,&naninff)) {
	    case NC_NAT: abort();
    	    case NC_FLOAT:
       	    case NC_DOUBLE:
	        d = naninf; break;
		break;
	    }
	    break;
	default: abort();
	}
        ncbytesappendn(buf,(char*)&d,sizeof(d));
	} break;
    case NC_STRING: {
	char* scopy = NULL;
	if(srctype != NC_STRING) {stat = NC_EINVAL; goto done;}
	/* Need to append the pointer and not what it points to */
	scopy = nulldup(zcvt.strv);
	ncbytesappendn(buf,(void*)&scopy,sizeof(scopy));
	scopy = NULL;
	} break;
    case NC_CHAR: {
	char digits[64];
	switch (srctype) {
	case NC_DOUBLE:
	    snprintf(digits,sizeof(digits),"%lf",(double)zcvt.float64v);
	    ncbytesappendn(buf,digits,strlen(digits));
	    break;
	case NC_INT64:
	    snprintf(digits,sizeof(digits),"%lli",(long long)zcvt.int64v);
	    ncbytesappendn(buf,digits,strlen(digits));
    	case NC_UINT64:
	    snprintf(digits,sizeof(digits),"%lli",(unsigned long long)zcvt.uint64v);
	    ncbytesappendn(buf,digits,strlen(digits));
	    break;
	case NC_STRING: 
	    len = strlen(zcvt.strv);
	    ncbytesappendn(buf,zcvt.strv,len);
	default: abort();
	}	
	} break;
    default: stat = NC_EINTERNAL; goto done;
    }

done:
    if(stat == NC_NOERR && outofrange) stat = NC_ERANGE;
    return stat;
}

/* Convert a memory value to a JSON string value */
int
NCZ_stringconvert1(nc_type srctype, char* src, NCjson* jvalue)
{
    int stat = NC_NOERR;
    struct ZCVT zcvt;
    nc_type dsttype = NC_NAT;
    char s[1024];
    char sq[1024+2+1];
    char* p = NULL;
    int isnanorinf = 0;

    assert(srctype >= NC_NAT && srctype != NC_CHAR && srctype <= NC_STRING);    
    /* Convert to a restricted set of values */
    switch (srctype) {
    case NC_BYTE: {
	zcvt.int64v = (signed long long)(*((signed char*)src));
	dsttype = NC_INT64;
	} break;
    case NC_UBYTE: {
	zcvt.uint64v = (unsigned long long)(*((unsigned char*)src));
	dsttype = NC_UINT64;
	} break;
    case NC_SHORT: {
	zcvt.int64v = (signed long long)(*((signed short*)src));
	dsttype = NC_INT64;
	} break;
    case NC_USHORT: {
	zcvt.uint64v = (unsigned long long)(*((unsigned short*)src));
	dsttype = NC_UINT64;
	} break;
    case NC_INT: {
	zcvt.int64v = (signed long long)(*((signed int*)src));
	dsttype = NC_INT64;
	} break;
    case NC_UINT: {
	zcvt.uint64v = (unsigned long long)(*((unsigned int*)src));
	dsttype = NC_UINT64;
	} break;
    case NC_INT64: {
	zcvt.int64v = (signed long long)(*((signed long long*)src));
	dsttype = NC_INT64;
	} break;
    case NC_UINT64: {
	zcvt.uint64v = (unsigned long long)(*((unsigned long long*)src));
	dsttype = NC_UINT64;
	} break;
    case NC_FLOAT: {
	zcvt.float64v = (double)(*((float*)src));
	dsttype = NC_DOUBLE;
	} break;
    case NC_DOUBLE: {
	dsttype = NC_DOUBLE;
	zcvt.float64v= (double)(*((double*)src));
	} break;
    case NC_STRING: {
	dsttype = NC_STRING;
	zcvt.strv= *((char**)src);
	} break;
    default: stat = NC_EINTERNAL; goto done;
    }

    /* Convert from restricted set of values to standardized string form*/
    switch (dsttype) {
    case NC_INT64: {
	snprintf(s,sizeof(s),"%lld",zcvt.int64v);
	} break;
    case NC_UINT64: {
	snprintf(s,sizeof(s),"%llu",zcvt.uint64v);
	} break;
    case NC_DOUBLE: {
#ifdef _MSC_VER
	switch (_fpclass(zcvt.float64v)) {
	case _FPCLASS_SNAN: case _FPCLASS_QNAN:
	     strcpy(s,"NaN"); isnanorinf = 1; break;
	case _FPCLASS_NINF:
	     strcpy(s,"-Infinity"); isnanorinf = 1; break;
	case _FPCLASS_PINF:
	     strcpy(s,"Infinity"); isnanorinf = 1; break;
	default:
	      snprintf(s,sizeof(s),"%lg",zcvt.float64v); /* handles NAN? */
	      break;
	}
#else
	if(isnan(zcvt.float64v))
	     {strcpy(s,"NaN"); isnanorinf = 1;}
	else if(isinf(zcvt.float64v) && zcvt.float64v < 0)
	     {strcpy(s,"-Infinity"); isnanorinf = 1;}
	else if(isinf(zcvt.float64v) && zcvt.float64v > 0)
	     {strcpy(s,"Infinity"); isnanorinf = 1;}
	else {
             snprintf(s,sizeof(s),"%lg",zcvt.float64v); /* handles NAN? */
	}
#endif
	/* Quote the nan/inf constant */
	if(isnanorinf) {
	    size_t l = strlen(s);
	    memcpy(sq,s,l+1);
	    s[0] = '"';
	    memcpy(s+1,sq,l);
	    s[l+1] = '"';
    	    s[l+2] = '\0';
	}
	} break;
    case NC_STRING: {
	p = nulldup(zcvt.strv);
        } break;
    default: stat = NC_EINTERNAL; goto done;
    }
    if(p == NULL)
	p = strdup(s);
    NCJsetstring(jvalue,p);
    p = NULL;
done:
    nullfree(p);
    return stat;
}

/* Convert arbitrary netcdf attribute vector to equivalent JSON */
int
NCZ_stringconvert(nc_type typeid, size_t len, void* data0, NCjson** jdatap)
{
    int stat = NC_NOERR;
    size_t i;
    char* src = data0; /* so we can do arithmetic on it */
    size_t typelen;
    char* str = NULL;
    NCjson* jvalue = NULL;
    NCjson* jdata = NULL;
    int jtype = NCJ_UNDEF;

    jtype = typeid2jtype(typeid);

    if((stat = NC4_inq_atomic_type(typeid, NULL, &typelen)))
	goto done;

    /* Handle char type specially */
    if(typeid == NC_CHAR) {
	/* Apply the JSON write convention */
        if((stat = NCJparsen(len,src,0,&jdata))) { /* !parseable */
  	    /* Create a string valued json object */
	    if((stat = NCJnewstringn(NCJ_STRING,len,src,&jdata))) goto done;
	}
    } else if(len == 1) { /* create singleton */
	if((stat = NCJnew(jtype,&jdata))) goto done;
        if((stat = NCZ_stringconvert1(typeid, src, jdata))) goto done;
    } else { /* len > 1 create array of values */
	if((stat = NCJnew(NCJ_ARRAY,&jdata))) goto done;
	for(i=0;i<len;i++) {
	    if((stat = NCJnew(jtype,&jvalue))) goto done;
	    if((stat = NCZ_stringconvert1(typeid, src, jvalue))) goto done;
	    NCJappend(jdata,jvalue);
	    jvalue = NULL;
	    src += typelen;
	}
    }
    if(jdatap) {*jdatap = jdata; jdata = NULL;}

done:
    nullfree(str);
    NCJreclaim(jvalue);
    NCJreclaim(jdata);
    return stat;
}

static int
typeid2jtype(nc_type typeid)
{
    switch (typeid) {
    case NC_BYTE: case NC_SHORT: case NC_INT: case NC_INT64:
    case NC_UBYTE: case NC_USHORT: case NC_UINT: case NC_UINT64:
	return NCJ_INT;
    case NC_FLOAT:
    case NC_DOUBLE:
	return NCJ_DOUBLE;
    case NC_CHAR:
    case NC_STRING:
	return NCJ_STRING;
    default: break;
    }
    return NCJ_UNDEF;
}

/* Test for Nan(f) and Inf(f)
   return 0 if not nan or inf
   return NC_FLOAT if nanf or inff
   return NC_DOUBLE if nan or inf
   Always fill in both double and float cases so caller can choose
*/
static int
naninftest(const char* s, double* dcase, float* fcase)
{
    nc_type nctype= NC_NAT;
    assert(dcase && fcase);
    if(strcasecmp(s,"nan")==0) {
	*dcase = NAN; *fcase = NANF;
	nctype = NC_DOUBLE;
    } else if(strcasecmp(s,"-nan")==0) {
	*dcase = (- NAN); *fcase = (- NANF);
	nctype = NC_DOUBLE;
    } else if(strcasecmp(s,"nanf")==0) {
	*dcase = NAN; *fcase = NANF;
	nctype = NC_FLOAT;
    } else if(strcasecmp(s,"-nan")==0) {
	*dcase = (- NAN); *fcase = (- NANF);
	nctype = NC_FLOAT;
    } else if(strcasecmp(s,"infinity")==0) {
	*dcase = INFINITY; *fcase = INFINITYF;
	nctype = NC_DOUBLE;
    } else if(strcasecmp(s,"-infinity")==0) {
	*dcase = (- INFINITY); *fcase = (- INFINITYF);
	nctype = NC_DOUBLE;
    } else if(strcasecmp(s,"infinityf")==0) {
	*dcase = INFINITY; *fcase = INFINITYF;
	nctype = NC_FLOAT;
    } else if(strcasecmp(s,"-infinityf")==0) {
	*dcase = (- INFINITY); *fcase = (- INFINITYF);
	nctype = NC_FLOAT;
    }    
    return nctype;
}
