/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

/**
 * @file
 * @internal Misc. utility code
 *
 * @author Dennis Heimbigner
 */

#include "zincludes.h"
#include <stddef.h>

#undef DEBUG

/**************************************************/
/* Static zarr type name table */

/* Table of nc_type X {Zarr,NCZarr} X endianness
Issue: Need to distinguish NC_STRING && MAXSTRLEN==1 from NC_CHAR
in a way that allows other Zarr implementations to read the data.

Available info:
Write: we have the netcdf type, so there is no ambiguity.
Read: we have the variable type and also any attribute dtype,
but those types are ambiguous.
We also have the attribute vs variable type problem.
For pure zarr, we have to infer the type of an attribute,
so if we have "var:strattr = \"abcdef\"", then we need
to decide how to infer the type: NC_STRING vs NC_CHAR.

Solution:
For variables and for NCZarr type attributes, distinguish by using:
* ">S1" for NC_CHAR.
* "|S1" for NC_STRING && MAXSTRLEN==1
* "|Sn" for NC_STRING && MAXSTRLEN==n
This is admittedly a bit of a hack, and the first case in particular
will probably cause errors in some other Zarr implementations; the Zarr
spec is unclear about what combinations are legal.
Note that we could use "|U1", but since this is utf-16 or utf-32
in python, it may cause problems when reading what amounts to utf-8.

For attributes, we infer:
* NC_CHAR if the hint is 0
  - e.g. var:strattr = 'abcdef'" => NC_CHAR
* NC_STRING if hint is NC_STRING.
  - e.g. string var:strattr = \"abc\", \"def\"" => NC_STRING

Note also that if we read a pure zarr file we will probably always
see "|S1", so we will never see a variable of type NC_CHAR.
We might however see an attribute of type string.
*/
static const struct ZTYPES {
    char* zarr[3];
    char* nczarr[3];
} znames[NUM_ATOMIC_TYPES] = {
/* nc_type          Pure Zarr          NCZarr
                   NE   LE     BE       NE    LE    BE*/
/*NC_NAT*/	{{NULL,NULL,NULL},   {NULL,NULL,NULL}},
/*NC_BYTE*/	{{"|i1","<i1",">i1"},{"|i1","<i1",">i1"}},
/*NC_CHAR*/	{{">S1",">S1",">S1"},{">S1",">S1",">S1"}},
/*NC_SHORT*/	{{"|i2","<i2",">i2"},{"|i2","<i2",">i2"}},
/*NC_INT*/	{{"|i4","<i4",">i4"},{"|i4","<i4",">i4"}},
/*NC_FLOAT*/	{{"|f4","<f4",">f4"},{"|f4","<f4",">f4"}},
/*NC_DOUBLE*/	{{"|f8","<f8",">f8"},{"|f8","<f8",">f8"}},
/*NC_UBYTE*/	{{"|u1","<u1",">u1"},{"|u1","<u1",">u1"}},
/*NC_USHORT*/	{{"|u2","<u2",">u2"},{"|u2","<u2",">u2"}},
/*NC_UINT*/	{{"|u4","<u4",">u4"},{"|u4","<u4",">u4"}},
/*NC_INT64*/	{{"|i8","<i8",">i8"},{"|i8","<i8",">i8"}},
/*NC_UINT64*/	{{"|u8","<u8",">u8"},{"|u8","<u8",">u8"}},
/*NC_STRING*/	{{"|S%d","|S%d","|S%d"},{"|S%d","|S%d","|S%d"}},
};

#if 0
static const char* zfillvalue[NUM_ATOMIC_TYPES] = {
NULL, /*NC_NAT*/
"-127", /*NC_BYTE*/
"0", /*NC_CHAR*/
"-32767", /*NC_SHORT*/
"-2147483647", /*NC_INT*/
"9.9692099683868690e+36f", /* near 15 * 2^119 */ /*NC_FLOAT*/
"9.9692099683868690e+36", /*NC_DOUBLE*/
"255", /*NC_UBYTE*/
"65535", /*NC_USHORT*/
"4294967295", /*NC_UINT*/
"-9223372036854775806", /*NC_INT64*/
"18446744073709551614", /*NC_UINT64*/
"", /*NC_STRING*/
};
#endif

/* map nc_type -> NCJ_SORT */
static int zjsonsort[NUM_ATOMIC_TYPES] = {
NCJ_UNDEF, /*NC_NAT*/
NCJ_INT, /*NC_BYTE*/
NCJ_INT, /*NC_CHAR*/
NCJ_INT, /*NC_SHORT*/
NCJ_INT, /*NC_INT*/
NCJ_DOUBLE, /*NC_FLOAT*/
NCJ_DOUBLE, /*NC_DOUBLE*/
NCJ_INT, /*NC_UBYTE*/
NCJ_INT, /*NC_USHORT*/
NCJ_INT, /*NC_UINT*/
NCJ_INT, /*NC_INT64*/
NCJ_INT, /*NC_UINT64*/
NCJ_STRING, /*NC_STRING*/
};

/* Forward */

/**************************************************/

/**
@internal Get key for a group
@param grp - [in] group
@param pathp - [out] full path
@return NC_NOERR
@author Dennis Heimbigner
*/
int
NCZ_grpkey(const NC_GRP_INFO_T* grp, char** pathp)
{
    int stat = NC_NOERR;
    NClist* segments = nclistnew();
    NCbytes* path = NULL;
    NC_GRP_INFO_T* parent = NULL;
    size_t i;

    nclistinsert(segments,0,(void*)grp);
    parent = grp->parent;
    while(parent != NULL) {
        nclistinsert(segments,0,parent);
        parent = parent->parent;
    }
    path = ncbytesnew();
    for(i=0;i<nclistlength(segments);i++) {
	grp = nclistget(segments,i);
	if(i > 1) ncbytescat(path,"/"); /* Assume root is named "/" */
	ncbytescat(path,grp->hdr.name);
    }        
    if(pathp) *pathp = ncbytesextract(path);

    nclistfree(segments);
    ncbytesfree(path);
    return stat;

}

/**
@internal Get key for a var
@param var - [in] var
@param pathp - [out] full path
@return NC_NOERR
@author Dennis Heimbigner
*/
int
NCZ_varkey(const NC_VAR_INFO_T* var, char** pathp)
{
    int stat = NC_NOERR;
    char* grppath = NULL;
    char* varpath = NULL;

    /* Start by creating the full path for the parent group */
    if((stat = NCZ_grpkey(var->container,&grppath)))
	goto done;
    /* Create the suffix path using the var name */
    if((stat = nczm_concat(grppath,var->hdr.name,&varpath)))
	goto done;
    /* return path */
    if(pathp) {*pathp = varpath; varpath = NULL;}

done:
    nullfree(grppath);
    nullfree(varpath);
    return stat;
}

/**
@internal Get key for a dimension
@param dim - [in] dim
@param pathp - [out] full path
@return NC_NOERR
@author Dennis Heimbigner
*/
int
NCZ_dimkey(const NC_DIM_INFO_T* dim, char** pathp)
{
    int stat = NC_NOERR;
    char* grppath = NULL;
    char* dimpath = NULL;

    /* Start by creating the full path for the parent group */
    if((stat = NCZ_grpkey(dim->container,&grppath)))
	goto done;
    /* Create the suffix path using the dim name */
    if((stat = nczm_concat(grppath,dim->hdr.name,&dimpath)))
	goto done;
    /* return path */
    if(pathp) {*pathp = dimpath; dimpath = NULL;}

done:
    nullfree(grppath);
    nullfree(dimpath);
    return stat;
}

/**
@internal Split a key into pieces along '/' character; elide any leading '/'
@param  key - [in]
@param segments - [out] split path
@return NC_NOERR
@author Dennis Heimbigner
*/
int
ncz_splitkey(const char* key, NClist* segments)
{
    return nczm_split(key,segments);
}

/**************************************************/
/* Json sync code */

/**
@internal Down load a .z... structure into memory
@param zmap - [in] controlling zarr map
@param key - [in] .z... object to load
@param jsonp - [out] root of the loaded json (NULL if key does not exist)
@return NC_NOERR
@return NC_EXXX
@author Dennis Heimbigner
*/
int
NCZ_downloadjson(NCZMAP* zmap, const char* key, NCjson** jsonp)
{
    int stat = NC_NOERR;
    size64_t len;
    char* content = NULL;
    NCjson* json = NULL;

    switch(stat = nczmap_len(zmap, key, &len)) {
    case NC_NOERR: break;
    case NC_ENOOBJECT: case NC_EEMPTY:
        stat = NC_NOERR;
        goto exit;
    default: goto done;
    }
    if((content = malloc(len+1)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    if((stat = nczmap_read(zmap, key, 0, len, (void*)content)))
	goto done;
    content[len] = '\0';
    if((stat = NCJparse(content,0,&json)) < 0)
	{stat = NC_ENCZARR; goto done;}

exit:
    if(jsonp) {*jsonp = json; json = NULL;}

done:
    NCJreclaim(json);
    nullfree(content);
    return stat;
}

/**
@internal  Upload a modified json tree to a .z... structure.
@param zmap - [in] controlling zarr map
@param key - [in] .z... object to load
@param json - [in] root of the json tree
@return NC_NOERR
@author Dennis Heimbigner
*/
int
NCZ_uploadjson(NCZMAP* zmap, const char* key, const NCjson* json)
{
    int stat = NC_NOERR;
    char* content = NULL;

    ZTRACE(4,"zmap=%p key=%s",zmap,key);

#ifdef DEBUG
fprintf(stderr,"uploadjson: %s\n",key); fflush(stderr);
#endif
    /* Unparse the modified json tree */
    if((stat = NCJunparse(json,0,&content)))
	goto done;
    ZTRACEMORE(4,"\tjson=%s",content);
    
if(getenv("NCS3JSON") != NULL)
fprintf(stderr,">>>> uploadjson: %s: %s\n",key,content);

    /* Write the metadata */
    if((stat = nczmap_write(zmap, key, strlen(content), content)))
	goto done;

done:
    nullfree(content);
    return ZUNTRACE(stat);
}

#if 0
/**
@internal create object, return empty dict; ok if already exists.
@param zmap - [in] map
@param key - [in] key of the object
@param jsonp - [out] return parsed json
@return NC_NOERR
@return NC_EINVAL if object exists
@author Dennis Heimbigner
*/
int
NCZ_createdict(NCZMAP* zmap, const char* key, NCjson** jsonp)
{
    int stat = NC_NOERR;
    NCjson* json = NULL;

    /* See if it already exists */
    if((stat = NCZ_downloadjson(zmap,key,&json))) goto done;
    ifjson == NULL) {
	if((stat = nczmap_def(zmap,key,NCZ_ISMETA))) goto done;	    
    } else {
	/* Already exists, fail */
	stat = NC_EINVAL;
	goto done;
    }
    /* Create the empty dictionary */
    if((stat = NCJnew(NCJ_DICT,&json)))
	goto done;
    if(jsonp) {*jsonp = json; json = NULL;}
done:
    NCJreclaim(json);
    return stat;
}

/**
@internal create object, return empty array; ok if already exists.
@param zmap - [in] map
@param key - [in] key of the object
@param jsonp - [out] return parsed json
@return NC_NOERR
@return NC_EINVAL if object exits
@author Dennis Heimbigner
*/
int
NCZ_createarray(NCZMAP* zmap, const char* key, NCjson** jsonp)
{
    int stat = NC_NOERR;
    NCjson* json = NULL;

    if((stat = NCZ_downloadjson(zmap,key,&json))) goto done;
    if(json == NULL) { /* create it */
	if((stat = nczmap_def(zmap,key,NCZ_ISMETA))) goto done;	    
        /* Create the initial array */
	if((stat = NCJnew(NCJ_ARRAY,&json))) goto done;
    } else {
	 stat = NC_EINVAL;
	goto done;
    }
    if(json->sort != NCJ_ARRAY) {stat = NC_ENCZARR; goto done;}
    if(jsonp) {*jsonp = json; json = NULL;}
done:
    NCJreclaim(json);
    return stat;
}
#endif /*0*/

#if 0
/**
@internal Given an nc_type, produce the corresponding
default fill value as a string.
@param nctype - [in] nc_type
@param defaltp - [out] pointer to hold pointer to the value
@return NC_NOERR
@author Dennis Heimbigner
*/

int
ncz_default_fill_value(nc_type nctype, const char** dfaltp)
{
    if(nctype <= 0 || nctype > NC_MAX_ATOMIC_TYPE) return NC_EINVAL;
    if(dfaltp) *dfaltp = zfillvalue[nctype];
    return NC_NOERR;	        
}
#endif

/**
@internal Given an nc_type, produce the corresponding
fill value JSON type
@param nctype - [in] nc_type
@param sortp - [out] pointer to hold pointer to the JSON type
@return NC_NOERR
@author Dennis Heimbigner
*/

int
ncz_fill_value_sort(nc_type nctype, int* sortp)
{
    if(nctype <= 0 || nctype > NC_MAX_ATOMIC_TYPE) return NC_EINVAL;
    if(sortp) *sortp = zjsonsort[nctype];
    return NC_NOERR;	        
}

/*
Given a path to a group, return the list of objects
that contain another object with the name of the tag.
For example, we can get the immediate list of subgroups
by using the tag ".zgroup".
Basically we return the set of X where <prefix>/X/<tag>
is an object in the map.
Note: need to test with "/", "", and with and without trailing "/".
*/
int
NCZ_subobjects(NCZMAP* map, const char* prefix, const char* tag, char dimsep, NClist* objlist)
{
    size_t i;
    int stat = NC_NOERR;
    NClist* matches = nclistnew();
    NCbytes* path = ncbytesnew();

    /* Get the list of names just below prefix */
    if((stat = nczmap_search(map,prefix,matches))) goto done;
    for(i=0;i<nclistlength(matches);i++) {
	const char* name = nclistget(matches,i);
	size_t namelen= strlen(name);	
	/* Ignore keys that start with .z or .nc or a potential chunk name */
	if(namelen >= 3 && name[0] == '.' && name[1] == 'n' && name[2] == 'c')
	    continue;
	if(namelen >= 2 && name[0] == '.' && name[1] == 'z')
	    continue;
	if(NCZ_ischunkname(name,dimsep))
	    continue;
	/* Create <prefix>/<name>/<tag> and see if it exists */
	ncbytesclear(path);
	ncbytescat(path,prefix);
	ncbytescat(path,"/");
	ncbytescat(path,name);
	ncbytescat(path,tag);
	/* See if this object exists */
        if((stat = nczmap_exists(map,ncbytescontents(path))) == NC_NOERR)
	    nclistpush(objlist,name);
    }

done:
    nclistfreeall(matches);
    ncbytesfree(path);
    return stat;
}

#if 0
/* Convert a netcdf-4 type integer */
int
ncz_nctypedecode(const char* snctype, nc_type* nctypep)
{
    unsigned nctype = 0;
    if(sscanf(snctype,"%u",&nctype)!=1) return NC_EINVAL;
    if(nctypep) *nctypep = nctype;
    return NC_NOERR;
}
#endif

/**
@internal Given an nc_type+other, produce the corresponding dtype string.
@param nctype     - [in] nc_type
@param endianness - [in] endianness
@param purezarr   - [in] 1=>pure zarr, 0 => nczarr
@param strlen     - [in] max string length
@param namep      - [out] pointer to hold pointer to the dtype; user frees
@return NC_NOERR
@return NC_EINVAL
@author Dennis Heimbigner
*/

int
ncz_nctype2dtype(nc_type nctype, int endianness, int purezarr, int len, char** dnamep)
{
    char dname[64];
    char* format = NULL;

    if(nctype <= NC_NAT || nctype > NC_MAX_ATOMIC_TYPE) return NC_EINVAL;
    if(purezarr)
        format = znames[nctype].zarr[endianness];
    else
        format = znames[nctype].nczarr[endianness];
    snprintf(dname,sizeof(dname),format,len);
    if(dnamep) *dnamep = strdup(dname);
    return NC_NOERR;		
}

/*
@internal Convert a numcodecs dtype spec to a corresponding nc_type.
@param nctype   - [in] dtype the dtype to convert
@param nctype   - [in] typehint help disambiguate char vs string
@param purezarr - [in] 1=>pure zarr, 0 => nczarr
@param nctypep  - [out] hold corresponding type
@param endianp  - [out] hold corresponding endianness
@param typelenp - [out] hold corresponding type size (for fixed length strings)
@return NC_NOERR
@return NC_EINVAL
@author Dennis Heimbigner
*/

int
ncz_dtype2nctype(const char* dtype, nc_type typehint, int purezarr, nc_type* nctypep, int* endianp, int* typelenp)
{
    int stat = NC_NOERR;
    int typelen = 0;
    int count;
    char tchar;
    nc_type nctype = NC_NAT;
    int endianness = -1;
    const char* p;
    int n;

    if(endianp) *endianp = NC_ENDIAN_NATIVE;
    if(nctypep) *nctypep = NC_NAT;

    if(dtype == NULL) goto zerr;
    p = dtype;
    switch (*p++) {
    case '<': endianness = NC_ENDIAN_LITTLE; break;
    case '>': endianness = NC_ENDIAN_BIG; break;
    case '|': endianness = NC_ENDIAN_NATIVE; break;
    default: p--; endianness = NC_ENDIAN_NATIVE; break;
    }
    tchar = *p++; /* get the base type */
    /* Decode the type length */
    count = sscanf(p,"%d%n",&typelen,&n);
    if(count == 0) goto zerr;
    p += n;

    /* Short circuit fixed length strings */
    if(tchar == 'S') {
	/* Fixed length string */
	switch (typelen) {
	case 1:
	    nctype = (endianness == NC_ENDIAN_BIG ? NC_CHAR : NC_STRING);
	    if(purezarr) nctype = NC_STRING; /* Zarr has no NC_CHAR type */
	    break;
	default:
	    nctype = NC_STRING;
	    break;
	}
	/* String/char have no endianness */
	endianness = NC_ENDIAN_NATIVE;
    } else {
	switch(typelen) {
        case 1:
	    switch (tchar) {
  	    case 'i': nctype = NC_BYTE; break;
   	    case 'u': nctype = NC_UBYTE; break;
	    default: goto zerr;
	    }
	    break;
        case 2:
	switch (tchar) {
	case 'i': nctype = NC_SHORT; break;
	case 'u': nctype = NC_USHORT; break;
	default: goto zerr;
	}
	break;
        case 4:
	switch (tchar) {
	case 'i': nctype = NC_INT; break;
	case 'u': nctype = NC_UINT; break;
	case 'f': nctype = NC_FLOAT; break;
	default: goto zerr;
	}
	break;
        case 8:
	switch (tchar) {
	case 'i': nctype = NC_INT64; break;
	case 'u': nctype = NC_UINT64; break;
	case 'f': nctype = NC_DOUBLE; break;
	default: goto zerr;
	}
	break;
        default: goto zerr;
        }
    }

#if 0
    /* Convert NC_ENDIAN_NATIVE and NC_ENDIAN_NA */
    if(endianness == NC_ENDIAN_NATIVE)
        endianness = (NC_isLittleEndian()?NC_ENDIAN_LITTLE:NC_ENDIAN_BIG);
#endif

    if(nctypep) *nctypep = nctype;
    if(typelenp) *typelenp = typelen;
    if(endianp) *endianp = endianness;

done:
    return stat;
zerr:
    stat = NC_ENCZARR;
    goto done;
}

/* Infer the attribute's type based
primarily on the first atomic value encountered
recursively.
*/
int
NCZ_inferattrtype(const NCjson* value, nc_type typehint, nc_type* typeidp)
{
    int i,stat = NC_NOERR;
    nc_type typeid;
    NCjson* j = NULL;
    unsigned long long u64;
    long long i64;
    int negative = 0;

    if(NCJsort(value) == NCJ_ARRAY && NCJarraylength(value) == 0)
        {typeid = NC_NAT; goto done;} /* Empty array is illegal */

    if(NCJsort(value) == NCJ_NULL)
        {typeid = NC_NAT; goto done;} /* NULL is also illegal */

    if(NCJsort(value) == NCJ_DICT) /* Complex JSON expr -- a dictionary */
        {typeid = NC_NAT; goto done;}

    /* If an array, make sure all the elements are simple */
    if(value->sort == NCJ_ARRAY) {
	for(i=0;i<NCJarraylength(value);i++) {
	    j=NCJith(value,i);
	    if(!NCJisatomic(j))
	        {typeid = NC_NAT; goto done;}
	}
    }

    /* Infer from first element */
    if(value->sort == NCJ_ARRAY) {
        j=NCJith(value,0);
	return NCZ_inferattrtype(j,typehint,typeidp);
    }

    /* At this point, value is a primitive JSON Value */

    switch (NCJsort(value)) {
    case NCJ_NULL:
        typeid = NC_NAT;
	return NC_NOERR;
    case NCJ_DICT:
    	typeid = NC_CHAR;
	goto done;
    case NCJ_UNDEF:
	return NC_EINVAL;
    default: /* atomic */
	break;
    }

    if(NCJstring(value) != NULL)
        negative = (NCJstring(value)[0] == '-');
    switch (value->sort) {
    case NCJ_INT:
	if(negative) {
	    sscanf(NCJstring(value),"%lld",&i64);
	    u64 = (unsigned long long)i64;
	} else
	    sscanf(NCJstring(value),"%llu",&u64);
	typeid = NCZ_inferinttype(u64,negative);
	break;
    case NCJ_DOUBLE:
	typeid = NC_DOUBLE;
	break;
    case NCJ_BOOLEAN:
	typeid = NC_UBYTE;
	break;
    case NCJ_STRING: /* requires special handling as an array of characters */
	typeid = NC_CHAR;
	break;
    default:
	stat = NC_ENCZARR;
    }
done:
    if(typeidp) *typeidp = typeid;
    return stat;
}

/* Infer the int type from the value;
   minimum type will be int.
*/
int
NCZ_inferinttype(unsigned long long u64, int negative)
{
    long long i64 = (long long)u64; /* keep bit pattern */
    if(!negative && u64 >= NC_MAX_INT64) return NC_UINT64;
    if(i64 < 0) {
	if(i64 >= NC_MIN_INT) return NC_INT;
	return NC_INT64;
    }
    if(i64 <= NC_MAX_INT) return NC_INT;
    if(i64 <= NC_MAX_UINT) return NC_UINT;
    return NC_INT64;
}
 
/**
@internal Similar to NCZ_grppath, but using group ids.
@param gid - [in] group id
@param pathp - [out] full path
@return NC_NOERR
@author Dennis Heimbigner
*/
int
NCZ_grpname_full(int gid, char** pathp)
{
    int stat = NC_NOERR;
    size_t len;
    char* path = NULL;

    if((stat = nc_inq_grpname_full(gid,&len,NULL))) return stat;
    if((path=malloc(len+1)) == NULL) return NC_ENOMEM;    
    if((stat = nc_inq_grpname_full(gid,&len,path))) return stat;
    path[len] = '\0'; /* ensure null terminated */
    if(pathp) {*pathp = path; path = NULL;}
    return stat;
}

/**
@internal Parse a commified string list
@param s [in] string to parse
@param list - [in/out] storage for the parsed list
@return NC_NOERR
@author Dennis Heimbigner
*/
int
NCZ_comma_parse(const char* s, NClist* list)
{
    int stat = NC_NOERR;
    const char* p = NULL;
    const char* endp = NULL;

    if(s == NULL || *s == '\0') goto done;

    /* Split s at the commas or EOL */
    p = s;
    for(;;) {
	char* s;
	ptrdiff_t slen;
	endp = strchr(p,',');
	if(endp == NULL) endp = p + strlen(p);
	slen = (endp - p);
	if((s = malloc((size_t)slen+1)) == NULL) {stat = NC_ENOMEM; goto done;}
	memcpy(s,p,(size_t)slen);
	s[slen] = '\0';
	if(nclistmatch(list,s,0)) {
	    nullfree(s); /* duplicate */
	} else {
	    nclistpush(list,s);
	}
	if(*endp == '\0') break;
	p = endp+1;
    }

done:
    return stat;
}

/**************************************************/
#if 0
/* Endianness support */
/* signature: void swapinline16(void* ip) */
#define swapinline16(ip) \
{ \
    union {char b[2]; unsigned short i;} u; \
    char* src = (char*)(ip); \
    u.b[0] = src[1]; \
    u.b[1] = src[0]; \
    *((unsigned short*)ip) = u.i; \
}

/* signature: void swapinline32(void* ip) */
#define swapinline32(ip) \
{ \
    union {char b[4]; unsigned int i;} u; \
    char* src = (char*)(ip); \
    u.b[0] = src[3]; \
    u.b[1] = src[2]; \
    u.b[2] = src[1]; \
    u.b[3] = src[0]; \
    *((unsigned int*)ip) = u.i; \
}

/* signature: void swapinline64(void* ip) */
#define swapinline64(ip) \
{ \
    union {char b[8]; unsigned long long i;} u; \
    char* src = (char*)(ip); \
    u.b[0] = src[7]; \
    u.b[1] = src[6]; \
    u.b[2] = src[5]; \
    u.b[3] = src[4]; \
    u.b[4] = src[3]; \
    u.b[5] = src[2]; \
    u.b[6] = src[1]; \
    u.b[7] = src[0]; \
    *((unsigned long long*)ip) = u.i; \
}
#endif /*0*/

int
NCZ_swapatomicdata(size_t datalen, void* data, int typesize)
{
    int stat = NC_NOERR;
    int i;

    assert(datalen % typesize == 0);

    if(typesize == 1) goto done;

    /*(typesize > 1)*/
    for(i=0;i<datalen;) {
	char* p = ((char*)data) + i;
        switch (typesize) {
        case 2: swapinline16(p); break;
        case 4: swapinline32(p); break;
        case 8: swapinline64(p); break;
        default: break;
	}
	i += typesize;
    }
done:
    return THROW(stat);
}

char**
NCZ_clonestringvec(size_t len, const char** vec)
{
    char** clone = NULL;
    size_t i;
    if(vec == NULL) return NULL;
    if(len == 0) { /* Figure out size as envv vector */
        const char** p;
        for(p=vec;*p;p++) len++;
    }
    clone = malloc(sizeof(char*) * (1+len));
    if(clone == NULL) return NULL;
    for(i=0;i<len;i++) {
	char* s = strdup(vec[i]);
	if(s == NULL) return NULL;
	clone[i] = s;
    }
    clone[len] = NULL;
    return clone;
}

void
NCZ_freestringvec(size_t len, char** vec)
{
    size_t i;
    if(vec == NULL) return;
    if(len == 0) { /* Figure out size as envv vector */
        char** p;
        for(p=vec;*p;p++) len++;
    }
    for(i=0;i<len;i++) {
	nullfree(vec[i]);
    }
    nullfree(vec);
}

int
NCZ_ischunkname(const char* name,char dimsep)
{
    int stat = NC_NOERR;
    const char* p;
    if(strchr("0123456789",name[0])== NULL)
        stat = NC_ENCZARR;
    else for(p=name;*p;p++) {
        if(*p != dimsep && strchr("0123456789",*p) == NULL) /* approximate */
	    {stat = NC_ENCZARR; break;}
    }
    return stat;
}

char*
NCZ_chunkpath(struct ChunkKey key)
{
    size_t plen = nulllen(key.varkey)+1+nulllen(key.chunkkey);
    char* path = (char*)malloc(plen+1);
    
    if(path == NULL) return NULL;
    path[0] = '\0';
    strlcat(path,key.varkey,plen+1);
    strlcat(path,"/",plen+1);
    strlcat(path,key.chunkkey,plen+1);
    return path;    
}

int
NCZ_reclaim_fill_value(NC_VAR_INFO_T* var)
{
    int stat = NC_NOERR;
    if(var->fill_value) {
	int tid = var->type_info->hdr.id;
	stat = NC_reclaim_data_all(var->container->nc4_info->controller,tid,var->fill_value,1);
	var->fill_value = NULL;
    }
    /* Reclaim any existing fill_chunk */
    if(!stat) stat = NCZ_reclaim_fill_chunk(((NCZ_VAR_INFO_T*)var->format_var_info)->cache);
    return stat;
}

int
NCZ_copy_fill_value(NC_VAR_INFO_T* var, void**  dstp)
{
    int stat = NC_NOERR;
    int tid = var->type_info->hdr.id;
    void* dst = NULL;

    if(var->fill_value) {
	if((stat = NC_copy_data_all(var->container->nc4_info->controller,tid,var->fill_value,1,&dst))) goto done;
    }
    if(dstp) {*dstp = dst; dst = NULL;}
done:
    if(dst) (void)NC_reclaim_data_all(var->container->nc4_info->controller,tid,dst,1);
    return stat;
}


/* Get max str len for a variable or grp */
/* Has side effect of setting values in the
   internal data structures */
int
NCZ_get_maxstrlen(NC_OBJ* obj)
{
    int maxstrlen = 0;
    assert(obj->sort == NCGRP || obj->sort == NCVAR);
    if(obj->sort == NCGRP) {
        NC_GRP_INFO_T* grp = (NC_GRP_INFO_T*)obj;
	NC_FILE_INFO_T* file = grp->nc4_info;
	NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
	if(zfile->default_maxstrlen == 0)
	    zfile->default_maxstrlen = NCZ_MAXSTR_DEFAULT;
	maxstrlen = zfile->default_maxstrlen;
    } else { /*(obj->sort == NCVAR)*/
        NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)obj;
	NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
        if(zvar->maxstrlen == 0)
	    zvar->maxstrlen = NCZ_get_maxstrlen((NC_OBJ*)var->container);
	maxstrlen = zvar->maxstrlen;
    }
    return maxstrlen;
}

int
NCZ_fixed2char(const void* fixed, char** charp, size_t count, int maxstrlen)
{
    size_t i;
    unsigned char* sp = NULL;
    const unsigned char* p = fixed;
    memset((void*)charp,0,sizeof(char*)*count);
    for(i=0;i<count;i++,p+=maxstrlen) {
	if(p[0] == '\0') {
	    sp = NULL;
	} else {
	    if((sp = (unsigned char*)malloc((size_t)maxstrlen+1))==NULL) /* ensure null terminated */
	        return NC_ENOMEM; 
	    memcpy(sp,p,(size_t)maxstrlen);
	    sp[maxstrlen] = '\0';
	}
	charp[i] = (char*)sp;
	sp = NULL;
    }
    return NC_NOERR;
}

int
NCZ_char2fixed(const char** charp, void* fixed, size_t count, int maxstrlen)
{
    size_t i;
    unsigned char* p = fixed;
    memset(fixed,0,maxstrlen*count); /* clear target */
    for(i=0;i<count;i++,p+=maxstrlen) {
	size_t len;
	if(charp[i] != NULL) {
	    len = strlen(charp[i]);
	    if(len > maxstrlen) len = maxstrlen;
	    memcpy(p,charp[i],len);
	} else {
	    memset(p,'\0',maxstrlen);
	}
    }
    return NC_NOERR;
}

/*
Wrap NC_copy_data, but take string value into account when overwriting
*/
int
NCZ_copy_data(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const void* memory, size_t count, int reading, void* copy)
{
    int stat = NC_NOERR;    
    NC_TYPE_INFO_T* xtype = var->type_info;
    if(xtype->hdr.id == NC_STRING && !reading) {
	size_t i;
	char** scopy = (char**)copy;
	/* Reclaim any string fill values in copy */
	for(i=0;i<count;i++) {
	    nullfree(scopy[i]);
	    scopy[i] = NULL;
	}
    }
    stat = NC_copy_data(file->controller,xtype->hdr.id,memory,count,copy);
    return stat;
}

#if 0
/* Recursive helper */
static int
checksimplejson(NCjson* json, int depth)
{
    int i;

    switch (NCJsort(json)) {
    case NCJ_ARRAY:
	if(depth > 0) return 0;  /* e.g. [...,[...],...]  or [...,{...},...] */
	for(i=0;i < NCJarraylength(json);i++) {
	    NCjson* j = NCJith(json,i);
	    if(!checksimplejson(j,depth+1)) return 0;
        }
	break;
    case NCJ_DICT:
    case NCJ_NULL:
    case NCJ_UNDEF:
	return 0;
    default: break;
    }
    return 1;
}
#endif

/* Return 1 if the attribute will be stored as a complex JSON valued attribute; return 0 otherwise */
int
NCZ_iscomplexjson(const NCjson* json, nc_type typehint)
{
    int i, stat = 0;

    switch (NCJsort(json)) {
    case NCJ_ARRAY:
	/* If the typehint is NC_CHAR, then always treat it as complex */
	if(typehint == NC_CHAR) {stat = 1; goto done;}
	/* Otherwise see if it is a simple vector of atomic values */
	for(i=0;i < NCJarraylength(json);i++) {
	    NCjson* j = NCJith(json,i);
	    if(!NCJisatomic(j)) {stat = 1; goto done;}
        }
	break;
    case NCJ_DICT:
    case NCJ_NULL:
    case NCJ_UNDEF:
	stat = 1; goto done;
    default: break;
    }
done:
    return stat;
}
