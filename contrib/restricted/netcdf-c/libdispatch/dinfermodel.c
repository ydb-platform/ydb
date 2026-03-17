/**
 * @file
 *
 * Infer as much as possible from the omode + path.
 * Rewrite the path to a canonical form.
 *
 * Copyright 2018 University Corporation for Atmospheric
 * Research/Unidata. See COPYRIGHT file for more info.
*/
#include "config.h"
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifndef _WIN32
#ifdef USE_HDF5
#include <hdf5.h>
#endif /* USE_HDF5 */
#endif /* _WIN32 */
#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif

#include "ncdispatch.h"
#include "ncpathmgr.h"
#include "netcdf_mem.h"
#include "fbits.h"
#include "ncbytes.h"
#include "nclist.h"
#include "nclog.h"
#include "nchttp.h"
#include "ncutil.h"
#ifdef NETCDF_ENABLE_S3
#include "ncs3sdk.h"
#endif

#ifndef nulldup
 #define nulldup(x) ((x)?strdup(x):(x))
#endif

#undef DEBUG

/* If Defined, then use only stdio for all magic number io;
   otherwise use stdio or mpio as required.
 */
#undef USE_STDIO

/**
Sort info for open/read/close of
file when searching for magic numbers
*/
struct MagicFile {
    const char* path;
    struct NCURI* uri;
    int omode;
    NCmodel* model;
    long long filelen;
    int use_parallel;
    int iss3;
    void* parameters; /* !NULL if inmemory && !diskless */
    FILE* fp;
#ifdef USE_PARALLEL
    MPI_File fh;
#endif
    char* curlurl; /* url to use with CURLOPT_SET_URL */
    NC_HTTP_STATE* state;
#ifdef NETCDF_ENABLE_S3
    NCS3INFO s3;
    void* s3client;
    char* errmsg;
#endif
};

/** @internal Magic number for HDF5 files. To be consistent with
 * H5Fis_hdf5, use the complete HDF5 magic number */
static char HDF5_SIGNATURE[MAGIC_NUMBER_LEN] = "\211HDF\r\n\032\n";

#define modelcomplete(model) ((model)->impl != 0)

#ifdef DEBUG
static void dbgflush(void)
{
    fflush(stdout);
    fflush(stderr);
}

static void
fail(int err)
{
    return;
}

static int
check(int err)
{
    if(err != NC_NOERR)
	fail(err);
    return err;
}
#else
#define check(err) (err)
#endif

/*
Define a table of "mode=" string values
from which the implementation can be inferred.
Note that only cases that can currently
take URLs are included.
*/
static struct FORMATMODES {
    const char* tag;
    const int impl; /* NC_FORMATX_XXX value */
    const int format; /* NC_FORMAT_XXX value */
} formatmodes[] = {
{"dap2",NC_FORMATX_DAP2,NC_FORMAT_CLASSIC},
{"dap4",NC_FORMATX_DAP4,NC_FORMAT_NETCDF4},
{"netcdf-3",NC_FORMATX_NC3,0}, /* Might be e.g. cdf5 */
{"classic",NC_FORMATX_NC3,0}, /* ditto */
{"netcdf-4",NC_FORMATX_NC4,NC_FORMAT_NETCDF4},
{"enhanced",NC_FORMATX_NC4,NC_FORMAT_NETCDF4},
{"udf0",NC_FORMATX_UDF0,0},
{"udf1",NC_FORMATX_UDF1,0},
{"udf2",NC_FORMATX_UDF2,0},
{"udf3",NC_FORMATX_UDF3,0},
{"udf4",NC_FORMATX_UDF4,0},
{"udf5",NC_FORMATX_UDF5,0},
{"udf6",NC_FORMATX_UDF6,0},
{"udf7",NC_FORMATX_UDF7,0},
{"udf8",NC_FORMATX_UDF8,0},
{"udf9",NC_FORMATX_UDF9,0},
{"nczarr",NC_FORMATX_NCZARR,NC_FORMAT_NETCDF4},
{"zarr",NC_FORMATX_NCZARR,NC_FORMAT_NETCDF4},
{"bytes",NC_FORMATX_NC4,NC_FORMAT_NETCDF4}, /* temporary until 3 vs 4 is determined */
{NULL,0},
};

/* Replace top-level name with defkey=defvalue */
static const struct MACRODEF {
    char* name;
    char* defkey;
    char* defvalues[4];
} macrodefs[] = {
{"zarr","mode",{"nczarr","zarr",NULL}},
{"dap2","mode",{"dap2",NULL}},
{"dap4","mode",{"dap4",NULL}},
{"s3","mode",{"s3","nczarr",NULL}},
{"bytes","mode",{"bytes",NULL}},
{"xarray","mode",{"zarr", NULL}},
{"noxarray","mode",{"nczarr", "noxarray", NULL}},
{"zarr","mode",{"nczarr","zarr", NULL}},
{"gs3","mode",{"gs3","nczarr",NULL}}, /* Google S3 API */
{NULL,NULL,{NULL}}
};

/*
Mode inferences: if mode contains key value, then add the inferred value;
Warning: be careful how this list is constructed to avoid infinite inferences.
In order to (mostly) avoid that consequence, any attempt to
infer a value that is already present will be ignored.
This effectively means that the inference graph
must be a DAG and may not have cycles.
You have been warned.
*/
static const struct MODEINFER {
    char* key;
    char* inference;
} modeinferences[] = {
{"zarr","nczarr"},
{"xarray","zarr"},
{"noxarray","nczarr"},
{"noxarray","zarr"},
{NULL,NULL}
};

/* Mode negations: if mode contains key, then remove all occurrences of the inference and repeat */
static const struct MODEINFER modenegations[] = {
{"bytes","nczarr"}, /* bytes negates (nc)zarr */
{"bytes","zarr"},
{"noxarray","xarray"},
{NULL,NULL}
};

/* Map FORMATX to readability to get magic number */
static struct Readable {
    int impl;
    int readable;
} readable[] = {
{NC_FORMATX_NC3,1},
{NC_FORMATX_NC_HDF5,1},
{NC_FORMATX_NC_HDF4,1},
{NC_FORMATX_PNETCDF,1},
{NC_FORMATX_DAP2,0},
{NC_FORMATX_DAP4,0},
{NC_FORMATX_UDF0,1},
{NC_FORMATX_UDF1,1},
{NC_FORMATX_UDF2,1},
{NC_FORMATX_UDF3,1},
{NC_FORMATX_UDF4,1},
{NC_FORMATX_UDF5,1},
{NC_FORMATX_UDF6,1},
{NC_FORMATX_UDF7,1},
{NC_FORMATX_UDF8,1},
{NC_FORMATX_UDF9,1},
{NC_FORMATX_NCZARR,0}, /* eventually make readable */
{0,0},
};

/* Define the known URL protocols and their interpretation */
static struct NCPROTOCOLLIST {
    const char* protocol;
    const char* substitute;
    const char* fragments; /* arbitrary fragment arguments */
} ncprotolist[] = {
    {"http",NULL,NULL},
    {"https",NULL,NULL},
    {"file",NULL,NULL},
    {"dods","http","mode=dap2"},
    {"dap4","http","mode=dap4"},
    {"s3","s3","mode=s3"},
    {"gs3","gs3","mode=gs3"},
    {NULL,NULL,NULL} /* Terminate search */
};

/* Forward */
static int NC_omodeinfer(int useparallel, int omode, NCmodel*);
static int check_file_type(const char *path, int omode, int use_parallel, void *parameters, NCmodel* model, NCURI* uri);
static int processuri(const char* path, NCURI** urip, NClist* fraglist);
static int processmacros(NClist* fraglistp, NClist* expanded);
static char* envvlist2string(NClist* pairs, const char*);
static void set_default_mode(int* cmodep);
static int parseonchar(const char* s, int ch, NClist* segments);
static int mergelist(NClist** valuesp);

static int openmagic(struct MagicFile* file);
static int readmagic(struct MagicFile* file, size_t pos, char* magic);
static int closemagic(struct MagicFile* file);
static int NC_interpret_magic_number(char* magic, NCmodel* model);
#ifdef DEBUG
static void printmagic(const char* tag, char* magic,struct MagicFile*);
static void printlist(NClist* list, const char* tag);
#endif
static int isreadable(NCURI*,NCmodel*);
static char* list2string(NClist*);
static int parsepair(const char* pair, char** keyp, char** valuep);
static NClist* parsemode(const char* modeval);
static const char* getmodekey(const NClist* envv);
static int replacemode(NClist* envv, const char* newval);
static void infernext(NClist* current, NClist* next);
static int negateone(const char* mode, NClist* modes);
static void cleanstringlist(NClist* strs, int caseinsensitive);
static int isdaoscontainer(const char* path);

/*
If the path looks like a URL, then parse it, reformat it.
*/
static int
processuri(const char* path, NCURI** urip, NClist* fraglenv)
{
    int stat = NC_NOERR;
    int found = 0;
    NClist* tmp = NULL;
    struct NCPROTOCOLLIST* protolist;
    NCURI* uri = NULL;
    size_t pathlen = strlen(path);
    char* str = NULL;
    const NClist* ufrags;
    size_t i;

    if(path == NULL || pathlen == 0) {stat = NC_EURL; goto done;}

    /* Defaults */
    if(urip) *urip = NULL;

    ncuriparse(path,&uri);
    if(uri == NULL) goto done; /* not url */

    /* Look up the protocol */
    for(found=0,protolist=ncprotolist;protolist->protocol;protolist++) {
        if(strcmp(uri->protocol,protolist->protocol) == 0) {
	    found = 1;
	    break;
	}
    }
    if(!found)
	{stat = NC_EINVAL; goto done;} /* unrecognized URL form */

    /* process the corresponding fragments for that protocol */
    if(protolist->fragments != NULL) {
	tmp = nclistnew();
	if((stat = parseonchar(protolist->fragments,'&',tmp))) goto done;
	for(i=0;i<nclistlength(tmp);i++) {
	    char* key=NULL;
    	    char* value=NULL;
	    if((stat = parsepair(nclistget(tmp,i),&key,&value))) goto done;
	    if(value == NULL) value = strdup("");
	    nclistpush(fraglenv,key);
    	    nclistpush(fraglenv,value);
	}
	nclistfreeall(tmp); tmp = NULL;
    }

    /* Substitute the protocol in any case */
    if(protolist->substitute) ncurisetprotocol(uri,protolist->substitute);

    /* capture the fragments of the url */
    ufrags = (const NClist*)ncurifragmentparams(uri);
    for(i=0;i<nclistlength(ufrags);i+=2) {
	const char* key = nclistget(ufrags,i);
	const char* value = nclistget(ufrags,i+1);
        nclistpush(fraglenv,nulldup(key));
	value = (value==NULL?"":value);
	nclistpush(fraglenv,strdup(value));
    }
    if(urip) {
	*urip = uri;
	uri = NULL;
    }

done:
    nclistfreeall(tmp);
    nullfree(str);
    if(uri != NULL) ncurifree(uri);
    return check(stat);
}

/* Split a key=value pair */
static int
parsepair(const char* pair, char** keyp, char** valuep)
{
    const char* p;
    char* key = NULL;
    char* value = NULL;

    if(pair == NULL)
        return NC_EINVAL; /* empty pair */
    if(pair[0] == '\0' || pair[0] == '=')
        return NC_EINVAL; /* no key */
    p = strchr(pair,'=');
    if(p == NULL) {
	value = NULL;
	key = strdup(pair);
    } else {
	ptrdiff_t len = (p-pair);
	if((key = malloc((size_t)len+1))==NULL) return NC_ENOMEM;
	memcpy(key,pair,(size_t)len);
	key[len] = '\0';
	if(p[1] == '\0')
	    value = NULL;
	else
	    value = strdup(p+1);
    }
    if(keyp) {*keyp = key; key = NULL;};
    if(valuep) {*valuep = value; value = NULL;};
    nullfree(key);
    nullfree(value);
    return NC_NOERR;
}

#if 0
static int
parseurlmode(const char* modestr, NClist* list)
{
    int stat = NC_NOERR;
    const char* p = NULL;
    const char* endp = NULL;

    if(modestr == NULL || *modestr == '\0') goto done;

    /* Split modestr at the commas or EOL */
    p = modestr;
    for(;;) {
	char* s;
	ptrdiff_t slen;
	endp = strchr(p,',');
	if(endp == NULL) endp = p + strlen(p);
	slen = (endp - p);
	if((s = malloc(slen+1)) == NULL) {stat = NC_ENOMEM; goto done;}
	memcpy(s,p,slen);
	s[slen] = '\0';
	nclistpush(list,s);
	if(*endp == '\0') break;
	p = endp+1;
    }

done:
    return check(stat);
}
#endif

/* Split a string at a given char */
static int
parseonchar(const char* s, int ch, NClist* segments)
{
    int stat = NC_NOERR;
    const char* p = NULL;
    const char* endp = NULL;

    if(s == NULL || *s == '\0') goto done;

    p = s;
    for(;;) {
	char* q;
	ptrdiff_t slen;
	endp = strchr(p,ch);
	if(endp == NULL) endp = p + strlen(p);
	slen = (endp - p);
	if((q = malloc((size_t)slen+1)) == NULL) {stat = NC_ENOMEM; goto done;}
	memcpy(q,p,(size_t)slen);
	q[slen] = '\0';
	nclistpush(segments,q);
	if(*endp == '\0') break;
	p = endp+1;
    }

done:
    return check(stat);
}

/* Convert a key,value envv pairlist into a delimited string*/
static char*
envvlist2string(NClist* envv, const char* delim)
{
    size_t i;
    NCbytes* buf = NULL;
    char* result = NULL;

    if(envv == NULL || nclistlength(envv) == 0) return NULL;
    buf = ncbytesnew();
    for(i=0;i<nclistlength(envv);i+=2) {
	const char* key = nclistget(envv,i);
	const char* val = nclistget(envv,i+1);
	if(key == NULL || strlen(key) == 0) continue;
	assert(val != NULL);
	if(i > 0) ncbytescat(buf,"&");
	ncbytescat(buf,key);
	if(val != NULL && val[0] != '\0') {
	    ncbytescat(buf,"=");
	    ncbytescat(buf,val);
	}
    }
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    return result;
}

/* Given a mode= argument, fill in the impl */
static int
processmodearg(const char* arg, NCmodel* model)
{
    int stat = NC_NOERR;
    struct FORMATMODES* format = formatmodes;
    for(;format->tag;format++) {
	if(strcmp(format->tag,arg)==0) {
            model->impl = format->impl;
	    if(format->format != 0) model->format = format->format;
	}
    }
    return check(stat);
}

/* Given an envv fragment list, do macro replacement */
static int
processmacros(NClist* fraglenv, NClist* expanded)
{
    size_t i;
    int stat = NC_NOERR;
    const struct MACRODEF* macros = NULL;

    for(i=0;i<nclistlength(fraglenv);i+=2) {
	int found = 0;
	char* key = nclistget(fraglenv,i);
	char* value = nclistget(fraglenv,i+1);
	if(strlen(value) == 0) { /* must be a singleton  */
            for(macros=macrodefs;macros->name;macros++) {
                if(strcmp(macros->name,key)==0) {
		    char* const * p;
		    nclistpush(expanded,strdup(macros->defkey));
		    for(p=macros->defvalues;*p;p++) 
			nclistpush(expanded,strdup(*p));
		    found = 1;		    
		    break;
	        }
	    }
	}
	if(!found) {/* pass thru */
	    nclistpush(expanded,strdup(key));
    	    nclistpush(expanded,strdup(value));
	}
    }

    return check(stat);
}

/* Process mode flag inferences */
static int
processinferences(NClist* fraglenv)
{
    int stat = NC_NOERR;
    const char* modeval = NULL;
    NClist* newmodes = nclistnew();
    NClist* currentmodes = NULL;
    NClist* nextmodes = nclistnew();
    size_t i;
    char* newmodeval = NULL;

    /* Get "mode" entry */
    if((modeval = getmodekey(fraglenv))==NULL) goto done;

    /* Get the mode as list */
    currentmodes = parsemode(modeval);

#ifdef DEBUG
    printlist(currentmodes,"processinferences: initial mode list");
#endif

    /* Do what amounts to breadth first inferencing down the inference DAG. */

    for(;;) {
        NClist* tmp = NULL;
        /* Compute the next set of inferred modes */
#ifdef DEBUG
printlist(currentmodes,"processinferences: current mode list");
#endif
        infernext(currentmodes,nextmodes);
#ifdef DEBUG
printlist(nextmodes,"processinferences: next mode list");
#endif
        /* move current modes into list of newmodes */
        for(i=0;i<nclistlength(currentmodes);i++) {
	    nclistpush(newmodes,nclistget(currentmodes,i));
	}
        nclistsetlength(currentmodes,0); /* clear current mode list */
        if(nclistlength(nextmodes) == 0) break; /* nothing more to do */
#ifdef DEBUG
printlist(newmodes,"processinferences: new mode list");
#endif
	/* Swap current and next */
        tmp = currentmodes;
	currentmodes = nextmodes;
	nextmodes = tmp;
        tmp = NULL;
    }
    /* cleanup any unused elements in currentmodes */
    nclistclearall(currentmodes);

    /* Ensure no duplicates */
    cleanstringlist(newmodes,1);

#ifdef DEBUG
    printlist(newmodes,"processinferences: final inferred mode list");
#endif

   /* Remove negative inferences */
   for(i=0;i<nclistlength(newmodes);i++) {
	const char* mode = nclistget(newmodes,i);
	negateone(mode,newmodes);
    }

    /* Store new mode value */
    if((newmodeval = list2string(newmodes))== NULL)
	{stat = NC_ENOMEM; goto done;}        
    if((stat=replacemode(fraglenv,newmodeval))) goto done;
    modeval = NULL;

done:
    nullfree(newmodeval);
    nclistfreeall(newmodes);
    nclistfreeall(currentmodes);
    nclistfreeall(nextmodes);
    return check(stat);
}


static int
negateone(const char* mode, NClist* newmodes)
{
    const struct MODEINFER* tests = modenegations;
    int changed = 0;
    for(;tests->key;tests++) {
	if(strcasecmp(tests->key,mode)==0) {
	    /* Find and remove all instances of the inference value */
	    for(size_t i = nclistlength(newmodes); i-- > 0;) {
		char* candidate = nclistget(newmodes,i);
		if(strcasecmp(candidate,tests->inference)==0) {
		    nclistremove(newmodes,i);
		    nullfree(candidate);
	            changed = 1;
		}
	    }
        }
    }
    return changed;
}

static void
infernext(NClist* current, NClist* next)
{
    size_t i;
    for(i=0;i<nclistlength(current);i++) {
        const struct MODEINFER* tests = NULL;
	const char* cur = nclistget(current,i);
        for(tests=modeinferences;tests->key;tests++) {
	    if(strcasecmp(tests->key,cur)==0) {
	        /* Append the inferred mode unless dup */
		if(!nclistmatch(next,tests->inference,1))
	            nclistpush(next,strdup(tests->inference));
	    }
        }
    }
}

/*
Given a list of strings, remove nulls and duplicates
*/
static int
mergelist(NClist** valuesp)
{
    size_t i,j;
    int stat = NC_NOERR;
    NClist* values = *valuesp;
    NClist* allvalues = nclistnew();
    NClist* newvalues = nclistnew();
    char* value = NULL;

    for(i=0;i<nclistlength(values);i++) {
	char* val1 = nclistget(values,i);
	/* split on commas and put pieces into allvalues */
	if((stat=parseonchar(val1,',',allvalues))) goto done;
    }
    /* Remove duplicates and "" */
    while(nclistlength(allvalues) > 0) {
	value = nclistremove(allvalues,0);
	if(strlen(value) == 0) {
	    nullfree(value); value = NULL;
	} else {
	    for(j=0;j<nclistlength(newvalues);j++) {
	        char* candidate = nclistget(newvalues,j);
	        if(strcasecmp(candidate,value)==0)
	            {nullfree(value); value = NULL; break;}
	     }
	}
	if(value != NULL) {nclistpush(newvalues,value); value = NULL;}
    }
    /* Make sure to have at least 1 value */
    if(nclistlength(newvalues)==0) nclistpush(newvalues,strdup(""));
    *valuesp = values; values = NULL;

done:
    nclistfree(allvalues);
    nclistfreeall(values);
    nclistfreeall(newvalues);
    return check(stat);
}

static int
lcontains(NClist* l, const char* key0)
{
    size_t i;
    for(i=0;i<nclistlength(l);i++) {
        const char* key1 = nclistget(l,i);
	if(strcasecmp(key0,key1)==0) return 1;
    }
    return 0;
}

/* Warning values should not use nclistfreeall */
static void
collectvaluesbykey(NClist* fraglenv, const char* key, NClist* values)
{
    size_t i;
    /* collect all the values with the same key (including this one) */
    for(i=0;i<nclistlength(fraglenv);i+=2) {
        const char* key2 = nclistget(fraglenv,i);
        if(strcasecmp(key,key2)==0) {
	    const char* value2 = nclistget(fraglenv,i+1);
	    nclistpush(values,value2); value2 = NULL;
	}
    }
}

/* Warning allkeys should not use nclistfreeall */
static void
collectallkeys(NClist* fraglenv, NClist* allkeys)
{
    size_t i;
    /* collect all the distinct keys */
    for(i=0;i<nclistlength(fraglenv);i+=2) {
	char* key = nclistget(fraglenv,i);
	if(!lcontains(allkeys,key)) {
	    nclistpush(allkeys,key);
	}
    }
}

/* Given a fragment envv list, coalesce duplicate keys and remove duplicate values*/
static int
cleanfragments(NClist* fraglenv, NClist* newlist)
{
    size_t i;
    int stat = NC_NOERR;
    NClist* tmp = NULL;
    NClist* allkeys = NULL;
    NCbytes* buf = NULL;
    char* key = NULL;
    char* value = NULL;

    buf = ncbytesnew();
    allkeys = nclistnew();
    tmp = nclistnew();

    /* collect all unique keys */
    collectallkeys(fraglenv,allkeys);
    /* Collect all values for same key across all fragment pairs */
    for(i=0;i<nclistlength(allkeys);i++) {
	key = nclistget(allkeys,i);
	collectvaluesbykey(fraglenv,key,tmp);
	/* merge the key values, remove duplicate */
	if((stat=mergelist(&tmp))) goto done;
        /* Construct key,value pair and insert into newlist */
	key = strdup(key);
	nclistpush(newlist,key);
	value = list2string(tmp);
	nclistpush(newlist,value);
	nclistclear(tmp);
    }
done:
    nclistfree(allkeys);
    nclistfree(tmp);
    ncbytesfree(buf);
    return check(stat);
}

/* process non-mode fragment keys in case they hold significance; currently not */
static int
processfragmentkeys(const char* key, const char* value, NCmodel* model)
{
    return NC_NOERR;
}

/*
Infer from the mode + useparallel
only call if iscreate or file is not easily readable.
*/
static int
NC_omodeinfer(int useparallel, int cmode, NCmodel* model)
{
    int stat = NC_NOERR;

    /* If no format flags are set, then use default */
    if(!fIsSet(cmode,NC_FORMAT_ALL))
	set_default_mode(&cmode);

    /* Process the cmode; may override some already set flags. The
     * user-defined formats must be checked first. They may choose to
     * use some of the other flags, like NC_NETCDF4, so we must first
     * check NC_UDF0-NC_UDF9 before checking for any other flag. */
    int udf_found = 0;
    /* Lookup table for all UDF mode flags. This replaces the previous bit-shift
     * calculation which was fragile due to non-sequential bit positions
     * (bits 16, 19-25 to avoid conflicts with NC_NOATTCREORD and NC_NODIMSCALE_ATTACH). */
    static const int udf_flags[NC_MAX_UDF_FORMATS] = {
        NC_UDF0, NC_UDF1, NC_UDF2, NC_UDF3, NC_UDF4,
        NC_UDF5, NC_UDF6, NC_UDF7, NC_UDF8, NC_UDF9
    };
    /* Check if any UDF format flag is set in the mode */
    for(int i = 0; i < NC_MAX_UDF_FORMATS; i++) {
        if(fIsSet(cmode, udf_flags[i])) {
            /* Convert array index to format constant (handles gap in numbering) */
            int formatx = (i <= 1) ? (NC_FORMATX_UDF0 + i) : (NC_FORMATX_UDF2 + i - 2);
            model->impl = formatx;
            udf_found = 1;
            break;
        }
    }
    
    if(udf_found)
    {
        if(fIsSet(cmode,NC_64BIT_OFFSET)) 
        {
            model->format = NC_FORMAT_64BIT_OFFSET;
        }
        else if(fIsSet(cmode,NC_64BIT_DATA))
        {
            model->format = NC_FORMAT_64BIT_DATA;
        }
        else if(fIsSet(cmode,NC_NETCDF4))
        {
            if(fIsSet(cmode,NC_CLASSIC_MODEL))
                model->format = NC_FORMAT_NETCDF4_CLASSIC;
            else
                model->format = NC_FORMAT_NETCDF4;
        }
        if(! model->format)
            model->format = NC_FORMAT_CLASSIC;
	goto done;
    }

    if(fIsSet(cmode,NC_64BIT_OFFSET)) {
	model->impl = NC_FORMATX_NC3;
	model->format = NC_FORMAT_64BIT_OFFSET;
        goto done;
    }

    if(fIsSet(cmode,NC_64BIT_DATA)) {
	model->impl = NC_FORMATX_NC3;
	model->format = NC_FORMAT_64BIT_DATA;
        goto done;
    }

    if(fIsSet(cmode,NC_NETCDF4)) {
	model->impl = NC_FORMATX_NC4;
        if(fIsSet(cmode,NC_CLASSIC_MODEL))
	    model->format = NC_FORMAT_NETCDF4_CLASSIC;
	else
	    model->format = NC_FORMAT_NETCDF4;
        goto done;
    }

    /* Default to classic model */
    model->format = NC_FORMAT_CLASSIC;
    model->impl = NC_FORMATX_NC3;

done:
    /* Apply parallel flag */
    if(useparallel) {
        if(model->impl == NC_FORMATX_NC3)
	    model->impl = NC_FORMATX_PNETCDF;
    }
    return check(stat);
}

/*
If the mode flags do not necessarily specify the
format, then default it by adding in appropriate flags.
*/

static void
set_default_mode(int* modep)
{
    int mode = *modep;
    int dfaltformat;

    dfaltformat = nc_get_default_format();
    switch (dfaltformat) {
    case NC_FORMAT_64BIT_OFFSET: mode |= NC_64BIT_OFFSET; break;
    case NC_FORMAT_64BIT_DATA: mode |= NC_64BIT_DATA; break;
    case NC_FORMAT_NETCDF4: mode |= NC_NETCDF4; break;
    case NC_FORMAT_NETCDF4_CLASSIC: mode |= (NC_NETCDF4|NC_CLASSIC_MODEL); break;
    case NC_FORMAT_CLASSIC: /* fall thru */
    default: break; /* default to classic */
    }
    *modep = mode; /* final result */
}

/**************************************************/
/*
   Infer model for this dataset using some
   combination of cmode, path, and reading the dataset.
   See the documentation in docs/internal.dox.

@param path
@param omode
@param iscreate
@param useparallel
@param params
@param model
@param newpathp
*/

int
NC_infermodel(const char* path, int* omodep, int iscreate, int useparallel, void* params, NCmodel* model, char** newpathp)
{
    size_t i;
    int stat = NC_NOERR;
    NCURI* uri = NULL;
    int omode = *omodep;
    NClist* fraglenv = nclistnew();
    NClist* modeargs = nclistnew();
    char* sfrag = NULL;
    const char* modeval = NULL;
    char* abspath = NULL;
    NClist* tmp = NULL;

    /* Phase 1:
       1. convert special protocols to http|https
       2. begin collecting fragments
    */
    if((stat = processuri(path, &uri, fraglenv))) goto done;

    if(uri != NULL) {
#ifdef DEBUG
	printlist(fraglenv,"processuri");
#endif

        /* Phase 2: Expand macros and add to fraglenv */
	nclistfreeall(tmp);
	tmp = nclistnew();
        if((stat = processmacros(fraglenv,tmp))) goto done;
	nclistfreeall(fraglenv);
	fraglenv = tmp; tmp = NULL;
#ifdef DEBUG
	printlist(fraglenv,"processmacros");
#endif
	/* Cleanup the fragment list */
	nclistfreeall(tmp);
	tmp = nclistnew();
        if((stat = cleanfragments(fraglenv,tmp))) goto done;
	nclistfreeall(fraglenv);
	fraglenv = tmp; tmp = NULL;

        /* Phase 2a: Expand mode inferences and add to fraglenv */
        if((stat = processinferences(fraglenv))) goto done;
#ifdef DEBUG
	printlist(fraglenv,"processinferences");
#endif

        /* Phase 3: coalesce duplicate fragment keys and remove duplicate values */
	nclistfreeall(tmp);
	tmp = nclistnew();
        if((stat = cleanfragments(fraglenv,tmp))) goto done;
	nclistfreeall(fraglenv);
	fraglenv = tmp; tmp = NULL;
#ifdef DEBUG
	printlist(fraglenv,"cleanfragments");
#endif

        /* Phase 4: Rebuild the url fragment and rebuilt the url */
        sfrag = envvlist2string(fraglenv,"&");
        nclistfreeall(fraglenv); fraglenv = NULL;
#ifdef DEBUG
	fprintf(stderr,"frag final: %s\n",sfrag);
#endif
        ncurisetfragments(uri,sfrag);
        nullfree(sfrag); sfrag = NULL;

#ifdef NETCDF_ENABLE_S3
	/* If s3, then rebuild the url */
	if(NC_iss3(uri,NULL)) {
	    NCURI* newuri = NULL;
	    if((stat = NC_s3urlrebuild(uri,NULL,&newuri))) goto done;
	    ncurifree(uri);
	    uri = newuri;
	} else
#endif
	if(strcmp(uri->protocol,"file")==0) {
            /* convert path to absolute */
	    char* canon = NULL;
	    abspath = NCpathabsolute(uri->path);
	    if((stat = NCpathcanonical(abspath,&canon))) goto done;
	    nullfree(abspath);
	    abspath = canon; canon = NULL;
	    if((stat = ncurisetpath(uri,abspath))) goto done;
	}
	
	/* rebuild the path */
        if(newpathp) {
            *newpathp = ncuribuild(uri,NULL,NULL,NCURIALL);
#ifdef DEBUG
	    fprintf(stderr,"newpath=|%s|\n",*newpathp); fflush(stderr);
#endif    
	}

        /* Phase 5: Process the mode key to see if we can tell the formatx */
        modeval = ncurifragmentlookup(uri,"mode");
        if(modeval != NULL) {
	    if((stat = parseonchar(modeval,',',modeargs))) goto done;
            for(i=0;i<nclistlength(modeargs);i++) {
        	const char* arg = nclistget(modeargs,i);
        	if((stat=processmodearg(arg,model))) goto done;
            }
	}

        /* Phase 6: Process the non-mode keys to see if we can tell the formatx */
	if(!modelcomplete(model)) {
	    size_t i;
	    NClist* p = (NClist*)ncurifragmentparams(uri); /* envv format */
	    for(i=0;i<nclistlength(p);i+=2) {
		const char* key = nclistget(p,0);
		const char* value = nclistget(p,1);
		if((stat=processfragmentkeys(key,value,model))) goto done;
	    }
	}

        /* Phase 7: Special cases: if this is a URL and model.impl is still not defined */
        /* Phase7a: Default is DAP2 */
        if(!modelcomplete(model)) {
	    model->impl = NC_FORMATX_DAP2;
	    model->format = NC_FORMAT_NC3;
        }

    } else {/* Not URL */
	if(newpathp) *newpathp = NULL;
    }

    /* Phase 8: mode inference from mode flags */
    /* The modeargs did not give us a model (probably not a URL).
       So look at the combination of mode flags and the useparallel flag */
    if(!modelcomplete(model)) {
        if((stat = NC_omodeinfer(useparallel,omode,model))) goto done;
    }

    /* Phase 9: Special case for file stored in DAOS container */
    if(isdaoscontainer(path) == NC_NOERR) {
        /* This is a DAOS container, so immediately assume it is HDF5. */
        model->impl = NC_FORMATX_NC_HDF5;
        model->format = NC_FORMAT_NETCDF4;
    } else {
        /* Phase 10: Infer from file content, if possible;
           this has highest precedence, so it may override
           previous decisions. Note that we do this last
           because we need previously determined model info
           to guess if this file is readable.
        */
        if(!iscreate && isreadable(uri,model)) {
	     /* Ok, we need to try to read the file */
            if((stat = check_file_type(path, omode, useparallel, params, model, uri))) goto done;
        }
    }

    /* Need a decision */
    if(!modelcomplete(model))
	{stat = NC_ENOTNC; goto done;}

    /* Force flag consistency */
    switch (model->impl) {
    case NC_FORMATX_NC4:
    case NC_FORMATX_NC_HDF4:
    case NC_FORMATX_DAP4:
    case NC_FORMATX_NCZARR:
	omode |= NC_NETCDF4;
	if(model->format == NC_FORMAT_NETCDF4_CLASSIC)
	    omode |= NC_CLASSIC_MODEL;
	break;
    case NC_FORMATX_NC3:
	omode &= ~NC_NETCDF4; /* must be netcdf-3 (CDF-1, CDF-2, CDF-5) */
	if(model->format == NC_FORMAT_64BIT_OFFSET) omode |= NC_64BIT_OFFSET;
	else if(model->format == NC_FORMAT_64BIT_DATA) omode |= NC_64BIT_DATA;
	break;
    case NC_FORMATX_PNETCDF:
	omode &= ~NC_NETCDF4; /* must be netcdf-3 (CDF-1, CDF-2, CDF-5) */
	if(model->format == NC_FORMAT_64BIT_OFFSET) omode |= NC_64BIT_OFFSET;
	else if(model->format == NC_FORMAT_64BIT_DATA) omode |= NC_64BIT_DATA;
	break;
    case NC_FORMATX_DAP2:
	omode &= ~(NC_NETCDF4|NC_64BIT_OFFSET|NC_64BIT_DATA|NC_CLASSIC_MODEL);
	break;
    case NC_FORMATX_UDF0:
    case NC_FORMATX_UDF1:
    case NC_FORMATX_UDF2:
    case NC_FORMATX_UDF3:
    case NC_FORMATX_UDF4:
    case NC_FORMATX_UDF5:
    case NC_FORMATX_UDF6:
    case NC_FORMATX_UDF7:
    case NC_FORMATX_UDF8:
    case NC_FORMATX_UDF9:
        if(model->format == NC_FORMAT_64BIT_OFFSET) 
            omode |= NC_64BIT_OFFSET;
        else if(model->format == NC_FORMAT_64BIT_DATA)
            omode |= NC_64BIT_DATA;
        else if(model->format == NC_FORMAT_NETCDF4)  
            omode |= NC_NETCDF4;
        else if(model->format == NC_FORMAT_NETCDF4_CLASSIC)  
            omode |= NC_NETCDF4|NC_CLASSIC_MODEL;
        break;
    default:
	{stat = NC_ENOTNC; goto done;}
    }

done:
    nullfree(sfrag);
    nullfree(abspath);
    ncurifree(uri);
    nclistfreeall(modeargs);
    nclistfreeall(fraglenv);
    nclistfreeall(tmp);
    *omodep = omode; /* in/out */
    return check(stat);
}

static int
isreadable(NCURI* uri, NCmodel* model)
{
    int canread = 0;
    struct Readable* r;
    /* Step 1: Look up the implementation */
    for(r=readable;r->impl;r++) {
	if(model->impl == r->impl) {canread = r->readable; break;}
    }
    /* Step 2: check for bytes mode */
    if(!canread && NC_testmode(uri,"bytes") && (model->impl == NC_FORMATX_NC4 || model->impl == NC_FORMATX_NC_HDF5))
        canread = 1;
    return canread;
}

#if 0
static char*
emptyify(char* s)
{
    if(s == NULL) s = strdup("");
    return strdup(s);
}

static const char*
nullify(const char* s)
{
    if(s != NULL && strlen(s) == 0)
        return NULL;
    return s;
}
#endif

/**************************************************/
/* Envv list utilities */

static const char*
getmodekey(const NClist* envv)
{
    size_t i;
    /* Get "mode" entry */
    for(i=0;i<nclistlength(envv);i+=2) {
	char* key = NULL;
	key = nclistget(envv,i);
	if(strcasecmp(key,"mode")==0)
	    return nclistget(envv,i+1);
    }
    return NULL;
}

static int
replacemode(NClist* envv, const char* newval)
{
    size_t i;
    /* Get "mode" entry */
    for(i=0;i<nclistlength(envv);i+=2) {
	char* key = NULL;
	char* val = NULL;
	key = nclistget(envv,i);
	if(strcasecmp(key,"mode")==0) {
	    val = nclistget(envv,i+1);	    
	    nclistset(envv,i+1,strdup(newval));
	    nullfree(val);
	    return NC_NOERR;
	}
    }
    return NC_EINVAL;
}

static NClist*
parsemode(const char* modeval)
{
    NClist* modes = nclistnew();
    if(modeval)
        (void)parseonchar(modeval,',',modes);/* split on commas */
    return modes;    
}

/* Convert a list into a comma'd string */
static char*
list2string(NClist* list)
{
    size_t i;
    NCbytes* buf = NULL;
    char* result = NULL;

    if(list == NULL || nclistlength(list)==0) return strdup("");
    buf = ncbytesnew();
    for(i=0;i<nclistlength(list);i++) {
	const char* m = nclistget(list,i);
	if(m == NULL || strlen(m) == 0) continue;
	if(i > 0) ncbytescat(buf,",");
	ncbytescat(buf,m);
    }
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    if(result == NULL) result = strdup("");
    return result;
}

#if 0
/* Given a comma separated string, remove duplicates; mostly used to cleanup mode list */
static char* 
cleancommalist(const char* commalist, int caseinsensitive)
{
    NClist* tmp = nclistnew();
    char* newlist = NULL;
    if(commalist == NULL || strlen(commalist)==0) return nulldup(commalist);
    (void)parseonchar(commalist,',',tmp);/* split on commas */
    cleanstringlist(tmp,caseinsensitive);
    newlist = list2string(tmp);
    nclistfreeall(tmp);
    return newlist;
}
#endif

/* Given a list of strings, remove nulls and duplicated */
static void
cleanstringlist(NClist* strs, int caseinsensitive)
{
    if(nclistlength(strs) == 0) return;
    /* Remove nulls */
    for(size_t i = nclistlength(strs); i-->0;) {
        if(nclistget(strs,i)==NULL) nclistremove(strs,i);
    }
    if(nclistlength(strs) <= 1) return;
    /* Remove duplicates*/
    for(size_t i=0;i<nclistlength(strs);i++) {
        const char* value = nclistget(strs,i);
        /* look ahead for duplicates */
        for(size_t j=nclistlength(strs)-1;j>i;j--) {
            int match;
            const char* candidate = nclistget(strs,j);
            if(caseinsensitive)
                match = (strcasecmp(value,candidate) == 0);
            else
                match = (strcmp(value,candidate) == 0);
            if(match) {char* dup = nclistremove(strs,j); nullfree(dup);}
        }
    }
}


/**************************************************/
/**
 * @internal Given an existing file, figure out its format and return
 * that format value (NC_FORMATX_XXX) in model arg. Assume any path
 * conversion was already performed at a higher level.
 *
 * @param path File name.
 * @param flags
 * @param use_parallel
 * @param parameters
 * @param model Pointer that gets the model to use for the dispatch table.
 * @param version Pointer that gets version of the file.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
*/
static int
check_file_type(const char *path, int omode, int use_parallel,
		   void *parameters, NCmodel* model, NCURI* uri)
{
    char magic[NC_MAX_MAGIC_NUMBER_LEN];
    int status = NC_NOERR;
    struct MagicFile magicinfo;
#ifdef _WIN32
    NC* nc = NULL;
#endif

    memset((void*)&magicinfo,0,sizeof(magicinfo));

#ifdef _WIN32 /* including MINGW */
    /* Windows does not handle multiple handles to the same file very well.
       So if file is already open/created, then find it and just get the
       model from that. */
    if((nc = find_in_NCList_by_name(path)) != NULL) {
	int format = 0;
	/* Get the model from this NC */
	if((status = nc_inq_format_extended(nc->ext_ncid,&format,NULL))) goto done;
	model->impl = format;
	if((status = nc_inq_format(nc->ext_ncid,&format))) goto done;
	model->format = format;
	goto done;
    }
#endif

    magicinfo.path = path; /* do not free */
    magicinfo.uri = uri; /* do not free */
    magicinfo.omode = omode;
    magicinfo.model = model; /* do not free */
    magicinfo.parameters = parameters; /* do not free */
#ifdef USE_STDIO
    magicinfo.use_parallel = 0;
#else
    magicinfo.use_parallel = use_parallel;
#endif

    if((status = openmagic(&magicinfo))) goto done;

    /* Verify we have a large enough file */
    if(MAGIC_NUMBER_LEN >= (unsigned long long)magicinfo.filelen)
	{status = NC_ENOTNC; goto done;}
    if((status = readmagic(&magicinfo,0L,magic)) != NC_NOERR) {
	status = NC_ENOTNC;
	goto done;
    }

    /* Look at the magic number */
    if(NC_interpret_magic_number(magic,model) == NC_NOERR
	&& model->format != 0) {
        if (use_parallel && (model->format == NC_FORMAT_NC3 || model->impl == NC_FORMATX_NC3))
            /* this is called from nc_open_par() and file is classic */
            model->impl = NC_FORMATX_PNETCDF;
        goto done; /* found something */
    }

    /* Remaining case when implementation is an HDF5 file;
       search forward at starting at 512
       and doubling to see if we have HDF5 magic number */
    {
	size_t pos = 512L;
        for(;;) {
	    if((pos+MAGIC_NUMBER_LEN) > (unsigned long long)magicinfo.filelen)
		{status = NC_ENOTNC; goto done;}
            if((status = readmagic(&magicinfo,pos,magic)) != NC_NOERR)
	        {status = NC_ENOTNC; goto done; }
            NC_interpret_magic_number(magic,model);
            if(model->impl == NC_FORMATX_NC4) break;
	    /* double and try again */
	    pos = 2*pos;
        }
    }
done:
    closemagic(&magicinfo);
    return check(status);
}

/**
\internal
\ingroup datasets
Provide open, read and close for use when searching for magic numbers
*/
static int
openmagic(struct MagicFile* file)
{
    int status = NC_NOERR;
    if(fIsSet(file->omode,NC_INMEMORY)) {
	/* Get its length */
	NC_memio* meminfo = (NC_memio*)file->parameters;
        assert(meminfo != NULL);
	file->filelen = (long long)meminfo->size;
	goto done;
    }
    if(file->uri != NULL) {
#ifdef NETCDF_ENABLE_BYTERANGE
	/* Construct a URL minus any fragment */
        file->curlurl = ncuribuild(file->uri,NULL,NULL,NCURISVC);
	/* Open the curl handle */
        if((status=nc_http_open(file->path, &file->state))) goto done;
	if((status=nc_http_size(file->state,&file->filelen))) goto done;
#else /*!BYTERANGE*/
	{status = NC_ENOTBUILT;}
#endif /*BYTERANGE*/
	goto done;
    }	
#ifdef USE_PARALLEL
    if (file->use_parallel) {
	int retval;
	MPI_Offset size;
        assert(file->parameters != NULL);
	if((retval = MPI_File_open(((NC_MPI_INFO*)file->parameters)->comm,
                                   (char*)file->path,MPI_MODE_RDONLY,
                                   ((NC_MPI_INFO*)file->parameters)->info,
                                   &file->fh)) != MPI_SUCCESS) {
#ifdef MPI_ERR_NO_SUCH_FILE
	    int errorclass;
	    MPI_Error_class(retval, &errorclass);
	    if (errorclass == MPI_ERR_NO_SUCH_FILE)
#ifdef NC_ENOENT
	        status = NC_ENOENT;
#else /*!NC_ENOENT*/
		status = errno;
#endif /*NC_ENOENT*/
	    else
#endif /*MPI_ERR_NO_SUCH_FILE*/
	        status = NC_EPARINIT;
	    file->fh = MPI_FILE_NULL;
	    goto done;
	}
	/* Get its length */
	if((retval=MPI_File_get_size(file->fh, &size)) != MPI_SUCCESS)
	    {status = NC_EPARINIT; goto done;}
	file->filelen = (long long)size;
	goto done;
    }
#endif /* USE_PARALLEL */
    {
        if (file->path == NULL || strlen(file->path) == 0)
            {status = NC_EINVAL; goto done;}
        file->fp = NCfopen(file->path, "r");
        if(file->fp == NULL)
	    {status = errno; goto done;}
	/* Get its length */
	{
	    int fd = fileno(file->fp);
#ifdef _WIN32
	    __int64 len64 = _filelengthi64(fd);
	    if(len64 < 0)
		{status = errno; goto done;}
	    file->filelen = (long long)len64;
#else
	    off_t size;
	    size = lseek(fd, 0, SEEK_END);
	    if(size == -1)
		{status = errno; goto done;}
		file->filelen = (long long)size;
#endif
	}
        int retval2 = fseek(file->fp, 0L, SEEK_SET);        
	    if(retval2 != 0)
		{status = errno; goto done;}
    }
done:
    return check(status);
}

static int
readmagic(struct MagicFile* file, size_t pos, char* magic)
{
    int status = NC_NOERR;
    NCbytes* buf = ncbytesnew();

    memset(magic,0,MAGIC_NUMBER_LEN);
    if(fIsSet(file->omode,NC_INMEMORY)) {
	char* mempos;
	NC_memio* meminfo = (NC_memio*)file->parameters;
	if((pos + MAGIC_NUMBER_LEN) > meminfo->size)
	    {status = NC_EINMEMORY; goto done;}
	mempos = ((char*)meminfo->memory) + pos;
	memcpy((void*)magic,mempos,MAGIC_NUMBER_LEN);
#ifdef DEBUG
	printmagic("XXX: readmagic",magic,file);
#endif
    } else if(file->uri != NULL) {
#ifdef NETCDF_ENABLE_BYTERANGE
        size64_t start = (size64_t)pos;
        size64_t count = MAGIC_NUMBER_LEN;
        status = nc_http_read(file->state, start, count, buf);
        if (status == NC_NOERR) {
            if (ncbyteslength(buf) != count)
                status = NC_EINVAL;
            else
                memcpy(magic, ncbytescontents(buf), count);
        }
#endif
    } else {
#ifdef USE_PARALLEL
        if (file->use_parallel) {
	    MPI_Status mstatus;
	    int retval;
	    if((retval = MPI_File_read_at_all(file->fh, pos, magic,
			    MAGIC_NUMBER_LEN, MPI_CHAR, &mstatus)) != MPI_SUCCESS)
	        {status = NC_EPARINIT; goto done;}
        }
        else
#endif /* USE_PARALLEL */
        { /* Ordinary read */
            long i;
            i = fseek(file->fp, (long)pos, SEEK_SET);
            if (i < 0) { status = errno; goto done; }
            ncbytessetlength(buf, 0);
            if ((status = NC_readfileF(file->fp, buf, MAGIC_NUMBER_LEN))) goto done;
            memcpy(magic, ncbytescontents(buf), MAGIC_NUMBER_LEN);
        }
    }

done:
    ncbytesfree(buf);
    if(file && file->fp) clearerr(file->fp);
    return check(status);
}

/**
 * Close the file opened to check for magic number.
 *
 * @param file pointer to the MagicFile struct for this open file.
 * @returns NC_NOERR for success
 * @returns NC_EPARINIT if there was a problem closing file with MPI
 * (parallel builds only).
 * @author Dennis Heimbigner
 */
static int
closemagic(struct MagicFile* file)
{
    int status = NC_NOERR;

    if(fIsSet(file->omode,NC_INMEMORY)) {
	/* noop */
    } else if(file->uri != NULL) {
#ifdef NETCDF_ENABLE_BYTERANGE
	    status = nc_http_close(file->state);
#endif
	    nullfree(file->curlurl);
    } else {
#ifdef USE_PARALLEL
        if (file->use_parallel) {
	    int retval;
	    if(file->fh != MPI_FILE_NULL
	       && (retval = MPI_File_close(&file->fh)) != MPI_SUCCESS)
		    {status = NC_EPARINIT; return status;}
        } else
#endif
        {
	    if(file->fp) fclose(file->fp);
        }
    }
    return status;
}

/*!
  Interpret the magic number found in the header of a netCDF file.
  This function interprets the magic number/string contained in the header of a netCDF file and sets the appropriate NC_FORMATX flags.

  @param[in] magic Pointer to a character array with the magic number block.
  @param[out] model Pointer to an integer to hold the corresponding netCDF type.
  @param[out] version Pointer to an integer to hold the corresponding netCDF version.
  @returns NC_NOERR if a legitimate file type found
  @returns NC_ENOTNC otherwise

\internal
\ingroup datasets

*/
static int
NC_interpret_magic_number(char* magic, NCmodel* model)
{
    int status = NC_NOERR;
    int tmpimpl = 0;
    /* Look at the magic number - save any UDF format on entry */
    if(model->impl >= NC_FORMATX_UDF0 && model->impl <= NC_FORMATX_UDF1)
        tmpimpl = model->impl;
    else if(model->impl >= NC_FORMATX_UDF2 && model->impl <= NC_FORMATX_UDF9)
        tmpimpl = model->impl;

    /* Use the complete magic number string for HDF5 */
    if(memcmp(magic,HDF5_SIGNATURE,sizeof(HDF5_SIGNATURE))==0) {
	model->impl = NC_FORMATX_NC4;
	model->format = NC_FORMAT_NETCDF4;
	goto done;
    }
    if(magic[0] == '\016' && magic[1] == '\003'
              && magic[2] == '\023' && magic[3] == '\001') {
	model->impl = NC_FORMATX_NC_HDF4;
	model->format = NC_FORMAT_NETCDF4;
	goto done;
    }
    if(magic[0] == 'C' && magic[1] == 'D' && magic[2] == 'F') {
        if(magic[3] == '\001') {
	    model->impl = NC_FORMATX_NC3;
	    model->format = NC_FORMAT_CLASSIC;
	    goto done;
	}
        if(magic[3] == '\002') {
	    model->impl = NC_FORMATX_NC3;
	    model->format = NC_FORMAT_64BIT_OFFSET;
	    goto done;
        }
        if(magic[3] == '\005') {
	  model->impl = NC_FORMATX_NC3;
	  model->format = NC_FORMAT_64BIT_DATA;
	  goto done;
	}
     }
     /* No match  */
     if (!tmpimpl) 
         status = NC_ENOTNC;         

     goto done;

done:
     /* if model->impl was any UDF format (0-9) on entry, make it so on exit */
     if(tmpimpl)
         model->impl = tmpimpl;
     /* if this is a UDF magic_number update the model->impl */
     for(int i = 0; i < NC_MAX_UDF_FORMATS; i++) {
         if (strlen(UDF_magic_numbers[i]) && !strncmp(UDF_magic_numbers[i], magic,
                                                       strlen(UDF_magic_numbers[i])))
         {
             int formatx = (i <= 1) ? (NC_FORMATX_UDF0 + i) : (NC_FORMATX_UDF2 + i - 2);
             model->impl = formatx;
             status = NC_NOERR;
             break;
         }
     }    

     return check(status);
}

/* Define macros to wrap getxattr and listxattrcalls */
#ifdef __APPLE__
#define GETXATTR(path,p,xvalue,xlen) getxattr(path, p, xvalue, (size_t)xlen, 0, 0);
#define LISTXATTR(path,xlist,xlen) listxattr(path, xlist, (size_t)xlen, 0)
#else
#define GETXATTR(path,p,xvalue,xlen) getxattr(path, p, xvalue, (size_t)xlen);
#define LISTXATTR(path,xlist,xlen) listxattr(path, xlist, (size_t)xlen)
#endif

/* Return NC_NOERR if path is a DAOS container; return NC_EXXX otherwise */
static int
isdaoscontainer(const char* path)
{
    int stat = NC_ENOTNC; /* default is that this is not a DAOS container */
#ifndef _WIN32
#ifdef USE_HDF5
#if H5_VERSION_GE(1,12,0)
    htri_t accessible;
    hid_t fapl_id;
    int rc;
    /* Check for a DAOS container */
    if((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {stat = NC_EHDFERR; goto done;}
    H5Pset_fapl_sec2(fapl_id);
    accessible = H5Fis_accessible(path, fapl_id);
    H5Pclose(fapl_id); /* Ignore any error */
    rc = 0;
    if(accessible > 0) {
#ifdef HAVE_SYS_XATTR_H
	ssize_t xlen;
	xlen = LISTXATTR(path,NULL,0);
        if(xlen > 0) {
  	    char* xlist = NULL;
	    char* xvalue = NULL;
	    char* p;
	    char* endp;
	    if((xlist = (char*)calloc(1,(size_t)xlen))==NULL)
		{stat = NC_ENOMEM; goto done;}
	    (void)LISTXATTR(path,xlist,xlen);
	    p = xlist; endp = p + xlen; /* delimit names */
	    /* walk the list of xattr names */
	    for(;p < endp;p += (strlen(p)+1)) {
		/* The popen version looks for the string ".daos";
                   It would be nice if we know whether that occurred
		   int the xattr's name or it value.
		   Oh well, we will do the general search */
		/* Look for '.daos' in the key */
		if(strstr(p,".daos") != NULL) {rc = 1; break;} /* success */
		/* Else get the p'th xattr's value size */
		xlen = GETXATTR(path,p,NULL,0);
		if((xvalue = (char*)calloc(1,(size_t)xlen))==NULL)
		    {stat = NC_ENOMEM; goto done;}
		/* Read the value */
		(void)GETXATTR(path,p,xvalue,xlen);
		/* Look for '.daos' in the value */
		if(strstr(xvalue,".daos") != NULL) {rc = 1; break;} /* success */
	    }
        }
#else /*!HAVE_SYS_XATTR_H*/

#ifdef HAVE_GETFATTR
	{
	    FILE *fp;
	    char cmd[4096];
	    memset(cmd,0,sizeof(cmd));
      snprintf(cmd,sizeof(cmd),"getfattr \"%s\" | grep -c '.daos'",path);
      fp = popen(cmd, "r");
      if(fp != NULL) {
        fscanf(fp, "%d", &rc);
        pclose(fp);
	    } else {
    		rc = 0; /* Cannot test; assume not DAOS */
	    }
	}
    }
#else /*!HAVE_GETFATTR*/
    /* We just can't test for DAOS container.*/
    rc = 0;
#endif /*HAVE_GETFATTR*/
#endif /*HAVE_SYS_XATTR_H*/
    }
    /* Test for DAOS container */
    stat = (rc == 1 ? NC_NOERR : NC_ENOTNC);
done:
#endif
#endif
#endif
    errno = 0; /* reset */
    return stat;
}

#ifdef DEBUG
static void
printmagic(const char* tag, char* magic, struct MagicFile* f)
{
    int i;
    fprintf(stderr,"%s: ispar=%d magic=",tag,f->use_parallel);
    for(i=0;i<MAGIC_NUMBER_LEN;i++) {
        unsigned int c = (unsigned int)magic[i];
	c = c & 0x000000FF;
	if(c == '\n')
	    fprintf(stderr," 0x%0x/'\\n'",c);
	else if(c == '\r')
	    fprintf(stderr," 0x%0x/'\\r'",c);
	else if(c < ' ')
	    fprintf(stderr," 0x%0x/'?'",c);
	else
	    fprintf(stderr," 0x%0x/'%c'",c,c);
    }
    fprintf(stderr,"\n");
    fflush(stderr);
}

static void
printlist(NClist* list, const char* tag)
{
    int i;
    fprintf(stderr,"%s:",tag);
    for(i=0;i<nclistlength(list);i++) {
        fprintf(stderr," %s",(char*)nclistget(list,i));
	fprintf(stderr,"[%p]",(char*)nclistget(list,i));
    }
    fprintf(stderr,"\n");
    dbgflush();
}


#endif
