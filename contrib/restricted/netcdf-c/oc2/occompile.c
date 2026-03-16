/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#include "config.h"
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include "ocinternal.h"
#include "ocdebug.h"
#include "ocdump.h"

/* Mnemonic */
#define TOPLEVEL 1

/* Forward */
static OCdata* newocdata(OCnode* pattern);
static off_t ocxdrsize(OCtype etype,int isscalar);
static OCerror occompile1(OCstate*, OCnode*, XXDR*, OCdata**);
static OCerror occompilerecord(OCstate*, OCnode*, XXDR*, OCdata**);
static OCerror occompilefields(OCstate*, OCdata*, XXDR*, int istoplevel);
static OCerror occompileatomic(OCstate*, OCdata*, XXDR*);
static int ocerrorstring(XXDR* xdrs);
static int istoplevel(OCnode* node);

/* Sequence tag constant */
static const char StartOfSequence = '\x5A';
static const char EndOfSequence = '\xA5';

/*
Provide an option that makes a single pass over 
the data packet and record pointers into it
to speed up access.
*/

/* Assume we are operating on the datadds tree */
OCerror
occompile(OCstate* state, OCnode* xroot)
{
    OCerror ocstat = OC_NOERR;
    XXDR* xxdrs;
    OCtree* xtree;
    OCdata* data;

    OCASSERT(state != NULL);
    OCASSERT(xroot != NULL);
    OCASSERT(xroot->tree != NULL);
    OCASSERT(xroot->tree->dxdclass == OCDATADDS);
    OCASSERT(xroot->tree->data.data == NULL);

    xtree = xroot->tree;

    xxdrs = xtree->data.xdrs;
    if(xxdrs == NULL) return OCTHROW(OC_EXDR);

    ocstat = occompile1(state,xroot,xxdrs,&data);
    if(ocstat == OC_NOERR)
	xtree->data.data = data;

#ifdef OCDEBUG
{
    Ncbytes* buffer = ncbytesnew();
    ocdumpdatatree(state,data,buffer,0);
    fprintf(stderr,"datatree:\n%s",ncbytescontents(buffer));
    ncbytesfree(buffer);
}
#endif
    return OCTHROW(ocstat);
}

static OCerror
occompile1(OCstate* state, OCnode* xnode, XXDR* xxdrs, OCdata** datap)
{
    OCerror ocstat = OC_NOERR;
    size_t i;
    OCdata* data = NULL;
    size_t nelements = 0;
    NClist* records = NULL;

    /* Allocate instance for this node */
    data = newocdata(xnode);
    MEMFAIL(data);

    /* Capture position(s) */
    data->xdroffset = xxdr_getpos(xxdrs);

    switch (xnode->octype) {

    case OC_Dataset:
    case OC_Grid: /* Always scalars */
	ocstat = occompilefields(state,data,xxdrs,istoplevel(xnode));
	if(ocstat != OC_NOERR) goto fail;
	break;

    case OC_Structure:
	if(xnode->array.rank == 0) {/* scalar */
	    ocstat = occompilefields(state,data,xxdrs,istoplevel(xnode));
	    if(ocstat != OC_NOERR) goto fail;
	} else { /* dimensioned structure */
	    unsigned int xdrcount;
    	    fset(data->datamode,OCDT_ARRAY);
	    /* Determine # of instances */
            nelements = octotaldimsize(xnode->array.rank,xnode->array.sizes);
            if(nelements == 0) {ocstat = OCTHROW(OC_ENODATA); goto fail;}
            /* Validate and skip the leading count field */
            if(!xxdr_uint(xxdrs,&xdrcount))
	        {ocstat = OC_EXDR; goto fail;}
	    if(xdrcount != nelements)
   	        {ocstat=OCTHROW(OC_EINVALCOORDS); goto fail;}

	    /* allocate space to capture all the element instances */
	    data->instances = (OCdata**)calloc(nelements,sizeof(OCdata*));
	    MEMGOTO(data->instances,ocstat,fail);
	    data->ninstances = 0;

	    /* create and fill the element instances */
	    for(i=0;i<nelements;i++) {
		OCdata* instance = newocdata(xnode);
		MEMGOTO(instance,ocstat,fail);
		fset(instance->datamode,OCDT_ELEMENT);
		data->instances[i] = instance;
		data->ninstances++;
		/* Capture the back link */
		instance->container = data;
		instance->index = i;
  	        /* capture the current instance position */
		instance->xdroffset = xxdr_getpos(xxdrs);
	        /* Now compile the fields of this instance */
		ocstat = occompilefields(state,instance,xxdrs,!TOPLEVEL);
		if(ocstat != OC_NOERR) {goto fail;}
	    }
	}
        break;

    case OC_Sequence:
	/* Since we do not know the # records beforehand,
           use a list to collect the record instances.
        */
	fset(data->datamode,OCDT_SEQUENCE);
	records = nclistnew();
	for(nelements=0;;nelements++) {
            /* pick up the sequence record begin marker*/
            char tmp[sizeof(unsigned int)];
            /* extract the tag byte*/
	    if(!xxdr_opaque(xxdrs,tmp,(off_t)sizeof(tmp)))
		{ocstat = OC_EXDR; goto fail;}
            if(tmp[0] == StartOfSequence) {
		/* Allocate a record instance */
		OCdata* record = NULL;
		ocstat = occompilerecord(state,xnode,xxdrs,&record);
	        if(ocstat != OC_NOERR || !record) goto fail;
		/* Capture the back link */
		record->container = data;
		record->index = nelements;
		nclistpush(records,(void*)record);
		record = NULL;
            } else if(tmp[0] == EndOfSequence) {
                break; /* we are done with the this sequence instance*/
            } else {
		nclog(NCLOGERR,"missing/invalid begin/end record marker\n");
                ocstat = OCTHROW(OC_EINVALCOORDS);
		goto fail;
            }
	}
        OCASSERT(nelements == nclistlength(records));
	/* extract the content */
	data->ninstances = nelements;
	data->instances = nclistextract(records);
	MEMGOTO(data,ocstat,fail);
	nclistfree(records);	    
	records = NULL;
        break;

    case OC_Atomic:
	fset(data->datamode,OCDT_ATOMIC);
	ocstat = occompileatomic(state,data,xxdrs);
	if(ocstat != OC_NOERR) goto fail;
	break;

    default:
	OCPANIC1("occompile: encountered unexpected node type: %x",xnode->octype);
        break;
    }

/*ok:*/
    if(datap) {
	*datap = data;
	data = NULL;
    }

    if(data != NULL)
	ocdata_free(state,data);

    return OCTHROW(ocstat);    

fail:
    /* See if we can extract error info from the response */
    ocerrorstring(xxdrs);

    if(records != NULL) {
	for(i=0;i<nclistlength(records);i++)
	    ocdata_free(state,(OCdata*)nclistget(records,i));
	nclistfree(records);
    }

    if(data != NULL)
	ocdata_free(state,data);

    return OCTHROW(ocstat);
}

static OCerror
occompilerecord(OCstate* state, OCnode* xnode, XXDR* xxdrs, OCdata** recordp)
{
    OCerror ocstat = OC_NOERR;
    OCdata* record = newocdata(xnode);/* create record record */
    MEMFAIL(record);
    fset(record->datamode,OCDT_RECORD);
    record->pattern = xnode;
    /* capture the current record position */
    record->xdroffset = xxdr_getpos(xxdrs);
    /* Compile the fields of this record */
    ocstat = OCTHROW(occompilefields(state,record,xxdrs,!TOPLEVEL));
    if(ocstat == OC_NOERR) {
        if(recordp) {
	    *recordp = record;
	    record = NULL;
	}
        if(record != NULL)
	    ocdata_free(state,record);
    }
    return OCTHROW(ocstat);    
}

static OCerror
occompilefields(OCstate* state, OCdata* data, XXDR* xxdrs, int istoplevel)
{
    size_t i;
    OCerror ocstat = OC_NOERR;
    size_t nelements;
    OCnode* xnode = data->pattern;

    assert(data != NULL);
    nelements = nclistlength(xnode->subnodes);
    if(nelements == 0)
	goto done;
    data->instances = (OCdata**)malloc(nelements*sizeof(OCdata*));
    MEMFAIL(data->instances);
    for(i=0;i<nelements;i++) {
        OCnode* fieldnode;
        OCdata* fieldinstance;
	fieldnode = (OCnode*)nclistget(xnode->subnodes,i);
        ocstat = occompile1(state,fieldnode,xxdrs,&fieldinstance);
	if(ocstat != OC_NOERR)
	    goto fail;
	fset(fieldinstance->datamode,OCDT_FIELD);
	data->instances[i] = fieldinstance;
	data->ninstances++;
	/* Capture the back link */
	fieldinstance->container = data;
        fieldinstance->index = i;
    }

    /* If top-level, then link the OCnode to the OCdata directly */
    if(istoplevel) {
	for(i=0;i<nelements;i++) {
            OCnode* fieldnode = (OCnode*)nclistget(xnode->subnodes,i);
            OCdata* fieldinstance = data->instances[i];
	    fieldnode->data = fieldinstance;
	}
    }

done:
    return OCTHROW(ocstat);

fail:
    if(data->instances != NULL) {
	for(i=0;i<data->ninstances;i++)
	    ocdata_free(state,data->instances[i]);
	data->ninstances = 0;
    }
    return OCTHROW(ocstat);
}

static OCerror
occompileatomic(OCstate* state, OCdata* data, XXDR* xxdrs)
{
    OCerror ocstat = OC_NOERR;
    int i;
    off_t xdrsize;
    unsigned int xxdrcount;
    OCnode* xnode = data->pattern;
    int scalar = (xnode->array.rank == 0);
    
    OCASSERT((xnode->octype == OC_Atomic));

    if(!scalar) {
        /* Use the count from the datadds */
        size_t nelements = octotaldimsize(xnode->array.rank,xnode->array.sizes);
        /* Get first copy of the dimension count */
        if(!xxdr_uint(xxdrs,&xxdrcount)) {ocstat = OC_EXDR; goto fail;}
        if(xxdrcount != nelements) {ocstat=OCTHROW(OC_EINVALCOORDS); goto fail;}
        if(xnode->etype != OC_String && xnode->etype != OC_URL) {
            /* Get second copy of the dimension count */
            if(!xxdr_uint(xxdrs,&xxdrcount)) {ocstat = OC_EXDR; goto fail;}
            if(xxdrcount != nelements) {ocstat=OCTHROW(OC_EINVALCOORDS); goto fail;}
        }
    } else { /*scalar*/
	xxdrcount = 1;
    }

    data->xdroffset = xxdr_getpos(xxdrs);
    data->ninstances = xxdrcount;
    data->xdrsize = ocxdrsize(xnode->etype,scalar);

    switch (xnode->etype) {

    /* Do the fixed sized, non-packed cases */
    case OC_Int16: case OC_UInt16:
    case OC_Int32: case OC_UInt32:
    case OC_Int64: case OC_UInt64:
    case OC_Float32: case OC_Float64:
	/* Skip the data */
	xxdr_skip(xxdrs,(off_t)data->ninstances*data->xdrsize);
	break;

    /* Do the fixed sized, possibly packed cases */
    case OC_Byte:
    case OC_UByte:
    case OC_Char:
	/* Get the totalsize and round up to multiple of XDRUNIT */
	xdrsize = (off_t)data->ninstances*data->xdrsize;
	xdrsize = RNDUP(xdrsize);
	/* Skip the data */
	xxdr_skip(xxdrs,xdrsize);
	break;

    /* Hard case, because strings are variable length */
    case OC_String: case OC_URL:
	/* Start by allocating a set of pointers for each string */
	data->nstrings = xxdrcount;
	data->strings = (off_t*)malloc(sizeof(off_t)*data->nstrings);
	/* We need to walk each string, get size, then skip */
        for(i=0;i<data->nstrings;i++) {
	    unsigned int len;
	    off_t lenz;
	    data->strings[i] = xxdr_getpos(xxdrs);
	    /* get exact string length */
	    if(!xxdr_uint(xxdrs,&len)) {ocstat = OC_EXDR; goto fail;}
	    lenz = (off_t)len;
	    lenz = RNDUP(lenz);
	    /* Skip the data */
	    xxdr_skip(xxdrs,lenz);
        }
        break;

    default:
	OCPANIC1("unexpected etype: %d",xnode->etype);

    } /* switch */

/*ok:*/
    return OCTHROW(ocstat);

fail:
    if(data->strings != NULL)
	free(data->strings);
    data->strings = NULL;
    data->ninstances = 0;
    return OCTHROW(ocstat);
}

void
ocdata_free(OCstate* state, OCdata* data)
{
    if(data == NULL)
	return;

    if(data->instances != NULL) {
	int i;
        for(i=0;i<data->ninstances;i++)
	    ocdata_free(state,data->instances[i]);
	free(data->instances);
    }
    if(data->strings != NULL)
	free(data->strings);
    free(data);
}

static OCdata*
newocdata(OCnode* pattern)
{
    OCdata* data = (OCdata*)calloc(1,sizeof(OCdata));
    MEMCHECK(data,NULL);
    data->header.magic = OCMAGIC;
    data->header.occlass = OC_Data;
    data->pattern = pattern;
    return data;
}

static int
istoplevel(OCnode* node)
{
    if(node == NULL)
	return 1; /* base case */
    if(!istoplevel(node->container))
	return 0;
    switch (node->octype) {
    case OC_Dataset: case OC_Grid: case OC_Atomic: return 1;
    case OC_Structure:
	return (node->array.rank == 0 ? 1 : 0); /* Toplevel if scalar */ 
    case OC_Sequence: default: return 0;
    }
    return 1;
}


/* XDR representation size depends on if this is scalar or not */
static off_t
ocxdrsize(OCtype etype, int isscalar)
{
    switch (etype) {
    case OC_Char:
    case OC_Byte:
    case OC_UByte:
	return (isscalar? XDRUNIT : 1);
    case OC_Int16:
    case OC_UInt16:
    case OC_Int32:
    case OC_UInt32:
    case OC_Float32:
	return XDRUNIT;
    case OC_Float64:
    case OC_Int64:
    case OC_UInt64:
	return 2*XDRUNIT;
    default:
	break; /* no simple size */
    }
    return 0;
}

#define tag "Error {\n"

static int
ocerrorstring(XXDR* xdrs)
{
    /* Check to see if the xdrs contains "Error {\n'; assume it is at the beginning of data */
    off_t avail = xxdr_getavail(xdrs);
    char* data;
    if(!xxdr_setpos(xdrs,(off_t)0)) return 0;
    data = (char*)malloc((size_t)avail);
    MEMCHECK(data,0);    
    if(!xxdr_opaque(xdrs,data,avail)) {free(data); return 0;}
    /* check for error tag at front */
    if(ocstrncmp(data,tag,sizeof(tag))==0) {
	char* p;
        if((p=strchr(data,'}')) != NULL) *(++p)='\0';
        nclog(NCLOGERR,"Server error: %s",data);
        /* Since important, report to stderr as well */
        fprintf(stderr,"Server error: %s",data);
	return 1;
    }
    return 0;
}
