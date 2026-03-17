/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information.
*/

#include "config.h"
#include "dapparselex.h"
#include "dapy.h"
#include <stddef.h>

/* Forward */

static void addedges(OCnode* node);
static void setroot(OCnode*,NClist*);
static int isglobalname(const char* name);
static int isdodsname(const char* name);
static OCnode* newocnode(char* name, OCtype octype, DAPparsestate* state);
static OCtype octypefor(Object etype);
static NClist* scopeduplicates(NClist* list);
static int check_int32(char* val, long* value);


/****************************************************/

/* Switch to DAS parsing SCAN_WORD definition */

/* Use the initial keyword to indicate what we are parsing */
void
dap_tagparse(DAPparsestate* state, int kind)
{
    switch (kind) {
    case SCAN_DATASET:
    case SCAN_ERROR:
	break;
    case SCAN_ATTR:
	dapsetwordchars(state->lexstate,1);
        break;
    default:
        fprintf(stderr,"tagparse: Unknown tag argument: %d\n",kind);
    }
}


Object
dap_datasetbody(DAPparsestate* state, Object name, Object decls)
{
    OCnode* root = newocnode((char*)name,OC_Dataset,state);
    char* dupname = NULL;
    NClist* dups = scopeduplicates((NClist*)decls);
    if(dups != NULL) {
	/* Sometimes, some servers (i.e. Thredds)
           return a dds with duplicate field names
           at the dataset level; simulate an errorbody response
        */
	ocnodes_free(dups);
        dap_parse_error(state,"Duplicate dataset field names: %s",(char*)name,dupname);
	state->error = OC_ENAMEINUSE;
	return (Object)NULL;
    }
    root->subnodes = (NClist*)decls;
    OCASSERT((state->root == NULL));
    state->root = root;
    state->root->root = state->root; /* make sure to cross link */
    addedges(root);
    setroot(root,state->ocnodes);
    return NULL;
}

Object
dap_attributebody(DAPparsestate* state, Object attrlist)
{
    OCnode* node;
    /* Check for and remove attribute duplicates */
    NClist* dups = scopeduplicates((NClist*)attrlist);
    if(dups != NULL) {
        ocnodes_free(dups);	
	dap_parse_error(state,"Duplicate attribute names in same scope");
	state->error = OC_ENAMEINUSE; /* semantic error */
	return NULL;
    }
    node = newocnode(NULL,OC_Attributeset,state);
    OCASSERT((state->root == NULL));
    state->root = node;
    /* make sure to cross link */
    state->root->root = state->root;
    node->subnodes = (NClist*)attrlist;
    addedges(node);
    return NULL;
}

void
dap_errorbody(DAPparsestate* state,
	  Object code, Object msg, Object ptype, Object prog)
{
    state->error = OC_EDAPSVC;
    state->code     = nulldup((char*)code);
    state->message  = nulldup((char*)msg);
    /* Ignore ptype and prog for now */
}

void
dap_unrecognizedresponse(DAPparsestate* state)
{
    /* see if this is an HTTP error */
    unsigned int httperr = 0;
    int i;
    char iv[32];
    (void)sscanf(state->lexstate->input,"%u ",&httperr);
    snprintf(iv,sizeof(iv),"%u",httperr);
    state->lexstate->next = state->lexstate->input;
    /* Limit the amount of input to prevent runaway */
    for(i=0;i<4096;i++) {if(state->lexstate->input[i] == '\0') break;}
    state->lexstate->input[i] = '\0';
    dap_errorbody(state,iv,state->lexstate->input,NULL,NULL);
}

Object
dap_declarations(DAPparsestate* state, Object decls, Object decl)
{
    NClist* alist = (NClist*)decls;
    if(alist == NULL)
	 alist = nclistnew();
    else
	nclistpush(alist,(void*)decl);
    return alist;
}

Object
dap_arraydecls(DAPparsestate* state, Object arraydecls, Object arraydecl)
{
    NClist* alist = (NClist*)arraydecls;
    if(alist == NULL)
	alist = nclistnew();
    else
	nclistpush(alist,(void*)arraydecl);
    return alist;
}

Object
dap_arraydecl(DAPparsestate* state, Object name, Object size)
{
    long value;
    OCnode* dim;
    if(!check_int32(size,&value)) {
	dap_parse_error(state,"Dimension not an integer");
	state->error = OC_EDIMSIZE; /* signal semantic error */
    }
    if(name != NULL)
	dim = newocnode((char*)name,OC_Dimension,state);
    else
	dim = newocnode(NULL,OC_Dimension,state);
    dim->dim.declsize = (size_t)value;
    return dim;
}

Object
dap_attrlist(DAPparsestate* state, Object attrlist, Object attrtuple)
{
    NClist* alist = (NClist*)attrlist;
    if(alist == NULL)
	alist = nclistnew();
    else {
	if(attrtuple != NULL) {/* NULL=>alias encountered, ignore */
            nclistpush(alist,(void*)attrtuple);
	}
    }
    return alist;
}

Object
dap_attrvalue(DAPparsestate* state, Object valuelist, Object value, Object etype)
{
    NClist* alist = (NClist*)valuelist;
    if(alist == NULL) alist = nclistnew();
    /* Watch out for null values */
    if(value == NULL) value = "";
    nclistpush(alist,(void*)strdup(value));
    return alist;
}

Object
dap_attribute(DAPparsestate* state, Object name, Object values, Object etype)
{
    OCnode* att;
    att = newocnode((char*)name,OC_Attribute,state);
    att->etype = octypefor(etype);
    att->att.values = (NClist*)values;
    return att;
}

Object
dap_attrset(DAPparsestate* state, Object name, Object attributes)
{
    OCnode* attset;
    attset = newocnode((char*)name,OC_Attributeset,state);
    /* Check var set vs global set */
    attset->att.isglobal = isglobalname(name);
    attset->att.isdods = isdodsname(name);
    attset->subnodes = (NClist*)attributes;
    addedges(attset);
    return attset;
}

static int
isglobalname(const char* name)
{
    size_t len = strlen(name);
    size_t glen = strlen("global");
    const char* p;
    if(len < glen) return 0;
    p = name + (len - glen);
    if(strcasecmp(p,"global") != 0)
	return 0;
    return 1;
}

static int
isdodsname(const char* name)
{
    size_t len = strlen(name);
    size_t glen = strlen("DODS");
    if(len < glen) return 0;
    if(ocstrncmp(name,"DODS",glen) != 0)
	return 0;
    return 1;
}

#if 0
static int
isnumber(const char* text)
{
    for(;*text;text++) {if(!isdigit(*text)) return 0;}
    return 1;
}
#endif

static void
dimension(OCnode* node, NClist* dimensions)
{
    size_t i;
    size_t rank = nclistlength(dimensions);
    node->array.dimensions = (NClist*)dimensions;
    node->array.rank = rank;
    for(i=0;i<rank;i++) {
        OCnode* dim = (OCnode*)nclistget(node->array.dimensions,i);
        dim->dim.array = node;
	dim->dim.arrayindex = i;
#if 0
	if(dim->name == NULL) {
	    dim->dim.anonymous = 1;
	    dim->name = dimnameanon(node->name,i);
	}
#endif
    }
}

char*
dimnameanon(char* basename, unsigned int index)
{
    char name[64];
    snprintf(name,sizeof(name),"%s_%d",basename,index);
    return strdup(name);
}

Object
dap_makebase(DAPparsestate* state, Object name, Object etype, Object dimensions)
{
    OCnode* node;
    node = newocnode((char*)name,OC_Atomic,state);
    node->etype = octypefor(etype);
    dimension(node,(NClist*)dimensions);
    return node;
}

Object
dap_makestructure(DAPparsestate* state, Object name, Object dimensions, Object fields)
{
    OCnode* node;
    NClist* dups = scopeduplicates((NClist*)fields);
    if(dups != NULL) {
	ocnodes_free(dups);
        dap_parse_error(state,"Duplicate structure field names in same structure: %s",(char*)name);
	state->error = OC_ENAMEINUSE; /* semantic error */
	return (Object)NULL;
    }
    node = newocnode(name,OC_Structure,state);
    node->subnodes = fields;
    dimension(node,(NClist*)dimensions);
    addedges(node);
    return node;
}

Object
dap_makesequence(DAPparsestate* state, Object name, Object members)
{
    OCnode* node;
    NClist* dups = scopeduplicates((NClist*)members);
    if(dups != NULL) {
	ocnodes_free(dups);
        dap_parse_error(state,"Duplicate sequence member names in same sequence: %s",(char*)name);
	return (Object)NULL;
    }
    node = newocnode(name,OC_Sequence,state);
    node->subnodes = members;
    addedges(node);
    return node;
}

Object
dap_makegrid(DAPparsestate* state, Object name, Object arraydecl, Object mapdecls)
{
    OCnode* node;
    /* Check for duplicate map names */
    NClist* dups = scopeduplicates((NClist*)mapdecls);
    if(dups != NULL) {
	ocnodes_free(dups);
        dap_parse_error(state,"Duplicate grid map names in same grid: %s",(char*)name);
	state->error = OC_ENAMEINUSE; /* semantic error */
	return (Object)NULL;
    }
    node = newocnode(name,OC_Grid,state);
    node->subnodes = (NClist*)mapdecls;
    nclistinsert(node->subnodes,0,(void*)arraydecl);
    addedges(node);
    return node;
}

static void
addedges(OCnode* node)
{
    unsigned int i;
    if(node->subnodes == NULL) return;
    for(i=0;i<nclistlength(node->subnodes);i++) {
        OCnode* subnode = (OCnode*)nclistget(node->subnodes,i);
	subnode->container = node;
    }
}

static void
setroot(OCnode* root, NClist* ocnodes)
{
    size_t i;
    for(i=0;i<nclistlength(ocnodes);i++) {
	OCnode* node = (OCnode*)nclistget(ocnodes,i);
	node->root = root;
    }
}

int
daperror(DAPparsestate* state, const char* msg)
{
    return dapsemanticerror(state,OC_EINVAL,msg);
}

int
dapsemanticerror(DAPparsestate* state, OCerror err, const char* msg)
{
    dap_parse_error(state,msg);
    state->error = err; /* semantic error */
    return 0;
}

static char*
flatten(char* s, char* tmp, size_t tlen)
{
    char c;
    char* p,*q;
    strncpy(tmp,s,tlen);
    tmp[tlen] = '\0';
    p = (q = tmp);
    while((c=*p++)) {
	switch (c) {
	case '\r': case '\n': break;
	case '\t': *q++ = ' '; break;
	case ' ': if(*p != ' ') *q++ = c; break;
	default: *q++ = c;
	}
    }
    *q = '\0';
    return tmp;
}

/* Create an ocnode and capture in the state->ocnode list */
static OCnode*
newocnode(char* name, OCtype octype, DAPparsestate* state)
{
    OCnode* node = ocnode_new(name,octype,state->root);
    nclistpush(state->ocnodes,(void*)node);
    return node;
}

static int
check_int32(char* val, long* value)
{
    char* ptr;
    int ok = 1;
    long iv = strtol(val,&ptr,0); /* 0=>auto determine base */
    if((iv == 0 && val == ptr) || *ptr != '\0') {ok=0; iv=1;}
    else if(iv > OC_INT32_MAX || iv < OC_INT32_MIN) ok=0;
    if(value != NULL) *value = iv;
    return ok;
}

static NClist*
scopeduplicates(NClist* list)
{
    unsigned int i,j;
    size_t len = nclistlength(list);
    NClist* dups = NULL;
    for(i=0;i<len;i++) {
	OCnode* io = (OCnode*)nclistget(list,i);
retry:
        for(j=i+1;j<len;j++) {
	    OCnode* jo = (OCnode*)nclistget(list,j);
	    if(strcmp(io->name,jo->name)==0) {
		if(dups == NULL) dups = nclistnew();
		nclistpush(dups,jo);
		nclistremove(list,j);
		len--;
		goto retry;
	    }
	}
    }
    return dups;
}

static OCtype
octypefor(Object etype)
{
    switch ((uintptr_t)etype) {
    case SCAN_BYTE: return OC_Byte;
    case SCAN_INT16: return OC_Int16;
    case SCAN_UINT16: return OC_UInt16;
    case SCAN_INT32: return OC_Int32;
    case SCAN_UINT32: return OC_UInt32;
    case SCAN_FLOAT32: return OC_Float32;
    case SCAN_FLOAT64: return OC_Float64;
    case SCAN_URL: return OC_URL;
    case SCAN_STRING: return OC_String;
    default: abort();
    }
    return OC_NAT;
}

void
dap_parse_error(DAPparsestate* state, const char *fmt, ...)
{
    size_t len, suffixlen, prefixlen;
    va_list argv;
    char* tmp = NULL;
    va_start(argv,fmt);
    (void) vfprintf(stderr,fmt,argv) ;
    (void) fputc('\n',stderr) ;
    len = strlen(state->lexstate->input);
    suffixlen = strlen(state->lexstate->next);
    prefixlen = (len - suffixlen);
    tmp = (char*)ocmalloc(len+1);
    flatten(state->lexstate->input,tmp,prefixlen);
    (void) fprintf(stderr,"context: %s",tmp);
    flatten(state->lexstate->next,tmp,suffixlen);
    (void) fprintf(stderr,"^%s\n",tmp);
    (void) fflush(stderr);	/* to ensure log files are current */
    ocfree(tmp);
    va_end(argv);
}

static void
dap_parse_cleanup(DAPparsestate* state)
{
    daplexcleanup(&state->lexstate);
    if(state->ocnodes != NULL) ocnodes_free(state->ocnodes);
    state->ocnodes = NULL;
    nullfree(state->code);
    nullfree(state->message);
    free(state);
}

static DAPparsestate*
dap_parse_init(char* buf)
{
    DAPparsestate* state = (DAPparsestate*)ocmalloc(sizeof(DAPparsestate)); /*ocmalloc zeros*/
    MEMCHECK(state,NULL);
    if(buf==NULL) {
        dap_parse_error(state,"dap_parse_init: no input buffer");
	state->error = OC_EINVAL; /* semantic error */
	dap_parse_cleanup(state);
	return NULL;
    }
    daplexinit(buf,&state->lexstate);
    return state;
}

/* Wrapper for dapparse */
OCerror
DAPparse(OCstate* conn, OCtree* tree, char* parsestring)
{
    DAPparsestate* state = dap_parse_init(parsestring);
    int parseresult;
    OCerror ocerr = OC_NOERR;
    state->ocnodes = nclistnew();
    state->conn = conn;
    if(ocdebug >= 2)
	dapdebug = 1;
    parseresult = dapparse(state);
    if(parseresult == 0) {/* 0 => parse ok */
	if(state->error == OC_EDAPSVC) {
	    /* we ended up parsing an error message from server */
            conn->error.code = nulldup(state->code);
            conn->error.message = nulldup(state->message);
	    tree->root = NULL;
	    /* Attempt to further decipher the error code */
	    if(state->code != NULL
                && (strcmp(state->code,"404") == 0 /* tds returns 404 */
		    || strcmp(state->code,"5") == 0)) /* hyrax returns 5 */
		ocerr = OC_ENOFILE;
	    else
	        ocerr = OC_EDAPSVC;
	} else if(state->error != OC_NOERR) {
	    /* Parse failed for semantic reasons */
	    ocerr = state->error;
	} else {
            tree->root = state->root;
	    state->root = NULL; /* avoid reclaim */
            tree->nodes = state->ocnodes;
	    state->ocnodes = NULL; /* avoid reclaim */
            tree->root->tree = tree;
	    ocerr = OC_NOERR;
	}
    } else { /* Parse failed */
	switch (tree->dxdclass) {
	case OCDAS: ocerr = OC_EDAS; break;
	case OCDDS: ocerr = OC_EDDS; break;
	case OCDATADDS: ocerr = OC_EDATADDS; break;
	default: ocerr = OC_EDAPSVC;
	}		
    }
    dap_parse_cleanup(state);
    return ocerr;
}

