/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "d4includes.h"
#include <stdarg.h>
#include <assert.h>
#include <stddef.h>
#include "ncxml.h"

/**
 * Implement the Dap4 Parser Using a DOM Parser
 *
 * This code creates in internal representation of the netcdf-4 metadata
 * to avoid having to make so many calls into the netcdf library.
 */

/***************************************************/

/*
Map an xml node name to an interpretation.
We use NCD4_NULL when we directly interpret
the tag and do not need to search for it.
E.g Dataset, Dim, econst, etc.
If the sort is NCD4_NULL, then that means it is
irrelevant because that keyword will never be
searched for in this table.
*/
static const struct KEYWORDINFO {
    char* tag; /* The xml tag e.g. <tag...> */
    NCD4sort sort; /* What kind of node are we building */
    nc_type subsort; /* discriminator */
    char* aliasfor; /* Some names are aliases for others */
} keywordmap[] = {
{"Attribute", NCD4_ATTR,NC_NAT,NULL},
{"Byte", NCD4_VAR,NC_BYTE,"Int8"},
{"Char", NCD4_VAR,NC_CHAR,NULL},
{"Dataset", NCD4_NULL,NC_NAT,NULL},
{"Dim", NCD4_NULL,NC_NAT,NULL},
{"Dimension", NCD4_DIM,NC_NAT,NULL},
{"Enum", NCD4_VAR,NC_ENUM,NULL},
{"Enumconst", NCD4_NULL,NC_NAT,NULL},
{"Enumeration", NCD4_TYPE,NC_ENUM,NULL},
{"Float32", NCD4_VAR,NC_FLOAT,NULL},
{"Float64", NCD4_VAR,NC_DOUBLE,NULL},
{"Group", NCD4_GROUP,NC_NAT,NULL},
{"Int16", NCD4_VAR,NC_SHORT,NULL},
{"Int32", NCD4_VAR,NC_INT,NULL},
{"Int64", NCD4_VAR,NC_INT64,NULL},
{"Int8", NCD4_VAR,NC_BYTE,NULL},
{"Map", NCD4_NULL,NC_NAT,NULL},
{"Opaque", NCD4_VAR,NC_OPAQUE,NULL},
{"OtherXML", NCD4_XML,NC_NAT,NULL},
{"Sequence", NCD4_VAR,NC_SEQ,NULL},
{"String", NCD4_VAR,NC_STRING,NULL},
{"Structure", NCD4_VAR,NC_STRUCT,NULL},
{"UByte", NCD4_VAR,NC_UBYTE,"UInt8"},
{"UInt16", NCD4_VAR,NC_USHORT,NULL},
{"UInt32", NCD4_VAR,NC_UINT,NULL},
{"UInt64", NCD4_VAR,NC_UINT64,NULL},
{"UInt8", NCD4_VAR,NC_UBYTE,NULL},
{"URL", NCD4_VAR,NC_STRING,"String"},
{"Url", NCD4_VAR,NC_STRING,"String"},
};
typedef struct KEYWORDINFO KEYWORDINFO;

/* Warning do not make const because sort will modify */
static struct ATOMICTYPEINFO {
    char* name; nc_type type; size_t size;
} atomictypeinfo[] = {
/* Will be sorted on first use */
/* Use lower case for canonical comparison, but keep proper name here */
{"Byte",NC_BYTE,sizeof(char)},
{"Char",NC_CHAR,sizeof(char)},
{"Float32",NC_FLOAT,sizeof(float)},
{"Float64",NC_DOUBLE,sizeof(double)},
{"Int16",NC_SHORT,sizeof(short)},
{"Int32",NC_INT,sizeof(int)},
{"Int64",NC_INT64,sizeof(long long)},
{"Int8",NC_BYTE,sizeof(char)},
{"String",NC_STRING,sizeof(char*)},
{"UByte",NC_UBYTE,sizeof(unsigned char)},
{"UInt16",NC_USHORT,sizeof(unsigned short)},
{"UInt32",NC_UINT,sizeof(unsigned int)},
{"UInt64",NC_UINT64,sizeof(unsigned long long)},
{"UInt8",NC_UBYTE,sizeof(unsigned char)},
{"Url",NC_STRING,sizeof(char*)},
};
#define NCD4_NATOMICTYPES (sizeof(atomictypeinfo)/sizeof(struct ATOMICTYPEINFO))
static int atomictypessorted = 0;

/***************************************************/

#ifdef D4DEBUG
static void setname(NCD4node* node, const char* name)
{
    nullfree(node->name);
    (node)->name = strdup(name); \
    fprintf(stderr,"setname: node=%lx name=%s\n",(unsigned long)(node),(node)->name); \
}
#define SETNAME(node,src) setname((node),src)
#else
#define SETNAME(node,src) do {nullfree((node)->name); (node)->name = strdup(src);} while(0);
#endif

/***************************************************/

/* Forwards */

static int addOrigType(NCD4parser*, NCD4node* src, NCD4node* dst, const char* tag);
static int defineAtomicTypes(NCD4meta*,NClist*);
static void classify(NCD4node* container, NCD4node* node);
static int convertString(union ATOMICS*, NCD4node* type, const char* s);
static int downConvert(union ATOMICS*, NCD4node* type);
static int fillgroup(NCD4parser*, NCD4node* group, ncxml_t xml);
static NCD4node* getOpaque(NCD4parser*, ncxml_t varxml, NCD4node* group);
static int getValueStrings(NCD4parser*, NCD4node*, ncxml_t xattr, NClist*);
static int isReserved(const char* name);
static const KEYWORDINFO* keyword(const char* name);
static NCD4node* lookupAtomicType(NClist*,const char* name);
static NCD4node* lookFor(NClist* elems, const char* name, NCD4sort sort);
static NCD4node* lookupFQN(NCD4parser*, const char* sfqn, NCD4sort);
static int lookupFQNList(NCD4parser*, NClist* fqn, NCD4sort sort, NCD4node** result);
static NCD4node* makeAnonDim(NCD4parser*, const char* sizestr);
static int makeNode(NCD4parser*, NCD4node* parent, ncxml_t, NCD4sort, nc_type, NCD4node**);
static int makeNodeStatic(NCD4meta* meta, NCD4node* parent, NCD4sort sort, nc_type subsort, NCD4node** nodep);
static int parseAtomicVar(NCD4parser*, NCD4node* container, ncxml_t xml, NCD4node**);
static int parseEnumVar(NCD4parser*, NCD4node* container, ncxml_t xml, NCD4node**);
static int parseAttributes(NCD4parser*, NCD4node* container, ncxml_t xml);
static int parseDimensions(NCD4parser*, NCD4node* group, ncxml_t xml);
static int parseDimRefs(NCD4parser*, NCD4node* var, ncxml_t xml);
static int parseEconsts(NCD4parser*, NCD4node* en, ncxml_t xml);
static int parseEnumerations(NCD4parser*, NCD4node* group, ncxml_t dom);
static int parseFields(NCD4parser*, NCD4node* container, ncxml_t xml);
static int parseError(NCD4parser*, ncxml_t errxml);
static int parseGroups(NCD4parser*, NCD4node* group, ncxml_t dom);
static int parseMaps(NCD4parser*, NCD4node* var, ncxml_t xml);
static int parseMetaData(NCD4parser*, NCD4node* node, ncxml_t xml);
static int parseStructure(NCD4parser*, NCD4node* container, ncxml_t dom, NCD4node**);
static int parseSequence(NCD4parser*, NCD4node* container, ncxml_t dom,NCD4node**);
static int parseLL(const char* text, long long*);
static int parseULL(const char* text, unsigned long long*);
static int parseVariables(NCD4parser*, NCD4node* group, ncxml_t xml);
static int parseVariable(NCD4parser*, NCD4node* group, ncxml_t xml, NCD4node**);
static void reclaimParser(NCD4parser* parser);
static void record(NCD4parser*, NCD4node* node);
static int splitOrigType(NCD4parser*, const char* fqn, NCD4node* var);
static void track(NCD4meta*, NCD4node* node);
static int traverse(NCD4parser*, ncxml_t dom);
static int parseForwards(NCD4parser* parser, NCD4node* root);
#ifndef FIXEDOPAQUE
static int defineBytestringType(NCD4parser*);
#endif

/***************************************************/
/* API */

int
NCD4_parse(NCD4meta* metadata, NCD4response* resp, int dapparse)
{
    int ret = NC_NOERR;
    NCD4parser* parser = NULL;
    ncxml_doc_t doc = NULL;
    ncxml_t dom = NULL;

    /* fill in the atomic types for meta*/
    metadata->atomictypes = nclistnew();
    if((ret=defineAtomicTypes(metadata,metadata->atomictypes))) goto done;

    /* Create and fill in the parser state */
    parser = (NCD4parser*)calloc(1,sizeof(NCD4parser));
    if(parser == NULL) {ret=NC_ENOMEM; goto done;}
    parser->controller = metadata->controller;
    parser->metadata = metadata;
    parser->response = resp;
    doc = ncxml_parse(parser->response->serial.dmr,strlen(parser->response->serial.dmr));
    if(doc == NULL) {ret=NC_ENOMEM; goto done;}
    dom = ncxml_root(doc);
    parser->types = nclistnew();
    parser->dims = nclistnew();
    parser->vars = nclistnew();
#ifdef D4DEBUG
    parser->debuglevel = 1;
#endif
    parser->dapparse = dapparse;

    /*Walk the DOM tree to build the DAP4 node tree*/
    ret = traverse(parser,dom);

done:
    if(doc != NULL)
	ncxml_free(doc);
    doc = NULL;
    reclaimParser(parser);
    return THROW(ret);
}

static void
reclaimParser(NCD4parser* parser)
{
    if(parser == NULL) return;
    nclistfree(parser->types);
    nclistfree(parser->dims);
    nclistfree(parser->vars);
    nclistfree(parser->groups);
    free (parser);
}

/**************************************************/

/* Recursively walk the DOM tree to create the metadata */
static int
traverse(NCD4parser* parser, ncxml_t dom)
{
    int ret = NC_NOERR;

    /* See if we have an <Error> or <Dataset> */
    if(strcmp(ncxml_name(dom),"Error")==0) {
	ret=parseError(parser,dom);
        /* Report the error */
	fprintf(stderr,"DAP4 Error: http-code=%d message=\"%s\" context=\"%s\"\n",
		parser->response->error.httpcode,
		parser->response->error.message,
		parser->response->error.context);
	fflush(stderr);
	ret=NC_EDMR;
	goto done;
    } else if(strcmp(ncxml_name(dom),"Dataset")==0) {
	char* xattr = NULL;
        if((ret=makeNode(parser,NULL,NULL,NCD4_GROUP,NC_NULL,&parser->metadata->root))) goto done;
        parser->metadata->root->group.isdataset = 1;
        parser->metadata->root->meta.id = parser->metadata->ncid;
        if(parser->metadata->groupbyid == NULL)
	    parser->metadata->groupbyid = nclistnew();
        SETNAME(parser->metadata->root,"/");
	xattr = ncxml_attr(dom,"name");
	if(xattr != NULL) parser->metadata->root->group.datasetname = xattr;
	xattr = ncxml_attr(dom,"dapVersion");
	if(xattr != NULL) parser->metadata->root->group.dapversion = xattr;
	xattr = ncxml_attr(dom,"dmrVersion");
	if(xattr != NULL) parser->metadata->root->group.dmrversion = xattr;
        /* Recursively walk the tree */
        if((ret = fillgroup(parser,parser->metadata->root,dom))) goto done;

        /* Walk a second time to parse allowed forward refs:
	   1. <Map>
	*/
        if((ret = parseForwards(parser,parser->metadata->root))) goto done;

    } else
	FAIL(NC_EINVAL,"Unexpected dom root name: %s",ncxml_name(dom));
done:
    return THROW(ret);
}

static int
fillgroup(NCD4parser* parser, NCD4node* group, ncxml_t xml)
{
    int ret = NC_NOERR;

    /* Extract Dimensions */
    if((ret = parseDimensions(parser,group,xml))) goto done;
    /* Extract Enum types */
    if((ret = parseEnumerations(parser,group,xml))) goto done;
    /* Extract variables */
    if((ret = parseVariables(parser,group,xml))) goto done;
    /* Extract subgroups*/
    if((ret = parseGroups(parser,group,xml))) goto done;
    /* Parse group level attributes */
    if((ret = parseAttributes(parser,group,xml))) goto done;
done:
    return THROW(ret);
}

static int
parseDimensions(NCD4parser* parser, NCD4node* group, ncxml_t xml)
{
    int ret = NC_NOERR;
    ncxml_t x;
    for(x=ncxml_child(xml, "Dimension");x != NULL;x = ncxml_next(x,"Dimension")) {
	NCD4node* dimnode = NULL;
	unsigned long long size;
	char* sizestr;
	char* unlimstr;
	sizestr = ncxml_attr(x,"size");
	if(sizestr == NULL)
	    FAIL(NC_EDIMSIZE,"Dimension has no size");
	unlimstr = ncxml_attr(x,UCARTAGUNLIM);
	if((ret = parseULL(sizestr,&size))) goto done;
	nullfree(sizestr);
	if((ret=makeNode(parser,group,x,NCD4_DIM,NC_NULL,&dimnode))) goto done;
	dimnode->dim.size = size;
	dimnode->dim.isunlimited = (unlimstr != NULL);
        nullfree(unlimstr);
	/* Process attributes */
	if((ret = parseAttributes(parser,dimnode,x))) goto done;
	classify(group,dimnode);
    }
done:
    return THROW(ret);
}

static int
parseEnumerations(NCD4parser* parser, NCD4node* group, ncxml_t xml)
{
    int ret = NC_NOERR;
    ncxml_t x;

    for(x=ncxml_child(xml, "Enumeration");x != NULL;x = ncxml_next(x, "Enumeration")) {
	NCD4node* node = NULL;
	NCD4node* basetype = NULL;
	 char* fqn = ncxml_attr(x,"basetype");
	basetype = lookupFQN(parser,fqn,NCD4_TYPE);
	if(basetype == NULL) {
	    FAIL(NC_EBADTYPE,"Enumeration has unknown type: ",fqn);
	}
	nullfree(fqn);
	if((ret=makeNode(parser,group,x,NCD4_TYPE,NC_ENUM,&node))) goto done;
	node->basetype = basetype;
	if((ret=parseEconsts(parser,node,x))) goto done;
	if(nclistlength(node->en.econsts) == 0)
	    FAIL(NC_EINVAL,"Enumeration has no values");
	classify(group,node);
	/* Finally, see if this type has UCARTAGORIGTYPE xml attribute */
	if(parser->metadata->controller->controls.translation == NCD4_TRANSNC4) {
	    char* typetag = ncxml_attr(x,UCARTAGORIGTYPE);
	    if(typetag != NULL) {
	    }
	    nullfree(typetag);
	}
    }
done:
    return THROW(ret);
}

static int
parseEconsts(NCD4parser* parser, NCD4node* en, ncxml_t xml)
{
    int ret = NC_NOERR;
    ncxml_t x;
    NClist* econsts = nclistnew();

    for(x=ncxml_child(xml, "EnumConst");x != NULL;x = ncxml_next(x, "EnumConst")) {
        NCD4node* ec = NULL;
	char* name;
	char* svalue;
	name = ncxml_attr(x,"name");
	if(name == NULL) FAIL(NC_EBADNAME,"Enum const with no name");
	if((ret=makeNode(parser,en,x,NCD4_ECONST,NC_NULL,&ec))) goto done	;
	nullfree(name);
	svalue = ncxml_attr(x,"value");
	if(svalue == NULL)
	    FAIL(NC_EINVAL,"Enumeration Constant has no value");
	if((ret=convertString(&ec->en.ecvalue,en->basetype,svalue)))
	    FAIL(NC_EINVAL,"Non-numeric Enumeration Constant: %s->%s",ec->name,svalue);
	nullfree(svalue);
	PUSH(econsts,ec);
    }
    en->en.econsts = econsts;
done:
    return THROW(ret);
}

static int
parseVariables(NCD4parser* parser, NCD4node* group, ncxml_t xml)
{
    int ret = NC_NOERR;
    ncxml_t x;
    for(x=ncxml_child_first(xml);x != NULL;x=ncxml_child_next(x)) {
	NCD4node* node = NULL;
	const KEYWORDINFO* info = keyword(ncxml_name(x));
	if(info == NULL)
	    FAIL(NC_ETRANSLATION,"Unexpected node type: %s",ncxml_name(x));
	/* Check if we need to process this node */
	if(!ISVAR(info->sort)) continue; /* Handle elsewhere */
	node = NULL;
	ret = parseVariable(parser,group,x,&node);
        if(ret != NC_NOERR || node == NULL) break;
    }
done:
    return THROW(ret);
}

static int
parseVariable(NCD4parser* parser, NCD4node* container, ncxml_t xml, NCD4node** nodep)
{
    int ret = NC_NOERR;
    NCD4node* node = NULL;
    const KEYWORDINFO* info = keyword(ncxml_name(xml));

    switch (info->subsort) {
    case NC_STRUCT:
	ret = parseStructure(parser,container,xml,&node);
	break;
    case NC_SEQ:
	ret = parseSequence(parser,container,xml,&node);
	break;
    case NC_ENUM:
	ret = parseEnumVar(parser,container,xml,&node);
	break;
    default:
	ret = parseAtomicVar(parser,container,xml,&node);
    }
    if(ret == NC_NOERR) {
        *nodep = node;
    }
    return THROW(ret);
}

static int
parseMetaData(NCD4parser* parser, NCD4node* container, ncxml_t xml)
{
    int ret = NC_NOERR;
    /* Process dimrefs */
    if((ret=parseDimRefs(parser,container,xml))) goto done;
    /* Process attributes */
    if((ret = parseAttributes(parser,container,xml))) goto done;
    /* Process maps */
    if((ret = parseMaps(parser,container,xml))) goto done;
done:
    return THROW(ret);
}

static int
parseStructure(NCD4parser* parser, NCD4node* container, ncxml_t xml, NCD4node** nodep)
{
    int ret = NC_NOERR;
    NCD4node* var = NULL;
    NCD4node* type = NULL;
    NCD4node* group = NULL;
    char* fqnname = NULL;

    group = NCD4_groupFor(container); /* default: put type in the same group as var */

    /* Make the structure as a variable with same name as structure; will be fixed later */
    if((ret=makeNode(parser,container,xml,NCD4_VAR,NC_STRUCT,&var))) goto done;
    classify(container,var);

    /* Make the structure as a type with (for now) partial fqn name from the variable */
    if((ret=makeNode(parser,group,xml,NCD4_TYPE,NC_STRUCT,&type))) goto done;
    classify(group,type);
    /* Set the basetype */
    var->basetype = type;
    /* Now change the struct typename */
    fqnname = NCD4_makeName(var,"_");
    if(fqnname == NULL)
	FAIL(NC_ENOMEM,"Out of memory");
    SETNAME(type,fqnname);

    /* Parse Fields into the type */
    if((ret = parseFields(parser,type,xml))) goto done;

    /* Parse attributes, dims, and maps into the var */
    if((ret = parseMetaData(parser,var,xml))) goto done;

    /* See if this var has UCARTAGORIGTYPE attribute */
    if(parser->metadata->controller->controls.translation == NCD4_TRANSNC4) {
	char* typetag = ncxml_attr(xml,UCARTAGORIGTYPE);
	if(typetag != NULL) {
	    /* yes, place it on the type */
	    if((ret=addOrigType(parser,var,type,typetag))) goto done;
	    nullfree(typetag);
 	}
    }

    if(nodep) *nodep = var;

done:
    nullfree(fqnname);
    return THROW(ret);
}

static int
parseFields(NCD4parser* parser, NCD4node* container, ncxml_t xml)
{
    int ret = NC_NOERR;
    ncxml_t x;
    for(x=ncxml_child_first(xml);x != NULL;x=ncxml_child_next(x)) {
    	NCD4node* node = NULL;
        const KEYWORDINFO* info = keyword(ncxml_name(x));
	if(!ISVAR(info->sort)) continue; /* not a field */
	ret = parseVariable(parser,container,x,&node);
	if(ret) goto done;
    }
done:
    return THROW(ret);
}

/*
Specialized version of parseFields that is used
to attach a singleton field to a vlentype
*/
static int
parseVlenField(NCD4parser* parser, NCD4node* container, ncxml_t xml, NCD4node** fieldp)
{
    int ret = NC_NOERR;
    NCD4node* field = NULL;
    ncxml_t x;
    for(x=ncxml_child_first(xml);x != NULL;x=ncxml_child_next(x)) {
        const KEYWORDINFO* info = keyword(ncxml_name(x));
	if(!ISVAR(info->sort)) continue; /* not a field */
	if(field != NULL)
	    {ret = NC_EBADTYPE; goto done;}
	if((ret = parseVariable(parser,container,x,&field)))
	    goto done;
    }
    if(field == NULL) {ret = NC_EBADTYPE; goto done;}
    if(fieldp) *fieldp = field;
done:
    return THROW(ret);
}

static int
parseSequence(NCD4parser* parser, NCD4node* container, ncxml_t xml, NCD4node** nodep)
{
    int ret = NC_NOERR;
    NCD4node* var = NULL;
    NCD4node* structtype = NULL;
    NCD4node* vlentype = NULL;
    NCD4node* group = NULL;
    char name[NC_MAX_NAME];
    char* fqnname = NULL;
    int usevlen = 0;

    group = NCD4_groupFor(container);

    /* Convert a sequence variable into two or three things:
	1. a compound type representing the fields of the sequence.
	2. a vlen type whose basetype is #1
	3. a variable whose basetype is #2.
	If we can infer that the sequence was riginally produced
	from a netcdf-4 vlen, then we can avoid creating #1.
	Naming is as follows. Assume the var name is V
	and the NCD4_makeName of the var is V1..._Vn.
	1. var name is V.
	2. vlen type is V1..._VN_t
	3. compound type (if any) is V1..._VN_cmpd (Note, d4meta will append _t to this)
     */

    /* Determine if we need to build a structure type or can go straight to a vlen
       Test:  UCARTAGVLEN xml attribute is set
    */
    if(parser->metadata->controller->controls.translation == NCD4_TRANSNC4) {
	char* vlentag = ncxml_attr(xml,UCARTAGVLEN);
	if(vlentag != NULL)
	    usevlen = 1;
	nullfree(vlentag);
    } else
	usevlen = 0;

    /* make secondary names from the var fqn name */
    if(usevlen) {
	/* Parse the singleton field and then use it to fix up the var */
	if((ret=parseVlenField(parser,container,xml,&var)))
	    goto done;
	/* compute a partial fqn */
        fqnname = NCD4_makeName(var,"_");
        if(fqnname == NULL)
	    {ret = NC_ENOMEM; goto done;}
	/* Now, create the vlen type using the field's basetype */
        if((ret=makeNode(parser,group,xml,NCD4_TYPE,NC_SEQ,&vlentype))) goto done;
        classify(group,vlentype);
	vlentype->basetype = var->basetype;
	/* Use name <fqnname>_t */
	strncpy(name,fqnname,sizeof(name));
	strlcat(name,"_t", sizeof(name));
        SETNAME(vlentype,name);
        /* Set the basetype */
        var->basetype = vlentype;
    } else {
	/* Start by creating the var node; will be fixed up later */
	if((ret=makeNode(parser,container,xml,NCD4_VAR,NC_SEQ,&var))) goto done;
	classify(container,var);
        fqnname = NCD4_makeName(var,"_");
        if(fqnname == NULL)
	    {ret = NC_ENOMEM; goto done;}
        if((ret=makeNode(parser,group,xml,NCD4_TYPE,NC_STRUCT,&structtype))) goto done;
        classify(group,structtype);
	/* Use name <fqnname>_base */
	strncpy(name,fqnname,sizeof(name));
	strlcat(name,"_base", sizeof(name));
        SETNAME(structtype,name);
        /* Parse Fields into type */
        if((ret = parseFields(parser,structtype,xml))) goto done;
	/* Create a seq type whose basetype is the compound type */
        if((ret=makeNode(parser,group,xml,NCD4_TYPE,NC_SEQ,&vlentype))) goto done;
        classify(group,vlentype);
	/* Use name <xname>_t */
	strncpy(name,fqnname,sizeof(name));
	strlcat(name,"_t", sizeof(name));
        SETNAME(vlentype,name);
	vlentype->basetype = structtype;
        /* Set the basetype */
        var->basetype = vlentype;
    }

    /* Parse attributes, dims, and maps into var*/
    if((ret = parseMetaData(parser,var,xml))) goto done;

    /* See if this var has UCARTAGORIGTYPE attribute */
    if(parser->metadata->controller->controls.translation == NCD4_TRANSNC4) {
	char* typetag = ncxml_attr(xml,UCARTAGORIGTYPE);
	if(typetag != NULL) {
	    /* yes, place it on the type */
	    if((ret=addOrigType(parser,var,vlentype,typetag))) goto done;
	    nullfree(typetag);
 	}
    }
    if(nodep) *nodep = var;

done:
    if(fqnname) free(fqnname);
    return THROW(ret);
}

static int
parseGroups(NCD4parser* parser, NCD4node* parent, ncxml_t xml)
{
    int ret = NC_NOERR;
    ncxml_t x;
    for(x=ncxml_child(xml, "Group");x != NULL;x = ncxml_next(x,"Group")) {
	NCD4node* group = NULL;
	char* name = ncxml_attr(x,"name");
	if(name == NULL) FAIL(NC_EBADNAME,"Group has no name");
	nullfree(name);
	if((ret=makeNode(parser,parent,x,NCD4_GROUP,NC_NULL,&group))) goto done;
	group->group.varbyid = nclistnew();
	classify(parent,group);
        if((ret = fillgroup(parser,group,x))) goto done;
        /* Parse group attributes */
        if((ret = parseAttributes(parser,group,x))) goto done;
    }
done:
    return THROW(ret);
}

static int
parseEnumVar(NCD4parser* parser, NCD4node* container, ncxml_t xml, NCD4node** nodep)
{
    int ret = NC_NOERR;
    NCD4node* node = NULL;
    NCD4node* base = NULL;
    const char* typename;
    const KEYWORDINFO* info;
    char* enumfqn;

    /* Check for aliases */
    for(typename=ncxml_name(xml);;) {
	info = keyword(typename);
	if(info->aliasfor == NULL) break;
	typename = info->aliasfor;
    }
    assert(info->subsort == NC_ENUM);
    enumfqn = ncxml_attr(xml,"enum");
    if(enumfqn == NULL)
	base = NULL;
    else
	base = lookupFQN(parser,enumfqn,NCD4_TYPE);
    nullfree(enumfqn);
    if(base == NULL || !ISTYPE(base->sort)) {
	FAIL(NC_EBADTYPE,"Unexpected variable type: %s",info->tag);
    }
    if((ret=makeNode(parser,container,xml,NCD4_VAR,base->subsort,&node))) goto done;
    classify(container,node);
    node->basetype = base;
    /* Parse attributes, dims, and maps */
    if((ret = parseMetaData(parser,node,xml))) goto done;
    /* See if this var has UCARTAGORIGTYPE attribute */
    if(parser->metadata->controller->controls.translation == NCD4_TRANSNC4) {
	char* typetag = ncxml_attr(xml,UCARTAGORIGTYPE);
	if(typetag != NULL) {
	    /* yes, place it on the type */
	    if((ret=addOrigType(parser,node,node,typetag))) goto done;
	    nullfree(typetag);
 	}
    }
    if(nodep) *nodep = node;
done:
    return THROW(ret);
}

static int
parseAtomicVar(NCD4parser* parser, NCD4node* container, ncxml_t xml, NCD4node** nodep)
{
    int ret = NC_NOERR;
    NCD4node* node = NULL;
    NCD4node* base = NULL;
    const char* typename;
    const KEYWORDINFO* info;
    NCD4node* group;

    /* Check for aliases */
    for(typename=ncxml_name(xml);;) {
	info = keyword(typename);
	if(info->aliasfor == NULL) break;
	typename = info->aliasfor;
    }
    group = NCD4_groupFor(container);
    /* Locate its basetype; handle opaque */
    assert(info->subsort != NC_ENUM);
    if(info->subsort == NC_OPAQUE) {
	/* See if the xml references an opaque type name */
	base = getOpaque(parser,xml,group);
    } else {
	base = lookupFQN(parser,info->tag,NCD4_TYPE);
    }
    if(base == NULL || !ISTYPE(base->sort)) {
	FAIL(NC_EBADTYPE,"Unexpected variable type: %s",info->tag);
    }
    if((ret=makeNode(parser,container,xml,NCD4_VAR,base->subsort,&node))) goto done;
    classify(container,node);
    node->basetype = base;
    /* Parse attributes, dims, and maps */
    if((ret = parseMetaData(parser,node,xml))) goto done;
    /* See if this var has UCARTAGORIGTYPE attribute */
    if(parser->metadata->controller->controls.translation == NCD4_TRANSNC4) {
	char* typetag = ncxml_attr(xml,UCARTAGORIGTYPE);
	if(typetag != NULL) {
	    /* yes, place it on the type */
	    if((ret=addOrigType(parser,node,node,typetag))) goto done;
	    nullfree(typetag);
 	}
    }
    if(nodep) *nodep = node;
done:
    return THROW(ret);
}

static int
parseDimRefs(NCD4parser* parser, NCD4node* var, ncxml_t xml)
{
    int ret = NC_NOERR;
    ncxml_t x;
    for(x=ncxml_child(xml, "Dim");x!= NULL;x=ncxml_next(x,"Dim")) {
	NCD4node* dim = NULL;
	char* fqn;

	fqn = ncxml_attr(x,"name");
	if(fqn != NULL) {
   	    dim = lookupFQN(parser,fqn,NCD4_DIM);
	    if(dim == NULL) {
	        FAIL(NC_EBADDIM,"Cannot locate dim with name: %s",fqn);
	    }
	    nullfree(fqn);
	} else {
	    char* sizestr = ncxml_attr(x,"size");
	    if(sizestr == NULL) {
	        FAIL(NC_EBADDIM,"Dimension reference has no name and no size");
	    }
	    /* Make or reuse anonymous dimension in root group */
	    dim = makeAnonDim(parser,sizestr);
	    if(dim == NULL)
		FAIL(NC_EBADDIM,"Cannot create anonymous dimension for size: %s",sizestr);
	    nullfree(sizestr);
	}
	PUSH(var->dims,dim);
    }
done:
    return THROW(ret);
}

static int
parseMaps(NCD4parser* parser, NCD4node* var, ncxml_t xml)
{
    int ret = NC_NOERR;
    ncxml_t x;

    NC_UNUSED(parser);
    for(x=ncxml_child(xml, "Map");x!= NULL;x=ncxml_next(x,"Map")) {
	char* fqn;
	fqn = ncxml_attr(x,"name");
	if(fqn == NULL)
	    FAIL(NC_ENOTVAR,"<Map> has no name attribute");
	PUSH(var->mapnames,fqn);
    }
done:
    return THROW(ret);
}

static int
parseAttributes(NCD4parser* parser, NCD4node* container, ncxml_t xml)
{
    int ret = NC_NOERR;
    ncxml_t x;
    NClist* values = NULL;
    char** pairs = NULL;

    /* First, transfer any reserved xml attributes */
    {
        char** p;
	if(!ncxml_attr_pairs(xml,&pairs))
	    {ret = NC_ENOMEM; goto done;}	
	if(container->xmlattributes) nclistfree(container->xmlattributes);
        container->xmlattributes = nclistnew();
	for(p=pairs;*p;p+=2) {
	    if(isReserved(*p)) {
 	        PUSH(container->xmlattributes,strdup(p[0]));
	        PUSH(container->xmlattributes,strdup(p[1]));
	    }
	}
    }

    for(x=ncxml_child(xml, "Attribute");x!= NULL;x=ncxml_next(x,"Attribute")) {
	char* name = ncxml_attr(x,"name");
	char* type = ncxml_attr(x,"type");
	NCD4node* attr = NULL;
	NCD4node* basetype;

	if(name == NULL) FAIL(NC_EBADNAME,"Missing <Attribute> name");
	nullfree(name);
#ifdef HYRAXHACK
	/* Hyrax specifies type="container" for container types */
	if(strcmp(type,"container")==0
	   || strcmp(type,"Container")==0) {
	    nullfree(type);
	    type = NULL;
	}
#endif
	if(type == NULL) {
	    /* <Attribute> containers not supported; ignore */
	    continue;
	}

	if((ret=makeNode(parser,container,x,NCD4_ATTR,NC_NULL,&attr))) goto done;
	basetype = lookupFQN(parser,type,NCD4_TYPE);
	if(basetype == NULL)
	    FAIL(NC_EBADTYPE,"Unknown <Attribute> type: %s",type);
	nullfree(type);
	if(basetype->subsort == NC_NAT && basetype->subsort != NC_ENUM)
	    FAIL(NC_EBADTYPE,"<Attribute> type must be atomic or enum: %s",type);
	attr->basetype = basetype;
	values = nclistnew();
	if((ret=getValueStrings(parser,basetype,x,values))) {
	    FAIL(NC_EINVAL,"Malformed attribute: %s",name);
	}
	attr->attr.values = values; values = NULL;
	PUSH(container->attributes,attr);
    }
done:
    if(pairs) {
	char** p = pairs;
	for(;*p;p++) nullfree(*p);
        free(pairs);
    }
    if(ret != NC_NOERR) {
        nclistfreeall(values);
    }
    return THROW(ret);
}

static int
parseError(NCD4parser* parser, ncxml_t errxml)
{
    char* shttpcode = ncxml_attr(errxml,"httpcode");
    ncxml_t x;
    if(shttpcode == NULL) shttpcode = strdup("400");
    if(sscanf(shttpcode,"%d",&parser->response->error.httpcode) != 1)
        nclog(NCLOGERR,"Malformed <ERROR> response");
    nullfree(shttpcode);
    x=ncxml_child(errxml, "Message");
    if(x != NULL) {
	char* txt = ncxml_text(x);
	parser->response->error.message = (txt == NULL ? NULL : txt);
    }
    x = ncxml_child(errxml, "Context");
    if(x != NULL) {
	const char* txt = ncxml_text(x);
	parser->response->error.context = (txt == NULL ? NULL : strdup(txt));
    }
    x=ncxml_child(errxml, "OtherInformation");
    if(x != NULL) {
	const char* txt = ncxml_text(x);
	parser->response->error.otherinfo = (txt == NULL ? NULL : strdup(txt));
    }
    return THROW(NC_NOERR);
}

/*
Find or create an opaque type
*/
static NCD4node*
getOpaque(NCD4parser* parser, ncxml_t varxml, NCD4node* group)
{
    size_t i;
    int ret = NC_NOERR;
    size_t len;
    NCD4node* opaquetype = NULL;
    char* xattr;

#ifndef FIXEDOPAQUE
    len = 0;
#else
    len = parser->metadata->controller->controls.opaquesize;
#endif
    if(parser->metadata->controller->controls.translation == NCD4_TRANSNC4) {
        /* See if this var has UCARTAGOPAQUE attribute */
        xattr = ncxml_attr(varxml,UCARTAGOPAQUE);
        if(xattr != NULL) {
            long long tmp = 0;
            if((ret = parseLL(xattr,&tmp)) || (tmp < 0))
            FAIL(NC_EINVAL,"Illegal opaque len: %s",xattr);
            len = (size_t)tmp;
            nullfree(xattr);
        }
    }
#ifndef FIXEDOPAQUE
    if(len == 0) {
        /* Need to use _bytestring */
	if((ret=defineBytestringType(parser)))
  	    goto done;
	assert(parser->metadata->_bytestring != NULL);
	opaquetype = parser->metadata->_bytestring;
    } else
#endif
    { /*(len > 0) || FIXEDOPAQUE */
        /* Try to locate existing opaque type with this length */
        for(i=0;i<nclistlength(parser->types); i++) {
	    NCD4node* op = (NCD4node*)nclistget(parser->types,i);
	    if(op->subsort != NC_OPAQUE) continue;
	    if(op->opaque.size == len) {opaquetype = op; break;}
	}
        if(opaquetype == NULL) {/* create it */
	    char name[NC_MAX_NAME+1];
	    /* Make name be "opaqueN" */
	    snprintf(name,NC_MAX_NAME,"opaque%zu_t",len);
	    /* Opaque types are always created in the current group */
	    if((ret=makeNode(parser,group,NULL,NCD4_TYPE,NC_OPAQUE,&opaquetype)))
	        goto done;
  	    SETNAME(opaquetype,name);
	    opaquetype->opaque.size = len;
	}
    }
done:
    return opaquetype;
}

/* get all value strings */
static int
getValueStrings(NCD4parser* parser, NCD4node* type, ncxml_t xattr, NClist* svalues)
{
    char* s;
    NC_UNUSED(parser);
    NC_UNUSED(type);
    /* See first if we have a "value" xml attribute */
    s = ncxml_attr(xattr,"value");
    if(s != NULL) 
	{PUSH(svalues,s); s = NULL;}
    else {/* look for <Value> subnodes */
	ncxml_t x;
        for(x=ncxml_child(xattr, "Value");x != NULL;x = ncxml_next(x,"Value")) {
	    char* es;
	    char* ds;
	    /* We assume that either their is a single xml attribute called "value",
               or there is a single chunk of text containing possibly multiple values.
	    */
	    s = ncxml_attr(x,"value");
	    if(s == NULL) {/* See if there is a text part. */
		s = ncxml_text(x);
		if(s == NULL) s = strdup("");
	    }
	    /* Need to de-escape the string */
	    es = NCD4_entityescape(s);
	    ds = NCD4_deescape(es);
	    PUSH(svalues,ds); ds = NULL;
	    nullfree(es); es = NULL;
    	    nullfree(s); s = NULL;
	}
    }
    nullfree(s);
    return THROW(NC_NOERR);
}

/***************************************************/
/* Utilities */

NCD4node*
NCD4_groupFor(NCD4node* node)
{
    while(node->sort != NCD4_GROUP) node = node->container;
    return node;
}

/* Determine is a name is reserved */
static int
isReserved(const char* name)
{
    if(name == NULL) return 0;
    return (name[0] == RESERVECHAR);
}

/* If a node has the UCARTAGORIGTYPE attribute,
   then capture that annotation. */
static int
addOrigType(NCD4parser* parser, NCD4node* src, NCD4node* dst, const char* oldname)
{
    int ret = NC_NOERR;

    if(dst == NULL) dst = src;
    /* Record the original type in the destination*/
    if((ret=splitOrigType(parser,oldname,dst))) goto done;
done:
    return THROW(ret);
}

static int
splitOrigType(NCD4parser* parser, const char* fqn, NCD4node* type)
{
    int ret = NC_NOERR;
    NClist* pieces = nclistnew();
    NCD4node* group = NULL;
    char* name = NULL;

    if((ret=NCD4_parseFQN(fqn,pieces))) goto done;
    /* It should be the case that the pieces are {/group}+/name */
    name = (char*)nclistpop(pieces);
    if((ret = lookupFQNList(parser,pieces,NCD4_GROUP,&group))) goto done;
    if(ret) {
	FAIL(NC_ENOGRP,"Non-existent group in FQN: ",fqn);
    }
    type->nc4.orig.name = strdup(name+1); /* plus 1 to skip the leading separator */
    type->nc4.orig.group = group;

done:
    return THROW(ret);
}

/* Locate an attribute.
   If not found, then *attrp will be null
*/
NCD4node*
NCD4_findAttr(NCD4node* container, const char* attrname)
{
    size_t i;
    /* Look directly under this xml for <Attribute> */
    for(i=0;i<nclistlength(container->attributes);i++) {
	NCD4node* attr = (NCD4node*)nclistget(container->attributes,i);
	if(strcmp(attr->name,attrname)!=0) continue;
	return attr;
    }
    return NULL;
}

/*
Parse a simple string of digits into an unsigned long long
Return the value.
*/

static int
parseULL(const char* text, unsigned long long* ullp)
{
    char* endptr;
    unsigned long long uint64 = 0;

    errno = 0; endptr = NULL;
#ifdef HAVE_STRTOULL
    uint64 = strtoull(text,&endptr,10);
    if(errno == ERANGE)
	return THROW(NC_ERANGE);
#else /*!(defined HAVE_STRTOLL && defined HAVE_STRTOULL)*/
    sscanf((char*)text, "%llu", &uint64);
    /* Have no useful way to detect out of range */
#endif /*!(defined HAVE_STRTOLL && defined HAVE_STRTOULL)*/
    if(ullp) *ullp = uint64;
    return THROW(NC_NOERR);
}

/*
Parse a simple string of digits into an signed long long
Return the value.
*/

static int
parseLL(const char* text, long long* llp)
{
    char* endptr;
    long long int64 = 0;

    errno = 0; endptr = NULL;
#ifdef HAVE_STRTOLL
    int64 = strtoll(text,&endptr,10);
    if(errno == ERANGE)
	return THROW(NC_ERANGE);
#else /*!(defined HAVE_STRTOLL && defined HAVE_STRTOLL)*/
    sscanf((char*)text, "%lld", &int64);
    /* Have no useful way to detect out of range */
#endif /*!(defined HAVE_STRTOLL && defined HAVE_STRTOLL)*/
    if(llp) *llp = int64;
    return THROW(NC_NOERR);
}

/*
Convert a sequence of fqn names into a specific node.
WARNING: This is highly specialized in that it assumes
that the final object is one of: dimension, type, or var.
This means that e.g. groups, attributes, econsts, cannot
be found by this procedure.
@return NC_NOERR found; result != NULL
@return NC_EINVAL !found; result == NULL
*/
static int
lookupFQNList(NCD4parser* parser, NClist* fqn, NCD4sort sort, NCD4node** result)
{
    int ret = NC_NOERR;
    size_t i;
    NCD4node* current;
    char* name = NULL;
    NCD4node* node = NULL;

    /* Step 1: walk thru groups until can go no further */
    current = parser->metadata->root;
    size_t nsteps = nclistlength(fqn);
    for(i=1;i<nsteps;i++) { /* start at 1 to side-step root name */
	assert(ISGROUP(current->sort));
	name = (char*)nclistget(fqn,i);
        /* See if we can find a matching subgroup */
	node = lookFor(current->group.elements,name,NCD4_GROUP);
	if(node == NULL)
	    break; /* reached the end of the group part of the fqn */
	current = node;
    }
    /* Invariant:
	1. i == nsteps => node != null => last node was a group:
                                          it must be our target
	2. i == (nsteps-1) => non-group node at the end; disambiguate
	3. i < (nsteps - 1) => need a compound var to continue
    */
    if(i == nsteps) {
	if(sort != NCD4_GROUP) /* Right name, wrong sort */
	goto notfound;
    }
    if(i == (nsteps - 1)) {
	assert (node == NULL);
        node = lookFor(current->group.elements,name,sort);
	if(node == NULL) goto notfound;
	goto done;
    }
    assert (i < (nsteps - 1)); /* case 3 */
    /* We have steps to take, so node better be a compound var */
    node = lookFor(current->group.elements,name,NCD4_VAR);
    if(node == NULL || !ISCMPD(node->basetype->subsort))
	goto notfound;
    /* So we are at a compound variable, so walk its fields recursively */
    /* Use the type to do the walk */
    current = node->basetype;
    assert (i < (nsteps - 1));
    i++; /* skip variable name */
    for(;;i++) {
	size_t j;
	name = (char*)nclistget(fqn,i);
	assert(ISTYPE(current->sort) && ISCMPD(current->subsort));
	for(node=NULL,j=0;j<nclistlength(current->vars);j++) {
	    NCD4node* field = (NCD4node*)nclistget(current->vars,j);
	    if(strcmp(field->name,name)==0)
		{node = field; break;}
	}
	if(node == NULL)
	    goto notfound; /* no match, so failed */
	if(i == (nsteps - 1))
	    break;
	if(!ISCMPD(node->basetype->subsort))
	    goto notfound; /* more steps, but no compound field, so failed */
	current = node->basetype;
    }
done:
    if(result) *result = node;
    return THROW(ret);
notfound:
    ret = NC_EINVAL;
    goto done;
}

static NCD4node*
lookFor(NClist* elems, const char* name, NCD4sort sort)
{
    size_t n,i;
    if(elems == NULL || nclistlength(elems) == 0) return NULL;
    n = nclistlength(elems);
    for(i=0;i<n;i++) {
	NCD4node* node = (NCD4node*)nclistget(elems,i);
	if(strcmp(node->name,name) == 0 && (sort == node->sort))
	    return node;
    }
    return NULL;
}

void
NCD4_printElems(NCD4node* group)
{
    size_t n,i;
    NClist* elems;
    elems = group->group.elements;
    if(elems == NULL || nclistlength(elems) == 0) return;
    n = nclistlength(elems);
    for(i=0;i<n;i++) {
	NCD4node* node = (NCD4node*)nclistget(elems,i);
	fprintf(stderr,"name=%s sort=%d subsort=%d\n",
		node->name,node->sort,node->subsort);
    }
    fflush(stderr);
}

static NCD4node*
lookupFQN(NCD4parser* parser, const char* sfqn, NCD4sort sort)
{
    int ret = NC_NOERR;
    NClist* fqnlist = nclistnew();
    NCD4node* match = NULL;

    /* Short circuit atomic types */
    if(NCD4_TYPE == sort) {
        match = lookupAtomicType(parser->metadata->atomictypes,(sfqn[0]=='/'?sfqn+1:sfqn));
        if(match != NULL)
	    goto done;
    }
    if((ret=NCD4_parseFQN(sfqn,fqnlist))) goto done;
    if((ret=lookupFQNList(parser,fqnlist,sort,&match))) goto done;
done:
    nclistfreeall(fqnlist);
    return (ret == NC_NOERR ? match : NULL);
}

static const KEYWORDINFO*
keyword(const char* name)
{
    int n = sizeof(keywordmap)/sizeof(KEYWORDINFO);
    int L = 0;
    int R = (n - 1);
    int m, cmp;
    const struct KEYWORDINFO* p;
    for(;;) {
	if(L > R) break;
        m = (L + R) / 2;
	p = &keywordmap[m];
	cmp = strcasecmp(p->tag,name);
	if(cmp == 0) return p;
	if(cmp < 0)
	    L = (m + 1);
	else /*cmp > 0*/
	    R = (m - 1);
    }
    return NULL;
}

#ifndef FIXEDOPAQUE
static int
defineBytestringType(NCD4parser* parser)
{
    int ret = NC_NOERR;
    NCD4node* bstring = NULL;
    if(parser->metadata->_bytestring == NULL) {
        /* Construct a single global opaque type for mapping DAP opaque type */
        ret = makeNode(parser,parser->metadata->root,NULL,NCD4_TYPE,NC_OPAQUE,&bstring);
        if(ret != NC_NOERR) goto done;
        SETNAME(bstring,"_bytestring");
	bstring->opaque.size = 0;
	bstring->basetype = lookupAtomicType(parser->meta->atomictypes,"UInt8");
        PUSH(parser->metadata->root->types,bstring);
	parser->metadata->_bytestring = bstring;
    } else
	bstring = parser->metadata->_bytestring;
done:
    return THROW(ret);
}
#endif

static int atisort(const void* a, const void* b)
{
    return strcasecmp(((struct ATOMICTYPEINFO*)a)->name,((struct ATOMICTYPEINFO*)b)->name);
}
		
static int
defineAtomicTypes(NCD4meta* meta, NClist* list)
{
    int ret = NC_NOERR;
    NCD4node* node;
    size_t i;
 
    if(list == NULL) return THROW(NC_EINTERNAL);
    if(!atomictypessorted) {
	qsort((void*)atomictypeinfo, NCD4_NATOMICTYPES,sizeof(struct ATOMICTYPEINFO),atisort);
	atomictypessorted = 1;
    }
    for(i=0;i<NCD4_NATOMICTYPES;i++) {
	const struct ATOMICTYPEINFO* ati = &atomictypeinfo[i];
        if((ret=makeNodeStatic(meta,NULL,NCD4_TYPE,ati->type,&node))) goto done;
	SETNAME(node,ati->name);
	PUSH(list,node);
    }
done:
    return THROW(ret);
}

static int
aticmp(const void* a, const void* b)
{
    const char* name = (const char*)a;
    NCD4node** nodebp = (NCD4node**)b;
    return strcasecmp(name,(*nodebp)->name);
}
		
/* Binary search the set of set of atomictypes */
static NCD4node*
lookupAtomicType(NClist* atomictypes, const char* name)
{
    void* match = NULL;
    size_t ntypes = 0;
    NCD4node** types = NULL;
    assert(atomictypessorted && nclistlength(atomictypes) > 0);
    ntypes = nclistlength(atomictypes);
    types = (NCD4node**)atomictypes->content;
    match = bsearch((void*)name,(void*)types,ntypes,sizeof(NCD4node*),aticmp);
    return (match==NULL?NULL:*(NCD4node**)match);
}

/**************************************************/

static int
makeNode(NCD4parser* parser, NCD4node* parent, ncxml_t xml, NCD4sort sort, nc_type subsort, NCD4node** nodep)
{
    int ret = NC_NOERR;    
    NCD4node* node = NULL;
    
    assert(parser);
    if((ret = makeNodeStatic(parser->metadata,parent,sort,subsort,&node))) goto done;

    /* Set node name, if it exists */
    if(xml != NULL) {
        char* name = ncxml_attr(xml,"name");
        if(name != NULL) {
	    if(strlen(name) > NC_MAX_NAME) {
	        nclog(NCLOGERR,"Name too long: %s",name);
	    }
	    SETNAME(node,name);
	    nullfree(name);
	}
    }
    record(parser,node);
    if(nodep) *nodep = node;
done:
    return THROW(ret);
}

static int
makeNodeStatic(NCD4meta* meta, NCD4node* parent, NCD4sort sort, nc_type subsort, NCD4node** nodep)
{
    int ret = NC_NOERR;
    NCD4node* node = (NCD4node*)calloc(1,sizeof(NCD4node));

    assert(meta);
    if(node == NULL) return THROW(NC_ENOMEM);
    node->sort = sort;
    node->subsort = subsort;
    node->container = parent;
#if 0
    if(parent != NULL) {
	if(parent->sort == NCD4_GROUP)
	    classify(parent,node);
    }
#endif
    track(meta,node);
    if(nodep) *nodep = node;
    return THROW(ret);
}

static NCD4node*
makeAnonDim(NCD4parser* parser, const char* sizestr)
{
    long long size = 0;
    int ret;
    char name[NC_MAX_NAME+1];
    NCD4node* dim = NULL;
    NCD4node* root = parser->metadata->root;

    ret = parseLL(sizestr,&size);
    if(ret) return NULL;
    snprintf(name,NC_MAX_NAME,"/%s_%lld",NCDIMANON,size);
    /* See if it exists already */
    dim = lookupFQN(parser,name,NCD4_DIM);
    if(dim == NULL) {/* create it */
	if((ret=makeNode(parser,root,NULL,NCD4_DIM,NC_NULL,&dim))) goto done;
	SETNAME(dim,name+1); /* leave out the '/' separator */
	dim->dim.size = (size_t)size;
	dim->dim.isanonymous = 1;
	classify(root,dim);
    }
done:
    return (ret?NULL:dim);
}

/*
Classify inserts the node into the proper container list
based on the node's sort.
*/
static void
classify(NCD4node* container, NCD4node* node)
{
    if(ISGROUP(container->sort))
	PUSH(container->group.elements,node);
    switch (node->sort) {
    case NCD4_GROUP:
	PUSH(container->groups,node);
        break;
    case NCD4_DIM:
	PUSH(container->dims,node);
        break;
    case NCD4_TYPE:
	PUSH(container->types,node);
        break;
    case NCD4_VAR:
	PUSH(container->vars,node);
        break;
    case NCD4_ATTR: case NCD4_XML:
	PUSH(container->attributes,node);
        break;
    default: break;
    }
}

/*
Classify inserts the node into the proper parser global list
based on the node's sort.
*/
static void
record(NCD4parser* parser, NCD4node* node)
{
    switch (node->sort) {
    case NCD4_GROUP:
	PUSH(parser->groups,node);
        break;
    case NCD4_DIM:
	PUSH(parser->dims,node);
        break;
    case NCD4_TYPE:
	PUSH(parser->types,node);
        break;
    case NCD4_VAR:
	PUSH(parser->vars,node);
        break;
    default: break;
    }
}

/*
Undo a classify and record
for a field node.
Used by buildSequenceType.
*/
#if 0
static void
forget(NCD4parser* parser, NCD4node* var)
{
    int i;
    NCD4node* container = var->container;
    assert(ISVAR(var->sort) && ISTYPE(container->sort) && ISCMPD(container->subsort));
    /* Unrecord: remove from the parser lists */
    for(i=0;i<parser->vars;i++) {
	NCD4node* test = nclistget(parser->vars,i);
	if(test == var) {
	    nclistremove(parser->vars,i);
	    break;
	}
    }
    /* Unclassify: remove from the container var list */
    for(i=0;i<container->vars;i++) {
	NCD4node* test = nclistget(container->vars,i);
	if(test == var) {
	    nclistremove(container->vars,i);
	    break;
	}
    }
}
#endif

static void
track(NCD4meta* meta, NCD4node* node)
{
#ifdef D4DEBUG
    fprintf(stderr,"track: node=%lx sort=%d subsort=%d",(unsigned long)node,node->sort,node->subsort);
    if(node->name != NULL)
        fprintf(stderr," name=%s\n",node->name);
    fprintf(stderr,"\n");
#endif
    PUSH(meta->allnodes,node);
#ifdef D4DEBUG
    fprintf(stderr,"track: |allnodes|=%ld\n",nclistlength(meta->allnodes));
    fflush(stderr);
#endif
}

/**************************************************/

static int
convertString(union ATOMICS* converter, NCD4node* type, const char* s)
{
    switch (type->subsort) {
    case NC_BYTE:
    case NC_SHORT:
    case NC_INT:
    case NC_INT64:
	if(sscanf(s,"%lld",converter->i64) != 1) return THROW(NC_ERANGE);
	break;
    case NC_UBYTE:
    case NC_USHORT:
    case NC_UINT:
    case NC_UINT64:
	if(sscanf(s,"%llu",converter->u64) != 1) return THROW(NC_ERANGE);
	break;
    case NC_FLOAT:
    case NC_DOUBLE:
	if(sscanf(s,"%lf",converter->f64) != 1) return THROW(NC_ERANGE);
	break;
    case NC_STRING:
	converter->s[0]= strdup(s);
	break;
    }/*switch*/
    return downConvert(converter,type);
}

static int
downConvert(union ATOMICS* converter, NCD4node* type)
{
    unsigned long long u64 = converter->u64[0];
    long long i64 = converter->i64[0];
    double f64 = converter->f64[0];
    char* s = converter->s[0];
    switch (type->subsort) {
    case NC_BYTE:
	converter->i8[0] = (char)i64;
	break;
    case NC_UBYTE:
	converter->u8[0] = (unsigned char)u64;
	break;
    case NC_SHORT:
	converter->i16[0] = (short)i64;
	break;
    case NC_USHORT:
	converter->u16[0] = (unsigned short)u64;
	break;
    case NC_INT:
	converter->i32[0] = (int)i64;
	break;
    case NC_UINT:
	converter->u32[0] = (unsigned int)u64;
	break;
    case NC_INT64:
	converter->i64[0] = i64;
	break;
    case NC_UINT64:
	converter->u64[0]= u64;
	break;
    case NC_FLOAT:
	converter->f32[0] = (float)f64;
	break;
    case NC_DOUBLE:
	converter->f64[0] = f64;
	break;
    case NC_STRING:
	converter->s[0]= s;
	break;
    }/*switch*/
    return THROW(NC_NOERR);
}

#if 0
/* Try to remove excess text from a value set */
static int
valueParse(NCD4node* type, const char* values0, NClist* vlist)
{
    char* values;
    char* p;
    char* q;
    char* s;
    char* line;
    ptrdiff_t len;

    if(values0 == NULL || (len=strlen(values0)) == 0)
	return THROW(NC_NOERR);
    values = strdup(values0);
    /* Compress the text by removing sequences of blanks and newlines:
       note that this will fail for string typed values that might have
       embedded blanks, so in that case, we assume each string is on a separate line.
       For NC_CHAR, we treat like strings, except we use the last char in the line
       as the value. This is all heuristic.
    */
    switch (type->subsort) {
    case NC_STRING:
	p = values;
	for(;;) {
	    if(*p == '\0') break;
	    line = p;
	    /* Start by looking for \n or \r\n */
            for(;*p;p++) {if(*p == '\n') break;}
	    q = p - 1;
	    *p++ = '\0';
	    if(*q == '\r') {*q = '\0';}
            PUSH(vlist,strdup(line));
	}
	break;
    case NC_CHAR:
	p = values;
	for(;*p;) {
	    char c[2];
	    line = p;
	    /* Start by looking for \n or \r\n */
            for(;*p;p++) {if(*p == '\n') break;}
	    q = p;
	    *p++ = '\0';
	    q--;
	    if(*q == '\r') {*q = '\0';}
	    len = strlen(line);
	    if(len > 0) {
		c[0] = *q;
		c[1] = '\0';
		PUSH(vlist,strdup(c));
	    }
	}
	break;
    default:
	p = values;
        for(;*p;p++) {if(*p <= ' ') *p = '\n';}
	line = values;
	for(p=line;*p;p++) {if(*p == '\n') break;}
	for(line=values;*line;) {
	    size_t size = strlen(line);
	    if(size > 0)
	        PUSH(vlist,strdup(line));
	    line += (size+1); /* skip terminating nul */
	}
	break;
    }
    free(values);
    return THROW(NC_NOERR);
}
#endif

/* Define extra attributes not present in the xml */
int
NCD4_defineattr(NCD4meta* meta, NCD4node* parent, const char* aname, const char* typename, NCD4node** attrp)
{
    NCD4node* attr = NULL;
    NCD4node* basetype = NULL;
    if((basetype = lookupAtomicType(meta->atomictypes,typename))==NULL)
	return NC_EINVAL;
    if(makeNode(NULL,parent,NULL,NCD4_ATTR,NC_NULL,&attr))
	return NC_EINVAL;
    SETNAME(attr,strdup(aname));
    attr->basetype = basetype;
    PUSH(parent->attributes,attr);		
    if(attrp) *attrp = attr;
    return NC_NOERR;
}

/*
Fill in forward references for selected Node:
1. <Map>
*/
static int
parseForwards(NCD4parser* parser, NCD4node* root)
{
    int ret = NC_NOERR;
    size_t i,j;

    NC_UNUSED(root);
    /* process all vars */
    for(i=0;i<nclistlength(parser->vars);i++) {
        NCD4node* var = (NCD4node*)nclistget(parser->vars,i);
	/* Process the variable's maps */
	for(j=0;j<nclistlength(var->mapnames);j++) {
            const char* mapname = (const char*)nclistget(var->mapnames,j);
            /* Find the corresponding variable */
            NCD4node* mapref = lookupFQN(parser,mapname,NCD4_VAR);
	    if(mapref != NULL)
	        PUSH(var->maps,mapref);
	    else if(!parser->dapparse) 
	        FAIL(NC_ENOTVAR,"<Map> name does not refer to a variable: %s",mapname);
	}
    }
    
done:
    return THROW(ret);
}

void
NCD4_setdebuglevel(NCD4parser* parser, int debuglevel)
{
    parser->debuglevel = debuglevel;
}
