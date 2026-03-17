/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "d4includes.h"
#include <stddef.h>

/**
This provides a simple dap4  metadata -> xml printer.
Used to test the parser
*/

#define NCD4_parse t_parse /* to avoid dup defs */

#include "ncd4types.h"
#include "ncd4.h"

/**************************************************/

typedef struct D4printer {
    NCbytes* out;
    NCbytes* tmp;
    NCD4meta* metadata;
} D4printer;

/**************************************************/

#define CAT(x) ncbytescat(out->out,x)
#define INDENT(x) indent(out,x)


/**************************************************/
/* Forwards */

static void atomicsToString(D4printer*, union ATOMICS* value, nc_type type);
static int hasMetaData(NCD4node* node);
static void indent(D4printer*, int depth);
static int printAttribute(D4printer*, NCD4node* attr, int depth);
static int printDataset(D4printer* out, NCD4node* node, int depth);
static int printDimref(D4printer*, NCD4node* d, int depth);
static int printGroup(D4printer* out, NCD4node* node, int depth);
static int printGroupBody(D4printer* out, NCD4node* node, int depth);
static int printMap(D4printer* out, NCD4node* mapref, int depth);
static int printMetaData(D4printer* out, NCD4node* node, int depth);
static int printNode(D4printer*, NCD4node* node, int depth);
static int printValue(D4printer*, const char* value, int depth);
static int printVariable(D4printer*, NCD4node* var, int depth);
static int printXMLAttributeAtomics(D4printer*, char* name, union ATOMICS* value, nc_type type);
static int printXMLAttributeName(D4printer*, char* name, const char* value);
static int printXMLAttributeSize(D4printer*, char* name, size_t value);
static int printXMLAttributeString(D4printer*, char* name, const char* value);

/**************************************************/

int
NCD4_print(NCD4meta* metadata, NCbytes* output)
{
    int ret = NC_NOERR;
    D4printer out;
    if(metadata == NULL || output == NULL) return THROW(NC_EINVAL);
    out.out = output;
    out.tmp = ncbytesnew();
    out.metadata = metadata;
    ret = printNode(&out,metadata->root,0);
    ncbytesfree(out.tmp);
    return THROW(ret);
}

/*************************************************/

/**
 * Print an arbitrary file and its subnodes in xml
 * Handling newlines is a bit tricky because they may be
 * embedded for e.g. groups, enums,
 * etc.	 So the rule is that the
 * last newline is elided and left
 * for the caller to print.
 * Exceptions: printMetadata
 * printDimrefs.
 *
 * @param out - the output buffer
 * @param node - the tree to print
 * @param depth - the depth of our code
 */

static int
printNode(D4printer* out, NCD4node* node, int depth)
{
    int ret = NC_NOERR;
    size_t i;
    char* fqn = NULL;

    switch (node->sort) {
    case NCD4_GROUP:
	if(node->group.isdataset)
	    printDataset(out,node,depth);
	else
	    printGroup(out,node,depth);
	break;

    case NCD4_DIM:
	INDENT(depth);
	CAT("<Dimension");
	if(node->name != NULL)
	    printXMLAttributeName(out, "name", node->name);
	printXMLAttributeSize(out, "size", node->dim.size);
	if(node->dim.isunlimited)
	    printXMLAttributeString(out, UCARTAGUNLIM, "1");
	CAT("/>");
	break;

    case NCD4_TYPE:
	switch (node->subsort) {
	default: break;
	case NC_OPAQUE:
	    INDENT(depth); CAT("<Opaque");
	    ncbytesclear(out->tmp);
	    printXMLAttributeName(out, "name", node->name);
	    if(node->opaque.size > 0)
	        printXMLAttributeSize(out, "size", node->opaque.size);
	    CAT("/>");
	    break;	    
	case NC_ENUM:
	    INDENT(depth); CAT("<Enumeration");
	    printXMLAttributeName(out, "name", node->name);
	    if(node->basetype->subsort <= NC_MAX_ATOMIC_TYPE)
		printXMLAttributeName(out, "basetype", node->basetype->name);
	    else {
		char* fqn = NULL;
		printXMLAttributeName(out, "basetype", (fqn = NCD4_makeFQN(node->basetype)));
		nullfree(fqn);
	    }
	    CAT(">\n");
	    depth++;
	    for(i=0;i<nclistlength(node->en.econsts);i++) {
		NCD4node* ec = (NCD4node*)nclistget(node->en.econsts,i);
		INDENT(depth);
		CAT("<EnumConst");
		printXMLAttributeName(out, "name", ec->name);
		printXMLAttributeAtomics(out, "value", &ec->en.ecvalue, node->basetype->subsort);
		CAT("/>\n");
	    }
	    depth--;
	    INDENT(depth); CAT("</Enumeration>");
	    break;
	case NC_STRUCT:
	    INDENT(depth);
	    CAT("<Structure");
	    printXMLAttributeName(out, "name", node->name);
	    CAT(">\n");
	    depth++;
	    for(i=0;i<nclistlength(node->vars);i++) {
		NCD4node* field = (NCD4node*)nclistget(node->vars,i);
		printVariable(out,field,depth);
		CAT("\n");
	    }
	    if((ret=printMetaData(out,node,depth))) goto done;
	    depth--;
	    INDENT(depth);
	    CAT("</Structure>");
	    break;
	case NC_SEQ:
	    INDENT(depth);
	    CAT("<Vlen");
	    printXMLAttributeName(out, "name", node->name);
	    printXMLAttributeName(out, "type", (fqn=NCD4_makeFQN(node->basetype)));
	    if(hasMetaData(node)) {
		CAT(">\n");
		depth++;
		if((ret=printMetaData(out,node,depth))) goto done;
		depth--;
		INDENT(depth);
		CAT("</Vlen>");
	    } else
	        CAT("/>");
  	    break;
        } break;
    case NCD4_VAR: /* Only top-level vars are printed here */
	if(ISTOPLEVEL(node)) {
  	    if((ret=printVariable(out,node,depth))) goto done;
	    CAT("\n");
	}
	break;

    default: abort(); break;
    }
done:
    nullfree(fqn);
    return THROW(ret);
}

static int
printVariable(D4printer* out, NCD4node* var, int depth)
{
    int ret = NC_NOERR;
    NCD4node* basetype = var->basetype;
    char* fqn = NULL;

    INDENT(depth); CAT("<");
    switch (var->subsort) {
    default:
	CAT(basetype->name);
	printXMLAttributeName(out, "name", var->name);
	break;
    case NC_ENUM:
	CAT("Enum");
	printXMLAttributeName(out, "name", var->name);
	printXMLAttributeName(out, "enum", (fqn=NCD4_makeFQN(basetype)));
	break;
    case NC_OPAQUE:
	CAT("Opaque");
	printXMLAttributeName(out, "name", var->name);
	printXMLAttributeName(out, "type", (fqn=NCD4_makeFQN(basetype)));
	break;
    case NC_SEQ:
	CAT("Seq");
	printXMLAttributeName(out, "name", var->name);
	printXMLAttributeName(out, "type", (fqn=NCD4_makeFQN(basetype)));
	break;
    case NC_STRUCT:
	CAT("Struct");
	printXMLAttributeName(out, "name", var->name);
	printXMLAttributeName(out, "type", (fqn=NCD4_makeFQN(basetype)));
	break;
    }
    if(hasMetaData(var)) {
	CAT(">\n");
	depth++;
	if((ret=printMetaData(out,var,depth))) goto done;
	depth--;
	INDENT(depth); CAT("</");
	if(basetype->subsort == NC_ENUM)
	    CAT("Enum");
	else if(basetype->subsort == NC_OPAQUE)
	    CAT("Opaque");
	else if(basetype->subsort == NC_STRUCT)
	    CAT("Struct");
	else if(basetype->subsort == NC_SEQ)
	    CAT("Sequence");
	else
	    CAT(basetype->name);
	CAT(">");
    } else
	CAT("/>");
done:
    nullfree(fqn);
    return THROW(ret);
}

static int
printDataset(D4printer* out, NCD4node* node, int depth)
{
    int ret = NC_NOERR;
    CAT("<Dataset\n");
    depth++;
    INDENT(depth);
    printXMLAttributeName(out,"name",node->group.datasetname);
    CAT("\n");
    INDENT(depth);
    printXMLAttributeName(out,"dapVersion",node->group.dapversion);
    CAT("\n");
    INDENT(depth);
    printXMLAttributeName(out,"dmrVersion",node->group.dmrversion);
    CAT("\n");
    INDENT(depth);
    printXMLAttributeName(out,"xmlns","http://xml.opendap.org/ns/DAP/4.0#");
    CAT("\n");
    INDENT(depth);
    printXMLAttributeName(out,"xmlns:dap","http://xml.opendap.org/ns/DAP/4.0#");
    depth--;
    CAT(">\n");
    depth++;
    ret = printGroupBody(out,node,depth);
    depth--;
    INDENT(depth);
    CAT("</Dataset>");
    return THROW(ret);	   
}

static int
printGroup(D4printer* out, NCD4node* node, int depth)
{
    int ret = NC_NOERR;
    INDENT(depth);
    CAT("<Group");
    printXMLAttributeName(out,"name",node->name);
    CAT(">\n");
    depth++;
    ret = printGroupBody(out,node,depth);
    depth--;
    INDENT(depth);
    CAT("</Group>");
    return THROW(ret);	   
}	

static int
printGroupBody(D4printer* out, NCD4node* node, int depth)
{
    int ret = NC_NOERR;
    size_t i;
    size_t ngroups = nclistlength(node->groups);
    size_t nvars = nclistlength(node->vars);
    size_t ntypes = nclistlength(node->types);
    size_t ndims = nclistlength(node->dims);
    size_t nattrs = nclistlength(node->attributes);

    if(ndims > 0) {
	INDENT(depth);
	CAT("<Dimensions>\n");
	depth++;
	for(i=0;i<nclistlength(node->dims);i++) {
	    NCD4node* dim = (NCD4node*)nclistget(node->dims,i);
	    printNode(out,dim,depth);
	    CAT("\n");
	}
	depth--;
	INDENT(depth);
	CAT("</Dimensions>\n");
    }
    if(ntypes > 0) {
	INDENT(depth);
	CAT("<Types>\n");
	depth++;
	for(i=0;i<nclistlength(node->types);i++) {
	    NCD4node* type = (NCD4node*)nclistget(node->types,i);
	    if(type->subsort <= NC_MAX_ATOMIC_TYPE) continue;
	    printNode(out,type,depth);
	    CAT("\n");
	}
	depth--;
	INDENT(depth);
	CAT("</Types>\n");
    }
    if(nvars > 0) {
	INDENT(depth);
	CAT("<Variables>\n");
	depth++;
	for(i=0;i<nclistlength(node->vars);i++) {
	    NCD4node* var = (NCD4node*)nclistget(node->vars,i);
	    printNode(out,var,depth);
	}
	depth--;
	INDENT(depth);
	CAT("</Variables>\n");
    }
    if(nattrs > 0) {
	for(i=0;i<nclistlength(node->attributes);i++) {
	    NCD4node* attr = (NCD4node*)nclistget(node->attributes,i);
	    printAttribute(out,attr,depth);
	    CAT("\n");
	}
    }
    if(ngroups > 0) {
	INDENT(depth);
	CAT("<Groups>\n");
	depth++;
	for(i=0;i<nclistlength(node->groups);i++) {
	    NCD4node* g = (NCD4node*)nclistget(node->groups,i);
	    printNode(out,g,depth);
	    CAT("\n");
	}
	depth--;
	INDENT(depth);
	CAT("</Groups>\n");
    }
    return THROW(ret);
}

static int
printMetaData(D4printer* out, NCD4node* node, int depth)
{
    int ret = NC_NOERR;
    size_t i;

    if(nclistlength(node->dims) > 0) {
	for(i=0;i<nclistlength(node->dims);i++) {
	    NCD4node* dim = (NCD4node*)nclistget(node->dims,i);
	    printDimref(out,dim,depth);
	    CAT("\n");
	}
    }
    if(nclistlength(node->maps) > 0) {
	for(i=0;i<nclistlength(node->maps);i++) {
	    NCD4node* mapref = (NCD4node*)nclistget(node->maps,i);
	    printMap(out,mapref,depth);
	    CAT("\n");
	}
     }
    if(nclistlength(node->attributes) > 0) {
	for(i=0;i<nclistlength(node->attributes);i++) {
	    NCD4node* attr = (NCD4node*)nclistget(node->attributes,i);
	    printAttribute(out,attr,depth);
	    CAT("\n");
	}
    }
    return THROW(ret);
}

static int
printXMLAttributeName(D4printer* out, char* name, const char* value)
{
    int ret = NC_NOERR;
    char* escaped = NULL;

    if(name == NULL) return THROW(ret);
    if(value == NULL) value = "";
    CAT(" "); CAT(name); CAT("=\"");
    /* add xml entity escaping */
    escaped = NCD4_entityescape(value);
    CAT(escaped);
    CAT("\"");
    nullfree(escaped);
    return THROW(ret);
}


static int
printXMLAttributeString(D4printer* out, char* name, const char* value)
{
    int ret = NC_NOERR;
    char* escaped = NULL;
    if(name == NULL) return THROW(ret);
    CAT(" "); CAT(name);
    CAT("=");
    CAT("\"");
    if(value == NULL) value = "";
    /* add xml entity escaping */
    escaped = NCD4_entityescape(value);
    CAT(escaped);
    CAT("\"");
    nullfree(escaped);
    return THROW(ret);
}

static int
printXMLAttributeSize(D4printer* out, char* name, size_t value)
{
    union ATOMICS atomics;
    atomics.u64[0] = (unsigned long long)value;
    return printXMLAttributeAtomics(out,name,&atomics,NC_UINT64);
}

static int
printXMLAttributeAtomics(D4printer* out, char* name, union ATOMICS* value, nc_type type)
{
    int ret = NC_NOERR;
    CAT(" "); CAT(name); CAT("=\"");
    atomicsToString(out,value,type);	
    CAT(ncbytescontents(out->tmp));
    CAT("\"");
    return THROW(ret);
}

static int
printAttribute(D4printer* out, NCD4node* attr, int depth)
{
    int ret = NC_NOERR;
    size_t i = 0;
    char* fqn = NULL;

    INDENT(depth); CAT("<Attribute");
    printXMLAttributeName(out,"name",attr->name);
    if(attr->basetype->subsort <=  NC_MAX_ATOMIC_TYPE)
	printXMLAttributeName(out,"type",attr->basetype->name);
    else {
	printXMLAttributeName(out,"type",(fqn = NCD4_makeFQN(attr->basetype)));
    }
    CAT(">\n");
    depth++;
    for(i=0;i<nclistlength(attr->attr.values);i++) {
	printValue(out,(const char*)nclistget(attr->attr.values,i),depth);
	CAT("\n");
    }
    depth--;
    INDENT(depth);
    CAT("</Attribute>");

    nullfree(fqn);
    return THROW(ret);
}

/**
 * Print the dimrefs for a variable's dimensions.
 * If the variable has a non-whole projection, then use size
 * else use the dimension name.
 *
 * @param var whole dimensions are to be printed
 * @throws DapException
 */
static int
printDimref(D4printer* out, NCD4node* d, int depth)
{
    char* fqn = NULL;
    INDENT(depth);
    CAT("<Dim");
    printXMLAttributeName(out, "name", (fqn=NCD4_makeFQN(d)));
    CAT("/>");
    nullfree(fqn);
    return THROW(NC_NOERR);
}

static int
printValue(D4printer* out, const char* value, int depth)
{
    int ret = NC_NOERR;
    INDENT(depth);
    CAT("<Value");
    printXMLAttributeString(out,"value",value);
    CAT("/>");
    return THROW(ret);
}

static int
printMap(D4printer* out, NCD4node* mapref, int depth)
{
    char* fqn = NULL;
    INDENT(depth);
    CAT("<Map");
    printXMLAttributeName(out, "name", (fqn=NCD4_makeFQN(mapref)));
    CAT("/>");
    nullfree(fqn);
    return THROW(NC_NOERR);
}

/*************************************************/
/* Misc. Static Utilities */

static void
indent(D4printer* out, int depth)
{
    while(depth-- > 0) ncbytescat(out->out,"  ");
}

static int
hasMetaData(NCD4node* node)
{
    return (nclistlength(node->dims) > 0
       || nclistlength(node->attributes) > 0
       || nclistlength(node->maps) > 0);
}

static void
atomicsToString(D4printer* out, union ATOMICS* value, nc_type type)
{
    char tmp[256];
    ncbytesclear(out->tmp);
    switch (type) {
    case NC_CHAR:
	snprintf(tmp,sizeof(tmp),"'%c'",value->i8[0]);
	break;
    case NC_BYTE:
	snprintf(tmp,sizeof(tmp),"%d",value->i8[0]);
	break;
    case NC_UBYTE:
	snprintf(tmp,sizeof(tmp),"%u",value->u8[0]);
	break;
    case NC_SHORT:
	snprintf(tmp,sizeof(tmp),"%d",value->i16[0]);
	break;
    case NC_USHORT:
	snprintf(tmp,sizeof(tmp),"%u",value->u16[0]);
	break;
    case NC_INT:
	snprintf(tmp,sizeof(tmp),"%d",value->i32[0]);
	break;
    case NC_UINT:
	snprintf(tmp,sizeof(tmp),"%u",value->u32[0]);
	break;
    case NC_INT64:
	snprintf(tmp,sizeof(tmp),"%lld",value->i64[0]);
	break;
    case NC_UINT64:
	snprintf(tmp,sizeof(tmp),"%llu",value->u64[0]);
	break;
    case NC_FLOAT:
	snprintf(tmp,sizeof(tmp),"%g",value->f32[0]);
	break;
    case NC_DOUBLE:
	snprintf(tmp,sizeof(tmp),"%g",value->f64[0]);
	break;
    case NC_STRING:
	ncbytescat(out->tmp,"\"");
	ncbytescat(out->tmp,value->s[0]);
	ncbytescat(out->tmp,"\"");
	break;
    default: abort();
    }
    if(type != NC_STRING) ncbytescat(out->tmp,tmp);
    ncbytesnull(out->tmp);
}
