/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#include "config.h"
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif

#include "netcdf.h"
#include "ocinternal.h"
#include "ocdebug.h"
#include "ocdump.h"
#include "nclog.h"
#include "ncrc.h"
#include "occurlfunctions.h"
#include "ochttp.h"
#include "ncpathmgr.h"

#undef TRACK

/**************************************************/

/* Track legal ids */

#define ocverify(o) ((o) != NULL && (((OCheader*)(o))->magic == OCMAGIC)?1:0)

#define ocverifyclass(o,cl) ((o) != NULL && (((OCheader*)(o))->occlass == cl)?1:0)

#define OCVERIFYX(k,x,r) if(!ocverify(x)||!ocverifyclass(x,k)) {return (r);}
#define OCVERIFY(k,x) OCVERIFYX(k,x,OCTHROW(OC_EINVAL))

#define OCDEREF(T,s,x) (s)=(T)(x)

/**************************************************/
/*!\file oc.c
*/

/*!\defgroup Link Link Management
@{*/

/*!
This procedure opens a link to some OPeNDAP
data server to request a specific url, possibly with constraints.
It returns an <i>OClink</i> object.
\param[in] url The url for the OPeNDAP server to which a connection
is created and the request is made.
\param[out] linkp A pointer to a location into which the link
object is to be returned.

\retval OC_NOERR The link was successfully created.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_open(const char* url, OCobject* linkp)
{
	OCerror ocerr = OC_NOERR;
    OCstate* state = NULL;
    ocerr = ocopen(&state,url);
    if(ocerr == OC_NOERR && linkp) {
      *linkp = (OCobject)(state);
    } else {
      if(state) free(state);
    }

    return OCTHROW(ocerr);
}

/*!
This procedure closes a previously opened
link and releases all resources associated with
that link.
\param[in] link The link object to be closed.

\retval OC_NOERR The link was successfully closed.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_close(OCobject link)
{
    OCstate* state;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    occlose(state);
    return OCTHROW(OC_NOERR);
}

/** @} */

/*!\defgroup Tree Tree Management
@{*/

/*!
This procedure is used to send requests to the server
to obtain either a DAS, DDS, or DATADDS response
and produce a corresponding tree.
It fetches and parses a given class of DXD the server specified
at open time, and using a specified set of constraints
and flags.

\param[in] link The link through which the server is accessed.
\param[in] constraint The constraint to be applied to the request.
\param[in] dxdkind The OCdxd value indicating what to fetch (i.e.
DAS, DDS, or DataDDS).
\param[in] flags The 'OR' of OCflags to control the fetch:
The OCONDISK flag is defined to cause the fetched
xdr data to be stored on disk instead of in memory.
\param[out] rootp A pointer a location to store
the root node of the tree associated with the the request.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_fetch(OCobject link, const char* constraint,
                 OCdxd dxdkind, OCflags flags, OCobject* rootp)
{
    OCstate* state;
    OCerror ocerr = OC_NOERR;
    OCnode* root;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);

    ocerr = ocfetch(state,constraint,dxdkind,flags,&root);
    if(ocerr) return OCTHROW(ocerr);

    if(rootp) *rootp = (OCobject)(root);
    return OCTHROW(ocerr);
}


/*!
This procedure reclaims all resources
associated with a given tree of objects
associated with a given root.
If the root is that of a DataDDS, then the associated data tree
will be reclaimed as well.

\param[in] link The link through which the server is accessed.
\param[in] ddsroot The root of the tree to be reclaimed.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_root_free(OCobject link, OCobject ddsroot)
{
    OCnode* root;
    OCVERIFY(OC_Node,ddsroot);
    OCDEREF(OCnode*,root,ddsroot);

    ocroot_free(root);
    return OCTHROW(OC_NOERR);
}

/*!
This procedure returns the textual part of
a DAS, DDS, or DATADDS request exactly as sent by the server.

\param[in] link The link through which the server is accessed.
\param[in] ddsroot The root of the tree whose text is to be returned.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

const char*
oc_tree_text(OCobject link, OCobject ddsroot)
{
    OCnode* root = NULL;
    OCVERIFYX(OC_Node,ddsroot,NULL);
    OCDEREF(OCnode*,root,ddsroot);

    if(root == NULL) return NULL;
    root = root->root;
    if(root->tree == NULL) return NULL;
    return root->tree->text;
}

/**@}*/

/*!\defgroup Node Node Management
@{*/


/*!
This procedure returns a variety of properties
associated with a specific node.
Any of the pointers may be NULL in the following procedure call;
If the node is of type Dataset, then return # of global attributes
If the node is of type Attribute, then return the # of values in nattrp.

\param[in] link The link through which the server is accessed.
\param[in] ddsnode The node whose properties are of interest.
\param[out] namep Pointer for storing the node's associated name.
The caller must free the returned name.
\param[out] octypep Pointer for storing the node's octype.
\param[out] atomtypep Pointer for storing the object's
atomic type (i.e. OC_NAT .. OC_URL);only defined when
the object's octype is OC_Atomic
\param[out] containerp Pointer for storing the
OCnode for which this object is a subnode. The value OCNULL
is stored if the object is a root object.
\param[out] rankp Pointer for storing the rank (i.e. the number
of dimensions) for this object; zero implies a scalar.
\param[out] nsubnodesp Pointer for storing the number
of subnodes of this object.
\param[out] nattrp Pointer for storing the number
of attributes associated with this object.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_properties(OCobject link,
 	  OCobject ddsnode,
	  char** namep,
	  OCtype* octypep,
	  OCtype* atomtypep, /* if objecttype == OC_Atomic */
	  OCobject* containerp,
	  size_t* rankp,
	  size_t* nsubnodesp,
	  size_t* nattrp)
{
    OCnode* node;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(namep) *namep = nulldup(node->name);
    if(octypep) *octypep = node->octype;
    if(atomtypep) *atomtypep = node->etype;
    if(rankp) *rankp = node->array.rank;
    if(containerp) *containerp = (OCobject)node->container;
    if(nsubnodesp) *nsubnodesp = nclistlength(node->subnodes);
    if(nattrp) {
        if(node->octype == OC_Attribute) {
            *nattrp = nclistlength(node->att.values);
        } else {
            *nattrp = nclistlength(node->attributes);
	}
    }
    return OCTHROW(OC_NOERR);
}

/*!
Specialized accessor function as an alternative to oc_dds_properties.
\param[in] link The link through which the server is accessed.
\param[in] ddsnode The node whose properties are of interest.
\param[out] namep A pointer into which the node name is stored
as a null terminated string. The caller must free this value
when no longer needed.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_name(OCobject link, OCobject ddsnode, char** namep)
{
    OCstate* state;
    OCnode* node;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(state == NULL || node == NULL) return OCTHROW(OCTHROW(OC_EINVAL));
    if(namep) *namep = nulldup(node->name);
    return OCTHROW(OC_NOERR);
}

/*!
Specialized accessor function as an alternative to oc_dds_properties.
\param[in] link The link through which the server is accessed.
\param[in] ddsnode The node whose properties are of interest.
\param[out] nsubnodesp A pointer into which the number of subnodes
is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_nsubnodes(OCobject link, OCobject ddsnode, size_t* nsubnodesp)
{
    OCnode* node;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(nsubnodesp) *nsubnodesp = nclistlength(node->subnodes);
    return OCTHROW(OC_NOERR);
}

/*!
Specialized accessor function as an alternative to oc_dds_properties.
\param[in] link The link through which the server is accessed.
\param[in] ddsnode The node whose properties are of interest.
\param[out] typep A pointer into which the atomictype is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_atomictype(OCobject link, OCobject ddsnode, OCtype* typep)
{
    OCnode* node;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(typep) *typep = node->etype;
    return OCTHROW(OC_NOERR);
}

/*!
Specialized accessor function as an alternative to oc_dds_properties.
\param[in] link The link through which the server is accessed.
\param[in] ddsnode The node whose properties are of interest.
\param[out] typep A pointer into which the octype is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_class(OCobject link, OCobject ddsnode, OCtype* typep)
{
    OCnode* node;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(typep) *typep = node->octype;
    return OCTHROW(OC_NOERR);
}

/*!
Specialized accessor function as an alternative to oc_dds_properties.
\param[in] link The link through which the server is accessed.
\param[in] ddsnode The node whose properties are of interest.
\param[out] rankp A pointer into which the rank is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_rank(OCobject link, OCobject ddsnode, size_t* rankp)
{
    OCnode* node;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(rankp) *rankp = node->array.rank;
    return OCTHROW(OC_NOERR);
}

/*!
Specialized accessor function as an alternative to oc_dds_properties.
\param[in] link The link through which the server is accessed.
\param[in] ddsnode The node whose properties are of interest.
\param[out] nattrp A pointer into which the number of attributes is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_attr_count(OCobject link, OCobject ddsnode, size_t* nattrp)
{
    OCnode* node;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(nattrp) {
        if(node->octype == OC_Attribute) {
            *nattrp = nclistlength(node->att.values);
        } else {
            *nattrp = nclistlength(node->attributes);
	}
    }
    return OCTHROW(OC_NOERR);
}

/*!
Specialized accessor function as an alternative to oc_dds_properties.
\param[in] link The link through which the server is accessed.
\param[in] ddsnode The node whose properties are of interest.
\param[out] rootp A pointer into which the the root of the tree containing
the node is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_root(OCobject link, OCobject ddsnode, OCobject* rootp)
{
    OCnode* node;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(rootp) *rootp = (OCobject)node->root;
    return OCTHROW(OC_NOERR);
}

/*!
Specialized accessor function as an alternative to oc_dds_properties.
\param[in] link The link through which the server is accessed.
\param[in] ddsnode The node whose properties are of interest.
\param[out] containerp A pointer into which the the immediate
container ddsnode is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_container(OCobject link, OCobject ddsnode, OCobject* containerp)
{
    OCnode* node;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(containerp) *containerp = (OCobject)node->container;
    return OCTHROW(OC_NOERR);
}

/*!
Obtain the DDS node corresponding to the i'th field
of a node that itself is a container (Dataset, Structure, Sequence, or Grid)

\param[in] link The link through which the server is accessed.
\param[in] ddsnode The container node of interest.
\param[in] index The index (starting at zero) of the field to return.
\param[out] fieldnodep  A pointer into which the i'th field node is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINDEX The index was greater than the number of fields.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
\retval OC_EBADTYPE The dds node is not a container node.
*/

OCerror
oc_dds_ithfield(OCobject link, OCobject ddsnode, size_t index, OCobject* fieldnodep)
{
    OCnode* node;
    OCnode* field;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(!ociscontainer(node->octype))
	return OCTHROW(OC_EBADTYPE);

    if(index >= nclistlength(node->subnodes))
	return OCTHROW(OC_EINDEX);

    field = (OCnode*)nclistget(node->subnodes,index);
    if(fieldnodep) *fieldnodep = (OCobject)field;
    return OCTHROW(OC_NOERR);
}

/*!
Alias for oc_dds_ithfield.

\param[in] link The link through which the server is accessed.
\param[in] ddsnode The container node of interest.
\param[in] index The index (starting at zero) of the field to return.
\param[out] fieldnodep  A pointer into which the i'th field node is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINDEX The index was greater than the number of fields.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
\retval OC_EBADTYPE The dds node is not a container node.
*/

OCerror
oc_dds_ithsubnode(OCobject link, OCobject ddsnode, size_t index, OCobject* fieldnodep)
{
    return OCTHROW(oc_dds_ithfield(link,ddsnode,index,fieldnodep));
}

/*!
Obtain the DDS node corresponding to the array of a Grid container.
Equivalent to oc_dds_ithfield(link,grid-container,0,arraynode).

\param[in] link The link through which the server is accessed.
\param[in] grid The grid container node of interest.
\param[out] arraynodep  A pointer into which the grid array node is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_gridarray(OCobject link, OCobject grid, OCobject* arraynodep)
{
    return OCTHROW(oc_dds_ithfield(link,grid,0,arraynodep));
}

/*!
Obtain the DDS node corresponding to the i'th map of a Grid container.
Equivalent to oc_dds_ithfield(link,grid-container,index+1,arraynode).
Note the map index starts at zero.

\param[in] link The link through which the server is accessed.
\param[in] grid The grid container node of interest.
\param[in] index The (zero-based) index of the map node to return.
\param[out] mapnodep  A pointer into which the grid map node is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINDEX The map index is illegal.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_gridmap(OCobject link, OCobject grid, size_t index, OCobject* mapnodep)
{
    return OCTHROW(oc_dds_ithfield(link,grid,index+1,mapnodep));
}


/*!
Obtain a dds node by name from a dds structure or dataset node.

\param[in] link The link through which the server is accessed.
\param[in] ddsnode The container node of interest.
\param[in] name The name of the field to return.
\param[out] fieldp  A pointer into which the name'th field node is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINDEX No field with the given name was found.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_fieldbyname(OCobject link, OCobject ddsnode, const char* name, OCobject* fieldp)
{
    OCerror err = OC_NOERR;
    OCnode* node;
    size_t count,i;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(!ociscontainer(node->octype))
	return OCTHROW(OC_EBADTYPE);

    /* Search the fields to find a name match */
    err = oc_dds_nsubnodes(link,ddsnode,&count);
    if(err != OC_NOERR) return err;
    for(i=0;i<count;i++) {
	OCobject field;
	char* fieldname = NULL;
	int match = 1;

        err = oc_dds_ithfield(link,ddsnode,i,&field);
        if(err != OC_NOERR) return err;
	/* Get the field's name */
        err = oc_dds_name(link,field,&fieldname);
        if(err != OC_NOERR) return err;
	if(fieldname != NULL) {
	    match = strcmp(name,fieldname);
	    free(fieldname);
	}
	if(match == 0) {
	    if(fieldp) *fieldp = field;
	    return OCTHROW(OC_NOERR);
	}
    }
    return OCTHROW(OC_EINDEX); /* name was not found */
}

/*!
Obtain the dimension nodes (of octype OC_Dimension)
associated with the node of interest.

\param[in] link The link through which the server is accessed.
\param[in] ddsnode The dds node of interest.
\param[out] dims  A vector into which the dimension nodes
are stored. The caller must allocate based on the rank of the node.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_dimensions(OCobject link, OCobject ddsnode, OCobject* dims)
{
    OCnode* node;
    size_t i;

    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(node->array.rank == 0) return OCTHROW(OCTHROW(OC_ESCALAR));
    if(dims != NULL) {
        for(i=0;i<node->array.rank;i++) {
            OCnode* dim = (OCnode*)nclistget(node->array.dimensions,i);
	    dims[i] = (OCobject)dim;
	}
    }
    return OCTHROW(OC_NOERR);
}

/*!
Obtain the i'th dimension node (of octype OC_Dimension)
associated with the node of interest.

\param[in] link The link through which the server is accessed.
\param[in] ddsnode The dds node of interest.
\param[in] index The index of the dimension to be returned.
\param[out] dimidp A pointer into which the index'th dimension is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINDEX The index is greater than the node's rank.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_ithdimension(OCobject link, OCobject ddsnode, size_t index, OCobject* dimidp)
{
    OCnode* node;
    OCobject dimid = NULL;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(node->array.rank == 0) return OCTHROW(OCTHROW(OC_ESCALAR));
    if(index >= node->array.rank) return OCTHROW(OCTHROW(OC_EINDEX));
    dimid = (OCobject)nclistget(node->array.dimensions,index);
    if(dimidp) *dimidp = dimid;
    return OCTHROW(OC_NOERR);
}

/*!
Obtain the properties of a dimension node.

\param[in] link The link through which the server is accessed.
\param[in] ddsnode The dimension node.
\param[out] sizep A pointer into which to store the size of the dimension.
\param[out] namep A pointer into which to store the name of the dimension.
If the dimension is anonymous, then the value NULL is returned as the name.
The caller must free the returned name.

\retval OC_NOERR The procedure executed normally.
\retval OC_BADTYPE If the node is not of type OC_Dimension.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dimension_properties(OCobject link, OCobject ddsnode, size_t* sizep, char** namep)
{
    OCnode* dim;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,dim,ddsnode);

    if(dim->octype != OC_Dimension) return OCTHROW(OCTHROW(OC_EBADTYPE));
    if(sizep) *sizep = dim->dim.declsize;
    if(namep) *namep = nulldup(dim->name);
    return OCTHROW(OC_NOERR);
}

/*!
Obtain just the set of sizes of the dimensions
associated with a dds node.

\param[in] link The link through which the server is accessed.
\param[in] ddsnode The node of interest
\param[out] dimsizes A vector into which the sizes of all
the dimensions of a node are stored. Its size is determined
by the rank of the node and must be allocated and free'd by the caller.

\retval OC_NOERR The procedure executed normally.
\retval OC_ESCALAR If the node is a scalar.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_dimensionsizes(OCobject link, OCobject ddsnode, size_t* dimsizes)
{
    OCnode* node;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    if(node->array.rank == 0) return OCTHROW(OCTHROW(OC_ESCALAR));
    if(dimsizes != NULL) {
	size_t i;
        for(i=0;i<node->array.rank;i++) {
            OCnode* dim = (OCnode*)nclistget(node->array.dimensions,i);
	    dimsizes[i] = dim->dim.declsize;
	}
    }
    return OCTHROW(OC_NOERR);
}

/*!
Return the name, type, length, and values associated with
the i'th attribute of a specified node. The actual attribute
strings are returned and the user must do any required
conversion based on the octype.  The strings argument must
be allocated and freed by caller.  Standard practice is to
call twice, once with the strings argument == NULL so we get
the number of values, then the second time with an allocated
char** vector.  The caller should reclaim the contents of
the returned string vector using <i>oc_reclaim_strings</i>.

\param[in] link The link through which the server is accessed.
\param[in] ddsnode The node of interest
\param[in] index Return the information of the index'th attribute.
\param[out] namep A pointer into which the attribute's name is stored.
It must be freed by the caller.
\param[out] octypep A pointer into which the attribute's atomic type is stored.
\param[out] nvaluesp A pointer into which the number
of attribute values is stored.
\param[out] strings A vector into which the values of the attribute
are stored. It must be allocated and free'd by the caller.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINDEX If the index is more than the number of attributes.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_attr(OCobject link, OCobject ddsnode, size_t index,
			   char** namep, OCtype* octypep,
			   size_t* nvaluesp, char** strings)
{
    int i;
    OCnode* node;
    OCattribute* attr;
    size_t nattrs;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);

    nattrs = nclistlength(node->attributes);
    if(index >= nattrs) return OCTHROW(OCTHROW(OC_EINDEX));
    attr = (OCattribute*)nclistget(node->attributes,index);
    if(namep) *namep = strdup(attr->name);
    if(octypep) *octypep = attr->etype;
    if(nvaluesp) *nvaluesp = attr->nvalues;
    if(strings) {
	if(attr->nvalues > 0) {
	    for(i=0;i<attr->nvalues;i++)
	        strings[i] = nulldup(attr->values[i]);
	}
    }
    return OCTHROW(OC_NOERR);
}

/*!
Given a counted vector of strings, free up all of the strings,
BUT NOT THE VECTOR since that was allocated by the caller.

\param[in] n The link through which the server is accessed.
\param[in] svec The node of interest.
*/

void
oc_reclaim_strings(size_t n, char** svec)
{
    int i;
    for(i=0;i<n;i++) if(svec[i] != NULL) free(svec[i]);
}

/*!
Return the count of DAS attribute values.

\param[in] link The link through which the server is accessed.
\param[in] dasnode The node of interest
\param[out] nvaluesp A pointer into which the number of attributes
is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EBADTPE If the node is not of type OC_Attribute.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_das_attr_count(OCobject link, OCobject dasnode, size_t* nvaluesp)
{
    OCnode* attr;
    OCVERIFY(OC_Node,dasnode);
    OCDEREF(OCnode*,attr,dasnode);
    if(attr->octype != OC_Attribute) return OCTHROW(OCTHROW(OC_EBADTYPE));
    if(nvaluesp) *nvaluesp = nclistlength(attr->att.values);
    return OCTHROW(OC_NOERR);
}

/*!
The procedure oc_das_attr returns the i'th string value
associated with a DAS object of type <i>OC_Attribute</i>.
Note carefully that this operation applies to DAS nodes
and not to DDS or DATADDS nodes.
Note also that the returned value is always a string
and it is the caller;'s responsibility to free the returned string.

\param[in] link The link through which the server is accessed.
\param[in] dasnode The DAS node of interest.
\param[in] index The index of the das value to return.
\param[in] atomtypep A pointer into which is stored the atomic
type of the attribute.
\param[out] valuep A vector into which the attribute's string values
are stored. Caller must allocate and free.

\retval OC_NOERR The procedure executed normally.
\retval OC_EBADTPE If the node is not of type OC_Attribute.
\retval OC_EINDEX If the index is larger than the number of attributes.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_das_attr(OCobject link, OCobject dasnode, size_t index, OCtype* atomtypep, char** valuep)
{
    OCnode* attr;
    size_t nvalues;
    OCVERIFY(OC_Node,dasnode);
    OCDEREF(OCnode*,attr,dasnode);

    if(attr->octype != OC_Attribute) return OCTHROW(OCTHROW(OC_EBADTYPE));
    nvalues = nclistlength(attr->att.values);
    if(index >= nvalues) return OCTHROW(OCTHROW(OC_EINDEX));
    if(atomtypep) *atomtypep = attr->etype;
    if(valuep) *valuep = nulldup((char*)nclistget(attr->att.values,index));
    return OCTHROW(OC_NOERR);
}

/**@}*/

/**************************************************/
/*!\defgroup Interconnection Node Interconnection Management */

/**@{*/

/*!
As a rule, the attributes of an object are accessed using
the <i>oc_dds_attr</i> procedure rather than by traversing a
DAS.  In order to support this, the <i>oc_merge_das</i>
procedure annotates a DDS node with attribute values taken
from a specified DAS node.

\param[in] link The link through which the server is accessed.
\param[in] dasroot The root object of a DAS tree.
\param[in] ddsroot The root object of a DDS (or DataDDS) tree.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_merge_das(OCobject link, OCobject dasroot, OCobject ddsroot)
{
    OCstate* state;
    OCnode* das;
    OCnode* dds;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Node,dasroot);
    OCDEREF(OCnode*,das,dasroot);
    OCVERIFY(OC_Node,ddsroot);
    OCDEREF(OCnode*,dds,ddsroot);

    return OCTHROW(ocddsdasmerge(state,das,dds));
}

/**@}*/

/**************************************************/

/*!\defgroup Data Data Management */
/**@{*/

/*!
Obtain the datanode root associated with a DataDDS tree.
Do not confuse this with oc_data_root.
This procedure, given the DDS tree root, gets the data tree root.

\param[in] link The link through which the server is accessed.
\param[in] ddsroot The DataDDS tree root.
\param[out] datarootp A pointer into which the datanode root is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_getdataroot(OCobject link, OCobject ddsroot, OCobject* datarootp)
{
    OCerror ocerr = OC_NOERR;
    OCstate* state;
    OCnode* root;
    OCdata* droot;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Node,ddsroot);
    OCDEREF(OCnode*,root,ddsroot);

    if(datarootp == NULL)
	return OCTHROW(OCTHROW(OC_EINVAL));
    ocerr = ocdata_getroot(state,root,&droot);
    if(ocerr == OC_NOERR && datarootp)
	*datarootp = (OCobject)droot;
    return OCTHROW(ocerr);
}

/*!
Obtain the data instance corresponding to the i'th field
of a data node instance that itself is a container instance.

\param[in] link The link through which the server is accessed.
\param[in] datanode The container data node instance of interest.
\param[in] index The index (starting at zero) of the field instance to return.
\param[out] fieldp  A pointer into which the i'th field instance is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINDEX The index was greater than the number of fields.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
\retval OC_EBADTYPE The data node is not a container node.
*/

OCerror
oc_data_ithfield(OCobject link, OCobject datanode, size_t index, OCobject* fieldp)
{
    OCerror ocerr = OC_NOERR;
    OCstate* state;
    OCdata* data;
    OCdata* field;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);

    if(fieldp == NULL) return OCTHROW(OCTHROW(OC_EINVAL));
    ocerr = ocdata_ithfield(state,data,index,&field);
    if(ocerr == OC_NOERR)
	*fieldp = (OCobject)field;
    return OCTHROW(ocerr);
}

/*!
Obtain a data node by name from a container data node.

\param[in] link The link through which the server is accessed.
\param[in] datanode The container data node instance of interest.
\param[in] name The name of the field instance to return.
\param[out] fieldp  A pointer into which the i'th field instance is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINDEX No field with the given name was found.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
\retval OC_EBADTYPE The data node is not a container node.
*/

OCerror
oc_data_fieldbyname(OCobject link, OCobject datanode, const char* name, OCobject* fieldp)
{
    OCerror err = OC_NOERR;
    size_t count=0,i;
    OCobject ddsnode;
    OCVERIFY(OC_State,link);
    OCVERIFY(OC_Data,datanode);

    /* Get the dds node for this datanode */
    err = oc_data_ddsnode(link,datanode,&ddsnode);
    if(err != OC_NOERR) return err;

    /* Search the fields to find a name match */
    err = oc_dds_nsubnodes(link,ddsnode,&count);
    if(err != OC_NOERR) return err;
    for(i=0;i<count;i++) {
	int match;
	OCobject field;
	char* fieldname = NULL;
        err = oc_dds_ithfield(link,ddsnode,i,&field);
        if(err != OC_NOERR) return err;
	/* Get the field's name */
        err = oc_dds_name(link,field,&fieldname);
        if(err != OC_NOERR) return err;
 	if(!fieldname)
	  return OCTHROW(OC_EINVAL);

	match = strcmp(name,fieldname);
	if(fieldname != NULL) free(fieldname);
	if(match == 0) {
	    /* Get the ith datasubnode */
	    err = oc_data_ithfield(link,datanode,i,&field);
            if(err != OC_NOERR) return err;
	    if(fieldp) *fieldp = field;
	    return OCTHROW(OC_NOERR);
	}
    }
    return OCTHROW(OC_EINDEX); /* name was not found */
}

/*!
Obtain the data instance corresponding to the array field
of a Grid container instance.
Equivalent to oc_data_ithfield(link,grid,0,arraydata).

\param[in] link The link through which the server is accessed.
\param[in] grid The grid container instance of interest.
\param[out] arraydatap  A pointer into which the grid array instance is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_data_gridarray(OCobject link, OCobject grid, OCobject* arraydatap)
{
    return OCTHROW(oc_data_ithfield(link,grid,0,arraydatap));
}

/*!
Obtain the data instance corresponding to the ith map field
of a Grid container instance.
Equivalent to oc_data_ithfield(link,grid-container,index+1,mapdata).
Note that Map indices start at zero.

\param[in] link The link through which the server is accessed.
\param[in] grid The grid container instance of interest.
\param[in] index The map index of the map to return.
\param[out] mapdatap A pointer into which the grid map instance is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_data_gridmap(OCobject link, OCobject grid, size_t index, OCobject* mapdatap)
{
    return OCTHROW(oc_data_ithfield(link,grid,index+1,mapdatap));
}

/*!
Obtain the data instance corresponding to the container
of a specified instance object.

\param[in] link The link through which the server is accessed.
\param[in] datanode The data instance of interest
\param[out] containerp  A pointer into which the container instance is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL The data object has no container
(=> it is a Dataset instance).
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_data_container(OCobject link,  OCobject datanode, OCobject* containerp)
{
    OCerror ocerr = OC_NOERR;
    OCstate* state;
    OCdata* data;
    OCdata* container;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);

    if(containerp == NULL) return OCTHROW(OCTHROW(OC_EINVAL));
    ocerr = ocdata_container(state,data,&container);
    if(ocerr == OC_NOERR)
	*containerp = (OCobject)container;
    return OCTHROW(ocerr);
}

/*!
Obtain the data instance corresponding to the root of the tree
of which the specified instance object is a part.
Do not confuse this with oc_dds_getdataroot.
This procedure, given any node in a data tree, get the root of that tree.

\param[in] link The link through which the server is accessed.
\param[in] datanode The data instance of interest
\param[out] rootp  A pointer into which the root instance is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_data_root(OCobject link, OCobject datanode, OCobject* rootp)
{
    OCerror ocerr = OC_NOERR;
    OCstate* state;
    OCdata* data;
    OCdata* root;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);

    if(rootp == NULL) return OCTHROW(OCTHROW(OC_EINVAL));
    ocerr = ocdata_root(state,data,&root);
    if(ocerr == OC_NOERR)
	*rootp = (OCobject)root;
    return OCTHROW(ocerr);
}

/*!
Return the data of a dimensioned Structure corresponding
to the element instance specified by the indices argument.

\param[in] link The link through which the server is accessed.
\param[in] datanode The data node instance of interest.
\param[in] indices A vector of indices specifying the element instance
to return. This vector must be allocated and free'd by the caller.
\param[out] elementp  A pointer into which the element instance is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EBADTYPE The data instance was not of type OC_Structure
or was a scalar.
\retval OC_EINDEX The indices specified an illegal element.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_data_ithelement(OCobject link, OCobject datanode, size_t* indices, OCobject* elementp)
{
    OCerror ocerr = OC_NOERR;
    OCstate* state;
    OCdata* data;
    OCdata* element;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);

    if(indices == NULL || elementp == NULL) return OCTHROW(OCTHROW(OC_EINVAL));
    ocerr = ocdata_ithelement(state,data,indices,&element);
    if(ocerr == OC_NOERR)
	*elementp = (OCobject)element;
    return OCTHROW(ocerr);
}

/*!
Return the i'th record instance
of a Sequence data instance.

\param[in] link The link through which the server is accessed.
\param[in] datanode The data node instance of interest.
\param[in] index The record instance to return.
\param[out] recordp  A pointer into which the record instance is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EBADTYPE The data instance was not of type OC_Sequence
\retval OC_EINDEX The indices is larger than the number of records
of the Sequence.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

extern OCerror oc_data_ithrecord(OCobject link, OCobject datanode, size_t index, OCobject* recordp)
{
    OCerror ocerr = OC_NOERR;
    OCstate* state;
    OCdata* data;
    OCdata* record;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);

    if(recordp == NULL) return OCTHROW(OCTHROW(OC_EINVAL));
    ocerr = ocdata_ithrecord(state,data,index,&record);
    if(ocerr == OC_NOERR)
	*recordp = (OCobject)record;
    return OCTHROW(ocerr);
}

/*!
Return the i'th record instance
of a Sequence data instance.
Return the indices for this data instance; Assumes the data
was obtained using oc_data_ithelement or oc_data_ithrecord.

\param[in] link The link through which the server is accessed.
\param[in] datanode The data node instance of interest.
\param[out] indices A vector into which the indices of the
data instance are stored. If the data instance is a record,
then only indices[0] is used.

\retval OC_NOERR The procedure executed normally.
\retval OC_EBADTYPE The data instance was not of type OC_Sequence
or it was not a dimensioned instance of OC_Structure.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_data_position(OCobject link, OCobject datanode, size_t* indices)
{
    OCstate* state;
    OCdata* data;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);
    if(indices == NULL) return OCTHROW(OCTHROW(OC_EINVAL));
    return OCTHROW(ocdata_position(state,data,indices));
}

/*!
Return the number of records associated with a Sequence
data object. Be warned that applying this procedure
to a record data instance (as opposed to an instance
representing a whole Sequence) will return an error.
More succinctly, the data object's OCtype must be of
type OC_Sequence and oc_data_indexable() must be true.

\param[in] link The link through which the server is accessed.
\param[in] datanode The data node instance of interest.
\param[out] countp A pointer into which the record count is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EBADTYPE The data instance was not of type OC_Sequence
or it was a record data instance.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_data_recordcount(OCobject link, OCobject datanode, size_t* countp)
{
    OCstate* state;
    OCdata* data;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);
    if(countp == NULL) return OCTHROW(OCTHROW(OC_EINVAL));
    return OCTHROW(ocdata_recordcount(state,data,countp));
}

/*!
Return the dds node that is the "pattern"
for this data instance.

\param[in] link The link through which the server is accessed.
\param[in] datanode The data node instance of interest.
\param[out] nodep A pointer into which the ddsnode is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_data_ddsnode(OCobject link, OCobject datanode, OCobject* nodep)
{
    OCerror ocerr = OC_NOERR;
    OCdata* data;
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);

    OCASSERT(data->pattern != NULL);
    if(nodep == NULL) ocerr = OC_EINVAL;
    else *nodep = (OCobject)data->pattern;
    return OCTHROW(ocerr);
}

/*!
Return the OCtype of the ddsnode that is the "pattern"
for this data instance. This is a convenience function
since it can be obtained using a combination of other
API procedures.

\param[in] link The link through which the server is accessed.
\param[in] datanode The data node instance of interest.
\param[out] typep A pointer into which the OCtype value is stored.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_data_octype(OCobject link, OCobject datanode, OCtype* typep)
{
    OCerror ocerr = OC_NOERR;
    OCdata* data;
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);

    OCASSERT(data->pattern != NULL);
    if(typep == NULL) ocerr = OC_EINVAL;
    else *typep = data->pattern->octype;
    return OCTHROW(ocerr);
}

/*!
Return the value one (1) if the specified data instance
is indexable. Indexable means that the data instance
is a dimensioned Structure or it is a Sequence (but not
a record in a Sequence).

\param[in] link The link through which the server is accessed.
\param[in] datanode The data node instance of interest.

\retval one(1) if the specified data instance is indexable.
\retval zero(0) otherwise.
*/

int
oc_data_indexable(OCobject link, OCobject datanode)
{
    OCdata* data;
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);

    return (fisset(data->datamode,OCDT_ARRAY)
	    || fisset(data->datamode,OCDT_SEQUENCE)) ? 1 : 0;
}

/*!
Return the value one (1) if the specified data instance
was obtained by applying either the procedure oc_data_ithelement
or oc_data_ithrecord. This means that the operation
oc_data_position() will succeed when applied to this data instance.

\param[in] link The link through which the server is accessed.
\param[in] datanode The data node instance of interest.

\retval one(1) if the specified data instance has an index.
\retval zero(0) otherwise.
*/

int
oc_data_indexed(OCobject link, OCobject datanode)
{
    OCdata* data;
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);

    return (fisset(data->datamode,OCDT_ELEMENT)
	    || fisset(data->datamode,OCDT_RECORD)) ? 1 : 0;
}

/**************************************************/

/*!
This procedure does the work of actually extracting data
from a leaf instance of a data tree and storing it into
memory for use by the calling code.  The data instance must be
referencing either a scalar primitive value or an array of
primitive values. That is, its oc_data_octype()
value must be OCatomic.
If the variable is a scalar, then the
index and edge vectors will be ignored.

\param[in] link The link through which the server is accessed.
\param[in] datanode The data node instance of interest.
\param[in] start A vector of indices specifying the starting element
to return.
\param[in] edges A vector of indices specifying the count in each dimension
of the number of elements to return.
\param[in] memsize The size (in bytes) of the memory argument.
\param[out] memory User allocated memory into which the extracted
data is to be stored. The caller is responsible for allocating and free'ing
this argument.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL The memsize argument is too small to hold
the specified data.
\retval OC_EINVALCOORDS The start and/or edges argument is outside
the range of legal indices.
\retval OC_EDATADDS The data retrieved from the server was malformed
and the read request cannot be completed.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_data_read(OCobject link, OCobject datanode,
                 size_t* start, size_t* edges,
	         size_t memsize, void* memory)
{
    OCdata* data;
    OCnode* pattern;
    size_t count, rank;

    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);

    if(start == NULL && edges == NULL) /* Assume it is a scalar read */
        return OCTHROW(oc_data_readn(link,datanode,start,0,memsize,memory));

    if(edges == NULL)
	return OCTHROW(OCTHROW(OC_EINVALCOORDS));

    /* Convert edges to a count */
    pattern = data->pattern;
    rank = pattern->array.rank;
    count = octotaldimsize(rank,edges);

    return OCTHROW(oc_data_readn(link,datanode,start,count,memsize,memory));
}


/*!
This procedure is a variant of oc_data_read for reading a
single scalar from a leaf instance of a data tree and
storing it into memory for use by the calling code.  The
data instance must be referencing a scalar primitive value.
That is, its oc_data_octype() value must be OCatomic.

\param[in] link The link through which the server is accessed.
\param[in] datanode The data node instance of interest.
\param[in] memsize The size (in bytes) of the memory argument.
\param[out] memory User allocated memory into which the extracted
data is to be stored. The caller is responsible for allocating and free'ing
this argument.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL The memsize argument is too small to hold
the specified data.
\retval OC_ESCALAR The data instance is not a scalar.
\retval OC_EDATADDS The data retrieved from the server was malformed
and the read request cannot be completed.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_data_readscalar(OCobject link, OCobject datanode,
	         size_t memsize, void* memory)
{
    return OCTHROW(oc_data_readn(link,datanode,NULL,0,memsize,memory));
}

/*!
This procedure is a variant of oc_data_read for reading
nelements of values starting at a given index position.
If the variable is a scalar, then the
index vector and count will be ignored.

\param[in] link The link through which the server is accessed.
\param[in] datanode The data node instance of interest.
\param[in] start A vector of indices specifying the starting element
to return.
\param[in] N The number of elements to read. Reading is assumed
to use row-major order.
\param[in] memsize The size (in bytes) of the memory argument.
\param[out] memory User allocated memory into which the extracted
data is to be stored. The caller is responsible for allocating and free'ing
this argument.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL The memsize argument is too small to hold
the specified data.
\retval OC_EINVALCOORDS The start and/or count argument is outside
the range of legal indices.
\retval OC_EDATADDS The data retrieved from the server was malformed
and the read request cannot be completed.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_data_readn(OCobject link, OCobject datanode,
                 const size_t* start, size_t N,
	         size_t memsize, void* memory)
{
    OCerror ocerr = OC_NOERR;
    OCstate* state;
    OCdata* data;
    OCnode* pattern;
    size_t rank,startpoint;

    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);

    /* Do argument validation */

    if(memory == NULL || memsize == 0)
	return OCTHROW(OC_EINVAL);

    pattern = data->pattern;
    rank = pattern->array.rank;

    if(rank == 0) {
	startpoint = 0;
	N = 1;
    } else if(start == NULL) {
        return OCTHROW(OCTHROW(OC_EINVALCOORDS));
    } else {/* not scalar */
	startpoint = ocarrayoffset(rank,pattern->array.sizes,start);
    }
    if(N > 0)
        ocerr = ocdata_read(state,data,startpoint,N,memory,memsize);
    if(ocerr == OC_EDATADDS)
	ocdataddsmsg(state,pattern->tree);
    return OCTHROW(OCTHROW(ocerr));
}



/*!
This procedure has the same semantics as oc_data_read.
However, it takes an OCddsnode as argument.
The limitation is that the DDS node must be a top-level,
atomic variable.
Top-level means that it is not nested in a Sequence or a
dimensioned Structure; being in a Grid is ok as is being in
a scalar structure.

\param[in] link The link through which the server is accessed.
\param[in] ddsnode The dds node instance of interest.
\param[in] start A vector of indices specifying the starting element
to return.
\param[in] edges A vector of indices specifying the count in each dimension
of the number of elements to return.
\param[in] memsize The size (in bytes) of the memory argument.
\param[out] memory User allocated memory into which the extracted
data is to be stored. The caller is responsible for allocating and free'ing
this argument.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL The memsize argument is too small to hold
the specified data.
\retval OC_EINVALCOORDS The start and/or edges argument is outside
the range of legal indices.
\retval OC_EDATADDS The data retrieved from the server was malformed
and the read request cannot be completed.

*/

OCerror
oc_dds_read(OCobject link, OCobject ddsnode,
                 size_t* start, size_t* edges,
	         size_t memsize, void* memory)
{
    OCdata* data;
    OCnode* dds;

    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,dds,ddsnode);

    /* Get the data associated with this top-level node */
    data = dds->data;
    if(data == NULL) return OCTHROW(OC_EINVAL);
    return OCTHROW(oc_data_read(link,data,start,edges,memsize,memory));
}


/*!
This procedure is a variant of oc_data_read for reading a single scalar.
This procedure has the same semantics as oc_data_readscalar.
However, it takes an OCddsnode as argument.
The limitation is that the DDS node must be a top-level, atomic variable.
Top-level means that it is not nested in a Sequence or a
dimensioned Structure; being in a Grid is ok as is being in
a scalar structure.

\param[in] link The link through which the server is accessed.
\param[in] ddsnode The dds node instance of interest.
\param[in] memsize The size (in bytes) of the memory argument.
\param[out] memory User allocated memory into which the extracted
data is to be stored. The caller is responsible for allocating and free'ing
this argument.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL The memsize argument is too small to hold
the specified data.
\retval OC_ESCALAR The data instance is not a scalar.
\retval OC_EDATADDS The data retrieved from the server was malformed
and the read request cannot be completed.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_readscalar(OCobject link, OCobject ddsnode,
	         size_t memsize, void* memory)
{
    return OCTHROW(oc_dds_readn(link,ddsnode,NULL,0,memsize,memory));
}

/*!
This procedure is a variant of oc_dds_read for reading
nelements of values starting at a given index position.
If the variable is a scalar, then the
index vector and count will be ignored.
This procedure has the same semantics as oc_data_readn.
However, it takes an OCddsnode as argument.
The limitation is that the DDS node must be a top-level, atomic variable.
Top-level means that it is not nested in a Sequence or a
dimensioned Structure; being in a Grid is ok as is being in
a scalar structure.

\param[in] link The link through which the server is accessed.
\param[in] ddsnode The dds node instance of interest.
\param[in] start A vector of indices specifying the starting element
to return.
\param[in] N The number of elements to read. Reading is assumed
to use row-major order.
\param[in] memsize The size (in bytes) of the memory argument.
\param[out] memory User allocated memory into which the extracted
data is to be stored. The caller is responsible for allocating and free'ing
this argument.

\retval OC_NOERR The procedure executed normally.
\retval OC_EINVAL The memsize argument is too small to hold
the specified data.
\retval OC_EINVALCOORDS The start and/or count argument is outside
the range of legal indices.
\retval OC_EDATADDS The data retrieved from the server was malformed
and the read request cannot be completed.
\retval OC_EINVAL  One of the arguments (link, etc.) was invalid.
*/

OCerror
oc_dds_readn(OCobject link, OCobject ddsnode,
                 size_t* start, size_t N,
	         size_t memsize, void* memory)
{
    OCdata* data;
    OCnode* dds;

    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,dds,ddsnode);

    /* Get the data associated with this top-level node */
    data = dds->data;
    if(data == NULL) return OCTHROW(OC_EINVAL);
    return OCTHROW(oc_data_readn(link,data,start,N,memsize,memory));
}

/**@}*/

/**************************************************/
/*!\defgroup OCtype OCtype Management
@{*/

/*!
Return the size of the C data structure corresponding
to a given atomic type.
For example,
oc_typesize(OC_Int32) == sizeof(int), and
oc_typesize(OC_String) == sizeof(char*).
Non-atomic types (e.g. OC_Structure) return zero.

\param[in] etype The atomic type.

\return The C size of the atomic type.
*/

size_t
oc_typesize(OCtype etype)
{
    return octypesize(etype);
}

/*!
Return a string corresponding to the
to a given OCtype.
For example,
oc_typetostring(OC_Int32) == "Int32" and
oc_typesize(OC_Structure) == "Structure".
The caller MUST NOT free the returned string.

\param[in] octype The OCtype value.

\return The name, as a string, of that OCtype value.
*/

const char*
oc_typetostring(OCtype octype)
{
    return octypetoddsstring(octype);
}

/*!
Print a value of an atomic type instance.
This is primarily for debugging and provides
a simple way to convert a value to a printable string.

\param[in] etype The OCtype atomic type.
\param[in] value A pointer to the value to be printed.
\param[in] bufsize The size of the buffer argument
\param[in] buffer The buffer into which to store the printable
value as a NULL terminated string.

\retval OC_NOERR if the procedure succeeded
\retval OC_EINVAL  if one of the arguments is illegal.
*/

OCerror
oc_typeprint(OCtype etype, void* value, size_t bufsize, char* buffer)
{
    return OCTHROW(octypeprint(etype,value,bufsize,buffer));
}

/**@}*/

/**************************************************/
/* The oc_logXXX procedures are defined in oclog.c */

/**************************************************/
/* Miscellaneous */

/*!\defgroup Miscellaneous Miscellaneous Procedures
@{*/

/*!
Return a user-readable error message corresponding
to a given OCerror value.

\param[in] err The OCerror value.

\return The error message
*/

const char*
oc_errstring(OCerror err)
{
    return ocerrstring(err);
}


/**************************************************/

/*!
Sometimes, when a fetch request fails, there will be
error information in the reply from the server.
Typically this only occurs if an API operation
returns OC_EDAS, OC_EDDS, OC_EDATADDS, or OC_EDAPSVC.
This procedure will attempt to locate and return information
from such an error reply.

The error reply contains three pieces of information.
<ol>
<li> code - a string representing a numeric error code.
<li> msg - a string representing an extended error message.
<li> http - an integer representing an HTTP error return (e.g. 404).
</ol>

\param[in] link The link through which the server is accessed.
\param[in] codep A pointer for returning the error code.
\param[in] msgp A pointer for returning the error message.
\param[in] httpp A pointer for returning the HTTP error number.

\retval OC_NOERR if an error was found and the return values are defined.
\retval OC_EINVAL  if no error reply could be found, so the return
values are meaningless.
*/

OCerror
oc_svcerrordata(OCobject link, char** codep,
                               char** msgp, long* httpp)
{
    OCstate* state;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    return OCTHROW(ocsvcerrordata(state,codep,msgp,httpp));
}

/*!
Obtain the HTTP code (e.g. 200, 404, etc) from the last
fetch command.

\param[in] link The link through which the server is accessed.

\retval the HTTP code
*/

int
oc_httpcode(OCobject link)
{
    OCstate* state;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    return (int)state->error.httpcode;
}

/**************************************************/
/* New 10/31/2009: return the size(in bytes)
   of the fetched datadds.
*/

/*!
Return the size of the in-memory or on-disk
data chunk returned by the server.

\param[in] link The link through which the server is accessed.
\param[in] ddsroot The root dds node of the tree whose xdr size is desired.
\param[out] xdrsizep The size in bytes of the returned packet.

\retval OC_NOERR if the procedure succeeded
\retval OC_EINVAL if an argument was invalid
*/

OCerror
oc_raw_xdrsize(OCobject link, OCobject ddsroot, off_t* xdrsizep)
{
    OCnode* root;
    OCVERIFY(OC_Node,ddsroot);
    OCDEREF(OCnode*,root,ddsroot);

    if(root->root == NULL || root->root->tree == NULL
	|| root->root->tree->dxdclass != OCDATADDS)
	    return OCTHROW(OCTHROW(OC_EINVAL));
    if(xdrsizep) *xdrsizep = root->root->tree->data.datasize;
    return OCTHROW(OC_NOERR);
}

/* Resend a url as a head request to check the Last-Modified time */
OCerror
oc_update_lastmodified_data(OCobject link, OCflags flags)
{
    OCstate* state;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    return OCTHROW(ocupdatelastmodifieddata(state,flags));
}

long
oc_get_lastmodified_data(OCobject link)
{
    OCstate* state;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    return state->datalastmodified;
}

/* Given an arbitrary OCnode, return the connection of which it is a part */
OCerror
oc_get_connection(OCobject ddsnode, OCobject* linkp)
{
    OCnode* node;
    OCVERIFY(OC_Node,ddsnode);
    OCDEREF(OCnode*,node,ddsnode);
    if(linkp) *linkp = node->root->tree->state;
    return OCTHROW(OC_NOERR);
}


/*!
Attempt to retrieve a dataset using a specified URL
and using the DAP protocol.

\param[in] url The url to use for the request.

\retval OC_NOERR if the request succeeded.
\retval OC_EINVAL if the request failed.
*/

OCerror
oc_ping(const char* url)
{
    return OCTHROW(ocping(url));
}
/**@}*/

/**************************************************/

int
oc_dumpnode(OCobject link, OCobject ddsroot)
{
    OCnode* root;
    OCerror ocerr = OC_NOERR;
    OCVERIFY(OC_Node,ddsroot);
    OCDEREF(OCnode*,root,ddsroot);
    ocdumpnode(root);
    return OCTHROW(ocerr);
}

/**************************************************/
/* ocx.h interface */
/* Following procedures are in API, but are not
   externally documented.
*/


OCerror
oc_dds_dd(OCobject link, OCobject ddsroot, int level)
{
    OCstate* state;
    OCnode* root;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Node,ddsroot);
    OCDEREF(OCnode*,root,ddsroot);

    ocdd(state,root,1,level);
    return OCTHROW(OC_NOERR);
}

OCerror
oc_dds_ddnode(OCobject link, OCobject ddsroot)
{
    OCnode* root;
    OCVERIFY(OC_Node,ddsroot);
    OCDEREF(OCnode*,root,ddsroot);

    ocdumpnode(root);
    return OCTHROW(OC_NOERR);
}

OCerror
oc_data_ddpath(OCobject link, OCobject datanode, char** resultp)
{
    OCstate* state;
    OCdata* data;
    NCbytes* buffer;

    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);

    buffer = ncbytesnew();
    ocdumpdatapath(state,data,buffer);
    if(resultp) *resultp = ncbytesdup(buffer);
    ncbytesfree(buffer);
    return OCTHROW(OC_NOERR);
}

OCerror
oc_data_ddtree(OCobject link, OCobject ddsroot)
{
    OCstate* state;
    OCdata* data;
    NCbytes* buffer;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    OCVERIFY(OC_Data,ddsroot);
    OCDEREF(OCdata*,data,ddsroot);

    buffer = ncbytesnew();
    ocdumpdatatree(state,data,buffer,0);
    fprintf(stderr,"%s\n",ncbytescontents(buffer));
    ncbytesfree(buffer);
    return OCTHROW(OC_NOERR);
}

OCerror
oc_data_mode(OCobject link, OCobject datanode, OCDT* modep)
{
    OCdata* data;
    OCVERIFY(OC_Data,datanode);
    OCDEREF(OCdata*,data,datanode);

    if(modep) *modep = data->datamode;
    return OC_NOERR;
}

#if 0
/* Free up a datanode that is no longer being used;
   Currently does nothing
*/
OCerror
oc_data_free(OCobject link, OCobject datanode)
{
    return OCTHROW(OC_NOERR);
}
#endif

/* Free up a ddsnode that is no longer being used;
   Currently does nothing
*/
OCerror
oc_dds_free(OCobject link, OCobject dds0)
{
    return OCTHROW(OC_NOERR);
}


/**************************************************/
/* Curl specific  options */

#if 0
/*!\defgroup Curl Curl-specifi Procedures
@{*/

/*!
Set an arbitrary curl option. Option
must be one of the ones supported by oc.

\param[in] link The link through which the server is accessed.
\param[in] option The name of the option to set;
                  See occurlfunction.c for
                  the set of supported flags and names.
\param[in] value The option value.

\retval OC_NOERR if the request succeeded.
\retval OC_ECURL if the request failed.
*/

OCerror
oc_set_curlopt(OClink link, const char* option, void* value)
{
    OCstate* state;
    struct OCCURLFLAG* f;

    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    f = occurlflagbyname(option);
    if(f == NULL)
	return OCTHROW(OC_ECURL);
    return OCTHROW(ocset_curlopt(state,f->flag,value));
}
#endif

/*!
Set the absolute path to use for the .netrc file

\param[in] link The link through which the server is accessed.
\param[in] netrc The path to use.

\retval OC_NOERR if the request succeeded.
\retval OC_EINVAL if the request failed, typically
                  because the path string is null or zero-length.
*/

OCerror
oc_set_netrc(OClink* link, const char* file)
{
    OCstate* state;
    FILE* f;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);


    if(file == NULL || strlen(file) == 0)
	return OC_EINVAL;
    nclog(OCLOGDBG,"OC: using netrc file: %s",file);
    /* See if it exists and is readable; ignore if not */
    f = NCfopen(file,"r");
    if(f != NULL) { /* Log what rc file is being used */
	nclog(NCLOGNOTE,"OC: netrc file found: %s",file);
	fclose(f);
    }
    return OCTHROW(ocset_netrc(state,file));
}

/*!
Set the user agent field.

\param[in] link The link through which the server is accessed.
\param[in] agent The user agent string

\retval OC_NOERR if the request succeeded.
\retval OC_EINVAL if the request failed, typically
                  because the agent string is null or zero-length.
*/

OCerror
oc_set_useragent(OCobject link, const char* agent)
{
    OCstate* state;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);

    if(agent == NULL || strlen(agent) == 0)
	return OC_EINVAL;
    return OCTHROW(ocset_useragent(state,agent));
}

/*!
Force the curl library to trace its actions.

\param[in] link The link through which the server is accessed.

\retval OC_NOERR if the request succeeded.
*/

OCerror
oc_trace_curl(OCobject link)
{
    OCstate* state;
    OCVERIFY(OC_State,link);
    OCDEREF(OCstate*,state,link);
    oc_curl_debug(state);
    return OCTHROW(OC_NOERR);
}

/**@}*/
