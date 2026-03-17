/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT dap for more information. */

/*
OC External Interface
Created: 4/4/2009
Last Revised: 12/23/2014
Version: 2.1
*/

#ifndef OC_H
#define OC_H

#include <stdlib.h>
#include <stdio.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

/*!\file oc.h
*/

/*!\defgroup Definitions Constants, types, etc.
@{*/

/*! OC_MAX_DIMS is the maximum allowable
number of dimensions.
Ideally, it should be greater or equal to max allowed by dap or netcdf
*/
#define OC_MAX_DIMENSIONS 1024

/*!\enum OCdxd
Define the legal kinds of fetch: DAS, DDS, or DataDDS
*/
typedef enum OCdxd {
OCDDS=0,
OCDAS=1,
OCDATADDS=2
} OCdxd;

/*!\def OCDATA
Define an alias for OCDATADDS
*/
#define OCDATA OCDATADDS

/*!
\typedef OCflags
Define flags for oc_fetch.
*/
typedef int OCflags;
/*!\def OCONDISK
Cause oc_fetch to store the retrieved data on disk.
*/
#define OCONDISK 1

/*!\def OCENCODEPATH
Cause oc_fetch to encode path part of a URL
*/
#define OCENCODEPATH 2

/*!\def OCENCODEQUERY
Cause oc_fetch to encode query part of a URL
*/
#define OCENCODEQUERY 4

/**************************************************/
/* OCtype */

/*!\enum OCtype
Define the set of legal types. The set is divided
into two parts. The atomic types define
leaf nodes of the DDS. The non-atomic types
are used to tag internal nodes of the DDS tree.
*/
typedef enum OCtype {
/* Atomic Types */
/* OC_Ubyte, OC_Char, OC_Int64 and OC_UInt64 are defined for future extension*/
OC_NAT=0,
OC_Char=1,
OC_Byte=2,
OC_UByte=3,
OC_Int16=4,
OC_UInt16=5,
OC_Int32=6,
OC_UInt32=7,
OC_Int64=8,
OC_UInt64=9,
OC_Float32=10,
OC_Float64=11,
OC_String=12,
OC_URL=13,

/* Non-Atomic Types */
OC_Atomic=100,
OC_Dataset=101,
OC_Sequence=102,
OC_Grid=103,
OC_Structure=104,
OC_Dimension=105,
OC_Attribute=106,
OC_Attributeset=107,
OC_Map=108,
OC_Group=109,
} OCtype;

/*!\enum OCerror
Define the set of error return codes.
The set consists of oc errors (negative
values) plus the set of system errors, which
are positive.
*/
typedef enum OCerror {
OC_NOERR=0,
OC_EBADID=-1,
OC_ECHAR=-2,
OC_EDIMSIZE=-3,
OC_EEDGE=-4,
OC_EINVAL=-5,
OC_EINVALCOORDS=-6,
OC_ENOMEM=-7,
OC_ENOTVAR=-8,
OC_EPERM=-9,
OC_ESTRIDE=-10,
OC_EDAP=-11,
OC_EXDR=-12,
OC_ECURL=-13,
OC_EBADURL=-14,
OC_EBADVAR=-15,
OC_EOPEN=-16,
OC_EIO=-17,
OC_ENODATA=-18,
OC_EDAPSVC=-19,
OC_ENAMEINUSE=-20,
OC_EDAS=-21,
OC_EDDS=-22,
OC_EDATADDS=-23,
OC_ERCFILE=-24,
OC_ENOFILE=-25,
OC_EINDEX=-26,
OC_EBADTYPE=-27,
OC_ESCALAR=-28,
OC_EOVERRUN=-29,
OC_EAUTH=-30,
OC_EACCESS=-31,
} OCerror;

/*!\def OCLOGNOTE
Tag a log entry as a note.
*/
#define OCLOGNOTE 0
/*!\def OCLOGWARN
Tag a log entry as a warning.
*/
#define OCLOGWARN 1
/*!\def OCLOGERR
Tag a log entry as an error.
*/
#define OCLOGERR  2
/*!\def OCLOGDBG
Tag a log entry as a debug note.
*/
#define OCLOGDBG  3

/**************************************************/
/* Define the opaque types */

/*!\typedef OCobject
Define a common opaque type.
*/
typedef void* OCobject;

/*!\typedef OCddsnode
The OCddsnode type provide a reference
to a component of a DAS or DDS tree:
e.g. Sequence, Grid, Dataset, etc.
These objects
are nested, so most objects reference a container object
and subnode objects. The term ddsnode is slightly misleading
since it also covers DAS nodes.
*/

typedef OCobject OCddsnode;

/*!\typedef OCdasnode
The OCdasnode is a alias for OCddsnode.
*/

typedef OCddsnode OCdasnode;

/* Data data type */
/*!\typedef OCdatanode
The OCdatanode type provide a reference
to a specific piece of data in the data
part of a Datadds.
*/
typedef OCobject OCdatanode;

/*!\typedef OClink
Think of OClink as analogous to the C stdio FILE structure;
it "holds" all the other state info about
a connection to the server, the url request, and the DAS/DDS/DATADDSinfo.
3/24/210: Renamed from OCconnection because of confusion about
term "connection"
*/
typedef OCobject OClink;

/**@}*/

/**************************************************/
/* extern API */
/* Declaration modifiers for DLL support (MSC et al) */
#if defined(DLL_NETCDF) /* define when library is a DLL */
#  if defined(DLL_EXPORT) /* define when building the library */
#   define MSC_EXTRA __declspec(dllexport)
#  else
#   define MSC_EXTRA __declspec(dllimport)
#  endif
#  include <io.h>
#else
#define MSC_EXTRA  /**< Needed for DLL build. */
#endif  /* defined(DLL_NETCDF) */

#define EXTERNL MSC_EXTRA extern /**< Needed for DLL build. */
#ifdef __cplusplus
external "C" {
#endif

/**************************************************/
/* Link management */

EXTERNL OCerror oc_open(const char* url, OClink*);
EXTERNL OCerror oc_close(OClink);

/**************************************************/
/* Tree Management */

EXTERNL OCerror oc_fetch(OClink,
			const char* constraint,
			OCdxd,
			OCflags,
			OCddsnode*);

EXTERNL OCerror oc_root_free(OClink, OCddsnode root);
EXTERNL const char* oc_tree_text(OClink, OCddsnode root);

/**************************************************/
/* Node Management */

EXTERNL OCerror oc_dds_properties(OClink, OCddsnode,
		  char** namep,
		  OCtype* typep,
		  OCtype* atomictypep, /* if octype == OC_Atomic */
		  OCddsnode* containerp,  /* NULL if octype == OC_Dataset */
		  size_t* rankp,       /* 0 if scalar */
		  size_t* nsubnodesp,
		  size_t* nattrp);

/* Define some individual accessors for convenience */

EXTERNL OCerror oc_dds_name(OClink,OCddsnode,char**);
EXTERNL OCerror oc_dds_class(OClink,OCddsnode,OCtype*);
EXTERNL OCerror oc_dds_atomictype(OClink,OCddsnode,OCtype*);
EXTERNL OCerror oc_dds_nsubnodes(OClink,OCddsnode,size_t*);
EXTERNL OCerror oc_dds_rank(OClink,OCddsnode,size_t*);
EXTERNL OCerror oc_dds_attr_count(OClink,OCddsnode,size_t*);
EXTERNL OCerror oc_dds_root(OClink,OCddsnode,OCddsnode*);
EXTERNL OCerror oc_dds_container(OClink,OCddsnode,OCddsnode*);

/* Aliases */
#define oc_dds_octype oc_dds_class
#define oc_dds_type oc_dds_class

/* Get the i'th field of the given (container) node; return OC_EINDEX
   if there is no such node; return OC_EBADTYPE if node is not
   a container
*/
EXTERNL OCerror oc_dds_ithfield(OClink, OCddsnode, size_t index, OCddsnode* ithfieldp);

/* Alias for oc_dds_ithfield */
EXTERNL OCerror oc_dds_ithsubnode(OClink, OCddsnode, size_t, OCddsnode*);

/* Convenience functions that are just combinations of ithfield with other functions */

/* Return the grid array dds node from the specified grid node*/
EXTERNL OCerror oc_dds_gridarray(OClink, OCddsnode grid, OCddsnode* arrayp);

/* Return the i'th grid map dds node from the specified grid dds node.
   NOTE: Map indices start at ZERO.
*/
EXTERNL OCerror oc_dds_gridmap(OClink, OCddsnode grid, size_t index, OCddsnode* mapp);

/* Retrieve a dds node by name from a dds structure or dataset node.
   return OC_EBADTYPE if node is not a container,
   return OC_EINDEX if no field by the given name is found.
*/
EXTERNL OCerror oc_dds_fieldbyname(OClink, OCddsnode, const char* name, OCddsnode* fieldp);


/* Return the dimension nodes, if any, associated with a given DDS node */
/* Caller must allocate and free the vector for dimids */
/* If the node is scalar, then return OC_ESCALAR. */
EXTERNL OCerror oc_dds_dimensions(OClink, OCddsnode, OCddsnode* dimids);

/* Return the i'th dimension node, if any, associated with a given object */
/* If there is no such dimension, then return OC_EINVAL */
EXTERNL OCerror oc_dds_ithdimension(OClink,OCddsnode, size_t, OCddsnode*);

/* Return the size and name associated with a given dimension object
   as defined in the DDS
*/
EXTERNL OCerror oc_dimension_properties(OClink,OCddsnode,size_t*,char**);

/* For convenience, return only the dimension sizes */
EXTERNL OCerror oc_dds_dimensionsizes(OClink,OCddsnode,size_t* dimsizes);

/* Attribute Management */

/* Obtain the attributes associated with a given DDS OCddsnode.
   One specifies the DDS root to get the global attributes
   The actual attribute strings are returned and the user
   must do any required conversion based on the octype.
   The strings vector must be allocated and freed by caller,
   The contents of the strings vector must also be reclaimed
   using oc_attr_reclaim(see below).
   Standard practice is to call twice, once with the strings
   argument == NULL so we get the number of values,
   then the second time with an allocated char** vector.

*/
EXTERNL OCerror oc_dds_attr(OClink,OCddsnode, size_t i,
			    char** name, OCtype* octype,
			    size_t* nvalues, char** strings);


/* Access ith value string of a DAS OC_Attribute object.
   OCddsnode of the object is assumed to be OC_Attribute.
   Note that this is  different than the above oc_dds_attr
   that works on DDS nodes.
   Note also that the return value is always a string.
   Caller must free returned string.
*/

EXTERNL OCerror oc_das_attr_count(OClink, OCddsnode, size_t* countp);

EXTERNL OCerror oc_das_attr(OClink,OCddsnode, size_t, OCtype*, char**);

/**************************************************/
/* Free up a ddsnode that is no longer being used */
EXTERNL OCerror oc_dds_free(OClink, OCddsnode);

/**************************************************/
/* Data Procedures */

/* Given the DDS tree root, get the root data of datadds */
EXTERNL OCerror oc_dds_getdataroot(OClink, OCddsnode treeroot, OCdatanode* rootp);

/* Alias for oc_dds_getdataroot */
#define oc_data_getroot oc_dds_getdataroot

/* Return the data of the container for the specified data.
   If it does not exist, then return NULL.
   In effect this procedure allows one to walk up the datatree one level.
*/
EXTERNL OCerror oc_data_container(OClink, OCdatanode data, OCdatanode* containerp);

/* Return the root node of the data tree that contains the specified data node.
   In effect this procedure allows one to walk to the root of the datatree
   containing the specified datanode.
*/
EXTERNL OCerror oc_data_root(OClink, OCdatanode data, OCdatanode* rootp);

/*
There are multiple ways to walk down a level in a data tree
depending on what kind of node we are currently visiting.
The possibilities are:
1. current node is the data for a structure or dataset node:
   => oc_data_ithfield -- get the data node corresponding
                          to the ith field of the structure
2. current node is the data for a grid node:
   => oc_data_gridarray -- get the data node for the grid array
   => oc_data_gridmap -- get the data node for the ith grid map
3. current node is the data for an array of structures
   => oc_data_ithelement -- get the data node corresponding to
                            the i'th structure in the array
                            If the structure is scalar, then the indices
                            are ignored.
4. current node is the data for a sequence
   => oc_data_ithrecord -- get the data node corresponding to
                            the i'th record in the sequence

Note that the above only apply to compound objects. Once
you have the data node for an atomic types object (dimensioned
or scalar), you can read its contents using oc_data_read
or oc_data_readscalar.
*/

/* Return the data node for the i'th field of the specified container data */
EXTERNL OCerror oc_data_ithfield(OClink, OCdatanode container, size_t index,
                                OCdatanode* fieldp);

/* Retrieve the data node by name from a container data node */
EXTERNL OCerror oc_dat_fieldbyname(OClink, OCdatanode, const char* name, OCdatanode* fieldp);

/* Return the grid array data for the specified grid data */
EXTERNL OCerror oc_data_gridarray(OClink, OCdatanode grid, OCdatanode* arrayp);

/* Return the i'th grid map data for the specified grid data.
   NOTE: Map indices start at ZERO.
*/
EXTERNL OCerror oc_data_gridmap(OClink, OCdatanode grid, size_t index, OCdatanode* mapp);

/* Return the data of a dimensioned Structure corresponding
   to the element specified by the indices.
*/
EXTERNL OCerror oc_data_ithelement(OClink, OCdatanode data, size_t* indices, OCdatanode* elementp);

/* Return the i'th record data of a Sequence data. */
EXTERNL OCerror oc_data_ithrecord(OClink, OCdatanode data, size_t index, OCdatanode* recordp);

/* Free up an data that is no longer being used */
/* Currently does nothing */
#define oc_data_free(link,datanode)

/* Count the records associated with a sequence */
EXTERNL OCerror oc_data_recordcount(OClink, OCdatanode, size_t*);

/* Return the actual data values associated with the specified leaf data.
   The OCdatanode is assumed to be referencing a leaf node that is
   either a atomic valued scalar or array.
   If scalar, then index and count are ignored.
   Caller is responsible for allocating memory(of proper size)
   and free'ing it.
   See also oc_dds_read().
*/
EXTERNL OCerror oc_data_read(OClink, OCdatanode, size_t*, size_t*, size_t, void*);

/* Like oc_data_read, but for reading a scalar.
   Caller is responsible for allocating memory(of proper size)
   and free'ing it.
   See also oc_dds_readscalar().
*/
EXTERNL OCerror oc_data_readscalar(OClink, OCdatanode, size_t, void*);

/* Like oc_data_read, but caller provides a starting set of indices
   and count of the number of elements to read.
   Caller is responsible for allocating memory(of proper size)
   and free'ing it.
   See also oc_dds_readn().
*/
EXTERNL OCerror oc_data_readn(OClink, OCdatanode, const size_t*, size_t, size_t, void*);

/* Return the indices for this data; Assumes the data
   was obtained using oc_data_ithelement or oc_data_ithrecord;
   if not, then an error is returned.
*/
EXTERNL OCerror oc_data_position(OClink, OCdatanode data, size_t* indices);

/* Return the pattern dds node for an data */
EXTERNL OCerror oc_data_ddsnode(OClink, OCdatanode data, OCddsnode*);

/* Return the octype of the data (convenience) */
EXTERNL OCerror oc_data_octype(OClink, OCdatanode data, OCtype*);

/* Return 1 if the specified data has a valid index, 0 otherwise.
   Valid index means it was created using
   oc_data_ithelement or oc_data_ithrecord.
*/
EXTERNL int oc_data_indexed(OClink link, OCdatanode datanode);

/* Return 1 if the specified data has a valid index, 0 otherwise.
   Valid index means it was created using
   oc_data_ithelement or oc_data_ithrecord.
*/
EXTERNL int oc_data_indexed(OClink, OCdatanode);

/* Return 1 if the specified data can be indexed
   Indexable means the data is pointing to
   an indexed structure or to a sequence.
*/
EXTERNL int oc_data_indexable(OClink, OCdatanode);

/**************************************************/
/*
For top-level, atomic variables, it is possible to directly
read the associated data without having to use the oc_data_XXX
procedures. Provide special procedures to support this.
*/

/* See oc_data_read for semantics */
EXTERNL OCerror oc_dds_read(OClink, OCddsnode, size_t*, size_t*, size_t, void*);

/* See oc_data_readscalar for semantics */
EXTERNL OCerror oc_dds_readscalar(OClink, OCddsnode, size_t, void*);

/* See oc_data_readn for semantics */
EXTERNL OCerror oc_dds_readn(OClink, OCddsnode, size_t*, size_t, size_t, void*);

/**************************************************/
/* Misc. OCtype-related functions */

/* Return size of the given type(Atomic only) */
EXTERNL size_t oc_typesize(OCtype);

/* Return a canonical printable string describing a given type:
   e.g. Byte, Int16, etc.
*/
EXTERNL const char* oc_typetostring(OCtype);

/* Given a value of a atomic OC type, provide a canonical
   string representing that value; mostly for debugging.
*/
EXTERNL OCerror oc_typeprint(OCtype, void* value, size_t bufsize, char* buf);

/**************************************************/
/* Logging */

EXTERNL void oc_loginit(void);
EXTERNL int  oc_setlogging(int onoff); /* 1=>start logging 0=>stop */
EXTERNL int  oc_logopen(const char* logfilename);
EXTERNL void oc_logclose(void);

EXTERNL void oc_log(int tag, const char* fmt, ...);

EXTERNL void oc_logtext(int tag, const char* text);

/**************************************************/
/* Miscellaneous */

/* Reclaim the strings within a string vector, but not the vector itself.
   This is useful for reclaiming the result of oc_data_read
   or oc_dds_attr when the type is OC_String or OC_URL.
   Note that only the strings are reclaimed, the string vector
   is not reclaimed because it was presumably allocated by the client.
*/
EXTERNL void oc_reclaim_strings(size_t n, char** svec);

/* Convert an OCerror to a human readable string */
EXTERNL const char* oc_errstring(OCerror err);

/* Get client parameters from the URL
   DO NOT free the result
*/
EXTERNL const char* oc_clientparam_get(OClink, const char* param);

/**************************************************/
/* Merging operations */

/* Merge a specified DAS into a specified DDS or DATADDS */
EXTERNL OCerror oc_merge_das(OClink, OCddsnode dasroot, OCddsnode ddsroot);

/**************************************************/
/* Debugging */

/* When a server error is detected, then it is possible
   to get DODS supplied server error info using this procedure */
EXTERNL OCerror oc_svcerrordata(OClink link, char** codep,
                               char** msgp, long* httpp);

/* Get the HTTP return code from the last call;
   note that this may or may not be the same as returned
   by oc_svcerrordata.
 */
EXTERNL int oc_httpcode(OClink);

/*
(Re-)initialize the oc library as if nothing had been called.
This is primarily for debugging of rc files.
*/
EXTERNL OCerror oc_initialize(void);

/**************************************************/
/* Curl options */
/* This is here because trial and error shows that
   libcurl shows thru too much. So bow to the inevitable.
*/

/*Cause the curl library to be verbose and save error messages*/
EXTERNL OCerror oc_trace_curl(OClink link);

/* Allow specification of the rc file */
EXTERNL OCerror oc_set_rcfile(const char* filepath);

/* Allow specification of the netrc file */
EXTERNL OCerror oc_set_netrc(OClink*, const char* filepath);

/* Set arbitrary curl option */
EXTERNL OCerror oc_set_curlopt(OClink link, const char* option, void* value);

/**************************************************/
/* Experimental/Undocumented */

/* Given an arbitrary OCnode, return the connection of which it is a part */
EXTERNL OCerror oc_get_connection(OCobject ocnode, OCobject* linkp);

/* Resend a url as a head request to check the Last-Modified time */
EXTERNL OCerror oc_update_lastmodified_data(OClink,OCflags);

/* Get last known modification time; -1 => data unknown */
EXTERNL long oc_get_lastmodified_data(OClink);

/* Test if a given url responds to a DAP protocol request */
EXTERNL OCerror oc_ping(const char* url);

/* Return the size of the in-memory or on-disk
   data chunk returned by the server for a given tree.
   Zero implies it is not defined.
*/
EXTERNL OCerror oc_raw_xdrsize(OClink,OCddsnode,off_t*);

EXTERNL int ocdebug;

#ifdef __cplusplus
}
#endif

#endif /*OC_H*/
