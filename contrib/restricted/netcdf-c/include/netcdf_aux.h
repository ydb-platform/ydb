/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *   $Id$
 *   $Header$
 *********************************************************************/

/*
 * In order to use any of the netcdf_XXX.h files, it is necessary
 * to include netcdf.h followed by any netcdf_XXX.h files.
 * Various things (like EXTERNL) are defined in netcdf.h
 * to make them available for use by the netcdf_XXX.h files.
*/

#ifndef NCAUX_H
#define NCAUX_H

#define NCAUX_ALIGN_C 0
#define NCAUX_ALIGN_UNIFORM 1

#if defined(__cplusplus)
extern "C" {
#endif


/**
Reclaim a vector of instances of arbitrary type.
Intended for use with e.g. nc_get_vara or the input to e.g. nc_put_vara.
This recursively walks the top-level instances to
reclaim any nested data such as vlen or strings or such.

Assumes it is passed a pointer to count instances of xtype.
Reclaims any nested data.
WARNING: ncaux_reclaim_data does not reclaim the top-level memory
because we do not know how it was allocated.
However ncaux_reclaim_data_all does reclaim top-level memory.

WARNING: all data blocks below the top-level (e.g. string instances)
will be reclaimed, so do not call if there is any static data in the instance.

Should work for any netcdf format.

WARNING: deprecated in favor the corresponding function in netcdf.h.
These are just wrappers for nc_reclaim_data and
nc_reclaim_data_all and nc_copy_data and nc_copy_data_all and
are here for back compatibility.
*/

EXTERNL int ncaux_reclaim_data(int ncid, int xtype, void* memory, size_t count);
EXTERNL int ncaux_reclaim_data_all(int ncid, int xtype, void* memory, size_t count);
EXTERNL int ncaux_copy_data(int ncid, int xtype, void* memory, size_t count, void* copy);
EXTERNL int ncaux_copy_data_all(int ncid, int xtype, void* memory, size_t count, void** copyp);

EXTERNL int ncaux_dump_data(int ncid, nc_type xtypeid, void* memory, size_t count, char** buf);


EXTERNL int ncaux_inq_any_type(int ncid, nc_type typeid, char *name, size_t *size, nc_type *basetypep, size_t *nfieldsp, int *classp);

/**************************************************/
/* Capture the id and parameters for a filter
   using the HDF5 unsigned int format
*/
typedef struct NC_H5_Filterspec {
    unsigned int filterid; /**< ID for arbitrary filter. */
    size_t nparams;        /**< nparams for arbitrary filter. */
    unsigned int* params;  /**< Params for arbitrary filter. */
} NC_H5_Filterspec;

EXTERNL int ncaux_h5filterspec_parse(const char* txt, unsigned int* idp, size_t* nparamsp, unsigned int** paramsp);
EXTERNL int ncaux_h5filterspec_parselist(const char* txt0, int* formatp, size_t* nspecsp, struct NC_H5_Filterspec*** vectorp);
EXTERNL int ncaux_h5filterspec_parse_parameter(const char* txt, size_t* nuiparamsp, unsigned int* uiparams);
EXTERNL void ncaux_h5filterspec_free(struct NC_H5_Filterspec* f);
EXTERNL void ncaux_h5filterspec_fix8(unsigned char* mem, int decode);
	    
/**************************************************/
/* Wrappers to export selected functions from libnetcdf */

EXTERNL int ncaux_readfile(const char* filename, size_t* sizep, void** content);
EXTERNL int ncaux_writefile(const char* filename, size_t size, void* content);

/**************************************************/

/* Takes any type */
EXTERNL int ncaux_type_alignment(int xtype, int ncid, size_t* alignp);
EXTERNL int ncaux_class_alignment(int ncclass, size_t* alignp);

/**************************************************/
/* Takes type classes only */

/* Build compound types and properly handle offset and alignment */

EXTERNL int ncaux_class_alignment(int ncclass, size_t* alignp);
EXTERNL int ncaux_begin_compound(int ncid, const char *name, int alignmode, void** tag);
EXTERNL int ncaux_end_compound(void* tag, nc_type* xtypeid);
EXTERNL int ncaux_abort_compound(void* tag);
EXTERNL int ncaux_add_field(void* tag,  const char *name, nc_type field_type,
			   int ndims, const int* dimsizes);

/**************************************************/
/* Path-list Utilities */

/* Opaque */
struct NCPluginList;

/**
Parse a counted string into a sequence of path directories.

The pathlist argument has the following syntax:
    paths := <empty> | dirlist
    dirlist := dir | dirlist separator dir
    separator := ';' | ':'
    dir := <OS specific directory path>

@param pathlen length of pathlist arg
@param pathlist a string encoding a list of directories
@param sep  one of ';' | ':' | '\0' where '\0' means use the platform's default separator.
@param dirs a pointer to an  NCPluginPath object for returning the number and vector of directories from the parse.
@return ::NC_NOERR | NC_EXXX

Note: If dirs->dirs is not NULL, then this function
will allocate the space for the vector of directory path.
The user is then responsible for free'ing that vector
(or call ncaux_plugin_path_reclaim).

Author: Dennis Heimbigner
*/
EXTERNL int ncaux_plugin_path_parsen(size_t pathlen, const char* pathlist, char sep, struct NCPluginList* dirs);

/**
Parse a nul-terminated string into a sequence of path directories.
@param pathlist a string encoding a list of directories
@param sep  one of ';' | ':' | '\0' where '\0' means use the platform's default separator.
@param dirs a pointer to an  NCPluginPath object for returning the number and vector of directories from the parse.
@return ::NC_NOERR | NC_EXXX
See also the comments for ncaux_plugin_path_parsen
Author: Dennis Heimbigner
*/
EXTERNL int ncaux_plugin_path_parse(const char* pathlist, char sep, struct NCPluginList* dirs);

/**
Concatenate a vector of directories with the separator between.
This is more-or-less the inverse of the ncaux_plugin_path_parse function

The resulting string has following syntax:
    paths := <empty> | dirlist
    dirlist := dir | dirlist separator dir
    separator := ';' | ':'
    dir := <OS specific directory path>
    
@param dirs a pointer to an  NCPluginList object giving the number and vector of directories to concatenate.
@param sep one of ';', ':', or '\0'
@param catlen length of the cat arg including a nul terminator
@param cat user provided space for holding the concatenation; nul termination guaranteed if catlen > 0.
@return ::NC_NOERR
@return ::NC_EINVAL for illegal arguments

Note: If dirs->dirs is not NULL, then this function
will allocate the space for the vector of directory path.
The user is then responsible for free'ing that vector
(or call ncaux_plugin_path_reclaim).

Author: Dennis Heimbigner
*/
EXTERNL int ncaux_plugin_path_tostring(const struct NCPluginList* dirs, char sep, char** catp);

/*
Clear the contents of a NCPluginList object.
@param dirs a pointer to an NCPluginList object giving the number and vector of directories to reclaim
@return ::NC_NOERR
@return ::NC_EINVAL for illegal arguments

Author: Dennis Heimbigner
*/
EXTERNL int ncaux_plugin_path_clear(struct NCPluginList* dirs);

/*
Reclaim a NCPluginList object possibly produced by ncaux_plugin_parse function.
WARNING: do not call with a static or stack allocated object.
@param dirs a pointer to an  NCPluginList object giving the number and vector of directories to reclaim
@return ::NC_NOERR
@return ::NC_EINVAL for illegal arguments

Author: Dennis Heimbigner
*/
EXTERNL int ncaux_plugin_path_reclaim(struct NCPluginList* dirs);

/*
Modify a plugin path set to append a new directory to the end.
@param dirs a pointer to an  NCPluginList object giving the number and vector of directories to which 'dir' argument is appended.
@return ::NC_NOERR
@return ::NC_EINVAL for illegal arguments

Author: Dennis Heimbigner
*/
EXTERNL int ncaux_plugin_path_append(struct NCPluginList* dirs, const char* dir);

/*
Modify a plugin path set to prepend a new directory to the front.
@param dirs a pointer to an  NCPluginList object giving the number and vector of directories to which 'dir' argument is appended.
@return ::NC_NOERR
@return ::NC_EINVAL for illegal arguments

Author: Dennis Heimbigner
*/
EXTERNL int ncaux_plugin_path_prepend(struct NCPluginList* dirs, const char* dir);

/**************************************************/
/* FORTRAN is not good at manipulating C char** vectors,
   so provide some wrappers for use by netcdf-fortran
   that read/write plugin path as a single string.
   For simplicity, the path separator is always semi-colon.
*/

/**
 * Return the length (as in strlen) of the current plugin path directories encoded as a string.
 * @return length of the string encoded plugin path.
 * @author Dennis Heimbigner
 *
 * @author: Dennis Heimbigner
*/
EXTERNL int ncaux_plugin_path_stringlen(void);

/**
 * Return the current sequence of directories in the internal global
 * plugin path list encoded as a string path using ';' as a path separator.
 * @param pathlen the length of the path argument.
 * @param path a string into which the current plugin paths are encodeded.
 * @return NC_NOERR | NC_EXXX
 * @author Dennis Heimbigner
 *
 * @author: Dennis Heimbigner
*/
EXTERNL int ncaux_plugin_path_stringget(int pathlen, char* path);

/**
 * Set the current sequence of directories in the internal global
 * plugin path list to the sequence of directories encoded as a
 * string path using ';' as a path separator.
 * @param pathlen the length of the path argument.
 * @param path a string encoding the sequence of directories and using ';' to separate them.
 * @return NC_NOERR | NC_EXXX
 * @author Dennis Heimbigner
 *
 * @author: Dennis Heimbigner
*/
EXTERNL int ncaux_plugin_path_stringset(int pathlen, const char* path);

/* Provide a parser for _NCProperties attribute.
 * @param ncprop the contents of the _NCProperties attribute.
 * @param pairsp allocate and return a pointer to a NULL terminated vector of (key,value) pairs.
 * @return NC_NOERR | NC_EXXX
 */
EXTERNL int ncaux_parse_provenance(const char* ncprop, char*** pairsp);

#if defined(__cplusplus)
}
#endif

#endif /*NCAUX_H*/
