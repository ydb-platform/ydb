/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */
#ifndef _NC_H_
#define _NC_H_

#include "config.h"
#include "netcdf.h"

   /* There's an external ncid (ext_ncid) and an internal ncid
    * (int_ncid). The ext_ncid is the ncid returned to the user. If
    * the user has opened or created a netcdf-4 file, then the
    * ext_ncid is the same as the int_ncid. If he has opened or
    * created a netcdf-3 file ext_ncid (which the user sees) is
    * different from the int_ncid, which is the ncid returned by the
    * netcdf-3 layer, which insists on inventing its own ncids,
    * regardless of what is already in use due to previously opened
    * netcdf-4 files. The ext_ncid contains the ncid for the root
    * group (i.e. group zero). */

/* Common Shared Structure for all Dispatched Objects */
typedef struct NC {
	int ext_ncid;
	int int_ncid;
	const struct NC_Dispatch* dispatch;
	void* dispatchdata; /*per-'file' data; points to e.g. NC3_INFO data*/
	char* path;
	int   mode; /* as provided to nc_open/nc_create */
} NC;

/*
 * Counted string for names and such
 */
typedef struct {
	/* all xdr'd */
	size_t nchars;
	char *cp;
} NC_string;

/* Define functions that are used across multiple dispatchers */

/* Begin defined in string.c */
extern void
free_NC_string(NC_string *ncstrp);

extern int
NC_check_name(const char *name);

extern NC_string *
new_NC_string(size_t slen, const char *str);
extern int
set_NC_string(NC_string *ncstrp, const char *str);

/* End defined in string.c */

extern int
NC_check_id(int ncid, NC **ncpp);

/* Create a pseudo file descriptor that does not
   overlap real file descriptors */
extern int nc__pseudofd(void);

/* This function gets a current default create flag */
extern int nc_get_default_format(void);

extern int add_to_NCList(NC*);
extern void del_from_NCList(NC*);/* does not free object */
extern NC* find_in_NCList(int ext_ncid);
extern NC* find_in_NCList_by_name(const char*);
extern int move_in_NCList(NC *ncp, int new_id);
extern void free_NCList(void);/* reclaim whole list */
extern int count_NCList(void); /* return # of entries in NClist */
extern int iterate_NCList(int i,NC**); /* Walk from 0 ...; ERANGE return => stop */

/* Defined in nc.c */
extern void free_NC(NC*);
extern int new_NC(const struct NC_Dispatch*, const char*, int, NC**);

/* Defined in dinstance_intern.c */

/**************************************************/
/**
Following are the internal implementation signatures upon which
are built the API functions: nc_reclaim_data, nc_reclaim_data_all,
nc_copy_data, and nc_copy_data_all.
These functions access internal data structures instead of using e.g. ncid.
This is to improve performance.
*/

/*
Reclaim a vector of instances of arbitrary type.
This recursively walks the top-level instances to reclaim any
nested data such as vlen or strings or such.

Assumes it is passed a pointer to count instances of xtype.
Reclaims any nested data.

WARNING: nc_reclaim_data does not reclaim the top-level
memory because we do not know how it was allocated.  However
nc_reclaim_data_all does reclaim top-level memory assuming that it
was allocated using malloc().

WARNING: all data blocks below the top-level (e.g. string
instances) will be reclaimed, so do not call if there is any
static data in the instance.

Should work for any netcdf format.
*/

EXTERNL int NC_reclaim_data(NC* nc, nc_type xtypeid, void* memory, size_t count);
EXTERNL int NC_reclaim_data_all(NC* nc, nc_type xtypeid, void* memory, size_t count);

/**
Copy vector of arbitrary type instances.  This recursively walks
the top-level instances to copy any nested data such as vlen or
strings or such.

Assumes it is passed a pointer to count instances of xtype.
WARNING: nc_copy_data does not copy the top-level memory, but
assumes a block of proper size was passed in.  However
nc_copy_data_all does allocate top-level memory copy.

Should work for any netcdf format.
*/

EXTERNL int NC_copy_data(NC* nc, nc_type xtypeid, const void* memory, size_t count, void* copy);
EXTERNL int NC_copy_data_all(NC* nc, nc_type xtypeid, const void* memory, size_t count, void** copyp);

/* Macros to map NC_FORMAT_XX to metadata structure (e.g. NC_FILE_INFO_T) */
/* Fast test for what file structure is used */
#define NC3INFOFLAGS ((1<<NC_FORMATX_NC3)|(1<<NC_FORMATX_PNETCDF)|(1<<NC_FORMATX_DAP2))
#define FILEINFOFLAGS ((1<<NC_FORMATX_NC_HDF5)|(1<<NC_FORMATX_NC_HDF4)|(1<<NC_FORMATX_DAP4)|(1<<NC_FORMATX_UDF1)|(1<<NC_FORMATX_UDF0)|(1<<NC_FORMATX_NCZARR))

#define USENC3INFO(nc) ((1<<(nc->dispatch->model)) & NC3INFOFLAGS)
#define USEFILEINFO(nc) ((1<<(nc->dispatch->model)) & FILEINFOFLAGS)
#define USED2INFO(nc) ((1<<(nc->dispatch->model)) & (1<<NC_FORMATX_DAP2))
#define USED4INFO(nc) ((1<<(nc->dispatch->model)) & (1<<NC_FORMATX_DAP4))

/* In DAP4 and Zarr (and maybe other places in the future)
   we may have dimensions with a size, but no name.
   In this case we need to create a name based on the size.
   As a rule, the dimension name is NCDIMANON_<n> where n is the size
   and NCDIMANON is a prefix defined here.
*/
#define NCDIMANON "_Anonymous_Dim"   

#endif /* _NC_H_ */
