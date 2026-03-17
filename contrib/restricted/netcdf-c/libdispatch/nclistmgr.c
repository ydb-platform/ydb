/*********************************************************************
   Copyright 2018, UCAR/Unidata See netcdf/COPYRIGHT file for
   copying and redistribution conditions.
*********************************************************************/
/**
 * @file
 *
 * Functions to manage the list of NC structs. There is one NC struct
 * for each open file.
 *
 * @author Dennis Heimbigner
*/

#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "ncdispatch.h"

/** This shift is applied to the ext_ncid in order to get the index in
 * the array of NC. */
#define ID_SHIFT (16)

/** This is the length of the NC list - the number of files that can
 * be open at one time. We use 2^16 = 65536 entries in the array, but
 * slot 0 is not used, so only 65535 files may be open at one
 * time. */
#define NCFILELISTLENGTH 0x10000

/** This is the pointer to the array of NC, one for each open file. */
static NC** nc_filelist = NULL;

/** The number of files currently open. */
static int numfiles = 0;

/**
 * How many files are currently open?
 *
 * @return number of open files.
 * @author Dennis Heimbigner
 */
int
count_NCList(void)
{
    return numfiles;
}

/**
 * Free an empty NCList. @note If list is not empty, or has not been
 * allocated, function will silently exit.
 *
 * @author Dennis Heimbigner
 */
void
free_NCList(void)
{
    if(numfiles > 0) return; /* not empty */
    if(nc_filelist != NULL) free(nc_filelist);
    nc_filelist = NULL;
}

/**
 * Add an already-allocated NC to the list. It will be assigned an
 * ncid in this function.
 *
 * If this is the first file to be opened, the nc_filelist will be
 * allocated and set to all 0.
 *
 * The ncid is assigned by finding the first open index in the
 * nc_filelist array (skipping index 0). The ncid is this index
 * left-shifted ID_SHIFT bits (16). This puts the file ID in the first
 * two bytes of the 4-byte integer, and leaves the last two bytes for
 * group IDs for netCDF-4 files.
 *
 * @param ncp Pointer to already-allocated and initialized NC struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Dennis Heimbigner
 */
int
add_to_NCList(NC* ncp)
{
    unsigned int i;
    unsigned int new_id;
    if(nc_filelist == NULL) {
        if (!(nc_filelist = calloc(1, sizeof(NC*)*NCFILELISTLENGTH)))
            return NC_ENOMEM;
        numfiles = 0;
    }

    new_id = 0; /* id's begin at 1 */
    for(i=1; i < NCFILELISTLENGTH; i++) {
        if(nc_filelist[i] == NULL) {new_id = i; break;}
    }
    if(new_id == 0) return NC_ENOMEM; /* no more slots */
    nc_filelist[new_id] = ncp;
    numfiles++;
    ncp->ext_ncid = (int)(new_id << ID_SHIFT);
    return NC_NOERR;
}

/**
 * Move an NC in the nc_filelist. This is required by PIO.
 *
 * @param ncp Pointer to already-allocated and initialized NC struct.
 * @param new_id New index in the nc_filelist for this file.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid input.
 * @author Ed Hartnett
 */
int
move_in_NCList(NC *ncp, int new_id)
{
    /* If no files in list, error. */
    if (!nc_filelist)
        return NC_EINVAL;

    /* If new slot is already taken, error. */
    if (nc_filelist[new_id])
        return NC_EINVAL;

    /* Move the file. */
    nc_filelist[ncp->ext_ncid >> ID_SHIFT] = NULL;
    nc_filelist[new_id] = ncp;
    ncp->ext_ncid = (new_id << ID_SHIFT);

    return NC_NOERR;
}

/**
 * Delete an NC struct from the list. This happens when the file is
 * closed. Relies on all memory in the NC being deallocated after this
 * function with freeNC().
 *
 * @note If the file list is empty, or this NC can't be found in the
 * list, this function will silently exit.
 *
 * @param ncp Pointer to NC to be removed from list.
 *
 * @author Dennis Heimbigner
 */
void
del_from_NCList(NC* ncp)
{
    unsigned int ncid = ((unsigned int)ncp->ext_ncid) >> ID_SHIFT;
    if(numfiles == 0 || ncid == 0 || nc_filelist == NULL) return;
    if(nc_filelist[ncid] != ncp) return;

    nc_filelist[ncid] = NULL;
    numfiles--;

    /* If all files have been closed, release the filelist memory. */
    if (numfiles == 0)
        free_NCList();
}

/**
 * Find an NC in the list, given an ext_ncid. The NC list is indexed
 * with the first two bytes of ext_ncid. (The last two bytes specify
 * the group for netCDF4 files, or are zeros for classic files.)
 *
 * @param ext_ncid The ncid of the file to find.
 *
 * @return pointer to NC or NULL if not found.
 * @author Dennis Heimbigner, Ed Hartnett
 */
NC *
find_in_NCList(int ext_ncid)
{
    NC* f = NULL;

    /* Discard the first two bytes of ext_ncid to get ncid. */
    unsigned int ncid = ((unsigned int)ext_ncid) >> ID_SHIFT;

    /* If we have a filelist, there will be an entry, possibly NULL,
     * for this ncid. */
    if (nc_filelist)
    {
        assert(numfiles);
        f = nc_filelist[ncid];
    }

    /* For classic files, ext_ncid must be a multiple of
     * (1<<ID_SHIFT). That is, the group part of the ext_ncid (the
     * last two bytes) must be zero. If not, then return NULL, which
     * will eventually lead to an NC_EBADID error being returned to
     * user. */
    if (f != NULL && f->dispatch != NULL
	&& f->dispatch->model == NC_FORMATX_NC3 && (ext_ncid % (1<<ID_SHIFT)))
        return NULL;

    return f;
}

/**
 * Find an NC in the list using the file name.
 *
 * @param path Name of the file.
 *
 * @return pointer to NC or NULL if not found.
 * @author Dennis Heimbigner
 */
NC*
find_in_NCList_by_name(const char* path)
{
    int i;
    NC* f = NULL;
    if(nc_filelist == NULL)
        return NULL;
    for(i=1; i < NCFILELISTLENGTH; i++) {
        if(nc_filelist[i] != NULL) {
            if(strcmp(nc_filelist[i]->path,path)==0) {
                f = nc_filelist[i];
                break;
            }
        }
    }
    return f;
}

/**
 * Find an NC in list based on its index. The index is ((unsigned
 * int)ext_ncid) >> ID_SHIFT. This is the two high bytes of the
 * ext_ncid. (The other two bytes are used for the group ID for
 * netCDF-4 files.)
 *
 * @param index The index in the NC list.
 * @param ncp Pointer that gets pointer to the next NC. Ignored if
 * NULL.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ERANGE Index out of range.
 * @author Dennis Heimbigner
 */
int
iterate_NCList(int index, NC** ncp)
{
    /* Walk from 0 ...; 0 return => stop */
    if(index < 0 || index >= NCFILELISTLENGTH)
        return NC_ERANGE;
    if(ncp) *ncp = nc_filelist[index];
    return NC_NOERR;
}
