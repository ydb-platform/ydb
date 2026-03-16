/*! \file
Functions for netCDF-4 features.

Copyright 2018 University Corporation for Atmospheric
Research/Unidata. See \ref copyright file for more info. */

#include "ncdispatch.h"

/** \defgroup groups Groups

NetCDF-4 added support for hierarchical groups within netCDF datasets.

Groups are identified with a ncid, which identifies both the open
file, and the group within that file. When a file is opened with
nc_open or nc_create, the ncid for the root group of that file is
provided. Using that as a starting point, users can add new groups, or
list and navigate existing groups or rename a group.

All netCDF calls take a ncid which determines where the call will take
its action. For example, the nc_def_var function takes a ncid as its
first parameter. It will create a variable in whichever group its ncid
refers to. Use the root ncid provided by nc_create or nc_open to
create a variable in the root group. Or use nc_def_grp to create a
group and use its ncid to define a variable in the new group.

Variable are only visible in the group in which they are defined. The
same applies to attributes. “Global” attributes are associated with
the group whose ncid is used.

Dimensions are visible in their groups, and all child groups.

Group operations are only permitted on netCDF-4 files - that is, files
created with the HDF5 flag in nc_create(). Groups are not compatible
with the netCDF classic data model, so files created with the
::NC_CLASSIC_MODEL file cannot contain groups (except the root group).

Encoding both the open file id and group id in a single integer
currently limits the number of groups per netCDF-4 file to no more
than 32767.  Similarly, the number of simultaneously open netCDF-4
files in one program context is limited to 32767.

 */

/** \{*/ /* All these functions are part of the above defgroup... */

/*! Return the group ID for a group given the name.


  @param[in] ncid      A valid file or group ncid.
  @param[in] name      The name of the group you are querying.
  @param[out] grp_ncid Pointer to memory to hold the group ncid.

  @returns Error code or ::NC_NOERR or no error.

 */
int nc_inq_ncid(int ncid, const char *name, int *grp_ncid)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->inq_ncid(ncid,name,grp_ncid);
}

/*! Get a list of groups or subgroups from a file or groupID.

  @param[in]  ncid    The ncid of the file or parent group.
  @param[out] numgrps Pointer to memory to hold the number of groups.
  @param[out] ncids   Pointer to memory to hold the ncid for each group.

  @returns Error code or ::NC_NOERR for no error.

 */
int nc_inq_grps(int ncid, int *numgrps, int *ncids)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->inq_grps(ncid,numgrps,ncids);
}

/*! Get the name of a group given an ID.

  @param[in]  ncid The ncid of the file or parent group.
  @param[out] name The name of the group associated with the id.

  @returns Error code or ::NC_NOERR for no error.
*/
int nc_inq_grpname(int ncid, char *name)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->inq_grpname(ncid,name);
}

/*! Get the full path/groupname of a group/subgroup given an ID.

  @param[in]  ncid      The ncid of the file or parent group.
  @param[out] lenp      Pointer to memory to hold the length of the full name.
  @param[out] full_name Pointer to memory to hold the full name of the group including root/parent.

  @returns Error code or ::NC_NOERR for no error.

*/

int nc_inq_grpname_full(int ncid, size_t *lenp, char *full_name)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->inq_grpname_full(ncid,lenp,full_name);
}

/*! Get the length of a group name given an ID.

  @param[in] ncid  The ncid of the group in question.
  @param[out] lenp Pointer to memory to hold the length of the name of the group in question.

  @returns Error code or ::NC_NOERR for no error.

*/
int nc_inq_grpname_len(int ncid, size_t *lenp)
{
    int stat = nc_inq_grpname_full(ncid,lenp,NULL);
    return stat;
}

/*! Get the ID of the parent based on a group ID.

  @param[in] ncid         The ncid of the group in question.
  @param[out] parent_ncid Pointer to memory to hold the identifier of the parent of the group in question.

  @returns Error code or ::NC_NOERR for no error.

 */
int nc_inq_grp_parent(int ncid, int *parent_ncid)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->inq_grp_parent(ncid,parent_ncid);
}

/*! Get a group ncid given the group name.

  @param[in] ncid      The ncid of the file.
  @param[in] grp_name  The name of the group in question.
  @param[out] grp_ncid Pointer to memory to hold the identifier of the group in question.

  @returns Error code or ::NC_NOERR for no error.

\note{This has same semantics as nc_inq_ncid}

*/
int nc_inq_grp_ncid(int ncid, const char *grp_name, int *grp_ncid)
{
    return nc_inq_ncid(ncid,grp_name,grp_ncid);
}

/*! Get the full ncid given a group name.

  @param[in] ncid      The ncid of the file.
  @param[in] full_name The full name of the group in question.
  @param[out] grp_ncid Pointer to memory to hold the identifier of the full group in question.

  @returns Error code or ::NC_NOERR for no error.

 */
int nc_inq_grp_full_ncid(int ncid, const char *full_name, int *grp_ncid)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->inq_grp_full_ncid(ncid,full_name,grp_ncid);
}


/*! Get a list of varids associated with a group given a group ID.

  @param[in] ncid    The ncid of the group in question.
  @param[out] nvars  Pointer to memory to hold the number of variables in the group in question.
  @param[out] varids Pointer to memory to hold the variable ids contained by the group in question.

  @returns Error code or ::NC_NOERR for no error.

*/
int nc_inq_varids(int ncid, int *nvars, int *varids)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->inq_varids(ncid,nvars,varids);
}

/*! Retrieve a list of dimension ids associated with a group.

  @param[in] ncid    The ncid of the group in question.
  @param[out] ndims  Pointer to memory to contain the number of dimids associated with the group.
  @param[out] dimids Pointer to memory to contain the number of dimensions associated with the group.
  @param[in] include_parents If non-zero, parent groups are also traversed.

  @returns Error code or ::NC_NOERR for no error.

 */
int nc_inq_dimids(int ncid, int *ndims, int *dimids, int include_parents)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->inq_dimids(ncid,ndims,dimids,include_parents);
}

/*! Retrieve a list of types associated with a group

  @param[in] ncid     The ncid for the group in question.
  @param[out] ntypes  Pointer to memory to hold the number of typeids contained by the group in question.
  @param[out] typeids Pointer to memory to hold the typeids contained by the group in question.

  @returns Error code or ::NC_NOERR for no error.

*/

int nc_inq_typeids(int ncid, int *ntypes, int *typeids)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->inq_typeids(ncid,ntypes,typeids);
}

/*! Define a new group.

  The function nc_def_grp() adds a new
  group to an open netCDF dataset in define mode.  It returns (as an
  argument) a group id, given the parent ncid and the name of the group.

  A group may be a top-level group if it is passed the ncid of the file,
  or a sub-group if passed the ncid of an existing group.

  @param[in]  parent_ncid The ncid of the parent for the group.
  @param[in]  name        Name of the new group.
  @param[out] new_ncid    Pointer to memory to hold the new ncid.

  @returns Error code or ::NC_NOERR for no error.

  @retval ::NC_NOERR No error.
  @retval ::NC_ENOTNC4 Not an nc4 file.
  @retval ::NC_ENOTINDEFINE Not in define mode.
  @retval ::NC_ESTRICTNC3 Not permissible in nc4 classic mode.
  @retval ::NC_EPERM Write to read only.
  @retval ::NC_ENOMEM Memory allocation (malloc) failure.
  @retval ::NC_ENAMEINUSE String match to name in use.

  \section nc_def_grp_example Example

  Here is an example using nc_def_grp() to create a new group.

  \code{.c}

  #include <netcdf.h>
  ...
  int status, ncid, grpid, latid, recid;
  ...

  \endcode

*/
int nc_def_grp(int parent_ncid, const char *name, int *new_ncid)
{
    NC* ncp;
    int stat = NC_check_id(parent_ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->def_grp(parent_ncid,name,new_ncid);
}

/*! Rename a group.

  @param[in] grpid The ID for the group in question.
  @param[in] name  The new name for the group.

  @returns Error code or ::NC_NOERR for no error.

*/
int nc_rename_grp(int grpid, const char *name)
{
    NC* ncp;
    int stat = NC_check_id(grpid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->rename_grp(grpid,name);
}

/*! Print the metadata for a file.

  @param[in] ncid The ncid of an open file.

  @returns Error code or ::NC_NOERR for no error.

 */
int nc_show_metadata(int ncid)
{
    NC* ncp;
    int stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    return ncp->dispatch->show_metadata(ncid);
}

/** \} */
