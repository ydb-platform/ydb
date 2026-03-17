/* Copyright 2018, UCAR/Unidata See netcdf/COPYRIGHT file for copying
 * and redistribution conditions.*/
/**
 * @file
 * @internal This file contains functions that return NC_EPERM, for
 * read-only dispatch layers.
 *
 * @author Ed Hartnett
 */

#include "ncdispatch.h"

/**
 * @internal Not allowed for read-only access.
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param name Ignored.
 * @param newname Ignored.
 *
 * @return ::NC_EPERM Not allowed.
 * @author Ed Hartnett
 */
int
NC_RO_rename_att(int ncid, int varid, const char *name, const char *newname)
{
   return NC_EPERM;
}

/**
 * @internal Not allowed for read-only access.
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param name Ignored.
 *
 * @return ::NC_EPERM Not allowed.
 * @author Ed Hartnett
 */
int
NC_RO_del_att(int ncid, int varid, const char *name)
{
   return NC_EPERM;
}

/**
 * @internal Not allowed for read-only access.
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param name Ignored.
 * @param file_type Ignored.
 * @param len Ignored.
 * @param data Ignored.
 * @param memtype Ignored.
 *
 * @return ::NC_EPERM Not allowed.
 * @author Ed Hartnett
 */
int
NC_RO_put_att(int ncid, int varid, const char *name, nc_type file_type,
              size_t len, const void *data, nc_type mem_type)
{
   return NC_EPERM;
}

/**
 * @internal Not allowed for read-only access.
 *
 * @param ncid Ignored.
 * @param name Ignored.
 * @param len Ignored.
 * @param idp Ignored.
 *
 * @return ::NC_EPERM Not allowed.
 * @author Ed Hartnett
 */
int
NC_RO_def_dim(int ncid, const char *name, size_t len, int *idp)
{
   return NC_EPERM;
}

/**
 * @internal Not allowed for read-only access.
 *
 * @param ncid Ignored.
 * @param dimid Ignored.
 * @param name Ignored.
 *
 * @return ::NC_EPERM Not allowed.
 * @author Ed Hartnett
 */
int
NC_RO_rename_dim(int ncid, int dimid, const char *name)
{
   return NC_EPERM;
}

/**
 * @internal Not allowed for read-only access.
 *
 * @param ncid Ignored.
 * @param name Ignored.
 * @param xtype Ignored.
 * @param ndims Ignored.
 * @param dimidsp Ignored.
 * @param varidp Ignored.
 *
 * @return ::NC_EPERM Not allowed.
 * @author Ed Hartnett
 */
int
NC_RO_def_var(int ncid, const char *name, nc_type xtype,
              int ndims, const int *dimidsp, int *varidp)
{
   return NC_EPERM;
}


/**
 * @internal Not allowed for classic model access.
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param no_fill Ignored.
 * @param fill_value Ignored.
 *
 * @return ::NC_EPERM Not allowed.
 * @author Ed Hartnett
 */
int
NC_RO_def_var_fill(int ncid, int varid, int no_fill, const void *fill_value)
{
   return NC_EPERM;
}

/**
 * @internal Not allowed for read-only access.
 *
 * @param ncid File ID.
 * @param varid Variable ID
 * @param name New name of the variable.
 *
 * @return ::NC_EPERM Not allowed.
 * @author Ed Hartnett
 */
int
NC_RO_rename_var(int ncid, int varid, const char *name)
{
   return NC_EPERM;
}

/**
 * @internal Not allowed for read-only access.
 * 
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param startp Array of start indices.
 * @param countp Array of counts.
 * @param op pointer that gets the data.
 * @param memtype The type of these data in memory.
 *
 * @return ::NC_EPERM Not allowed.
 * @author Ed Hartnett
 */
int
NC_RO_put_vara(int ncid, int varid, const size_t *startp,
               const size_t *countp, const void *op, int memtype)
{
   return NC_EPERM;
}

/**
 * @internal Not allowed for read-only access.
 *
 * @param ncid File and group ID.
 * @param fillmode File mode.
 * @param old_modep Pointer that gets old mode. Ignored if NULL.
 *
 * @return ::NC_EPERM Not allowed.
 * @author Ed Hartnett
 */
int
NC_RO_set_fill(int ncid, int fillmode, int *old_modep)
{
   return NC_EPERM;
}

/**
 * @internal Not allowed for read-only access.
 *
 * @param ncid Ignored.
 *
 * @return ::NC_EPERM Not allowed.
 * @author Ed Hartnett
 */
int
NC_RO_redef(int ncid)
{
   return NC_EPERM;
}

/**
 * @internal Not allowed for read-only access.
 *
 * @param ncid Ignored.
 * @param h_minfree Ignored.
 * @param v_align Ignored.
 * @param v_minfree Ignored.
 * @param r_align Ignored.
 *
 * @return ::NC_EPERM Not allowed.
 * @author Ed Hartnett
 */
int
NC_RO__enddef(int ncid, size_t h_minfree, size_t v_align,
              size_t v_minfree, size_t r_align)
{
   return NC_NOERR;   
}

/**
 * @internal Not allowed for read-only access.
 *
 * @param ncid Ignored.
 *
 * @return ::NC_EPERM Not allowed.
 * @author Ed Hartnett
 */
int
NC_RO_sync(int ncid)
{
   return NC_NOERR;
}

/**
 * @internal Not allowed for read-only access.
 *
 * @param path Ignored.
 * @param cmode Ignored.
 * @param initialsz Ignored.
 * @param basepe Ignored.
 * @param chunksizehintp Ignored.
 * @param use_parallel Ignored.
 * @param parameters Ignored.
 * @param dispatch Ignored.
 * @param ncid Ignored.
 *
 * @return ::NC_EPERM Cannot create files.
 * @author Ed Hartnett
 */
int
NC_RO_create(const char* path, int cmode, size_t initialsz, int basepe,
             size_t *chunksizehintp, void *parameters,
             const NC_Dispatch *dispatch, int ncid)
{
   return NC_EPERM;
}

