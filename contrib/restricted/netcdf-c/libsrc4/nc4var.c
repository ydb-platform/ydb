/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions.*/
/**
 * @file
 * @internal This file is part of netcdf-4, a netCDF-like interface
 * for HDF5, or a HDF5 backend for netCDF, depending on your point of
 * view. This file handles the NetCDF-4 variable functions.
 *
 * @author Ed Hartnett, Dennis Heimbigner, Ward Fisher
 */

#include "config.h"
#include "nc4internal.h"
#include "nc4dispatch.h"
#ifdef USE_HDF5
#include "hdf5internal.h"
#endif
#include <math.h>

/** @internal Default size for unlimited dim chunksize. */
#define DEFAULT_1D_UNLIM_SIZE (4096)

/* Define log_e for 10 and 2. Prefer constants defined in math.h,
 * however, GCC environments can have hard time defining M_LN10/M_LN2
 * despite finding math.h */
#ifndef M_LN10
# define M_LN10         2.30258509299404568402  /**< log_e 10 */
#endif /* M_LN10 */
#ifndef M_LN2
# define M_LN2          0.69314718055994530942  /**< log_e 2 */
#endif /* M_LN2 */

/** Used in quantize code. Number of explicit bits in significand for
 * floats. Bits 0-22 of SP significands are explicit. Bit 23 is
 * implicitly 1. Currently redundant with NC_QUANTIZE_MAX_FLOAT_NSB
 * and with limits.h/climit (FLT_MANT_DIG-1) */
#define BIT_XPL_NBR_SGN_FLT (23)

/** Used in quantize code. Number of explicit bits in significand for
 * doubles. Bits 0-51 of DP significands are explicit. Bit 52 is
 * implicitly 1. Currently redundant with NC_QUANTIZE_MAX_DOUBLE_NSB 
 * and with limits.h/climit (DBL_MANT_DIG-1) */
#define BIT_XPL_NBR_SGN_DBL (52) 
  
/** Pointer union for floating point and bitmask types. */
typedef union { /* ptr_unn */
  float *fp;
  double *dp;
  unsigned int *ui32p;
  unsigned long long *ui64p;
  void *vp;
} ptr_unn;

/**
 * @internal This is called by nc_get_var_chunk_cache(). Get chunk
 * cache size for a variable.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param sizep Gets size in bytes of cache.
 * @param nelemsp Gets number of element slots in cache.
 * @param preemptionp Gets cache swapping setting.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Not a netCDF-4 file.
 * @author Ed Hartnett
 */
int
NC4_get_var_chunk_cache(int ncid, int varid, size_t *sizep,
                        size_t *nelemsp, float *preemptionp)
{
    NC *nc;
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    int retval;

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_nc_grp_h5(ncid, &nc, &grp, &h5)))
        return retval;
    assert(nc && grp && h5);

    /* Find the var. */
    var = (NC_VAR_INFO_T*)ncindexith(grp->vars,(size_t)varid);
    if(!var)
        return NC_ENOTVAR;
    assert(var && var->hdr.id == varid);

    /* Give the user what they want. */
    if (sizep)
        *sizep = var->chunkcache.size;
    if (nelemsp)
        *nelemsp = var->chunkcache.nelems;
    if (preemptionp)
        *preemptionp = var->chunkcache.preemption;

    return NC_NOERR;
}

/**
 * @internal A wrapper for NC4_get_var_chunk_cache(), we need this
 * version for fortran.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param sizep Gets size in MB of cache.
 * @param nelemsp Gets number of element slots in cache.
 * @param preemptionp Gets cache swapping setting.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Not a netCDF-4 file.
 * @author Ed Hartnett
 */
int
nc_get_var_chunk_cache_ints(int ncid, int varid, int *sizep,
                            int *nelemsp, int *preemptionp)
{
    size_t real_size, real_nelems;
    float real_preemption;
    int ret;

    if ((ret = NC4_get_var_chunk_cache(ncid, varid, &real_size,
                                       &real_nelems, &real_preemption)))
        return ret;

    if (sizep)
        *sizep = (int)(real_size / MEGABYTE);
    if (nelemsp)
        *nelemsp = (int)real_nelems;
    if(preemptionp)
        *preemptionp = (int)(real_preemption * 100);

    return NC_NOERR;
}

/**
 * @internal Get all the information about a variable. Pass NULL for
 * whatever you don't care about. This is the internal function called
 * by nc_inq_var(), nc_inq_var_deflate(), nc_inq_var_fletcher32(),
 * nc_inq_var_chunking(), nc_inq_var_chunking_ints(),
 * nc_inq_var_fill(), nc_inq_var_endian(), nc_inq_var_filter(), and
 * nc_inq_var_szip().
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param name Gets name.
 * @param xtypep Gets type.
 * @param ndimsp Gets number of dims.
 * @param dimidsp Gets array of dim IDs.
 * @param nattsp Gets number of attributes.
 * @param shufflep Gets shuffle setting.
 * @param deflatep Gets deflate setting.
 * @param deflate_levelp Gets deflate level.
 * @param fletcher32p Gets fletcher32 setting.
 * @param storagep Gets storage setting.
 * @param chunksizesp Gets chunksizes.
 * @param no_fill Gets fill mode.
 * @param fill_valuep Gets fill value.
 * @param endiannessp Gets one of ::NC_ENDIAN_BIG ::NC_ENDIAN_LITTLE
 * ::NC_ENDIAN_NATIVE
 * @param idp Pointer to memory to store filter id.
 * @param nparamsp Pointer to memory to store filter parameter count.
 * @param params Pointer to vector of unsigned integers into which
 * to store filter parameters.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Bad varid.
 * @returns ::NC_ENOMEM Out of memory.
 * @returns ::NC_EINVAL Invalid input.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep,
                int *ndimsp, int *dimidsp, int *nattsp,
                int *shufflep, int *deflatep, int *deflate_levelp,
                int *fletcher32p, int *storagep, size_t *chunksizesp,
                int *no_fill, void *fill_valuep, int *endiannessp,
                unsigned int *idp, size_t *nparamsp, unsigned int *params)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    int d;
    int retval;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_nc_grp_h5(ncid, NULL, &grp, &h5)))
        return retval;
    assert(grp && h5);

    /* If the varid is -1, find the global atts and call it a day. */
    if (varid == NC_GLOBAL && nattsp)
    {
        *nattsp = ncindexcount(grp->att);
        return NC_NOERR;
    }

    /* Find the var. */
    if (!(var = (NC_VAR_INFO_T *)ncindexith(grp->vars, (size_t)varid)))
        return NC_ENOTVAR;
    assert(var && var->hdr.id == varid);

    /* Copy the data to the user's data buffers. */
    if (name)
        strcpy(name, var->hdr.name);
    if (xtypep)
        *xtypep = var->type_info->hdr.id;
    if (ndimsp)
        *ndimsp = (int)var->ndims;
    if (dimidsp)
        for (d = 0; d < var->ndims; d++)
            dimidsp[d] = var->dimids[d];
    if (nattsp)
        *nattsp = ncindexcount(var->att);

    /* Did the user want the chunksizes? */
    if (var->storage == NC_CHUNKED && chunksizesp)
    {
        for (d = 0; d < var->ndims; d++)
        {
            chunksizesp[d] = var->chunksizes[d];
            LOG((4, "chunksizesp[%d]=%d", d, chunksizesp[d]));
        }
    }

    /* Did the user inquire about the storage? */
    if (storagep)
	*storagep = var->storage;

    /* Filter stuff. */
    if (shufflep) {
	retval = nc_inq_var_filter_info(ncid,varid,H5Z_FILTER_SHUFFLE,0,NULL);
	if(retval && retval != NC_ENOFILTER) return retval;
	*shufflep = (retval == NC_NOERR?1:0);
    }
    if (fletcher32p) {
	retval = nc_inq_var_filter_info(ncid,varid,H5Z_FILTER_FLETCHER32,0,NULL);
	if(retval && retval != NC_ENOFILTER) return retval;
        *fletcher32p = (retval == NC_NOERR?1:0);
    }
    if (deflatep)
	return NC_EFILTER;

    if (idp) {
	return NC_EFILTER;
    }

    /* Fill value stuff. */
    if (no_fill)
        *no_fill = (int)var->no_fill;

    /* Don't do a thing with fill_valuep if no_fill mode is set for
     * this var, or if fill_valuep is NULL. */
    if (!var->no_fill && fill_valuep)
    {
        /* Do we have a fill value for this var? */
        if (var->fill_value)
        {
	    int xtype = var->type_info->hdr.id;
	    if((retval = NC_copy_data(h5->controller,xtype,var->fill_value,1,fill_valuep))) return retval;
	}
        else
        {
            if ((retval = nc4_get_default_fill_value(var->type_info, fill_valuep)))
                    return retval;
        }
    }

    /* Does the user want the endianness of this variable? */
    if (endiannessp)
        *endiannessp = var->endianness;

    return NC_NOERR;
}

/**
 * @internal Inquire about chunking settings for a var. This is used
 * by the fortran API.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param storagep Gets contiguous setting.
 * @param chunksizesp Gets chunksizes.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_EINVAL Invalid input
 * @returns ::NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
nc_inq_var_chunking_ints(int ncid, int varid, int *storagep, int *chunksizesp)
{
    NC_VAR_INFO_T *var;
    size_t *cs = NULL;
    int i, retval;

    /* Get pointer to the var. */
    if ((retval = nc4_find_grp_h5_var(ncid, varid, NULL, NULL, &var)))
        return retval;
    assert(var);

    /* Allocate space for the size_t copy of the chunksizes array. */
    if (var->ndims)
        if (!(cs = malloc(var->ndims * sizeof(size_t))))
            return NC_ENOMEM;

    /* Call the netcdf-4 version directly. */
    retval = NC4_inq_var_all(ncid, varid, NULL, NULL, NULL, NULL, NULL,
                             NULL, NULL, NULL, NULL, storagep, cs, NULL,
                             NULL, NULL, NULL, NULL, NULL);

    /* Copy from size_t array. */
    if (!retval && chunksizesp && var->storage == NC_CHUNKED)
    {
        for (i = 0; i < var->ndims; i++)
        {
            chunksizesp[i] = (int)cs[i];
            if (cs[i] > NC_MAX_INT)
                retval = NC_ERANGE;
        }
    }

    if (var->ndims)
        free(cs);
    return retval;
}

/**
 * @internal Find the ID of a variable, from the name. This function
 * is called by nc_inq_varid().
 *
 * @param ncid File ID.
 * @param name Name of the variable.
 * @param varidp Gets variable ID.

 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Bad variable ID.
 */
int
NC4_inq_varid(int ncid, const char *name, int *varidp)
{
    NC *nc;
    NC_GRP_INFO_T *grp;
    NC_VAR_INFO_T *var;
    char norm_name[NC_MAX_NAME + 1];
    int retval;

    if (!name)
        return NC_EINVAL;
    if (!varidp)
        return NC_NOERR;

    LOG((2, "%s: ncid 0x%x name %s", __func__, ncid, name));

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_nc_grp_h5(ncid, &nc, &grp, NULL)))
        return retval;

    /* Normalize name. */
    if ((retval = nc4_normalize_name(name, norm_name)))
        return retval;

    /* Find var of this name. */
    var = (NC_VAR_INFO_T*)ncindexlookup(grp->vars,norm_name);
    if(var)
    {
        *varidp = var->hdr.id;
        return NC_NOERR;
    }
    return NC_ENOTVAR;
}

/**
 * @internal
 *
 * This function will change the parallel access of a variable from
 * independent to collective.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param par_access NC_COLLECTIVE or NC_INDEPENDENT.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Invalid ncid passed.
 * @returns ::NC_ENOTVAR Invalid varid passed.
 * @returns ::NC_ENOPAR LFile was not opened with nc_open_par/nc_create_par.
 * @returns ::NC_EINVAL Invalid par_access specified.
 * @returns ::NC_NOERR for success
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_var_par_access(int ncid, int varid, int par_access)
{
#ifndef USE_PARALLEL4
    NC_UNUSED(ncid);
    NC_UNUSED(varid);
    NC_UNUSED(par_access);
    return NC_ENOPAR;
#else
    NC *nc;
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T *h5;
    NC_VAR_INFO_T *var;
    int retval;

    LOG((1, "%s: ncid 0x%x varid %d par_access %d", __func__, ncid,
         varid, par_access));

    if (par_access != NC_INDEPENDENT && par_access != NC_COLLECTIVE)
        return NC_EINVAL;

    /* Find info for this file and group, and set pointer to each. */
    if ((retval = nc4_find_nc_grp_h5(ncid, &nc, &grp, &h5)))
        return retval;

    /* This function only for files opened with nc_open_par or nc_create_par. */
    if (!h5->parallel)
        return NC_ENOPAR;

    /* Find the var, and set its preference. */
    var = (NC_VAR_INFO_T*)ncindexith(grp->vars,varid);
    if (!var) return NC_ENOTVAR;
    assert(var->hdr.id == varid);

    /* If zlib, shuffle, or fletcher32 filters are in use, then access
     * must be collective. Fail an attempt to set such a variable to
     * independent access. */
    if (nclistlength((NClist*)var->filters) > 0 &&
        par_access == NC_INDEPENDENT)
        return NC_EINVAL;

    if (par_access)
        var->parallel_access = NC_COLLECTIVE;
    else
        var->parallel_access = NC_INDEPENDENT;
    return NC_NOERR;
#endif /* USE_PARALLEL4 */
}

/**
 * @internal Copy data from one buffer to another, performing
 * appropriate data conversion.
 *
 * This function will copy data from one buffer to another, in
 * accordance with the types. Range errors will be noted, and the fill
 * value used (or the default fill value if none is supplied) for
 * values that overflow the type.
 *
 * This function applies quantization to float and double data, if
 * desired. The code to do this is derived from the corresponding 
 * filter in the CCR project (e.g., 
 * https://github.com/ccr/ccr/blob/master/hdf5_plugins/BITGROOM/src/H5Zbitgroom.c).
 *
 * @param src Pointer to source of data.
 * @param dest Pointer that gets data.
 * @param src_type Type ID of source data.
 * @param dest_type Type ID of destination data.
 * @param len Number of elements of data to copy.
 * @param range_error Pointer that gets 1 if there was a range error.
 * @param fill_value The fill value.
 * @param strict_nc3 Non-zero if strict model in effect.
 * @param quantize_mode May be ::NC_NOQUANTIZE, ::NC_QUANTIZE_BITGROOM, 
 * ::NC_QUANTIZE_GRANULARBR, or ::NC_QUANTIZE_BITROUND.
 * @param nsd Number of significant digits for quantize. Ignored
 * unless quantize_mode is ::NC_QUANTIZE_BITGROOM, 
 * ::NC_QUANTIZE_GRANULARBR, or ::NC_QUANTIZE_BITROUND
 * 
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADTYPE Type not found.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc4_convert_type(const void *src, void *dest, const nc_type src_type,
                 const nc_type dest_type, const size_t len, int *range_error,
                 const void *fill_value, int strict_nc3, int quantize_mode,
		 int nsd)
{
    /* These vars are used with quantize feature. */
    const double bit_per_dgt = M_LN10 / M_LN2; /* 3.32 [frc] Bits per decimal digit of precision  = log2(10) */
    const double dgt_per_bit= M_LN2 / M_LN10; /* 0.301 [frc] Decimal digits per bit of precision = log10(2) */
    double mnt; /* [frc] Mantissa, 0.5 <= mnt < 1.0 */
    double mnt_fabs; /* [frc] fabs(mantissa) */
    double mnt_log10_fabs; /* [frc] log10(fabs(mantissa))) */
    double mss_val_cmp_dbl; /* Missing value for comparison to double precision values */
    double val_dbl; /* [frc] Copy of input value to avoid indirection */
    float mss_val_cmp_flt; /* Missing value for comparison to single precision values */
    float val_flt; /* [frc] Copy of input value to avoid indirection */
    int bit_xpl_nbr_zro; /* [nbr] Number of explicit bits to zero */
    int dgt_nbr; /* [nbr] Number of digits before decimal point */
    int qnt_pwr; /* [nbr] Power of two in quantization mask: qnt_msk = 2^qnt_pwr */
    int xpn_bs2; /* [nbr] Binary exponent xpn_bs2 in val = sign(val) * 2^xpn_bs2 * mnt, 0.5 < mnt <= 1.0 */
    size_t idx;
    unsigned int *u32_ptr;
    unsigned int msk_f32_u32_zro;
    unsigned int msk_f32_u32_one;
    unsigned int msk_f32_u32_hshv;
    unsigned long long int *u64_ptr;
    unsigned long long int msk_f64_u64_zro;
    unsigned long long int msk_f64_u64_one;
    unsigned long long int msk_f64_u64_hshv;
    unsigned short prc_bnr_xpl_rqr; /* [nbr] Explicitly represented binary digits required to retain */
    ptr_unn op1; /* I/O [frc] Values to quantize */
    
    char *cp, *cp1;
    float *fp, *fp1;
    double *dp, *dp1;
    int *ip, *ip1;
    short *sp, *sp1;
    signed char *bp, *bp1;
    unsigned char *ubp, *ubp1;
    unsigned short *usp, *usp1;
    unsigned int *uip, *uip1;
    long long *lip, *lip1;
    unsigned long long *ulip, *ulip1;
    size_t count = 0;

    *range_error = 0;
    LOG((3, "%s: len %d src_type %d dest_type %d", __func__, len, src_type,
         dest_type));

    /* If quantize is in use, set up some values. Quantize can only be
     * used when the destination type is NC_FLOAT or NC_DOUBLE. */
    if (quantize_mode != NC_NOQUANTIZE)
      {
        assert(dest_type == NC_FLOAT || dest_type == NC_DOUBLE);

	/* Parameters shared by all quantization codecs */
        if (dest_type == NC_FLOAT)
	  {
            /* Determine the fill value. */
            if (fill_value)
	      mss_val_cmp_flt = *(float *)fill_value;
            else
	      mss_val_cmp_flt = NC_FILL_FLOAT;

	  }
        else
	  {
	
            /* Determine the fill value. */
            if (fill_value)
	      mss_val_cmp_dbl = *(double *)fill_value;
            else
	      mss_val_cmp_dbl = NC_FILL_DOUBLE;

	  }

	/* Set parameters used by BitGroom and BitRound here, outside value loop.
	   Equivalent parameters used by GranularBR are set inside value loop,
	   since keep bits and thus masks can change for every value. */
	if (quantize_mode == NC_QUANTIZE_BITGROOM ||
	    quantize_mode == NC_QUANTIZE_BITROUND )
	  {

	    if (quantize_mode == NC_QUANTIZE_BITGROOM){

	      /* BitGroom interprets nsd as number of significant decimal digits
	       * Must convert that to number of significant bits to preserve
	       * How many bits to preserve? Being conservative, we round up the
	       * exact binary digits of precision. Add one because the first bit
	       * is implicit not explicit but corner cases prevent our taking
	       * advantage of this. */
	      prc_bnr_xpl_rqr = (unsigned short)ceil(nsd * bit_per_dgt) + 1;

	    }else if (quantize_mode == NC_QUANTIZE_BITROUND){

	      /* BitRound interprets nsd as number of significant binary digits (bits) */
	      prc_bnr_xpl_rqr = (unsigned short)nsd;
	      
	    }
	    
	    if (dest_type == NC_FLOAT)
	      {

		bit_xpl_nbr_zro = BIT_XPL_NBR_SGN_FLT - prc_bnr_xpl_rqr;

		/* Create mask */
		msk_f32_u32_zro = 0U; /* Zero all bits */
		msk_f32_u32_zro = ~msk_f32_u32_zro; /* Turn all bits to ones */
		
		/* BitShave mask for AND: Left shift zeros into bits to be
		 * rounded, leave ones in untouched bits. */
		msk_f32_u32_zro <<= bit_xpl_nbr_zro;
		
		/* BitSet mask for OR: Put ones into bits to be set, zeros in
		 * untouched bits. */
		msk_f32_u32_one = ~msk_f32_u32_zro;

		/* BitRound mask for ADD: Set one bit: the MSB of LSBs */
		msk_f32_u32_hshv=msk_f32_u32_one & (msk_f32_u32_zro >> 1);

	      }
	    else
	      {

		bit_xpl_nbr_zro = BIT_XPL_NBR_SGN_DBL - prc_bnr_xpl_rqr;
		/* Create mask. */
		msk_f64_u64_zro = 0UL; /* Zero all bits. */
		msk_f64_u64_zro = ~msk_f64_u64_zro; /* Turn all bits to ones. */
		
		/* BitShave mask for AND: Left shift zeros into bits to be
		 * rounded, leave ones in untouched bits. */
		msk_f64_u64_zro <<= bit_xpl_nbr_zro;
		
		/* BitSet mask for OR: Put ones into bits to be set, zeros in
		 * untouched bits. */
		msk_f64_u64_one =~ msk_f64_u64_zro;

		/* BitRound mask for ADD: Set one bit: the MSB of LSBs */
		msk_f64_u64_hshv = msk_f64_u64_one & (msk_f64_u64_zro >> 1);

	      }

	  }
	  
      } /* endif quantize */
	    
    /* OK, this is ugly. If you can think of anything better, I'm open
       to suggestions!

       Note that we don't use a default fill value for type
       NC_BYTE. This is because Lord Voldemort cast a nofilleramous spell
       at Harry Potter, but it bounced off his scar and hit the netcdf-4
       code.
    */
    switch (src_type)
    {
    case NC_CHAR:
        switch (dest_type)
        {
        case NC_CHAR:
            for (cp = (char *)src, cp1 = dest; count < len; count++)
                *cp1++ = *cp++;
            break;
        default:
            LOG((0, "%s: Unknown destination type.", __func__));
        }
        break;

    case NC_BYTE:
        switch (dest_type)
        {
        case NC_BYTE:
            for (bp = (signed char *)src, bp1 = dest; count < len; count++)
                *bp1++ = *bp++;
            break;
        case NC_UBYTE:
            for (bp = (signed char *)src, ubp = dest; count < len; count++)
            {
                if (*bp < 0)
                    (*range_error)++;
                *ubp++ = (unsigned char)*bp++;
            }
            break;
        case NC_SHORT:
            for (bp = (signed char *)src, sp = dest; count < len; count++)
                *sp++ = *bp++;
            break;
        case NC_USHORT:
            for (bp = (signed char *)src, usp = dest; count < len; count++)
            {
                if (*bp < 0)
                    (*range_error)++;
                *usp++ = (unsigned short)*bp++;
            }
            break;
        case NC_INT:
            for (bp = (signed char *)src, ip = dest; count < len; count++)
                *ip++ = *bp++;
            break;
        case NC_UINT:
            for (bp = (signed char *)src, uip = dest; count < len; count++)
            {
                if (*bp < 0)
                    (*range_error)++;
                *uip++ = (unsigned int)*bp++;
            }
            break;
        case NC_INT64:
            for (bp = (signed char *)src, lip = dest; count < len; count++)
                *lip++ = *bp++;
            break;
        case NC_UINT64:
            for (bp = (signed char *)src, ulip = dest; count < len; count++)
            {
                if (*bp < 0)
                    (*range_error)++;
                *ulip++ = (unsigned long long)*bp++;
            }
            break;
        case NC_FLOAT:
	    for (bp = (signed char *)src, fp = dest; count < len; count++)
		*fp++ = *bp++;
            break;
        case NC_DOUBLE:
            for (bp = (signed char *)src, dp = dest; count < len; count++)
                *dp++ = *bp++;
            break;
        default:
            LOG((0, "%s: unexpected dest type. src_type %d, dest_type %d",
                 __func__, src_type, dest_type));
            return NC_EBADTYPE;
        }
        break;

    case NC_UBYTE:
        switch (dest_type)
        {
        case NC_BYTE:
            for (ubp = (unsigned char *)src, bp = dest; count < len; count++)
            {
                if (!strict_nc3 && *ubp > X_SCHAR_MAX)
                    (*range_error)++;
                *bp++ = (signed char)*ubp++;
            }
            break;
        case NC_SHORT:
            for (ubp = (unsigned char *)src, sp = dest; count < len; count++)
                *sp++ = *ubp++;
            break;
        case NC_UBYTE:
            for (ubp = (unsigned char *)src, ubp1 = dest; count < len; count++)
                *ubp1++ = *ubp++;
            break;
        case NC_USHORT:
            for (ubp = (unsigned char *)src, usp = dest; count < len; count++)
                *usp++ = *ubp++;
            break;
        case NC_INT:
            for (ubp = (unsigned char *)src, ip = dest; count < len; count++)
                *ip++ = *ubp++;
            break;
        case NC_UINT:
            for (ubp = (unsigned char *)src, uip = dest; count < len; count++)
                *uip++ = *ubp++;
            break;
        case NC_INT64:
            for (ubp = (unsigned char *)src, lip = dest; count < len; count++)
                *lip++ = *ubp++;
            break;
        case NC_UINT64:
            for (ubp = (unsigned char *)src, ulip = dest; count < len; count++)
                *ulip++ = *ubp++;
            break;
        case NC_FLOAT:
            for (ubp = (unsigned char *)src, fp = dest; count < len; count++)
                *fp++ = *ubp++;
            break;
        case NC_DOUBLE:
            for (ubp = (unsigned char *)src, dp = dest; count < len; count++)
                *dp++ = *ubp++;
            break;
        default:
            LOG((0, "%s: unexpected dest type. src_type %d, dest_type %d",
                 __func__, src_type, dest_type));
            return NC_EBADTYPE;
        }
        break;

    case NC_SHORT:
        switch (dest_type)
        {
        case NC_UBYTE:
            for (sp = (short *)src, ubp = dest; count < len; count++)
            {
                if (*sp > X_UCHAR_MAX || *sp < 0)
                    (*range_error)++;
                *ubp++ = (unsigned char)*sp++;
            }
            break;
        case NC_BYTE:
            for (sp = (short *)src, bp = dest; count < len; count++)
            {
                if (*sp > X_SCHAR_MAX || *sp < X_SCHAR_MIN)
                    (*range_error)++;
                *bp++ = (signed char)*sp++;
            }
            break;
        case NC_SHORT:
            for (sp = (short *)src, sp1 = dest; count < len; count++)
                *sp1++ = *sp++;
            break;
        case NC_USHORT:
            for (sp = (short *)src, usp = dest; count < len; count++)
            {
                if (*sp < 0)
                    (*range_error)++;
                *usp++ = (unsigned short)*sp++;
            }
            break;
        case NC_INT:
            for (sp = (short *)src, ip = dest; count < len; count++)
                *ip++ = *sp++;
            break;
        case NC_UINT:
            for (sp = (short *)src, uip = dest; count < len; count++)
            {
                if (*sp < 0)
                    (*range_error)++;
                *uip++ = (unsigned int)*sp++;
            }
            break;
        case NC_INT64:
            for (sp = (short *)src, lip = dest; count < len; count++)
                *lip++ = *sp++;
            break;
        case NC_UINT64:
            for (sp = (short *)src, ulip = dest; count < len; count++)
            {
                if (*sp < 0)
                    (*range_error)++;
                *ulip++ = (unsigned long long)*sp++;
            }
            break;
        case NC_FLOAT:
            for (sp = (short *)src, fp = dest; count < len; count++)
                *fp++ = *sp++;
            break;
        case NC_DOUBLE:
            for (sp = (short *)src, dp = dest; count < len; count++)
                *dp++ = *sp++;
            break;
        default:
            LOG((0, "%s: unexpected dest type. src_type %d, dest_type %d",
                 __func__, src_type, dest_type));
            return NC_EBADTYPE;
        }
        break;

    case NC_USHORT:
        switch (dest_type)
        {
        case NC_UBYTE:
            for (usp = (unsigned short *)src, ubp = dest; count < len; count++)
            {
                if (*usp > X_UCHAR_MAX)
                    (*range_error)++;
                *ubp++ = (unsigned char)*usp++;
            }
            break;
        case NC_BYTE:
            for (usp = (unsigned short *)src, bp = dest; count < len; count++)
            {
                if (*usp > X_SCHAR_MAX)
                    (*range_error)++;
                *bp++ = (signed char)*usp++;
            }
            break;
        case NC_SHORT:
            for (usp = (unsigned short *)src, sp = dest; count < len; count++)
            {
                if (*usp > X_SHORT_MAX)
                    (*range_error)++;
                *sp++ = (signed short)*usp++;
            }
            break;
        case NC_USHORT:
            for (usp = (unsigned short *)src, usp1 = dest; count < len; count++)
                *usp1++ = *usp++;
            break;
        case NC_INT:
            for (usp = (unsigned short *)src, ip = dest; count < len; count++)
                *ip++ = *usp++;
            break;
        case NC_UINT:
            for (usp = (unsigned short *)src, uip = dest; count < len; count++)
                *uip++ = *usp++;
            break;
        case NC_INT64:
            for (usp = (unsigned short *)src, lip = dest; count < len; count++)
                *lip++ = *usp++;
            break;
        case NC_UINT64:
            for (usp = (unsigned short *)src, ulip = dest; count < len; count++)
                *ulip++ = *usp++;
            break;
        case NC_FLOAT:
            for (usp = (unsigned short *)src, fp = dest; count < len; count++)
                *fp++ = *usp++;
            break;
        case NC_DOUBLE:
            for (usp = (unsigned short *)src, dp = dest; count < len; count++)
                *dp++ = *usp++;
            break;
        default:
            LOG((0, "%s: unexpected dest type. src_type %d, dest_type %d",
                 __func__, src_type, dest_type));
            return NC_EBADTYPE;
        }
        break;

    case NC_INT:
        switch (dest_type)
        {
        case NC_UBYTE:
            for (ip = (int *)src, ubp = dest; count < len; count++)
            {
                if (*ip > X_UCHAR_MAX || *ip < 0)
                    (*range_error)++;
                *ubp++ = (unsigned char)*ip++;
            }
            break;
        case NC_BYTE:
            for (ip = (int *)src, bp = dest; count < len; count++)
            {
                if (*ip > X_SCHAR_MAX || *ip < X_SCHAR_MIN)
                    (*range_error)++;
                *bp++ = (signed char)*ip++;
            }
            break;
        case NC_SHORT:
            for (ip = (int *)src, sp = dest; count < len; count++)
            {
                if (*ip > X_SHORT_MAX || *ip < X_SHORT_MIN)
                    (*range_error)++;
                *sp++ = (short)*ip++;
            }
            break;
        case NC_USHORT:
            for (ip = (int *)src, usp = dest; count < len; count++)
            {
                if (*ip > X_USHORT_MAX || *ip < 0)
                    (*range_error)++;
                *usp++ = (unsigned short)*ip++;
            }
            break;
        case NC_INT: /* src is int */
            for (ip = (int *)src, ip1 = dest; count < len; count++)
            {
                if (*ip > X_INT_MAX || *ip < X_INT_MIN)
                    (*range_error)++;
                *ip1++ = *ip++;
            }
            break;
        case NC_UINT:
            for (ip = (int *)src, uip = dest; count < len; count++)
            {
                if (*ip > X_UINT_MAX || *ip < 0)
                    (*range_error)++;
                *uip++ = (unsigned int)*ip++;
            }
            break;
        case NC_INT64:
            for (ip = (int *)src, lip = dest; count < len; count++)
                *lip++ = *ip++;
            break;
        case NC_UINT64:
            for (ip = (int *)src, ulip = dest; count < len; count++)
            {
                if (*ip < 0)
                    (*range_error)++;
                *ulip++ = (unsigned long long)*ip++;
            }
            break;
        case NC_FLOAT:
            for (ip = (int *)src, fp = dest; count < len; count++)
                *fp++ = (float)*ip++;
            break;
        case NC_DOUBLE:
            for (ip = (int *)src, dp = dest; count < len; count++)
                *dp++ = (double)*ip++;
            break;
        default:
            LOG((0, "%s: unexpected dest type. src_type %d, dest_type %d",
                 __func__, src_type, dest_type));
            return NC_EBADTYPE;
        }
        break;

    case NC_UINT:
        switch (dest_type)
        {
        case NC_UBYTE:
            for (uip = (unsigned int *)src, ubp = dest; count < len; count++)
            {
                if (*uip > X_UCHAR_MAX)
                    (*range_error)++;
                *ubp++ = (unsigned char)*uip++;
            }
            break;
        case NC_BYTE:
            for (uip = (unsigned int *)src, bp = dest; count < len; count++)
            {
                if (*uip > X_SCHAR_MAX)
                    (*range_error)++;
                *bp++ = (signed char)*uip++;
            }
            break;
        case NC_SHORT:
            for (uip = (unsigned int *)src, sp = dest; count < len; count++)
            {
                if (*uip > X_SHORT_MAX)
                    (*range_error)++;
                *sp++ = (signed short)*uip++;
            }
            break;
        case NC_USHORT:
            for (uip = (unsigned int *)src, usp = dest; count < len; count++)
            {
                if (*uip > X_USHORT_MAX)
                    (*range_error)++;
                *usp++ = (unsigned short)*uip++;
            }
            break;
        case NC_INT:
            for (uip = (unsigned int *)src, ip = dest; count < len; count++)
            {
                if (*uip > X_INT_MAX)
                    (*range_error)++;
                *ip++ = (int)*uip++;
            }
            break;
        case NC_UINT:
            for (uip = (unsigned int *)src, uip1 = dest; count < len; count++)
            {
                if (*uip > X_UINT_MAX)
                    (*range_error)++;
                *uip1++ = *uip++;
            }
            break;
        case NC_INT64:
            for (uip = (unsigned int *)src, lip = dest; count < len; count++)
                *lip++ = *uip++;
            break;
        case NC_UINT64:
            for (uip = (unsigned int *)src, ulip = dest; count < len; count++)
                *ulip++ = *uip++;
            break;
        case NC_FLOAT:
            for (uip = (unsigned int *)src, fp = dest; count < len; count++)
                *fp++ = (float)*uip++;
            break;
        case NC_DOUBLE:
            for (uip = (unsigned int *)src, dp = dest; count < len; count++)
                *dp++ = *uip++;
            break;
        default:
            LOG((0, "%s: unexpected dest type. src_type %d, dest_type %d",
                 __func__, src_type, dest_type));
            return NC_EBADTYPE;
        }
        break;

    case NC_INT64:
        switch (dest_type)
        {
        case NC_UBYTE:
            for (lip = (long long *)src, ubp = dest; count < len; count++)
            {
                if (*lip > X_UCHAR_MAX || *lip < 0)
                    (*range_error)++;
                *ubp++ = (unsigned char)*lip++;
            }
            break;
        case NC_BYTE:
            for (lip = (long long *)src, bp = dest; count < len; count++)
            {
                if (*lip > X_SCHAR_MAX || *lip < X_SCHAR_MIN)
                    (*range_error)++;
                *bp++ = (signed char)*lip++;
            }
            break;
        case NC_SHORT:
            for (lip = (long long *)src, sp = dest; count < len; count++)
            {
                if (*lip > X_SHORT_MAX || *lip < X_SHORT_MIN)
                    (*range_error)++;
                *sp++ = (short)*lip++;
            }
            break;
        case NC_USHORT:
            for (lip = (long long *)src, usp = dest; count < len; count++)
            {
                if (*lip > X_USHORT_MAX || *lip < 0)
                    (*range_error)++;
                *usp++ = (unsigned short)*lip++;
            }
            break;
        case NC_UINT:
            for (lip = (long long *)src, uip = dest; count < len; count++)
            {
                if (*lip > X_UINT_MAX || *lip < 0)
                    (*range_error)++;
                *uip++ = (unsigned int)*lip++;
            }
            break;
        case NC_INT:
            for (lip = (long long *)src, ip = dest; count < len; count++)
            {
                if (*lip > X_INT_MAX || *lip < X_INT_MIN)
                    (*range_error)++;
                *ip++ = (int)*lip++;
            }
            break;
        case NC_INT64:
            for (lip = (long long *)src, lip1 = dest; count < len; count++)
                *lip1++ = *lip++;
            break;
        case NC_UINT64:
            for (lip = (long long *)src, ulip = dest; count < len; count++)
            {
                if (*lip < 0)
                    (*range_error)++;
                *ulip++ = (unsigned long long)*lip++;
            }
            break;
        case NC_FLOAT:
            for (lip = (long long *)src, fp = dest; count < len; count++)
                *fp++ = (float)*lip++;
            break;
        case NC_DOUBLE:
            for (lip = (long long *)src, dp = dest; count < len; count++)
                *dp++ = (double)*lip++;
            break;
        default:
            LOG((0, "%s: unexpected dest type. src_type %d, dest_type %d",
                 __func__, src_type, dest_type));
            return NC_EBADTYPE;
        }
        break;

    case NC_UINT64:
        switch (dest_type)
        {
        case NC_UBYTE:
            for (ulip = (unsigned long long *)src, ubp = dest; count < len; count++)
            {
                if (*ulip > X_UCHAR_MAX)
                    (*range_error)++;
                *ubp++ = (unsigned char)*ulip++;
            }
            break;
        case NC_BYTE:
            for (ulip = (unsigned long long *)src, bp = dest; count < len; count++)
            {
                if (*ulip > X_SCHAR_MAX)
                    (*range_error)++;
                *bp++ = (signed char)*ulip++;
            }
            break;
        case NC_SHORT:
            for (ulip = (unsigned long long *)src, sp = dest; count < len; count++)
            {
                if (*ulip > X_SHORT_MAX)
                    (*range_error)++;
                *sp++ = (short)*ulip++;
            }
            break;
        case NC_USHORT:
            for (ulip = (unsigned long long *)src, usp = dest; count < len; count++)
            {
                if (*ulip > X_USHORT_MAX)
                    (*range_error)++;
                *usp++ = (unsigned short)*ulip++;
            }
            break;
        case NC_UINT:
            for (ulip = (unsigned long long *)src, uip = dest; count < len; count++)
            {
                if (*ulip > X_UINT_MAX)
                    (*range_error)++;
                *uip++ = (unsigned int)*ulip++;
            }
            break;
        case NC_INT:
            for (ulip = (unsigned long long *)src, ip = dest; count < len; count++)
            {
                if (*ulip > X_INT_MAX)
                    (*range_error)++;
                *ip++ = (int)*ulip++;
            }
            break;
        case NC_INT64:
            for (ulip = (unsigned long long *)src, lip = dest; count < len; count++)
            {
                if (*ulip > X_INT64_MAX)
                    (*range_error)++;
                *lip++ = (long long)*ulip++;
            }
            break;
        case NC_UINT64:
            for (ulip = (unsigned long long *)src, ulip1 = dest; count < len; count++)
                *ulip1++ = *ulip++;
            break;
        case NC_FLOAT:
            for (ulip = (unsigned long long *)src, fp = dest; count < len; count++)
                *fp++ = (float)*ulip++;
            break;
        case NC_DOUBLE:
            for (ulip = (unsigned long long *)src, dp = dest; count < len; count++)
                *dp++ = (double)*ulip++;
            break;
        default:
            LOG((0, "%s: unexpected dest type. src_type %d, dest_type %d",
                 __func__, src_type, dest_type));
            return NC_EBADTYPE;
        }
        break;

    case NC_FLOAT:
        switch (dest_type)
        {
        case NC_UBYTE:
            for (fp = (float *)src, ubp = dest; count < len; count++)
            {
                if (*fp > X_UCHAR_MAX || *fp < 0)
                    (*range_error)++;
                *ubp++ = (unsigned char)*fp++;
            }
            break;
        case NC_BYTE:
            for (fp = (float *)src, bp = dest; count < len; count++)
            {
                if (*fp > (double)X_SCHAR_MAX || *fp < (double)X_SCHAR_MIN)
                    (*range_error)++;
                *bp++ = (signed char)*fp++;
            }
            break;
        case NC_SHORT:
            for (fp = (float *)src, sp = dest; count < len; count++)
            {
                if (*fp > (double)X_SHORT_MAX || *fp < (double)X_SHORT_MIN)
                    (*range_error)++;
                *sp++ = (short)*fp++;
            }
            break;
        case NC_USHORT:
            for (fp = (float *)src, usp = dest; count < len; count++)
            {
                if (*fp > X_USHORT_MAX || *fp < 0)
                    (*range_error)++;
                *usp++ = (unsigned short)*fp++;
            }
            break;
        case NC_UINT:
            for (fp = (float *)src, uip = dest; count < len; count++)
            {
                if (*fp > (float)X_UINT_MAX || *fp < 0)
                    (*range_error)++;
                *uip++ = (unsigned int)*fp++;
            }
            break;
        case NC_INT:
            for (fp = (float *)src, ip = dest; count < len; count++)
            {
                if (*fp > (double)X_INT_MAX || *fp < (double)X_INT_MIN)
                    (*range_error)++;
                *ip++ = (int)*fp++;
            }
            break;
        case NC_INT64:
            for (fp = (float *)src, lip = dest; count < len; count++)
            {
                if (*fp > (float)X_INT64_MAX || *fp <X_INT64_MIN)
                    (*range_error)++;
                *lip++ = (long long)*fp++;
            }
            break;
        case NC_UINT64:
            for (fp = (float *)src, ulip = dest; count < len; count++)
            {
                if (*fp > (float)X_UINT64_MAX || *fp < 0)
                    (*range_error)++;
                *ulip++ = (unsigned long long)*fp++;
            }
            break;
        case NC_FLOAT:
            for (fp = (float *)src, fp1 = dest; count < len; count++)
                *fp1++ = *fp++;
            break;
        case NC_DOUBLE:
            for (fp = (float *)src, dp = dest; count < len; count++)
                *dp++ = *fp++;
            break;
        default:
            LOG((0, "%s: unexpected dest type. src_type %d, dest_type %d",
                 __func__, src_type, dest_type));
            return NC_EBADTYPE;
        }
        break;

    case NC_DOUBLE:
        switch (dest_type)
        {
        case NC_UBYTE:
            for (dp = (double *)src, ubp = dest; count < len; count++)
            {
                if (*dp > X_UCHAR_MAX || *dp < 0)
                    (*range_error)++;
                *ubp++ = (unsigned char)*dp++;
            }
            break;
        case NC_BYTE:
            for (dp = (double *)src, bp = dest; count < len; count++)
            {
                if (*dp > X_SCHAR_MAX || *dp < X_SCHAR_MIN)
                    (*range_error)++;
                *bp++ = (signed char)*dp++;
            }
            break;
        case NC_SHORT:
            for (dp = (double *)src, sp = dest; count < len; count++)
            {
                if (*dp > X_SHORT_MAX || *dp < X_SHORT_MIN)
                    (*range_error)++;
                *sp++ = (short)*dp++;
            }
            break;
        case NC_USHORT:
            for (dp = (double *)src, usp = dest; count < len; count++)
            {
                if (*dp > X_USHORT_MAX || *dp < 0)
                    (*range_error)++;
                *usp++ = (unsigned short)*dp++;
            }
            break;
        case NC_UINT:
            for (dp = (double *)src, uip = dest; count < len; count++)
            {
                if (*dp > X_UINT_MAX || *dp < 0)
                    (*range_error)++;
                *uip++ = (unsigned int)*dp++;
            }
            break;
        case NC_INT:
            for (dp = (double *)src, ip = dest; count < len; count++)
            {
                if (*dp > X_INT_MAX || *dp < X_INT_MIN)
                    (*range_error)++;
                *ip++ = (int)*dp++;
            }
            break;
        case NC_INT64:
            for (dp = (double *)src, lip = dest; count < len; count++)
            {
                if (*dp > (double)X_INT64_MAX || *dp < X_INT64_MIN)
                    (*range_error)++;
                *lip++ = (long long)*dp++;
            }
            break;
        case NC_UINT64:
            for (dp = (double *)src, ulip = dest; count < len; count++)
            {
                if (*dp > (double)X_UINT64_MAX || *dp < 0)
                    (*range_error)++;
                *ulip++ = (unsigned long long)*dp++;
            }
            break;
        case NC_FLOAT:
            for (dp = (double *)src, fp = dest; count < len; count++)
            {
                if (isgreater(*dp, X_FLOAT_MAX) || isless(*dp, X_FLOAT_MIN))
                    (*range_error)++;
                *fp++ = (float)*dp++;
            }
            break;
        case NC_DOUBLE:
            for (dp = (double *)src, dp1 = dest; count < len; count++)
                *dp1++ = *dp++;
            break;
        default:
            LOG((0, "%s: unexpected dest type. src_type %d, dest_type %d",
                 __func__, src_type, dest_type));
            return NC_EBADTYPE;
        }
        break;

    default:
        LOG((0, "%s: unexpected src type. src_type %d, dest_type %d",
             __func__, src_type, dest_type));
        return NC_EBADTYPE;
    }

    /* If quantize is in use, determine masks, copy the data, do the
     * quantization. */
    if (quantize_mode == NC_QUANTIZE_BITGROOM)
    {
        if (dest_type == NC_FLOAT)
        {
            /* BitGroom: alternately shave and set LSBs */
            op1.fp = (float *)dest;
            u32_ptr = op1.ui32p;
	    /* Do not quantize _FillValue, +/- zero, or NaN */
            for (idx = 0L; idx < len; idx += 2L)
	        if ((val_flt=op1.fp[idx]) != mss_val_cmp_flt && val_flt != 0.0f && !isnan(val_flt))
                    u32_ptr[idx] &= msk_f32_u32_zro;
            for (idx = 1L; idx < len; idx += 2L)
	        if ((val_flt=op1.fp[idx]) != mss_val_cmp_flt && val_flt != 0.0f && !isnan(val_flt))
                    u32_ptr[idx] |= msk_f32_u32_one;
        }
        else
        {
            /* BitGroom: alternately shave and set LSBs. */
            op1.dp = (double *)dest;
            u64_ptr = op1.ui64p;
	    /* Do not quantize _FillValue, +/- zero, or NaN */
            for (idx = 0L; idx < len; idx += 2L)
	        if ((val_dbl=op1.dp[idx]) != mss_val_cmp_dbl && val_dbl != 0.0 && !isnan(val_dbl))
                    u64_ptr[idx] &= msk_f64_u64_zro;
            for (idx = 1L; idx < len; idx += 2L)
	        if ((val_dbl=op1.dp[idx]) != mss_val_cmp_dbl && val_dbl != 0.0 && !isnan(val_dbl))
                    u64_ptr[idx] |= msk_f64_u64_one;
        }
    } /* endif BitGroom */

    if (quantize_mode == NC_QUANTIZE_BITROUND)
      {
        if (dest_type == NC_FLOAT)
	  {
            /* BitRound: Quantize to user-specified NSB with IEEE-rounding */
            op1.fp = (float *)dest;
            u32_ptr = op1.ui32p;
            for (idx = 0L; idx < len; idx++){
	      /* Do not quantize _FillValue, +/- zero, or NaN */
	      if ((val_flt=op1.fp[idx]) != mss_val_cmp_flt && val_flt != 0.0f && !isnan(val_flt)){
		u32_ptr[idx] += msk_f32_u32_hshv; /* Add 1 to the MSB of LSBs, carry 1 to mantissa or even exponent */
		u32_ptr[idx] &= msk_f32_u32_zro; /* Shave it */
	      }
	    }
	  }
        else
	  {
            /* BitRound: Quantize to user-specified NSB with IEEE-rounding */
            op1.dp = (double *)dest;
            u64_ptr = op1.ui64p;
            for (idx = 0L; idx < len; idx++){
	      /* Do not quantize _FillValue, +/- zero, or NaN */
	      if ((val_dbl=op1.dp[idx]) != mss_val_cmp_dbl && val_dbl != 0.0 && !isnan(val_dbl)){
		u64_ptr[idx] += msk_f64_u64_hshv; /* Add 1 to the MSB of LSBs, carry 1 to mantissa or even exponent */
		u64_ptr[idx] &= msk_f64_u64_zro; /* Shave it */
	      }
	    }
	  }
      } /* endif BitRound */
    
    if (quantize_mode == NC_QUANTIZE_GRANULARBR)
    {
        if (dest_type == NC_FLOAT)
        {
            /* Granular BitRound */
            op1.fp = (float *)dest;
            u32_ptr = op1.ui32p;
            for (idx = 0L; idx < len; idx++)
	      {
		/* Do not quantize _FillValue, +/- zero, or NaN */
		if((val_dbl=op1.fp[idx]) != mss_val_cmp_flt && val_dbl != 0.0 && !isnan(val_dbl))
		  {
		    mnt = frexp(val_dbl, &xpn_bs2); /* DGG19 p. 4102 (8) */
		    mnt_fabs = fabs(mnt);
		    mnt_log10_fabs = log10(mnt_fabs);
		    /* 20211003 Continuous determination of dgt_nbr improves CR by ~10% */
		    dgt_nbr = (int)floor(xpn_bs2 * dgt_per_bit + mnt_log10_fabs) + 1; /* DGG19 p. 4102 (8.67) */
		    qnt_pwr = (int)floor(bit_per_dgt * (dgt_nbr - nsd)); /* DGG19 p. 4101 (7) */
		    prc_bnr_xpl_rqr = mnt_fabs == 0.0 ? 0 : (unsigned short)abs((int)floor(xpn_bs2 - bit_per_dgt*mnt_log10_fabs) - qnt_pwr); /* Protect against mnt = -0.0 */
		    prc_bnr_xpl_rqr--; /* 20211003 Reduce formula result by 1 bit: Passes all tests, improves CR by ~10% */

		    bit_xpl_nbr_zro = BIT_XPL_NBR_SGN_FLT - prc_bnr_xpl_rqr;
		    msk_f32_u32_zro = 0U; /* Zero all bits */
		    msk_f32_u32_zro = ~msk_f32_u32_zro; /* Turn all bits to ones */
		    /* Bit Shave mask for AND: Left shift zeros into bits to be rounded, leave ones in untouched bits */
		    msk_f32_u32_zro <<= bit_xpl_nbr_zro;
		    /* Bit Set   mask for OR:  Put ones into bits to be set, zeros in untouched bits */
		    msk_f32_u32_one = ~msk_f32_u32_zro;
		    msk_f32_u32_hshv = msk_f32_u32_one & (msk_f32_u32_zro >> 1); /* Set one bit: the MSB of LSBs */
		    u32_ptr[idx] += msk_f32_u32_hshv; /* Add 1 to the MSB of LSBs, carry 1 to mantissa or even exponent */
		    u32_ptr[idx] &= msk_f32_u32_zro; /* Shave it */

		  } /* !mss_val_cmp_flt */

	      } 
        }
        else
        {
            /* Granular BitRound */
            op1.dp = (double *)dest;
            u64_ptr = op1.ui64p;
            for (idx = 0L; idx < len; idx++)
	      {
		/* Do not quantize _FillValue, +/- zero, or NaN */
		if((val_dbl=op1.dp[idx]) != mss_val_cmp_dbl && val_dbl != 0.0 && !isnan(val_dbl))
		  {
		    mnt = frexp(val_dbl, &xpn_bs2); /* DGG19 p. 4102 (8) */
		    mnt_fabs = fabs(mnt);
		    mnt_log10_fabs = log10(mnt_fabs);
		    /* 20211003 Continuous determination of dgt_nbr improves CR by ~10% */
		    dgt_nbr = (int)floor(xpn_bs2 * dgt_per_bit + mnt_log10_fabs) + 1; /* DGG19 p. 4102 (8.67) */
		    qnt_pwr = (int)floor(bit_per_dgt * (dgt_nbr - nsd)); /* DGG19 p. 4101 (7) */
		    prc_bnr_xpl_rqr = mnt_fabs == 0.0 ? 0 : (unsigned short)abs((int)floor(xpn_bs2 - bit_per_dgt*mnt_log10_fabs) - qnt_pwr); /* Protect against mnt = -0.0 */
		    prc_bnr_xpl_rqr--; /* 20211003 Reduce formula result by 1 bit: Passes all tests, improves CR by ~10% */

		    bit_xpl_nbr_zro = BIT_XPL_NBR_SGN_DBL - prc_bnr_xpl_rqr;
		    msk_f64_u64_zro = 0ULL; /* Zero all bits */
		    msk_f64_u64_zro = ~msk_f64_u64_zro; /* Turn all bits to ones */
		    /* Bit Shave mask for AND: Left shift zeros into bits to be rounded, leave ones in untouched bits */
		    msk_f64_u64_zro <<= bit_xpl_nbr_zro;
		    /* Bit Set   mask for OR:  Put ones into bits to be set, zeros in untouched bits */
		    msk_f64_u64_one = ~msk_f64_u64_zro;
		    msk_f64_u64_hshv = msk_f64_u64_one & (msk_f64_u64_zro >> 1); /* Set one bit: the MSB of LSBs */
		    u64_ptr[idx] += msk_f64_u64_hshv; /* Add 1 to the MSB of LSBs, carry 1 to mantissa or even exponent */
		    u64_ptr[idx] &= msk_f64_u64_zro; /* Shave it */

		  } /* !mss_val_cmp_dbl */

	      }
        }
    } /* endif GranularBR */

    return NC_NOERR;
}

/**
 * @internal What fill value should be used for a variable?
 *
 * @param h5 Pointer to HDF5 file info struct.
 * @param var Pointer to variable info struct.
 * @param fillp Pointer that gets pointer to fill value.
 *
 * @returns NC_NOERR No error.
 * @returns NC_ENOMEM Out of memory.
 * @author Ed Hartnett
 */
int
nc4_get_fill_value(NC_FILE_INFO_T *h5, NC_VAR_INFO_T *var, void **fillp)
{
    size_t size;
    int retval;

    /* Find out how much space we need for this type's fill value. */
    if (var->type_info->nc_type_class == NC_VLEN)
        size = sizeof(nc_vlen_t);
    else if (var->type_info->nc_type_class == NC_STRING)
        size = sizeof(char *);
    else
    {
        if ((retval = nc4_get_typelen_mem(h5, var->type_info->hdr.id, &size)))
            return retval;
    }
    assert(size);

    /* Allocate the space. */
    if (!((*fillp) = calloc(1, size)))
        return NC_ENOMEM;

    /* If the user has set a fill_value for this var, use, otherwise
     * find the default fill value. */
    if (var->fill_value)
    {
        LOG((4, "Found a fill value for var %s", var->hdr.name));
        if (var->type_info->nc_type_class == NC_VLEN)
        {
            nc_vlen_t *in_vlen = (nc_vlen_t *)(var->fill_value), *fv_vlen = (nc_vlen_t *)(*fillp);
            size_t basetypesize = 0;

            if((retval=nc4_get_typelen_mem(h5, var->type_info->u.v.base_nc_typeid, &basetypesize)))
                return retval;

            fv_vlen->len = in_vlen->len;
            if (!(fv_vlen->p = malloc(basetypesize * in_vlen->len)))
            {
                free(*fillp);
                *fillp = NULL;
                return NC_ENOMEM;
            }
            memcpy(fv_vlen->p, in_vlen->p, in_vlen->len * basetypesize);
        }
        else if (var->type_info->nc_type_class == NC_STRING)
        {
            if (*(char **)var->fill_value)
                if (!(**(char ***)fillp = strdup(*(char **)var->fill_value)))
                {
                    free(*fillp);
                    *fillp = NULL;
                    return NC_ENOMEM;
                }
        }
        else
            memcpy((*fillp), var->fill_value, size);
    }
    else
    {
        if (nc4_get_default_fill_value(var->type_info, *fillp))
        {
            /* Note: release memory, but don't return error on failure */
            free(*fillp);
            *fillp = NULL;
        }
    }

    return NC_NOERR;
}

/**
 * @internal Get the length, in bytes, of one element of a type in
 * memory.
 *
 * @param h5 Pointer to HDF5 file info struct.
 * @param xtype NetCDF type ID.
 * @param len Pointer that gets length in bytes.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EBADTYPE Type not found.
 * @author Ed Hartnett
 */
int
nc4_get_typelen_mem(NC_FILE_INFO_T *h5, nc_type xtype, size_t *len)
{
    NC_TYPE_INFO_T *type;
    int retval;

    LOG((4, "%s xtype: %d", __func__, xtype));
    assert(len);

    /* If this is an atomic type, the answer is easy. */
    switch (xtype)
    {
    case NC_BYTE:
    case NC_CHAR:
    case NC_UBYTE:
        *len = sizeof(char);
        return NC_NOERR;
    case NC_SHORT:
    case NC_USHORT:
        *len = sizeof(short);
        return NC_NOERR;
    case NC_INT:
    case NC_UINT:
        *len = sizeof(int);
        return NC_NOERR;
    case NC_FLOAT:
        *len = sizeof(float);
        return NC_NOERR;
    case NC_DOUBLE:
        *len = sizeof(double);
        return NC_NOERR;
    case NC_INT64:
    case NC_UINT64:
        *len = sizeof(long long);
        return NC_NOERR;
    case NC_STRING:
        *len = sizeof(char *);
        return NC_NOERR;
    }

    /* See if var is compound type. */
    if ((retval = nc4_find_type(h5, xtype, &type)))
        return retval;

    if (!type)
        return NC_EBADTYPE;

    *len = type->size;

    LOG((5, "type->size: %d", type->size));

    return NC_NOERR;
}


/**
 * @internal Check a set of chunksizes to see if they specify a chunk
 * that is too big.
 *
 * @param grp Pointer to the group info.
 * @param var Pointer to the var info.
 * @param chunksizes Array of chunksizes to check.
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_EBADCHUNK Bad chunksize.
 */
int
nc4_check_chunksizes(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *var, const size_t *chunksizes)
{
    double dprod;
    size_t type_len;
    int d;
    int retval;

    if ((retval = nc4_get_typelen_mem(grp->nc4_info, var->type_info->hdr.id, &type_len)))
        return retval;
    if (var->type_info->nc_type_class == NC_VLEN)
        dprod = (double)sizeof(nc_vlen_t);
    else
        dprod = (double)type_len;
    for (d = 0; d < var->ndims; d++)
        dprod *= (double)chunksizes[d];

    if (dprod > (double) NC_MAX_UINT)
        return NC_EBADCHUNK;

    return NC_NOERR;
}

/**
 * @internal Determine some default chunksizes for a variable.
 *
 * @param grp Pointer to the group info.
 * @param var Pointer to the var info.
 *
 * @returns ::NC_NOERR for success
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
nc4_find_default_chunksizes2(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *var)
{
    int d;
    size_t type_size;
    float num_values = 1, num_unlim = 0;
    int retval;
    size_t suggested_size;
#ifdef LOGGING
    double total_chunk_size;
#endif

    if (var->type_info->nc_type_class == NC_STRING)
        type_size = sizeof(char *);
    else
        type_size = var->type_info->size;

#ifdef LOGGING
    /* Later this will become the total number of bytes in the default
     * chunk. */
    total_chunk_size = (double) type_size;
#endif

    if(var->chunksizes == NULL) {
        if((var->chunksizes = calloc(1,sizeof(size_t)*var->ndims)) == NULL)
            return NC_ENOMEM;
    }

    /* How many values in the variable (or one record, if there are
     * unlimited dimensions). */
    for (d = 0; d < var->ndims; d++)
    {
        assert(var->dim[d]);
        if (! var->dim[d]->unlimited)
            num_values *= (float)var->dim[d]->len;
        else {
            num_unlim++;
            var->chunksizes[d] = 1; /* overwritten below, if all dims are unlimited */
        }
    }
    /* Special case to avoid 1D vars with unlim dim taking huge amount
       of space (DEFAULT_CHUNK_SIZE bytes). Instead we limit to about
       4KB */
    if (var->ndims == 1 && num_unlim == 1) {
        if (DEFAULT_CHUNK_SIZE / type_size <= 0)
            suggested_size = 1;
        else if (DEFAULT_CHUNK_SIZE / type_size > DEFAULT_1D_UNLIM_SIZE)
            suggested_size = DEFAULT_1D_UNLIM_SIZE;
        else
            suggested_size = DEFAULT_CHUNK_SIZE / type_size;
        var->chunksizes[0] = suggested_size / type_size;
        LOG((4, "%s: name %s dim %d DEFAULT_CHUNK_SIZE %d num_values %f type_size %d "
             "chunksize %ld", __func__, var->hdr.name, d, DEFAULT_CHUNK_SIZE, num_values, type_size, var->chunksizes[0]));
    }
    if (var->ndims > 1 && (float)var->ndims == num_unlim) { /* all dims unlimited */
        suggested_size = (size_t)pow((double)DEFAULT_CHUNK_SIZE/(double)type_size, 1.0/(double)(var->ndims));
        for (d = 0; d < var->ndims; d++)
        {
            var->chunksizes[d] = suggested_size ? suggested_size : 1;
            LOG((4, "%s: name %s dim %d DEFAULT_CHUNK_SIZE %d num_values %f type_size %d "
                 "chunksize %ld", __func__, var->hdr.name, d, DEFAULT_CHUNK_SIZE, num_values, type_size, var->chunksizes[d]));
        }
    }

    /* Pick a chunk length for each dimension, if one has not already
     * been picked above. */
    for (d = 0; d < var->ndims; d++)
        if (!var->chunksizes[d])
        {
            suggested_size = (size_t)(pow((double)DEFAULT_CHUNK_SIZE/(num_values * (double)type_size),
                                        1.0/(double)((double)var->ndims - num_unlim)) * (double)var->dim[d]->len - .5);
            if (suggested_size > var->dim[d]->len)
                suggested_size = var->dim[d]->len;
            var->chunksizes[d] = suggested_size ? suggested_size : 1;
            LOG((4, "%s: name %s dim %d DEFAULT_CHUNK_SIZE %d num_values %f type_size %d "
                 "chunksize %ld", __func__, var->hdr.name, d, DEFAULT_CHUNK_SIZE, num_values, type_size, var->chunksizes[d]));
        }

#ifdef LOGGING
    /* Find total chunk size. */
    for (d = 0; d < var->ndims; d++)
        total_chunk_size *= (double) var->chunksizes[d];
    LOG((4, "total_chunk_size %f", total_chunk_size));
#endif

    /* But did this result in a chunk that is too big? */
    retval = nc4_check_chunksizes(grp, var, var->chunksizes);
    if (retval)
    {
        /* Other error? */
        if (retval != NC_EBADCHUNK)
            return retval;

        /* Chunk is too big! Reduce each dimension by half and try again. */
        for ( ; retval == NC_EBADCHUNK; retval = nc4_check_chunksizes(grp, var, var->chunksizes))
            for (d = 0; d < var->ndims; d++)
                var->chunksizes[d] = var->chunksizes[d]/2 ? var->chunksizes[d]/2 : 1;
    }

    /* Do we have any big data overhangs? They can be dangerous to
     * babies, the elderly, or confused campers who have had too much
     * beer. */
    for (d = 0; d < var->ndims; d++)
    {
        size_t num_chunks;
        size_t overhang;
        assert(var->chunksizes[d] > 0);
        num_chunks = (var->dim[d]->len + var->chunksizes[d] - 1) / var->chunksizes[d];
        if(num_chunks > 0) {
            overhang = (num_chunks * var->chunksizes[d]) - var->dim[d]->len;
            var->chunksizes[d] -= overhang / num_chunks;
        }
    }


    return NC_NOERR;
}

/**
 * @internal Get the default fill value for an atomic type. Memory for
 * fill_value must already be allocated, or you are DOOMED!
 *
 * @param tinfo type object
 * @param fill_value Pointer that gets the default fill value.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EINVAL Can't find atomic type.
 * @author Ed Hartnett
 */
int
nc4_get_default_fill_value(NC_TYPE_INFO_T* tinfo, void *fill_value)
{
    if(tinfo->hdr.id > NC_NAT && tinfo->hdr.id <= NC_MAX_ATOMIC_TYPE)
        return nc4_get_default_atomic_fill_value(tinfo->hdr.id,fill_value);
#ifdef USE_NETCDF4
    switch(tinfo->nc_type_class) {
    case NC_ENUM:
	return nc4_get_default_atomic_fill_value(tinfo->u.e.base_nc_typeid,fill_value);
    case NC_OPAQUE:
    case NC_VLEN:
    case NC_COMPOUND:
	if(fill_value)
	    memset(fill_value,0,tinfo->size);
	break;	
    default: return NC_EBADTYPE;
    }
#endif
    return NC_NOERR;
}

/**
 * @internal Get the default fill value for an atomic type. Memory for
 * fill_value must already be allocated, or you are DOOMED!
 *
 * @param xtype type id
 * @param fill_value Pointer that gets the default fill value.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EINVAL Can't find atomic type.
 * @author Ed Hartnett
 */
int
nc4_get_default_atomic_fill_value(nc_type xtype, void *fill_value)
{
    switch (xtype)
    {
    case NC_CHAR:
        *(char *)fill_value = NC_FILL_CHAR;
        break;

    case NC_STRING:
        *(char **)fill_value = strdup(NC_FILL_STRING);
        break;

    case NC_BYTE:
        *(signed char *)fill_value = NC_FILL_BYTE;
        break;

    case NC_SHORT:
        *(short *)fill_value = NC_FILL_SHORT;
        break;

    case NC_INT:
        *(int *)fill_value = NC_FILL_INT;
        break;

    case NC_UBYTE:
        *(unsigned char *)fill_value = NC_FILL_UBYTE;
        break;

    case NC_USHORT:
        *(unsigned short *)fill_value = NC_FILL_USHORT;
        break;

    case NC_UINT:
        *(unsigned int *)fill_value = NC_FILL_UINT;
        break;

    case NC_INT64:
        *(long long *)fill_value = NC_FILL_INT64;
        break;

    case NC_UINT64:
        *(unsigned long long *)fill_value = NC_FILL_UINT64;
        break;

    case NC_FLOAT:
        *(float *)fill_value = NC_FILL_FLOAT;
        break;

    case NC_DOUBLE:
        *(double *)fill_value = NC_FILL_DOUBLE;
        break;

    default:
        return NC_EINVAL;
    }
    return NC_NOERR;
}

