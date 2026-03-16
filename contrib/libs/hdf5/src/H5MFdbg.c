/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*-------------------------------------------------------------------------
 *
 * Created:             H5MFdbg.c
 *
 * Purpose:             File memory management debugging functions.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#define H5F_FRIEND      /*suppress error about including H5Fpkg	  */
#include "H5MFmodule.h" /* This source code file is part of the H5MF module */
#define H5MF_DEBUGGING  /* Need access to file space debugging routines */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Fpkg.h"     /* File access				*/
#include "H5MFpkg.h"    /* File memory management		*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* User data for free space section iterator callback */
typedef struct {
    H5FS_t *fspace; /* Free space manager */
    FILE   *stream; /* Stream for output */
    int     indent; /* Indentation amount */
    int     fwidth; /* Field width amount */
} H5MF_debug_iter_ud_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

static herr_t H5MF__sects_debug_cb(H5FS_section_info_t *_sect, void *_udata);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5MF__sects_debug_cb
 *
 * Purpose:	Prints debugging info about a free space section for a file
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5MF__sects_debug_cb(H5FS_section_info_t *_sect, void *_udata)
{
    H5MF_free_section_t  *sect      = (H5MF_free_section_t *)_sect;   /* Section to dump info */
    H5MF_debug_iter_ud_t *udata     = (H5MF_debug_iter_ud_t *)_udata; /* User data for callbacks */
    herr_t                ret_value = SUCCEED;                        /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(sect);
    assert(udata);

    /* Print generic section information */
    fprintf(udata->stream, "%*s%-*s %s\n", udata->indent, "", udata->fwidth, "Section type:",
            (sect->sect_info.type == H5MF_FSPACE_SECT_SIMPLE
                 ? "simple"
                 : (sect->sect_info.type == H5MF_FSPACE_SECT_SMALL
                        ? "small"
                        : (sect->sect_info.type == H5MF_FSPACE_SECT_LARGE ? "large" : "unknown"))));
    fprintf(udata->stream, "%*s%-*s %" PRIuHADDR "\n", udata->indent, "", udata->fwidth,
            "Section address:", sect->sect_info.addr);
    fprintf(udata->stream, "%*s%-*s %" PRIuHSIZE "\n", udata->indent, "", udata->fwidth,
            "Section size:", sect->sect_info.size);
    fprintf(udata->stream, "%*s%-*s %" PRIuHADDR "\n", udata->indent, "", udata->fwidth,
            "End of section:", (haddr_t)((sect->sect_info.addr + sect->sect_info.size) - 1));
    fprintf(udata->stream, "%*s%-*s %s\n", udata->indent, "", udata->fwidth,
            "Section state:", (sect->sect_info.state == H5FS_SECT_LIVE ? "live" : "serialized"));

    /* Dump section-specific debugging information */
    if (H5FS_sect_debug(udata->fspace, _sect, udata->stream, udata->indent + 3, MAX(0, udata->fwidth - 3)) <
        0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_BADITER, FAIL, "can't dump section's debugging info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5MF__sects_debug_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5MF_sects_debug
 *
 * Purpose:	Iterate over free space sections for a file
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5MF_sects_debug(H5F_t *f, haddr_t fs_addr, FILE *stream, int indent, int fwidth)
{
    H5F_mem_page_t type;                /* Memory type for iteration */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(H5AC__FREESPACE_TAG, FAIL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    for (type = H5F_MEM_PAGE_DEFAULT; type < H5F_MEM_PAGE_NTYPES; type++)
        if (H5_addr_eq(f->shared->fs_addr[type], fs_addr)) {
            if (!f->shared->fs_man[type])
                if (H5MF__open_fstype(f, type) < 0)
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, FAIL, "can't initialize file free space");

            if (f->shared->fs_man[type]) {
                H5MF_debug_iter_ud_t udata; /* User data for callbacks */

                /* Prepare user data for section iteration callback */
                udata.fspace = f->shared->fs_man[type];
                udata.stream = stream;
                udata.indent = indent;
                udata.fwidth = fwidth;

                /* Iterate over all the free space sections */
                if (H5FS_sect_iterate(f, f->shared->fs_man[type], H5MF__sects_debug_cb, &udata) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_BADITER, FAIL, "can't iterate over heap's free space");

                /* Close the free space information */
                if (H5FS_close(f, f->shared->fs_man[type]) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't release free space info");
            } /* end if */
            break;
        } /* end if */

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* H5MF_sects_debug() */

#ifdef H5MF_ALLOC_DEBUG_DUMP

/*-------------------------------------------------------------------------
 * Function:	H5MF__sects_dump
 *
 * Purpose:	Prints debugging info about free space sections for a file.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5MF__sects_dump(H5F_t *f, FILE *stream)
{
    haddr_t eoa;                 /* End of allocated space in the file */
    int     indent    = 0;       /* Amount to indent */
    int     fwidth    = 50;      /* Field width */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(H5AC__FREESPACE_TAG)
#ifdef H5MF_ALLOC_DEBUG
    fprintf(stderr, "%s: Dumping file free space sections\n", __func__);
#endif /* H5MF_ALLOC_DEBUG */

    /*
     * Check arguments.
     */
    assert(f);
    assert(stream);

    /* Retrieve the 'eoa' for the file */
    if (HADDR_UNDEF == (eoa = H5F_get_eoa(f, H5FD_MEM_DEFAULT)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "driver get_eoa request failed");
#ifdef H5MF_ALLOC_DEBUG
    fprintf(stderr, "%s: for type = H5FD_MEM_DEFAULT, eoa = %" PRIuHADDR "\n", __func__, eoa);
#endif /* H5MF_ALLOC_DEBUG */

    if (H5F_PAGED_AGGR(f)) {  /* File space paging */
        H5F_mem_page_t ptype; /* Memory type for iteration -- page fs */

        for (ptype = H5F_MEM_PAGE_META; ptype < H5F_MEM_PAGE_NTYPES; ptype++) {
            /* Print header for type */
            fprintf(stream, "%*sFile Free Space Info for type = %u:\n", indent, "", (unsigned)ptype);

            /* Print header for sections */
            fprintf(stream, "%*sSections:\n", indent + 3, "");

            /* If there is a free space manager for this type, iterate over them */
            if (f->shared->fs_man[ptype]) {
                H5MF_debug_iter_ud_t udata; /* User data for callbacks */

                /* Prepare user data for section iteration callback */
                udata.fspace = f->shared->fs_man[ptype];
                udata.stream = stream;
                udata.indent = indent + 6;
                udata.fwidth = MAX(0, fwidth - 6);

                /* Iterate over all the free space sections */
                if (H5FS_sect_iterate(f, f->shared->fs_man[ptype], H5MF__sects_debug_cb, &udata) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_BADITER, FAIL, "can't iterate over heap's free space");
            } /* end if */
            else
                /* No sections of this type */
                fprintf(stream, "%*s<none>\n", indent + 6, "");
        }                                  /* end for */
    }                                      /* end if */
    else {                                 /* not file space paging */
        H5FD_mem_t atype;                  /* Memory type for iteration -- aggr fs */
        haddr_t    ma_addr  = HADDR_UNDEF; /* Base "metadata aggregator" address */
        hsize_t    ma_size  = 0;           /* Size of "metadata aggregator" */
        haddr_t    sda_addr = HADDR_UNDEF; /* Base "small data aggregator" address */
        hsize_t    sda_size = 0;           /* Size of "small data aggregator" */

        /* Retrieve metadata aggregator info, if available */
        H5MF__aggr_query(f, &(f->shared->meta_aggr), &ma_addr, &ma_size);
#ifdef H5MF_ALLOC_DEBUG
        fprintf(stderr,
                "%s: ma_addr = %" PRIuHADDR ", ma_size = %" PRIuHSIZE ", end of ma = %" PRIuHADDR "\n",
                __func__, ma_addr, ma_size, (haddr_t)((ma_addr + ma_size) - 1));
#endif /* H5MF_ALLOC_DEBUG */

        /* Retrieve 'small data' aggregator info, if available */
        H5MF__aggr_query(f, &(f->shared->sdata_aggr), &sda_addr, &sda_size);
#ifdef H5MF_ALLOC_DEBUG
        fprintf(stderr,
                "%s: sda_addr = %" PRIuHADDR ", sda_size = %" PRIuHSIZE ", end of sda = %" PRIuHADDR "\n",
                __func__, sda_addr, sda_size, (haddr_t)((sda_addr + sda_size) - 1));
#endif /* H5MF_ALLOC_DEBUG */

        /* Iterate over all the free space types that have managers and dump each free list's space */
        for (atype = H5FD_MEM_DEFAULT; atype < H5FD_MEM_NTYPES; atype++) {
            /* Print header for type */
            fprintf(stream, "%*sFile Free Space Info for type = %u:\n", indent, "", (unsigned)atype);

            /* Check for this type being mapped to another type */
            if (H5FD_MEM_DEFAULT == f->shared->fs_type_map[atype] || atype == f->shared->fs_type_map[atype]) {
                /* Retrieve the 'eoa' for this file memory type */
                if (HADDR_UNDEF == (eoa = H5F_get_eoa(f, atype)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "driver get_eoa request failed");
                fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent + 3, "", MAX(0, fwidth - 3), "eoa:", eoa);

                /* Print header for sections */
                fprintf(stream, "%*sSections:\n", indent + 3, "");

                /* If there is a free space manager for this type, iterate over them */
                if (f->shared->fs_man[atype]) {
                    H5MF_debug_iter_ud_t udata; /* User data for callbacks */

                    /* Prepare user data for section iteration callback */
                    udata.fspace = f->shared->fs_man[atype];
                    udata.stream = stream;
                    udata.indent = indent + 6;
                    udata.fwidth = MAX(0, fwidth - 6);

                    /* Iterate over all the free space sections */
                    if (H5FS_sect_iterate(f, f->shared->fs_man[atype], H5MF__sects_debug_cb, &udata) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_BADITER, FAIL, "can't iterate over heap's free space");
                }    /* end if */
                else /* No sections of this type */
                    fprintf(stream, "%*s<none>\n", indent + 6, "");
            } /* end if */
            else
                fprintf(stream, "%*sMapped to type = %u\n", indent, "",
                        (unsigned)f->shared->fs_type_map[atype]);
        } /* end for */
    }     /* end else */

done:
    fprintf(stderr, "%s: Done dumping file free space sections\n", __func__);
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5MF__sects_dump() */
#endif /* H5MF_ALLOC_DEBUG_DUMP */
