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

/*
 * Purpose:     Free space section routines for fractal heaps
 */

/****************/
/* Module Setup */
/****************/

#include "H5HFmodule.h" /* This source code file is part of the H5HF module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5HFpkg.h"     /* Fractal heaps			*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5VMprivate.h" /* Vectors and arrays 			*/

/****************/
/* Local Macros */
/****************/

/* Size of serialized indirect section information */
#define H5HF_SECT_INDIRECT_SERIAL_SIZE(h)                                                                    \
    ((unsigned)(h)->heap_off_size /* Indirect block's offset in "heap space" */                              \
     + (unsigned)2                /* Row */                                                                  \
     + (unsigned)2                /* Column */                                                               \
     + (unsigned)2                /* # of entries */                                                         \
    )

/******************/
/* Local Typedefs */
/******************/

/* Typedef for "class private" information for sections */
typedef struct {
    H5HF_hdr_t *hdr; /* Pointer to fractal heap header */
} H5HF_sect_private_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* Shared routines */
static herr_t               H5FS__sect_init_cls(H5FS_section_class_t *cls, H5HF_hdr_t *hdr);
static herr_t               H5FS__sect_term_cls(H5FS_section_class_t *cls);
static H5HF_free_section_t *H5FS__sect_node_new(unsigned sect_type, haddr_t sect_addr, hsize_t sect_size,
                                                H5FS_section_state_t state);
static herr_t               H5HF__sect_node_free(H5HF_free_section_t *sect, H5HF_indirect_t *parent);

/* 'single' section routines */
static herr_t H5HF__sect_single_locate_parent(H5HF_hdr_t *hdr, bool refresh, H5HF_free_section_t *sect);
static herr_t H5HF__sect_single_full_dblock(H5HF_hdr_t *hdr, H5HF_free_section_t *sect);

/* 'single' section callbacks */
static herr_t               H5HF__sect_single_add(H5FS_section_info_t **sect, unsigned *flags, void *udata);
static H5FS_section_info_t *H5HF__sect_single_deserialize(const H5FS_section_class_t *cls, const uint8_t *buf,
                                                          haddr_t sect_addr, hsize_t sect_size,
                                                          unsigned *des_flags);
static htri_t H5HF__sect_single_can_merge(const H5FS_section_info_t *sect1, const H5FS_section_info_t *sect2,
                                          void *udata);
static herr_t H5HF__sect_single_merge(H5FS_section_info_t **sect1, H5FS_section_info_t *sect2, void *udata);
static htri_t H5HF__sect_single_can_shrink(const H5FS_section_info_t *sect, void *udata);
static herr_t H5HF__sect_single_shrink(H5FS_section_info_t **_sect, void *udata);
static herr_t H5HF__sect_single_valid(const H5FS_section_class_t *cls, const H5FS_section_info_t *sect);

/* 'row' section routines */
static H5HF_free_section_t *H5HF__sect_row_create(haddr_t sect_off, hsize_t sect_size, bool is_first,
                                                  unsigned row, unsigned col, unsigned nentries,
                                                  H5HF_free_section_t *under_sect);
static herr_t               H5HF__sect_row_first(H5HF_hdr_t *hdr, H5HF_free_section_t *sect);
static herr_t               H5HF__sect_row_parent_removed(H5HF_free_section_t *sect);
static herr_t H5HF__sect_row_from_single(H5HF_hdr_t *hdr, H5HF_free_section_t *sect, H5HF_direct_t *dblock);
static herr_t H5HF__sect_row_free_real(H5HF_free_section_t *sect);

/* 'row' section callbacks */
static herr_t H5HF__sect_row_init_cls(H5FS_section_class_t *cls, void *udata);
static herr_t H5HF__sect_row_term_cls(H5FS_section_class_t *cls);
static herr_t H5HF__sect_row_serialize(const H5FS_section_class_t *cls, const H5FS_section_info_t *sect,
                                       uint8_t *buf);
static H5FS_section_info_t *H5HF__sect_row_deserialize(const H5FS_section_class_t *cls, const uint8_t *buf,
                                                       haddr_t sect_addr, hsize_t sect_size,
                                                       unsigned *des_flags);
static htri_t H5HF__sect_row_can_merge(const H5FS_section_info_t *sect1, const H5FS_section_info_t *sect2,
                                       void *udata);
static herr_t H5HF__sect_row_merge(H5FS_section_info_t **sect1, H5FS_section_info_t *sect2, void *udata);
static htri_t H5HF__sect_row_can_shrink(const H5FS_section_info_t *sect, void *udata);
static herr_t H5HF__sect_row_shrink(H5FS_section_info_t **sect, void *udata);
static herr_t H5HF__sect_row_free(H5FS_section_info_t *sect);
static herr_t H5HF__sect_row_valid(const H5FS_section_class_t *cls, const H5FS_section_info_t *sect);
static herr_t H5HF__sect_row_debug(const H5FS_section_info_t *sect, FILE *stream, int indent, int fwidth);

/* 'indirect' section routines */
static H5HF_free_section_t *H5HF__sect_indirect_new(H5HF_hdr_t *hdr, haddr_t sect_off, hsize_t sect_size,
                                                    H5HF_indirect_t *iblock, hsize_t iblock_off, unsigned row,
                                                    unsigned col, unsigned nentries);
static herr_t H5HF__sect_indirect_init_rows(H5HF_hdr_t *hdr, H5HF_free_section_t *sect, bool first_child,
                                            H5HF_free_section_t **first_row_sect, unsigned space_flags,
                                            unsigned start_row, unsigned start_col, unsigned end_row,
                                            unsigned end_col);
static H5HF_free_section_t *H5HF__sect_indirect_for_row(H5HF_hdr_t *hdr, H5HF_indirect_t *iblock,
                                                        H5HF_free_section_t *row_sect);
static herr_t               H5HF__sect_indirect_decr(H5HF_free_section_t *sect);
static herr_t               H5HF__sect_indirect_revive_row(H5HF_hdr_t *hdr, H5HF_free_section_t *sect);
static herr_t               H5HF__sect_indirect_revive(H5HF_hdr_t *hdr, H5HF_free_section_t *sect,
                                                       H5HF_indirect_t *sect_iblock);
static herr_t               H5HF__sect_indirect_reduce_row(H5HF_hdr_t *hdr, H5HF_free_section_t *row_sect,
                                                           bool *alloc_from_start);
static herr_t H5HF__sect_indirect_reduce(H5HF_hdr_t *hdr, H5HF_free_section_t *sect, unsigned child_entry);
static herr_t H5HF__sect_indirect_first(H5HF_hdr_t *hdr, H5HF_free_section_t *sect);
static bool   H5HF__sect_indirect_is_first(H5HF_free_section_t *sect);
static H5HF_indirect_t     *H5HF__sect_indirect_get_iblock(H5HF_free_section_t *sect);
static hsize_t              H5HF__sect_indirect_iblock_off(const H5HF_free_section_t *sect);
static H5HF_free_section_t *H5HF__sect_indirect_top(H5HF_free_section_t *sect);
static herr_t               H5HF__sect_indirect_merge_row(H5HF_hdr_t *hdr, H5HF_free_section_t *sect1,
                                                          H5HF_free_section_t *sect2);
static herr_t               H5HF__sect_indirect_build_parent(H5HF_hdr_t *hdr, H5HF_free_section_t *sect);
static herr_t               H5HF__sect_indirect_shrink(H5HF_hdr_t *hdr, H5HF_free_section_t *sect);
static herr_t H5HF__sect_indirect_serialize(H5HF_hdr_t *hdr, const H5HF_free_section_t *sect, uint8_t *buf);
static H5FS_section_info_t *H5HF__sect_indirect_deserialize(H5HF_hdr_t *hdr, const uint8_t *buf,
                                                            haddr_t sect_addr, hsize_t sect_size,
                                                            unsigned *des_flags);
static herr_t               H5HF__sect_indirect_free(H5HF_free_section_t *sect);
static herr_t               H5HF__sect_indirect_valid(const H5HF_hdr_t *hdr, const H5HF_free_section_t *sect);
static herr_t H5HF__sect_indirect_debug(const H5HF_free_section_t *sect, FILE *stream, int indent,
                                        int fwidth);

/* 'indirect' section callbacks */
static herr_t H5HF__sect_indirect_init_cls(H5FS_section_class_t *cls, void *udata);
static herr_t H5HF__sect_indirect_term_cls(H5FS_section_class_t *cls);

/*********************/
/* Package Variables */
/*********************/

/* Class info for "single" free space sections */
H5FS_section_class_t H5HF_FSPACE_SECT_CLS_SINGLE[1] = {{
    /* Class variables */
    H5HF_FSPACE_SECT_SINGLE, /* Section type                 */
    0,                       /* Extra serialized size        */
    H5FS_CLS_MERGE_SYM,      /* Class flags                  */
    NULL,                    /* Class private info           */

    /* Class methods */
    NULL, /* Initialize section class     */
    NULL, /* Terminate section class      */

    /* Object methods */
    H5HF__sect_single_add,         /* Add section                  */
    NULL,                          /* Serialize section            */
    H5HF__sect_single_deserialize, /* Deserialize section          */
    H5HF__sect_single_can_merge,   /* Can sections merge?          */
    H5HF__sect_single_merge,       /* Merge sections               */
    H5HF__sect_single_can_shrink,  /* Can section shrink container?*/
    H5HF__sect_single_shrink,      /* Shrink container w/section   */
    H5HF__sect_single_free,        /* Free section                 */
    H5HF__sect_single_valid,       /* Check validity of section    */
    NULL,                          /* Split section node for alignment */
    NULL,                          /* Dump debugging for section   */
}};

/* Class info for "first row" free space sections */
/* (Same as "normal" row sections, except they also act as a proxy for the
 *      underlying indirect section
 */
H5FS_section_class_t H5HF_FSPACE_SECT_CLS_FIRST_ROW[1] = {{
    /* Class variables */
    H5HF_FSPACE_SECT_FIRST_ROW, /* Section type                 */
    0,                          /* Extra serialized size        */
    H5FS_CLS_MERGE_SYM,         /* Class flags                  */
    NULL,                       /* Class private info           */

    /* Class methods */
    H5HF__sect_row_init_cls, /* Initialize section class     */
    H5HF__sect_row_term_cls, /* Terminate section class      */

    /* Object methods */
    NULL,                       /* Add section                  */
    H5HF__sect_row_serialize,   /* Serialize section            */
    H5HF__sect_row_deserialize, /* Deserialize section          */
    H5HF__sect_row_can_merge,   /* Can sections merge?          */
    H5HF__sect_row_merge,       /* Merge sections               */
    H5HF__sect_row_can_shrink,  /* Can section shrink container?*/
    H5HF__sect_row_shrink,      /* Shrink container w/section   */
    H5HF__sect_row_free,        /* Free section                 */
    H5HF__sect_row_valid,       /* Check validity of section    */
    NULL,                       /* Split section node for alignment */
    H5HF__sect_row_debug,       /* Dump debugging for section   */
}};

/* Class info for "normal row" free space sections */
H5FS_section_class_t H5HF_FSPACE_SECT_CLS_NORMAL_ROW[1] = {{
    /* Class variables */
    H5HF_FSPACE_SECT_NORMAL_ROW,                                  /* Section type                 */
    0,                                                            /* Extra serialized size        */
    H5FS_CLS_MERGE_SYM | H5FS_CLS_SEPAR_OBJ | H5FS_CLS_GHOST_OBJ, /* Class flags                  */
    NULL,                                                         /* Class private info           */

    /* Class methods */
    H5HF__sect_row_init_cls, /* Initialize section class     */
    H5HF__sect_row_term_cls, /* Terminate section class      */

    /* Object methods */
    NULL,                 /* Add section                  */
    NULL,                 /* Serialize section            */
    NULL,                 /* Deserialize section          */
    NULL,                 /* Can sections merge?          */
    NULL,                 /* Merge sections               */
    NULL,                 /* Can section shrink container?*/
    NULL,                 /* Shrink container w/section   */
    H5HF__sect_row_free,  /* Free section                 */
    H5HF__sect_row_valid, /* Check validity of section    */
    NULL,                 /* Split section node for alignment */
    H5HF__sect_row_debug, /* Dump debugging for section   */
}};

/* Class info for "indirect" free space sections */
/* (No object callbacks necessary - objects of this class should never be in
 *      section manager)
 */
H5FS_section_class_t H5HF_FSPACE_SECT_CLS_INDIRECT[1] = {{
    /* Class variables */
    H5HF_FSPACE_SECT_INDIRECT,               /* Section type                 */
    0,                                       /* Extra serialized size        */
    H5FS_CLS_MERGE_SYM | H5FS_CLS_GHOST_OBJ, /* Class flags                  */
    NULL,                                    /* Class private info           */

    /* Class methods */
    H5HF__sect_indirect_init_cls, /* Initialize section class     */
    H5HF__sect_indirect_term_cls, /* Terminate section class      */

    /* Object methods */
    NULL, /* Add section                  */
    NULL, /* Serialize section            */
    NULL, /* Deserialize section          */
    NULL, /* Can sections merge?          */
    NULL, /* Merge sections               */
    NULL, /* Can section shrink container?*/
    NULL, /* Shrink container w/section   */
    NULL, /* Free section                 */
    NULL, /* Check validity of section    */
    NULL, /* Split section node for alignment */
    NULL, /* Dump debugging for section   */
}};

/* Declare a free list to manage the H5HF_free_section_t struct */
H5FL_DEFINE_STATIC(H5HF_free_section_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5FS__sect_init_cls
 *
 * Purpose:	Initialize the common class structure
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sect_init_cls(H5FS_section_class_t *cls, H5HF_hdr_t *hdr)
{
    H5HF_sect_private_t *cls_prvt;            /* Pointer to class private info */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(cls);
    assert(!cls->cls_private);

    /* Allocate & initialize the class-private (i.e. private shared) information
     * for this type of section
     */
    if (NULL == (cls_prvt = (H5HF_sect_private_t *)H5MM_malloc(sizeof(H5HF_sect_private_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
    cls_prvt->hdr    = hdr;
    cls->cls_private = cls_prvt;

    /* Increment reference count on heap header */
    if (H5HF__hdr_incr(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, FAIL, "can't increment reference count on shared heap header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sect_init_cls() */

/*-------------------------------------------------------------------------
 * Function:	H5FS__sect_term_cls
 *
 * Purpose:	Terminate the common class structure
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sect_term_cls(H5FS_section_class_t *cls)
{
    H5HF_sect_private_t *cls_prvt;            /* Pointer to class private info */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(cls);

    /* Get pointer to class private info */
    cls_prvt = (H5HF_sect_private_t *)cls->cls_private;

    /* Decrement reference count on heap header */
    if (H5HF__hdr_decr(cls_prvt->hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't decrement reference count on shared heap header");

    /* Free the class private information */
    cls->cls_private = H5MM_xfree(cls_prvt);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sect_term_cls() */

/*-------------------------------------------------------------------------
 * Function:	H5FS__sect_node_new
 *
 * Purpose:	Allocate a free space section node of a particular type
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static H5HF_free_section_t *
H5FS__sect_node_new(unsigned sect_type, haddr_t sect_addr, hsize_t sect_size, H5FS_section_state_t sect_state)
{
    H5HF_free_section_t *new_sect;         /* New section */
    H5HF_free_section_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(H5_addr_defined(sect_addr));

    /* Create free list section node */
    if (NULL == (new_sect = H5FL_MALLOC(H5HF_free_section_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                    "memory allocation failed for direct block free list section");

    /* Set the information passed in */
    new_sect->sect_info.addr = sect_addr;
    new_sect->sect_info.size = sect_size;

    /* Set the section's class & state */
    new_sect->sect_info.type  = sect_type;
    new_sect->sect_info.state = sect_state;

    /* Set return value */
    ret_value = new_sect;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sect_node_new() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_node_free
 *
 * Purpose:	Free a section node
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_node_free(H5HF_free_section_t *sect, H5HF_indirect_t *iblock)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(sect);

    /* Release indirect block, if there was one */
    if (iblock)
        if (H5HF__iblock_decr(iblock) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL,
                        "can't decrement reference count on section's indirect block");

    /* Release the section */
    sect = H5FL_FREE(H5HF_free_section_t, sect);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_node_free() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_single_new
 *
 * Purpose:	Create a new 'single' section and return it to the caller
 *
 * Return:	Pointer to new section on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5HF_free_section_t *
H5HF__sect_single_new(hsize_t sect_off, size_t sect_size, H5HF_indirect_t *parent, unsigned par_entry)
{
    H5HF_free_section_t *sect      = NULL; /* 'Single' free space section to add */
    H5HF_free_section_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(sect_size);

    /* Create free space section node */
    if (NULL ==
        (sect = H5FS__sect_node_new(H5HF_FSPACE_SECT_SINGLE, sect_off, (hsize_t)sect_size, H5FS_SECT_LIVE)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for single section");

    /* Set the 'single' specific fields */
    sect->u.single.parent = parent;
    if (sect->u.single.parent) {
        if (H5HF__iblock_incr(sect->u.single.parent) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, NULL,
                        "can't increment reference count on shared indirect block");
    } /* end if */
    sect->u.single.par_entry = par_entry;

    /* Set return value */
    ret_value = sect;

done:
    if (!ret_value && sect) {
        /* Release the section */
        sect = H5FL_FREE(H5HF_free_section_t, sect);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_single_new() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_single_locate_parent
 *
 * Purpose:	Locate the parent indirect block for a single section
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_single_locate_parent(H5HF_hdr_t *hdr, bool refresh, H5HF_free_section_t *sect)
{
    H5HF_indirect_t *sec_iblock;          /* Pointer to section indirect block */
    unsigned         sec_entry;           /* Entry within section indirect block */
    bool             did_protect;         /* Whether we protected the indirect block or not */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(hdr->man_dtable.curr_root_rows > 0);
    assert(sect);

    /* Look up indirect block containing direct blocks for range */
    if (H5HF__man_dblock_locate(hdr, sect->sect_info.addr, &sec_iblock, &sec_entry, &did_protect,
                                H5AC__READ_ONLY_FLAG) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTCOMPUTE, FAIL, "can't compute row & column of section");

    /* Increment reference count on indirect block that free section is in */
    if (H5HF__iblock_incr(sec_iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, FAIL, "can't increment reference count on shared indirect block");

    /* Check for refreshing existing parent information */
    if (refresh) {
        if (sect->u.single.parent) {
            /* Release hold on previous parent indirect block */
            if (H5HF__iblock_decr(sect->u.single.parent) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL,
                            "can't decrement reference count on section's indirect block");
        } /* end if */
    }     /* end if */

    /* Set the information for the section */
    sect->u.single.parent    = sec_iblock;
    sect->u.single.par_entry = sec_entry;

    /* Unlock indirect block */
    if (H5HF__man_iblock_unprotect(sec_iblock, H5AC__NO_FLAGS_SET, did_protect) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap indirect block");
    sec_iblock = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_single_locate_parent() */

/*-------------------------------------------------------------------------
 * Function:    H5HF__sect_single_revive
 *
 * Purpose:     Update the memory information for a 'single' free section
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__sect_single_revive(H5HF_hdr_t *hdr, H5HF_free_section_t *sect)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(sect);
    assert(sect->sect_info.state == H5FS_SECT_SERIALIZED);

    /* Check for root direct block */
    if (hdr->man_dtable.curr_root_rows == 0) {
        /* Set the information for the section */
        assert(H5_addr_defined(hdr->man_dtable.table_addr));
        sect->u.single.parent    = NULL;
        sect->u.single.par_entry = 0;
    } /* end if */
    else {
        /* Look up indirect block information for section */
        if (H5HF__sect_single_locate_parent(hdr, false, sect) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't get section's parent info");
    } /* end else */

    /* Section is "live" now */
    sect->sect_info.state = H5FS_SECT_LIVE;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_single_revive() */

/*-------------------------------------------------------------------------
 * Function:    H5HF__sect_single_dblock_info
 *
 * Purpose:     Retrieve the direct block information for a single section
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__sect_single_dblock_info(H5HF_hdr_t *hdr, const H5HF_free_section_t *sect, haddr_t *dblock_addr,
                              size_t *dblock_size)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(sect);
    assert(sect->sect_info.type == H5HF_FSPACE_SECT_SINGLE);
    assert(sect->sect_info.state == H5FS_SECT_LIVE);
    assert(dblock_addr);
    assert(dblock_size);

    /* Check for root direct block */
    if (hdr->man_dtable.curr_root_rows == 0) {
        /* Retrieve direct block info from heap header */
        assert(H5_addr_defined(hdr->man_dtable.table_addr));
        *dblock_addr = hdr->man_dtable.table_addr;
        *dblock_size = hdr->man_dtable.cparam.start_block_size;
    } /* end if */
    else {
        /* Retrieve direct block info from parent indirect block */
        *dblock_addr = sect->u.single.parent->ents[sect->u.single.par_entry].addr;
        *dblock_size =
            hdr->man_dtable.row_block_size[sect->u.single.par_entry / hdr->man_dtable.cparam.width];
    } /* end else */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__sect_single_dblock_info() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_single_reduce
 *
 * Purpose:	Reduce the size of a single section (possibly freeing it)
 *              and re-add it back to the free space manager for the heap
 *              (if it hasn't been freed)
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__sect_single_reduce(H5HF_hdr_t *hdr, H5HF_free_section_t *sect, size_t amt)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(sect);
    assert(sect->sect_info.type == H5HF_FSPACE_SECT_SINGLE);
    assert(sect->sect_info.state == H5FS_SECT_LIVE);

    /* Check for eliminating the section */
    if (sect->sect_info.size == amt) {
        /* Free single section */
        if (H5HF__sect_single_free((H5FS_section_info_t *)sect) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free single section node");
    } /* end if */
    else {
        /* Adjust information for section */
        sect->sect_info.addr += amt;
        sect->sect_info.size -= amt;

        /* Re-insert section node into heap's free space */
        if (H5HF__space_add(hdr, sect, 0) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't re-add single section to free space manager");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_single_reduce() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_single_full_dblock
 *
 * Purpose:	Checks if a single section covers the entire direct block
 *              that it resides in, and converts it to a row section if so
 *
 * Note:        Does not convert a single section to a row section if the
 *              single section is for a root direct block
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_single_full_dblock(H5HF_hdr_t *hdr, H5HF_free_section_t *sect)
{
    haddr_t dblock_addr;         /* Section's direct block's address */
    size_t  dblock_size;         /* Section's direct block's size */
    size_t  dblock_overhead;     /* Direct block's overhead */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect);
    assert(sect->sect_info.state == H5FS_SECT_LIVE);
    assert(hdr);

    /* Retrieve direct block address from section */
    if (H5HF__sect_single_dblock_info(hdr, sect, &dblock_addr, &dblock_size) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't retrieve direct block information");

    /* Check for section occupying entire direct block */
    /* (and not the root direct block) */
    dblock_overhead = H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr);
    if ((dblock_size - dblock_overhead) == sect->sect_info.size && hdr->man_dtable.curr_root_rows > 0) {
        H5HF_direct_t *dblock;         /* Pointer to direct block for section */
        bool           parent_removed; /* Whether the direct block parent was removed from the file */

        if (NULL == (dblock = H5HF__man_dblock_protect(hdr, dblock_addr, dblock_size, sect->u.single.parent,
                                                       sect->u.single.par_entry, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to load fractal heap direct block");
        assert(H5_addr_eq(dblock->block_off + dblock_overhead, sect->sect_info.addr));

        /* Convert 'single' section into 'row' section */
        if (H5HF__sect_row_from_single(hdr, sect, dblock) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTCONVERT, FAIL, "can't convert single section into row section");

        /* Destroy direct block */
        if (H5HF__man_dblock_destroy(hdr, dblock, dblock_addr, &parent_removed) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't release direct block");
        dblock = NULL;

        /* If the parent for this direct block was removed and the indirect
         *      section is still "live", switch it to the "serialized" state.
         */
        if (parent_removed && H5FS_SECT_LIVE == sect->u.row.under->sect_info.state)
            if (H5HF__sect_row_parent_removed(sect) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTUPDATE, FAIL, "can't update section info");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_single_full_dblock() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_single_add
 *
 * Purpose:	Perform any actions on section as it is added to free space
 *              manager
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_single_add(H5FS_section_info_t **_sect, unsigned *flags, void *_udata)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Don't need to check section if we are deserializing, because it should
     *  have already been checked when it was first added
     */
    if (!(*flags & H5FS_ADD_DESERIALIZING)) {
        H5HF_free_section_t **sect  = (H5HF_free_section_t **)_sect; /* Fractal heap free section */
        H5HF_sect_add_ud_t   *udata = (H5HF_sect_add_ud_t *)_udata;  /* User callback data */
        H5HF_hdr_t           *hdr   = udata->hdr;                    /* Fractal heap header */

        /* Sanity check */
        assert(sect);
        assert(hdr);

        /* Check if single section covers entire direct block it's in */
        /* (converts to row section possibly) */
        if (H5HF__sect_single_full_dblock(hdr, (*sect)) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTCONVERT, FAIL, "can't check/convert single section");

        /* Set the "returned space" flag if the single section was changed
         *      into a row section, so the "merging & shrinking" algorithm
         *      gets executed in the free space manager
         */
        if ((*sect)->sect_info.type != H5HF_FSPACE_SECT_SINGLE)
            *flags |= H5FS_ADD_RETURNED_SPACE;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_single_add() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_single_deserialize
 *
 * Purpose:	Deserialize a buffer into a "live" single section
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static H5FS_section_info_t *
H5HF__sect_single_deserialize(const H5FS_section_class_t H5_ATTR_UNUSED *cls,
                              const uint8_t H5_ATTR_UNUSED *buf, haddr_t sect_addr, hsize_t sect_size,
                              unsigned H5_ATTR_UNUSED *des_flags)
{
    H5HF_free_section_t *new_sect;         /* New section */
    H5FS_section_info_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(H5_addr_defined(sect_addr));
    assert(sect_size);

    /* Create free list section node */
    if (NULL ==
        (new_sect = H5FS__sect_node_new(H5HF_FSPACE_SECT_SINGLE, sect_addr, sect_size, H5FS_SECT_SERIALIZED)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "allocation failed for direct block free list section");

    /* Set return value */
    ret_value = (H5FS_section_info_t *)new_sect;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_single_deserialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_single_can_merge
 *
 * Purpose:	Can two sections of this type merge?
 *
 * Note:        Second section must be "after" first section
 *
 * Return:	Success:	non-negative (true/false)
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5HF__sect_single_can_merge(const H5FS_section_info_t *_sect1, const H5FS_section_info_t *_sect2,
                            void H5_ATTR_UNUSED *_udata)
{
    const H5HF_free_section_t *sect1 = (const H5HF_free_section_t *)_sect1; /* Fractal heap free section */
    const H5HF_free_section_t *sect2 = (const H5HF_free_section_t *)_sect2; /* Fractal heap free section */
    htri_t                     ret_value = false;                           /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(sect1);
    assert(sect2);
    assert(sect1->sect_info.type == sect2->sect_info.type); /* Checks "MERGE_SYM" flag */
    assert(H5_addr_lt(sect1->sect_info.addr, sect2->sect_info.addr));

    /* Check if second section adjoins first section */
    /* (This can only occur within a direct block, due to the direct block
     *  overhead at the beginning of a block, so no need to check if sections
     *  are actually within the same direct block)
     */
    if (H5_addr_eq(sect1->sect_info.addr + sect1->sect_info.size, sect2->sect_info.addr))
        HGOTO_DONE(true);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_single_can_merge() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_single_merge
 *
 * Purpose:	Merge two sections of this type
 *
 * Note:        Second section always merges into first node
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_single_merge(H5FS_section_info_t **_sect1, H5FS_section_info_t *_sect2, void *_udata)
{
    H5HF_free_section_t **sect1     = (H5HF_free_section_t **)_sect1; /* Fractal heap free section */
    H5HF_free_section_t  *sect2     = (H5HF_free_section_t *)_sect2;  /* Fractal heap free section */
    H5HF_sect_add_ud_t   *udata     = (H5HF_sect_add_ud_t *)_udata;   /* User callback data */
    H5HF_hdr_t           *hdr       = udata->hdr;                     /* Fractal heap header */
    herr_t                ret_value = SUCCEED;                        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect1);
    assert((*sect1)->sect_info.type == H5HF_FSPACE_SECT_SINGLE);
    assert(sect2);
    assert(sect2->sect_info.type == H5HF_FSPACE_SECT_SINGLE);
    assert(H5_addr_eq((*sect1)->sect_info.addr + (*sect1)->sect_info.size, sect2->sect_info.addr));

    /* Add second section's size to first section */
    (*sect1)->sect_info.size += sect2->sect_info.size;

    /* Get rid of second section */
    if (H5HF__sect_single_free((H5FS_section_info_t *)sect2) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free section node");

    /* Check to see if we should revive first section */
    if ((*sect1)->sect_info.state != H5FS_SECT_LIVE)
        if (H5HF__sect_single_revive(hdr, (*sect1)) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't revive single free section");

    /* Check if single section covers entire direct block it's in */
    /* (converts to row section possibly) */
    if (H5HF__sect_single_full_dblock(hdr, (*sect1)) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTCONVERT, FAIL, "can't check/convert single section");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_single_merge() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_single_can_shrink
 *
 * Purpose:	Can this section shrink the container?
 *
 * Note:        This isn't actually shrinking the heap (since that's already
 *              been done) as much as it's cleaning up _after_ the heap
 *              shrink.
 *
 * Return:	Success:	non-negative (true/false)
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5HF__sect_single_can_shrink(const H5FS_section_info_t *_sect, void *_udata)
{
    const H5HF_free_section_t *sect      = (const H5HF_free_section_t *)_sect; /* Fractal heap free section */
    H5HF_sect_add_ud_t        *udata     = (H5HF_sect_add_ud_t *)_udata;       /* User callback data */
    H5HF_hdr_t                *hdr       = udata->hdr;                         /* Fractal heap header */
    htri_t                     ret_value = false;                              /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(sect);

    /* Check for section occupying entire root direct block */
    /* (We shouldn't ever have a single section that occupies an entire
     *      direct block, unless it's in the root direct block (because it
     *      would have been converted into a row section, if there was an
     *      indirect block that covered it)
     */
    if (hdr->man_dtable.curr_root_rows == 0) {
        size_t dblock_size;     /* Section's direct block's size */
        size_t dblock_overhead; /* Direct block's overhead */

        dblock_size     = hdr->man_dtable.cparam.start_block_size;
        dblock_overhead = H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr);
        if ((dblock_size - dblock_overhead) == sect->sect_info.size)
            HGOTO_DONE(true);
    } /* end if */
    else {
        /* We shouldn't have a situation where the 'next block' iterator
         *      is moved before a direct block that still has objects within it.
         */
        assert(hdr->man_iter_off > sect->sect_info.addr);
        HGOTO_DONE(false);
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_single_can_shrink() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_single_shrink
 *
 * Purpose:	Shrink container with section
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_single_shrink(H5FS_section_info_t **_sect, void *_udata)
{
    H5HF_free_section_t **sect  = (H5HF_free_section_t **)_sect; /* Fractal heap free section */
    H5HF_sect_add_ud_t   *udata = (H5HF_sect_add_ud_t *)_udata;  /* User callback data */
    H5HF_hdr_t           *hdr   = udata->hdr;                    /* Fractal heap header */
    H5HF_direct_t        *dblock;                                /* Pointer to direct block for section */
    haddr_t               dblock_addr;                           /* Section's direct block's address */
    size_t                dblock_size;                           /* Section's direct block's size */
    herr_t                ret_value = SUCCEED;                   /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect);
    assert(*sect);
    assert((*sect)->sect_info.type == H5HF_FSPACE_SECT_SINGLE);

    /* Check to see if we should revive section */
    if ((*sect)->sect_info.state != H5FS_SECT_LIVE)
        if (H5HF__sect_single_revive(hdr, (*sect)) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't revive single free section");

    /* Retrieve direct block address from section */
    if (H5HF__sect_single_dblock_info(hdr, (*sect), &dblock_addr, &dblock_size) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't retrieve direct block information");

    /* Protect the direct block for the section */
    /* (should be a root direct block) */
    assert(dblock_addr == hdr->man_dtable.table_addr);
    if (NULL == (dblock = H5HF__man_dblock_protect(hdr, dblock_addr, dblock_size, (*sect)->u.single.parent,
                                                   (*sect)->u.single.par_entry, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to load fractal heap direct block");
    assert(H5_addr_eq(dblock->block_off + dblock_size, (*sect)->sect_info.addr + (*sect)->sect_info.size));

    /* Get rid of section */
    if (H5HF__sect_single_free((H5FS_section_info_t *)*sect) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free section node");

    /* Destroy direct block */
    if (H5HF__man_dblock_destroy(hdr, dblock, dblock_addr, NULL) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't release direct block");
    dblock = NULL;

    /* Indicate that the section has been released */
    *sect = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_single_shrink() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_single_free
 *
 * Purpose:	Free a 'single' section node
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__sect_single_free(H5FS_section_info_t *_sect)
{
    H5HF_free_section_t *sect      = (H5HF_free_section_t *)_sect; /* Pointer to section to free */
    H5HF_indirect_t     *parent    = NULL;                         /* Parent indirect block for section */
    herr_t               ret_value = SUCCEED;                      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect);

    /* Check for live reference to an indirect block */
    if (sect->sect_info.state == H5FS_SECT_LIVE)
        /* Get parent indirect block, if there was one */
        if (sect->u.single.parent)
            parent = sect->u.single.parent;

    /* Release the section */
    if (H5HF__sect_node_free(sect, parent) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free section node");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_single_free() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_single_valid
 *
 * Purpose:	Check the validity of a section
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_single_valid(const H5FS_section_class_t H5_ATTR_UNUSED *cls, const H5FS_section_info_t *_sect)
{
    const H5HF_free_section_t *sect = (const H5HF_free_section_t *)_sect; /* Pointer to section to check */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(sect);

    if (sect->sect_info.state == H5FS_SECT_LIVE) {
        /* Check if this section is not in a direct block that is the root direct block */
        /* (not enough information to check on a single section in a root direct block) */
        if (sect->u.single.parent != NULL) {
            H5HF_indirect_t             *iblock; /* Indirect block that section's direct block resides in */
            haddr_t                      dblock_addr;       /* Direct block address */
            size_t                       dblock_size;       /* Direct block size */
            unsigned                     dblock_status = 0; /* Direct block's status in the metadata cache */
            size_t H5_ATTR_NDEBUG_UNUSED dblock_overhead;   /* Direct block's overhead */
            herr_t H5_ATTR_NDEBUG_UNUSED status;            /* Generic status value */

            /* Sanity check settings for section's direct block's parent */
            iblock = sect->u.single.parent;
            assert(H5_addr_defined(iblock->ents[sect->u.single.par_entry].addr));

            /* Retrieve direct block address from section */
            status = H5HF__sect_single_dblock_info(iblock->hdr, (const H5HF_free_section_t *)sect,
                                                   &dblock_addr, &dblock_size);
            assert(status >= 0);
            assert(H5_addr_eq(iblock->ents[sect->u.single.par_entry].addr, dblock_addr));
            assert(dblock_size > 0);

            /* Check if the section is actually within the heap */
            assert(sect->sect_info.addr < iblock->hdr->man_iter_off);

            /* Check that the direct block has been merged correctly */
            dblock_overhead = H5HF_MAN_ABS_DIRECT_OVERHEAD(iblock->hdr);
            assert((sect->sect_info.size + dblock_overhead) < dblock_size);

            /* Check the direct block's status in the metadata cache */
            status = H5AC_get_entry_status(iblock->hdr->f, dblock_addr, &dblock_status);
            assert(status >= 0);

            /* If the direct block for the section isn't already protected,
             *  protect it here in order to check single section's sanity
             *  against it.
             */
            if (!(dblock_status & H5AC_ES__IS_PROTECTED)) {
                H5HF_direct_t *dblock; /* Direct block for section */

                /* Protect the direct block for the section */
                dblock = H5HF__man_dblock_protect(iblock->hdr, dblock_addr, dblock_size, iblock,
                                                  sect->u.single.par_entry, H5AC__READ_ONLY_FLAG);
                assert(dblock);

                /* Sanity check settings for section */
                assert(dblock_size == dblock->size);
                assert(dblock->size > sect->sect_info.size);
                assert(H5_addr_lt(dblock->block_off, sect->sect_info.addr));
                assert(H5_addr_ge((dblock->block_off + dblock->size),
                                  (sect->sect_info.addr + sect->sect_info.size)));

                /* Release direct block */
                status = H5AC_unprotect(iblock->hdr->f, H5AC_FHEAP_DBLOCK, dblock_addr, dblock,
                                        H5AC__NO_FLAGS_SET);
                assert(status >= 0);
            } /* end if */
        }     /* end if */
    }         /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__sect_single_valid() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_create
 *
 * Purpose:	Create a new 'row' section
 *
 * Return:	Success:	pointer to new section
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static H5HF_free_section_t *
H5HF__sect_row_create(haddr_t sect_off, hsize_t sect_size, bool is_first, unsigned row, unsigned col,
                      unsigned nentries, H5HF_free_section_t *under_sect)
{
    H5HF_free_section_t *sect      = NULL; /* 'Row' section created */
    H5HF_free_section_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect_size);
    assert(nentries);
    assert(under_sect);

    /* Create 'row' free space section node */
    /* ("inherits" underlying indirect section's state) */
    if (NULL == (sect = H5FS__sect_node_new(
                     (unsigned)(is_first ? H5HF_FSPACE_SECT_FIRST_ROW : H5HF_FSPACE_SECT_NORMAL_ROW),
                     sect_off, sect_size, under_sect->sect_info.state)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for row section");

    /* Set the 'row' specific fields */
    sect->u.row.under       = under_sect;
    sect->u.row.row         = row;
    sect->u.row.col         = col;
    sect->u.row.num_entries = nentries;
    sect->u.row.checked_out = false;

    /* Set return value */
    ret_value = sect;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_row_create() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_from_single
 *
 * Purpose:	Convert a 'single' section into a 'row' section
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_row_from_single(H5HF_hdr_t *hdr, H5HF_free_section_t *sect, H5HF_direct_t *dblock)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(sect);
    assert(dblock);

    /* Convert 'single' section information to 'row' section info */
    sect->sect_info.addr    = dblock->block_off;
    sect->sect_info.type    = H5HF_FSPACE_SECT_FIRST_ROW;
    sect->u.row.row         = dblock->par_entry / hdr->man_dtable.cparam.width;
    sect->u.row.col         = dblock->par_entry % hdr->man_dtable.cparam.width;
    sect->u.row.num_entries = 1;
    sect->u.row.checked_out = false;

    /* Create indirect section that underlies the row section */
    if (NULL == (sect->u.row.under = H5HF__sect_indirect_for_row(hdr, dblock->parent, sect)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTCREATE, FAIL, "serializing row section not supported yet");

    /* Release single section's hold on underlying indirect block */
    if (H5HF__iblock_decr(dblock->parent) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't decrement reference count on shared indirect block");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_row_from_single() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_revive
 *
 * Purpose:	Update the memory information for a 'row' free section
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__sect_row_revive(H5HF_hdr_t *hdr, H5HF_free_section_t *sect)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(sect);
    assert(sect->u.row.under);

    /* If the indirect section's iblock has been removed from the cache, but the
     * section is still marked as "live", switch it to the "serialized" state.
     */
    if ((H5FS_SECT_LIVE == sect->u.row.under->sect_info.state) &&
        (true == sect->u.row.under->u.indirect.u.iblock->removed_from_cache))
        if (H5HF__sect_row_parent_removed(sect) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTUPDATE, FAIL, "can't update section info");

    /* Pass along "revive" request to underlying indirect section */
    /* (which will mark this section as "live") */
    if (H5HF__sect_indirect_revive_row(hdr, sect->u.row.under) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTREVIVE, FAIL, "can't revive indirect section");
    assert(sect->sect_info.state == H5FS_SECT_LIVE);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_row_revive() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_reduce
 *
 * Purpose:	Reduce the size of a row section (possibly freeing it)
 *              and re-add it back to the free space manager for the heap
 *              (if it hasn't been freed)
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__sect_row_reduce(H5HF_hdr_t *hdr, H5HF_free_section_t *sect, unsigned *entry_p)
{
    bool   alloc_from_start;    /* Whether to allocate from the end of the row */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(sect);
    assert(sect->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW ||
           sect->sect_info.type == H5HF_FSPACE_SECT_NORMAL_ROW);
    assert(sect->sect_info.state == H5FS_SECT_LIVE);
    assert(entry_p);

    /* Mark the row as checked out from the free space manager */
    assert(sect->u.row.checked_out == false);
    sect->u.row.checked_out = true;

    /* Forward row section to indirect routines, to handle reducing underlying indirect section */
    alloc_from_start = false;
    if (H5HF__sect_indirect_reduce_row(hdr, sect, &alloc_from_start) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTSHRINK, FAIL, "can't reduce underlying section");

    /* Determine entry allocated */
    *entry_p = (sect->u.row.row * hdr->man_dtable.cparam.width) + sect->u.row.col;
    if (!alloc_from_start)
        *entry_p += (sect->u.row.num_entries - 1);

    /* Check for eliminating the section */
    if (sect->u.row.num_entries == 1) {
        /* Free row section */
        if (H5HF__sect_row_free((H5FS_section_info_t *)sect) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free row section node");
    } /* end if */
    else {
        /* Check whether to allocate from the beginning or end of the row */
        if (alloc_from_start) {
            /* Adjust section start */
            sect->sect_info.addr += hdr->man_dtable.row_block_size[sect->u.row.row];
            sect->u.row.col++;
        } /* end else */

        /* Adjust span of blocks covered */
        sect->u.row.num_entries--;

        /* Check the row back in */
        sect->u.row.checked_out = false;

        /* Add 'row' section back to free space list */
        if (H5HF__space_add(hdr, sect, 0) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't re-add indirect section to free space manager");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_row_reduce() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_first
 *
 * Purpose:	Make row a "first row"
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_row_first(H5HF_hdr_t *hdr, H5HF_free_section_t *sect)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(sect);
    assert(sect->sect_info.type == H5HF_FSPACE_SECT_NORMAL_ROW);

    /* If the row is already checked out from the free space manager, just
     *  change it's class directly and the free space manager will adjust when
     *  it is checked back in.
     */
    if (sect->u.row.checked_out)
        sect->sect_info.type = H5HF_FSPACE_SECT_FIRST_ROW;
    else
        /* Change row section to be the "first row" */
        if (H5HF__space_sect_change_class(hdr, sect, H5HF_FSPACE_SECT_FIRST_ROW) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTSET, FAIL, "can't set row section to be first row");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_row_first() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_get_iblock
 *
 * Purpose:	Retrieve the indirect block for a row section
 *
 * Return:	Pointer to indirect block on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5HF_indirect_t *
H5HF__sect_row_get_iblock(H5HF_free_section_t *sect)
{
    H5HF_indirect_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(sect);
    assert(sect->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW ||
           sect->sect_info.type == H5HF_FSPACE_SECT_NORMAL_ROW);
    assert(sect->sect_info.state == H5FS_SECT_LIVE);

    ret_value = H5HF__sect_indirect_get_iblock(sect->u.row.under);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_row_get_iblock() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_parent_removed
 *
 * Purpose:	Update the information for a row and its parent indirect
 *              when an indirect block is removed from the metadata cache.
 *
 * Return:	Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_row_parent_removed(H5HF_free_section_t *sect)
{
    hsize_t  tmp_iblock_off;      /* Indirect block offset for row */
    unsigned u;                   /* Local index value */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(sect);

    /* Get a copy of the indirect block's offset before decrementing refcount on it */
    tmp_iblock_off = sect->u.row.under->u.indirect.u.iblock->block_off;

    /* Decrement the refcount on the indirect block, since serialized sections don't hold a reference */
    if (H5HF__iblock_decr(sect->u.row.under->u.indirect.u.iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't decrement reference count on shared indirect block");

    /* Switch indirect block info to serialized form */
    /* (Overwrites iblock pointer in the indirect section) */
    sect->u.row.under->u.indirect.u.iblock_off   = tmp_iblock_off;
    sect->u.row.under->u.indirect.iblock_entries = 0;

    /* Loop over derived row sections and mark them all as 'live' now */
    for (u = 0; u < sect->u.row.under->u.indirect.dir_nrows; u++)
        sect->u.row.under->u.indirect.dir_rows[u]->sect_info.state = H5FS_SECT_SERIALIZED;

    /* Mark the indirect section as serialized now */
    sect->u.row.under->sect_info.state = H5FS_SECT_SERIALIZED;

    /* Mark the row section as serialized now */
    sect->sect_info.state = H5FS_SECT_SERIALIZED;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_row_parent_removed() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_init_cls
 *
 * Purpose:	Initialize the "row" section class structure
 *
 * Note:	Since 'row' sections are proxies for 'indirect' sections, this
 *              routine forwards call to 'indirect' class initialization
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_row_init_cls(H5FS_section_class_t *cls, void *_udata)
{
    H5HF_hdr_t *hdr       = (H5HF_hdr_t *)_udata; /* Fractal heap header */
    herr_t      ret_value = SUCCEED;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(cls);
    assert(hdr);

    /* Call common class initialization */
    if (H5FS__sect_init_cls(cls, hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize common section class");

    /* First row sections actually are proxies for indirection sections on disk */
    if (cls->type == H5HF_FSPACE_SECT_FIRST_ROW)
        cls->serial_size = H5HF_SECT_INDIRECT_SERIAL_SIZE(hdr);
    else
        cls->serial_size = 0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_row_init_cls() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_term_cls
 *
 * Purpose:	Terminate the "row" section class structure
 *
 * Note:	Since 'row' sections are proxies for 'indirect' sections, this
 *              routine forwards call to 'indirect' class termination
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_row_term_cls(H5FS_section_class_t *cls)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(cls);

    /* Call common class termination */
    if (H5FS__sect_term_cls(cls) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't terminate common section class");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_row_term_cls() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_serialize
 *
 * Purpose:	Serialize a "live" row section into a buffer
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_row_serialize(const H5FS_section_class_t *cls, const H5FS_section_info_t *_sect, uint8_t *buf)
{
    H5HF_hdr_t                *hdr; /* Fractal heap header */
    const H5HF_free_section_t *sect      = (const H5HF_free_section_t *)_sect;
    herr_t                     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(cls);
    assert(buf);
    assert(sect);
    assert(sect->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW);
    assert(sect->sect_info.addr == sect->u.row.under->sect_info.addr);

    /* Forward to indirect routine to serialize underlying section */
    hdr = ((H5HF_sect_private_t *)(cls->cls_private))->hdr;
    if (H5HF__sect_indirect_serialize(hdr, sect->u.row.under, buf) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTSERIALIZE, FAIL,
                    "can't serialize row section's underlying indirect section");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_row_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_deserialize
 *
 * Purpose:	Deserialize a buffer into a "live" row section
 *
 * Note:        Actually this routine just forwards to the 'indirect'
 *              deserialize routine, which creates the row section.
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static H5FS_section_info_t *
H5HF__sect_row_deserialize(const H5FS_section_class_t *cls, const uint8_t *buf, haddr_t sect_addr,
                           hsize_t sect_size, unsigned *des_flags)
{
    H5HF_hdr_t          *hdr;              /* Fractal heap header */
    H5FS_section_info_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(cls);
    assert(buf);
    assert(H5_addr_defined(sect_addr));
    assert(sect_size);

    /* Forward to indirect routine to deserialize underlying section */
    hdr = ((H5HF_sect_private_t *)(cls->cls_private))->hdr;
    if (NULL == (ret_value = H5HF__sect_indirect_deserialize(hdr, buf, sect_addr, sect_size, des_flags)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDECODE, NULL,
                    "can't deserialize row section's underlying indirect section");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_row_deserialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_can_merge
 *
 * Purpose:	Can two sections of this type merge?
 *
 * Note:        Second section must be "after" first section
 *
 * Return:	Success:	non-negative (true/false)
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5HF__sect_row_can_merge(const H5FS_section_info_t *_sect1, const H5FS_section_info_t *_sect2,
                         void H5_ATTR_UNUSED *_udata)
{
    const H5HF_free_section_t *sect1 = (const H5HF_free_section_t *)_sect1; /* Fractal heap free section */
    const H5HF_free_section_t *sect2 = (const H5HF_free_section_t *)_sect2; /* Fractal heap free section */
    H5HF_free_section_t       *top_indir_sect1, *top_indir_sect2; /* Top indirect section for each row */
    htri_t                     ret_value = false;                 /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(sect1);
    assert(sect1->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW);
    assert(sect2);
    assert(sect1->sect_info.type == sect2->sect_info.type); /* Checks "MERGE_SYM" flag */
    assert(H5_addr_lt(sect1->sect_info.addr, sect2->sect_info.addr));

    /* Get the top indirect section underlying each row */
    top_indir_sect1 = H5HF__sect_indirect_top(sect1->u.row.under);
    assert(top_indir_sect1);
    top_indir_sect2 = H5HF__sect_indirect_top(sect2->u.row.under);
    assert(top_indir_sect2);

    /* Check if second section shares the same underlying indirect block as
     *  the first section, but doesn't already have same underlying indirect
     *  section.
     */
    if (top_indir_sect1 != top_indir_sect2)
        if (H5HF__sect_indirect_iblock_off(sect1->u.row.under) ==
            H5HF__sect_indirect_iblock_off(sect2->u.row.under))
            /* Check if second section adjoins first section */
            if (H5_addr_eq((top_indir_sect1->sect_info.addr + top_indir_sect1->u.indirect.span_size),
                           top_indir_sect2->sect_info.addr))
                HGOTO_DONE(true);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_row_can_merge() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_merge
 *
 * Purpose:	Merge two sections of this type
 *
 * Note:        Second section always merges into first node
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_row_merge(H5FS_section_info_t **_sect1, H5FS_section_info_t *_sect2, void *_udata)
{
    H5HF_free_section_t **sect1     = (H5HF_free_section_t **)_sect1; /* Fractal heap free section */
    H5HF_free_section_t  *sect2     = (H5HF_free_section_t *)_sect2;  /* Fractal heap free section */
    H5HF_sect_add_ud_t   *udata     = (H5HF_sect_add_ud_t *)_udata;   /* User callback data */
    H5HF_hdr_t           *hdr       = udata->hdr;                     /* Fractal heap header */
    herr_t                ret_value = SUCCEED;                        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect1);
    assert((*sect1)->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW);
    assert(sect2);
    assert(sect2->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW);

    /* Check if second section is past end of "next block" iterator */
    if (sect2->sect_info.addr >= hdr->man_iter_off) {
        H5HF_free_section_t *top_indir_sect; /* Top indirect section for row */

        /* Get the top indirect section underlying second row section */
        top_indir_sect = H5HF__sect_indirect_top(sect2->u.row.under);

        /* Shrink away underlying indirect section */
        if (H5HF__sect_indirect_shrink(hdr, top_indir_sect) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTSHRINK, FAIL, "can't shrink underlying indirect section");
    } /* end if */
    else
        /* Merge rows' underlying indirect sections together */
        if (H5HF__sect_indirect_merge_row(hdr, (*sect1), sect2) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTMERGE, FAIL, "can't merge underlying indirect sections");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_row_merge() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_can_shrink
 *
 * Purpose:	Can this section shrink the container?
 *
 * Note:        This isn't actually shrinking the heap (since that's already
 *              been done) as much as it's cleaning up _after_ the heap
 *              shrink.
 *
 * Return:	Success:	non-negative (true/false)
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5HF__sect_row_can_shrink(const H5FS_section_info_t *_sect, void H5_ATTR_UNUSED *_udata)
{
    const H5HF_free_section_t *sect      = (const H5HF_free_section_t *)_sect; /* Fractal heap free section */
    H5HF_sect_add_ud_t        *udata     = (H5HF_sect_add_ud_t *)_udata;       /* User callback data */
    H5HF_hdr_t                *hdr       = udata->hdr;                         /* Fractal heap header */
    htri_t                     ret_value = false;                              /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(sect);
    assert(sect->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW);

    /* Check if section is past end of "next block" iterator */
    if (sect->sect_info.addr >= hdr->man_iter_off)
        HGOTO_DONE(true);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_row_can_shrink() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_shrink
 *
 * Purpose:	Shrink container with section
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_row_shrink(H5FS_section_info_t **_sect, void *_udata)
{
    H5HF_free_section_t **sect = (H5HF_free_section_t **)_sect;     /* Fractal heap free section */
    H5HF_free_section_t  *top_indir_sect;                           /* Top indirect section for row */
    H5HF_sect_add_ud_t   *udata     = (H5HF_sect_add_ud_t *)_udata; /* User callback data */
    H5HF_hdr_t           *hdr       = udata->hdr;                   /* Fractal heap header */
    herr_t                ret_value = SUCCEED;                      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect);
    assert(*sect);
    assert((*sect)->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW);

    /* Get the top indirect section underlying each row */
    top_indir_sect = H5HF__sect_indirect_top((*sect)->u.row.under);

    /* Shrink away underlying indirect section */
    if (H5HF__sect_indirect_shrink(hdr, top_indir_sect) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTSHRINK, FAIL, "can't shrink underlying indirect section");

    /* Indicate that the section has been released */
    *sect = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_row_shrink() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_free_real
 *
 * Purpose:	Free a 'row' section node
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_row_free_real(H5HF_free_section_t *sect)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(sect);

    /* Release the section */
    if (H5HF__sect_node_free(sect, NULL) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free section node");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_row_free_real() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_free
 *
 * Purpose:	Free a 'row' section node
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_row_free(H5FS_section_info_t *_sect)
{
    H5HF_free_section_t *sect      = (H5HF_free_section_t *)_sect; /* Pointer to section to free */
    herr_t               ret_value = SUCCEED;                      /* Return value */

    FUNC_ENTER_PACKAGE

    assert(sect);
    assert(sect->u.row.under);

    /* Decrement the ref. count on the row section's underlying indirect section */
    if (H5HF__sect_indirect_decr(sect->u.row.under) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't detach section node");

    /* Release the section */
    if (H5HF__sect_row_free_real(sect) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free section node");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_row_free() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_valid
 *
 * Purpose:	Check the validity of a section
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_row_valid(const H5FS_section_class_t *cls, const H5FS_section_info_t *_sect)
{
    H5HF_sect_private_t       *cls_prvt;                                  /* Pointer to class private info */
    const H5HF_hdr_t          *hdr;                                       /* Fractal heap header */
    const H5HF_free_section_t *sect = (const H5HF_free_section_t *)_sect; /* Pointer to section to check */
    const H5HF_free_section_t *indir_sect;    /* Pointer to underlying indirect section */
    unsigned H5_ATTR_NDEBUG_UNUSED indir_idx; /* Index of row in underlying indirect section's row array */

    FUNC_ENTER_PACKAGE_NOERR

    /* Basic sanity check */
    assert(cls);
    assert(sect);

    /* Retrieve class private information */
    cls_prvt = (H5HF_sect_private_t *)cls->cls_private;
    hdr      = cls_prvt->hdr;

    /* Sanity checking on the row */
    assert(sect->u.row.under);
    assert(sect->u.row.num_entries);
    assert(sect->u.row.checked_out == false);
    indir_sect = sect->u.row.under;
    indir_idx  = sect->u.row.row - indir_sect->u.indirect.row;
    assert(indir_sect->u.indirect.dir_rows[indir_idx] == sect);

    /* Check if the section is actually within the heap */
    assert(sect->sect_info.addr < hdr->man_iter_off);

    /* Different checking for different kinds of rows */
    if (sect->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW) {
        H5HF_free_section_t *top_indir_sect; /* Top indirect section for row */

        /* Some extra sanity checks on the row */
        assert(sect->u.row.row == indir_sect->u.indirect.row);

        /* Get the top indirect section underlying row */
        top_indir_sect = H5HF__sect_indirect_top(sect->u.row.under);

        /* Check that the row's underlying indirect section is valid */
        H5HF__sect_indirect_valid(hdr, top_indir_sect);
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__sect_row_valid() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_row_debug
 *
 * Purpose:	Dump debugging information about an row free space section
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_row_debug(const H5FS_section_info_t *_sect, FILE *stream, int indent, int fwidth)
{
    const H5HF_free_section_t *sect = (const H5HF_free_section_t *)_sect; /* Section to dump info */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(sect);

    /* Print indirect section information */
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Row:", sect->u.row.row);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Column:", sect->u.row.col);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Number of entries:", sect->u.row.num_entries);

    /* If this is a first row section display information about underlying indirect section */
    if (sect->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW) {
        /* Print indirect section header */
        fprintf(stream, "%*s%-*s\n", indent, "", fwidth, "Underlying indirect section:");

        H5HF__sect_indirect_debug(sect->u.row.under, stream, indent + 3, MAX(0, fwidth - 3));
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__sect_row_debug() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_iblock_off
 *
 * Purpose:	Get the offset of the indirect block for the section
 *
 * Return:	Offset of indirect block in "heap space" (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static hsize_t
H5HF__sect_indirect_iblock_off(const H5HF_free_section_t *sect)
{
    hsize_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(sect);

    ret_value = sect->sect_info.state == H5FS_SECT_LIVE ? sect->u.indirect.u.iblock->block_off
                                                        : sect->u.indirect.u.iblock_off;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_iblock_off() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_top
 *
 * Purpose:	Get the "top" indirect section
 *
 * Return:	Pointer to the top indirect section (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static H5HF_free_section_t *
H5HF__sect_indirect_top(H5HF_free_section_t *sect)
{
    H5HF_free_section_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(sect);

    if (sect->u.indirect.parent)
        ret_value = H5HF__sect_indirect_top(sect->u.indirect.parent);
    else
        ret_value = sect;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_top() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_init_cls
 *
 * Purpose:	Initialize the "indirect" class structure
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_init_cls(H5FS_section_class_t *cls, void *_udata)
{
    H5HF_hdr_t *hdr       = (H5HF_hdr_t *)_udata; /* Fractal heap header */
    herr_t      ret_value = SUCCEED;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(cls);
    assert(hdr);

    /* Call to common class initialization */
    if (H5FS__sect_init_cls(cls, hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize common section class");

    /* Set the size of all serialized objects of this class of sections */
    cls->serial_size = H5HF_SECT_INDIRECT_SERIAL_SIZE(hdr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_indirect_init_cls() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_term_cls
 *
 * Purpose:	Terminate the "indirect" class structure
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_term_cls(H5FS_section_class_t *cls)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(cls);

    /* Call common class termination */
    if (H5FS__sect_term_cls(cls) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't terminate common section class");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_indirect_term_cls() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_new
 *
 * Purpose:	Create a new 'indirect' section for other routines to finish
 *              initializing.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static H5HF_free_section_t *
H5HF__sect_indirect_new(H5HF_hdr_t *hdr, haddr_t sect_off, hsize_t sect_size, H5HF_indirect_t *iblock,
                        hsize_t iblock_off, unsigned row, unsigned col, unsigned nentries)
{
    H5HF_free_section_t *sect      = NULL; /* 'Indirect' free space section to add */
    H5HF_free_section_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(nentries);

    /* Create free space section node */
    if (NULL == (sect = H5FS__sect_node_new(H5HF_FSPACE_SECT_INDIRECT, sect_off, sect_size,
                                            (iblock ? H5FS_SECT_LIVE : H5FS_SECT_SERIALIZED))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for indirect section");

    /* Set the 'indirect' specific fields */
    if (iblock) {
        sect->u.indirect.u.iblock       = iblock;
        sect->u.indirect.iblock_entries = hdr->man_dtable.cparam.width * sect->u.indirect.u.iblock->max_rows;
        if (H5HF__iblock_incr(sect->u.indirect.u.iblock) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, NULL,
                        "can't increment reference count on shared indirect block");
    } /* end if */
    else {
        sect->u.indirect.u.iblock_off   = iblock_off;
        sect->u.indirect.iblock_entries = 0;
    } /* end else */
    sect->u.indirect.row         = row;
    sect->u.indirect.col         = col;
    sect->u.indirect.num_entries = nentries;

    /* Compute span size of indirect section */
    sect->u.indirect.span_size = H5HF__dtable_span_size(&hdr->man_dtable, row, col, nentries);
    assert(sect->u.indirect.span_size > 0);

    /* This indirect section doesn't (currently) have a parent */
    sect->u.indirect.parent    = NULL;
    sect->u.indirect.par_entry = 0;

    /* Set return value */
    ret_value = sect;

done:
    if (!ret_value && sect) {
        /* Release the section */
        sect = H5FL_FREE(H5HF_free_section_t, sect);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_new() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_for_row
 *
 * Purpose:	Create the underlying indirect section for a new row section
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static H5HF_free_section_t *
H5HF__sect_indirect_for_row(H5HF_hdr_t *hdr, H5HF_indirect_t *iblock, H5HF_free_section_t *row_sect)
{
    H5HF_free_section_t *sect      = NULL; /* 'Indirect' free space section to add */
    H5HF_free_section_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(iblock);
    assert(row_sect);
    assert(row_sect->u.row.row < hdr->man_dtable.max_direct_rows);

    /* Create free space section node */
    if (NULL == (sect = H5HF__sect_indirect_new(hdr, row_sect->sect_info.addr, row_sect->sect_info.size,
                                                iblock, iblock->block_off, row_sect->u.row.row,
                                                row_sect->u.row.col, row_sect->u.row.num_entries)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, NULL, "can't create indirect section");

    /* Set # of direct rows covered */
    sect->u.indirect.dir_nrows = 1;

    /* Allocate space for the derived row sections */
    if (NULL ==
        (sect->u.indirect.dir_rows = (H5HF_free_section_t **)H5MM_malloc(sizeof(H5HF_free_section_t *))))
        HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, NULL, "allocation failed for row section pointer array");

    /* Attach the new row section to indirect section */
    sect->u.indirect.dir_rows[0] = row_sect;
    sect->u.indirect.rc          = 1;

    /* No indirect rows in current section */
    sect->u.indirect.indir_nents = 0;
    sect->u.indirect.indir_ents  = NULL;

    /* Set return value */
    ret_value = sect;

done:
    if (!ret_value && sect)
        if (H5HF__sect_indirect_free(sect) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTRELEASE, NULL, "can't free indirect section node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_for_row() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_init_rows
 *
 * Purpose:	Initialize the derived row sections for a newly created
 *              indirect section
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_init_rows(H5HF_hdr_t *hdr, H5HF_free_section_t *sect, bool first_child,
                              H5HF_free_section_t **first_row_sect, unsigned space_flags, unsigned start_row,
                              unsigned start_col, unsigned end_row, unsigned end_col)
{
    hsize_t  curr_off;            /* Offset of new section in "heap space" */
    size_t   dblock_overhead;     /* Direct block's overhead */
    unsigned row_entries;         /* # of entries in row */
    unsigned row_col;             /* Column within current row */
    unsigned curr_entry;          /* Current entry within indirect section */
    unsigned curr_indir_entry;    /* Current indirect entry within indirect section */
    unsigned curr_row;            /* Current row within indirect section */
    unsigned dir_nrows;           /* # of direct rows in indirect section */
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(sect);
    assert(sect->u.indirect.span_size > 0);

    /* Reset reference count for indirect section */
    /* (Also reset the direct & indirect row pointers */
    sect->u.indirect.rc         = 0;
    sect->u.indirect.dir_rows   = NULL;
    sect->u.indirect.indir_ents = NULL;

    /* Set up direct block information, if necessary */
    if (start_row < hdr->man_dtable.max_direct_rows) {
        unsigned max_direct_row; /* Max. direct row covered */

        /* Compute max. direct row covered by indirect section */
        max_direct_row = MIN(end_row, (hdr->man_dtable.max_direct_rows - 1));

        /* Compute # of direct rows covered */
        dir_nrows = (max_direct_row - start_row) + 1;

        /* Don't set the of direct rows in section yet, so sanity
         * checking works (enabled in free section manager, with H5FS_DEBUG
         * macro) correctly.
         */
        sect->u.indirect.dir_nrows = 0;

        /* Allocate space for the derived row sections */
        if (NULL == (sect->u.indirect.dir_rows =
                         (H5HF_free_section_t **)H5MM_malloc(sizeof(H5HF_free_section_t *) * dir_nrows)))
            HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL, "allocation failed for row section pointer array");
    } /* end if */
    else {
        /* No rows of direct blocks covered, reset direct row information */
        dir_nrows                  = 0;
        sect->u.indirect.dir_nrows = 0;
    } /* end else */

    /* Set up indirect block information, if necessary */
    if (end_row >= hdr->man_dtable.max_direct_rows) {
        unsigned indirect_start_row;   /* Row to start indirect entries on */
        unsigned indirect_start_col;   /* Column to start indirect entries on */
        unsigned indirect_start_entry; /* Index of starting indirect entry */
        unsigned indirect_end_entry;   /* Index of ending indirect entry */

        /* Compute starting indirect entry */
        if (start_row < hdr->man_dtable.max_direct_rows) {
            indirect_start_row = hdr->man_dtable.max_direct_rows;
            indirect_start_col = 0;
        } /* end if */
        else {
            indirect_start_row = start_row;
            indirect_start_col = start_col;
        } /* end else */
        indirect_start_entry = (indirect_start_row * hdr->man_dtable.cparam.width) + indirect_start_col;

        /* Compute ending indirect entry */
        indirect_end_entry = (end_row * hdr->man_dtable.cparam.width) + end_col;

        /* Compute # of indirect entries covered */
        sect->u.indirect.indir_nents = (indirect_end_entry - indirect_start_entry) + 1;

        /* Allocate space for the child indirect sections */
        if (NULL == (sect->u.indirect.indir_ents = (H5HF_free_section_t **)H5MM_malloc(
                         sizeof(H5HF_free_section_t *) * sect->u.indirect.indir_nents)))
            HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL, "allocation failed for indirect section pointer array");
    } /* end if */
    else {
        /* No indirect block entries covered, reset indirect row information */
        sect->u.indirect.indir_nents = 0;
    } /* end else */

    /* Set up initial row information */
    if (start_row == end_row)
        row_entries = (end_col - start_col) + 1;
    else
        row_entries = hdr->man_dtable.cparam.width - start_col;
    row_col = start_col;

    /* Loop over creating the sections covered by this indirect section */
    curr_off         = sect->sect_info.addr;
    curr_entry       = (start_row * hdr->man_dtable.cparam.width) + start_col;
    curr_row         = 0;
    curr_indir_entry = 0;
    dblock_overhead  = H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr);
    for (u = start_row; u <= end_row; u++, curr_row++) {
        if (u < hdr->man_dtable.max_direct_rows) {
            H5HF_free_section_t *row_sect = NULL; /* 'Row' free space section to add */

            /* Create 'row' free space section node */
            if (NULL == (row_sect = H5HF__sect_row_create(
                             curr_off, (hdr->man_dtable.row_block_size[u] - dblock_overhead), first_child, u,
                             row_col, row_entries, sect)))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTCREATE, FAIL, "creation failed for child row section");

            /* Add new row section to array for indirect section */
            sect->u.indirect.dir_rows[curr_row] = row_sect;

            /* Check to see if we should grab the first row section instead of adding it immediately */
            if (first_row_sect)
                *first_row_sect = row_sect;
            else
                /* Add new row section to free space manager for the heap */
                if (H5HF__space_add(hdr, row_sect, space_flags) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't add row section to free space");

            /* Increment reference count for underlying indirect section */
            sect->u.indirect.rc++;

            /* Advance the offset to the next section */
            curr_off += row_entries * hdr->man_dtable.row_block_size[u];

            /* Advance the current entry to the next row*/
            curr_entry += row_entries;

            /* Reset the 'first child' parameters */
            first_child    = false;
            first_row_sect = NULL;
        } /* end if */
        else {
            H5HF_indirect_t     *child_iblock;   /* Child indirect block */
            H5HF_free_section_t *child_sect;     /* Child 'indirect' section to add */
            unsigned             child_nrows;    /* Number of child rows in indirect blocks for this row */
            unsigned             child_nentries; /* Number of child entries in indirect blocks for this row */
            unsigned             v;              /* Local index variable */

            /* Compute info about row's indirect blocks for child section */
            child_nrows    = H5HF__dtable_size_to_rows(&hdr->man_dtable, hdr->man_dtable.row_block_size[u]);
            child_nentries = child_nrows * hdr->man_dtable.cparam.width;

            /* Add an indirect section for each indirect block in the row */
            for (v = 0; v < row_entries; v++) {
                bool did_protect = false; /* Whether we protected the indirect block or not */

                /* Try to get the child section's indirect block, if it's available */
                if (sect->sect_info.state == H5FS_SECT_LIVE) {
                    haddr_t child_iblock_addr; /* Child indirect block's address on disk */

                    /* Get the address of the child indirect block */
                    if (H5HF__man_iblock_entry_addr(sect->u.indirect.u.iblock, curr_entry,
                                                    &child_iblock_addr) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL,
                                    "unable to retrieve child indirect block's address");

                    /* If the child indirect block's address is defined, protect it */
                    if (H5_addr_defined(child_iblock_addr)) {
                        if (NULL == (child_iblock = H5HF__man_iblock_protect(
                                         hdr, child_iblock_addr, child_nrows, sect->u.indirect.u.iblock,
                                         curr_entry, false, H5AC__NO_FLAGS_SET, &did_protect)))
                            HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL,
                                        "unable to protect fractal heap indirect block");
                    } /* end if */
                    else
                        child_iblock = NULL;
                } /* end if */
                else
                    child_iblock = NULL;

                /* Create free space section node */
                if (NULL == (child_sect = H5HF__sect_indirect_new(hdr, curr_off, (hsize_t)0, child_iblock,
                                                                  curr_off, 0, 0, child_nentries)))
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't create indirect section");

                /* Initialize rows for new indirect section */
                if (H5HF__sect_indirect_init_rows(hdr, child_sect, first_child, first_row_sect, space_flags,
                                                  0, 0, (child_nrows - 1),
                                                  (hdr->man_dtable.cparam.width - 1)) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize indirect section");

                /* If we have a valid child indirect block, release it now */
                /* (will be pinned, if rows reference it) */
                if (child_iblock)
                    if (H5HF__man_iblock_unprotect(child_iblock, H5AC__NO_FLAGS_SET, did_protect) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL,
                                    "unable to release fractal heap indirect block");

                /* Attach child section to this section */
                child_sect->u.indirect.parent                 = sect;
                child_sect->u.indirect.par_entry              = curr_entry;
                sect->u.indirect.indir_ents[curr_indir_entry] = child_sect;
                sect->u.indirect.rc++;

                /* Advance the offset for the next section */
                curr_off += hdr->man_dtable.row_block_size[u];

                /* Advance to the next entry */
                curr_entry++;
                curr_indir_entry++;

                /* Reset the 'first child' parameters */
                first_child    = false;
                first_row_sect = NULL;
            } /* end for */
        }     /* end else */

        /* Compute the # of entries for the next row */
        if (u < (end_row - 1))
            row_entries = hdr->man_dtable.cparam.width;
        else
            row_entries = end_col + 1;

        /* Reset column for all other rows */
        row_col = 0;
    } /* end for */

    /* Set the final # of direct rows in section */
    sect->u.indirect.dir_nrows = dir_nrows;

    /* Make certain we've tracked the section's dependents correctly */
    assert(sect->u.indirect.rc == (sect->u.indirect.indir_nents + sect->u.indirect.dir_nrows));

done:
    if (ret_value < 0) {
        if (sect->u.indirect.indir_ents)
            H5MM_xfree(sect->u.indirect.indir_ents);
        if (sect->u.indirect.dir_rows)
            H5MM_xfree(sect->u.indirect.dir_rows);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_init_rows() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_add
 *
 * Purpose:	Add a new 'indirect' section to the free space manager for this
 *              heap
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__sect_indirect_add(H5HF_hdr_t *hdr, H5HF_indirect_t *iblock, unsigned start_entry, unsigned nentries)
{
    H5HF_free_section_t *sect           = NULL; /* 'Indirect' free space section to add */
    H5HF_free_section_t *first_row_sect = NULL; /* First row section in new indirect section */
    hsize_t              sect_off;              /* Offset of section in heap space */
    unsigned             start_row;             /* Start row in indirect block */
    unsigned             start_col;             /* Start column in indirect block */
    unsigned             end_entry;             /* End entry in indirect block */
    unsigned             end_row;               /* End row in indirect block */
    unsigned             end_col;               /* End column in indirect block */
    unsigned             u;                     /* Local index variable */
    herr_t               ret_value = SUCCEED;   /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(iblock);
    assert(nentries);

    /* Compute starting column & row */
    start_row = start_entry / hdr->man_dtable.cparam.width;
    start_col = start_entry % hdr->man_dtable.cparam.width;

    /* Compute end column & row */
    end_entry = (start_entry + nentries) - 1;
    end_row   = end_entry / hdr->man_dtable.cparam.width;
    end_col   = end_entry % hdr->man_dtable.cparam.width;

    /* Initialize information for rows skipped over */
    sect_off = iblock->block_off;
    for (u = 0; u < start_row; u++)
        sect_off += hdr->man_dtable.row_block_size[u] * hdr->man_dtable.cparam.width;
    sect_off += hdr->man_dtable.row_block_size[start_row] * start_col;

    /* Create free space section node */
    if (NULL == (sect = H5HF__sect_indirect_new(hdr, sect_off, (hsize_t)0, iblock, iblock->block_off,
                                                start_row, start_col, nentries)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't create indirect section");

    /* Initialize rows for new indirect section */
    if (H5HF__sect_indirect_init_rows(hdr, sect, true, &first_row_sect, H5FS_ADD_SKIP_VALID, start_row,
                                      start_col, end_row, end_col) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize indirect section");
    assert(first_row_sect);

    /* Now that underlying indirect section is consistent, add first row
     *  section to free space manager for the heap
     */
    if (H5HF__space_add(hdr, first_row_sect, H5FS_ADD_RETURNED_SPACE) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't add row section to free space");

done:
    if (ret_value < 0 && sect)
        if (H5HF__sect_indirect_free(sect) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free indirect section node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_add() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_decr
 *
 * Purpose:	Decrement ref. count on indirect section
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_decr(H5HF_free_section_t *sect)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(sect);
    assert(sect->u.indirect.rc);

    /* Decrement ref. count for indirect section */
    sect->u.indirect.rc--;

    /* If the indirect section's ref. count drops to zero, free the section */
    if (sect->u.indirect.rc == 0) {
        H5HF_free_section_t *par_sect; /* Parent indirect section */

        /* Preserve pointer to parent indirect section when freeing this section */
        par_sect = sect->u.indirect.parent;

        /* Free indirect section */
        if (H5HF__sect_indirect_free(sect) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free indirect section node");

        /* Decrement ref. count on indirect section's parent */
        if (par_sect)
            if (H5HF__sect_indirect_decr(par_sect) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL,
                            "can't decrement ref. count on parent indirect section");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_decr() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_revive_row
 *
 * Purpose:	Update the memory information for a 'indirect' free section
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_revive_row(H5HF_hdr_t *hdr, H5HF_free_section_t *sect)
{
    H5HF_indirect_t *sec_iblock;          /* Pointer to section indirect block */
    bool             did_protect;         /* Whether we protected the indirect block or not */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(sect);
    assert(sect->sect_info.state == H5FS_SECT_SERIALIZED);

    /* Look up indirect block containing indirect blocks for section */
    if (H5HF__man_dblock_locate(hdr, sect->sect_info.addr, &sec_iblock, NULL, &did_protect,
                                H5AC__READ_ONLY_FLAG) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTCOMPUTE, FAIL, "can't compute row & column of section");

    /* Review the section */
    if (H5HF__sect_indirect_revive(hdr, sect, sec_iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTREVIVE, FAIL, "can't revive indirect section");

done:
    /* Unlock indirect block */
    if (sec_iblock && H5HF__man_iblock_unprotect(sec_iblock, H5AC__NO_FLAGS_SET, did_protect) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap indirect block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_revive_row() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_revive
 *
 * Purpose:	Update the memory information for a 'indirect' free section
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_revive(H5HF_hdr_t *hdr, H5HF_free_section_t *sect, H5HF_indirect_t *sect_iblock)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(sect);
    assert(sect->sect_info.state == H5FS_SECT_SERIALIZED);
    assert(sect_iblock);

    /* Increment reference count on indirect block that free section is in */
    if (H5HF__iblock_incr(sect_iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't decrement reference count on shared indirect block");

    /* Set the pointer to the section's indirect block */
    sect->u.indirect.u.iblock = sect_iblock;

    /* Set the number of entries in the indirect block */
    sect->u.indirect.iblock_entries = hdr->man_dtable.cparam.width * sect->u.indirect.u.iblock->max_rows;

    /* Section is "live" now */
    sect->sect_info.state = H5FS_SECT_LIVE;

    /* Loop over derived row sections and mark them all as 'live' now */
    for (u = 0; u < sect->u.indirect.dir_nrows; u++)
        sect->u.indirect.dir_rows[u]->sect_info.state = H5FS_SECT_LIVE;

    /* Revive parent indirect section, if there is one */
    if (sect->u.indirect.parent && sect->u.indirect.parent->sect_info.state == H5FS_SECT_SERIALIZED)
        if (H5HF__sect_indirect_revive(hdr, sect->u.indirect.parent, sect->u.indirect.u.iblock->parent) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTREVIVE, FAIL, "can't revive indirect section");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_revive() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_reduce_row
 *
 * Purpose:	Remove a block from an indirect section (possibly freeing it)
 *              and re-add it back to the free space manager for the heap
 *              (if it hasn't been freed)
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_reduce_row(H5HF_hdr_t *hdr, H5HF_free_section_t *row_sect, bool *alloc_from_start)
{
    H5HF_free_section_t *sect;                /* Indirect section underlying row section */
    unsigned             row_start_entry;     /* Entry for first block covered in row section */
    unsigned             row_end_entry;       /* Entry for last block covered in row section */
    unsigned             row_entry;           /* Entry to allocate in row section */
    unsigned             start_entry;         /* Entry for first block covered */
    unsigned             start_row;           /* Start row in indirect block */
    unsigned             start_col;           /* Start column in indirect block */
    unsigned             end_entry;           /* Entry for last block covered */
    unsigned             end_row;             /* End row in indirect block */
    H5HF_free_section_t *peer_sect = NULL;    /* Peer indirect section */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(row_sect);

    /* Compute starting & ending information for row section */
    row_start_entry = (row_sect->u.row.row * hdr->man_dtable.cparam.width) + row_sect->u.row.col;
    row_end_entry   = (row_start_entry + row_sect->u.row.num_entries) - 1;

    /* Compute starting & ending information for indirect section */
    sect        = row_sect->u.row.under;
    start_row   = sect->u.indirect.row;
    start_col   = sect->u.indirect.col;
    start_entry = (start_row * hdr->man_dtable.cparam.width) + start_col;
    end_entry   = (start_entry + sect->u.indirect.num_entries) - 1;
    end_row     = end_entry / hdr->man_dtable.cparam.width;

    /* Additional sanity check */
    assert(sect->u.indirect.span_size > 0);
    assert(sect->u.indirect.iblock_entries > 0);
    assert(sect->u.indirect.dir_nrows > 0);
    assert(sect->u.indirect.dir_rows);
    assert(sect->u.indirect.dir_rows[(row_sect->u.row.row - start_row)] == row_sect);

    /* Check if we should allocate from end of indirect section */
    if (row_end_entry == end_entry && start_row != end_row) {
        *alloc_from_start = false;
        row_entry         = row_end_entry;
    } /* end if */
    else {
        *alloc_from_start = true;
        row_entry         = row_start_entry;
    } /* end else */

    /* Check if we have a parent section to be detached from */
    if (sect->u.indirect.parent) {
        bool is_first; /* Flag to indicate that this section is the first section in hierarchy */

        /* Check if this section is the first section */
        is_first = H5HF__sect_indirect_is_first(sect);

        /* Remove this indirect section from parent indirect section */
        if (H5HF__sect_indirect_reduce(hdr, sect->u.indirect.parent, sect->u.indirect.par_entry) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTSHRINK, FAIL, "can't reduce parent indirect section");
        sect->u.indirect.parent    = NULL;
        sect->u.indirect.par_entry = 0;

        /* If we weren't the first section, set "first row" for this indirect section */
        if (!is_first)
            if (H5HF__sect_indirect_first(hdr, sect) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't make new 'first row' for indirect section");
    } /* end if */

    /* Adjust indirect section's span size */
    sect->u.indirect.span_size -= row_sect->sect_info.size;

    /* Check how to adjust section for allocated entry */
    if (sect->u.indirect.num_entries > 1) {
        if (row_entry == start_entry) {
            /* Adjust section start */
            sect->sect_info.addr += hdr->man_dtable.row_block_size[sect->u.indirect.row];

            /* Adjust block coordinates of span */
            sect->u.indirect.col++;
            if (sect->u.indirect.col == hdr->man_dtable.cparam.width) {
                assert(row_sect->u.row.num_entries == 1);

                /* Adjust section's span information */
                sect->u.indirect.row++;
                sect->u.indirect.col = 0;

                /* Adjust direct row information */
                sect->u.indirect.dir_nrows--;

                /* Adjust direct row sections for indirect section */
                if (sect->u.indirect.dir_nrows > 0) {
                    assert(sect->u.indirect.dir_rows);
                    memmove(&sect->u.indirect.dir_rows[0], &sect->u.indirect.dir_rows[1],
                            sect->u.indirect.dir_nrows * sizeof(H5HF_free_section_t *));
                    assert(sect->u.indirect.dir_rows[0]);

                    /* Make new "first row" in indirect section */
                    if (row_sect->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW)
                        if (H5HF__sect_row_first(hdr, sect->u.indirect.dir_rows[0]) < 0)
                            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL,
                                        "can't make new 'first row' for indirect section");
                } /* end if */
                else {
                    /* Sanity check */
                    assert(sect->u.indirect.indir_nents > 0);
                    assert(sect->u.indirect.indir_ents);

                    /* Eliminate direct rows for this section */
                    sect->u.indirect.dir_rows = (H5HF_free_section_t **)H5MM_xfree(sect->u.indirect.dir_rows);

                    /* Make new "first row" in indirect section */
                    if (row_sect->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW)
                        if (H5HF__sect_indirect_first(hdr, sect->u.indirect.indir_ents[0]) < 0)
                            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL,
                                        "can't make new 'first row' for child indirect section");
                } /* end else */
            }     /* end if */

            /* Adjust number of entries covered */
            sect->u.indirect.num_entries--;
        } /* end if */
        else if (row_entry == end_entry) {
            unsigned new_end_row; /* New end row for entries */

            /* Sanity check */
            assert(sect->u.indirect.indir_nents == 0);
            assert(sect->u.indirect.indir_ents == NULL);

            /* Adjust number of entries covered */
            sect->u.indirect.num_entries--;

            /* Check for eliminating a direct row */
            new_end_row = ((start_entry + sect->u.indirect.num_entries) - 1) / hdr->man_dtable.cparam.width;
            assert(new_end_row <= end_row);
            if (new_end_row < end_row) {
                assert(new_end_row == (end_row - 1));
                sect->u.indirect.dir_nrows--;
            } /* end if */
        }     /* end if */
        else {
            H5HF_indirect_t *iblock;         /* Pointer to indirect block for this section */
            hsize_t          iblock_off;     /* Section's indirect block's offset in "heap space" */
            unsigned         peer_nentries;  /* Number of entries in new peer indirect section */
            unsigned         peer_dir_nrows; /* Number of direct rows in new peer indirect section */
            unsigned         new_start_row;  /* New starting row for current indirect section */
            unsigned         u;              /* Local index variable */

            /* Sanity checks */
            assert(row_sect->u.row.col == 0);
            assert(row_sect->u.row.row > 0);
            assert(row_sect->u.row.row < hdr->man_dtable.max_direct_rows);
            assert(row_sect->u.row.num_entries == hdr->man_dtable.cparam.width);
            assert(row_sect->sect_info.type == H5HF_FSPACE_SECT_NORMAL_ROW);

            /* Compute basic information about peer & current indirect sections */
            new_start_row  = row_sect->u.row.row;
            peer_nentries  = row_entry - start_entry;
            peer_dir_nrows = new_start_row - start_row;

            /* Get indirect block information for peer */
            if (sect->sect_info.state == H5FS_SECT_LIVE) {
                iblock     = sect->u.indirect.u.iblock;
                iblock_off = sect->u.indirect.u.iblock->block_off;
            } /* end if */
            else {
                iblock     = NULL;
                iblock_off = sect->u.indirect.u.iblock_off;
            } /* end else */

            /* Create peer indirect section */
            if (NULL ==
                (peer_sect = H5HF__sect_indirect_new(hdr, sect->sect_info.addr, sect->sect_info.size, iblock,
                                                     iblock_off, start_row, start_col, peer_nentries)))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't create indirect section");

            /* Set up direct row & indirect entry information for peer section */
            peer_sect->u.indirect.indir_nents = 0;
            peer_sect->u.indirect.indir_ents  = NULL;
            peer_sect->u.indirect.dir_nrows   = peer_dir_nrows;
            if (NULL == (peer_sect->u.indirect.dir_rows = (H5HF_free_section_t **)H5MM_malloc(
                             sizeof(H5HF_free_section_t *) * peer_dir_nrows)))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "allocation failed for row section pointer array");

            /* Transfer row sections between current & peer sections */
            H5MM_memcpy(&peer_sect->u.indirect.dir_rows[0], &sect->u.indirect.dir_rows[0],
                        (sizeof(H5HF_free_section_t *) * peer_dir_nrows));
            memmove(&sect->u.indirect.dir_rows[0], &sect->u.indirect.dir_rows[peer_dir_nrows],
                    (sizeof(H5HF_free_section_t *) * (sect->u.indirect.dir_nrows - peer_dir_nrows)));
            sect->u.indirect.dir_nrows -= peer_dir_nrows;
            assert(row_sect == sect->u.indirect.dir_rows[0]);

            /* Re-target transferred row sections to point to new underlying indirect section */
            for (u = 0; u < peer_dir_nrows; u++)
                peer_sect->u.indirect.dir_rows[u]->u.row.under = peer_sect;

            /* Change first row section in indirect section to be the "first row" */
            /* (But we don't have to tell the free space manager about it,
             *  because the row section is "checked out" from the free space
             * manager currently.
             */
            row_sect->sect_info.type = H5HF_FSPACE_SECT_FIRST_ROW;

            /* Adjust reference counts for current & peer sections */
            peer_sect->u.indirect.rc = peer_dir_nrows;
            sect->u.indirect.rc -= peer_dir_nrows;

            /* Transfer/update cached information about indirect block */
            peer_sect->u.indirect.iblock_entries = sect->u.indirect.iblock_entries;
            peer_sect->u.indirect.span_size      = row_sect->sect_info.addr - peer_sect->sect_info.addr;

            /* Update information for current section */
            sect->sect_info.addr = row_sect->sect_info.addr + hdr->man_dtable.row_block_size[new_start_row];
            sect->u.indirect.span_size -=
                peer_sect->u.indirect.span_size; /* (span for row section has already been removed) */
            sect->u.indirect.row = new_start_row;
            sect->u.indirect.col = row_sect->u.row.col + 1;
            sect->u.indirect.num_entries -=
                (peer_nentries + 1); /* Transferred entries, plus the entry allocated out of the row */

            /* Make certain we've tracked the sections' dependents correctly */
            assert(sect->u.indirect.rc == (sect->u.indirect.indir_nents + sect->u.indirect.dir_nrows));
            assert(peer_sect->u.indirect.rc ==
                   (peer_sect->u.indirect.indir_nents + peer_sect->u.indirect.dir_nrows));

            /* Reset the peer_sect variable, to indicate that it has been hooked into the data structures
             * correctly and shouldn't be freed */
            peer_sect = NULL;
        } /* end else */
    }     /* end if */
    else {
        /* Decrement count of entries & rows */
        sect->u.indirect.num_entries--;
        sect->u.indirect.dir_nrows--;
        assert(sect->u.indirect.dir_nrows == 0);

        /* Eliminate direct rows for this section */
        sect->u.indirect.dir_rows = (H5HF_free_section_t **)H5MM_xfree(sect->u.indirect.dir_rows);
    } /* end else */

done:
    /* Free allocated peer_sect.  Note that this is necessary for all failures until peer_sect is linked
     * into the main free space structures (via the direct blocks), and the reference count is updated. */
    if (peer_sect) {
        /* Sanity check - we should only be here if an error occurred */
        assert(ret_value < 0);

        if (H5HF__sect_indirect_free(peer_sect) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free indirect section node");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_reduce_row() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_reduce
 *
 * Purpose:	Reduce the size of a indirect section (possibly freeing it)
 *              and re-add it back to the free space manager for the heap
 *              (if it hasn't been freed)
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_reduce(H5HF_hdr_t *hdr, H5HF_free_section_t *sect, unsigned child_entry)
{
    unsigned             start_entry;         /* Entry for first block covered */
    unsigned             start_row;           /* Start row in indirect block */
    unsigned             start_col;           /* Start column in indirect block */
    unsigned             end_entry;           /* Entry for last block covered */
    unsigned             end_row;             /* End row in indirect block */
    H5HF_free_section_t *peer_sect = NULL;    /* Peer indirect section */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(sect);
    assert(sect->u.indirect.span_size > 0);
    assert(sect->u.indirect.iblock_entries > 0);

    /* Compute starting & ending information for indirect section */
    start_row   = sect->u.indirect.row;
    start_col   = sect->u.indirect.col;
    start_entry = (start_row * hdr->man_dtable.cparam.width) + start_col;
    end_entry   = (start_entry + sect->u.indirect.num_entries) - 1;
    end_row     = end_entry / hdr->man_dtable.cparam.width;

    /* Check how to adjust section for allocated entry */
    if (sect->u.indirect.num_entries > 1) {
        /* Check if we have a parent section to be detached from */
        if (sect->u.indirect.parent) {
            bool is_first; /* Flag to indicate that this section is the first section in hierarchy */

            /* Check if this section is the first section */
            is_first = H5HF__sect_indirect_is_first(sect);

            /* Reduce parent indirect section */
            if (H5HF__sect_indirect_reduce(hdr, sect->u.indirect.parent, sect->u.indirect.par_entry) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTSHRINK, FAIL, "can't reduce parent indirect section");
            sect->u.indirect.parent    = NULL;
            sect->u.indirect.par_entry = 0;

            /* If we weren't the first section, set "first row" for this indirect section */
            if (!is_first)
                if (H5HF__sect_indirect_first(hdr, sect) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL,
                                "can't make new 'first row' for indirect section");
        } /* end if */

        /* Check if we can allocate from start of indirect section */
        if (child_entry == start_entry) {
            /* Sanity check */
            assert(sect->u.indirect.dir_nrows == 0);
            assert(sect->u.indirect.dir_rows == NULL);
            assert(sect->u.indirect.indir_nents > 0);
            assert(sect->u.indirect.indir_ents);

            /* Adjust section start */
            sect->sect_info.addr += hdr->man_dtable.row_block_size[start_row];

            /* Adjust span of blocks covered */
            sect->u.indirect.col++;
            if (sect->u.indirect.col == hdr->man_dtable.cparam.width) {
                sect->u.indirect.row++;
                sect->u.indirect.col = 0;
            } /* end if */
            sect->u.indirect.num_entries--;
            sect->u.indirect.span_size -= hdr->man_dtable.row_block_size[start_row];

            /* Adjust indirect entry information */
            sect->u.indirect.indir_nents--;
            memmove(&sect->u.indirect.indir_ents[0], &sect->u.indirect.indir_ents[1],
                    sect->u.indirect.indir_nents * sizeof(H5HF_free_section_t *));
            assert(sect->u.indirect.indir_ents[0]);

            /* Make new "first row" in new first indirect child section */
            if (H5HF__sect_indirect_first(hdr, sect->u.indirect.indir_ents[0]) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL,
                            "can't make new 'first row' for child indirect section");
        } /* end if */
        else if (child_entry == end_entry) {
            /* Sanity check */
            assert(sect->u.indirect.indir_nents > 0);
            assert(sect->u.indirect.indir_ents);

            /* Adjust span of blocks covered */
            sect->u.indirect.num_entries--;
            sect->u.indirect.span_size -= hdr->man_dtable.row_block_size[end_row];

            /* Adjust indirect entry information */
            sect->u.indirect.indir_nents--;
            if (sect->u.indirect.indir_nents == 0)
                sect->u.indirect.indir_ents = (H5HF_free_section_t **)H5MM_xfree(sect->u.indirect.indir_ents);
        } /* end if */
        else {
            H5HF_indirect_t *iblock;         /* Pointer to indirect block for this section */
            hsize_t          iblock_off;     /* Section's indirect block's offset in "heap space" */
            haddr_t          peer_sect_addr; /* Address of new peer section in "heap space" */
            unsigned         peer_nentries;  /* Number of entries in new peer indirect section */
            unsigned         peer_start_row; /* Starting row for new peer indirect section */
            unsigned         peer_start_col; /* Starting column for new peer indirect section */
            unsigned         child_row;      /* Row where child entry is located */
            unsigned         new_nentries;   /* New number of entries for current indirect section */
            unsigned         u;              /* Local index variable */

            /* Sanity check */
            assert(sect->u.indirect.indir_nents > 0);
            assert(sect->u.indirect.indir_ents);

            /* Compute basic information about peer & current indirect sections */
            peer_nentries  = end_entry - child_entry;
            peer_start_row = (child_entry + 1) / hdr->man_dtable.cparam.width;
            peer_start_col = (child_entry + 1) % hdr->man_dtable.cparam.width;
            child_row      = child_entry / hdr->man_dtable.cparam.width;
            new_nentries   = sect->u.indirect.num_entries - (peer_nentries + 1);
            assert(child_row >= hdr->man_dtable.max_direct_rows);

            /* Get indirect block information for peer */
            if (sect->sect_info.state == H5FS_SECT_LIVE) {
                iblock     = sect->u.indirect.u.iblock;
                iblock_off = sect->u.indirect.u.iblock->block_off;
            } /* end if */
            else {
                iblock     = NULL;
                iblock_off = sect->u.indirect.u.iblock_off;
            } /* end else */

            /* Update the number of entries in current section & calculate it's span size */
            /* (Will use this to compute the section address for the peer section */
            sect->u.indirect.num_entries = new_nentries;
            sect->u.indirect.span_size   = H5HF__dtable_span_size(&hdr->man_dtable, sect->u.indirect.row,
                                                                  sect->u.indirect.col, new_nentries);
            assert(sect->u.indirect.span_size > 0);

            /* Compute address of peer indirect section */
            peer_sect_addr = sect->sect_info.addr;
            peer_sect_addr += sect->u.indirect.span_size;
            peer_sect_addr += hdr->man_dtable.row_block_size[child_row];

            /* Create peer indirect section */
            if (NULL == (peer_sect = H5HF__sect_indirect_new(hdr, peer_sect_addr, sect->sect_info.size,
                                                             iblock, iblock_off, peer_start_row,
                                                             peer_start_col, peer_nentries)))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't create indirect section");

            /* Set up direct row & indirect entry information for peer section */
            peer_sect->u.indirect.dir_nrows   = 0;
            peer_sect->u.indirect.dir_rows    = NULL;
            peer_sect->u.indirect.indir_nents = peer_nentries;
            if (NULL == (peer_sect->u.indirect.indir_ents = (H5HF_free_section_t **)H5MM_malloc(
                             sizeof(H5HF_free_section_t *) * peer_nentries)))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL,
                            "allocation failed for indirect section pointer array");

            /* Transfer child indirect sections between current & peer sections */
            H5MM_memcpy(&peer_sect->u.indirect.indir_ents[0],
                        &sect->u.indirect.indir_ents[sect->u.indirect.indir_nents - peer_nentries],
                        (sizeof(H5HF_free_section_t *) * peer_nentries));
            sect->u.indirect.indir_nents -= (peer_nentries + 1); /* Transferred blocks, plus child entry */

            /* Eliminate indirect entries for this section, if appropriate */
            if (sect->u.indirect.indir_nents == 0)
                sect->u.indirect.indir_ents = (H5HF_free_section_t **)H5MM_xfree(sect->u.indirect.indir_ents);

            /* Re-target transferred row sections to point to new underlying indirect section */
            for (u = 0; u < peer_nentries; u++)
                peer_sect->u.indirect.indir_ents[u]->u.indirect.parent = peer_sect;

            /* Adjust reference counts for current & peer sections */
            peer_sect->u.indirect.rc = peer_nentries;
            sect->u.indirect.rc -= peer_nentries;

            /* Transfer cached information about indirect block */
            peer_sect->u.indirect.iblock_entries = sect->u.indirect.iblock_entries;

            /* Make certain we've tracked the sections' dependents correctly */
            /* (Note modified on current section's ref. count, since we haven't
             *  detached the child section yet)
             */
            assert((sect->u.indirect.rc - 1) == (sect->u.indirect.indir_nents + sect->u.indirect.dir_nrows));
            assert(peer_sect->u.indirect.rc ==
                   (peer_sect->u.indirect.indir_nents + peer_sect->u.indirect.dir_nrows));

            /* Make new "first row" in peer section */
            if (H5HF__sect_indirect_first(hdr, peer_sect->u.indirect.indir_ents[0]) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL,
                            "can't make new 'first row' for peer indirect section");

            /* Reset the peer_sect variable, to indicate that it has been hooked into the data structures
             * correctly and shouldn't be freed */
            peer_sect = NULL;
        } /* end else */
    }     /* end if */
    else {
        /* Decrement count of entries & indirect entries */
        sect->u.indirect.num_entries--;
        sect->u.indirect.indir_nents--;
        assert(sect->u.indirect.indir_nents == 0);

        /* Eliminate indirect entries for this section */
        sect->u.indirect.indir_ents = (H5HF_free_section_t **)H5MM_xfree(sect->u.indirect.indir_ents);
    } /* end else */

    /* Decrement # of sections which depend on this row */
    /* (Must be last as section can be freed) */
    if (H5HF__sect_indirect_decr(sect) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't decrement section's ref. count ");

done:
    /* Free allocated peer_sect.  Note that this is necessary for all failures until peer_sect is linked
     * into the main free space structures (via the direct blocks), and the reference count is updated. */
    if (peer_sect) {
        /* Sanity check - we should only be here if an error occurred */
        assert(ret_value < 0);

        if (H5HF__sect_indirect_free(peer_sect) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free indirect section node");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_reduce() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_is_first
 *
 * Purpose:	Check if indirect section is first in all parents
 *
 * Return:	Non-negative (true/false) on success/<can't fail>
 *
 *-------------------------------------------------------------------------
 */
static bool
H5HF__sect_indirect_is_first(H5HF_free_section_t *sect)
{
    bool ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(sect);

    /* Recurse to parent */
    if (sect->u.indirect.parent) {
        if (sect->sect_info.addr == sect->u.indirect.parent->sect_info.addr)
            ret_value = H5HF__sect_indirect_is_first(sect->u.indirect.parent);
    } /* end if */
    else
        ret_value = true;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_is_first() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_first
 *
 * Purpose:	Make new 'first row' for indirect section
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_first(H5HF_hdr_t *hdr, H5HF_free_section_t *sect)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(sect);

    /* Check if this indirect section has direct block rows */
    if (sect->u.indirect.dir_nrows > 0) {
        /* Sanity checks */
        assert(sect->u.indirect.row == 0);
        assert(sect->u.indirect.col == 0);
        assert(sect->u.indirect.dir_rows);
        assert(sect->u.indirect.dir_rows[0]);

        /* Change first row section in indirect section to be the "first row" */
        if (H5HF__sect_row_first(hdr, sect->u.indirect.dir_rows[0]) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTSET, FAIL, "can't set row section to be first row");
    } /* end if */
    else {
        /* Sanity checks */
        assert(sect->u.indirect.indir_nents > 0);
        assert(sect->u.indirect.indir_ents);
        assert(sect->u.indirect.indir_ents[0]);

        /* Forward to first child indirect section */
        if (H5HF__sect_indirect_first(hdr, sect->u.indirect.indir_ents[0]) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTSET, FAIL, "can't set child indirect section to be first row");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_first() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_get_iblock
 *
 * Purpose:	Retrieve the indirect block for a indirect section
 *
 * Return:	Pointer to indirect block on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
static H5HF_indirect_t *
H5HF__sect_indirect_get_iblock(H5HF_free_section_t *sect)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(sect);
    assert(sect->sect_info.type == H5HF_FSPACE_SECT_INDIRECT);
    assert(sect->sect_info.state == H5FS_SECT_LIVE);

    FUNC_LEAVE_NOAPI(sect->u.indirect.u.iblock)
} /* end H5HF__sect_indirect_get_iblock() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_merge_row
 *
 * Purpose:	Merge two sections of this type
 *
 * Note:        Second section always merges into first node
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_merge_row(H5HF_hdr_t *hdr, H5HF_free_section_t *row_sect1, H5HF_free_section_t *row_sect2)
{
    H5HF_free_section_t *sect1, *sect2;          /* Indirect sections underlying row sections */
    unsigned             start_entry1;           /* Start entry for section #1 */
    unsigned             start_row1, start_col1; /* Starting row & column for section #1 */
    unsigned             end_entry1;             /* End entry for section #1 */
    unsigned             end_row1;               /* Ending row for section #1 */
    unsigned             start_row2;             /* Starting row for section #2 */
    bool                 merged_rows;            /* Flag to indicate that rows was merged together */
    unsigned             u;                      /* Local index variable */
    herr_t               ret_value = SUCCEED;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check parameters */
    assert(hdr);
    assert(row_sect1);
    assert(row_sect1->u.row.under);
    assert(row_sect2);
    assert(row_sect2->u.row.under);
    assert(row_sect2->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW);

    /* Set up indirect section information */
    sect1 = H5HF__sect_indirect_top(row_sect1->u.row.under);
    assert(sect1);
    sect2 = H5HF__sect_indirect_top(row_sect2->u.row.under);
    assert(sect2);

    /* Sanity check some assumptions about the indirect sections */
    assert(sect1->u.indirect.span_size > 0);
    assert(sect2->u.indirect.span_size > 0);

    /* Set up span information */
    start_row1   = sect1->u.indirect.row;
    start_col1   = sect1->u.indirect.col;
    start_entry1 = (start_row1 * hdr->man_dtable.cparam.width) + start_col1;
    end_entry1   = (start_entry1 + sect1->u.indirect.num_entries) - 1;
    end_row1     = end_entry1 / hdr->man_dtable.cparam.width;
    start_row2   = sect2->u.indirect.row;

    /* Check for direct sections in second section */
    /* (second indirect section can be parent of indirect section for second
     *          row, and thus have no row sections of it's own)
     */
    if (sect2->u.indirect.dir_nrows > 0) {
        hsize_t  sect1_iblock_off, sect2_iblock_off; /* Offset of indirect block underlying row section */
        unsigned new_dir_nrows1; /* New value for number of direct rows in first section */
        unsigned src_row2;       /* Source row for copying from second section */
        unsigned nrows_moved2;   /* Number of rows to move from second section to first */

        /* Sanity check child row assumptions */
        /* (second indirect section should be at top of equal or deeper
         *      hier. of row/indirect sections, so if second indirect section
         *      has child row sections, first indirect section _must_ have
         *      them also)
         */
        assert(sect1->u.indirect.dir_nrows > 0);
        assert(sect1->u.indirect.dir_rows);

        /* Get the offsets for the indirect blocks under the rows */
        if (H5FS_SECT_LIVE == row_sect1->u.row.under->sect_info.state)
            sect1_iblock_off = row_sect1->u.row.under->u.indirect.u.iblock->block_off;
        else
            sect1_iblock_off = row_sect1->u.row.under->u.indirect.u.iblock_off;
        if (H5FS_SECT_LIVE == row_sect2->u.row.under->sect_info.state)
            sect2_iblock_off = row_sect2->u.row.under->u.indirect.u.iblock->block_off;
        else
            sect2_iblock_off = row_sect2->u.row.under->u.indirect.u.iblock_off;

        /* Check for sections sharing a row in the same underlying indirect block */
        if (sect1_iblock_off == sect2_iblock_off && end_row1 == start_row2) {
            H5HF_free_section_t *last_row_sect1; /* Last row in first indirect section */

            /* Locate the last row section in first indirect section, if we don't already have it */
            if (row_sect1->u.row.row != end_row1)
                last_row_sect1 = sect1->u.indirect.dir_rows[sect1->u.indirect.dir_nrows - 1];
            else
                last_row_sect1 = row_sect1;
            assert(last_row_sect1);
            assert(last_row_sect1->u.row.row == end_row1);

            /* Adjust info for first row section, to absorb second row section */
            assert((last_row_sect1->u.row.col + last_row_sect1->u.row.num_entries) == row_sect2->u.row.col);
            last_row_sect1->u.row.num_entries += row_sect2->u.row.num_entries;

            /* Set up parameters for transfer of rows */
            src_row2       = 1;
            nrows_moved2   = sect2->u.indirect.dir_nrows - 1;
            new_dir_nrows1 = (sect1->u.indirect.dir_nrows + sect2->u.indirect.dir_nrows) - 1;

            /* Indicate that the rows were merged */
            merged_rows = true;
        } /* end if */
        else {

            /* Set up parameters for transfer of rows */
            src_row2       = 0;
            nrows_moved2   = sect2->u.indirect.dir_nrows;
            new_dir_nrows1 = sect1->u.indirect.dir_nrows + sect2->u.indirect.dir_nrows;

            /* Indicate that the rows were _not_ merged */
            merged_rows = false;
        } /* end else */

        /* Check if we need to move additional rows */
        if (nrows_moved2 > 0) {
            H5HF_free_section_t **new_dir_rows; /* Pointer to new array of direct row pointers */

            /* Extend the first section's row array */
            if (NULL == (new_dir_rows = (H5HF_free_section_t **)H5MM_realloc(
                             sect1->u.indirect.dir_rows, sizeof(H5HF_free_section_t *) * new_dir_nrows1)))
                HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL, "allocation failed for row section pointer array");
            sect1->u.indirect.dir_rows = new_dir_rows;

            /* Transfer the second section's rows to first section */
            H5MM_memcpy(&sect1->u.indirect.dir_rows[sect1->u.indirect.dir_nrows],
                        &sect2->u.indirect.dir_rows[src_row2],
                        (sizeof(H5HF_free_section_t *) * nrows_moved2));

            /* Re-target the row sections moved from second section */
            for (u = sect1->u.indirect.dir_nrows; u < new_dir_nrows1; u++)
                sect1->u.indirect.dir_rows[u]->u.row.under = sect1;

            /* Adjust reference counts to account for transferred rows */
            sect1->u.indirect.rc += nrows_moved2;
            sect2->u.indirect.rc -= nrows_moved2;

            /* Update information for first section */
            sect1->u.indirect.dir_nrows = new_dir_nrows1;
        } /* end if */
    }     /* end if */
    else
        /* Indicate that the rows were _not_ merged */
        merged_rows = false;

    /* Check for indirect sections in second section */
    if (sect2->u.indirect.indir_nents > 0) {
        unsigned new_indir_nents1; /* New value for number of indirect entries in first section */

        /* Some sanity checks on second indirect section */
        assert(sect2->u.indirect.rc > 0);
        assert(sect2->u.indirect.indir_nents > 0);
        assert(sect2->u.indirect.indir_ents);

        /* Set up parameters for transfer of entries */
        new_indir_nents1 = sect1->u.indirect.indir_nents + sect2->u.indirect.indir_nents;

        /* Check if first section can just take over second section's memory buffer */
        if (sect1->u.indirect.indir_ents == NULL) {
            sect1->u.indirect.indir_ents = sect2->u.indirect.indir_ents;
            sect2->u.indirect.indir_ents = NULL;
        } /* end if */
        else {
            H5HF_free_section_t **new_indir_ents; /* Pointer to new array of indirect entries */

            /* Extend the first section's entry array */
            if (NULL == (new_indir_ents = (H5HF_free_section_t **)H5MM_realloc(
                             sect1->u.indirect.indir_ents, sizeof(H5HF_free_section_t *) * new_indir_nents1)))
                HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL, "allocation failed for row section pointer array");
            sect1->u.indirect.indir_ents = new_indir_ents;

            /* Transfer the second section's entries to first section */
            H5MM_memcpy(&sect1->u.indirect.indir_ents[sect1->u.indirect.indir_nents],
                        &sect2->u.indirect.indir_ents[0],
                        (sizeof(H5HF_free_section_t *) * sect2->u.indirect.indir_nents));
        } /* end else */

        /* Re-target the child indirect sections moved from second section */
        for (u = sect1->u.indirect.indir_nents; u < new_indir_nents1; u++)
            sect1->u.indirect.indir_ents[u]->u.indirect.parent = sect1;

        /* Adjust reference counts for transferred child indirect sections */
        sect1->u.indirect.rc += sect2->u.indirect.indir_nents;
        sect2->u.indirect.rc -= sect2->u.indirect.indir_nents;

        /* Update information for first section */
        sect1->u.indirect.indir_nents = new_indir_nents1;
    } /* end if */

    /* Update information for first section */
    sect1->u.indirect.num_entries += sect2->u.indirect.num_entries;
    sect1->u.indirect.span_size += sect2->u.indirect.span_size;

    /* Make certain we've tracked the first section's dependents correctly */
    assert(sect1->u.indirect.rc == (sect1->u.indirect.indir_nents + sect1->u.indirect.dir_nrows));

    /* Wrap up, freeing or re-inserting second row section */
    /* (want this to be after the first indirect section is consistent again) */
    if (merged_rows) {
        /* Release second row section */
        /* (indirectly releases second indirect section, since all of it's
         *  other dependents are gone)
         */
        assert(sect2->u.indirect.rc == 1);
        if (H5HF__sect_row_free((H5FS_section_info_t *)row_sect2) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free row section");
    } /* end if */
    else {
        /* Decrement ref. count on second indirect section's parent */
        assert(sect2->u.indirect.rc == 0);
        if (sect2->u.indirect.parent)
            if (H5HF__sect_indirect_decr(sect2->u.indirect.parent) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL,
                            "can't decrement ref. count on parent indirect section");

        /* Free second indirect section */
        if (H5HF__sect_indirect_free(sect2) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free indirect section node");

        /* Re-add the second section's first row */
        /* (it's already been added to first indirect section, but it's been removed
         *  from the free space manager and needs to be re-added)
         */
        row_sect2->sect_info.type = H5HF_FSPACE_SECT_NORMAL_ROW;
        if (H5HF__space_add(hdr, row_sect2, H5FS_ADD_SKIP_VALID) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't re-add second row section to free space");
    } /* end else */

    /* Check if we can create parent indirect section for first section */
    /* (i.e. merged indirect sections cover an entire indirect block) */
    if (sect1->u.indirect.iblock_entries == sect1->u.indirect.num_entries) {
        /* Build parent section for fully populated indirect section */
        assert(sect1->u.indirect.parent == NULL);
        if (H5HF__sect_indirect_build_parent(hdr, sect1) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTCREATE, FAIL, "can't create parent for full indirect section");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_merge_row() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_build_parent
 *
 * Purpose:	Build a parent indirect section for a full indirect section
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_build_parent(H5HF_hdr_t *hdr, H5HF_free_section_t *sect)
{
    H5HF_indirect_t     *par_iblock;          /* Indirect block for parent section */
    H5HF_free_section_t *par_sect = NULL;     /* Parent indirect section */
    hsize_t              par_block_off;       /* Offset of parent's block */
    unsigned             par_row, par_col;    /* Row & column in parent indirect section */
    unsigned             par_entry;           /* Entry within parent indirect section */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check parameters */
    assert(hdr);
    assert(sect);
    assert(H5FS_SECT_LIVE == sect->sect_info.state);
    assert(sect->u.indirect.span_size > 0);
    assert(sect->u.indirect.iblock_entries > 0);
    assert(sect->u.indirect.iblock_entries == sect->u.indirect.num_entries);
    assert(sect->u.indirect.u.iblock);
    assert(sect->u.indirect.parent == NULL);

    /* Get information for creating parent indirect section */
    if (sect->u.indirect.u.iblock->parent) {
        par_entry     = sect->u.indirect.u.iblock->par_entry;
        par_iblock    = sect->u.indirect.u.iblock->parent;
        par_block_off = par_iblock->block_off;
    } /* end if */
    else {
        /* Retrieve the information for the parent block */
        if (H5HF__man_iblock_parent_info(hdr, sect->sect_info.addr, &par_block_off, &par_entry) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't get block entry");
        par_iblock = NULL;
    } /* end else */

    /* Compute row & column for block in parent */
    par_row = par_entry / hdr->man_dtable.cparam.width;
    par_col = par_entry % hdr->man_dtable.cparam.width;
    assert(par_row >= hdr->man_dtable.max_direct_rows);

    /* Create parent indirect section */
    if (NULL == (par_sect = H5HF__sect_indirect_new(hdr, sect->sect_info.addr, sect->sect_info.size,
                                                    par_iblock, par_block_off, par_row, par_col, 1)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't create indirect section");

    /* No rows of direct blocks covered in parent, reset direct row information */
    par_sect->u.indirect.dir_nrows = 0;
    par_sect->u.indirect.dir_rows  = NULL;

    /* Allocate space for the child indirect sections */
    par_sect->u.indirect.indir_nents = 1;
    if (NULL == (par_sect->u.indirect.indir_ents =
                     (H5HF_free_section_t **)H5MM_malloc(sizeof(H5HF_free_section_t *))))
        HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL, "allocation failed for indirect section pointer array");

    /* Attach sections together */
    sect->u.indirect.parent            = par_sect;
    sect->u.indirect.par_entry         = par_entry;
    par_sect->u.indirect.indir_ents[0] = sect;
    par_sect->u.indirect.rc            = 1;

done:
    if (ret_value < 0)
        if (par_sect && H5HF__sect_indirect_free(par_sect) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free indirect section node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_build_parent() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_shrink
 *
 * Purpose:	"Shrink" container w/section
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_shrink(H5HF_hdr_t *hdr, H5HF_free_section_t *sect)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check parameters */
    assert(hdr);
    assert(sect);

    /* Sanity check some assumptions about the indirect section */
    assert(sect->u.indirect.dir_nrows > 0 || sect->u.indirect.indir_nents > 0);

    /* Walk through direct rows, freeing them */
    for (u = 0; u < sect->u.indirect.dir_nrows; u++) {
        /* Remove the normal rows from free space manager */
        if (sect->u.indirect.dir_rows[u]->sect_info.type != H5HF_FSPACE_SECT_FIRST_ROW) {
            assert(sect->u.indirect.dir_rows[u]->sect_info.type == H5HF_FSPACE_SECT_NORMAL_ROW);
            if (H5HF__space_remove(hdr, sect->u.indirect.dir_rows[u]) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTREMOVE, FAIL, "can't remove section from heap free space");
        } /* end if */

        /* Release the row section */
        if (H5HF__sect_row_free_real(sect->u.indirect.dir_rows[u]) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free child section node");
    } /* end for */

    /* Walk through indirect entries, freeing them (recursively) */
    for (u = 0; u < sect->u.indirect.indir_nents; u++)
        if (H5HF__sect_indirect_shrink(hdr, sect->u.indirect.indir_ents[u]) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free child section node");

    /* Free the indirect section itself */
    if (H5HF__sect_indirect_free(sect) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free indirect section node");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__sect_indirect_shrink() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_serialize
 *
 * Purpose:	Serialize a "live" indirect section into a buffer
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_serialize(H5HF_hdr_t *hdr, const H5HF_free_section_t *sect, uint8_t *buf)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(sect);
    assert(buf);

    /* Check if this indirect section has a parent & forward if this section is first */
    if (sect->u.indirect.parent) {
        if (sect->sect_info.addr == sect->u.indirect.parent->sect_info.addr)
            if (H5HF__sect_indirect_serialize(hdr, sect->u.indirect.parent, buf) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTSERIALIZE, FAIL,
                            "can't serialize indirect section's parent indirect section");
    } /* end if */
    else {
        /* Indirect range's indirect block's block offset */
        if (sect->sect_info.state == H5FS_SECT_LIVE) {
            assert(sect->u.indirect.u.iblock);
            UINT64ENCODE_VAR(buf, sect->u.indirect.u.iblock->block_off, hdr->heap_off_size);
        } /* end if */
        else
            UINT64ENCODE_VAR(buf, sect->u.indirect.u.iblock_off, hdr->heap_off_size);

        /* Indirect range's row */
        UINT16ENCODE(buf, sect->u.indirect.row);

        /* Indirect range's column */
        UINT16ENCODE(buf, sect->u.indirect.col);

        /* Indirect range's # of entries */
        UINT16ENCODE(buf, sect->u.indirect.num_entries);
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_indirect_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_deserialize
 *
 * Purpose:	Deserialize a buffer into a "live" indirect section
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static H5FS_section_info_t *
H5HF__sect_indirect_deserialize(H5HF_hdr_t *hdr, const uint8_t *buf, haddr_t sect_addr, hsize_t sect_size,
                                unsigned *des_flags)
{
    H5HF_free_section_t *new_sect;         /* New indirect section */
    hsize_t              iblock_off;       /* Indirect block's offset */
    unsigned             start_row;        /* Indirect section's start row */
    unsigned             start_col;        /* Indirect section's start column */
    unsigned             nentries;         /* Indirect section's number of entries */
    unsigned             start_entry;      /* Start entry in indirect block */
    unsigned             end_entry;        /* End entry in indirect block */
    unsigned             end_row;          /* End row in indirect block */
    unsigned             end_col;          /* End column in indirect block */
    H5FS_section_info_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(hdr);
    assert(buf);
    assert(H5_addr_defined(sect_addr));
    assert(sect_size);

    /* Indirect range's indirect block's block offset */
    UINT64DECODE_VAR(buf, iblock_off, hdr->heap_off_size);

    /* Indirect section's row */
    UINT16DECODE(buf, start_row);

    /* Indirect section's column */
    UINT16DECODE(buf, start_col);

    /* Indirect section's # of entries */
    UINT16DECODE(buf, nentries);

    /* Create free space section node */
    if (NULL == (new_sect = H5HF__sect_indirect_new(hdr, sect_addr, sect_size, NULL, iblock_off, start_row,
                                                    start_col, nentries)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, NULL, "can't create indirect section");

    /* Compute start entry */
    start_entry = (start_row * hdr->man_dtable.cparam.width) + start_col;

    /* Compute end column & row */
    end_entry = (start_entry + nentries) - 1;
    end_row   = end_entry / hdr->man_dtable.cparam.width;
    end_col   = end_entry % hdr->man_dtable.cparam.width;

    /* Initialize rows for new indirect section */
    if (H5HF__sect_indirect_init_rows(hdr, new_sect, true, NULL, H5FS_ADD_DESERIALIZING,
                                      new_sect->u.indirect.row, new_sect->u.indirect.col, end_row,
                                      end_col) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, NULL, "can't initialize indirect section");

    /* Indicate that this section shouldn't be added to free space manager's list */
    *des_flags |= H5FS_DESERIALIZE_NO_ADD;

    /* Set return value */
    ret_value = (H5FS_section_info_t *)new_sect;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_indirect_deserialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_free
 *
 * Purpose:	Free a 'indirect' section node
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_free(H5HF_free_section_t *sect)
{
    H5HF_indirect_t *iblock    = NULL;    /* Indirect block for section */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(sect);

    /* Release the memory for tracking direct rows */
    sect->u.indirect.dir_rows = (H5HF_free_section_t **)H5MM_xfree(sect->u.indirect.dir_rows);

    /* Release the memory for tracking indirect entries */
    sect->u.indirect.indir_ents = (H5HF_free_section_t **)H5MM_xfree(sect->u.indirect.indir_ents);

    /* Check for live reference to an indirect block */
    if (sect->sect_info.state == H5FS_SECT_LIVE)
        /* Get indirect block, if there was one */
        if (sect->u.indirect.u.iblock)
            iblock = sect->u.indirect.u.iblock;

    /* Release the sections */
    if (H5HF__sect_node_free(sect, iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't free section node");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__sect_indirect_free() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_valid
 *
 * Purpose:	Check the validity of a section
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_valid(const H5HF_hdr_t *hdr, const H5HF_free_section_t *sect)
{
    unsigned start_row;   /* Row for first block covered */
    unsigned start_col;   /* Column for first block covered */
    unsigned start_entry; /* Entry for first block covered */
    unsigned end_row;     /* Row for last block covered */
    unsigned end_entry;   /* Entry for last block covered */
    unsigned u;           /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check arguments */
    assert(hdr);
    assert(sect);

    /* Compute starting entry, column & row */
    start_row   = sect->u.indirect.row;
    start_col   = sect->u.indirect.col;
    start_entry = (start_row * hdr->man_dtable.cparam.width) + start_col;

    /* Compute ending entry, column & row */
    end_entry = (start_entry + sect->u.indirect.num_entries) - 1;
    end_row   = end_entry / hdr->man_dtable.cparam.width;

    /* Sanity check any direct rows */
    if (sect->u.indirect.dir_nrows > 0) {
        unsigned dir_nrows;   /* Number of direct rows in section */
        unsigned max_dir_row; /* Maximum direct row in section */

        /* Check for indirect rows in section */
        if (end_row >= hdr->man_dtable.max_direct_rows)
            max_dir_row = hdr->man_dtable.max_direct_rows - 1;
        else
            max_dir_row = end_row;

        /* Iterate over direct rows, checking pointer references */
        dir_nrows = (max_dir_row - start_row) + 1;
        assert(dir_nrows == sect->u.indirect.dir_nrows);
        for (u = 0; u < dir_nrows; u++) {
            const H5HF_free_section_t H5_ATTR_NDEBUG_UNUSED *tmp_row_sect; /* Pointer to row section */

            tmp_row_sect = sect->u.indirect.dir_rows[u];
            assert(tmp_row_sect->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW ||
                   tmp_row_sect->sect_info.type == H5HF_FSPACE_SECT_NORMAL_ROW);
            assert(tmp_row_sect->u.row.under == sect);
            assert(tmp_row_sect->u.row.row == (start_row + u));
            if (u > 0) {
                const H5HF_free_section_t H5_ATTR_NDEBUG_UNUSED *tmp_row_sect2; /* Pointer to row section */

                tmp_row_sect2 = sect->u.indirect.dir_rows[u - 1];
                assert(tmp_row_sect2->u.row.row < tmp_row_sect->u.row.row);
                assert(H5_addr_lt(tmp_row_sect2->sect_info.addr, tmp_row_sect->sect_info.addr));
                assert(tmp_row_sect2->sect_info.size <= tmp_row_sect->sect_info.size);
            } /* end if */
        }     /* end for */
    }         /* end if */

    /* Sanity check any indirect entries */
    if (sect->u.indirect.indir_nents > 0) {
        /* Basic sanity checks */
        if (sect->sect_info.state == H5FS_SECT_LIVE) {
            assert(sect->u.indirect.iblock_entries);
            assert(sect->u.indirect.indir_nents <= sect->u.indirect.iblock_entries);
        } /* end if */
        assert(sect->u.indirect.indir_ents);

        /* Sanity check each child indirect section */
        for (u = 0; u < sect->u.indirect.indir_nents; u++) {
            const H5HF_free_section_t *tmp_child_sect; /* Pointer to child indirect section */

            tmp_child_sect = sect->u.indirect.indir_ents[u];
            assert(tmp_child_sect->sect_info.type == H5HF_FSPACE_SECT_INDIRECT);
            assert(tmp_child_sect->u.indirect.parent == sect);
            if (u > 0) {
                const H5HF_free_section_t H5_ATTR_NDEBUG_UNUSED
                    *tmp_child_sect2; /* Pointer to child indirect section */

                tmp_child_sect2 = sect->u.indirect.indir_ents[u - 1];
                assert(H5_addr_lt(tmp_child_sect2->sect_info.addr, tmp_child_sect->sect_info.addr));
            } /* end if */

            /* Recursively check child indirect section */
            H5HF__sect_indirect_valid(hdr, tmp_child_sect);
        } /* end for */
    }     /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__sect_indirect_valid() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__sect_indirect_debug
 *
 * Purpose:	Dump debugging information about an indirect free space section
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__sect_indirect_debug(const H5HF_free_section_t *sect, FILE *stream, int indent, int fwidth)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(sect);

    /* Print indirect section information */
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Row:", sect->u.indirect.row);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Column:", sect->u.indirect.col);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Number of entries:", sect->u.indirect.num_entries);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__sect_indirect_debug() */
