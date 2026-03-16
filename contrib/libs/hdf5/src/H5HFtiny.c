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
 * Created:     H5HFtiny.c
 *
 * Purpose:     Routines for "tiny" objects in fractal heap
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5HFmodule.h" /* This source code file is part of the H5HF module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions        */
#include "H5Eprivate.h"  /* Error handling           */
#include "H5HFpkg.h"     /* Fractal heaps            */
#include "H5MMprivate.h" /* Memory management			*/

/****************/
/* Local Macros */
/****************/

/* Tiny object length information */
#define H5HF_TINY_LEN_SHORT  16     /* Max. length able to be encoded in first heap ID byte */
#define H5HF_TINY_MASK_SHORT 0x0F   /* Mask for length in first heap ID byte                */
#define H5HF_TINY_MASK_EXT   0x0FFF /* Mask for length in two heap ID bytes                 */
#define H5HF_TINY_MASK_EXT_1 0x0F00 /* Mask for length in first byte of two heap ID bytes   */
#define H5HF_TINY_MASK_EXT_2 0x00FF /* Mask for length in second byte of two heap ID bytes  */

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/
static herr_t H5HF__tiny_op_real(H5HF_hdr_t *hdr, const uint8_t *id, H5HF_operator_t op, void *op_data);

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
 * Function:    H5HF__tiny_init
 *
 * Purpose:     Initialize information for tracking 'tiny' objects
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__tiny_init(H5HF_hdr_t *hdr)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(hdr);

    /* Compute information about 'tiny' objects for the heap */

    /* Check if tiny objects need an extra byte for their length */
    /* (account for boundary condition when length of an object would need an
     *  extra byte, but using that byte means that the extra length byte is
     *  unnecessary)
     */
    if ((hdr->id_len - 1) <= H5HF_TINY_LEN_SHORT) {
        hdr->tiny_max_len      = hdr->id_len - 1;
        hdr->tiny_len_extended = false;
    } /* end if */
    else if ((hdr->id_len - 1) == (H5HF_TINY_LEN_SHORT + 1)) {
        hdr->tiny_max_len      = H5HF_TINY_LEN_SHORT;
        hdr->tiny_len_extended = false;
    } /* end if */
    else {
        hdr->tiny_max_len      = hdr->id_len - 2;
        hdr->tiny_len_extended = true;
    } /* end else */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__tiny_init() */

/*-------------------------------------------------------------------------
 * Function:    H5HF__tiny_insert
 *
 * Purpose:     Pack a 'tiny' object in a heap ID
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__tiny_insert(H5HF_hdr_t *hdr, size_t obj_size, const void *obj, void *_id)
{
    uint8_t *id = (uint8_t *)_id; /* Pointer to ID buffer */
    size_t   enc_obj_size;        /* Encoded object size */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(obj_size <= hdr->tiny_max_len);
    assert(obj_size <= (H5HF_TINY_MASK_EXT + 1));
    assert(obj);
    assert(id);

    /* Adjust object's size for encoding it */
    enc_obj_size = obj_size - 1;

    /* Encode object into ID */
    if (!hdr->tiny_len_extended) {
        *id++ = (uint8_t)(H5HF_ID_VERS_CURR | H5HF_ID_TYPE_TINY | (enc_obj_size & H5HF_TINY_MASK_SHORT));
    } /* end if */
    else {
        *id++ =
            (uint8_t)(H5HF_ID_VERS_CURR | H5HF_ID_TYPE_TINY | ((enc_obj_size & H5HF_TINY_MASK_EXT_1) >> 8));
        *id++ = enc_obj_size & H5HF_TINY_MASK_EXT_2;
    } /* end else */

    H5MM_memcpy(id, obj, obj_size);
    memset(id + obj_size, 0, (hdr->id_len - ((size_t)1 + (size_t)hdr->tiny_len_extended + obj_size)));

    /* Update statistics about heap */
    hdr->tiny_size += obj_size;
    hdr->tiny_nobjs++;

    /* Mark heap header as modified */
    if (H5HF__hdr_dirty(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark heap header as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__tiny_insert() */

/*-------------------------------------------------------------------------
 * Function:    H5HF__tiny_get_obj_len
 *
 * Purpose:     Get the size of a 'tiny' object in a fractal heap
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__tiny_get_obj_len(H5HF_hdr_t *hdr, const uint8_t *id, size_t *obj_len_p)
{
    size_t enc_obj_size; /* Encoded object size */

    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(id);
    assert(obj_len_p);

    /* Check if 'tiny' object ID is in extended form, and retrieve encoded size */
    if (!hdr->tiny_len_extended)
        enc_obj_size = *id & H5HF_TINY_MASK_SHORT;
    else
        /* (performed in this odd way to avoid compiler bug on tg-login3 with
         *  gcc 3.2.2 - QAK)
         */
        enc_obj_size = (size_t) * (id + 1) | ((size_t)(*id & H5HF_TINY_MASK_EXT_1) << 8);

    /* Set the object's length */
    *obj_len_p = enc_obj_size + 1;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__tiny_get_obj_len() */

/*-------------------------------------------------------------------------
 * Function:    H5HF__tiny_op_real
 *
 * Purpose:     Internal routine to perform operation on 'tiny' object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__tiny_op_real(H5HF_hdr_t *hdr, const uint8_t *id, H5HF_operator_t op, void *op_data)
{
    size_t enc_obj_size;        /* Encoded object size */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(id);
    assert(op);

    /* Get the object's encoded length */
    /* H5HF__tiny_obj_len can't fail */
    ret_value = H5HF__tiny_get_obj_len(hdr, id, &enc_obj_size);

    /* Advance past flag byte(s) */
    if (!hdr->tiny_len_extended)
        id++;
    else {
        /* (performed in two steps to avoid compiler bug on tg-login3 with
         *  gcc 3.2.2 - QAK)
         */
        id++;
        id++;
    }

    /* Call the user's 'op' callback */
    if (op(id, enc_obj_size, op_data) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTOPERATE, FAIL, "application's callback failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__tiny_op_real() */

/*-------------------------------------------------------------------------
 * Function:    H5HF__tiny_read
 *
 * Purpose:     Read a 'tiny' object from the heap
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__tiny_read(H5HF_hdr_t *hdr, const uint8_t *id, void *obj)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(id);
    assert(obj);

    /* Call the internal 'op' routine */
    if (H5HF__tiny_op_real(hdr, id, H5HF__op_read, obj) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTOPERATE, FAIL, "unable to operate on heap object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__tiny_read() */

/*-------------------------------------------------------------------------
 * Function:    H5HF__tiny_op
 *
 * Purpose:     Operate directly on a 'tiny' object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__tiny_op(H5HF_hdr_t *hdr, const uint8_t *id, H5HF_operator_t op, void *op_data)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(id);
    assert(op);

    /* Call the internal 'op' routine routine */
    if (H5HF__tiny_op_real(hdr, id, op, op_data) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTOPERATE, FAIL, "unable to operate on heap object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__tiny_op() */

/*-------------------------------------------------------------------------
 * Function:    H5HF__tiny_remove
 *
 * Purpose:     Remove a 'tiny' object from the heap statistics
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__tiny_remove(H5HF_hdr_t *hdr, const uint8_t *id)
{
    size_t enc_obj_size;        /* Encoded object size */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(id);

    /* Get the object's encoded length */
    /* H5HF__tiny_obj_len can't fail */
    ret_value = H5HF__tiny_get_obj_len(hdr, id, &enc_obj_size);

    /* Update statistics about heap */
    hdr->tiny_size -= enc_obj_size;
    hdr->tiny_nobjs--;

    /* Mark heap header as modified */
    if (H5HF__hdr_dirty(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark heap header as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__tiny_remove() */
