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

/****************/
/* Module Setup */
/****************/

#define H5O_FRIEND      /*suppress error about including H5Opkg	  */
#include "H5SMmodule.h" /* This source code file is part of the H5SM module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Opkg.h"      /* Object Headers                       */
#include "H5SMpkg.h"     /* Shared object header messages        */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* Udata struct for calls to H5SM__compare_cb and H5SM__compare_iter_op*/
typedef struct H5SM_compare_udata_t {
    const H5SM_mesg_key_t *key; /* Key; compare this against stored message */
    H5O_msg_crt_idx_t      idx; /* Index of the message in the OH, if applicable */
    herr_t                 ret; /* Return value; set this to result of memcmp */
} H5SM_compare_udata_t;

/********************/
/* Local Prototypes */
/********************/
static herr_t H5SM__compare_cb(const void *obj, size_t obj_len, void *udata);
static herr_t H5SM__compare_iter_op(H5O_t *oh, H5O_mesg_t *mesg, unsigned sequence, unsigned *oh_modified,
                                    void *udata);

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
 * Function:	H5SM__compare_cb
 *
 * Purpose:	Callback for H5HF_op, used in H5SM__message_compare below.
 *              Determines whether the search key passed in in _UDATA is
 *              equal to OBJ or not.
 *
 *              Passes back the result in _UDATA->RET
 *
 * Return:	Negative on error, non-negative on success
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__compare_cb(const void *obj, size_t obj_len, void *_udata)
{
    H5SM_compare_udata_t *udata = (H5SM_compare_udata_t *)_udata;

    FUNC_ENTER_PACKAGE_NOERR

    /* If the encoding sizes are different, it's not the same object */
    if (udata->key->encoding_size > obj_len)
        udata->ret = 1;
    else if (udata->key->encoding_size < obj_len)
        udata->ret = -1;
    else
        /* Sizes are the same.  Return result of memcmp */
        udata->ret = memcmp(udata->key->encoding, obj, obj_len);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__compare_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5SM__compare_iter_op
 *
 * Purpose:	OH iteration callback to compare a key against a message in
 *              an OH
 *
 * Return:	0 if this is not the message we're searching for
 *              1 if this is the message we're searching for (with memcmp
 *                      result returned in udata)
 *              negative on error
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__compare_iter_op(H5O_t *oh, H5O_mesg_t *mesg /*in,out*/, unsigned sequence,
                      unsigned H5_ATTR_UNUSED *oh_modified, void *_udata /*in,out*/)
{
    H5SM_compare_udata_t *udata     = (H5SM_compare_udata_t *)_udata;
    herr_t                ret_value = H5_ITER_CONT;

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(oh);
    assert(mesg);
    assert(udata && udata->key);

    /* Check the creation index for this message */
    if (sequence == udata->idx) {
        size_t aligned_encoded_size = H5O_ALIGN_OH(oh, udata->key->encoding_size);

        /* Sanity check the message's length */
        assert(mesg->raw_size > 0);

        if (aligned_encoded_size > mesg->raw_size)
            udata->ret = 1;
        else if (aligned_encoded_size < mesg->raw_size)
            udata->ret = -1;
        else {
            /* Check if the message is dirty & flush it to the object header if so */
            if (mesg->dirty)
                if (H5O_msg_flush(udata->key->file, oh, mesg) < 0)
                    HGOTO_ERROR(H5E_SOHM, H5E_CANTENCODE, H5_ITER_ERROR,
                                "unable to encode object header message");

            assert(udata->key->encoding_size <= mesg->raw_size);
            udata->ret = memcmp(udata->key->encoding, mesg->raw, udata->key->encoding_size);
        } /* end else */

        /* Indicate that we found the message we were looking for */
        ret_value = H5_ITER_STOP;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__compare_iter_op() */

/*-------------------------------------------------------------------------
 * Function:	H5SM__message_compare
 *
 * Purpose:	Determine whether the search key rec1 represents a shared
 *              message that is equal to rec2 or not, and if not, whether
 *              rec1 is "greater than" or "less than" rec2.
 *
 * Return:	0 if rec1 == rec2
 *              Negative if rec1 < rec2
 *              Positive if rec1 > rec2
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM__message_compare(const void *rec1, const void *rec2, int *result)
{
    const H5SM_mesg_key_t *key       = (const H5SM_mesg_key_t *)rec1;
    const H5SM_sohm_t     *mesg      = (const H5SM_sohm_t *)rec2;
    herr_t                 ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* If the key has an fheap ID, we're looking for a message that's
     * already in the index; if the fheap ID matches, we've found the message
     * and can stop immediately.
     * Likewise, if the message has an OH location that is matched by the
     * message in the index, we've found the message.
     */
    if (mesg->location == H5SM_IN_HEAP && key->message.location == H5SM_IN_HEAP) {
        if (key->message.u.heap_loc.fheap_id.val == mesg->u.heap_loc.fheap_id.val) {
            *result = 0;
            HGOTO_DONE(SUCCEED);
        }
    } /* end if */
    else if (mesg->location == H5SM_IN_OH && key->message.location == H5SM_IN_OH) {
        if (key->message.u.mesg_loc.oh_addr == mesg->u.mesg_loc.oh_addr &&
            key->message.u.mesg_loc.index == mesg->u.mesg_loc.index &&
            key->message.msg_type_id == mesg->msg_type_id) {
            *result = 0;
            HGOTO_DONE(SUCCEED);
        }
    } /* end if */

    /* Compare hash values */
    if (key->message.hash > mesg->hash)
        *result = 1;
    else if (key->message.hash < mesg->hash)
        *result = -1;
    /* If the hash values match, make sure the messages are really the same */
    else {
        /* Hash values match; compare the encoded message with the one in
         * the index.
         */
        H5SM_compare_udata_t udata;

        assert(key->message.hash == mesg->hash);
        assert(key->encoding_size > 0 && key->encoding);

        /* Set up user data for callback */
        udata.key = key;

        /* Compare the encoded message with either the message in the heap or
         * the message in an object header.
         */
        if (mesg->location == H5SM_IN_HEAP) {
            /* Call heap op routine with comparison callback */
            if (H5HF_op(key->fheap, &(mesg->u.heap_loc.fheap_id), H5SM__compare_cb, &udata) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");
        } /* end if */
        else {
            H5O_loc_t           oloc; /* Object owning the message */
            H5O_mesg_operator_t op;   /* Message operator */

            /* Sanity checks */
            assert(key->file);
            assert(mesg->location == H5SM_IN_OH);

            /* Reset the object location */
            if (H5O_loc_reset(&oloc) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTRESET, FAIL, "unable to initialize target location");

            /* Set up object location */
            oloc.file = key->file;
            oloc.addr = mesg->u.mesg_loc.oh_addr;

            /* Finish setting up user data for iterator */
            udata.idx = mesg->u.mesg_loc.index;

            /* Locate the right message and compare with it */
            op.op_type  = H5O_MESG_OP_LIB;
            op.u.lib_op = H5SM__compare_iter_op;
            if (H5O_msg_iterate(&oloc, mesg->msg_type_id, &op, &udata) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "error iterating over links");
        } /* end else */

        *result = udata.ret;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__message_compare */

/*-------------------------------------------------------------------------
 * Function:	H5SM__message_encode
 *
 * Purpose:	Serialize a H5SM_sohm_t struct into a buffer RAW.
 *
 * Return:	Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM__message_encode(uint8_t *raw, const void *_nrecord, void *_ctx)
{
    H5SM_bt2_ctx_t    *ctx     = (H5SM_bt2_ctx_t *)_ctx; /* Callback context structure */
    const H5SM_sohm_t *message = (const H5SM_sohm_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    *raw++ = (uint8_t)message->location;
    UINT32ENCODE(raw, message->hash);

    if (message->location == H5SM_IN_HEAP) {
        UINT32ENCODE(raw, message->u.heap_loc.ref_count);
        H5MM_memcpy(raw, message->u.heap_loc.fheap_id.id, (size_t)H5O_FHEAP_ID_LEN);
    } /* end if */
    else {
        assert(message->location == H5SM_IN_OH);

        *raw++ = 0; /* reserved (possible flags byte) */
        *raw++ = (uint8_t)message->msg_type_id;
        UINT16ENCODE(raw, message->u.mesg_loc.index);
        H5F_addr_encode_len((size_t)ctx->sizeof_addr, &raw, message->u.mesg_loc.oh_addr);
    } /* end else */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__message_encode */

/*-------------------------------------------------------------------------
 * Function:	H5SM__message_decode
 *
 * Purpose:	Read an encoded SOHM message from RAW into an H5SM_sohm_t struct.
 *
 * Return:	Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM__message_decode(const uint8_t *raw, void *_nrecord, void *_ctx)
{
    H5SM_bt2_ctx_t *ctx     = (H5SM_bt2_ctx_t *)_ctx; /* Callback context structure */
    H5SM_sohm_t    *message = (H5SM_sohm_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    message->location = (H5SM_storage_loc_t)*raw++;
    UINT32DECODE(raw, message->hash);

    if (message->location == H5SM_IN_HEAP) {
        UINT32DECODE(raw, message->u.heap_loc.ref_count);
        H5MM_memcpy(message->u.heap_loc.fheap_id.id, raw, (size_t)H5O_FHEAP_ID_LEN);
    } /* end if */
    else {
        assert(message->location == H5SM_IN_OH);

        raw++; /* reserved */
        message->msg_type_id = *raw++;
        UINT16DECODE(raw, message->u.mesg_loc.index);
        H5F_addr_decode_len((size_t)ctx->sizeof_addr, &raw, &message->u.mesg_loc.oh_addr);
    } /* end else */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__message_decode */
