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
 * Purpose:	Functions that operate on a shared message.  The shared
 *		message doesn't ever actually appear in the object header as
 *		a normal message.  Instead, if a message is shared, the
 *		H5O_FLAG_SHARED bit is set and the message body is that
 *		defined here for H5O_SHARED.  The message ID is the ID of the
 *		pointed-to message and the pointed-to message is stored in
 *		the global heap.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Omodule.h" /* This source code file is part of the H5O module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access				*/
#include "H5Gprivate.h"  /* Groups				*/
#include "H5HFprivate.h" /* Fractal heap				*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Opkg.h"      /* Object headers			*/
#include "H5SMprivate.h" /* Shared object header messages        */
#include "H5WBprivate.h" /* Wrapped Buffers                      */

/****************/
/* Local Macros */
/****************/

/* First version, with full symbol table entry as link for object header sharing */
#define H5O_SHARED_VERSION_1 1

/* Older version, with just address of object as link for object header sharing */
#define H5O_SHARED_VERSION_2 2

/* Newest version, which recognizes messages that are stored in the SOHM heap */
#define H5O_SHARED_VERSION_3      3
#define H5O_SHARED_VERSION_LATEST H5O_SHARED_VERSION_3

/* Size of stack buffer for serialized messages */
#define H5O_MESG_BUF_SIZE 128

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/
static void  *H5O__shared_read(H5F_t *f, H5O_t *open_oh, unsigned *ioflags, const H5O_shared_t *shared,
                               const H5O_msg_class_t *type);
static herr_t H5O__shared_link_adj(H5F_t *f, H5O_t *open_oh, const H5O_msg_class_t *type,
                                   H5O_shared_t *shared, int adjust);

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
 * Function:    H5O__shared_read
 *
 * Purpose:     Reads a message referred to by a shared message.
 *
 * Return:      Success:    Ptr to message in native format. The message
 *                          should be freed by calling H5O_msg_reset().
 *
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__shared_read(H5F_t *f, H5O_t *open_oh, unsigned *ioflags, const H5O_shared_t *shared,
                 const H5O_msg_class_t *type)
{
    H5HF_t *fheap = NULL;
    H5WB_t *wb    = NULL;                /* Wrapped buffer for attribute data */
    uint8_t mesg_buf[H5O_MESG_BUF_SIZE]; /* Buffer for deserializing messages */
    void   *ret_value = NULL;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(shared);
    assert(type);
    assert(type->share_flags & H5O_SHARE_IS_SHARABLE);

    /* This message could have a heap ID (SOHM) or the address of an object
     * header on disk (named datatype)
     */
    assert(H5O_IS_STORED_SHARED(shared->type));

    /* Check for implicit shared object header message */
    if (shared->type == H5O_SHARE_TYPE_SOHM) {
        haddr_t  fheap_addr; /* Address of SOHM heap */
        uint8_t *mesg_ptr;   /* Pointer to raw message in heap */
        size_t   mesg_size;  /* Size of message */

        /* Retrieve the fractal heap address for shared messages */
        if (H5SM_get_fheap_addr(f, type->id, &fheap_addr) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "can't get fheap address for shared messages");

        /* Open the fractal heap */
        if (NULL == (fheap = H5HF_open(f, fheap_addr)))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "unable to open fractal heap");

        /* Get the size of the message in the heap */
        if (H5HF_get_obj_len(fheap, &(shared->u.heap_id), &mesg_size) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "can't get message size from fractal heap.");

        /* Wrap the local buffer for serialized message */
        if (NULL == (wb = H5WB_wrap(mesg_buf, sizeof(mesg_buf))))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "can't wrap buffer");

        /* Get a pointer to a buffer that's large enough for message */
        if (NULL == (mesg_ptr = (uint8_t *)H5WB_actual(wb, mesg_size)))
            HGOTO_ERROR(H5E_OHDR, H5E_NOSPACE, NULL, "can't get actual buffer");

        /* Retrieve the message from the heap */
        if (H5HF_read(fheap, &(shared->u.heap_id), mesg_ptr) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "can't read message from fractal heap.");

        /* Decode the message */
        if (NULL == (ret_value = (type->decode)(f, open_oh, 0, ioflags, mesg_size, mesg_ptr)))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTDECODE, NULL, "can't decode shared message.");
    } /* end if */
    else {
        H5O_loc_t oloc; /* Location for object header where message is stored */

        assert(shared->type == H5O_SHARE_TYPE_COMMITTED);

        /* Build the object location for the shared message's object header */
        oloc.file         = f;
        oloc.addr         = shared->u.loc.oh_addr;
        oloc.holding_file = false;

        if (open_oh && oloc.addr == H5O_OH_GET_ADDR(open_oh)) {
            /* The shared message is in the already opened object header.  This
             * is possible, for example, if an attribute's datatype is shared in
             * the same object header the attribute is in.  Read the message
             * directly. */
            if (NULL == (ret_value = H5O_msg_read_oh(f, open_oh, type->id, NULL)))
                HGOTO_ERROR(H5E_OHDR, H5E_READERROR, NULL, "unable to read message");
        }
        else
            /* The shared message is in another object header */
            if (NULL == (ret_value = H5O_msg_read(&oloc, type->id, NULL)))
                HGOTO_ERROR(H5E_OHDR, H5E_READERROR, NULL, "unable to read message");
    } /* end else */

    /* Mark the message as shared */
    if (H5O_msg_set_share(type->id, shared, ret_value) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "unable to set sharing information");

done:
    /* Release resources */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTFREE, NULL, "can't close fractal heap");
    if (wb && H5WB_unwrap(wb) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CLOSEERROR, NULL, "can't close wrapped buffer");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__shared_read() */

/*-------------------------------------------------------------------------
 * Function:    H5O__shared_link_adj
 *
 * Purpose:     Changes the link count for the object referenced by a shared
 *              message.
 *
 *              This function changes the object header link count and is
 *              only relevant for committed messages.  Messages shared in
 *              the heap are re-shared each time they're written, so their
 *              reference count is stored in the file-wide shared message
 *              index and is changed in a different place in the code.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__shared_link_adj(H5F_t *f, H5O_t *open_oh, const H5O_msg_class_t *type, H5O_shared_t *shared, int adjust)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(shared);

    /* Check for type of shared message */
    if (shared->type == H5O_SHARE_TYPE_COMMITTED) {
        H5O_loc_t oloc; /* Location for object header where message is stored */

        /*
         * The shared message is stored in some object header.
         * The other object header must be in the same file as the
         * new object header. Adjust the reference count on that
         * object header.
         */
        /* Unfortunately, it is possible for the shared->file pointer to become
         * invalid if the oh is kept in cache (which is contained in
         * shared->file->shared while shared->file is closed.  Just ignore
         * shared->file until the "top-level" file pointer is removed at some
         * point in the future.  -NAF */
        /* This is related to Jira issue #7638 and should be uncommented after
         * the library has been refactored to shift to using shared file
         * pointers for file operations, instead of using top file pointers.
         * -QAK */
        /*if(shared->file->shared != f->shared)
            HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "interfile hard links are not allowed")*/

        /* Build the object location for the shared message's object header */
        oloc.file         = f;
        oloc.addr         = shared->u.loc.oh_addr;
        oloc.holding_file = false;

        if (open_oh && oloc.addr == H5O_OH_GET_ADDR(open_oh)) {
            /* The shared message is in the already opened object header.  This
             * is possible, for example, if an attribute's datatype is shared in
             * the same object header the attribute is in.  Adjust the link
             * count directly. */
            bool deleted = false; /* This is used only to satisfy H5O__link_oh */

            if (H5O__link_oh(f, adjust, open_oh, &deleted) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_LINKCOUNT, FAIL, "unable to adjust shared object link count");

            assert(!deleted);
        }
        else
            /* The shared message is in another object header */
            if (H5O_link(&oloc, adjust) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_LINKCOUNT, FAIL, "unable to adjust shared object link count");
    } /* end if */
    else {
        assert(shared->type == H5O_SHARE_TYPE_SOHM || shared->type == H5O_SHARE_TYPE_HERE);

        /* Check for decrementing reference count on shared message */
        if (adjust < 0) {
            if (H5SM_delete(f, open_oh, shared) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTDEC, FAIL, "unable to delete message from SOHM table");
        } /* end if */
        /* Check for incrementing reference count on message */
        else if (adjust > 0) {
            if (H5SM_try_share(f, open_oh, 0, type->id, shared, NULL) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTINC, FAIL, "error trying to share message");
        } /* end if */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__shared_link_adj() */

/*-------------------------------------------------------------------------
 * Function:    H5O__shared_decode
 *
 * Purpose:     Decodes a shared object message
 *
 * Return:      Success:    A buffer containing the decoded shared object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5O__shared_decode(H5F_t *f, H5O_t *open_oh, unsigned *ioflags, const uint8_t *buf,
                   const H5O_msg_class_t *type)
{
    H5O_shared_t sh_mesg;          /* Shared message info */
    unsigned     version;          /* Shared message version */
    void        *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(f);
    assert(buf);
    assert(type);

    /* Version */
    version = *buf++;
    if (version < H5O_SHARED_VERSION_1 || version > H5O_SHARED_VERSION_LATEST)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad version number for shared object message");

    /* Get the shared information type
     * Flags are unused before version 3.
     */
    if (version >= H5O_SHARED_VERSION_2)
        sh_mesg.type = *buf++;
    else {
        sh_mesg.type = H5O_SHARE_TYPE_COMMITTED;
        buf++;
    } /* end else */

    /* Skip reserved bytes (for version 1) */
    if (version == H5O_SHARED_VERSION_1)
        buf += 6;

    /* Body */
    if (version == H5O_SHARED_VERSION_1) {
        /* Initialize other location fields */
        sh_mesg.u.loc.index = 0;

        /* Decode stored "symbol table entry" into message location */
        buf += H5F_SIZEOF_SIZE(f); /* Skip over local heap address */
        H5F_addr_decode(f, &buf, &(sh_mesg.u.loc.oh_addr));
    } /* end if */
    else if (version >= H5O_SHARED_VERSION_2) {
        /* If this message is in the heap, copy a heap ID.
         * Otherwise, it is a named datatype, so copy an H5O_loc_t.
         */
        if (sh_mesg.type == H5O_SHARE_TYPE_SOHM) {
            assert(version >= H5O_SHARED_VERSION_3);
            H5MM_memcpy(&sh_mesg.u.heap_id, buf, sizeof(sh_mesg.u.heap_id));
        } /* end if */
        else {
            /* The H5O_COMMITTED_FLAG should be set if this message
             * is from an older version before the flag existed.
             */
            if (version < H5O_SHARED_VERSION_3)
                sh_mesg.type = H5O_SHARE_TYPE_COMMITTED;

            sh_mesg.u.loc.index = 0;
            H5F_addr_decode(f, &buf, &sh_mesg.u.loc.oh_addr);
        } /* end else */
    }     /* end else if */

    /* Set file pointer & message type for all types of shared messages */
    sh_mesg.file        = f;
    sh_mesg.msg_type_id = type->id;

    /* Retrieve actual message, through decoded shared message info */
    if (NULL == (ret_value = H5O__shared_read(f, open_oh, ioflags, &sh_mesg, type)))
        HGOTO_ERROR(H5E_OHDR, H5E_READERROR, NULL, "unable to retrieve native message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__shared_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__shared_encode
 *
 * Purpose:     Encodes message _MESG into buffer BUF.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__shared_encode(const H5F_t *f, uint8_t *buf /*out*/, const H5O_shared_t *sh_mesg)
{
    unsigned version;

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(f);
    assert(buf);
    assert(sh_mesg);

    /* If this message is shared in the heap, we need to use version 3 of the
     * encoding and encode the SHARED_IN_HEAP flag.
     */
    if (sh_mesg->type == H5O_SHARE_TYPE_SOHM)
        version = H5O_SHARED_VERSION_LATEST;
    else {
        assert(sh_mesg->type == H5O_SHARE_TYPE_COMMITTED);
        version = H5O_SHARED_VERSION_2; /* version 1 is no longer used */
    }                                   /* end else */

    *buf++ = (uint8_t)version;
    *buf++ = (uint8_t)sh_mesg->type;

    /* Encode either the heap ID of the message or the address of the
     * object header that holds it.
     */
    if (sh_mesg->type == H5O_SHARE_TYPE_SOHM)
        H5MM_memcpy(buf, &(sh_mesg->u.heap_id), sizeof(sh_mesg->u.heap_id));
    else
        H5F_addr_encode(f, &buf, sh_mesg->u.loc.oh_addr);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__shared_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5O_set_shared
 *
 * Purpose:     Sets the shared component for a message.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_set_shared(H5O_shared_t *dst, const H5O_shared_t *src)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* check args */
    assert(dst);
    assert(src);

    /* copy */
    *dst = *src;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O_set_shared() */

/*-------------------------------------------------------------------------
 * Function:    H5O__shared_size
 *
 * Purpose:     Returns the length of a shared object message.
 *
 * Return:      Success:    Length
 *              Failure:    0
 *
 *-------------------------------------------------------------------------
 */
size_t
H5O__shared_size(const H5F_t *f, const H5O_shared_t *sh_mesg)
{
    size_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    if (sh_mesg->type == H5O_SHARE_TYPE_COMMITTED) {
        ret_value = (size_t)1 +                 /* Version                      */
                    (size_t)1 +                 /* Type field                   */
                    (size_t)H5F_SIZEOF_ADDR(f); /* Sharing by another obj hdr   */
    }                                           /* end if */
    else {
        assert(sh_mesg->type == H5O_SHARE_TYPE_SOHM);
        ret_value = 1 +               /* Version              */
                    1 +               /* Type field           */
                    H5O_FHEAP_ID_LEN; /* Shared in the heap   */
    }                                 /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__shared_size() */

/*-------------------------------------------------------------------------
 * Function:    H5O__shared_delete
 *
 * Purpose:     Free file space referenced by message
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__shared_delete(H5F_t *f, H5O_t *open_oh, const H5O_msg_class_t *type, H5O_shared_t *sh_mesg)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(sh_mesg);

    /*
     * Committed datatypes increment the OH of the original message when they
     * are written (in H5O_shared_link) and decrement it here.
     * SOHMs in the heap behave differently; their refcount is incremented
     * during H5SM_share when they are going to be written (in H5O_msg_append
     * or H5O_msg_write). Their refcount in the SOHM indexes still needs to
     * be decremented when they're deleted (in H5O__shared_link_adj).
     */

    /* Decrement the reference count on the shared object */
    if (H5O__shared_link_adj(f, open_oh, type, sh_mesg, -1) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_LINKCOUNT, FAIL, "unable to adjust shared object link count");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__shared_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5O__shared_link
 *
 * Purpose:     Increment reference count on any objects referenced by
 *              message
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__shared_link(H5F_t *f, H5O_t *open_oh, const H5O_msg_class_t *type, H5O_shared_t *sh_mesg)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(f);
    assert(sh_mesg);

    /* Increment the reference count on the shared object */
    if (H5O__shared_link_adj(f, open_oh, type, sh_mesg, 1) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_LINKCOUNT, FAIL, "unable to adjust shared object link count");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__shared_link() */

/*-------------------------------------------------------------------------
 * Function:    H5O__shared_copy_file
 *
 * Purpose:     Copies a message from _MESG to _DEST in file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__shared_copy_file(H5F_t H5_ATTR_NDEBUG_UNUSED *file_src, H5F_t *file_dst,
                      const H5O_msg_class_t *mesg_type, const void *_native_src, void *_native_dst,
                      bool H5_ATTR_UNUSED *recompute_size, unsigned *mesg_flags,
                      H5O_copy_t H5_ATTR_NDEBUG_UNUSED *cpy_info, void H5_ATTR_UNUSED *udata)
{
    const H5O_shared_t *shared_src =
        (const H5O_shared_t *)_native_src; /* Alias to shared info in native source */
    H5O_shared_t *shared_dst =
        (H5O_shared_t *)_native_dst; /* Alias to shared info in native destination message */
    herr_t ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(file_src);
    assert(file_dst);
    assert(mesg_type);
    assert(shared_src);
    assert(shared_dst);
    assert(recompute_size);
    assert(cpy_info);

    /* Committed shared messages create a shared message at the destination
     * and also copy the committed object that they point to.
     *
     * Other messages simulate sharing the destination message to determine how
     * it will eventually be shared (if at all), but do not actually share the
     * message until "post copy".  The "H5O_shared_t" part of the message will
     * be updated (to allow calculation of the final size) but the message is
     * not actually shared.
     */
    if (shared_src->type != H5O_SHARE_TYPE_COMMITTED) {
        /* Simulate trying to share new message in the destination file. */
        /* Set copied metadata tag */
        H5_BEGIN_TAG(H5AC__COPIED_TAG)

        if (H5SM_try_share(file_dst, NULL, H5SM_DEFER, mesg_type->id, _native_dst, mesg_flags) < 0)
            HGOTO_ERROR_TAG(H5E_OHDR, H5E_WRITEERROR, FAIL,
                            "unable to determine if message should be shared");

        /* Reset metadata tag */
        H5_END_TAG
    } /* end if */
    else {
        /* Mark the message as committed - as it will be committed in post copy
         */
        H5O_UPDATE_SHARED(shared_dst, H5O_SHARE_TYPE_COMMITTED, file_dst, mesg_type->id, 0, HADDR_UNDEF)
        *mesg_flags |= H5O_MSG_FLAG_SHARED;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__shared_copy_file() */

/*-------------------------------------------------------------------------
 * Function:    H5O__shared_post_copy_file
 *
 * Purpose:     Delete a shared message and replace with a new one.
 *              The function is needed at cases such as copying a shared reg_ref attribute.
 *              When a shared reg_ref attribute is copied from one file to
 *              another, the values in file need to be replaced. The only way
 *              to complish that is to delete the old message and write the
 *              new message with the correct values.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__shared_post_copy_file(H5F_t *f, const H5O_msg_class_t *mesg_type, const H5O_shared_t *shared_src,
                           H5O_shared_t *shared_dst, unsigned *mesg_flags, H5O_copy_t *cpy_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(shared_src);
    assert(shared_dst);

    /* Copy the target of committed messages, try to share others */
    if (shared_src->type == H5O_SHARE_TYPE_COMMITTED) {
        H5O_loc_t dst_oloc;
        H5O_loc_t src_oloc;

        /* Copy the shared object from source to destination */
        H5O_loc_reset(&dst_oloc);
        dst_oloc.file = f;
        src_oloc.file = shared_src->file;
        src_oloc.addr = shared_src->u.loc.oh_addr;
        if (H5O_copy_header_map(&src_oloc, &dst_oloc, cpy_info, false, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, FAIL, "unable to copy object");

        /* Set up destination message's shared info */
        H5O_UPDATE_SHARED(shared_dst, H5O_SHARE_TYPE_COMMITTED, f, mesg_type->id, 0, dst_oloc.addr)
    } /* end if */
    else
        /* Share the message */
        if (H5SM_try_share(f, NULL, H5SM_WAS_DEFERRED, mesg_type->id, shared_dst, mesg_flags) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_BADMESG, FAIL, "can't share message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__shared_post_copy_file() */

/*-------------------------------------------------------------------------
 * Function:	H5O__shared_debug
 *
 * Purpose:	Prints debugging info for the message
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__shared_debug(const H5O_shared_t *mesg, FILE *stream, int indent, int fwidth)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(mesg);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    switch (mesg->type) {
        case H5O_SHARE_TYPE_UNSHARED:
            fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Shared Message type:", "Unshared");
            break;

        case H5O_SHARE_TYPE_COMMITTED:
            fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Shared Message type:", "Obj Hdr");
            fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
                    "Object address:", mesg->u.loc.oh_addr);
            break;

        case H5O_SHARE_TYPE_SOHM:
            fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Shared Message type:", "SOHM");
            fprintf(stream, "%*s%-*s %016llx\n", indent, "", fwidth,
                    "Heap ID:", (unsigned long long)mesg->u.heap_id.val);
            break;

        case H5O_SHARE_TYPE_HERE:
            fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Shared Message type:", "Here");
            break;

        default:
            fprintf(stream, "%*s%-*s %s (%u)\n", indent, "", fwidth, "Shared Message type:", "Unknown",
                    (unsigned)mesg->type);
    } /* end switch */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__shared_debug() */
