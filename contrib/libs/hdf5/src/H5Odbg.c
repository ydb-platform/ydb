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
 * Created:		H5Odbg.c
 *
 * Purpose:		Object header debugging routines.
 *
 *-------------------------------------------------------------------------
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
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Opkg.h"      /* Object headers			*/
#include "H5Ppublic.h"   /* Property Lists			*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

#ifdef H5O_DEBUG

/*-------------------------------------------------------------------------
 * Function:    H5O__assert
 *
 * Purpose:     Sanity check the information for an object header data
 *              structure.
 *
 * Return:      SUCCEED (Doesn't fail, just crashes)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__assert(const H5O_t *oh)
{
    H5O_mesg_t *curr_msg;            /* Pointer to current message to examine */
    H5O_mesg_t *tmp_msg;             /* Pointer to temporary message to examine */
    unsigned    cont_msgs_found = 0; /* # of continuation messages for object */
    size_t      meta_space;          /* Size of header metadata */
    size_t      mesg_space;          /* Size of message raw data */
    size_t      free_space;          /* Size of free space in header */
    size_t      hdr_size;            /* Size of header's chunks */
    unsigned    u, v;                /* Local index variables */

    FUNC_ENTER_PACKAGE_NOERR

    /* Initialize the tracking variables */
    hdr_size   = 0;
    meta_space = (size_t)H5O_SIZEOF_HDR(oh) + (size_t)(H5O_SIZEOF_CHKHDR_OH(oh) * (oh->nchunks - 1));
    mesg_space = 0;
    free_space = 0;

    /* Loop over all chunks in object header */
    for (u = 0; u < oh->nchunks; u++) {
        /* Accumulate the size of the header on header */
        hdr_size += oh->chunk[u].size;

        /* If the chunk has a gap, add it to the free space */
        free_space += oh->chunk[u].gap;

        /* Check for valid raw data image */
        assert(oh->chunk[u].image);
        assert(oh->chunk[u].size > (size_t)H5O_SIZEOF_CHKHDR_OH(oh));

        /* All chunks must be allocated on disk */
        assert(H5_addr_defined(oh->chunk[u].addr));

        /* Version specific checks */
        if (oh->version > H5O_VERSION_1) {
            /* Make certain that the magic number is correct for each chunk */
            assert(!memcmp(oh->chunk[u].image, (u == 0 ? H5O_HDR_MAGIC : H5O_CHK_MAGIC), H5_SIZEOF_MAGIC));

            /* Check for valid gap size */
            assert(oh->chunk[u].gap < (size_t)H5O_SIZEOF_MSGHDR_OH(oh));
        } /* end if */
        else
            /* Gaps should never occur in version 1 of the format */
            assert(oh->chunk[u].gap == 0);
    } /* end for */

    /* Check for correct chunk #0 size flags */
    if (oh->version > H5O_VERSION_1) {
        uint64_t chunk0_size = oh->chunk[0].size - (size_t)H5O_SIZEOF_HDR(oh);

        if (chunk0_size <= 255)
            assert((oh->flags & H5O_HDR_CHUNK0_SIZE) == H5O_HDR_CHUNK0_1);
        else if (chunk0_size <= 65535)
            assert((oh->flags & H5O_HDR_CHUNK0_SIZE) == H5O_HDR_CHUNK0_2);
        else if (chunk0_size <= 4294967295)
            assert((oh->flags & H5O_HDR_CHUNK0_SIZE) == H5O_HDR_CHUNK0_4);
        else
            assert((oh->flags & H5O_HDR_CHUNK0_SIZE) == H5O_HDR_CHUNK0_8);
    } /* end if */

    /* Loop over all messages in object header */
    for (u = 0, curr_msg = &oh->mesg[0]; u < oh->nmesgs; u++, curr_msg++) {
        uint8_t H5_ATTR_NDEBUG_UNUSED *curr_hdr;      /* Start of current message header */
        size_t                         curr_tot_size; /* Total size of current message (including header) */

        curr_hdr      = curr_msg->raw - H5O_SIZEOF_MSGHDR_OH(oh);
        curr_tot_size = curr_msg->raw_size + (size_t)H5O_SIZEOF_MSGHDR_OH(oh);

        /* Accumulate information, based on the type of message */
        if (H5O_NULL_ID == curr_msg->type->id)
            free_space += curr_tot_size;
        else if (H5O_CONT_ID == curr_msg->type->id) {
            H5O_cont_t                *cont        = (H5O_cont_t *)curr_msg->native;
            bool H5_ATTR_NDEBUG_UNUSED found_chunk = false; /* Found a chunk that matches */

            assert(cont);

            /* Increment # of continuation messages found */
            cont_msgs_found++;

            /* Sanity check that every continuation message has a matching chunk */
            /* (and only one) */
            for (v = 0; v < oh->nchunks; v++) {
                if (H5_addr_eq(cont->addr, oh->chunk[v].addr) && cont->size == oh->chunk[v].size) {
                    assert(cont->chunkno == v);
                    assert(!found_chunk);
                    found_chunk = true;
                } /* end if */
            }     /* end for */
            assert(found_chunk);

            meta_space += curr_tot_size;
        } /* end if */
        else {
            meta_space += (size_t)H5O_SIZEOF_MSGHDR_OH(oh);
            mesg_space += curr_msg->raw_size;

            /* Make sure the message has a native form if it is marked dirty */
            assert(curr_msg->native || !curr_msg->dirty);
        } /* end else */

        /* Make certain that the message is in a valid chunk */
        assert(curr_msg->chunkno < oh->nchunks);

        /* Make certain null messages aren't in chunks with gaps */
        if (H5O_NULL_ID == curr_msg->type->id)
            assert(oh->chunk[curr_msg->chunkno].gap == 0);

        /* Make certain that the message is completely in a chunk message area */
        assert(curr_tot_size <= (oh->chunk[curr_msg->chunkno].size) -
                                    (H5O_SIZEOF_CHKSUM_OH(oh) + oh->chunk[curr_msg->chunkno].gap));
        if (curr_msg->chunkno == 0)
            assert(curr_hdr >=
                   oh->chunk[curr_msg->chunkno].image + (H5O_SIZEOF_HDR(oh) - H5O_SIZEOF_CHKSUM_OH(oh)));
        else
            assert(curr_hdr >= oh->chunk[curr_msg->chunkno].image +
                                   (H5O_SIZEOF_CHKHDR_OH(oh) - H5O_SIZEOF_CHKSUM_OH(oh)));
        assert(curr_msg->raw + curr_msg->raw_size <=
               (oh->chunk[curr_msg->chunkno].image + oh->chunk[curr_msg->chunkno].size) -
                   (H5O_SIZEOF_CHKSUM_OH(oh) + oh->chunk[curr_msg->chunkno].gap));

        /* Make certain that no other messages overlap this message */
        for (v = 0, tmp_msg = &oh->mesg[0]; v < oh->nmesgs; v++, tmp_msg++) {
            if (u != v)
                assert(!((tmp_msg->raw - H5O_SIZEOF_MSGHDR_OH(oh)) >= curr_hdr &&
                         (tmp_msg->raw - H5O_SIZEOF_MSGHDR_OH(oh)) < (curr_hdr + curr_tot_size)));
        } /* end for */
    }     /* end for */

    /* Sanity check that the # of cont. messages is correct for the # of chunks */
    assert(oh->nchunks == (cont_msgs_found + 1));

    /* Sanity check that all the bytes are accounted for */
    assert(hdr_size == (free_space + meta_space + mesg_space));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__assert() */
#endif /* H5O_DEBUG */

/*-------------------------------------------------------------------------
 * Function:	H5O_debug_id
 *
 * Purpose:	Act as a proxy for calling the 'debug' method for a
 *              particular class of object header.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_debug_id(unsigned type_id, H5F_t *f, const void *mesg, FILE *stream, int indent, int fwidth)
{
    const H5O_msg_class_t *type;             /* Actual H5O class type for the ID */
    herr_t                 ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(type_id < NELMTS(H5O_msg_class_g));
    type = H5O_msg_class_g[type_id]; /* map the type ID to the actual type object */
    assert(type);
    assert(type->debug);
    assert(f);
    assert(mesg);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /* Call the debug method in the class */
    if ((ret_value = (type->debug)(f, mesg, stream, indent, fwidth)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_BADTYPE, FAIL, "unable to debug message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_debug_id() */

/*-------------------------------------------------------------------------
 * Function:    H5O__debug_real
 *
 * Purpose:     Prints debugging info about an object header.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__debug_real(H5F_t *f, H5O_t *oh, haddr_t addr, FILE *stream, int indent, int fwidth)
{
    size_t    mesg_total = 0, chunk_total = 0, gap_total = 0;
    unsigned *sequence = NULL;
    unsigned  i; /* Local index variable */
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(oh);
    assert(H5_addr_defined(addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /* debug */
    fprintf(stream, "%*sObject Header...\n", indent, "");

    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Dirty:", oh->cache_info.is_dirty ? "TRUE" : "FALSE");
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Version:", oh->version);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Header size (in bytes):", (unsigned)H5O_SIZEOF_HDR(oh));
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Number of links:", oh->nlink);

    /* Extra information for later versions */
    if (oh->version > H5O_VERSION_1) {
        /* Display object's status flags */
        fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Attribute creation order tracked:",
                (oh->flags & H5O_HDR_ATTR_CRT_ORDER_TRACKED) ? "Yes" : "No");
        fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Attribute creation order indexed:",
                (oh->flags & H5O_HDR_ATTR_CRT_ORDER_INDEXED) ? "Yes" : "No");
        fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Attribute storage phase change values:",
                (oh->flags & H5O_HDR_ATTR_STORE_PHASE_CHANGE) ? "Non-default" : "Default");
        fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
                "Timestamps:", (oh->flags & H5O_HDR_STORE_TIMES) ? "Enabled" : "Disabled");
        if (oh->flags & ~H5O_HDR_ALL_FLAGS)
            fprintf(stream, "*** UNKNOWN OBJECT HEADER STATUS FLAG: %02x!\n", (unsigned)oh->flags);

        /* Only dump times, if they are tracked */
        if (oh->flags & H5O_HDR_STORE_TIMES) {
            struct tm *tm;       /* Time structure */
            char       buf[128]; /* Buffer for formatting time info */

            /* Time fields */
            tm = HDlocaltime(&oh->atime);
            strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S %Z", tm);
            fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Access Time:", buf);
            tm = HDlocaltime(&oh->mtime);
            strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S %Z", tm);
            fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Modification Time:", buf);
            tm = HDlocaltime(&oh->ctime);
            strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S %Z", tm);
            fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Change Time:", buf);
            tm = HDlocaltime(&oh->btime);
            strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S %Z", tm);
            fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Birth Time:", buf);
        } /* end if */

        /* Attribute tracking fields */
        if (oh->flags & H5O_HDR_ATTR_STORE_PHASE_CHANGE) {
            fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
                    "Max. compact attributes:", (unsigned)oh->max_compact);
            fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
                    "Min. dense attributes:", (unsigned)oh->min_dense);
        } /* end if */
    }     /* end if */

    fprintf(stream, "%*s%-*s %zu (%zu)\n", indent, "", fwidth, "Number of messages (allocated):", oh->nmesgs,
            oh->alloc_nmesgs);
    fprintf(stream, "%*s%-*s %zu (%zu)\n", indent, "", fwidth, "Number of chunks (allocated):", oh->nchunks,
            oh->alloc_nchunks);

    /* debug each chunk */
    for (i = 0, chunk_total = 0; i < oh->nchunks; i++) {
        size_t chunk_size;

        fprintf(stream, "%*sChunk %d...\n", indent, "", i);

        fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent + 3, "", MAX(0, fwidth - 3),
                "Address:", oh->chunk[i].addr);

        /* Decrement chunk 0's size by the object header prefix size */
        if (0 == i) {
            if (H5_addr_ne(oh->chunk[i].addr, addr))
                fprintf(stream, "*** WRONG ADDRESS FOR CHUNK #0!\n");
            chunk_size = oh->chunk[i].size - (size_t)H5O_SIZEOF_HDR(oh);
        } /* end if */
        else
            chunk_size = oh->chunk[i].size;

        /* Accumulate chunk's size to total */
        chunk_total += chunk_size;
        gap_total += oh->chunk[i].gap;

        fprintf(stream, "%*s%-*s %zu\n", indent + 3, "", MAX(0, fwidth - 3), "Size in bytes:", chunk_size);

        fprintf(stream, "%*s%-*s %zu\n", indent + 3, "", MAX(0, fwidth - 3), "Gap:", oh->chunk[i].gap);
    } /* end for */

    /* debug each message */
    if (NULL == (sequence = (unsigned *)H5MM_calloc(NELMTS(H5O_msg_class_g) * sizeof(unsigned))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
    for (i = 0, mesg_total = 0; i < oh->nmesgs; i++) {
        const H5O_msg_class_t *debug_type; /* Type of message to use for callbacks */
        unsigned               chunkno;    /* Chunk for message */

        /* Accumulate message's size to total */
        mesg_total += (size_t)H5O_SIZEOF_MSGHDR_OH(oh) + oh->mesg[i].raw_size;

        /* For version 2 object header, add size of "OCHK" for continuation chunk */
        if (oh->mesg[i].type->id == H5O_CONT_ID)
            mesg_total += H5O_SIZEOF_CHKHDR_OH(oh);

        fprintf(stream, "%*sMessage %d...\n", indent, "", i);

        /* check for bad message id */
        if (oh->mesg[i].type->id >= (int)NELMTS(H5O_msg_class_g)) {
            fprintf(stream, "*** BAD MESSAGE ID 0x%04x\n", oh->mesg[i].type->id);
            continue;
        } /* end if */

        /* message name and size */
        fprintf(stream, "%*s%-*s 0x%04x `%s' (%d)\n", indent + 3, "", MAX(0, fwidth - 3),
                "Message ID (sequence number):", (unsigned)(oh->mesg[i].type->id), oh->mesg[i].type->name,
                sequence[oh->mesg[i].type->id]++);
        fprintf(stream, "%*s%-*s %s\n", indent + 3, "", MAX(0, fwidth - 3),
                "Dirty:", oh->mesg[i].dirty ? "TRUE" : "FALSE");
        fprintf(stream, "%*s%-*s ", indent + 3, "", MAX(0, fwidth - 3), "Message flags:");
        if (oh->mesg[i].flags) {
            bool flag_printed = false;

            /* Sanity check that all flags in format are covered below */
            HDcompile_assert(H5O_MSG_FLAG_BITS ==
                             (H5O_MSG_FLAG_CONSTANT | H5O_MSG_FLAG_SHARED | H5O_MSG_FLAG_DONTSHARE |
                              H5O_MSG_FLAG_FAIL_IF_UNKNOWN_AND_OPEN_FOR_WRITE | H5O_MSG_FLAG_MARK_IF_UNKNOWN |
                              H5O_MSG_FLAG_WAS_UNKNOWN | H5O_MSG_FLAG_SHAREABLE |
                              H5O_MSG_FLAG_FAIL_IF_UNKNOWN_ALWAYS));

            if (oh->mesg[i].flags & H5O_MSG_FLAG_CONSTANT) {
                fprintf(stream, "%sC", (flag_printed ? ", " : "<"));
                flag_printed = true;
            } /* end if */
            if (oh->mesg[i].flags & H5O_MSG_FLAG_SHARED) {
                fprintf(stream, "%sS", (flag_printed ? ", " : "<"));
                flag_printed = true;
            } /* end if */
            if (oh->mesg[i].flags & H5O_MSG_FLAG_DONTSHARE) {
                fprintf(stream, "%sDS", (flag_printed ? ", " : "<"));
                flag_printed = true;
            } /* end if */
            if (oh->mesg[i].flags & H5O_MSG_FLAG_FAIL_IF_UNKNOWN_AND_OPEN_FOR_WRITE) {
                fprintf(stream, "%sFIUW", (flag_printed ? ", " : "<"));
                flag_printed = true;
            } /* end if */
            if (oh->mesg[i].flags & H5O_MSG_FLAG_MARK_IF_UNKNOWN) {
                fprintf(stream, "%sMIU", (flag_printed ? ", " : "<"));
                flag_printed = true;
            } /* end if */
            if (oh->mesg[i].flags & H5O_MSG_FLAG_WAS_UNKNOWN) {
                assert(oh->mesg[i].flags & H5O_MSG_FLAG_MARK_IF_UNKNOWN);
                fprintf(stream, "%sWU", (flag_printed ? ", " : "<"));
                flag_printed = true;
            } /* end if */
            if (oh->mesg[i].flags & H5O_MSG_FLAG_SHAREABLE) {
                fprintf(stream, "%sSA", (flag_printed ? ", " : "<"));
                flag_printed = true;
            } /* end if */
            if (oh->mesg[i].flags & H5O_MSG_FLAG_FAIL_IF_UNKNOWN_ALWAYS) {
                fprintf(stream, "%sFIUA", (flag_printed ? ", " : "<"));
                flag_printed = true;
            } /* end if */
            if (!flag_printed)
                fprintf(stream, "-");
            fprintf(stream, ">\n");
            if (oh->mesg[i].flags & ~H5O_MSG_FLAG_BITS)
                fprintf(stream, "%*s%-*s 0x%02x\n", indent + 3, "", MAX(0, fwidth - 3),
                        "*** ADDITIONAL UNKNOWN FLAGS --->", oh->mesg[i].flags & ~H5O_MSG_FLAG_BITS);
        } /* end if */
        else
            fprintf(stream, "<none>\n");

        fprintf(stream, "%*s%-*s %u\n", indent + 3, "", MAX(0, fwidth - 3),
                "Chunk number:", oh->mesg[i].chunkno);
        chunkno = oh->mesg[i].chunkno;
        if (chunkno >= oh->nchunks)
            fprintf(stream, "*** BAD CHUNK NUMBER\n");
        fprintf(stream, "%*s%-*s (%zu, %zu) bytes\n", indent + 3, "", MAX(0, fwidth - 3),
                "Raw message data (offset, size) in chunk:",
                (size_t)(oh->mesg[i].raw - oh->chunk[chunkno].image), oh->mesg[i].raw_size);

        /* check the size */
        if ((oh->mesg[i].raw + oh->mesg[i].raw_size > oh->chunk[chunkno].image + oh->chunk[chunkno].size) ||
            (oh->mesg[i].raw < oh->chunk[chunkno].image))
            fprintf(stream, "*** BAD MESSAGE RAW ADDRESS\n");

        /* decode the message */
        debug_type = oh->mesg[i].type;
        if (NULL == oh->mesg[i].native && debug_type->decode)
            H5O_LOAD_NATIVE(f, H5O_DECODEIO_NOCHANGE, oh, &oh->mesg[i], FAIL)

        /* print the message */
        fprintf(stream, "%*s%-*s\n", indent + 3, "", MAX(0, fwidth - 3), "Message Information:");
        if (debug_type->debug && oh->mesg[i].native != NULL)
            (debug_type->debug)(f, oh->mesg[i].native, stream, indent + 6, MAX(0, fwidth - 6));
        else
            fprintf(stream, "%*s<No info for this message>\n", indent + 6, "");
    } /* end for */

    if ((mesg_total + gap_total) != chunk_total)
        fprintf(stream, "*** TOTAL SIZE DOES NOT MATCH ALLOCATED SIZE!\n");

done:
    /* Release resources */
    if (sequence)
        sequence = (unsigned *)H5MM_xfree(sequence);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__debug_real() */

/*-------------------------------------------------------------------------
 * Function:	H5O_debug
 *
 * Purpose:	Prints debugging info about an object header.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth)
{
    H5O_t    *oh = NULL;           /* Object header to display */
    H5O_loc_t loc;                 /* Object location for object to delete */
    herr_t    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check args */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /* Set up the object location */
    loc.file         = f;
    loc.addr         = addr;
    loc.holding_file = false;

    if (NULL == (oh = H5O_protect(&loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* debug */
    if (H5O__debug_real(f, oh, addr, stream, indent, fwidth) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_SYSTEM, FAIL, "debug dump call failed");

done:
    if (oh && H5O_unprotect(&loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_debug() */
