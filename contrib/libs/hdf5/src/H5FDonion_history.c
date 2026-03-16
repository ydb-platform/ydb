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
 * Onion Virtual File Driver (VFD)
 *
 * Purpose:     Code for the onion file's history
 */

/* This source code file is part of the H5FD driver module */
#include "H5FDdrvr_module.h"

#include "H5private.h"      /* Generic Functions                        */
#include "H5Eprivate.h"     /* Error handling                           */
#include "H5FDprivate.h"    /* File drivers                             */
#include "H5FDonion.h"      /* Onion file driver                        */
#include "H5FDonion_priv.h" /* Onion file driver internals              */
#include "H5MMprivate.h"    /* Memory management                        */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_write_history
 *
 * Purpose:     Read and decode the history information from `raw_file` at
 *              `addr` .. `addr + size` (taken from history header), and store
 *              the decoded information in the structure at `history_out`.
 *
 * Returns:     SUCCEED/FAIL
 *-----------------------------------------------------------------------------
 */
herr_t
H5FD__onion_ingest_history(H5FD_onion_history_t *history_out, H5FD_t *raw_file, haddr_t addr, haddr_t size)
{
    unsigned char *buf       = NULL;
    uint32_t       sum       = 0;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(history_out);
    assert(raw_file);

    /* Set early so we can clean up properly on errors */
    history_out->record_locs = NULL;

    if (H5FD_get_eof(raw_file, H5FD_MEM_DRAW) < (addr + size))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "header indicates history beyond EOF");

    if (NULL == (buf = H5MM_malloc(sizeof(char) * size)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't allocate buffer space");

    if (H5FD_set_eoa(raw_file, H5FD_MEM_DRAW, (addr + size)) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't modify EOA");

    if (H5FD_read(raw_file, H5FD_MEM_DRAW, addr, size, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "can't read history from file");

    if (H5FD__onion_history_decode(buf, history_out) != size)
        HGOTO_ERROR(H5E_VFL, H5E_CANTDECODE, FAIL, "can't decode history (initial)");

    sum = H5_checksum_fletcher32(buf, size - 4);
    if (history_out->checksum != sum)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "checksum mismatch between buffer and stored");

    if (history_out->n_revisions > 0)
        if (NULL == (history_out->record_locs =
                         H5MM_calloc(history_out->n_revisions * sizeof(H5FD_onion_record_loc_t))))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't allocate record pointer list");

    if (H5FD__onion_history_decode(buf, history_out) != size)
        HGOTO_ERROR(H5E_VFL, H5E_CANTDECODE, FAIL, "can't decode history (final)");

done:
    H5MM_xfree(buf);
    if (ret_value < 0)
        H5MM_xfree(history_out->record_locs);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_ingest_history() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_write_history
 *
 * Purpose:     Encode and write history to file at the given address.
 *
 * Returns:     Success:    Number of bytes written to destination file (always non-zero)
 *              Failure:    0
 *-----------------------------------------------------------------------------
 */
uint64_t
H5FD__onion_write_history(H5FD_onion_history_t *history, H5FD_t *file, haddr_t off_start,
                          haddr_t filesize_curr)
{
    uint32_t       _sum      = 0; /* Required by the API call but unused here */
    uint64_t       size      = 0;
    unsigned char *buf       = NULL;
    uint64_t       ret_value = 0;

    FUNC_ENTER_PACKAGE

    if (NULL == (buf = H5MM_malloc(H5FD_ONION_ENCODED_SIZE_HISTORY +
                                   (H5FD_ONION_ENCODED_SIZE_RECORD_POINTER * history->n_revisions))))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, 0, "can't allocate buffer for updated history");

    if (0 == (size = H5FD__onion_history_encode(history, buf, &_sum)))
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, 0, "problem encoding updated history");

    if ((size + off_start > filesize_curr) && (H5FD_set_eoa(file, H5FD_MEM_DRAW, off_start + size) < 0))
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, 0, "can't modify EOA for updated history");

    if (H5FD_write(file, H5FD_MEM_DRAW, off_start, size, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, 0, "can't write history as intended");

    ret_value = size;

done:
    H5MM_xfree(buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_write_history() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_history_decode
 *
 * Purpose:     Attempt to read a buffer and store it as a history
 *              structure.
 *
 *              Implementation must correspond with
 *              H5FD__onion_history_encode().
 *
 *              MUST BE CALLED TWICE:
 *              On the first call, n_records in the destination structure must
 *              be zero, and record_locs be NULL.
 *
 *              If the buffer is well-formed, the destination structure is
 *              tentatively populated with fixed-size values, and the number of
 *              bytes read are returned.
 *
 *              Prior to the second call, the user must allocate space for
 *              record_locs to hold n_records record-pointer structs.
 *
 *              Then the decode operation is called a second time, and all
 *              components will be populated (and again number of bytes read is
 *              returned).
 *
 * Return:      Success:    Number of bytes read from buffer
 *              Failure:    0
 *-----------------------------------------------------------------------------
 */
size_t H5_ATTR_NO_OPTIMIZE
H5FD__onion_history_decode(unsigned char *buf, H5FD_onion_history_t *history)
{
    uint32_t       ui32        = 0;
    uint32_t       sum         = 0;
    uint64_t       ui64        = 0;
    uint64_t       n_revisions = 0;
    uint8_t       *ui8p        = NULL;
    unsigned char *ptr         = NULL;
    size_t         ret_value   = 0;

    FUNC_ENTER_PACKAGE

    assert(buf != NULL);
    assert(history != NULL);
    assert(H5FD_ONION_HISTORY_VERSION_CURR == history->version);

    if (strncmp((const char *)buf, H5FD_ONION_HISTORY_SIGNATURE, 4))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "invalid signature");

    if (H5FD_ONION_HISTORY_VERSION_CURR != buf[4])
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "invalid version");

    ptr = buf + 8;

    H5MM_memcpy(&ui64, ptr, 8);
    ui8p = (uint8_t *)&ui64;
    UINT64DECODE(ui8p, n_revisions);
    ptr += 8;

    if (0 == history->n_revisions) {
        history->n_revisions = n_revisions;
        ptr += H5FD_ONION_ENCODED_SIZE_RECORD_POINTER * n_revisions;
    }
    else {
        if (history->n_revisions != n_revisions)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0,
                        "history argument suggests different revision count than encoded buffer");
        if (NULL == history->record_locs)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "list is NULL -- cannot populate");

        for (uint64_t i = 0; i < n_revisions; i++) {
            H5FD_onion_record_loc_t *rloc = &history->record_locs[i];

            /* Decode into appropriately sized types, then do a checked
             * assignment to the struct value. We don't have access to
             * the H5F_t struct for this file, so we can't use the
             * offset/length macros in H5Fprivate.h.
             */
            uint64_t record_size;
            uint64_t phys_addr;

            H5MM_memcpy(&ui64, ptr, 8);
            ui8p = (uint8_t *)&ui64;
            UINT64DECODE(ui8p, phys_addr);
            H5_CHECKED_ASSIGN(rloc->phys_addr, haddr_t, phys_addr, uint64_t);
            ptr += 8;

            H5MM_memcpy(&ui64, ptr, 8);
            ui8p = (uint8_t *)&ui64;
            UINT64DECODE(ui8p, record_size);
            H5_CHECKED_ASSIGN(rloc->record_size, hsize_t, record_size, uint64_t);
            ptr += 8;

            H5MM_memcpy(&ui32, ptr, 4);
            ui8p = (uint8_t *)&ui32;
            UINT32DECODE(ui8p, rloc->checksum);
            ptr += 4;
        }
    }

    sum = H5_checksum_fletcher32(buf, (size_t)(ptr - buf));

    H5MM_memcpy(&ui32, ptr, 4);
    ui8p = (uint8_t *)&ui32;
    UINT32DECODE(ui8p, history->checksum);
    ptr += 4;

    if (sum != history->checksum)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "checksum mismatch");

    ret_value = (size_t)(ptr - buf);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_history_decode() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_history_encode
 *
 * Purpose:     Write history structure to the given buffer.
 *              All multi-byte elements are stored in little-endian word order.
 *
 *              Implementation must correspond with
 *              H5FD__onion_history_decode().
 *
 *              The destination buffer must be sufficiently large to hold the
 *              encoded contents.
 *              (Hint: `sizeof(history struct) +
 *              sizeof(record-pointer-struct) * n_records)` guarantees
 *              ample/excess space.)
 *
 * Return:      Number of bytes written to buffer.
 *              The checksum of the generated buffer contents (excluding the
 *              checksum itself) is stored in the pointer `checksum`).
 *-----------------------------------------------------------------------------
 */
size_t
H5FD__onion_history_encode(H5FD_onion_history_t *history, unsigned char *buf, uint32_t *checksum)
{
    unsigned char *ptr      = buf;
    size_t         vers_u32 = (uint32_t)history->version; /* pad out unused bytes */

    FUNC_ENTER_PACKAGE_NOERR

    assert(history != NULL);
    assert(H5FD_ONION_HISTORY_VERSION_CURR == history->version);
    assert(buf != NULL);
    assert(checksum != NULL);

    H5MM_memcpy(ptr, H5FD_ONION_HISTORY_SIGNATURE, 4);
    ptr += 4;
    UINT32ENCODE(ptr, vers_u32);
    UINT64ENCODE(ptr, history->n_revisions);
    if (history->n_revisions > 0) {
        assert(history->record_locs != NULL);
        for (uint64_t i = 0; i < history->n_revisions; i++) {
            H5FD_onion_record_loc_t *rloc = &history->record_locs[i];

            /* Do a checked assignment from the struct value into appropriately
             * sized types. We don't have access to the H5F_t struct for this
             * file, so we can't use the offset/length macros in H5Fprivate.h.
             */
            uint64_t phys_addr;
            uint64_t record_size;

            H5_CHECKED_ASSIGN(phys_addr, uint64_t, rloc->phys_addr, haddr_t);
            H5_CHECKED_ASSIGN(record_size, uint64_t, rloc->record_size, hsize_t);

            UINT64ENCODE(ptr, phys_addr);
            UINT64ENCODE(ptr, record_size);
            UINT32ENCODE(ptr, rloc->checksum);
        }
    }
    *checksum = H5_checksum_fletcher32(buf, (size_t)(ptr - buf));
    UINT32ENCODE(ptr, *checksum);

    FUNC_LEAVE_NOAPI((size_t)(ptr - buf))
} /* end H5FD__onion_history_encode() */
