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
 * Purpose:     Code for the onion file's header
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
 * Function:    H5FD_ingest_header
 *
 * Purpose:     Read and decode the history header information from `raw_file`
 *              at `addr`, and store the decoded information in the structure
 *              at `hdr_out`.
 *
 * Return:      SUCCEED/FAIL
 *-----------------------------------------------------------------------------
 */
herr_t
H5FD__onion_ingest_header(H5FD_onion_header_t *hdr_out, H5FD_t *raw_file, haddr_t addr)
{
    unsigned char *buf       = NULL;
    herr_t         ret_value = SUCCEED;
    haddr_t        size      = (haddr_t)H5FD_ONION_ENCODED_SIZE_HEADER;
    uint32_t       sum       = 0;

    FUNC_ENTER_PACKAGE

    if (H5FD_get_eof(raw_file, H5FD_MEM_DRAW) < (addr + size))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "header indicates history beyond EOF");

    if (NULL == (buf = H5MM_malloc(sizeof(char) * size)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't allocate buffer space");

    if (H5FD_set_eoa(raw_file, H5FD_MEM_DRAW, (addr + size)) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't modify EOA");

    if (H5FD_read(raw_file, H5FD_MEM_DRAW, addr, size, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "can't read history header from file");

    if (H5FD__onion_header_decode(buf, hdr_out) == 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTDECODE, FAIL, "can't decode history header");

    sum = H5_checksum_fletcher32(buf, size - 4);
    if (hdr_out->checksum != sum)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "checksum mismatch between buffer and stored");

done:
    H5MM_xfree(buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_ingest_header() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__onion_write_header
 *
 * Purpose:     Write in-memory history header to appropriate backing file.
 *              Overwrites existing header data.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
herr_t
H5FD__onion_write_header(H5FD_onion_header_t *header, H5FD_t *file)
{
    uint32_t       sum       = 0; /* Not used, but required by the encoder */
    uint64_t       size      = 0;
    unsigned char *buf       = NULL;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    if (NULL == (buf = H5MM_malloc(H5FD_ONION_ENCODED_SIZE_HEADER)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't allocate buffer for updated history header");

    if (0 == (size = H5FD__onion_header_encode(header, buf, &sum)))
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "problem encoding updated history header");

    if (H5FD_write(file, H5FD_MEM_DRAW, 0, (haddr_t)size, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "can't write updated history header");

done:
    H5MM_xfree(buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_write_header()*/

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_header_decode
 *
 * Purpose:     Attempt to read a buffer and store it as a history-header
 *              structure.
 *
 *              Implementation must correspond with
 *              H5FD__onion_header_encode().
 *
 * Return:      Success:    Number of bytes read from buffer
 *              Failure:    0
 *-----------------------------------------------------------------------------
 */
size_t
H5FD__onion_header_decode(unsigned char *buf, H5FD_onion_header_t *header)
{
    uint32_t       ui32      = 0;
    uint32_t       sum       = 0;
    uint64_t       ui64      = 0;
    uint8_t       *ui8p      = NULL;
    unsigned char *ptr       = NULL;
    size_t         ret_value = 0;

    FUNC_ENTER_PACKAGE

    assert(buf != NULL);
    assert(header != NULL);
    assert(H5FD_ONION_HEADER_VERSION_CURR == header->version);

    if (strncmp((const char *)buf, H5FD_ONION_HEADER_SIGNATURE, 4))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "invalid header signature");

    if (buf[4] != H5FD_ONION_HEADER_VERSION_CURR)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "invalid header version");

    ptr  = buf + 5;
    ui32 = 0;
    H5MM_memcpy(&ui32, ptr, 3);
    ui8p = (uint8_t *)&ui32;
    UINT32DECODE(ui8p, header->flags);
    ptr += 3;

    H5MM_memcpy(&ui32, ptr, 4);
    ui8p = (uint8_t *)&ui32;
    UINT32DECODE(ui8p, header->page_size);
    ptr += 4;

    H5MM_memcpy(&ui64, ptr, 8);
    ui8p = (uint8_t *)&ui64;
    UINT32DECODE(ui8p, header->origin_eof);
    ptr += 8;

    H5MM_memcpy(&ui64, ptr, 8);
    ui8p = (uint8_t *)&ui64;
    UINT32DECODE(ui8p, header->history_addr);
    ptr += 8;

    H5MM_memcpy(&ui64, ptr, 8);
    ui8p = (uint8_t *)&ui64;
    UINT32DECODE(ui8p, header->history_size);
    ptr += 8;

    sum = H5_checksum_fletcher32(buf, (size_t)(ptr - buf));

    H5MM_memcpy(&ui32, ptr, 4);
    ui8p = (uint8_t *)&ui32;
    UINT32DECODE(ui8p, header->checksum);
    ptr += 4;

    if (sum != header->checksum)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "checksum mismatch");

    ret_value = (size_t)(ptr - buf);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_header_decode() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_header_encode
 *
 * Purpose:     Write history-header structure to the given buffer.
 *              All multi-byte elements are stored in little-endian word order.
 *
 *              Implementation must correspond with
 *              H5FD__onion_header_decode().
 *
 *              The destination buffer must be sufficiently large to hold the
 *              encoded contents (H5FD_ONION_ENCODED_SIZE_HEADER).
 *
 * Return:      Number of bytes written to buffer.
 *              The checksum of the generated buffer contents (excluding the
 *              checksum itself) is stored in the pointer `checksum`).
 *-----------------------------------------------------------------------------
 */
size_t
H5FD__onion_header_encode(H5FD_onion_header_t *header, unsigned char *buf, uint32_t *checksum /*out*/)
{
    unsigned char *ptr       = buf;
    size_t         ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    assert(buf != NULL);
    assert(checksum != NULL);
    assert(header != NULL);
    assert(H5FD_ONION_HEADER_VERSION_CURR == header->version);
    assert(0 == (header->flags & 0xFF000000)); /* max three bits long */

    H5MM_memcpy(ptr, H5FD_ONION_HEADER_SIGNATURE, 4);
    ptr += 4;
    H5MM_memcpy(ptr, (unsigned char *)&header->version, 1);
    ptr += 1;
    UINT32ENCODE(ptr, header->flags);
    ptr -= 1; /* truncate to three bytes */
    UINT32ENCODE(ptr, header->page_size);
    UINT64ENCODE(ptr, header->origin_eof);
    UINT64ENCODE(ptr, header->history_addr);
    UINT64ENCODE(ptr, header->history_size);
    *checksum = H5_checksum_fletcher32(buf, (size_t)(ptr - buf));
    UINT32ENCODE(ptr, *checksum);
    ret_value = (size_t)(ptr - buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_header_encode() */
