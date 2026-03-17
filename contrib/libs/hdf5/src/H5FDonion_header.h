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
 * Purpose:     Interface for the onion file's header
 */

#ifndef H5FDonion_header_H
#define H5FDonion_header_H

/* Number of bytes to encode fixed-size components */
#define H5FD_ONION_ENCODED_SIZE_HEADER 40

/* Flags must align exactly one per bit, up to 24 bits */
#define H5FD_ONION_HEADER_FLAG_WRITE_LOCK     0x1
#define H5FD_ONION_HEADER_FLAG_PAGE_ALIGNMENT 0x2
#define H5FD_ONION_HEADER_SIGNATURE           "OHDH"
#define H5FD_ONION_HEADER_VERSION_CURR        1

/* In-memory representation of the on-store onion history file header.
 */
typedef struct H5FD_onion_header_t {
    uint8_t  version;
    uint32_t flags; /* At most three bytes used! */
    uint32_t page_size;
    uint64_t origin_eof; /* Size of the 'original' canonical file */
    uint64_t history_addr;
    uint64_t history_size;
    uint32_t checksum;
} H5FD_onion_header_t;

#ifdef __cplusplus
extern "C" {
#endif
H5_DLL herr_t H5FD__onion_ingest_header(H5FD_onion_header_t *hdr_out, H5FD_t *raw_file, haddr_t addr);
H5_DLL herr_t H5FD__onion_write_header(H5FD_onion_header_t *header, H5FD_t *file);
H5_DLL size_t H5FD__onion_header_decode(unsigned char *buf, H5FD_onion_header_t *header);
H5_DLL size_t H5FD__onion_header_encode(H5FD_onion_header_t *header, unsigned char *buf, uint32_t *checksum);

#ifdef __cplusplus
}
#endif

#endif /* H5FDonion_header_H */
