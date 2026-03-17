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

#ifndef H5FDonion_index_H
#define H5FDonion_index_H

#define H5FD_ONION_ARCHIVAL_INDEX_VERSION_CURR 1

/* Number of bytes to encode fixed-size components */
#define H5FD_ONION_ENCODED_SIZE_INDEX_ENTRY     20
#define H5FD_ONION_ENCODED_SIZE_RECORD_POINTER  20
#define H5FD_ONION_ENCODED_SIZE_REVISION_RECORD 68

#define H5FD_ONION_REVISION_INDEX_HASH_CHAIN_NODE_VERSION_CURR 1
#define H5FD_ONION_REVISION_INDEX_STARTING_SIZE_LOG2           10 /* 2^n slots */
#define H5FD_ONION_REVISION_INDEX_VERSION_CURR                 1

#define H5FD_ONION_REVISION_RECORD_SIGNATURE    "ORRS"
#define H5FD_ONION_REVISION_RECORD_VERSION_CURR 1

/*
 * Onion Virtual File Driver (VFD)
 *
 * Purpose:     Interface for the archival and revision indexes
 */

/*-----------------------------------------------------------------------------
 *
 * Structure    H5FD__onion_index_entry
 *
 * Purpose:     Map a page in the logical file to a 'physical address' in the
 *              onion file.
 *
 * logical_page:
 *
 *              Page 'id' in the logical file.
 *
 * phys_addr:
 *
 *              Address/offset of start of page in the onion file.
 *
 *-----------------------------------------------------------------------------
 */
typedef struct H5FD_onion_index_entry_t {
    uint64_t logical_page;
    haddr_t  phys_addr;
} H5FD_onion_index_entry_t;

/*-----------------------------------------------------------------------------
 *
 * Structure    H5FD__onion_archival_index
 *
 * Purpose:     Encapsulate archival index and associated data.
 *              Convenience structure with sanity-checking components.
 *
 * version:     Future-proofing identifier. Informs struct membership.
 *              Must equal H5FD_ONION_ARCHIVAL_INDEX_VERSION_CURR to be
 *              considered valid.
 *
 * page_size:   Interval to which the `logical_page` component of each list
 *              entry must align.
 *              Value is taken from the onion history data; must not change
 *              following onionization or file or creation of onion file.
 *
 * n_entries:   Number of entries in the list.
 *
 * list:        Pointer to array of archival index entries.
 *              Cannot be NULL.
 *              Entries must be sorted by `logical_page_id` in ascending order.
 *
 *-----------------------------------------------------------------------------
 */
typedef struct H5FD_onion_archival_index_t {
    uint8_t                   version;
    uint32_t                  page_size_log2;
    uint64_t                  n_entries;
    H5FD_onion_index_entry_t *list;
} H5FD_onion_archival_index_t;

/* data structure for storing index entries at a hash key collision */
/* version 1 implements a singly-linked list */
typedef struct H5FD_onion_revision_index_hash_chain_node_t H5FD_onion_revision_index_hash_chain_node_t;
struct H5FD_onion_revision_index_hash_chain_node_t {
    uint8_t                                      version;
    H5FD_onion_index_entry_t                     entry_data;
    H5FD_onion_revision_index_hash_chain_node_t *next;
};

typedef struct H5FD_onion_revision_index_t {
    uint8_t                                       version;
    uint32_t                                      page_size_log2;
    uint64_t                                      n_entries;             /* count of all entries in table */
    uint64_t                                      _hash_table_size;      /* 'slots' in hash table */
    uint64_t                                      _hash_table_size_log2; /* 2^(n) -> 'slots' in hash table */
    uint64_t                                      _hash_table_n_keys_populated; /* count of slots not NULL */
    H5FD_onion_revision_index_hash_chain_node_t **_hash_table;
} H5FD_onion_revision_index_t;

/* In-memory representation of the on-store revision record.
 */
typedef struct H5FD_onion_revision_record_t {
    uint8_t                     version;
    uint64_t                    revision_num;
    uint64_t                    parent_revision_num;
    char                        time_of_creation[16];
    uint64_t                    logical_eof;
    H5FD_onion_archival_index_t archival_index;
    uint32_t                    comment_size;
    char                       *comment;
    uint32_t                    checksum;
} H5FD_onion_revision_record_t;

#ifdef __cplusplus
extern "C" {
#endif
H5_DLL herr_t H5FD__onion_ingest_revision_record(H5FD_onion_revision_record_t *r_out, H5FD_t *raw_file,
                                                 const H5FD_onion_history_t *history, uint64_t revision_num);

H5_DLL bool H5FD__onion_archival_index_is_valid(const H5FD_onion_archival_index_t *);
H5_DLL int  H5FD__onion_archival_index_find(const H5FD_onion_archival_index_t *, uint64_t,
                                            const H5FD_onion_index_entry_t **);

H5_DLL H5FD_onion_revision_index_t *H5FD__onion_revision_index_init(uint32_t page_size);
H5_DLL herr_t                       H5FD__onion_revision_index_destroy(H5FD_onion_revision_index_t *);
H5_DLL herr_t                       H5FD__onion_revision_index_insert(H5FD_onion_revision_index_t *,
                                                                      const H5FD_onion_index_entry_t *);
H5_DLL int H5FD__onion_revision_index_find(const H5FD_onion_revision_index_t *, uint64_t,
                                           const H5FD_onion_index_entry_t **);

H5_DLL herr_t H5FD__onion_merge_revision_index_into_archival_index(const H5FD_onion_revision_index_t *,
                                                                   H5FD_onion_archival_index_t *);

H5_DLL size_t H5FD__onion_revision_record_decode(unsigned char *buf, H5FD_onion_revision_record_t *record);
H5_DLL size_t H5FD__onion_revision_record_encode(H5FD_onion_revision_record_t *record, unsigned char *buf,
                                                 uint32_t *checksum);

#ifdef __cplusplus
}
#endif

#endif /* H5FDonion_index_H */
