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
 * Purpose:     Code for the archival and revision indexes
 */

/* This source code file is part of the H5FD driver module */
#include "H5FDdrvr_module.h"

#include "H5private.h"      /* Generic Functions                        */
#include "H5Eprivate.h"     /* Error handling                           */
#include "H5FDprivate.h"    /* File drivers                             */
#include "H5FDonion.h"      /* Onion file driver                        */
#include "H5FDonion_priv.h" /* Onion file driver internals              */
#include "H5MMprivate.h"    /* Memory management                        */

/* 2^n for uint64_t types -- H5_EXP2 unsafe past 32 bits */
#define U64_EXP2(n) ((uint64_t)1 << (n))

static int    H5FD__onion_archival_index_list_sort_cmp(const void *, const void *);
static herr_t H5FD__onion_revision_index_resize(H5FD_onion_revision_index_t *rix);

/*-----------------------------------------------------------------------------
 * Read and decode the revision_record information from `raw_file` at
 * `addr` .. `addr + size` (taken from history), and store the decoded
 * information in the structure at `r_out`.
 *-----------------------------------------------------------------------------
 */
herr_t
H5FD__onion_ingest_revision_record(H5FD_onion_revision_record_t *r_out, H5FD_t *raw_file,
                                   const H5FD_onion_history_t *history, uint64_t revision_num)
{
    unsigned char *buf       = NULL;
    herr_t         ret_value = SUCCEED;
    uint64_t       n         = 0;
    uint64_t       high      = 0;
    uint64_t       low       = 0;
    uint64_t       range     = 0;
    uint32_t       sum       = 0;
    haddr_t        addr      = 0;
    size_t         size      = 0;

    FUNC_ENTER_PACKAGE

    assert(r_out);
    assert(raw_file);
    assert(history);
    assert(history->record_locs);
    assert(history->n_revisions > 0);

    high  = history->n_revisions - 1;
    range = high;
    addr  = history->record_locs[high].phys_addr;
    size  = history->record_locs[high].record_size;

    /* Initialize r_out
     *
     * TODO: This function should completely initialize r_out. Relying on
     *       other code to some of the work while we just paste over parts
     *       of the struct here is completely bananas.
     */
    r_out->comment             = H5MM_xfree(r_out->comment);
    r_out->archival_index.list = H5MM_xfree(r_out->archival_index.list);

    if (H5FD_get_eof(raw_file, H5FD_MEM_DRAW) < (addr + size))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "at least one record extends beyond EOF");

    /* recovery-open may have EOA below revision record */
    if ((H5FD_get_eoa(raw_file, H5FD_MEM_DRAW) < (addr + size)) &&
        (H5FD_set_eoa(raw_file, H5FD_MEM_DRAW, (addr + size)) < 0)) {
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't modify EOA");
    }

    /* Perform binary search on records to find target revision by ID.
     * As IDs are added sequentially, they are guaranteed to be sorted.
     */
    while (range > 0) {
        n    = (range / 2) + low;
        addr = history->record_locs[n].phys_addr;
        size = history->record_locs[n].record_size;

        if (NULL == (buf = H5MM_malloc(sizeof(char) * size)))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't allocate buffer space");

        if (H5FD_read(raw_file, H5FD_MEM_DRAW, addr, size, buf) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "can't read revision record from file");

        if (H5FD__onion_revision_record_decode(buf, r_out) != size)
            HGOTO_ERROR(H5E_VFL, H5E_CANTDECODE, FAIL, "can't decode revision record (initial)");

        sum = H5_checksum_fletcher32(buf, size - 4);
        if (r_out->checksum != sum)
            HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "checksum mismatch between buffer and stored");

        if (revision_num == r_out->revision_num)
            break;

        H5MM_xfree(buf);
        buf = NULL;

        r_out->archival_index.n_entries = 0;
        r_out->comment_size             = 0;

        if (r_out->revision_num < revision_num)
            low = (n == high) ? high : n + 1;
        else
            high = (n == low) ? low : n - 1;
        range = high - low;
    } /* end while 'non-leaf' binary search */

    if (range == 0) {
        n    = low;
        addr = history->record_locs[n].phys_addr;
        size = history->record_locs[n].record_size;

        if (NULL == (buf = H5MM_malloc(sizeof(char) * size)))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't allocate buffer space");

        if (H5FD_read(raw_file, H5FD_MEM_DRAW, addr, size, buf) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "can't read revision record from file");

        if (H5FD__onion_revision_record_decode(buf, r_out) != size)
            HGOTO_ERROR(H5E_VFL, H5E_CANTDECODE, FAIL, "can't decode revision record (initial)");

        sum = H5_checksum_fletcher32(buf, size - 4);
        if (r_out->checksum != sum)
            HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "checksum mismatch between buffer and stored");

        if (revision_num != r_out->revision_num)
            HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL,
                        "could not find target revision!"); /* TODO: corrupted? */
    } /* end if revision ID at 'leaf' in binary search */

    if (r_out->comment_size > 0)
        if (NULL == (r_out->comment = H5MM_malloc(sizeof(char) * r_out->comment_size)))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't allocate comment space");

    if (r_out->archival_index.n_entries > 0)
        if (NULL == (r_out->archival_index.list =
                         H5MM_calloc(r_out->archival_index.n_entries * sizeof(H5FD_onion_index_entry_t))))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "can't allocate index entry list");

    if (H5FD__onion_revision_record_decode(buf, r_out) != size)
        HGOTO_ERROR(H5E_VFL, H5E_CANTDECODE, FAIL, "can't decode revision record (final)");

done:
    H5MM_xfree(buf);
    if (ret_value == FAIL) {
        H5MM_xfree(r_out->comment);
        H5MM_xfree(r_out->archival_index.list);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_ingest_revision_record() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_archival_index_is_valid
 *
 * Purpose:     Determine whether an archival index structure is valid.
 *
 *              + Verify page size (power of two).
 *              + Verify list exists.
 *              + Verify list contents:
 *                + Sorted by increasing logical address (no duplicates)
 *                + Logical addresses are multiples of page size.
 *
 * Return:      true/false
 *-----------------------------------------------------------------------------
 */
bool
H5FD__onion_archival_index_is_valid(const H5FD_onion_archival_index_t *aix)
{
    bool ret_value = true;

    FUNC_ENTER_PACKAGE_NOERR

    assert(aix);

    if (H5FD_ONION_ARCHIVAL_INDEX_VERSION_CURR != aix->version)
        HGOTO_DONE(false);
    if (NULL == aix->list)
        HGOTO_DONE(false);

    /* Ensure list is sorted on logical_page field */
    if (aix->n_entries > 1)
        for (uint64_t i = 1; i < aix->n_entries - 1; i++)
            if (aix->list[i + 1].logical_page <= aix->list[i].logical_page)
                HGOTO_DONE(false);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_archival_index_is_valid() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_archival_index_find
 *
 * Purpose:     Retrieve the archival index entry by logical page ID.
 *
 *              The archival index pointer must point to a valid index entry.
 *              The entry out pointer-pointer cannot be null.
 *
 * Return:      Success: Positive value (1) -- entry found.
 *                       Entry out pointer-pointer is set to point to entry.
 *              Failure: Zero (0) -- entry not found.
 *                       Entry out pointer-pointer is unmodified.
 *-----------------------------------------------------------------------------
 */
int
H5FD__onion_archival_index_find(const H5FD_onion_archival_index_t *aix, uint64_t logical_page,
                                const H5FD_onion_index_entry_t **entry_out)
{
    uint64_t                  low       = 0;
    uint64_t                  high      = 0;
    uint64_t                  n         = 0;
    uint64_t                  range     = 0;
    H5FD_onion_index_entry_t *x         = NULL;
    int                       ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    assert(aix);
    assert(H5FD_ONION_ARCHIVAL_INDEX_VERSION_CURR == aix->version);
    assert(entry_out);
    if (aix->n_entries != 0)
        assert(aix->list);

    high  = aix->n_entries - 1;
    range = high;

    /* Trivial cases */
    if (aix->n_entries == 0 || logical_page > aix->list[high].logical_page ||
        logical_page < aix->list[0].logical_page)
        HGOTO_DONE(0);

    /*
     * Binary search on sorted list
     */

    /* Winnow down to first of found or one element */
    while (range > 0) {
        assert(high < aix->n_entries);
        n = low + (range / 2);
        x = &(aix->list[n]);
        if (x->logical_page == logical_page) {
            *entry_out = x; /* element found at fence */
            ret_value  = 1;
            goto done;
        }
        else if (x->logical_page < logical_page) {
            low = (n == high) ? high : n + 1;
        }
        else {
            high = (n == low) ? low : n - 1;
        }
        range = high - low;
    }

    assert(high == low); /* one element */

    /* n == low/high check because we may have tested it already above */
    if ((n != low || n != high) && (aix->list[low].logical_page == logical_page)) {
        *entry_out = &aix->list[low];
        ret_value  = 1;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_archival_index_find() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_revision_index_destroy
 *
 * Purpose:     Release all resources of a revision index.
 *
 * Return:      SUCCEED/FAIL
 *-----------------------------------------------------------------------------
 */
herr_t
H5FD__onion_revision_index_destroy(H5FD_onion_revision_index_t *rix)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_NOERR

    assert(rix);
    assert(H5FD_ONION_REVISION_INDEX_VERSION_CURR == rix->version);

    for (size_t i = 0; 0 < rix->_hash_table_n_keys_populated && i < rix->_hash_table_size; i++) {
        H5FD_onion_revision_index_hash_chain_node_t *next = NULL;
        H5FD_onion_revision_index_hash_chain_node_t *node = rix->_hash_table[i];

        if (node != NULL)
            rix->_hash_table_n_keys_populated -= 1;

        while (node != NULL) {
            assert(H5FD_ONION_REVISION_INDEX_HASH_CHAIN_NODE_VERSION_CURR == node->version);

            next = node->next;
            H5MM_xfree(node);
            node = next;
        }
    }
    H5MM_xfree(rix->_hash_table);
    H5MM_xfree(rix);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_revision_index_destroy() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_revision_index_init
 *
 * Purpose:     Initialize a revision index structure with a default starting
 *              size. A new structure is allocated and populated with initial
 *              values.
 *
 * Return:      Success:    Pointer to newly-allocated structure
 *              Failure:    NULL
 *-----------------------------------------------------------------------------
 */
H5FD_onion_revision_index_t *
H5FD__onion_revision_index_init(uint32_t page_size)
{
    uint64_t                     table_size = U64_EXP2(H5FD_ONION_REVISION_INDEX_STARTING_SIZE_LOG2);
    H5FD_onion_revision_index_t *rix        = NULL;
    H5FD_onion_revision_index_t *ret_value  = NULL;

    FUNC_ENTER_PACKAGE

    assert(0 != page_size);
    assert(POWER_OF_TWO(page_size));

    if (NULL == (rix = H5MM_calloc(sizeof(H5FD_onion_revision_index_t))))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "cannot allocate index");

    if (NULL ==
        (rix->_hash_table = H5MM_calloc(table_size * sizeof(H5FD_onion_revision_index_hash_chain_node_t *))))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "cannot allocate hash table");

    rix->version   = H5FD_ONION_REVISION_INDEX_VERSION_CURR;
    rix->n_entries = 0;
    /* Compute and store log2(page_size) */
    for (rix->page_size_log2 = 0; (((uint32_t)1 << rix->page_size_log2) & page_size) == 0;
         rix->page_size_log2++)
        ;
    rix->_hash_table_size             = table_size;
    rix->_hash_table_size_log2        = H5FD_ONION_REVISION_INDEX_STARTING_SIZE_LOG2;
    rix->_hash_table_n_keys_populated = 0;

    ret_value = rix;

done:
    if (NULL == ret_value)
        H5MM_xfree(rix);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_revision_index_init() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_revision_index_resize()
 *
 * Purpose:     Replace the hash table in the revision index.
 *
 *              Doubles the available number of keys, re-hashes table contents,
 *              and updates relevant components in the index structure.
 *
 *              Fails if unable to allocate space for larger hash table.
 *
 * Return:      SUCCEED/FAIL
 *-----------------------------------------------------------------------------
 */
static herr_t
H5FD__onion_revision_index_resize(H5FD_onion_revision_index_t *rix)
{
    H5FD_onion_revision_index_hash_chain_node_t **new_table = NULL;

    uint64_t new_size_log2        = rix->_hash_table_size_log2 + 1;
    uint64_t new_size             = U64_EXP2(new_size_log2);
    uint64_t new_n_keys_populated = 0;
    herr_t   ret_value            = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(rix);
    assert(H5FD_ONION_REVISION_INDEX_VERSION_CURR == rix->version);
    assert(rix->_hash_table);

    if (NULL == (new_table = H5MM_calloc(new_size * sizeof(H5FD_onion_revision_index_hash_chain_node_t *))))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "cannot allocate new hash table");

    for (uint64_t i = 0; i < rix->_hash_table_size; i++) {
        while (rix->_hash_table[i] != NULL) {
            H5FD_onion_revision_index_hash_chain_node_t *node = NULL;
            uint64_t                                     key  = 0;

            /* Pop entry off of bucket stack and re-hash */
            node                = rix->_hash_table[i];
            rix->_hash_table[i] = node->next;
            node->next          = NULL;
            key                 = node->entry_data.logical_page & (new_size - 1);

            if (NULL == new_table[key]) {
                new_table[key] = node;
                new_n_keys_populated++;
            }
            else {
                node->next   = new_table[i];
                new_table[i] = node;
            }
        }
    }

    H5MM_xfree(rix->_hash_table);
    rix->_hash_table_size             = new_size;
    rix->_hash_table_size_log2        = new_size_log2;
    rix->_hash_table_n_keys_populated = new_n_keys_populated;
    rix->_hash_table                  = new_table;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_revision_index_resize() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_revision_index_insert()
 *
 * Purpose:     Add an entry to the revision index, or update an existing
 *              entry. Must be used to update entries as well as add --
 *              checksum value will change.
 *
 *              Entry data is copied into separate memory region; user pointer
 *              can be safley re-used or discarded after operation.
 *
 * Return:      SUCCEED/FAIL
 *-----------------------------------------------------------------------------
 */
herr_t
H5FD__onion_revision_index_insert(H5FD_onion_revision_index_t *rix, const H5FD_onion_index_entry_t *entry)
{
    uint64_t                                      key         = 0;
    H5FD_onion_revision_index_hash_chain_node_t  *node        = NULL;
    H5FD_onion_revision_index_hash_chain_node_t **append_dest = NULL;
    herr_t                                        ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(rix);
    assert(H5FD_ONION_REVISION_INDEX_VERSION_CURR == rix->version);
    assert(entry);

    /* Resize and re-hash table if necessary */
    if (rix->n_entries >= (rix->_hash_table_size * 2) ||
        rix->_hash_table_n_keys_populated >= (rix->_hash_table_size / 2)) {
        if (H5FD__onion_revision_index_resize(rix) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_NONE_MINOR, FAIL, "unable to resize and hash table");
    }

    key = entry->logical_page & (rix->_hash_table_size - 1);
    assert(key < rix->_hash_table_size);

    if (NULL == rix->_hash_table[key]) {
        /* Key maps to empty bucket */

        append_dest = &rix->_hash_table[key];
        rix->_hash_table_n_keys_populated++;
    }
    else {
        /* Key maps to populated bucket */

        for (node = rix->_hash_table[key]; node != NULL; node = node->next) {
            append_dest = &node->next; /* look for bucket tail */
            if (entry->logical_page == node->entry_data.logical_page) {
                if (entry->phys_addr != node->entry_data.phys_addr) {
                    HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "physical address mismatch");
                }
                H5MM_memcpy(&node->entry_data, entry, sizeof(H5FD_onion_index_entry_t));
                append_dest = NULL; /* Node updated, do not append */
                break;
            }
        }
    }

    /* Add new entry to bucket chain */
    if (append_dest != NULL) {
        if (NULL == (node = H5MM_malloc(sizeof(H5FD_onion_revision_index_hash_chain_node_t))))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "cannot allocate new ash chain node");
        node->version = H5FD_ONION_REVISION_INDEX_HASH_CHAIN_NODE_VERSION_CURR;
        node->next    = NULL;
        H5MM_memcpy(&node->entry_data, entry, sizeof(H5FD_onion_index_entry_t));
        *append_dest = node;
        rix->n_entries++;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_revision_index_insert() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_revision_index_find()
 *
 *
 * Purpose:     Get pointer to revision index entry with the given page number,
 *              if it exists in the index.
 *
 * Return:      Success: Positive value (1) -- entry found.
 *                       Entry out pointer-pointer is set to point to entry.
 *              Failure: Zero (0) -- entry not found.
 *                       Entry out pointer-pointer is unmodified.
 *-----------------------------------------------------------------------------
 */
int
H5FD__onion_revision_index_find(const H5FD_onion_revision_index_t *rix, uint64_t logical_page,
                                const H5FD_onion_index_entry_t **entry_out)
{
    uint64_t key       = 0;
    int      ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    assert(rix);
    assert(H5FD_ONION_REVISION_INDEX_VERSION_CURR == rix->version);
    assert(rix->_hash_table);
    assert(entry_out);

    key = logical_page & (rix->_hash_table_size - 1);
    assert(key < rix->_hash_table_size);

    if (rix->_hash_table[key] != NULL) {
        H5FD_onion_revision_index_hash_chain_node_t *node = NULL;

        for (node = rix->_hash_table[key]; node != NULL; node = node->next) {
            if (logical_page == node->entry_data.logical_page) {
                *entry_out = &node->entry_data;
                ret_value  = 1;
                break;
            }
        }
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_revision_index_find() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_revision_record_decode
 *
 * Purpose:     Attempt to read a buffer and store it as a revision record
 *              structure.
 *
 *              Implementation must correspond with
 *              H5FD__onion_revision_record_encode().
 *
 *              MUST BE CALLED TWICE:
 *              On the first call, n_entries and comment_size in the
 *              destination structure must all all be zero, and their
 *              respective variable-length components (index_entry_list,
 *              comment) must all be NULL.
 *
 *              If the buffer is well-formed, the destination structure is
 *              tentatively populated with fixed-size values, and the number of
 *              bytes read are returned.
 *
 *              Prior to the second call, the user must allocate space for the
 *              variable-length components, in accordance with the associated
 *              indicators (array of index-entry structures for
 *              index_entry_list, of size n_entries; character arrays for
 *              comment, allocated with the *_size number of bytes -- space
 *              for NULL-terminator is included in _size).
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
H5FD__onion_revision_record_decode(unsigned char *buf, H5FD_onion_revision_record_t *record)
{
    uint32_t       ui32         = 0;
    uint32_t       page_size    = 0;
    uint32_t       sum          = 0;
    uint64_t       ui64         = 0;
    uint64_t       n_entries    = 0;
    uint32_t       comment_size = 0;
    uint8_t       *ui8p         = NULL;
    unsigned char *ptr          = NULL;
    size_t         ret_value    = 0;

    FUNC_ENTER_PACKAGE

    assert(buf != NULL);
    assert(record != NULL);
    assert(H5FD_ONION_REVISION_RECORD_VERSION_CURR == record->version);
    assert(H5FD_ONION_ARCHIVAL_INDEX_VERSION_CURR == record->archival_index.version);

    if (strncmp((const char *)buf, H5FD_ONION_REVISION_RECORD_SIGNATURE, 4))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "invalid signature");

    if (H5FD_ONION_REVISION_RECORD_VERSION_CURR != buf[4])
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "invalid record version");

    ptr = buf + 8;

    H5MM_memcpy(&ui64, ptr, 8);
    ui8p = (uint8_t *)&ui64;
    UINT64DECODE(ui8p, record->revision_num);
    ptr += 8;

    H5MM_memcpy(&ui64, ptr, 8);
    ui8p = (uint8_t *)&ui64;
    UINT64DECODE(ui8p, record->parent_revision_num);
    ptr += 8;

    H5MM_memcpy(record->time_of_creation, ptr, 16);
    ptr += 16;

    H5MM_memcpy(&ui64, ptr, 8);
    ui8p = (uint8_t *)&ui64;
    UINT64DECODE(ui8p, record->logical_eof);
    ptr += 8;

    H5MM_memcpy(&ui32, ptr, 4);
    ui8p = (uint8_t *)&ui32;
    UINT32DECODE(ui8p, page_size);
    ptr += 4;

    if (page_size == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "page size is zero");
    if (!POWER_OF_TWO(page_size))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "page size not power of two");

    for (record->archival_index.page_size_log2 = 0;
         (((uint32_t)1 << record->archival_index.page_size_log2) & page_size) == 0;
         record->archival_index.page_size_log2++)
        ;

    H5MM_memcpy(&ui64, ptr, 8);
    ui8p = (uint8_t *)&ui64;
    UINT64DECODE(ui8p, n_entries);
    ptr += 8;

    H5MM_memcpy(&ui32, ptr, 4);
    ui8p = (uint8_t *)&ui32;
    UINT32DECODE(ui8p, comment_size);
    ptr += 4;

    if (record->archival_index.n_entries == 0) {
        record->archival_index.n_entries = n_entries;
        ptr += H5FD_ONION_ENCODED_SIZE_INDEX_ENTRY * n_entries;
    }
    else if (n_entries != record->archival_index.n_entries) {
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "n_entries in archival index does not match decoded");
    }
    else {
        H5FD_onion_index_entry_t *entry = NULL;

        if (record->archival_index.list == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "no archival index entry list");

        for (size_t i = 0; i < n_entries; i++) {
            entry = &record->archival_index.list[i];

            H5MM_memcpy(&ui64, ptr, 8);
            ui8p = (uint8_t *)&ui64;
            UINT64DECODE(ui8p, entry->logical_page);
            ptr += 8;

            /* logical_page actually encoded as address; check and convert */
            if (entry->logical_page & (page_size - 1))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "logical address does not align with page size");

            entry->logical_page = entry->logical_page >> record->archival_index.page_size_log2;

            H5MM_memcpy(&ui64, ptr, 8);
            ui8p = (uint8_t *)&ui64;
            UINT64DECODE(ui8p, entry->phys_addr);
            ptr += 8;

            H5MM_memcpy(&ui32, ptr, 4);
            ui8p = (uint8_t *)&ui32;
            UINT32DECODE(ui8p, sum);
            ptr += 4;

            ui32 = H5_checksum_fletcher32((ptr - 20), 16);
            if (ui32 != sum)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "index entry checksum mismatch");
        }
    }

    if (record->comment_size == 0) {
        if (record->comment != NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "comment pointer prematurely allocated");
        record->comment_size = comment_size;
    }
    else {
        if (record->comment == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "no comment pointer");
        H5MM_memcpy(record->comment, ptr, comment_size);
    }
    ptr += comment_size;

    sum = H5_checksum_fletcher32(buf, (size_t)(ptr - buf));

    H5MM_memcpy(&ui32, ptr, 4);
    ui8p = (uint8_t *)&ui32;
    UINT32DECODE(ui8p, record->checksum);
    ptr += 4;

    if (sum != record->checksum)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "checksum mismatch");

    ret_value = (size_t)(ptr - buf);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_revision_record_decode() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_revision_record_encode
 *
 * Purpose:     Write revision-record structure to the given buffer.
 *              All multi-byte elements are stored in little-endian word order.
 *
 *              Implementation must correspond with
 *              H5FD__onion_revision_record_decode().
 *
 *              The destination buffer must be sufficiently large to hold the
 *              encoded contents.
 *              (Hint: `sizeof(revision-record-struct) + comment-size +
 *              sizeof(index-entry-struct) * n_entries)`
 *              guarantees ample/excess space.)
 *
 * Return:      Number of bytes written to buffer.
 *              The checksum of the generated buffer contents (excluding the
 *              checksum itself) is stored in the pointer `checksum`).
 *-----------------------------------------------------------------------------
 */
size_t
H5FD__onion_revision_record_encode(H5FD_onion_revision_record_t *record, unsigned char *buf,
                                   uint32_t *checksum)
{
    unsigned char *ptr       = buf;                       /* original pointer */
    uint32_t       vers_u32  = (uint32_t)record->version; /* pad out unused bytes */
    uint32_t       page_size = 0;

    FUNC_ENTER_PACKAGE_NOERR

    assert(checksum != NULL);
    assert(buf != NULL);
    assert(record != NULL);
    assert(vers_u32 < 0x100);
    assert(H5FD_ONION_REVISION_RECORD_VERSION_CURR == record->version);
    assert(H5FD_ONION_ARCHIVAL_INDEX_VERSION_CURR == record->archival_index.version);

    page_size = (uint32_t)(1 << record->archival_index.page_size_log2);

    H5MM_memcpy(ptr, H5FD_ONION_REVISION_RECORD_SIGNATURE, 4);
    ptr += 4;
    UINT32ENCODE(ptr, vers_u32);
    UINT64ENCODE(ptr, record->revision_num);
    UINT64ENCODE(ptr, record->parent_revision_num);
    H5MM_memcpy(ptr, record->time_of_creation, 16);
    ptr += 16;
    UINT64ENCODE(ptr, record->logical_eof);
    UINT32ENCODE(ptr, page_size);
    UINT64ENCODE(ptr, record->archival_index.n_entries);
    UINT32ENCODE(ptr, record->comment_size);

    if (record->archival_index.n_entries > 0) {
        uint64_t page_size_log2 = record->archival_index.page_size_log2;

        assert(record->archival_index.list != NULL);
        for (uint64_t i = 0; i < record->archival_index.n_entries; i++) {
            uint32_t                  sum       = 0;
            H5FD_onion_index_entry_t *entry     = NULL;
            uint64_t                  logi_addr = 0;

            entry     = &record->archival_index.list[i];
            logi_addr = entry->logical_page << page_size_log2;

            UINT64ENCODE(ptr, logi_addr);
            UINT64ENCODE(ptr, entry->phys_addr);
            sum = H5_checksum_fletcher32((ptr - 16), 16);
            UINT32ENCODE(ptr, sum);
        }
    }

    if (record->comment_size > 0) {
        assert(record->comment != NULL && *record->comment != '\0');
        H5MM_memcpy(ptr, record->comment, record->comment_size);
        ptr += record->comment_size;
    }

    *checksum = H5_checksum_fletcher32(buf, (size_t)(ptr - buf));
    UINT32ENCODE(ptr, *checksum);

    FUNC_LEAVE_NOAPI((size_t)(ptr - buf))
} /* end H5FD__onion_revision_record_encode() */

/*-----------------------------------------------------------------------------
 * Callback for comparisons in sorting archival index entries by logical_page.
 *-----------------------------------------------------------------------------
 */
static int
H5FD__onion_archival_index_list_sort_cmp(const void *_a, const void *_b)
{
    const H5FD_onion_index_entry_t *a = (const H5FD_onion_index_entry_t *)_a;
    const H5FD_onion_index_entry_t *b = (const H5FD_onion_index_entry_t *)_b;

    if (a->logical_page < b->logical_page)
        return -1;
    else if (a->logical_page > b->logical_page)
        return 1;
    return 0;
} /* end H5FD__onion_archival_index_list_sort_cmp() */

/*-----------------------------------------------------------------------------
 * Function:    H5FD__onion_merge_revision_index_into_archival_index
 *
 * Purpose:     Merge index entries from revision index into archival index.
 *
 *              If successful, the archival index is expanded 'behind the
 *              scenes' and new entries from the revision index are inserted.
 *              The archival index remains sorted in ascending order of logical
 *              address.
 *
 *              The conversion to archival index changes logical pages in
 *              revision index entries to their logical addresses in-file.
 *
 * Return:      SUCCEED/FAIL
 *-----------------------------------------------------------------------------
 */
herr_t
H5FD__onion_merge_revision_index_into_archival_index(const H5FD_onion_revision_index_t *rix,
                                                     H5FD_onion_archival_index_t       *aix)
{
    uint64_t                    n_kept    = 0;
    H5FD_onion_index_entry_t   *kept_list = NULL;
    H5FD_onion_archival_index_t new_aix   = {
        H5FD_ONION_ARCHIVAL_INDEX_VERSION_CURR, 0, /* page_size_log2 tbd */
        0,                                         /* n_entries */
        NULL,                                      /* list pointer (allocated later) */
    };
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(rix);
    assert(aix);
    assert(H5FD_ONION_REVISION_INDEX_VERSION_CURR == rix->version);
    assert(H5FD_ONION_ARCHIVAL_INDEX_VERSION_CURR == aix->version);
    assert(aix->page_size_log2 == rix->page_size_log2);

    /* If the revision index is empty there is nothing to archive */
    if (rix->n_entries == 0)
        goto done;

    /* Add all revision index entries to new archival list */
    new_aix.page_size_log2 = aix->page_size_log2;

    if (NULL == (new_aix.list = H5MM_calloc(rix->n_entries * sizeof(H5FD_onion_index_entry_t))))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate new archival index list");

    for (uint64_t i = 0; i < rix->_hash_table_size; i++) {
        const H5FD_onion_revision_index_hash_chain_node_t *node = NULL;

        for (node = rix->_hash_table[i]; node != NULL; node = node->next) {
            H5MM_memcpy(&new_aix.list[new_aix.n_entries], &node->entry_data,
                        sizeof(H5FD_onion_index_entry_t));
            new_aix.n_entries++;
        }
    }

    /* Sort the new archival list */
    qsort(new_aix.list, new_aix.n_entries, sizeof(H5FD_onion_index_entry_t),
          H5FD__onion_archival_index_list_sort_cmp);

    /* Add the old archival index entries to a 'kept' list containing the
     * old archival list entries that are not also included in the revision
     * list.
     *
     * Note that kept_list will be NULL if there are no entries in the passed-in
     * archival list.
     */
    if (aix->n_entries > 0)
        if (NULL == (kept_list = H5MM_calloc(aix->n_entries * sizeof(H5FD_onion_index_entry_t))))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate larger archival index list");

    for (uint64_t i = 0; i < aix->n_entries; i++) {
        const H5FD_onion_index_entry_t *entry = NULL;

        /* Add only if page not already added from revision index */
        if (H5FD__onion_archival_index_find(&new_aix, aix->list[i].logical_page, &entry) == 0) {
            H5MM_memcpy(&kept_list[n_kept], &aix->list[i], sizeof(H5FD_onion_index_entry_t));
            n_kept++;
        }
    }

    /* Destroy the old archival list and replace with a list big enough to hold
     * the revision list entries and the kept list entries
     */
    H5MM_xfree(aix->list);
    if (NULL == (aix->list = H5MM_calloc((new_aix.n_entries + n_kept) * sizeof(H5FD_onion_index_entry_t))))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate exact-size archival index list");

    /* Copy (new) revision list entries to replacement list */
    H5MM_memcpy(aix->list, new_aix.list, sizeof(H5FD_onion_index_entry_t) * new_aix.n_entries);
    aix->n_entries = new_aix.n_entries;

    /* Copy (old) kept archival list entries to replacement list */
    if (n_kept > 0) {
        H5MM_memcpy(&aix->list[aix->n_entries], kept_list, sizeof(H5FD_onion_index_entry_t) * n_kept);
        aix->n_entries += n_kept;
    }

    /* Sort this list */
    qsort(aix->list, aix->n_entries, sizeof(H5FD_onion_index_entry_t),
          H5FD__onion_archival_index_list_sort_cmp);

done:
    /* Free the temporary lists */
    H5MM_xfree(kept_list);
    H5MM_xfree(new_aix.list);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__onion_merge_revision_index_into_archival_index() */
