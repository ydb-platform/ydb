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
 * Created:   H5Cprivate.h
 *
 * Purpose:   Constants and typedefs available to the rest of the
 *            library.
 *
 *-------------------------------------------------------------------------
 */

#ifndef H5Cprivate_H
#define H5Cprivate_H

#include "H5Cpublic.h" /* public prototypes            */

/* Private headers needed by this header */
#include "H5private.h"  /* Generic Functions            */
#include "H5Fprivate.h" /* File access                  */

/**************************/
/* Library Private Macros */
/**************************/

/* Cache configuration settings */
#define H5C__MAX_NUM_TYPE_IDS 30
#define H5C__PREFIX_LEN       32

/* This sanity checking constant was picked out of the air.  Increase
 * or decrease it if appropriate.  Its purposes is to detect corrupt
 * object sizes, so it probably doesn't matter if it is a bit big.
 */
#define H5C_MAX_ENTRY_SIZE ((size_t)(32 * 1024 * 1024))

#ifdef H5_HAVE_PARALLEL
/* we must maintain the clean and dirty LRU lists when we are compiled
 * with parallel support.
 */
#define H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS 1
#else /* H5_HAVE_PARALLEL */
/* The clean and dirty LRU lists don't buy us anything here -- we may
 * want them on for testing on occasion, but in general they should be
 * off.
 */
#define H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS 0
#endif /* H5_HAVE_PARALLEL */

/* Flags for cache client class behavior */
#define H5C__CLASS_NO_FLAGS_SET          ((unsigned)0x0)
#define H5C__CLASS_SPECULATIVE_LOAD_FLAG ((unsigned)0x1)
/* The following flags may only appear in test code */
#define H5C__CLASS_SKIP_READS  ((unsigned)0x2)
#define H5C__CLASS_SKIP_WRITES ((unsigned)0x4)

/* Flags for pre-serialize callback */
#define H5C__SERIALIZE_NO_FLAGS_SET ((unsigned)0)
#define H5C__SERIALIZE_RESIZED_FLAG ((unsigned)0x1)
#define H5C__SERIALIZE_MOVED_FLAG   ((unsigned)0x2)

/* Upper and lower limits on cache size.  These limits are picked
 * out of a hat -- you should be able to change them as necessary.
 *
 * However, if you need a very big cache, you should also increase the
 * size of the hash table (H5C__HASH_TABLE_LEN in H5Cpkg.h).  The current
 * upper bound on cache size is rather large for the current hash table
 * size.
 */
#define H5C__MAX_MAX_CACHE_SIZE ((size_t)(128 * 1024 * 1024))
#define H5C__MIN_MAX_CACHE_SIZE ((size_t)(1024))

/* Default max cache size and min clean size are give here to make
 * them generally accessible.
 */
#define H5C__DEFAULT_MAX_CACHE_SIZE ((size_t)(4 * 1024 * 1024))
#define H5C__DEFAULT_MIN_CLEAN_SIZE ((size_t)(2 * 1024 * 1024))

/* Cache configuration validation definitions */
#define H5C_RESIZE_CFG__VALIDATE_GENERAL      0x1
#define H5C_RESIZE_CFG__VALIDATE_INCREMENT    0x2
#define H5C_RESIZE_CFG__VALIDATE_DECREMENT    0x4
#define H5C_RESIZE_CFG__VALIDATE_INTERACTIONS 0x8
/* clang-format off */
#define H5C_RESIZE_CFG__VALIDATE_ALL      \
(                                         \
    H5C_RESIZE_CFG__VALIDATE_GENERAL |    \
    H5C_RESIZE_CFG__VALIDATE_INCREMENT |  \
    H5C_RESIZE_CFG__VALIDATE_DECREMENT |  \
    H5C_RESIZE_CFG__VALIDATE_INTERACTIONS \
)
/* clang-format on */

/* Cache configuration versions */
#define H5C__CURR_AUTO_SIZE_CTL_VER       1
#define H5C__CURR_AUTO_RESIZE_RPT_FCN_VER 1
#define H5C__CURR_CACHE_IMAGE_CTL_VER     1

/* Default configuration settings */
#define H5C__DEF_AR_UPPER_THRESHHOLD 0.9999
#define H5C__DEF_AR_LOWER_THRESHHOLD 0.9
#define H5C__DEF_AR_MAX_SIZE         ((size_t)(16 * 1024 * 1024))
#define H5C__DEF_AR_INIT_SIZE        ((size_t)(1 * 1024 * 1024))
#define H5C__DEF_AR_MIN_SIZE         ((size_t)(1 * 1024 * 1024))
#define H5C__DEF_AR_MIN_CLEAN_FRAC   0.5
#define H5C__DEF_AR_INCREMENT        2.0
#define H5C__DEF_AR_MAX_INCREMENT    ((size_t)(2 * 1024 * 1024))
#define H5C__DEF_AR_FLASH_MULTIPLE   1.0
#define H5C__DEV_AR_FLASH_THRESHOLD  0.25
#define H5C__DEF_AR_DECREMENT        0.9
#define H5C__DEF_AR_MAX_DECREMENT    ((size_t)(1 * 1024 * 1024))
#define H5C__DEF_AR_EPCHS_B4_EVICT   3
#define H5C__DEF_AR_EMPTY_RESERVE    0.05
#define H5C__MIN_AR_EPOCH_LENGTH     100
#define H5C__DEF_AR_EPOCH_LENGTH     50000
#define H5C__MAX_AR_EPOCH_LENGTH     1000000

/* #defines of flags used in the flags parameters in some of the
 * following function calls.  Note that not all flags are applicable
 * to all function calls.  Flags that don't apply to a particular
 * function are ignored in that function.
 *
 * These flags apply to all function calls:
 *     H5C__NO_FLAGS_SET (generic "no flags set" for all fcn calls)
 *
 *
 * These flags apply to H5C_insert_entry():
 *    H5C__SET_FLUSH_MARKER_FLAG
 *    H5C__PIN_ENTRY_FLAG
 *    H5C__FLUSH_LAST_FLAG        ; super block only
 *    H5C__FLUSH_COLLECTIVELY_FLAG    ; super block only
 *
 * These flags apply to H5C_protect()
 *    H5C__READ_ONLY_FLAG
 *    H5C__FLUSH_LAST_FLAG        ; super block only
 *    H5C__FLUSH_COLLECTIVELY_FLAG    ; super block only
 *
 * These flags apply to H5C_unprotect():
 *    H5C__SET_FLUSH_MARKER_FLAG
 *    H5C__DELETED_FLAG
 *    H5C__DIRTIED_FLAG
 *    H5C__PIN_ENTRY_FLAG
 *    H5C__UNPIN_ENTRY_FLAG
 *    H5C__FREE_FILE_SPACE_FLAG
 *    H5C__TAKE_OWNERSHIP_FLAG
 *
 * These flags apply to H5C_expunge_entry():
 *    H5C__FREE_FILE_SPACE_FLAG
 *
 * These flags apply to H5C_evict():
 *    H5C__EVICT_ALLOW_LAST_PINS_FLAG
 *
 * These flags apply to H5C_flush_cache():
 *    H5C__FLUSH_INVALIDATE_FLAG
 *    H5C__FLUSH_CLEAR_ONLY_FLAG
 *    H5C__FLUSH_MARKED_ENTRIES_FLAG
 *    H5C__FLUSH_IGNORE_PROTECTED_FLAG (can't use this flag in combination
 *                      with H5C__FLUSH_INVALIDATE_FLAG)
 *    H5C__DURING_FLUSH_FLAG
 *
 * These flags apply to H5C_flush_single_entry():
 *    H5C__FLUSH_INVALIDATE_FLAG
 *    H5C__FLUSH_CLEAR_ONLY_FLAG
 *    H5C__FLUSH_MARKED_ENTRIES_FLAG
 *    H5C__TAKE_OWNERSHIP_FLAG
 *    H5C__DEL_FROM_SLIST_ON_DESTROY_FLAG
 *    H5C__GENERATE_IMAGE_FLAG
 *    H5C__UPDATE_PAGE_BUFFER_FLAG
 */
#define H5C__NO_FLAGS_SET                   0x00000
#define H5C__SET_FLUSH_MARKER_FLAG          0x00001
#define H5C__DELETED_FLAG                   0x00002
#define H5C__DIRTIED_FLAG                   0x00004
#define H5C__PIN_ENTRY_FLAG                 0x00008
#define H5C__UNPIN_ENTRY_FLAG               0x00010
#define H5C__FLUSH_INVALIDATE_FLAG          0x00020
#define H5C__FLUSH_CLEAR_ONLY_FLAG          0x00040
#define H5C__FLUSH_MARKED_ENTRIES_FLAG      0x00080
#define H5C__FLUSH_IGNORE_PROTECTED_FLAG    0x00100
#define H5C__READ_ONLY_FLAG                 0x00200
#define H5C__FREE_FILE_SPACE_FLAG           0x00400
#define H5C__TAKE_OWNERSHIP_FLAG            0x00800
#define H5C__FLUSH_LAST_FLAG                0x01000
#define H5C__FLUSH_COLLECTIVELY_FLAG        0x02000
#define H5C__EVICT_ALLOW_LAST_PINS_FLAG     0x04000
#define H5C__DEL_FROM_SLIST_ON_DESTROY_FLAG 0x08000
#define H5C__DURING_FLUSH_FLAG              0x10000 /* Set when the entire cache is being flushed */
#define H5C__GENERATE_IMAGE_FLAG            0x20000 /* Set during parallel I/O */
#define H5C__UPDATE_PAGE_BUFFER_FLAG        0x40000 /* Set during parallel I/O */

/* Debugging/sanity checking/statistics settings */
/* #define H5C_DO_SANITY_CHECKS */
/* #define H5C_DO_SLIST_SANITY_CHECKS */
/* #define H5C_DO_TAGGING_SANITY_CHECKS */
/* #define H5C_DO_EXTREME_SANITY_CHECKS */

/*
 * If not already set externally (e.g., from the build
 * system), set a few debugging options for debug builds.
 */
#ifndef NDEBUG
#ifndef H5C_DO_SANITY_CHECKS
#define H5C_DO_SANITY_CHECKS
#endif

#ifndef H5C_DO_TAGGING_SANITY_CHECKS
#define H5C_DO_TAGGING_SANITY_CHECKS
#endif
#endif

/* Cork actions: cork/uncork/get cork status of an object */
#define H5C__SET_CORK   0x1
#define H5C__UNCORK     0x2
#define H5C__GET_CORKED 0x4

/* Note: The memory sanity checks aren't going to work until I/O filters are
 *      changed to call a particular alloc/free routine for their buffers,
 *      because the H5AC__SERIALIZE_RESIZED_FLAG set by the fractal heap
 *      direct block serialize callback calls H5Z_pipeline().  When the I/O
 *      filters are changed, then we should implement "cache image alloc/free"
 *      routines that the fractal heap direct block (and global heap) serialize
 *      calls can use when resizing (and re-allocating) their image in the
 *      cache. -QAK */
#define H5C_DO_MEMORY_SANITY_CHECKS 0

/* H5C_COLLECT_CACHE_STATS controls overall collection of statistics
 * on cache activity.  In general, this #define should be set to 1 in
 * debug mode, and 0 in production mode..
 */

#ifndef NDEBUG
#define H5C_COLLECT_CACHE_STATS 1
#else /* NDEBUG */
#define H5C_COLLECT_CACHE_STATS 0
#endif /* NDEBUG */

/* H5C_COLLECT_CACHE_ENTRY_STATS controls collection of statistics
 * in individual cache entries.
 *
 * H5C_COLLECT_CACHE_ENTRY_STATS should only be defined to true if
 * H5C_COLLECT_CACHE_STATS is also defined to true.
 */
#if H5C_COLLECT_CACHE_STATS
#define H5C_COLLECT_CACHE_ENTRY_STATS 1
#else
#define H5C_COLLECT_CACHE_ENTRY_STATS 0
#endif /* H5C_COLLECT_CACHE_STATS */

/****************************/
/* Library Private Typedefs */
/****************************/

/* Typedef for the main structure for the cache (defined in H5Cpkg.h) */
typedef struct H5C_t H5C_t;

/*
 *
 * Struct H5C_class_t
 *
 * Instances of H5C_class_t are used to specify the callback functions
 * used by the metadata cache for each class of metadata cache entry.
 * The fields of the structure are discussed below:
 *
 * id:    Integer field containing the unique ID of the class of metadata
 *     cache entries.
 *
 * name: Pointer to a string containing the name of the class of metadata
 *     cache entries.
 *
 * mem_type:  Instance of H5FD_mem_t, that is used to supply the
 *     mem type passed into H5F_block_read().
 *
 * flags:  Flags indicating class-specific behavior.
 *
 *    Possible flags are:
 *
 *    H5C__CLASS_NO_FLAGS_SET: No special processing.
 *
 *    H5C__CLASS_SPECULATIVE_LOAD_FLAG: This flag is used only in
 *              H5C_load_entry().  When it is set, entries are
 *        permitted to change their sizes on the first attempt
 *        to load.
 *
 *        If the new size is larger than the old, the read buffer
 *        is reallocated to the new size, loaded from file, and the
 *        deserialize routine is called a second time on the
 *        new buffer.  The entry returned by the first call to
 *        the deserialize routine is discarded (via the free_icr
 *        call) after the new size is retrieved (via the image_len
 *        call).  Note that the new size is used as the size of the
 *        entry in the cache.
 *
 *        If the new size is smaller than the old, no new loads
 *        or deserializes are performed, but the new size becomes
 *        the size of the entry in the cache.
 *
 *        When this flag is set, an attempt to read past the
 *        end of file could occur.  In this case, if the size
 *        returned get_load_size callback would result in a
 *        read past the end of file, the size is truncated to
 *        avoid this, and processing proceeds as normal.
 *
 *      The following flags may only appear in test code.
 *
 *    H5C__CLASS_SKIP_READS: This flags is intended only for use in test
 *        code.  When it is set, reads on load will be skipped,
 *        and an uninitialize buffer will be passed to the
 *        deserialize function.
 *
 *    H5C__CLASS_SKIP_WRITES: This flags is intended only for use in test
 *        code.  When it is set, writes of buffers prepared by the
 *        serialize callback will be skipped.
 *
 * GET_INITIAL_LOAD_SIZE: Pointer to the 'get initial load size' function.
 *
 *    This function determines the size based on the information in the
 *    parameter "udata" or an initial speculative guess.  The size is
 *    returned in the parameter "image_len_ptr".
 *
 *     For an entry with H5C__CLASS_NO_FLAGS_SET:
 *     This function returns in "image_len_ptr" the on disk size of the
 *              entry.
 *
 *     For an entry with H5C__CLASS_SPECULATIVE_LOAD_FLAG:
 *     This function returns in "image_len_ptr" an initial guess of the
 *              entry's on disk size.  This many bytes will be loaded from
 *              the file and then passed to 'get_final_load_size' callback
 *              for the actual (final) image length to be determined.
 *
 *    The typedef for the get_initial_load_size callback is as follows:
 *
 *        typedef herr_t (*H5C_get_initial_load_size_func_t)(void *udata_ptr,
 *                                                   size_t *image_len_ptr);
 *
 *    The parameters of the get_initial_load_size callback are as follows:
 *
 *    udata_ptr: Pointer to user data provided in the protect call, which
 *             will also be passed through to the 'get_final_load_size',
 *              'verify_chksum', and 'deserialize' callbacks.
 *
 *    image_len_ptr: Pointer to the length in bytes of the in-file image to
 *              be deserialized is to be returned.
 *
 *              This value is used by the cache to determine the size of
 *              the disk image for the metadata, in order to read the disk
 *              image from the file.
 *
 *    Processing in the get_load_size function should proceed as follows:
 *
 *    If successful, the function will place the length in the *image_len_ptr
 *      associated with supplied user data and then return SUCCEED.
 *
 *    On failure, the function must return FAIL and push error information
 *    onto the error stack with the error API routines, without modifying
 *      the value pointed to by image_len_ptr.
 *
 *
 * GET_FINAL_LOAD_SIZE: Pointer to the 'get final load size' function.
 *
 *    This function determines the final size of a speculatively loaded
 *      metadata cache entry based on the parameter "image" and the "udata"
 *      parameters.  This callback _must_ be implemented for cache clients
 *      which set the H5C__CLASS_SPECULATIVE_LOAD_FLAG and must return the
 *      actual length of on-disk image after being called once.
 *
 *    This function might deserialize the needed metadata information to
 *    determine the actual size.  The size is returned in the parameter
 *      "actual_len_ptr".
 *
 *    The typedef for the get_load_size callback is as follows:
 *
 *        typedef herr_t (*H5C_get_final_load_size_func_t)(const void *image_ptr,
 *                                                   size_t image_len,
 *                              void *udata_ptr,
 *                              size_t *actual_len_ptr);
 *
 *    The parameters of the get_load_size callback are as follows:
 *
 *    image_ptr: Pointer to a buffer containing the (possibly partial)
 *              metadata read in.
 *
 *    image_len: The length in bytes of the (possibly partial) in-file image
 *              to be queried for an actual length.
 *
 *    udata_ptr: Pointer to user data provided in the protect call, which
 *             will also be passed through to the 'verify_chksum' and
 *              'deserialize' callbacks.
 *
 *    actual_len_ptr: Pointer to the location containing the actual length
 *            of the metadata entry on disk.
 *
 *    Processing in the get_final_load_size function should proceed as follows:
 *
 *    If successful, the function will place the length in the *actual_len_ptr
 *      associated with supplied image and/or user data and then return SUCCEED.
 *
 *    On failure, the function must return FAIL and push error information
 *    onto the error stack with the error API routines, without modifying
 *      the value pointed to by actual_len_ptr.
 *
 *
 * VERIFY_CHKSUM: Pointer to the verify_chksum function.
 *
 *    This function verifies the checksum computed for the metadata is
 *    the same as the checksum stored in the metadata.
 *
 *    It computes the checksum based on the metadata stored in the
 *    parameter "image_ptr" and the actual length of the metadata in the
 *    parameter "len"  which is obtained from the "get_load_size" callback.
 *
 *    The typedef for the verify_chksum callback is as follows:
 *
 *       typedef htri_t (*H5C_verify_chksum_func_t)(const void *image_ptr,
 *                              size_t len,
 *                              void *udata_ptr);
 *
 *    The parameters of the verify_chksum callback are as follows:
 *
 *    image_ptr: Pointer to a buffer containing the metadata read in.
 *
 *    len: The actual length of the metadata.
 *
 *    udata_ptr: Pointer to user data.
 *
 *
 * DESERIALIZE: Pointer to the deserialize function.
 *
 *    This function must be able to deserialize a buffer containing the
 *      on-disk image of a metadata cache entry, allocate and initialize the
 *      equivalent in core representation, and return a pointer to that
 *      representation.
 *
 *    The typedef for the deserialize callback is as follows:
 *
 *        typedef void *(*H5C_deserialize_func_t)(const void * image_ptr,
 *                                                size_t len,
 *                                                 void * udata_ptr,
 *                                                 boolean * dirty_ptr);
 *
 *    The parameters of the deserialize callback are as follows:
 *
 *    image_ptr: Pointer to a buffer of length len containing the
 *        contents of the file starting at addr and continuing
 *        for len bytes.
 *
 *    len:    Length in bytes of the in file image to be deserialized.
 *
 *              This parameter is supplied mainly for sanity checking.
 *              Sanity checks should be performed when compiled in debug
 *              mode, but the parameter may be unused when compiled in
 *              production mode.
 *
 *    udata_ptr: Pointer to user data provided in the protect call, which
 *             must be passed through to the deserialize callback.
 *
 *      dirty_ptr:  Pointer to boolean which the deserialize function
 *          must use to mark the entry dirty if it has to modify
 *          the entry to clean up file corruption left over from
 *          an old bug in the HDF5 library.
 *
 *    Processing in the deserialize function should proceed as follows:
 *
 *      If the image contains valid data, and is of the correct length,
 *      the deserialize function must allocate space for an in-core
 *      representation of that data, deserialize the contents of the image
 *      into the space allocated for the in-core representation, and return
 *      a pointer to the in core representation.  Observe that an
 *      instance of H5C_cache_entry_t must be the first item in this
 *      representation.  The cache will initialize it after the callback
 *      returns.
 *
 *      Note that the structure of the in-core representation is otherwise
 *      up to the cache client.  All that is required is that the pointer
 *      returned be sufficient for the client's purposes when it is returned
 *      on a protect call.
 *
 *      If the deserialize function has to clean up file corruption
 *      left over from an old bug in the HDF5 library, it must set
 *      *dirty_ptr to true.  If it doesn't, no action is needed as
 *      *dirty_ptr will be set to false before the deserialize call.
 *
 *      If the operation fails for any reason (i.e. bad data in buffer, bad
 *      buffer length, malloc failure, etc.) the function must return NULL and
 *      push error information on the error stack with the error API routines.
 *
 *
 * IMAGE_LEN: Pointer to the image length callback.
 *
 *    The image_len callback is used to obtain the size of newly inserted
 *      entries and assert verification.
 *
 *      The typedef for the image_len callback is as follows:
 *
 *      typedef herr_t (*H5C_image_len_func_t)(void *thing,
 *                                           size_t *image_len_ptr);
 *
 *    The parameters of the image_len callback are as follows:
 *
 *    thing:  Pointer to the in core representation of the entry.
 *
 *    image_len_ptr: Pointer to size_t in which the callback will return
 *        the length (in bytes) of the cache entry.
 *
 *    Processing in the image_len function should proceed as follows:
 *
 *    If successful, the function will place the length of the on disk
 *    image associated with the in core representation provided in the
 *    thing parameter in *image_len_ptr, and then return SUCCEED.
 *
 *    If the function fails, it must return FAIL and push error information
 *      onto the error stack with the error API routines, and return without
 *      modifying the values pointed to by the image_len_ptr parameter.
 *
 *
 * PRE_SERIALIZE: Pointer to the pre-serialize callback.
 *
 *    The pre-serialize callback is invoked by the metadata cache before
 *    it needs a current on-disk image of the metadata entry for purposes
 *    either constructing a journal or flushing the entry to disk.
 *
 *    If the client needs to change the address or length of the entry prior
 *    to flush, the pre-serialize callback is responsible for these actions,
 *    so that the actual serialize callback (described below) is only
 *    responsible for serializing the data structure, not moving it on disk
 *    or resizing it.
 *
 *    In addition, the client may use the pre-serialize callback to
 *    ensure that the entry is ready to be flushed -- in particular,
 *    if the entry contains references to other entries that are in
 *    temporary file space, the pre-serialize callback must move those
 *    entries into real file space so that the serialized entry will
 *    contain no invalid data.
 *
 *    One would think that the base address and length of
 *    the length of the entry's image on disk would be well known.
 *    However, that need not be the case as free space section info
 *    entries will change size (and possibly location) depending on the
 *    number of blocks of free space being manages, and fractal heap
 *    direct blocks can change compressed size (and possibly location)
 *    on serialization if compression is enabled.  Similarly, it may
 *    be necessary to move entries from temporary to real file space.
 *
 *    The pre-serialize callback must report any such changes to the
 *    cache, which must then update its internal structures as needed.
 *
 *    The typedef for the pre-serialize callback is as follows:
 *
 *    typedef herr_t (*H5C_pre_serialize_func_t)(H5F_t *f,
 *                                             void * thing,
 *                                             haddr_t addr,
 *                                             size_t len,
 *                                             haddr_t * new_addr_ptr,
 *                                             size_t * new_len_ptr,
 *                                             unsigned * flags_ptr);
 *
 *    The parameters of the pre-serialize callback are as follows:
 *
 *    f:    File pointer -- needed if other metadata cache entries
 *        must be modified in the process of serializing the
 *        target entry.
 *
 *    thing:  Pointer to void containing the address of the in core
 *        representation of the target metadata cache entry.
 *        This is the same pointer returned by a protect of the
 *        addr and len given above.
 *
 *    addr:   Base address in file of the entry to be serialized.
 *
 *        This parameter is supplied mainly for sanity checking.
 *        Sanity checks should be performed when compiled in debug
 *        mode, but the parameter may be unused when compiled in
 *        production mode.
 *
 *    len:    Length in bytes of the in file image of the entry to be
 *        serialized.  Also the size the image passed to the
 *        serialize callback (discussed below) unless that
 *        value is altered by this function.
 *
 *        This parameter is supplied mainly for sanity checking.
 *        Sanity checks should be performed when compiled in debug
 *        mode, but the parameter may be unused when compiled in
 *        production mode.
 *
 *    new_addr_ptr:  Pointer to haddr_t.  If the entry is moved by
 *        the serialize function, the new on disk base address must
 *        be stored in *new_addr_ptr, and the appropriate flag set
 *        in *flags_ptr.
 *
 *        If the entry is not moved by the serialize function,
 *        *new_addr_ptr is undefined on pre-serialize callback
 *        return.
 *
 *    new_len_ptr:  Pointer to size_t.  If the entry is resized by the
 *        serialize function, the new length of the on disk image
 *        must be stored in *new_len_ptr, and the appropriate flag set
 *              in *flags_ptr.
 *
 *        If the entry is not resized by the pre-serialize function,
 *        *new_len_ptr is undefined on pre-serialize callback
 *        return.
 *
 *    flags_ptr:  Pointer to an unsigned integer used to return flags
 *        indicating whether the preserialize function resized or moved
 *        the entry.  If the entry was neither resized or moved, the
 *              serialize function must set *flags_ptr to zero.  The
 *              H5C__SERIALIZE_RESIZED_FLAG or H5C__SERIALIZE_MOVED_FLAG must
 *              be set to indicate a resize or move respectively.
 *
 *            If the H5C__SERIALIZE_RESIZED_FLAG is set, the new length
 *            must be stored in *new_len_ptr.
 *
 *            If the H5C__SERIALIZE_MOVED_FLAG flag is set, the
 *            new image base address must be stored in *new_addr_ptr.
 *
 *    Processing in the pre-serialize function should proceed as follows:
 *
 *    The pre-serialize function must examine the in core representation
 *    indicated by the thing parameter, if the pre-serialize function does
 *      not need to change the size or location of the on-disk image, it must
 *      set *flags_ptr to zero.
 *
 *    If the size of the on-disk image must be changed, the pre-serialize
 *      function must load the length of the new image into *new_len_ptr, and
 *      set the H5C__SERIALIZE_RESIZED_FLAG in *flags_ptr.
 *
 *    If the base address of the on disk image must be changed, the
 *      pre-serialize function must set *new_addr_ptr to the new base address,
 *      and set the H5C__SERIALIZE_MOVED_FLAG in *flags_ptr.
 *
 *    In addition, the pre-serialize callback may perform any other
 *    processing required before the entry is written to disk
 *
 *    If it is successful, the function must return SUCCEED.
 *
 *    If it fails for any reason, the function must return FAIL and
 *    push error information on the error stack with the error API
 *    routines.
 *
 *
 * SERIALIZE: Pointer to the serialize callback.
 *
 *    The serialize callback is invoked by the metadata cache whenever
 *    it needs a current on disk image of the metadata entry for purposes
 *    either constructing a journal entry or flushing the entry to disk.
 *
 *    At this point, the base address and length of the entry's image on
 *      disk must be well known and not change during the serialization
 *      process.
 *
 *    While any size and/or location changes must have been handled
 *    by a pre-serialize call, the client may elect to handle any other
 *    changes to the entry required to place it in correct form for
 *    writing to disk in this call.
 *
 *    The typedef for the serialize callback is as follows:
 *
 *    typedef herr_t (*H5C_serialize_func_t)(const H5F_t *f,
 *                                             void * image_ptr,
 *                                             size_t len,
 *                                             void * thing);
 *
 *    The parameters of the serialize callback are as follows:
 *
 *    f:    File pointer -- needed if other metadata cache entries
 *        must be modified in the process of serializing the
 *        target entry.
 *
 *    image_ptr: Pointer to a buffer of length len bytes into which a
 *        serialized image of the target metadata cache entry is
 *        to be written.
 *
 *         Note that this buffer will not in general be initialized
 *         to any particular value.  Thus the serialize function may
 *         not assume any initial value and must set each byte in
 *         the buffer.
 *
 *    len:    Length in bytes of the in file image of the entry to be
 *        serialized.  Also the size of *image_ptr (below).
 *
 *        This parameter is supplied mainly for sanity checking.
 *        Sanity checks should be performed when compiled in debug
 *        mode, but the parameter may be unused when compiled in
 *        production mode.
 *
 *    thing:  Pointer to void containing the address of the in core
 *        representation of the target metadata cache entry.
 *        This is the same pointer returned by a protect of the
 *        addr and len given above.
 *
 *    Processing in the serialize function should proceed as follows:
 *
 *    If there are any remaining changes to the entry required before
 *    write to disk, they must be dealt with first.
 *
 *    The serialize function must then examine the in core
 *    representation indicated by the thing parameter, and write a
 *    serialized image of its contents into the provided buffer.
 *
 *    If it is successful, the function must return SUCCEED.
 *
 *    If it fails for any reason, the function must return FAIL and
 *    push error information on the error stack with the error API
 *    routines.
 *
 *
 * NOTIFY: Pointer to the notify callback.
 *
 *      The notify callback is invoked by the metadata cache when a cache
 *      action on an entry has taken/will take place and the client indicates
 *      it wishes to be notified about the action.
 *
 *    The typedef for the notify callback is as follows:
 *
 *    typedef herr_t (*H5C_notify_func_t)(H5C_notify_action_t action,
 *                                          void *thing);
 *
 *     The parameters of the notify callback are as follows:
 *
 *    action: An enum indicating the metadata cache action that has taken/
 *              will take place.
 *
 *    thing:  Pointer to void containing the address of the in core
 *        representation of the target metadata cache entry.  This
 *        is the same pointer that would be returned by a protect
 *        of the addr and len of the entry.
 *
 *    Processing in the notify function should proceed as follows:
 *
 *    The notify function may perform any action it would like, including
 *      metadata cache calls.
 *
 *    If the function is successful, it must return SUCCEED.
 *
 *    If it fails for any reason, the function must return FAIL and
 *    push error information on the error stack with the error API
 *    routines.
 *
 *
 * FREE_ICR: Pointer to the free ICR callback.
 *
 *    The free ICR callback is invoked by the metadata cache when it
 *    wishes to evict an entry, and needs the client to free the memory
 *    allocated for the in core representation.
 *
 *    The typedef for the free ICR callback is as follows:
 *
 *    typedef herr_t (*H5C_free_icr_func_t)(void * thing));
 *
 *     The parameters of the free ICR callback are as follows:
 *
 *    thing:  Pointer to void containing the address of the in core
 *        representation of the target metadata cache entry.  This
 *        is the same pointer that would be returned by a protect
 *        of the addr and len of the entry.
 *
 *    Processing in the free ICR function should proceed as follows:
 *
 *    The free ICR function must free all memory allocated to the
 *    in core representation.
 *
 *    If the function is successful, it must return SUCCEED.
 *
 *    If it fails for any reason, the function must return FAIL and
 *    push error information on the error stack with the error API
 *    routines.
 *
 *    At least when compiled with debug, it would be useful if the
 *    free ICR call would fail if the in core representation has been
 *    modified since the last serialize callback.
 *
 * GET_FSF_SIZE: Pointer to the get file space free size callback.
 *
 *    In principle, there is no need for the get file space free size
 *    callback.  However, as an optimization, it is sometimes convenient
 *    to allocate and free file space for a number of cache entries
 *    simultaneously in a single contiguous block of file space.
 *
 *    File space allocation is done by the client, so the metadata cache
 *    need not be involved.  However, since the metadata cache typically
 *      handles file space release when an entry is destroyed, some
 *    adjustment on the part of the metadata cache is required for this
 *    operation.
 *
 *      The get file space free size callback exists to support this
 *    operation.
 *
 *    If a group of cache entries that were allocated as a group are to
 *    be discarded and their file space released, the type of the first
 *    (i.e. lowest address) entry in the group must implement the
 *    get free file space size callback.
 *
 *    To free the file space of all entries in the group in a single
 *    operation, first expunge all entries other than the first without
 *    the free file space flag.
 *
 *    Then, to complete the operation, unprotect or expunge the first
 *    entry in the block with the free file space flag set.  Since
 *    the get free file space callback is implemented, the metadata
 *    cache will use this callback to get the size of the block to be
 *    freed, instead of using the size of the entry as is done otherwise.
 *
 *    At present this callback is used only by the H5FA and H5EA dblock
 *    and dblock page client classes.
 *
 *      The typedef for the get_fsf_size callback is as follows:
 *
 *      typedef herr_t (*H5C_get_fsf_size_t)(const void * thing,
 *                                                hsize_t *fsf_size_ptr);
 *
 *      The parameters of the get_fsf_size callback are as follows:
 *
 *      thing:  Pointer to void containing the address of the in core
 *              representation of the target metadata cache entry.  This
 *              is the same pointer that would be returned by a protect()
 *              call of the associated addr and len.
 *
 *    fs_size_ptr: Pointer to hsize_t in which the callback will return
 *              the size of the piece of file space to be freed.  Note
 *        that the space to be freed is presumed to have the same
 *        base address as the cache entry.
 *
 *      The function simply returns the size of the block of file space
 *    to be freed in *fsf_size_ptr.
 *
 *    If the function is successful, it must return SUCCEED.
 *
 *      If it fails for any reason, the function must return FAIL and
 *      push error information on the error stack with the error API
 *      routines.
 *
 ***************************************************************************/

/* Actions that can be reported to 'notify' client callback */
typedef enum H5C_notify_action_t {
    H5C_NOTIFY_ACTION_AFTER_INSERT,       /* Entry has been added to the cache
                                           * via the insert call
                                           */
    H5C_NOTIFY_ACTION_AFTER_LOAD,         /* Entry has been loaded into the
                                           * from file via the protect call
                                           */
    H5C_NOTIFY_ACTION_AFTER_FLUSH,        /* Entry has just been flushed to
                                           * file.
                                           */
    H5C_NOTIFY_ACTION_BEFORE_EVICT,       /* Entry is about to be evicted
                                           * from cache.
                                           */
    H5C_NOTIFY_ACTION_ENTRY_DIRTIED,      /* Entry has been marked dirty. */
    H5C_NOTIFY_ACTION_ENTRY_CLEANED,      /* Entry has been marked clean. */
    H5C_NOTIFY_ACTION_CHILD_DIRTIED,      /* Dependent child has been marked dirty. */
    H5C_NOTIFY_ACTION_CHILD_CLEANED,      /* Dependent child has been marked clean. */
    H5C_NOTIFY_ACTION_CHILD_UNSERIALIZED, /* Dependent child has been marked unserialized. */
    H5C_NOTIFY_ACTION_CHILD_SERIALIZED    /* Dependent child has been marked serialized. */
} H5C_notify_action_t;

/* Cache client callback function pointers */
typedef herr_t (*H5C_get_initial_load_size_func_t)(void *udata_ptr, size_t *image_len_ptr);
typedef herr_t (*H5C_get_final_load_size_func_t)(const void *image_ptr, size_t image_len, void *udata_ptr,
                                                 size_t *actual_len_ptr);
typedef htri_t (*H5C_verify_chksum_func_t)(const void *image_ptr, size_t len, void *udata_ptr);
typedef void *(*H5C_deserialize_func_t)(const void *image_ptr, size_t len, void *udata_ptr, bool *dirty_ptr);
typedef herr_t (*H5C_image_len_func_t)(const void *thing, size_t *image_len_ptr);
typedef herr_t (*H5C_pre_serialize_func_t)(H5F_t *f, void *thing, haddr_t addr, size_t len,
                                           haddr_t *new_addr_ptr, size_t *new_len_ptr, unsigned *flags_ptr);
typedef herr_t (*H5C_serialize_func_t)(const H5F_t *f, void *image_ptr, size_t len, void *thing);
typedef herr_t (*H5C_notify_func_t)(H5C_notify_action_t action, void *thing);
typedef herr_t (*H5C_free_icr_func_t)(void *thing);
typedef herr_t (*H5C_get_fsf_size_t)(const void *thing, hsize_t *fsf_size_ptr);

/* Metadata cache client class definition */
typedef struct H5C_class_t {
    int                              id;
    const char                      *name;
    H5FD_mem_t                       mem_type;
    unsigned                         flags;
    H5C_get_initial_load_size_func_t get_initial_load_size;
    H5C_get_final_load_size_func_t   get_final_load_size;
    H5C_verify_chksum_func_t         verify_chksum;
    H5C_deserialize_func_t           deserialize;
    H5C_image_len_func_t             image_len;
    H5C_pre_serialize_func_t         pre_serialize;
    H5C_serialize_func_t             serialize;
    H5C_notify_func_t                notify;
    H5C_free_icr_func_t              free_icr;
    H5C_get_fsf_size_t               fsf_size;
} H5C_class_t;

/* Type definitions of callback functions used by the cache as a whole */
typedef herr_t (*H5C_write_permitted_func_t)(const H5F_t *f, bool *write_permitted_ptr);
typedef herr_t (*H5C_log_flush_func_t)(H5C_t *cache_ptr, haddr_t addr, bool was_dirty, unsigned flags);

/****************************************************************************
 *
 * H5C_ring_t & associated #defines
 *
 * The metadata cache uses the concept of rings to order the flushes of
 * classes of entries.  In this arrangement, each entry in the cache is
 * assigned to a ring, and on flush, the members of the outermost ring
 * are flushed first, followed by the next outermost, and so on with the
 * members of the innermost ring being flushed last.
 *
 * Note that flush dependencies are used to order flushes within rings.
 *
 * Note also that at the conceptual level, rings are argueably superfluous,
 * as a similar effect could be obtained via the flush dependency mechanism.
 * However, this would require all entries in the cache to participate in a
 * flush dependency -- with the implied setup and takedown overhead and
 * added complexity.  Further, the flush ordering between rings need only
 * be enforced on flush operations, and thus the use of flush dependencies
 * instead would apply unnecessary constraints on flushes under normal
 * operating circumstances.
 *
 * As of this writing, all metadata entries pertaining to data sets and
 * groups must be flushed first, and are thus assigned to the outermost
 * ring.
 *
 * Free space managers managing file space must be flushed next,
 * and are assigned to the second and third outermost rings.  Two rings
 * are used here as the raw data free space manager must be flushed before
 * the metadata free space manager.
 *
 * The object header and associated chunks used to implement superblock
 * extension messages must be flushed next, and are thus assigned to
 * the fourth outermost ring.
 *
 * The superblock proper must be flushed last, and is thus assigned to
 * the innermost ring.
 *
 * The H5C_ring_t and the associated #defines below are used to define
 * the rings.  Each entry must be assigned to the appropriate ring on
 * insertion or protect.
 *
 * Note that H5C_ring_t was originally an enumerated type.  It was
 * converted to an integer and a set of #defines for convenience in
 * debugging.
 */

#define H5C_RING_UNDEFINED 0 /* shouldn't appear in the cache */
#define H5C_RING_USER      1 /* outermost ring */
#define H5C_RING_RDFSM     2
#define H5C_RING_MDFSM     3
#define H5C_RING_SBE       4
#define H5C_RING_SB        5 /* innermost ring */
#define H5C_RING_NTYPES    6

typedef int H5C_ring_t;

/****************************************************************************
 *
 * structure H5C_cache_entry_t
 *
 * Instances of the H5C_cache_entry_t structure are used to store cache
 * entries in a hash table and sometimes in a skip list.
 * See H5SL.c for the particulars of the skip list.
 *
 * In typical application, this structure is the first field in a
 * structure to be cached.  For historical reasons, the external module
 * is responsible for managing the is_dirty field (this is no longer
 * completely true.  See the comment on the is_dirty field for details).
 * All other fields are managed by the cache.
 *
 * The fields of this structure are discussed individually below:
 *
 * cache_ptr:    Pointer to the cache that this entry is contained within.
 *
 * addr:    Base address of the cache entry on disk.
 *
 * size:    Length of the cache entry on disk in bytes Note that unlike
 *              normal caches, the entries in this cache are of arbitrary size.
 *
 *        The file space allocations for cache entries implied by the
 *              addr and size fields must be disjoint.
 *
 * image_ptr:    Pointer to void.  When not NULL, this field points to a
 *         dynamically allocated block of size bytes in which the
 *         on disk image of the metadata cache entry is stored.
 *
 *         If the entry is dirty, the pre-serialize and serialize
 *         callbacks must be used to update this image before it is
 *         written to disk
 *
 * image_up_to_date:  Boolean flag that is set to true when *image_ptr
 *         is up to date, and set to false when the entry is dirtied.
 *
 * type:    Pointer to the instance of H5C_class_t containing pointers
 *        to the methods for cache entries of the current type.  This
 *        field should be NULL when the instance of H5C_cache_entry_t
 *        is not in use.
 *
 *        The name is not particularly descriptive, but is retained
 *        to avoid changes in existing code.
 *
 * is_dirty:    Boolean flag indicating whether the contents of the cache
 *        entry has been modified since the last time it was written
 *        to disk.
 *
 * dirtied:    Boolean flag used to indicate that the entry has been
 *         dirtied while protected.
 *
 *         This field is set to false in the protect call, and may
 *         be set to true by the H5C_mark_entry_dirty() call at any
 *         time prior to the unprotect call.
 *
 *         The H5C_mark_entry_dirty() call exists as a convenience
 *         function for the fractal heap code which may not know if
 *         an entry is protected or pinned, but knows that is either
 *         protected or pinned.  The dirtied field was added as in
 *         the parallel case, it is necessary to know whether a
 *         protected entry is dirty prior to the protect call.
 *
 * is_protected: Boolean flag indicating whether this entry is protected
 *         (or locked, to use more conventional terms).  When it is
 *         protected, the entry cannot be flushed or accessed until
 *         it is unprotected (or unlocked -- again to use more
 *         conventional terms).
 *
 *         Note that protected entries are removed from the LRU lists
 *         and inserted on the protected list.
 *
 * is_read_only: Boolean flag that is only meaningful if is_protected is
 *         true.  In this circumstance, it indicates whether the
 *         entry has been protected read-only, or read/write.
 *
 *         If the entry has been protected read-only (i.e. is_protected
 *         and is_read_only are both true), we allow the entry to be
 *         protected more than once.
 *
 *         In this case, the number of readers is maintained in the
 *         ro_ref_count field (see below), and unprotect calls simply
 *         decrement that field until it drops to zero, at which point
 *         the entry is actually unprotected.
 *
 * ro_ref_count: Integer field used to maintain a count of the number of
 *         outstanding read-only protects on this entry.  This field
 *         must be zero whenever either is_protected or is_read_only
 *         are true.
 *
 * is_pinned:    Boolean flag indicating whether the entry has been pinned
 *         in the cache.
 *
 *         For very hot entries, the protect / unprotect overhead
 *         can become excessive.  Thus the cache has been extended
 *         to allow an entry to be "pinned" in the cache.
 *
 *         Pinning an entry in the cache has several implications:
 *
 *         1) A pinned entry cannot be evicted.  Thus unprotected
 *            pinned entries must be stored in the pinned entry
 *            list, instead of being managed by the replacement
 *            policy code (LRU at present).
 *
 *         2) A pinned entry can be accessed or modified at any time.
 *            This places an extra burden on the pre-serialize and
 *            serialize callbacks, which must ensure that a pinned
 *            entry is consistent and ready to write to disk before
 *            generating an image.
 *
 *         3) A pinned entry can be marked as dirty (and possibly
 *            change size) while it is unprotected.
 *
 *        4) The flush-destroy code must allow pinned entries to
 *           be unpinned (and possibly unprotected) during the
 *           flush.
 *
 * in_slist:    Boolean flag indicating whether the entry is in the skip list
 *        As a general rule, entries are placed in the list when they are
 *        marked dirty.
 *
 * flush_marker:  Boolean flag indicating that the entry is to be flushed
 *        the next time H5C_flush_cache() is called with the
 *        H5C__FLUSH_MARKED_ENTRIES_FLAG.  The flag is reset when
 *        the entry is flushed for whatever reason.
 *
 * flush_me_last:  Boolean flag indicating that this entry should not be
 *        flushed from the cache until all other entries without the
 *        flush_me_last flag set have been flushed.
 *
 *        Note: At this time, the flush_me_last flag will only be applied to
 *              two types of entries: the superblock and the file driver info
 *              message.  The code utilizing these flags is protected with
 *              asserts to enforce this.
 *
 * clear_on_unprotect:  Boolean flag used only in PHDF5.  When H5C is used
 *        to implement the metadata cache In the parallel case, only
 *        the cache with mpi rank 0 is allowed to actually write to
 *        file -- all other caches must retain dirty entries until they
 *        are advised that the entry is clean.
 *
 *        This flag is used in the case that such an advisory is
 *        received when the entry is protected.  If it is set when an
 *        entry is unprotected, and the dirtied flag is not set in
 *        the unprotect, the entry's is_dirty flag is reset by flushing
 *        it with the H5C__FLUSH_CLEAR_ONLY_FLAG.
 *
 * flush_immediately:  Boolean flag used only in Phdf5 -- and then only
 *        for H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED.
 *
 *        When a distributed metadata write is triggered at a
 *        sync point, this field is used to mark entries that
 *        must be flushed before leaving the sync point.  At all
 *        other times, this field should be set to false.
 *
 * flush_in_progress:  Boolean flag that is set to true iff the entry
 *        is in the process of being flushed.  This allows the cache
 *        to detect when a call is the result of a flush callback.
 *
 * destroy_in_progress:  Boolean flag that is set to true iff the entry
 *        is in the process of being flushed and destroyed.
 *
 *
 * Fields supporting rings for flush ordering:
 *
 * All entries in the metadata cache are assigned to a ring.  On cache
 * flush, all entries in the outermost ring are flushed first, followed
 * by all members of the next outermost ring, and so on until the
 * innermost ring is flushed.  Note that this ordering is ONLY applied
 * in flush and serialize calls.  Rings are ignored during normal operations
 * in which entries are flushed as directed by the replacement policy.
 *
 * See the header comment on H5C_ring_t above for further details.
 *
 * Note that flush dependencies (see below) are used to order flushes
 * within rings.  Unlike rings, flush dependencies are applied to ALL
 * writes, not just those triggered by flush or serialize calls.
 *
 * ring:    Instance of H5C_ring_t indicating the ring to which this
 *        entry is assigned.
 *
 *
 * Fields supporting the 'flush dependency' feature:
 *
 * Entries in the cache may have 'flush dependencies' on other entries in the
 * cache.  A flush dependency requires that all dirty child entries be flushed
 * to the file before a dirty parent entry (of those child entries) can be
 * flushed to the file.  This can be used by cache clients to create data
 * structures that allow Single-Writer/Multiple-Reader (SWMR) access for the
 * data structure.
 *
 * flush_dep_parent:    Pointer to the array of flush dependency parent entries
 *              for this entry.
 *
 * flush_dep_nparents:  Number of flush dependency parent entries for this
 *              entry, i.e. the number of valid elements in flush_dep_parent.
 *
 * flush_dep_parent_nalloc: The number of allocated elements in
 *              flush_dep_parent_nalloc.
 *
 * flush_dep_nchildren: Number of flush dependency children for this entry.  If
 *              this field is nonzero, then this entry must be pinned and
 *              therefore cannot be evicted.
 *
 * flush_dep_ndirty_children: Number of flush dependency children that are
 *              either dirty or have a nonzero flush_dep_ndirty_children.  If
 *              this field is nonzero, then this entry cannot be flushed.
 *
 * flush_dep_nunser_children:  Number of flush dependency children
 *        that are either unserialized, or have a non-zero number of
 *        positive number of unserialized children.
 *
 *        Note that since there is no requirement that a clean entry
 *        be serialized, it is possible that flush_dep_nunser_children
 *        to be greater than flush_dep_ndirty_children.
 *
 *        This field exist to facilitate correct ordering of entry
 *        serializations when it is necessary to serialize all the
 *        entries in the metadata cache.  Thus in the cache
 *        serialization, no entry can be serialized unless this
 *        field contains 0.
 *
 * Fields supporting the hash table:
 *
 * Entries in the cache are indexed by a more or less conventional hash table.
 * If there are multiple entries in any hash bin, they are stored in a doubly
 * linked list.
 *
 * We have come to scan all entries in the cache frequently enough that
 * the cost of doing so by scanning the hash table has become unacceptable.
 * To reduce this cost, the index now also maintains a doubly linked list
 * of all entries in the index.  This list is known as the index list.
 * The il_next and il_prev fields discussed below were added to support
 * the index list.
 *
 * ht_next:    Next pointer used by the hash table to store multiple
 *        entries in a single hash bin.  This field points to the
 *        next entry in the doubly linked list of entries in the
 *        hash bin, or NULL if there is no next entry.
 *
 * ht_prev:     Prev pointer used by the hash table to store multiple
 *              entries in a single hash bin.  This field points to the
 *              previous entry in the doubly linked list of entries in
 *        the hash bin, or NULL if there is no previuos entry.
 *
 * il_next:    Next pointer used by the index to maintain a doubly linked
 *        list of all entries in the index (and thus in the cache).
 *        This field contains a pointer to the next entry in the
 *        index list, or NULL if there is no next entry.
 *
 * il_prev:    Prev pointer used by the index to maintain a doubly linked
 *        list of all entries in the index (and thus in the cache).
 *        This field contains a pointer to the previous entry in the
 *        index list, or NULL if there is no previous entry.
 *
 *
 * Fields supporting replacement policies:
 *
 * The cache must have a replacement policy, and it will usually be
 * necessary for this structure to contain fields supporting that policy.
 *
 * While there has been interest in several replacement policies for
 * this cache, the initial development schedule is tight.  Thus I have
 * elected to support only a modified LRU policy for the first cut.
 *
 * When additional replacement policies are added, the fields in this
 * section will be used in different ways or not at all.  Thus the
 * documentation of these fields is repeated for each replacement policy.
 *
 * Modified LRU:
 *
 * When operating in parallel mode, we must ensure that a read does not
 * cause a write.  If it does, the process will hang, as the write will
 * be collective and the other processes will not know to participate.
 *
 * To deal with this issue, I have modified the usual LRU policy by adding
 * clean and dirty LRU lists to the usual LRU list.  When reading in
 * parallel mode, we evict from the clean LRU list only.  This implies
 * that we must try to ensure that the clean LRU list is reasonably well
 * stocked.  See the comments on H5C_t in H5Cpkg.h for more details.
 *
 * Note that even if we start with a completely clean cache, a sequence
 * of protects without unprotects can empty the clean LRU list.  In this
 * case, the cache must grow temporarily.  At the next write, we will
 * attempt to evict enough entries to get the cache down to its nominal
 * maximum size.
 *
 * The use of the replacement policy fields under the Modified LRU policy
 * is discussed below:
 *
 * next:    Next pointer in either the LRU, the protected list, or
 *        the pinned list depending on the current values of
 *        is_protected and is_pinned.  If there is no next entry
 *        on the list, this field should be set to NULL.
 *
 * prev:    Prev pointer in either the LRU, the protected list,
 *        or the pinned list depending on the current values of
 *        is_protected and is_pinned.  If there is no previous
 *        entry on the list, this field should be set to NULL.
 *
 * aux_next:    Next pointer on either the clean or dirty LRU lists.
 *        This entry should be NULL when either is_protected or
 *        is_pinned is true.
 *
 *        When is_protected and is_pinned are false, and is_dirty is
 *        true, it should point to the next item on the dirty LRU
 *        list.
 *
 *        When is_protected and is_pinned are false, and is_dirty is
 *        false, it should point to the next item on the clean LRU
 *        list.  In either case, when there is no next item, it
 *        should be NULL.
 *
 * aux_prev:    Previous pointer on either the clean or dirty LRU lists.
 *        This entry should be NULL when either is_protected or
 *        is_pinned is true.
 *
 *        When is_protected and is_pinned are false, and is_dirty is
 *        true, it should point to the previous item on the dirty
 *        LRU list.
 *
 *        When is_protected and is_pinned are false, and is_dirty
 *        is false, it should point to the previous item on the
 *        clean LRU list.
 *
 *        In either case, when there is no previous item, it should
 *        be NULL.
 *
 * Fields supporting the cache image feature:
 *
 * The following fields are used to store data about the entry which must
 * be stored in the cache image block, but which will typically be either
 * lost or heavily altered in the process of serializing the cache and
 * preparing its contents to be copied into the cache image block.
 *
 * Some fields are also used in loading the contents of the metadata cache
 * image back into the cache, and in managing such entries until they are
 * either protected by the library (at which point they become regular
 * entries) or are evicted.  See discussion of the prefetched field for
 * further details.
 *
 * include_in_image: Boolean flag indicating whether this entry should
 *        be included in the metadata cache image.  This field should
 *        always be false prior to the H5C_prep_for_file_close() call.
 *        During that call, it should be set to true for all entries
 *        that are to be included in the metadata cache image.  At
 *        present, only the superblock, the superblock extension
 *        object header and its chunks (if any) are omitted from
 *        the image.
 *
 * lru_rank:    Rank of the entry in the LRU just prior to file close.
 *
 *        Note that the first entry on the LRU has lru_rank 1,
 *        and that entries not on the LRU at that time will have
 *        either lru_rank -1 (if pinned) or 0 (if loaded during
 *        the process of flushing the cache.
 *
 * image_dirty: Boolean flag indicating whether the entry should be marked
 *        as dirty in the metadata cache image.  The flag is set to
 *        true iff the entry is dirty when H5C_prep_for_file_close()
 *        is called.
 *
 * fd_parent_count: If the entry is a child in one or more flush dependency
 *        relationships, this field contains the number of flush
 *        dependency parents.
 *
 *        In all other cases, the field is set to zero.
 *
 *        Note that while this count is initially taken from the
 *        flush dependency fields above, if the entry is in the
 *        cache image (i.e. include_in_image is true), any parents
 *        that are not in the image are removed from this count and
 *        from the fd_parent_addrs array below.
 *
 *        Finally observe that if the entry is dirty and in the
 *        cache image, and its parent is dirty and not in the cache
 *        image, then the entry must be removed from the cache image
 *        to avoid violating the flush dependency flush ordering.
 *
 * fd_parent_addrs: If the entry is a child in one or more flush dependency
 *        relationship when H5C_prep_for_file_close() is called, this
 *        field must contain a pointer to an array of size
 *        fd_parent_count containing the on disk addresses of the
 *        parent.
 *
 *        In all other cases, the field is set to NULL.
 *
 *        Note that while this list of addresses is initially taken
 *        from the flush dependency fields above, if the entry is in the
 *        cache image (i.e. include_in_image is true), any parents
 *        that are not in the image are removed from this list, and
 *        and from the fd_parent_count above.
 *
 *        Finally observe that if the entry is dirty and in the
 *        cache image, and its parent is dirty and not in the cache
 *        image, then the entry must be removed from the cache image
 *        to avoid violating the flush dependency flush ordering.
 *
 * fd_child_count: If the entry is a parent in a flush dependency
 *        relationship, this field contains the number of flush
 *        dependency children.
 *
 *        In all other cases, the field is set to zero.
 *
 *        Note that while this count is initially taken from the
 *        flush dependency fields above, if the entry is in the
 *        cache image (i.e. include_in_image is true), any children
 *        that are not in the image are removed from this count.
 *
 * fd_dirty_child_count: If the entry is a parent in a flush dependency
 *        relationship, this field contains the number of dirty flush
 *        dependency children.
 *
 *        In all other cases, the field is set to zero.
 *
 *        Note that while this count is initially taken from the
 *        flush dependency fields above, if the entry is in the
 *        cache image (i.e. include_in_image is true), any dirty
 *        children that are not in the image are removed from this
 *        count.
 *
 * image_fd_height: Flush dependency height of the entry in the cache image.
 *
 *        The flush dependency height of any entry involved in a
 *        flush dependency relationship is defined to be the
 *        longest flush dependency path from that entry to an entry
 *        with no flush dependency children.
 *
 *        Since the image_fd_height is used to order entries in the
 *        cache image so that fd parents precede fd children, for
 *        purposes of this field, and entry is at flush dependency
 *        level 0 if it either has no children, or if all of its
 *        children are not in the cache image.
 *
 *        Note that if a child in a flush dependency relationship is
 *        dirty and in the cache image, and its parent is dirty and
 *        not in the cache image, then the child must be excluded
 *        from the cache image to maintain flush ordering.
 *
 * prefetched:    Boolean flag indicating that the on disk image of the entry
 *        has been loaded into the cache prior any request for the
 *        entry by the rest of the library.
 *
 *        As of this writing (8/10/15), this can only happen through
 *        the load of a cache image block, although other scenarios
 *        are contemplated for the use of this feature.  Note that
 *        unlike the usual prefetch situation, this means that a
 *        prefetched entry can be dirty, and/or can be a party to
 *        flush dependency relationship(s).  This complicates matters
 *        somewhat.
 *
 *        The essential feature of a prefetched entry is that it
 *        consists only of a buffer containing the on disk image of
 *        the entry.  Thus it must be deserialized before it can
 *        be passed back to the library on a protect call.  This
 *        task is handled by H5C_deserialized_prefetched_entry().
 *        In essence, this routine calls the deserialize callback
 *        provided in the protect call with the on disk image,
 *        deletes the prefetched entry from the cache, and replaces
 *        it with the deserialized entry returned by the deserialize
 *        callback.
 *
 *        Further, if the prefetched entry is a flush dependency parent,
 *        all its flush dependency children (which must also be
 *        prefetched entries), must be transferred to the new cache
 *        entry returned by the deserialization callback.
 *
 *        Finally, if the prefetched entry is a flush dependency child,
 *        this flush dependency must be destroyed prior to the
 *        deserialize call.
 *
 *        In addition to the above special processing on the first
 *        protect call on a prefetched entry (after which is no longer
 *        a prefetched entry), prefetched entries also require special
 *        tretment on flush and evict.
 *
 *        On flush, a dirty prefetched entry must simply be written
 *        to disk and marked clean without any call to any client
 *        callback.
 *
 *        On eviction, if a prefetched entry is a flush dependency
 *        child, that flush dependency relationship must be destroyed
 *        just prior to the eviction.  If the flush dependency code
 *        is working properly, it should be impossible for any entry
 *        that is a flush dependency parent to be evicted.
 *
 * prefetch_type_id: Integer field containing the type ID of the prefetched
 *        entry.  This ID must match the ID of the type provided in any
 *        protect call on the prefetched entry.
 *
 *        The value of this field is undefined in prefetched is false.
 *
 * age:        Number of times a prefetched entry has appeared in
 *        subsequent cache images. The field exists to allow
 *        imposition of a limit on how many times a prefetched
 *        entry can appear in subsequent cache images without being
 *        converted to a regular entry.
 *
 *        This field must be zero if prefetched is false.
 *
 * prefetched_dirty:  Boolean field that must be set to false unless the
 *        following conditions hold:
 *
 *            1) The file has been opened R/O.
 *
 *            2) The entry is either a prefetched entry, or was
 *                     re-constructed from a prefetched entry.
 *
 *                  3) The base prefetched entry was marked dirty.
 *
 *              This field exists to solve the following problem with
 *              files containing cache images that are opened R/O.
 *
 *              If the cache image contains a dirty entry, that entry
 *              must be marked clean when it is inserted into the cache
 *              in the read-only case, as otherwise the metadata cache
 *              will attempt to flush it on file close -- which is poor
 *              form in the read-only case.
 *
 *              However, since the entry is marked clean, it is possible
 *              that the metadata cache will evict it if the size of the
 *              metadata in the file exceeds the size of the metadata cache,
 *              and the application visits much of this data.
 *
 *              If this happens, and the metadata cache is then asked for
 *              this entry, it will attempt to read it from file, and will
 *              obtain either obsolete or invalid data depending on whether
 *              the entry has ever been written to it assigned location in
 *              the file.
 *
 *              With this background, the purpose of this field should be
 *              obvious -- when set, it allows the eviction candidate
 *              selection code to skip over the entry, thus avoiding the
 *              issue.
 *
 *              Since the issue only arises in the R/O case, there is
 *              no possible interaction with SWMR.  There are also
 *              potential interactions with Evict On Close -- at present,
 *              we deal with this by disabling EOC in the R/O case.
 *
 * serialization_count:  Integer field used to maintain a count of the
 *        number of times each entry is serialized during cache
 *        serialization.  While no entry should be serialized more than
 *        once in any serialization call, throw an assertion if any
 *        flush dependency parent is serialized more than once during
 *        a single cache serialization.
 *
 *        This is a debugging field, and thus is maintained only if
 *        NDEBUG is undefined.
 *
 * Fields supporting tagged entries:
 *
 * Entries in the cache that belong to a single object in the file are
 * joined into a doubly-linked list, and are "tagged" with the object header
 * address for that object's base header "chunk" (which is used as the
 * canonical address for the object).  Global and shared entries are
 * not tagged.  Tagged entries have a pointer to the tag info for the object,
 * which is shared state for all the entries for that object.
 *
 * tl_next:    Pointer to the next entry in the tag list for an object.
 *        NULL for the tail entry in the list, as well as untagged
 *        entries.
 *
 * tl_prev:    Pointer to the previous entry in the tag list for an object.
 *        NULL for the head entry in the list, as well as untagged
 *        entries.
 *
 * tag_info:    Pointer to the common tag state for all entries belonging to
 *              an object.  NULL for untagged entries.
 *
 *
 * Cache entry stats collection fields:
 *
 * These fields should only be compiled in when both H5C_COLLECT_CACHE_STATS
 * and H5C_COLLECT_CACHE_ENTRY_STATS are true.  When present, they allow
 * collection of statistics on individual cache entries.
 *
 * accesses:  int32_t containing the number of times this cache entry has
 *            been referenced in its lifetime.
 *
 * clears:    int32_t containing the number of times this cache entry has
 *            been cleared in its life time.
 *
 * flushes:   int32_t containing the number of times this cache entry has
 *            been flushed to file in its life time.
 *
 * pins:      int32_t containing the number of times this cache entry has
 *            been pinned in cache in its life time.
 *
 ****************************************************************************/
typedef struct H5C_cache_entry_t {
    H5C_t             *cache_ptr;
    haddr_t            addr;
    size_t             size;
    void              *image_ptr;
    bool               image_up_to_date;
    const H5C_class_t *type;
    bool               is_dirty;
    bool               dirtied;
    bool               is_protected;
    bool               is_read_only;
    int                ro_ref_count;
    bool               is_pinned;
    bool               in_slist;
    bool               flush_marker;
    bool               flush_me_last;
#ifdef H5_HAVE_PARALLEL
    bool clear_on_unprotect;
    bool flush_immediately;
    bool coll_access;
#endif /* H5_HAVE_PARALLEL */
    bool flush_in_progress;
    bool destroy_in_progress;

    /* fields supporting rings for purposes of flush ordering */
    H5C_ring_t ring;

    /* fields supporting the 'flush dependency' feature: */
    struct H5C_cache_entry_t **flush_dep_parent;
    unsigned                   flush_dep_nparents;
    unsigned                   flush_dep_parent_nalloc;
    unsigned                   flush_dep_nchildren;
    unsigned                   flush_dep_ndirty_children;
    unsigned                   flush_dep_nunser_children;
    bool                       pinned_from_client;
    bool                       pinned_from_cache;

    /* fields supporting the hash table: */
    struct H5C_cache_entry_t *ht_next;
    struct H5C_cache_entry_t *ht_prev;
    struct H5C_cache_entry_t *il_next;
    struct H5C_cache_entry_t *il_prev;

    /* fields supporting replacement policies: */
    struct H5C_cache_entry_t *next;
    struct H5C_cache_entry_t *prev;
#if H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS
    struct H5C_cache_entry_t *aux_next;
    struct H5C_cache_entry_t *aux_prev;
#endif /* H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS */
#ifdef H5_HAVE_PARALLEL
    struct H5C_cache_entry_t *coll_next;
    struct H5C_cache_entry_t *coll_prev;
#endif /* H5_HAVE_PARALLEL */

    /* fields supporting cache image */
    bool     include_in_image;
    int32_t  lru_rank;
    bool     image_dirty;
    uint64_t fd_parent_count;
    haddr_t *fd_parent_addrs;
    uint64_t fd_child_count;
    uint64_t fd_dirty_child_count;
    uint32_t image_fd_height;
    bool     prefetched;
    int      prefetch_type_id;
    int32_t  age;
    bool     prefetched_dirty;

#ifndef NDEBUG /* debugging field */
    int serialization_count;
#endif /* NDEBUG */

    /* fields supporting tag lists */
    struct H5C_cache_entry_t *tl_next;
    struct H5C_cache_entry_t *tl_prev;
    struct H5C_tag_info_t    *tag_info;

#if H5C_COLLECT_CACHE_ENTRY_STATS
    /* cache entry stats fields */
    int32_t accesses;
    int32_t clears;
    int32_t flushes;
    int32_t pins;
#endif /* H5C_COLLECT_CACHE_ENTRY_STATS */
} H5C_cache_entry_t;

/****************************************************************************
 *
 * structure H5C_image_entry_t
 *
 * Instances of the H5C_image_entry_t structure are used to store data on
 * metadata cache entries used in the construction of the metadata cache
 * image block.  In essence this structure is a greatly simplified version
 * of H5C_cache_entry_t.
 *
 * The fields of this structure are discussed individually below:
 *
 * addr:    Base address of the cache entry on disk.
 *
 * size:    Length of the cache entry on disk in bytes.
 *
 * ring:    Instance of H5C_ring_t indicating the flush ordering ring
 *        to which this entry is assigned.
 *
 * age:     Number of times this prefetech entry has appeared in
 *        the current sequence of cache images.  This field is
 *        initialized to 0 if the instance of H5C_image_entry_t
 *        is constructed from a regular entry.
 *
 *        If the instance is constructed from a prefetched entry
 *        currently residing in the metadata cache, the field is
 *        set to 1 + the age of the prefetched entry, or to
 *        H5AC__CACHE_IMAGE__ENTRY_AGEOUT__MAX if that sum exceeds
 *        H5AC__CACHE_IMAGE__ENTRY_AGEOUT__MAX.
 *
 * type_id:    Integer field containing the type ID of the entry.
 *
 * lru_rank:    Rank of the entry in the LRU just prior to file close.
 *
 *        Note that the first entry on the LRU has lru_rank 1,
 *        and that entries not on the LRU at that time will have
 *        either lru_rank -1 (if pinned) or 0 (if loaded during
 *        the process of flushing the cache.
 *
 * is_dirty:    Boolean flag indicating whether the contents of the cache
 *        entry has been modified since the last time it was written
 *        to disk as a regular piece of metadata.
 *
 * image_fd_height: Flush dependency height of the entry in the cache image.
 *
 *              The flush dependency height of any entry involved in a
 *              flush dependency relationship is defined to be the
 *              longest flush dependency path from that entry to an entry
 *              with no flush dependency children.
 *
 *              Since the image_fd_height is used to order entries in the
 *              cache image so that fd parents precede fd children, for
 *              purposes of this field, an entry is at flush dependency
 *              level 0 if it either has no children, or if all of its
 *              children are not in the cache image.
 *
 *              Note that if a child in a flush dependency relationship is
 *              dirty and in the cache image, and its parent is dirty and
 *              not in the cache image, then the child must be excluded
 *              from the cache image to maintain flush ordering.
 *
 * fd_parent_count: If the entry is a child in one or more flush dependency
 *              relationships, this field contains the number of flush
 *              dependency parents.
 *
 *              In all other cases, the field is set to zero.
 *
 *              Note that while this count is initially taken from the
 *              flush dependency fields in the associated instance of
 *              H5C_cache_entry_t, if the entry is in the cache image
 *              (i.e. include_in_image is true), any parents that are
 *              not in the image are removed from this count and
 *              from the fd_parent_addrs array below.
 *
 *              Finally observe that if the entry is dirty and in the
 *              cache image, and its parent is dirty and not in the cache
 *              image, then the entry must be removed from the cache image
 *              to avoid violating the flush dependency flush ordering.
 *              This should have happened before the construction of
 *              the instance of H5C_image_entry_t.
 *
 * fd_parent_addrs: If the entry is a child in one or more flush dependency
 *              relationship when H5C_prep_for_file_close() is called, this
 *              field must contain a pointer to an array of size
 *              fd_parent_count containing the on disk addresses of the
 *              parents.
 *
 *              In all other cases, the field is set to NULL.
 *
 *              Note that while this list of addresses is initially taken
 *              from the flush dependency fields in the associated instance of
 *              H5C_cache_entry_t, if the entry is in the cache image
 *              (i.e. include_in_image is true), any parents that are not
 *              in the image are removed from this list, and from the
 *              fd_parent_count above.
 *
 *              Finally observe that if the entry is dirty and in the
 *              cache image, and its parent is dirty and not in the cache
 *              image, then the entry must be removed from the cache image
 *              to avoid violating the flush dependency flush ordering.
 *              This should have happened before the construction of
 *              the instance of H5C_image_entry_t.
 *
 * fd_child_count: If the entry is a parent in a flush dependency
 *              relationship, this field contains the number of flush
 *              dependency children.
 *
 *              In all other cases, the field is set to zero.
 *
 *              Note that while this count is initially taken from the
 *              flush dependency fields in the associated instance of
 *              H5C_cache_entry_t, if the entry is in the cache image
 *              (i.e. include_in_image is true), any children
 *              that are not in the image are removed from this count.
 *
 * fd_dirty_child_count: If the entry is a parent in a flush dependency
 *              relationship, this field contains the number of dirty flush
 *              dependency children.
 *
 *              In all other cases, the field is set to zero.
 *
 *              Note that while this count is initially taken from the
 *              flush dependency fields in the associated instance of
 *              H5C_cache_entry_t, if the entry is in the cache image
 *              (i.e. include_in_image is true), any dirty children
 *              that are not in the image are removed from this count.
 *
 * image_ptr:    Pointer to void.  When not NULL, this field points to a
 *               dynamically allocated block of size bytes in which the
 *               on disk image of the metadata cache entry is stored.
 *
 *               If the entry is dirty, the pre-serialize and serialize
 *               callbacks must be used to update this image before it is
 *               written to disk
 *
 ****************************************************************************/
typedef struct H5C_image_entry_t {
    haddr_t    addr;
    size_t     size;
    H5C_ring_t ring;
    int32_t    age;
    int32_t    type_id;
    int32_t    lru_rank;
    bool       is_dirty;
    unsigned   image_fd_height;
    uint64_t   fd_parent_count;
    haddr_t   *fd_parent_addrs;
    uint64_t   fd_child_count;
    uint64_t   fd_dirty_child_count;
    void      *image_ptr;
} H5C_image_entry_t;

/****************************************************************************
 *
 * structure H5C_auto_size_ctl_t
 *
 * Instances of H5C_auto_size_ctl_t are used to get and set the control
 * fields for automatic cache re-sizing.
 *
 * The fields of the structure are discussed individually below:
 *
 * version: Integer field containing the version number of this version
 *    of the H5C_auto_size_ctl_t structure.  Any instance of
 *    H5C_auto_size_ctl_t passed to the cache must have a known
 *    version number, or an error will be flagged.
 *
 * rpt_fcn:  Pointer to the function that is to be called to report
 *    activities each time the auto cache resize code is executed.  If the
 *    field is NULL, no call is made.
 *
 *    If the field is not NULL, it must contain the address of a function
 *    of type H5C_auto_resize_report_fcn.
 *
 * set_initial_size: Boolean flag indicating whether the size of the
 *    initial size of the cache is to be set to the value given in
 *    the initial_size field.  If set_initial_size is false, the
 *    initial_size field is ignored.
 *
 * initial_size: If enabled, this field contain the size the cache is
 *    to be set to upon receipt of this structure.  Needless to say,
 *    initial_size must lie in the closed interval [min_size, max_size].
 *
 * min_clean_fraction: double in the range 0 to 1 indicating the fraction
 *    of the cache that is to be kept clean.  This field is only used
 *    in parallel mode.  Typical values are 0.1 to 0.5.
 *
 * max_size: Maximum size to which the cache can be adjusted.  The
 *    supplied value must fall in the closed interval
 *    [MIN_MAX_CACHE_SIZE, MAX_MAX_CACHE_SIZE].  Also, max_size must
 *    be greater than or equal to min_size.
 *
 * min_size: Minimum size to which the cache can be adjusted.  The
 *    supplied value must fall in the closed interval
 *    [MIN_MAX_CACHE_SIZE, MAX_MAX_CACHE_SIZE].  Also, min_size must
 *    be less than or equal to max_size.
 *
 * epoch_length: Number of accesses on the cache over which to collect
 *    hit rate stats before running the automatic cache resize code,
 *    if it is enabled.
 *
 *    At the end of an epoch, we discard prior hit rate data and start
 *    collecting afresh.  The epoch_length must lie in the closed
 *    interval [H5C__MIN_AR_EPOCH_LENGTH, H5C__MAX_AR_EPOCH_LENGTH].
 *
 *
 * Cache size increase control fields:
 *
 * incr_mode: Instance of the H5C_cache_incr_mode enumerated type whose
 *    value indicates how we determine whether the cache size should be
 *    increased.  At present there are two possible values:
 *
 *    H5C_incr__off:    Don't attempt to increase the size of the cache
 *        automatically.
 *
 *        When this increment mode is selected, the remaining fields
 *        in the cache size increase section ar ignored.
 *
 *    H5C_incr__threshold: Attempt to increase the size of the cache
 *        whenever the average hit rate over the last epoch drops
 *        below the value supplied in the lower_hr_threshold
 *        field.
 *
 *        Note that this attempt will fail if the cache is already
 *        at its maximum size, or if the cache is not already using
 *        all available space.
 *
 * lower_hr_threshold: Lower hit rate threshold.  If the increment mode
 *       (incr_mode) is H5C_incr__threshold and the hit rate drops below the
 *       value supplied in this field in an epoch, increment the cache size by
 *       size_increment.  Note that cache size may not be incremented above
 *       max_size, and that the increment may be further restricted by the
 *       max_increment field if it is enabled.
 *
 *       When enabled, this field must contain a value in the range [0.0, 1.0].
 *       Depending on the incr_mode selected, it may also have to be less than
 *       upper_hr_threshold.
 *
 * increment:  Double containing the multiplier used to derive the new
 *       cache size from the old if a cache size increment is triggered.
 *       The increment must be greater than 1.0, and should not exceed 2.0.
 *
 *       The new cache size is obtained by multiplying the current max cache
 *       size by the increment, and then clamping to max_size and to stay
 *       within the max_increment as necessary.
 *
 * apply_max_increment:  Boolean flag indicating whether the max_increment
 *       field should be used to limit the maximum cache size increment.
 *
 * max_increment: If enabled by the apply_max_increment field described
 *       above, this field contains the maximum number of bytes by which the
 *       cache size can be increased in a single re-size.
 *
 * flash_incr_mode:  Instance of the H5C_cache_flash_incr_mode enumerated
 *      type whose value indicates whether and by what algorithm we should
 *      make flash increases in the size of the cache to accommodate insertion
 *      of large entries and large increases in the size of a single entry.
 *
 *      The addition of the flash increment mode was occasioned by performance
 *      problems that appear when a local heap is increased to a size in excess
 *      of the current cache size.  While the existing re-size code dealt with
 *      this eventually, performance was very bad for the remainder of the
 *      epoch.
 *
 *      At present, there are two possible values for the flash_incr_mode:
 *
 *      H5C_flash_incr__off:  Don't perform flash increases in the size of
 *              the cache.
 *
 *      H5C_flash_incr__add_space:  Let x be either the size of a newly
 *              newly inserted entry, or the number of bytes by which the
 *              size of an existing entry has been increased.
 *
 *              If
 *                   x > flash_threshold * current max cache size,
 *
 *              increase the current maximum cache size by x * flash_multiple
 *              less any free space in the cache, and start a new epoch.  For
 *              now at least, pay no attention to the maximum increment.
 *
 *
 *      With a little thought, it should be obvious that the above flash
 *      cache size increase algorithm is not sufficient for all
 *      circumstances -- for example, suppose the user round robins through
 *      (1/flash_threshold) +1 groups, adding one data set to each on each
 *      pass.  Then all will increase in size at about the same time, requiring
 *      the max cache size to at least double to maintain acceptable
 *      performance, however the above flash increment algorithm will not be
 *      triggered.
 *
 * flash_multiple: Double containing the multiple described above in the
 *      H5C_flash_incr__add_space section of the discussion of the
 *      flash_incr_mode section.  This field is ignored unless flash_incr_mode
 *      is H5C_flash_incr__add_space.
 *
 * flash_threshold: Double containing the factor by which current max cache
 *     size is multiplied to obtain the size threshold for the add_space
 *     flash increment algorithm.  The field is ignored unless
 *     flash_incr_mode is H5C_flash_incr__add_space.
 *
 *
 * Cache size decrease control fields:
 *
 * decr_mode: Instance of the H5C_cache_decr_mode enumerated type whose
 *    value indicates how we determine whether the cache size should be
 *    decreased.  At present there are four possibilities.
 *
 *    H5C_decr__off:    Don't attempt to decrease the size of the cache
 *        automatically.
 *
 *        When this increment mode is selected, the remaining fields
 *        in the cache size decrease section are ignored.
 *
 *    H5C_decr__threshold: Attempt to decrease the size of the cache
 *        whenever the average hit rate over the last epoch rises
 *        above the value    supplied in the upper_hr_threshold
 *        field.
 *
 *    H5C_decr__age_out:  At the end of each epoch, search the cache for
 *        entries that have not been accessed for at least the number
 *        of epochs specified in the epochs_before_eviction field, and
 *        evict these entries.  Conceptually, the maximum cache size
 *        is then decreased to match the new actual cache size.  However,
 *        this reduction may be modified by the min_size, the
 *        max_decrement, and/or the empty_reserve.
 *
 *    H5C_decr__age_out_with_threshold:  Same as age_out, but we only
 *        attempt to reduce the cache size when the hit rate observed
 *        over the last epoch exceeds the value provided in the
 *        upper_hr_threshold field.
 *
 * upper_hr_threshold: Upper hit rate threshold.  The use of this field
 *    varies according to the current decr_mode:
 *
 *    H5C_decr__off or H5C_decr__age_out:  The value of this field is
 *        ignored.
 *
 *    H5C_decr__threshold:  If the hit rate exceeds this threshold in any
 *        epoch, attempt to decrement the cache size by size_decrement.
 *
 *        Note that cache size may not be decremented below min_size.
 *
 *        Note also that if the upper_threshold is 1.0, the cache size
 *        will never be reduced.
 *
 *    H5C_decr__age_out_with_threshold:  If the hit rate exceeds this
 *        threshold in any epoch, attempt to reduce the cache size
 *        by evicting entries that have not been accessed for more
 *        than the specified number of epochs.
 *
 * decrement: This field is only used when the decr_mode is
 *    H5C_decr__threshold.
 *
 *    The field is a double containing the multiplier used to derive the
 *    new cache size from the old if a cache size decrement is triggered.
 *    The decrement must be in the range 0.0 (in which case the cache will
 *    try to contract to its minimum size) to 1.0 (in which case the
 *    cache will never shrink).
 *
 * apply_max_decrement:  Boolean flag used to determine whether decrements
 *    in cache size are to be limited by the max_decrement field.
 *
 * max_decrement: Maximum number of bytes by which the cache size can be
 *    decreased in a single re-size.  Note that decrements may also be
 *    restricted by the min_size of the cache, and (in age out modes) by
 *    the empty_reserve field.
 *
 * epochs_before_eviction:  Integer field used in H5C_decr__age_out and
 *    H5C_decr__age_out_with_threshold decrement modes.
 *
 *    This field contains the number of epochs an entry must remain
 *    unaccessed before it is evicted in an attempt to reduce the
 *    cache size.  If applicable, this field must lie in the range
 *    [1, H5C__MAX_EPOCH_MARKERS].
 *
 * apply_empty_reserve:  Boolean field controlling whether the empty_reserve
 *    field is to be used in computing the new cache size when the
 *    decr_mode is H5C_decr__age_out or H5C_decr__age_out_with_threshold.
 *
 * empty_reserve:  To avoid a constant racheting down of cache size by small
 *    amounts in the H5C_decr__age_out and H5C_decr__age_out_with_threshold
 *    modes, this field allows one to require that any cache size
 *    reductions leave the specified fraction of unused space in the cache.
 *
 *    The value of this field must be in the range [0.0, 1.0].  I would
 *    expect typical values to be in the range of 0.01 to 0.1.
 *
 ****************************************************************************/

enum H5C_resize_status {
    in_spec,
    increase,
    flash_increase,
    decrease,
    at_max_size,
    at_min_size,
    increase_disabled,
    decrease_disabled,
    not_full
}; /* enum H5C_resize_conditions */

typedef void (*H5C_auto_resize_rpt_fcn)(H5C_t *cache_ptr, int32_t version, double hit_rate,
                                        enum H5C_resize_status status, size_t old_max_cache_size,
                                        size_t new_max_cache_size, size_t old_min_clean_size,
                                        size_t new_min_clean_size);

typedef struct H5C_auto_size_ctl_t {
    /* general configuration fields: */
    int32_t                 version;
    H5C_auto_resize_rpt_fcn rpt_fcn;
    bool                    set_initial_size;
    size_t                  initial_size;
    double                  min_clean_fraction;
    size_t                  max_size;
    size_t                  min_size;
    int64_t                 epoch_length;

    /* size increase control fields: */
    enum H5C_cache_incr_mode       incr_mode;
    double                         lower_hr_threshold;
    double                         increment;
    bool                           apply_max_increment;
    size_t                         max_increment;
    enum H5C_cache_flash_incr_mode flash_incr_mode;
    double                         flash_multiple;
    double                         flash_threshold;

    /* size decrease control fields: */
    enum H5C_cache_decr_mode decr_mode;
    double                   upper_hr_threshold;
    double                   decrement;
    bool                     apply_max_decrement;
    size_t                   max_decrement;
    int32_t                  epochs_before_eviction;
    bool                     apply_empty_reserve;
    double                   empty_reserve;
} H5C_auto_size_ctl_t;

/****************************************************************************
 *
 * structure H5C_cache_image_ctl_t
 *
 * Instances of H5C_image_ctl_t are used to get and set the control
 * fields for generation of a metadata cache image on file close.
 *
 * At present control of construction of a cache image is via a FAPL
 * property at file open / create.
 *
 * The fields of the structure are discussed individually below:
 *
 * version: Integer field containing the version number of this version
 *    of the H5C_image_ctl_t structure.  Any instance of
 *    H5C_image_ctl_t passed to the cache must have a known
 *    version number, or an error will be flagged.
 *
 * generate_image:  Boolean flag indicating whether a cache image should
 *    be created on file close.
 *
 * save_resize_status:  Boolean flag indicating whether the cache image
 *      should include the adaptive cache resize configuration and status.
 *      Note that this field is ignored at present.
 *
 * entry_ageout:        Integer field indicating the maximum number of
 *      times a prefetched entry can appear in subsequent cache images.
 *      This field exists to allow the user to avoid the buildup of
 *      infrequently used entries in long sequences of cache images.
 *
 *      The value of this field must lie in the range
 *      H5AC__CACHE_IMAGE__ENTRY_AGEOUT__NONE (-1) to
 *      H5AC__CACHE_IMAGE__ENTRY_AGEOUT__MAX (100).
 *
 *      H5AC__CACHE_IMAGE__ENTRY_AGEOUT__NONE means that no limit
 *      is imposed on number of times a prefetched entry can appear
 *      in subsequent cache images.
 *
 *      A value of 0 prevents prefetched entries from being included
 *      in cache images.
 *
 *      Positive integers restrict prefetched entries to the specified
 *      number of appearances.
 *
 *      Note that the number of subsequent cache images that a prefetched
 *      entry has appeared in is tracked in an 8 bit field.  Thus, while
 *      H5AC__CACHE_IMAGE__ENTRY_AGEOUT__MAX can be increased from its
 *      current value, any value in excess of 255 will be the functional
 *      equivalent of H5AC__CACHE_IMAGE__ENTRY_AGEOUT__NONE.
 *
 * flags: Unsigned integer containing flags controlling which aspects of the
 *    cache image functionality is actually executed.  The primary impetus
 *    behind this field is to allow development of tests for partial
 *    implementations that will require little if any modification to run
 *    with the full implementation.  In normal operation, all flags should
 *    be set.
 *
 ****************************************************************************/

#define H5C_CI__GEN_MDCI_SBE_MESG    ((unsigned)0x0001)
#define H5C_CI__GEN_MDC_IMAGE_BLK    ((unsigned)0x0002)
#define H5C_CI__SUPRESS_ENTRY_WRITES ((unsigned)0x0004)
#define H5C_CI__WRITE_CACHE_IMAGE    ((unsigned)0x0008)

/* This #define must set all defined H5C_CI flags.  It is
 * used in the default value for instances of H5C_cache_image_ctl_t.
 * This value will only be modified in test code.
 */
#define H5C_CI__ALL_FLAGS ((unsigned)0x000F)

#define H5C__DEFAULT_CACHE_IMAGE_CTL                                                                         \
    {                                                                                                        \
        H5C__CURR_CACHE_IMAGE_CTL_VER,             /* = version */                                           \
            false,                                 /* = generate_image */                                    \
            false,                                 /* = save_resize_status */                                \
            H5AC__CACHE_IMAGE__ENTRY_AGEOUT__NONE, /* = entry_ageout */                                      \
            H5C_CI__ALL_FLAGS                      /* = flags */                                             \
    }

typedef struct H5C_cache_image_ctl_t {
    int32_t  version;
    bool     generate_image;
    bool     save_resize_status;
    int32_t  entry_ageout;
    unsigned flags;
} H5C_cache_image_ctl_t;

/* The cache logging output style */
typedef enum H5C_log_style_t { H5C_LOG_STYLE_JSON, H5C_LOG_STYLE_TRACE } H5C_log_style_t;

/***************************************/
/* Library-private Function Prototypes */
/***************************************/

H5_DLL H5C_t *H5C_create(size_t max_cache_size, size_t min_clean_size, int max_type_id,
                         const H5C_class_t *const  *class_table_ptr,
                         H5C_write_permitted_func_t check_write_permitted, bool write_permitted,
                         H5C_log_flush_func_t log_flush, void *aux_ptr);
H5_DLL void   H5C_def_auto_resize_rpt_fcn(H5C_t *cache_ptr, int32_t version, double hit_rate,
                                          enum H5C_resize_status status, size_t old_max_cache_size,
                                          size_t new_max_cache_size, size_t old_min_clean_size,
                                          size_t new_min_clean_size);
H5_DLL herr_t H5C_dest(H5F_t *f);
H5_DLL herr_t H5C_evict(H5F_t *f);
H5_DLL herr_t H5C_expunge_entry(H5F_t *f, const H5C_class_t *type, haddr_t addr, unsigned flags);
H5_DLL herr_t H5C_flush_cache(H5F_t *f, unsigned flags);
H5_DLL herr_t H5C_flush_tagged_entries(H5F_t *f, haddr_t tag);
H5_DLL herr_t H5C_evict_tagged_entries(H5F_t *f, haddr_t tag, bool match_global);
H5_DLL herr_t H5C_expunge_tag_type_metadata(H5F_t *f, haddr_t tag, int type_id, unsigned flags);
H5_DLL herr_t H5C_get_tag(const void *thing, /*OUT*/ haddr_t *tag);
#ifdef H5C_DO_TAGGING_SANITY_CHECKS
herr_t H5C_verify_tag(int id, haddr_t tag);
#endif
H5_DLL herr_t H5C_flush_to_min_clean(H5F_t *f);
H5_DLL herr_t H5C_get_cache_auto_resize_config(const H5C_t *cache_ptr, H5C_auto_size_ctl_t *config_ptr);
H5_DLL herr_t H5C_get_cache_size(const H5C_t *cache_ptr, size_t *max_size_ptr, size_t *min_clean_size_ptr,
                                 size_t *cur_size_ptr, uint32_t *cur_num_entries_ptr);
H5_DLL herr_t H5C_get_cache_flush_in_progress(const H5C_t *cache_ptr, bool *flush_in_progress_ptr);
H5_DLL herr_t H5C_get_cache_hit_rate(const H5C_t *cache_ptr, double *hit_rate_ptr);
H5_DLL herr_t H5C_get_entry_status(const H5F_t *f, haddr_t addr, size_t *size_ptr, bool *in_cache_ptr,
                                   bool *is_dirty_ptr, bool *is_protected_ptr, bool *is_pinned_ptr,
                                   bool *is_corked_ptr, bool *is_flush_dep_parent_ptr,
                                   bool *is_flush_dep_child_ptr, bool *image_up_to_date_ptr);
H5_DLL herr_t H5C_get_evictions_enabled(const H5C_t *cache_ptr, bool *evictions_enabled_ptr);
H5_DLL void  *H5C_get_aux_ptr(const H5C_t *cache_ptr);
H5_DLL herr_t H5C_insert_entry(H5F_t *f, const H5C_class_t *type, haddr_t addr, void *thing,
                               unsigned int flags);
H5_DLL herr_t H5C_load_cache_image_on_next_protect(H5F_t *f, haddr_t addr, hsize_t len, bool rw);
H5_DLL herr_t H5C_mark_entry_dirty(void *thing);
H5_DLL herr_t H5C_mark_entry_clean(void *thing);
H5_DLL herr_t H5C_mark_entry_unserialized(void *thing);
H5_DLL herr_t H5C_mark_entry_serialized(void *thing);
H5_DLL herr_t H5C_move_entry(H5C_t *cache_ptr, const H5C_class_t *type, haddr_t old_addr, haddr_t new_addr);
H5_DLL herr_t H5C_pin_protected_entry(void *thing);
H5_DLL herr_t H5C_prep_for_file_close(H5F_t *f);
H5_DLL herr_t H5C_create_flush_dependency(void *parent_thing, void *child_thing);
H5_DLL void  *H5C_protect(H5F_t *f, const H5C_class_t *type, haddr_t addr, void *udata, unsigned flags);
H5_DLL herr_t H5C_reset_cache_hit_rate_stats(H5C_t *cache_ptr);
H5_DLL herr_t H5C_resize_entry(void *thing, size_t new_size);
H5_DLL herr_t H5C_set_cache_auto_resize_config(H5C_t *cache_ptr, H5C_auto_size_ctl_t *config_ptr);
H5_DLL herr_t H5C_set_cache_image_config(const H5F_t *f, H5C_t *cache_ptr, H5C_cache_image_ctl_t *config_ptr);
H5_DLL herr_t H5C_set_evictions_enabled(H5C_t *cache_ptr, bool evictions_enabled);
H5_DLL herr_t H5C_set_slist_enabled(H5C_t *cache_ptr, bool slist_enabled, bool clear_slist);
H5_DLL herr_t H5C_set_prefix(H5C_t *cache_ptr, char *prefix);
H5_DLL herr_t H5C_stats(H5C_t *cache_ptr, const char *cache_name, bool display_detailed_stats);
H5_DLL void   H5C_stats__reset(H5C_t *cache_ptr);
H5_DLL herr_t H5C_unpin_entry(void *thing);
H5_DLL herr_t H5C_destroy_flush_dependency(void *parent_thing, void *child_thing);
H5_DLL herr_t H5C_unprotect(H5F_t *f, haddr_t addr, void *thing, unsigned int flags);
H5_DLL herr_t H5C_validate_cache_image_config(H5C_cache_image_ctl_t *ctl_ptr);
H5_DLL herr_t H5C_validate_resize_config(H5C_auto_size_ctl_t *config_ptr, unsigned int tests);
H5_DLL herr_t H5C_ignore_tags(H5C_t *cache_ptr);
H5_DLL bool   H5C_get_ignore_tags(const H5C_t *cache_ptr);
H5_DLL uint32_t H5C_get_num_objs_corked(const H5C_t *cache_ptr);
H5_DLL herr_t   H5C_retag_entries(H5C_t *cache_ptr, haddr_t src_tag, haddr_t dest_tag);
H5_DLL herr_t   H5C_cork(H5C_t *cache_ptr, haddr_t obj_addr, unsigned action, bool *corked);
H5_DLL herr_t   H5C_get_entry_ring(const H5F_t *f, haddr_t addr, H5C_ring_t *ring);
H5_DLL herr_t   H5C_unsettle_entry_ring(void *thing);
H5_DLL herr_t   H5C_unsettle_ring(H5F_t *f, H5C_ring_t ring);
H5_DLL herr_t   H5C_remove_entry(void *thing);
H5_DLL herr_t   H5C_cache_image_status(H5F_t *f, bool *load_ci_ptr, bool *write_ci_ptr);
H5_DLL bool     H5C_cache_image_pending(const H5C_t *cache_ptr);
H5_DLL herr_t   H5C_get_mdc_image_info(const H5C_t *cache_ptr, haddr_t *image_addr, hsize_t *image_len);

/* Logging functions */
H5_DLL herr_t H5C_start_logging(H5C_t *cache);
H5_DLL herr_t H5C_stop_logging(H5C_t *cache);
H5_DLL herr_t H5C_get_logging_status(const H5C_t *cache, /*OUT*/ bool *is_enabled,
                                     /*OUT*/ bool *is_currently_logging);

#ifdef H5_HAVE_PARALLEL
H5_DLL herr_t H5C_apply_candidate_list(H5F_t *f, H5C_t *cache_ptr, unsigned num_candidates,
                                       haddr_t *candidates_list_ptr, int mpi_rank, int mpi_size);
H5_DLL herr_t H5C_construct_candidate_list__clean_cache(H5C_t *cache_ptr);
H5_DLL herr_t H5C_construct_candidate_list__min_clean(H5C_t *cache_ptr);
H5_DLL herr_t H5C_clear_coll_entries(H5C_t *cache_ptr, bool partial);
H5_DLL herr_t H5C_mark_entries_as_clean(H5F_t *f, unsigned ce_array_len, haddr_t *ce_array_ptr);
#endif /* H5_HAVE_PARALLEL */

#ifndef NDEBUG /* debugging functions */
H5_DLL herr_t H5C_dump_cache(H5C_t *cache_ptr, const char *cache_name);
H5_DLL herr_t H5C_dump_cache_LRU(H5C_t *cache_ptr, const char *cache_name);
H5_DLL bool   H5C_get_serialization_in_progress(const H5C_t *cache_ptr);
H5_DLL bool   H5C_cache_is_clean(const H5C_t *cache_ptr, H5C_ring_t inner_ring);
H5_DLL herr_t H5C_dump_cache_skip_list(H5C_t *cache_ptr, char *calling_fcn);
H5_DLL herr_t H5C_get_entry_ptr_from_addr(H5C_t *cache_ptr, haddr_t addr, void **entry_ptr_ptr);
H5_DLL herr_t H5C_flush_dependency_exists(H5C_t *cache_ptr, haddr_t parent_addr, haddr_t child_addr,
                                          bool *fd_exists_ptr);
H5_DLL herr_t H5C_verify_entry_type(H5C_t *cache_ptr, haddr_t addr, const H5C_class_t *expected_type,
                                    bool *in_cache_ptr, bool *type_ok_ptr);
H5_DLL herr_t H5C_validate_index_list(H5C_t *cache_ptr);
#endif /* NDEBUG */

#endif /* H5Cprivate_H */
