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
 * Created:		H5Oprivate.h
 *
 * Purpose:		Object header private include file.
 *
 *-------------------------------------------------------------------------
 */
#ifndef H5Oprivate_H
#define H5Oprivate_H

/* Early typedefs to avoid circular dependencies */
typedef struct H5O_t      H5O_t;
typedef struct H5O_fill_t H5O_fill_t;

/* Include the public header file for this API */
#include "H5Opublic.h" /* Object header functions             */

/* Public headers needed by this file */
#include "H5Dpublic.h" /* Dataset functions                   */
#include "H5Lpublic.h" /* Link functions                      */
#include "H5Spublic.h" /* Dataspace functions			*/

/* Private headers needed by this file */
#include "H5private.h"   /* Generic Functions                   */
#include "H5ACprivate.h" /* Metadata cache                      */
#include "H5Fprivate.h"  /* File access				*/
#include "H5HGprivate.h" /* Global Heaps                        */
#include "H5SLprivate.h" /* Skip lists				*/
#include "H5Tprivate.h"  /* Datatype functions			*/
#include "H5VLprivate.h" /* Virtual Object Layer                */
#include "H5Zprivate.h"  /* I/O pipeline filters		*/

/* Forward references of package typedefs */
typedef struct H5O_msg_class_t H5O_msg_class_t;
typedef struct H5O_mesg_t      H5O_mesg_t;

/* Values used to create the shared message & attribute heaps */
/* (Note that these parameters have been tuned so that the resulting heap ID
 *      is exactly 8 bytes.  This is an efficient size as it can be stored
 *      directly in an 8 byte integer in memory, think carefully before changing it.
 *      -QAK)
 */
#define H5O_FHEAP_MAN_WIDTH            4
#define H5O_FHEAP_MAN_START_BLOCK_SIZE 1024
#define H5O_FHEAP_MAN_MAX_DIRECT_SIZE  (64 * 1024)
#define H5O_FHEAP_MAN_MAX_INDEX        40
#define H5O_FHEAP_MAN_START_ROOT_ROWS  1
#define H5O_FHEAP_CHECKSUM_DBLOCKS     true
#define H5O_FHEAP_MAX_MAN_SIZE         (4 * 1024)
#define H5O_FHEAP_ID_LEN               8

/* Object header macros */
#define H5O_MESG_MAX_SIZE 65536 /*max obj header message size	     */
#define H5O_ALL           (-1)  /* Operate on all messages of type   */
#define H5O_FIRST         (-2)  /* Operate on first message of type  */

/* Flags needed when encoding messages */
#define H5O_MSG_NO_FLAGS_SET                            0x00u
#define H5O_MSG_FLAG_CONSTANT                           0x01u
#define H5O_MSG_FLAG_SHARED                             0x02u
#define H5O_MSG_FLAG_DONTSHARE                          0x04u
#define H5O_MSG_FLAG_FAIL_IF_UNKNOWN_AND_OPEN_FOR_WRITE 0x08u
#define H5O_MSG_FLAG_MARK_IF_UNKNOWN                    0x10u
#define H5O_MSG_FLAG_WAS_UNKNOWN                        0x20u
#define H5O_MSG_FLAG_SHAREABLE                          0x40u
#define H5O_MSG_FLAG_FAIL_IF_UNKNOWN_ALWAYS             0x80u
#define H5O_MSG_FLAG_BITS                                                                                    \
    (H5O_MSG_FLAG_CONSTANT | H5O_MSG_FLAG_SHARED | H5O_MSG_FLAG_DONTSHARE |                                  \
     H5O_MSG_FLAG_FAIL_IF_UNKNOWN_AND_OPEN_FOR_WRITE | H5O_MSG_FLAG_MARK_IF_UNKNOWN |                        \
     H5O_MSG_FLAG_WAS_UNKNOWN | H5O_MSG_FLAG_SHAREABLE | H5O_MSG_FLAG_FAIL_IF_UNKNOWN_ALWAYS)

/* Flags for updating messages */
#define H5O_UPDATE_TIME  0x01u
#define H5O_UPDATE_FORCE 0x02u /* Force updating the message */

/* Hash value constants */
#define H5O_HASH_SIZE 32

/* Enable reading/writing "bogus" messages */
/* #define H5O_ENABLE_BOGUS */

/* ========= Object Creation properties ============ */
#define H5O_CRT_ATTR_MAX_COMPACT_NAME "max compact attr"    /* Max. # of attributes to store compactly */
#define H5O_CRT_ATTR_MIN_DENSE_NAME   "min dense attr"      /* Min. # of attributes to store densely */
#define H5O_CRT_OHDR_FLAGS_NAME       "object header flags" /* Object header flags */
#define H5O_CRT_PIPELINE_NAME         "pline"               /* Filter pipeline */
#define H5O_CRT_PIPELINE_DEF                                                                                 \
    {                                                                                                        \
        {0, NULL, H5O_NULL_ID, {{0, HADDR_UNDEF}}}, H5O_PLINE_VERSION_1, 0, 0, NULL                          \
    }
#ifdef H5O_ENABLE_BOGUS
#define H5O_BOGUS_MSG_FLAGS_NAME "bogus msg flags" /* Flags for 'bogus' message */
#define H5O_BOGUS_MSG_FLAGS_SIZE sizeof(uint8_t)

/* bogus ID can be either (a) H5O_BOGUS_VALID_ID or (b) H5O_BOGUS_INVALID_ID */
#define H5O_BOGUS_MSG_ID_NAME "bogus msg id" /* ID for 'bogus' message */
#define H5O_BOGUS_MSG_ID_SIZE sizeof(unsigned)

#endif /* H5O_ENABLE_BOGUS */

#ifdef H5O_ENABLE_BAD_MESG_COUNT
#define H5O_BAD_MESG_COUNT_NAME "bad message count" /* Flag setting bad message count */
#define H5O_BAD_MESG_COUNT_SIZE sizeof(bool)
#endif /* H5O_ENABLE_BAD_MESG_COUNT */

/* ========= Object Copy properties ============ */
#define H5O_CPY_OPTION_NAME "copy object" /* Copy options */
#define H5O_CPY_MERGE_COMM_DT_LIST_NAME                                                                      \
    "merge committed dtype list" /* List of datatype paths to search in the dest file for merging */
#define H5O_CPY_MCDT_SEARCH_CB_NAME                                                                          \
    "committed dtype list search" /* Callback function when the search for a matching committed datatype is  \
                                     complete */

/* If the module using this macro is allowed access to the private variables, access them directly */
#ifdef H5O_MODULE
#define H5O_OH_GET_ADDR(O)    ((O)->chunk[0].addr)
#define H5O_OH_GET_VERSION(O) ((O)->version)
#define H5O_OH_GET_FLAGS(O)   ((O)->flags)
#define H5O_OH_GET_MTIME(O)   ((O)->mtime)
#else /* H5O_MODULE */
#define H5O_OH_GET_ADDR(O)    (H5O_get_oh_addr(O))
#define H5O_OH_GET_VERSION(O) (H5O_get_oh_version(O))
#define H5O_OH_GET_FLAGS(O)   (H5O_get_oh_flags(O))
#define H5O_OH_GET_MTIME(O)   (H5O_get_oh_mtime(O))
#endif /* H5O_MODULE */

/* Set the fields in a shared message structure */
#define H5O_UPDATE_SHARED(SH_MESG, SH_TYPE, F, MSG_TYPE, CRT_IDX, OH_ADDR)                                   \
    {                                                                                                        \
        (SH_MESG)->type          = (SH_TYPE);                                                                \
        (SH_MESG)->file          = (F);                                                                      \
        (SH_MESG)->msg_type_id   = (MSG_TYPE);                                                               \
        (SH_MESG)->u.loc.index   = (CRT_IDX);                                                                \
        (SH_MESG)->u.loc.oh_addr = (OH_ADDR);                                                                \
    } /* end block */

/* Fractal heap ID type for shared message & attribute heap IDs. */
typedef union {
    uint8_t  id[H5O_FHEAP_ID_LEN]; /* Buffer to hold ID, for encoding/decoding */
    uint64_t val;                  /* Value, for quick comparisons */
} H5O_fheap_id_t;

/* The object location information for an object */
typedef struct H5O_loc_t {
    H5F_t  *file;         /* File that object header is located within */
    haddr_t addr;         /* File address of object header */
    bool    holding_file; /* True if this object header has incremented
                           * its file's count of open objects. */
} H5O_loc_t;

/* Typedef for linked list of datatype merge suggestions */
typedef struct H5O_copy_dtype_merge_list_t {
    char                               *path; /* Path to datatype in destination file */
    struct H5O_copy_dtype_merge_list_t *next; /* Next object in list */
} H5O_copy_dtype_merge_list_t;

/* Structure for callback property before searching the global list of committed datatypes at destination */
typedef struct H5O_mcdt_cb_info_t {
    H5O_mcdt_search_cb_t func;
    void                *user_data;
} H5O_mcdt_cb_info_t;

/* Settings/flags for copying an object */
typedef struct H5O_copy_t {
    bool                         copy_shallow;           /* Flag to perform shallow hierarchy copy */
    bool                         expand_soft_link;       /* Flag to expand soft links */
    bool                         expand_ext_link;        /* Flag to expand external links */
    bool                         expand_ref;             /* Flag to expand object references */
    bool                         copy_without_attr;      /* Flag to not copy attributes */
    bool                         preserve_null;          /* Flag to not delete NULL messages */
    bool                         merge_comm_dt;          /* Flag to merge committed datatypes in dest file */
    H5O_copy_dtype_merge_list_t *dst_dt_suggestion_list; /* Suggestions for merging committed datatypes */
    int                          curr_depth;             /* Current depth in hierarchy copied */
    int                          max_depth;              /* Maximum depth in hierarchy to copy */
    H5SL_t                      *map_list;               /* Skip list to hold address mappings */
    H5SL_t                      *dst_dt_list; /* Skip list to hold committed datatypes in dest file */
    bool dst_dt_list_complete;      /* Whether the destination datatype list is complete (i.e. not only
                                          populated with "suggestions" from H5Padd_merge_committed_dtype_path) */
    H5O_t *oh_dst;                  /* The destination object header */
    H5F_t *file_dst;                /* The destination file pointer */
                                    /* This is used in the pre_copy_file callback to obtain
                                       the destination file's high bound.  The high bound
                                       is used to index into the corresponding message's
                                       array of versions for doing version bounds check. */
    void                *shared_fo; /* The shared pointer for the src object */
    H5O_mcdt_search_cb_t mcdt_cb;   /* The callback to invoke before searching the global list of committed
                                       datatypes at destination */
    void *mcdt_ud;                  /* User data passed to callback */
    hid_t lcpl_id;                  /* LCPL for callbacks' use */
} H5O_copy_t;

/* Header message IDs */
#define H5O_NULL_ID        0x0000 /* Null Message.  */
#define H5O_SDSPACE_ID     0x0001 /* Dataspace Message. */
#define H5O_LINFO_ID       0x0002 /* Link info Message. */
#define H5O_DTYPE_ID       0x0003 /* Datatype Message.  */
#define H5O_FILL_ID        0x0004 /* Fill Value Message. (Old)  */
#define H5O_FILL_NEW_ID    0x0005 /* Fill Value Message. (New)  */
#define H5O_LINK_ID        0x0006 /* Link Message. */
#define H5O_EFL_ID         0x0007 /* External File List Message  */
#define H5O_LAYOUT_ID      0x0008 /* Data Layout Message.  */
#define H5O_BOGUS_VALID_ID 0x0009 /* "Bogus valid" Message.  */
#define H5O_GINFO_ID       0x000a /* Group info Message.  */
#define H5O_PLINE_ID       0x000b /* Filter pipeline message.  */
#define H5O_ATTR_ID        0x000c /* Attribute Message.  */
#define H5O_NAME_ID        0x000d /* Object name message.  */
#define H5O_MTIME_ID       0x000e /* Modification time message. (Old)  */
#define H5O_SHMESG_ID      0x000f /* Shared message "SOHM" table. */
#define H5O_CONT_ID        0x0010 /* Object header continuation message.  */
#define H5O_STAB_ID        0x0011 /* Symbol table message.  */
#define H5O_MTIME_NEW_ID   0x0012 /* Modification time message. (New)  */
#define H5O_BTREEK_ID      0x0013 /* v1 B-tree 'K' values message.  */
#define H5O_DRVINFO_ID     0x0014 /* Driver info message.  */
#define H5O_AINFO_ID       0x0015 /* Attribute info message.  */
#define H5O_REFCOUNT_ID    0x0016 /* Reference count message.  */
#define H5O_FSINFO_ID      0x0017 /* File space info message.  */
#define H5O_MDCI_MSG_ID    0x0018 /* Metadata Cache Image Message */
#define H5O_UNKNOWN_ID     0x0019 /* Placeholder message ID for unknown message.  */
/* (this should never exist in a file) */
/*
 * Note: Must increment H5O_MSG_TYPES in H5Opkg.h and update H5O_msg_class_g
 *      in H5O.c when creating a new message type.  Also bump the value of
 *      H5O_BOGUS_INVALID_ID, below, to be one greater than the value of
 *      H5O_UNKNOWN_ID, and re-run gen_bogus.c.
 *
 * (this should never exist in a file)
 */
#define H5O_BOGUS_INVALID_ID 0x001a /* "Bogus invalid" Message.  */

/* Shared object message types.
 * Shared objects can be committed, in which case the shared message contains
 * the location of the object header that holds the message, or shared in the
 * heap, in which case the shared message holds their heap ID.
 */
#define H5O_SHARE_TYPE_UNSHARED  0 /* Message is not shared */
#define H5O_SHARE_TYPE_SOHM      1 /* Message is stored in SOHM heap */
#define H5O_SHARE_TYPE_COMMITTED 2 /* Message is stored in another object header */
#define H5O_SHARE_TYPE_HERE      3 /* Message is stored in this object header, but is shareable */

/* Detect messages that aren't stored in message's object header */
#define H5O_IS_STORED_SHARED(T)                                                                              \
    ((((T) == H5O_SHARE_TYPE_SOHM) || ((T) == H5O_SHARE_TYPE_COMMITTED)) ? true : false)

/* Detect shared messages that are "tracked" in some other location */
#define H5O_IS_TRACKED_SHARED(T) ((T) > 0)

/* Specify the object header address and index needed
 *      to locate a message in another object header.
 */
typedef struct H5O_mesg_loc_t {
    H5O_msg_crt_idx_t index;   /* index within object header   */
    haddr_t           oh_addr; /* address of object header    */
} H5O_mesg_loc_t;

/*
 * Shared object header message info.
 *
 * (This structure is used in other messages that can be shared and will
 * include a H5O_shared_t struct as the first field in their "native" type)
 */
typedef struct H5O_shared_t {
    unsigned type;        /* Type describing how message is shared */
    H5F_t   *file;        /* File that message is located within */
    unsigned msg_type_id; /* Message's type ID */
    union {
        H5O_mesg_loc_t loc;     /* Object location info		     */
        H5O_fheap_id_t heap_id; /* ID within the SOHM heap           */
    } u;
} H5O_shared_t;

/*
 * Link Info Message.
 * (Contains dynamic information about links in a group)
 * (Data structure in memory)
 * (if the fields in this struct are changed, remember to change the default
 *      link info structure in src/H5Gprivate.h - QAK)
 * (if the fields in this struct are changed, also look at the code that
 *      creates intermediate groups in src/H5Gtraverse.c - QAK)
 * (The "max. creation order" field is signed so that we might have an easy
 *      way to add links to the front of the creation ordering (with negative
 *      values) as well as the end of the creation ordering - QAK)
 */
typedef struct H5O_linfo_t {
    /* Creation order info */
    bool    track_corder;    /* Are creation order values tracked on links? */
    bool    index_corder;    /* Are creation order values indexed on links? */
    int64_t max_corder;      /* Current max. creation order value for group */
    haddr_t corder_bt2_addr; /* Address of v2 B-tree for indexing creation order values of links */

    /* Storage management info */
    hsize_t nlinks;        /* Number of links in the group      */
    haddr_t fheap_addr;    /* Address of fractal heap for storing "dense" links */
    haddr_t name_bt2_addr; /* Address of v2 B-tree for indexing names of links */
} H5O_linfo_t;

/* Initial version of the "old" fill value information */
/* (It doesn't look like this value was ever used in the file -QAK) */
#define H5O_FILL_VERSION_1 1
/* Revised version of the "new" fill value information */
#define H5O_FILL_VERSION_2 2
/* Version of the "new" fill value information with smaller default format */
#define H5O_FILL_VERSION_3 3

/* The latest version of the format.  Look through the 'encode', 'decode'
 *      and 'size' callback for places to change when updating this. */
#define H5O_FILL_VERSION_LATEST H5O_FILL_VERSION_3

/*
 * Fill Value Message.
 * (Data structure in memory for both "old" and "new" fill value messages)
 *
 * The fill value message is fill value plus space allocation time, fill value
 * writing time, whether fill value is defined, and the location of the
 * message if it's shared.
 */

struct H5O_fill_t {
    H5O_shared_t sh_loc; /* Shared message info (must be first) */

    unsigned         version;      /* Encoding version number           */
    H5T_t           *type;         /*type. Null implies same as dataset */
    ssize_t          size;         /*number of bytes in the fill value  */
    void            *buf;          /*the fill value		     */
    H5D_alloc_time_t alloc_time;   /* time to allocate space	     */
    H5D_fill_time_t  fill_time;    /* time to write fill value	     */
    bool             fill_defined; /* whether fill value is defined     */
};

/*
 * Link message.
 * (Data structure in memory)
 */
typedef struct H5O_link_hard_t {
    haddr_t addr; /* Object header address */
} H5O_link_hard_t;

typedef struct H5O_link_soft_t {
    char *name; /* Destination name */
} H5O_link_soft_t;

typedef struct H5O_link_ud_t {
    void  *udata; /* Opaque data supplied by the user */
    size_t size;  /* Size of udata */
} H5O_link_ud_t;

typedef struct H5O_link_t {
    H5L_type_t type;         /* Type of link */
    bool       corder_valid; /* Creation order for link is valid (not stored) */
    int64_t    corder;       /* Creation order for link (stored if it's valid) */
    H5T_cset_t cset;         /* Character set of link name	*/
    char      *name;         /* Link name */
    union {
        H5O_link_hard_t hard; /* Information for hard links */
        H5O_link_soft_t soft; /* Information for soft links */
        H5O_link_ud_t   ud;   /* Information for user-defined links */
    } u;
} H5O_link_t;

/*
 * External File List Message
 * (Data structure in memory)
 */
#define H5O_EFL_ALLOC     16            /*number of slots to alloc at once   */
#define H5O_EFL_UNLIMITED H5F_UNLIMITED /*max possible file size	     */

typedef struct H5O_efl_entry_t {
    size_t  name_offset; /*offset of name within heap	     */
    char   *name;        /*malloc'd name			     */
    HDoff_t offset;      /*offset of data within file	     */
    hsize_t size;        /*size allocated within file	     */
} H5O_efl_entry_t;

typedef struct H5O_efl_t {
    haddr_t          heap_addr; /*address of name heap		     */
    size_t           nalloc;    /*number of slots allocated	     */
    size_t           nused;     /*number of slots used		     */
    H5O_efl_entry_t *slot;      /*array of external file entries     */
} H5O_efl_t;

/*
 * Data Layout Message.
 * (Data structure in file)
 */
#define H5O_LAYOUT_NDIMS (H5S_MAX_RANK + 1)

/* Flags for chunked layout feature encoding */
#define H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS 0x01
#define H5O_LAYOUT_CHUNK_SINGLE_INDEX_WITH_FILTER         0x02
#define H5O_LAYOUT_ALL_CHUNK_FLAGS                                                                           \
    (H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS | H5O_LAYOUT_CHUNK_SINGLE_INDEX_WITH_FILTER)

/* Version number of encoded virtual dataset global heap blocks */
#define H5O_LAYOUT_VDS_GH_ENC_VERS 0

/* Initial version of the layout information.  Used when space is allocated */
#define H5O_LAYOUT_VERSION_1 1

/* This version added support for delaying allocation */
#define H5O_LAYOUT_VERSION_2 2

/* This version is revised to store just the information needed for each
 *      storage type, and to straighten out problems with contiguous layout's
 *      sizes (was encoding them as 4-byte values when they were really n-byte
 *      values (where n usually is 8)).
 */
#define H5O_LAYOUT_VERSION_3 3

/* This version adds different types of indices to chunked datasets, allows
 *      for larger chunk dimensions, stores chunk indices into their own
 *      message (the "layout index" message), adds features for compact/dense
 *      storage of elements and/or chunk records, adds features for abbreviating
 *      the storage used for partial chunks on boundaries, adds the virtual
 *      layout type, etc.
 */
#define H5O_LAYOUT_VERSION_4 4

/* The default version of the format.  (Earlier versions had bugs) */
#define H5O_LAYOUT_VERSION_DEFAULT H5O_LAYOUT_VERSION_3

/* The latest version of the format.  Look through the 'encode'
 *      and 'size' callbacks for places to change when updating this. */
#define H5O_LAYOUT_VERSION_LATEST H5O_LAYOUT_VERSION_4

/* Forward declaration of structs used below */
struct H5D_layout_ops_t; /* Defined in H5Dpkg.h               */
struct H5D_chunk_ops_t;  /* Defined in H5Dpkg.h               */

typedef struct H5O_storage_contig_t {
    haddr_t addr; /* File address of data              */
    hsize_t size; /* Size of data in bytes             */
} H5O_storage_contig_t;

typedef struct H5O_storage_chunk_btree_t {
    haddr_t dset_ohdr_addr; /* File address dataset's object header */
    H5UC_t *shared;         /* Ref-counted shared info for B-tree nodes */
} H5O_storage_chunk_btree_t;

/* Forward declaration of structs used below */
struct H5FA_t; /* Defined in H5FAprivate.h          */

typedef struct H5O_storage_chunk_farray_t {
    haddr_t        dset_ohdr_addr; /* File address dataset's object header */
    struct H5FA_t *fa;             /* Pointer to fixed index array struct */
} H5O_storage_chunk_farray_t;

/* Forward declaration of structs used below */
struct H5EA_t; /* Defined in H5EAprivate.h          */

typedef struct H5O_storage_chunk_earray_t {
    haddr_t        dset_ohdr_addr; /* File address dataset's object header */
    struct H5EA_t *ea;             /* Pointer to extensible index array struct */
} H5O_storage_chunk_earray_t;

/* Filtered info for single chunk index */
typedef struct H5O_storage_chunk_single_filt_t {
    uint32_t nbytes;      /* Size of chunk (in file) */
    uint32_t filter_mask; /* Excluded filters for chunk */
} H5O_storage_chunk_single_filt_t;

/* Forward declaration of structs used below */
struct H5B2_t; /* Defined in H5B2pkg.h          */

typedef struct H5O_storage_chunk_bt2_t {
    haddr_t        dset_ohdr_addr; /* File address dataset's object header */
    struct H5B2_t *bt2;            /* Pointer to b-tree 2 struct */
} H5O_storage_chunk_bt2_t;

typedef struct H5O_storage_chunk_t {
    H5D_chunk_index_t             idx_type; /* Type of chunk index               */
    haddr_t                       idx_addr; /* File address of chunk index       */
    const struct H5D_chunk_ops_t *ops;      /* Pointer to chunked storage operations */
    union {
        H5O_storage_chunk_btree_t       btree;  /* Information for v1 B-tree index   */
        H5O_storage_chunk_bt2_t         btree2; /* Information for v2 B-tree index */
        H5O_storage_chunk_earray_t      earray; /* Information for extensible array index   */
        H5O_storage_chunk_farray_t      farray; /* Information for fixed array index   */
        H5O_storage_chunk_single_filt_t single; /* Information for single chunk w/ filters index */
    } u;
} H5O_storage_chunk_t;

typedef struct H5O_storage_compact_t {
    bool   dirty; /* Dirty flag for compact dataset    */
    size_t size;  /* Size of buffer in bytes           */
    void  *buf;   /* Buffer for compact dataset        */
} H5O_storage_compact_t;

typedef struct H5O_storage_virtual_srcdset_t {
    /* Stored */
    struct H5S_t *virtual_select; /* Selection in the virtual dataset that is mapped to source selection */

    /* Not stored */
    char         *file_name;              /* Source file name used for virtual dataset mapping */
    char         *dset_name;              /* Source dataset name used for virtual dataset mapping */
    struct H5S_t *clipped_source_select;  /* Clipped version of source_select  */
    struct H5S_t *clipped_virtual_select; /* Clipped version of virtual_select */
    struct H5D_t *dset;                   /* Source dataset                     */
    bool          dset_exists;            /* Whether the dataset exists (was opened successfully) */

    /* Temporary - only used during I/O operation, NULL at all other times */
    struct H5S_t *projected_mem_space; /* Selection within mem_space for this mapping */
} H5O_storage_virtual_srcdset_t;

typedef struct H5O_storage_virtual_name_seg_t {
    char                                  *name_segment; /* String for this name segment       */
    struct H5O_storage_virtual_name_seg_t *next;         /* Next name segment         */
} H5O_storage_virtual_name_seg_t;

typedef enum H5O_virtual_space_status_t {
    H5O_VIRTUAL_STATUS_INVALID = 0, /* Space extent is invalid            */
    H5O_VIRTUAL_STATUS_SEL_BOUNDS,  /* Space extent set to bounds of selection */
    H5O_VIRTUAL_STATUS_USER,        /* Space extent provided by application */
    H5O_VIRTUAL_STATUS_CORRECT      /* Space extent matches dataset       */
} H5O_virtual_space_status_t;

typedef struct H5O_storage_virtual_ent_t {
    /* Stored */
    H5O_storage_virtual_srcdset_t source_dset;      /* Information about the source dataset */
    char                         *source_file_name; /* Original (unparsed) source file name */
    char                         *source_dset_name; /* Original (unparsed) source dataset name */
    struct H5S_t                 *source_select;    /* Selection in the source dataset for mapping */

    /* Not stored */
    H5O_storage_virtual_srcdset_t *sub_dset;        /* Array of sub-source dataset info structs */
    size_t                         sub_dset_nalloc; /* Number of slots allocated in sub_dset */
    size_t sub_dset_nused; /* Number of slots "used" in sub_dset - essentially the farthest sub dataset in the
                              extent */
    size_t sub_dset_io_start; /* First element in sub_dset involved in current I/O op.  Field has no meaning
                                 and may be uninitialized at all other times */
    size_t sub_dset_io_end; /* First element in sub_dset outside of current I/O op.  Field has no meaning and
                               may be uninitialized at all other times */
    H5O_storage_virtual_name_seg_t *parsed_source_file_name; /* Parsed version of source_dset.file_name */
    size_t psfn_static_strlen; /* Length of parsed_source_file_name without block number substitutions */
    size_t psfn_nsubs;         /* Number of block number substitutions in parsed_source_file_name */
    H5O_storage_virtual_name_seg_t *parsed_source_dset_name; /* Parsed version of source_dset.dset_name */
    size_t  psdn_static_strlen;   /* Length of parsed_source_dset_name without block number substitutions */
    size_t  psdn_nsubs;           /* Number of block number substitutions in parsed_source_dset_name */
    int     unlim_dim_source;     /* Unlimited dimension in source_select */
    int     unlim_dim_virtual;    /* Unlimited dimension in virtual_select */
    hsize_t unlim_extent_source;  /* Extent of unlimited dimension in source dset last time virtual_select was
                                     patched to match selection */
    hsize_t unlim_extent_virtual; /* Extent of unlimited dimension in virtual dset last time source_select was
                                     patched to match selection */
    hsize_t clip_size_virtual;    /* Size selection would be clipped to in virtual selection, ignoring other
                                     mappings, when source extent == unlim_extent_source */
    hsize_t clip_size_source; /* Size selection would be clipped to in source selection when virtual extent ==
                                 unlim_extent_virtual */
    H5O_virtual_space_status_t source_space_status;  /* Extent patching status of source_select */
    H5O_virtual_space_status_t virtual_space_status; /* Extent patching status of virtual_select */
} H5O_storage_virtual_ent_t;

typedef struct H5O_storage_virtual_t {
    /* Stored in message */
    H5HG_t serial_list_hobjid; /* Global heap ID for the list of virtual mapping entries stored on disk */

    /* Stored in heap */
    size_t                     list_nused; /* Number of array elements used in list    */
    H5O_storage_virtual_ent_t *list;       /* Array of virtual dataset mapping entries */

    /* Not stored */
    size_t list_nalloc; /* Number of slots allocated          */
    hsize_t
        min_dims[H5S_MAX_RANK]; /* Minimum extent of VDS (maximum of all non-unlimited selection bounds) */
    H5D_vds_view_t
            view; /* Method for calculating the extent of the virtual dataset with unlimited selections */
    hsize_t printf_gap; /* Maximum number of sequential missing source datasets before terminating the search
                           for more */
    hid_t source_fapl;  /* FAPL to use to open source files */
    hid_t source_dapl;  /* DAPL to use to open source datasets */
    bool  init;         /* Whether all information has been completely initialized */
} H5O_storage_virtual_t;

typedef struct H5O_storage_t {
    H5D_layout_t type; /* Type of layout                    */
    union {
        H5O_storage_contig_t  contig;  /* Information for contiguous storage */
        H5O_storage_chunk_t   chunk;   /* Information for chunked storage    */
        H5O_storage_compact_t compact; /* Information for compact storage    */
        H5O_storage_virtual_t virt;    /* Information for virtual storage    */
    } u;
} H5O_storage_t;

typedef struct H5O_layout_chunk_farray_t {
    /* Creation parameters for fixed array data structure */
    struct {
        uint8_t max_dblk_page_nelmts_bits; /* Log2(Max. # of elements in a data block page) -
                                              i.e. # of bits needed to store max. # of elements
                                              in a data block page */
    } cparam;
} H5O_layout_chunk_farray_t;

typedef struct H5O_layout_chunk_earray_t {
    /* Creation parameters for extensible array data structure */
    struct {
        uint8_t max_nelmts_bits; /* Log2(Max. # of elements in array) - i.e. # of bits needed to store max. #
                                    of elements */
        uint8_t idx_blk_elmts;   /* # of elements to store in index block */
        uint8_t data_blk_min_elmts;        /* Min. # of elements per data block */
        uint8_t sup_blk_min_data_ptrs;     /* Min. # of data block pointers for a super block */
        uint8_t max_dblk_page_nelmts_bits; /* Log2(Max. # of elements in data block page) - i.e. # of bits
                                              needed to store max. # of elements in data block page */
    } cparam;

    unsigned unlim_dim;                                 /* Rank of unlimited dimension for dataset */
    uint32_t swizzled_dim[H5O_LAYOUT_NDIMS];            /* swizzled chunk dimensions */
    hsize_t  swizzled_down_chunks[H5O_LAYOUT_NDIMS];    /* swizzled "down" size of number of chunks in each
                                                           dimension */
    hsize_t swizzled_max_down_chunks[H5O_LAYOUT_NDIMS]; /* swizzled max "down" size of number of chunks in
                                                           each dimension */
} H5O_layout_chunk_earray_t;

typedef struct H5O_layout_chunk_bt2_t {
    /* Creation parameters for v2 B-tree data structure */
    struct {
        uint32_t node_size;     /* Size of each node (in bytes) */
        uint8_t  split_percent; /* % full to split nodes */
        uint8_t  merge_percent; /* % full to merge nodes */
    } cparam;
} H5O_layout_chunk_bt2_t;

typedef struct H5O_layout_chunk_t {
    H5D_chunk_index_t idx_type;                      /* Type of chunk index               */
    uint8_t           flags;                         /* Chunk layout flags                */
    unsigned          ndims;                         /* Num dimensions in chunk           */
    uint32_t          dim[H5O_LAYOUT_NDIMS];         /* Size of chunk in elements         */
    unsigned          enc_bytes_per_dim;             /* Encoded # of bytes for storing each chunk dimension */
    uint32_t          size;                          /* Size of chunk in bytes            */
    hsize_t           nchunks;                       /* Number of chunks in dataset	     */
    hsize_t           max_nchunks;                   /* Max. number of chunks in dataset	     */
    hsize_t           chunks[H5O_LAYOUT_NDIMS];      /* # of chunks in each dataset dimension  */
    hsize_t           max_chunks[H5O_LAYOUT_NDIMS];  /* # of chunks in each dataset's max. dimension */
    hsize_t           down_chunks[H5O_LAYOUT_NDIMS]; /* "down" size of number of chunks in each dimension */
    hsize_t           max_down_chunks[H5O_LAYOUT_NDIMS]; /* "down" size of number of chunks in each max dim */
    union {
        H5O_layout_chunk_farray_t farray; /* Information for fixed array index */
        H5O_layout_chunk_earray_t earray; /* Information for extensible array index */
        H5O_layout_chunk_bt2_t    btree2; /* Information for v2 B-tree index */
    } u;
} H5O_layout_chunk_t;

typedef struct H5O_layout_t {
    H5D_layout_t                   type;    /* Type of layout                    */
    unsigned                       version; /* Version of message                */
    const struct H5D_layout_ops_t *ops;     /* Pointer to data layout I/O operations */
    union {
        H5O_layout_chunk_t chunk; /* Information for chunked layout    */
    } u;
    H5O_storage_t storage; /* Information for storing dataset elements */
} H5O_layout_t;

#ifdef H5O_ENABLE_BOGUS
/*
 * "Bogus" Message.
 * (Data structure in memory)
 */
#define H5O_BOGUS_VALUE 0xdeadbeef
typedef struct H5O_bogus_t {
    H5O_shared_t sh_loc; /* Shared message info (must be first) */
    unsigned     u;      /* Hold the bogus info */
} H5O_bogus_t;
#endif /* H5O_ENABLE_BOGUS */

/*
 * Group info message.
 * (Contains constant information about a group)
 * (Data structure in memory)
 * (if the fields in this struct are changed, remember to change the default
 *      group info structure in src/H5Gprivate.h - QAK)
 */
typedef struct H5O_ginfo_t {
    /* "Old" format group info (not stored) */
    uint32_t lheap_size_hint; /* Local heap size hint              */

    /* "New" format group info (stored) */

    /* (storage management info) */
    bool     store_link_phase_change; /* Whether to store the link phase change values */
    uint16_t max_compact;             /* Maximum # of compact links        */
    uint16_t min_dense;               /* Minimum # of "dense" links        */

    /* (initial object header size info) */
    bool     store_est_entry_info; /* Whether to store the est. entry values */
    uint16_t est_num_entries;      /* Estimated # of entries in group   */
    uint16_t est_name_len;         /* Estimated length of entry name    */
} H5O_ginfo_t;

/*
 * Filter pipeline message.
 * (Data structure in memory)
 */

/* The initial version of the format */
#define H5O_PLINE_VERSION_1 1

/* This version encodes the message fields more efficiently */
/* (Drops the reserved bytes, doesn't align the name and doesn't encode the
 *      filter name at all if it's a filter provided by the library)
 */
#define H5O_PLINE_VERSION_2 2

/* The latest version of the format.  Look through the 'encode' and 'size'
 *      callbacks for places to change when updating this. */
#define H5O_PLINE_VERSION_LATEST H5O_PLINE_VERSION_2

typedef struct H5O_pline_t {
    H5O_shared_t sh_loc; /* Shared message info (must be first) */

    unsigned           version; /* Encoding version number */
    size_t             nalloc;  /*num elements in `filter' array     */
    size_t             nused;   /*num filters defined		     */
    H5Z_filter_info_t *filter;  /*array of filters		     */
} H5O_pline_t;

/*
 * Object name message.
 * (Data structure in memory)
 */

typedef struct H5O_name_t {
    char *s; /*ptr to malloc'd memory	     */
} H5O_name_t;

/*
 * Shared message table message
 * Information about file-wide shared message table, stored in superblock
 * extension
 * (Data structure in memory)
 */
typedef struct H5O_shmesg_table_t {
    haddr_t  addr;     /*file address of SOHM table */
    unsigned version;  /*SOHM table version number */
    unsigned nindexes; /*number of indexes in the table */
} H5O_shmesg_table_t;

/*
 * Object header continuation message.
 * (Data structure in memory)
 */

typedef struct H5O_cont_t {
    haddr_t addr; /*address of continuation block	     */
    size_t  size; /*size of continuation block	     */

    /* the following field(s) do not appear on disk */
    unsigned chunkno; /*chunk this mesg refers to	     */
} H5O_cont_t;

/*
 * Symbol table message.
 * (Data structure in memory)
 */
typedef struct H5O_stab_t {
    haddr_t btree_addr; /*address of B-tree		     */
    haddr_t heap_addr;  /*address of name heap		     */
} H5O_stab_t;

/*
 * v1 B-tree 'K' value message
 * Information about file-wide non-default v1 B-tree 'K' values, stored in
 * superblock extension
 * (Data structure in memory)
 */
typedef struct H5O_btreek_t {
    unsigned btree_k[H5B_NUM_BTREE_ID]; /* B-tree internal node 'K' values */
    unsigned sym_leaf_k;                /* Symbol table leaf node's 'K' value */
} H5O_btreek_t;

/*
 * Driver info message
 * Information about driver info, stored in superblock extension
 * (Data structure in memory)
 */
typedef struct H5O_drvinfo_t {
    /* Information for H5AC cache functions, _must_ be first field in structure */
    H5AC_info_t cache_info;
    char        name[9]; /* Driver name */
    size_t      len;     /* Length of encoded buffer */
    uint8_t    *buf;     /* Buffer for encoded info */
} H5O_drvinfo_t;

/*
 * Attribute Info Message.
 * (Contains dynamic information about attributes on an object)
 * (Data structure in memory)
 */
typedef struct H5O_ainfo_t {
    /* Creation order info */
    bool              track_corder; /* Are creation order values tracked on attributes? */
    bool              index_corder; /* Are creation order values indexed on attributes? */
    H5O_msg_crt_idx_t max_crt_idx;  /* Maximum attribute creation index used */
    haddr_t
        corder_bt2_addr; /* Address of v2 B-tree for indexing creation order values of "dense" attributes */

    /* Storage management info */
    hsize_t nattrs;        /* Number of attributes on the object */
    haddr_t fheap_addr;    /* Address of fractal heap for storing "dense" attributes */
    haddr_t name_bt2_addr; /* Address of v2 B-tree for indexing names of "dense" attributes */
} H5O_ainfo_t;

/*
 * Reference Count Message.
 * (Data structure in memory)
 */
typedef uint32_t H5O_refcount_t; /* Contains # of links to object, if >1 */

/*
 * "Unknown" Message.
 * (Data structure in memory)
 */
typedef unsigned H5O_unknown_t; /* Original message type ID */

/* To indicate an invalid version for a message that does not exist yet for the release */
/* Message version is 1 byte so the value can be 0 to 255 */
#define H5O_INVALID_VERSION 256

/* The initial version of the fsinfo message: deprecated */
/* This version is mapped to version 1 from release 1.10.1 onwards */
#define H5O_FSINFO_VERSION_0 0

/* The latest version for fsinfo message */
#define H5O_FSINFO_VERSION_1      1
#define H5O_FSINFO_VERSION_LATEST H5O_FSINFO_VERSION_1
/*
 * File space info Message.
 * Contains file space management info and
 * addresses of free space managers for file memory
 * (Data structure in memory)
 */
typedef struct H5O_fsinfo_t {
    unsigned              version;          /* Version number */
    H5F_fspace_strategy_t strategy;         /* File space strategy */
    bool                  persist;          /* Persisting free-space or not */
    hsize_t               threshold;        /* Free-space section threshold */
    hsize_t               page_size;        /* For paged aggregation: file space page size */
    size_t                pgend_meta_thres; /* For paged aggregation: page end metadata threshold */
    haddr_t eoa_pre_fsm_fsalloc; /* For paged aggregation: the eoa before free-space headers & sinfo */
    haddr_t fs_addr[H5F_MEM_PAGE_NTYPES - 1]; /* 13 addresses of free-space managers */
                                              /* For non-paged aggregation: only 6 addresses are used */
    bool mapped;                              /* Not stored */
                                              /* Indicate the message is mapped from version 0 to version 1 */
} H5O_fsinfo_t;

/*
 * Metadata Cache Image Message.
 * Contains base address and length of the metadata cache image.
 * (Data structure in memory)
 */
typedef struct H5O_mdci_t {
    haddr_t addr; /* address of MDC image block */
    hsize_t size; /* size of MDC image block    */
} H5O_mdci_t;

/* Typedef for "application" iteration operations */
typedef herr_t (*H5O_operator_t)(const void *mesg /*in*/, unsigned idx, void *operator_data /*in,out*/);

/* Typedef for "internal library" iteration operations */
typedef herr_t (*H5O_lib_operator_t)(H5O_t *oh, H5O_mesg_t *mesg /*in,out*/, unsigned sequence,
                                     unsigned *oh_modified /*out*/, void *operator_data /*in,out*/);

/* Some syntactic sugar to make the compiler happy with two different kinds of iterator callbacks */
typedef enum H5O_mesg_operator_type_t {
    H5O_MESG_OP_APP, /* Application callback */
    H5O_MESG_OP_LIB  /* Library internal callback */
} H5O_mesg_operator_type_t;

#define H5O_MODIFY_CONDENSE 0x01
#define H5O_MODIFY          0x02

typedef struct {
    H5O_mesg_operator_type_t op_type;
    union {
        H5O_operator_t     app_op; /* Application callback for each message */
        H5O_lib_operator_t lib_op; /* Library internal callback for each message */
    } u;
} H5O_mesg_operator_t;

/* Typedef for abstract object creation */
typedef struct {
    H5O_type_t obj_type; /* Type of object to create */
    void      *crt_info; /* Information for object creation callback */
    void      *new_obj;  /* Pointer to new object created */
} H5O_obj_create_t;

/* Forward declarations for prototype arguments */
struct H5P_genplist_t;

/* Object header routines */
H5_DLL herr_t H5O_init(void);
H5_DLL herr_t H5O_create(H5F_t *f, size_t size_hint, size_t initial_rc, hid_t ocpl_id,
                         H5O_loc_t *loc /*out*/);
H5_DLL H5O_t *H5O_create_ohdr(H5F_t *f, hid_t ocpl_id);
H5_DLL herr_t H5O_apply_ohdr(H5F_t *f, H5O_t *oh, hid_t ocpl_id, size_t size_hint, size_t initial_rc,
                             H5O_loc_t *loc_out);
H5_DLL herr_t H5O_open(H5O_loc_t *loc);
H5_DLL void  *H5O_open_by_loc(const H5G_loc_t *obj_loc, H5I_type_t *opened_type /*out*/);
H5_DLL herr_t H5O_close(H5O_loc_t *loc, bool *file_closed /*out*/);
H5_DLL int    H5O_link(const H5O_loc_t *loc, int adjust);
H5_DLL H5O_t *H5O_protect(const H5O_loc_t *loc, unsigned prot_flags, bool pin_all_chunks);
H5_DLL H5O_t *H5O_pin(const H5O_loc_t *loc);
H5_DLL herr_t H5O_unpin(H5O_t *oh);
H5_DLL herr_t H5O_dec_rc_by_loc(const H5O_loc_t *loc);
H5_DLL herr_t H5O_unprotect(const H5O_loc_t *loc, H5O_t *oh, unsigned oh_flags);
H5_DLL herr_t H5O_touch(const H5O_loc_t *loc, bool force);
H5_DLL herr_t H5O_touch_oh(H5F_t *f, H5O_t *oh, bool force);
#ifdef H5O_ENABLE_BOGUS
H5_DLL herr_t H5O_bogus_oh(H5F_t *f, H5O_t *oh, unsigned mesg_id, unsigned mesg_flags);
#endif /* H5O_ENABLE_BOGUS */
H5_DLL herr_t  H5O_delete(H5F_t *f, haddr_t addr);
H5_DLL herr_t  H5O_get_hdr_info(const H5O_loc_t *oloc, H5O_hdr_info_t *hdr);
H5_DLL herr_t  H5O_get_info(const H5O_loc_t *oloc, H5O_info2_t *oinfo, unsigned fields);
H5_DLL herr_t  H5O_get_native_info(const H5O_loc_t *oloc, H5O_native_info_t *oinfo, unsigned fields);
H5_DLL herr_t  H5O_obj_type(const H5O_loc_t *loc, H5O_type_t *obj_type);
H5_DLL herr_t  H5O_get_create_plist(const H5O_loc_t *loc, struct H5P_genplist_t *oc_plist);
H5_DLL void   *H5O_open_name(const H5G_loc_t *loc, const char *name, H5I_type_t *opened_type /*out*/);
H5_DLL herr_t  H5O_get_nlinks(const H5O_loc_t *loc, hsize_t *nlinks);
H5_DLL void   *H5O_obj_create(H5F_t *f, H5O_type_t obj_type, void *crt_info, H5G_loc_t *obj_loc);
H5_DLL haddr_t H5O_get_oh_addr(const H5O_t *oh);
H5_DLL uint8_t H5O_get_oh_flags(const H5O_t *oh);
H5_DLL time_t  H5O_get_oh_mtime(const H5O_t *oh);
H5_DLL uint8_t H5O_get_oh_version(const H5O_t *oh);
H5_DLL herr_t  H5O_get_rc_and_type(const H5O_loc_t *oloc, unsigned *rc, H5O_type_t *otype);
H5_DLL H5AC_proxy_entry_t *H5O_get_proxy(const H5O_t *oh);

/* Object header message routines */
H5_DLL herr_t H5O_msg_create(const H5O_loc_t *loc, unsigned type_id, unsigned mesg_flags,
                             unsigned update_flags, void *mesg);
H5_DLL herr_t H5O_msg_append_oh(H5F_t *f, H5O_t *oh, unsigned type_id, unsigned mesg_flags,
                                unsigned update_flags, void *mesg);
H5_DLL herr_t H5O_msg_write(const H5O_loc_t *loc, unsigned type_id, unsigned mesg_flags,
                            unsigned update_flags, void *mesg);
H5_DLL herr_t H5O_msg_write_oh(H5F_t *f, H5O_t *oh, unsigned type_id, unsigned mesg_flags,
                               unsigned update_flags, void *mesg);
H5_DLL herr_t H5O_msg_flush(H5F_t *f, H5O_t *oh, H5O_mesg_t *mesg);
H5_DLL void  *H5O_msg_read(const H5O_loc_t *loc, unsigned type_id, void *mesg);
H5_DLL void  *H5O_msg_read_oh(H5F_t *f, H5O_t *oh, unsigned type_id, void *mesg);
H5_DLL herr_t H5O_msg_reset(unsigned type_id, void *native);
H5_DLL void  *H5O_msg_free(unsigned type_id, void *mesg);
H5_DLL void  *H5O_msg_free_real(const H5O_msg_class_t *type, void *mesg);
H5_DLL void  *H5O_msg_copy(unsigned type_id, const void *mesg, void *dst);
H5_DLL int    H5O_msg_count(const H5O_loc_t *loc, unsigned type_id);
H5_DLL htri_t H5O_msg_exists(const H5O_loc_t *loc, unsigned type_id);
H5_DLL htri_t H5O_msg_exists_oh(const H5O_t *oh, unsigned type_id);
H5_DLL herr_t H5O_msg_remove(const H5O_loc_t *loc, unsigned type_id, int sequence, bool adj_link);
H5_DLL herr_t H5O_msg_remove_op(const H5O_loc_t *loc, unsigned type_id, int sequence, H5O_operator_t op,
                                void *op_data, bool adj_link);
H5_DLL herr_t H5O_msg_iterate(const H5O_loc_t *loc, unsigned type_id, const H5O_mesg_operator_t *op,
                              void *op_data);
H5_DLL size_t H5O_msg_raw_size(const H5F_t *f, unsigned type_id, bool disable_shared, const void *mesg);
H5_DLL size_t H5O_msg_size_f(const H5F_t *f, hid_t ocpl_id, unsigned type_id, const void *mesg,
                             size_t extra_raw);
H5_DLL size_t H5O_msg_size_oh(const H5F_t *f, const H5O_t *oh, unsigned type_id, const void *mesg,
                              size_t extra_raw);
H5_DLL htri_t H5O_msg_is_shared(unsigned type_id, const void *mesg);
H5_DLL htri_t H5O_msg_can_share(unsigned type_id, const void *mesg);
H5_DLL htri_t H5O_msg_can_share_in_ohdr(unsigned type_id);
H5_DLL herr_t H5O_msg_set_share(unsigned type_id, const H5O_shared_t *share, void *mesg);
H5_DLL herr_t H5O_msg_reset_share(unsigned type_id, void *mesg);
H5_DLL herr_t H5O_msg_get_crt_index(unsigned type_id, const void *mesg, H5O_msg_crt_idx_t *crt_idx);
H5_DLL herr_t H5O_msg_encode(H5F_t *f, unsigned type_id, bool disable_shared, unsigned char *buf,
                             const void *obj);
H5_DLL void  *H5O_msg_decode(H5F_t *f, H5O_t *open_oh, unsigned type_id, size_t buf_size,
                             const unsigned char *buf);
H5_DLL herr_t H5O_msg_delete(H5F_t *f, H5O_t *open_oh, unsigned type_id, void *mesg);
H5_DLL herr_t H5O_msg_get_flags(const H5O_loc_t *loc, unsigned type_id, uint8_t *flags);

/* Object metadata flush/refresh routines */
H5_DLL herr_t H5O_flush(H5O_loc_t *oloc, hid_t obj_id);
H5_DLL herr_t H5O_flush_common(H5O_loc_t *oloc, hid_t obj_id);
H5_DLL herr_t H5O_refresh_metadata(H5O_loc_t *oloc, hid_t oid);
H5_DLL herr_t H5O_refresh_metadata_reopen(hid_t oid, hid_t apl_id, H5G_loc_t *obj_loc, H5VL_t *vol_driver,
                                          bool start_swmr);

/* Object copying routines */
H5_DLL herr_t H5O_copy_header_map(const H5O_loc_t *oloc_src, H5O_loc_t *oloc_dst /*out */,
                                  H5O_copy_t *cpy_info, bool inc_depth, H5O_type_t *obj_type, void **udata);
H5_DLL herr_t H5O_copy_expand_ref(H5F_t *file_src, hid_t tid_src, const H5T_t *dt_src, void *buf_src,
                                  size_t nbytes_src, H5F_t *file_dst, void *buf_dst, H5O_copy_t *cpy_info);

/* Debugging routines */
H5_DLL herr_t H5O_debug_id(unsigned type_id, H5F_t *f, const void *mesg, FILE *stream, int indent,
                           int fwidth);
H5_DLL herr_t H5O_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth);

/* These functions operate on object locations */
H5_DLL herr_t     H5O_loc_reset(H5O_loc_t *loc);
H5_DLL herr_t     H5O_loc_copy(H5O_loc_t *dst, H5O_loc_t *src, H5_copy_depth_t depth);
H5_DLL herr_t     H5O_loc_copy_shallow(H5O_loc_t *dst, H5O_loc_t *src);
H5_DLL herr_t     H5O_loc_copy_deep(H5O_loc_t *dst, const H5O_loc_t *src);
H5_DLL herr_t     H5O_loc_hold_file(H5O_loc_t *loc);
H5_DLL herr_t     H5O_loc_free(H5O_loc_t *loc);
H5_DLL H5O_loc_t *H5O_get_loc(hid_t id);

/* EFL operators */
H5_DLL hsize_t H5O_efl_total_size(H5O_efl_t *efl);

/* File space info routines */
H5_DLL herr_t H5O_fsinfo_set_version(H5F_libver_t low, H5F_libver_t high, H5O_fsinfo_t *fsinfo);
H5_DLL herr_t H5O_fsinfo_check_version(H5F_libver_t high, H5O_fsinfo_t *fsinfo);

/* Fill value operators */
H5_DLL herr_t H5O_fill_reset_dyn(H5O_fill_t *fill);
H5_DLL herr_t H5O_fill_convert(H5O_fill_t *fill, H5T_t *type, bool *fill_changed);
H5_DLL herr_t H5O_fill_set_version(H5F_t *f, H5O_fill_t *fill);

/* Link operators */
H5_DLL herr_t H5O_link_delete(H5F_t *f, H5O_t *open_oh, void *_mesg);

/* Filter pipeline operators */
H5_DLL herr_t H5O_pline_set_version(H5F_t *f, H5O_pline_t *pline);

/* Shared message operators */
H5_DLL herr_t H5O_set_shared(H5O_shared_t *dst, const H5O_shared_t *src);

#endif /* H5Oprivate_H */
