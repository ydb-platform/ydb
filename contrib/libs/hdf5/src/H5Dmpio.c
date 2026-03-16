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
 * Purpose:    Functions to read/write directly between app buffer and file.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Dmodule.h" /* This source code file is part of the H5D module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions */
#include "H5CXprivate.h" /* API Contexts      */
#include "H5Dpkg.h"      /* Datasets          */
#include "H5Eprivate.h"  /* Error handling    */
#include "H5Fprivate.h"  /* File access       */
#include "H5FDprivate.h" /* File drivers      */
#include "H5FLprivate.h" /* Free Lists        */
#include "H5Iprivate.h"  /* IDs               */
#include "H5MMprivate.h" /* Memory management */
#include "H5Oprivate.h"  /* Object headers    */
#include "H5Pprivate.h"  /* Property lists    */
#include "H5Sprivate.h"  /* Dataspaces        */
#include "H5VMprivate.h" /* Vector            */

#ifdef H5_HAVE_PARALLEL

/****************/
/* Local Macros */
/****************/

/* Macros to represent different IO options */
#define H5D_ONE_LINK_CHUNK_IO          0
#define H5D_MULTI_CHUNK_IO             1
#define H5D_ONE_LINK_CHUNK_IO_MORE_OPT 2
#define H5D_MULTI_CHUNK_IO_MORE_OPT    3

/* Macros to represent different IO modes(NONE, Independent or collective)for multiple chunk IO case */
#define H5D_CHUNK_IO_MODE_COL 1

/* Macros to represent the regularity of the selection for multiple chunk IO case. */
#define H5D_CHUNK_SELECT_REG 1

/*
 * Threshold value for redistributing shared filtered chunks
 * on all MPI ranks, or just MPI rank 0
 */
#define H5D_CHUNK_REDISTRIBUTE_THRES ((size_t)((25 * H5_MB) / sizeof(H5D_chunk_redistribute_info_t)))

/*
 * Initial allocation size for the arrays that hold
 * buffers for chunk modification data that is sent
 * to other ranks and the MPI_Request objects for
 * those send operations
 */
#define H5D_CHUNK_NUM_SEND_MSGS_INIT 64

/*
 * Define a tag value for the MPI messages sent/received for
 * chunk modification data
 */
#define H5D_CHUNK_MOD_DATA_TAG 64

/*
 * Macro to initialize a H5D_chk_idx_info_t
 * structure, given a pointer to a H5D_io_info_t
 * structure
 */
#define H5D_MPIO_INIT_CHUNK_IDX_INFO(index_info, dset)                                                       \
    do {                                                                                                     \
        (index_info).f       = (dset)->oloc.file;                                                            \
        (index_info).pline   = &((dset)->shared->dcpl_cache.pline);                                          \
        (index_info).layout  = &((dset)->shared->layout.u.chunk);                                            \
        (index_info).storage = &((dset)->shared->layout.storage.u.chunk);                                    \
    } while (0)

/******************/
/* Local Typedefs */
/******************/

/* Combine chunk/piece address and chunk/piece info into a struct for
 * better performance. */
typedef struct H5D_chunk_addr_info_t {
    /* piece for multi-dset */
    haddr_t          piece_addr;
    H5D_piece_info_t piece_info;
} H5D_chunk_addr_info_t;

/* Rank 0 Bcast values */
typedef enum H5D_mpio_no_rank0_bcast_cause_t {
    H5D_MPIO_RANK0_BCAST            = 0x00,
    H5D_MPIO_RANK0_NOT_H5S_ALL      = 0x01,
    H5D_MPIO_RANK0_NOT_CONTIGUOUS   = 0x02,
    H5D_MPIO_RANK0_NOT_FIXED_SIZE   = 0x04,
    H5D_MPIO_RANK0_GREATER_THAN_2GB = 0x08
} H5D_mpio_no_rank0_bcast_cause_t;

/*
 * Information necessary for re-allocating file space for a chunk
 * during a parallel write of a chunked dataset with filters
 * applied.
 */
typedef struct H5D_chunk_alloc_info_t {
    H5F_block_t chunk_current;
    H5F_block_t chunk_new;
    hsize_t     chunk_idx;
    haddr_t     dset_oloc_addr;
} H5D_chunk_alloc_info_t;

/*
 * Information for a chunk pertaining to the dataset's chunk
 * index entry for the chunk.
 *
 * NOTE: To support efficient lookups of H5D_filtered_collective_chunk_info_t
 * structures during parallel writes to filtered chunks, the
 * chunk_idx and dset_oloc_addr fields of this structure are used
 * together as a key for a hash table by following the approach
 * outlined at https://troydhanson.github.io/uthash/userguide.html#_compound_keys.
 * This means the following:
 *
 * - Instances of this structure should be memset to 0 when
 *   used for hashing to ensure that any padding between the
 *   chunk_idx and dset_oloc_addr fields does not affect the
 *   generated key.
 *
 * - The chunk_idx and dset_oloc_addr fields should be arranged
 *   in that specific order, as the code currently relies on
 *   this ordering when calculating the key length and it
 *   performs memory operations on the structure starting from
 *   the chunk_idx field and using the calculated key length.
 *
 * - The chunk_idx and dset_oloc_addr fields should ideally
 *   be arranged next to each other in the structure to minimize
 *   the calculated key length.
 */
typedef struct H5D_chunk_index_info_t {
    /*
     * These two fields must come in this order and next to
     * each other for proper and efficient hashing
     */
    hsize_t chunk_idx;
    haddr_t dset_oloc_addr;

    unsigned filter_mask;
    bool     need_insert;
} H5D_chunk_index_info_t;

/*
 * Information about a single chunk when performing collective filtered I/O. All
 * of the fields of one of these structs are initialized at the start of collective
 * filtered I/O in the function H5D__mpio_collective_filtered_chunk_io_setup(). This
 * struct's fields are as follows:
 *
 * index_info - A structure containing the information needed when collectively
 *              re-inserting the chunk into the dataset's chunk index. The structure
 *              is distributed to all ranks during the re-insertion operation. Its fields
 *              are as follows:
 *
 *     chunk_idx - The index of the chunk in the dataset's chunk index.
 *
 *     filter_mask - A bit-mask that indicates which filters are to be applied to the
 *                   chunk. Each filter in a chunk's filter pipeline has a bit position
 *                   that can be masked to disable that particular filter for the chunk.
 *                   This filter mask is saved alongside the chunk in the file.
 *
 *     need_insert - A flag which determines whether or not a chunk needs to be re-inserted into
 *                   the chunk index after the write operation.
 *
 * chunk_info - A pointer to the chunk's H5D_piece_info_t structure, which contains useful
 *              information like the dataspaces containing the selection in the chunk.
 *
 * chunk_current - The address in the file and size of this chunk before the filtering
 *                 operation. When reading a chunk from the file, this field is used to
 *                 read the correct amount of bytes. It is also used when redistributing
 *                 shared chunks among MPI ranks and as a parameter to the chunk file
 *                 space reallocation function.
 *
 * chunk_new - The address in the file and size of this chunk after the filtering
 *             operation. This field is relevant when collectively re-allocating space
 *             in the file for all of the chunks written to in the I/O operation, as
 *             their sizes may have changed after their data has been filtered.
 *
 * need_read - A flag which determines whether or not a chunk needs to be read from the
 *             file. During writes, if a chunk is being fully overwritten (the entire extent
 *             is selected in its file dataspace), then it is not necessary to read the chunk
 *             from the file. However, if the chunk is not being fully overwritten, it has to
 *             be read from the file in order to update the chunk without trashing the parts
 *             of the chunk that are not selected. During reads, this field should generally
 *             be true, but may be false if the chunk isn't allocated, for example.
 *
 * skip_filter_pline - A flag which determines whether to skip calls to the filter pipeline
 *                     for this chunk. This flag is mostly useful for correct handling of
 *                     partial edge chunks when the "don't filter partial edge chunks" flag
 *                     is set on the dataset's DCPL.
 *
 * io_size - The total size of I/O to this chunk. This field is an accumulation of the size of
 *           I/O to the chunk from each MPI rank which has the chunk selected and is used to
 *           determine the value for the previous `full_overwrite` flag.
 *
 * chunk_buf_size - The size in bytes of the data buffer allocated for the chunk
 *
 * orig_owner - The MPI rank which originally had this chunk selected at the beginning of
 *              the collective filtered I/O operation. This field is currently used when
 *              redistributing shared chunks among MPI ranks.
 *
 * new_owner - The MPI rank which has been selected to perform the modifications to this chunk.
 *
 * num_writers - The total number of MPI ranks writing to this chunk. This field is used when
 *               the new owner of a chunk is receiving messages from other MPI ranks that
 *               contain their selections in the chunk and the data to update the chunk with.
 *               The new owner must know how many MPI ranks it should expect messages from so
 *               that it can post an equal number of receive calls.
 *
 * buf - A pointer which serves the dual purpose of holding either the chunk data which is to be
 *       written to the file or the chunk data which has been read from the file.
 *
 * hh - A handle for hash tables provided by the uthash.h header
 *
 */
typedef struct H5D_filtered_collective_chunk_info_t {
    H5D_chunk_index_info_t index_info;

    H5D_piece_info_t *chunk_info;
    H5F_block_t       chunk_current;
    H5F_block_t       chunk_new;
    bool              need_read;
    bool              skip_filter_pline;
    size_t            io_size;
    size_t            chunk_buf_size;
    int               orig_owner;
    int               new_owner;
    int               num_writers;
    void             *buf;

    UT_hash_handle hh;
} H5D_filtered_collective_chunk_info_t;

/*
 * Information cached about each dataset involved when performing
 * collective I/O on filtered chunks.
 */
typedef struct H5D_mpio_filtered_dset_info_t {
    const H5D_dset_io_info_t *dset_io_info;
    H5D_fill_buf_info_t       fb_info;
    H5D_chk_idx_info_t        chunk_idx_info;
    hsize_t                   file_chunk_size;
    haddr_t                   dset_oloc_addr;
    H5S_t                    *fill_space;
    bool                      should_fill;
    bool                      fb_info_init;
    bool                      index_empty;

    UT_hash_handle hh;
} H5D_mpio_filtered_dset_info_t;

/*
 * Top-level structure that contains an array of H5D_filtered_collective_chunk_info_t
 * chunk info structures for collective filtered I/O, as well as other useful information.
 * The struct's fields are as follows:
 *
 * chunk_infos - An array of H5D_filtered_collective_chunk_info_t structures that each
 *               contain information about a single chunk when performing collective filtered
 *               I/O.
 *
 * chunk_hash_table - A hash table storing H5D_filtered_collective_chunk_info_t structures
 *                    that is populated when chunk modification data has to be shared between
 *                    MPI processes during collective filtered I/O. This hash table facilitates
 *                    quicker and easier lookup of a particular chunk by its "chunk index"
 *                    value when applying chunk data modification messages from another MPI
 *                    process. Each modification message received from another MPI process
 *                    will contain the chunk's "chunk index" value that can be used for chunk
 *                    lookup operations.
 *
 * chunk_hash_table_keylen - The calculated length of the key used for the chunk info hash
 *                           table, depending on whether collective I/O is being performed
 *                           on a single or multiple filtered datasets.
 *
 * num_chunks_infos - The number of entries in the `chunk_infos` array.
 *
 * num_chunks_to_read - The number of entries (or chunks) in the `chunk_infos` array that
 *                      will need to be read in the case of a read operation (which can
 *                      occur during dataset reads or during dataset writes when a chunk
 *                      has to go through a read - modify - write cycle). The value for
 *                      this field is based on a chunk's `need_read` field in its particular
 *                      H5D_filtered_collective_chunk_info_t structure, but may be adjusted
 *                      later depending on file space allocation timing and other factors.
 *
 *                      This field can be helpful to avoid needing to scan through the list
 *                      of chunk info structures to determine how big of I/O vectors to
 *                      allocate during read operations, as an example.
 *
 * all_dset_indices_empty - A boolean determining whether all the datasets involved in the
 *                          I/O operation have empty chunk indices. If this is the case,
 *                          collective read operations can be skipped during processing
 *                          of chunks.
 *
 * no_dset_index_insert_methods - A boolean determining whether all the datasets involved
 *                                in the I/O operation have no chunk index insertion
 *                                methods. If this is the case, collective chunk reinsertion
 *                                operations can be skipped during processing of chunks.
 *
 * single_dset_info - A pointer to a H5D_mpio_filtered_dset_info_t structure containing
 *                    information that is used when performing collective I/O on a single
 *                    filtered dataset.
 *
 * dset_info_hash_table - A hash table storing H5D_mpio_filtered_dset_info_t structures
 *                        that is populated when performing collective I/O on multiple
 *                        filtered datasets at a time using the multi-dataset I/O API
 *                        routines.
 *
 */
typedef struct H5D_filtered_collective_io_info_t {
    H5D_filtered_collective_chunk_info_t *chunk_infos;
    H5D_filtered_collective_chunk_info_t *chunk_hash_table;
    size_t                                chunk_hash_table_keylen;
    size_t                                num_chunk_infos;
    size_t                                num_chunks_to_read;
    bool                                  all_dset_indices_empty;
    bool                                  no_dset_index_insert_methods;

    union {
        H5D_mpio_filtered_dset_info_t *single_dset_info;
        H5D_mpio_filtered_dset_info_t *dset_info_hash_table;
    } dset_info;
} H5D_filtered_collective_io_info_t;

/*
 * Information necessary for redistributing shared chunks during
 * a parallel write of a chunked dataset with filters applied.
 */
typedef struct H5D_chunk_redistribute_info_t {
    H5F_block_t chunk_block;
    hsize_t     chunk_idx;
    haddr_t     dset_oloc_addr;
    int         orig_owner;
    int         new_owner;
    int         num_writers;
} H5D_chunk_redistribute_info_t;

/*
 * Information used when re-inserting a chunk into a dataset's
 * chunk index during a parallel write of a chunked dataset with
 * filters applied.
 */
typedef struct H5D_chunk_insert_info_t {
    H5F_block_t            chunk_block;
    H5D_chunk_index_info_t index_info;
} H5D_chunk_insert_info_t;

/********************/
/* Local Prototypes */
/********************/
static herr_t H5D__piece_io(H5D_io_info_t *io_info);
static herr_t H5D__multi_chunk_collective_io(H5D_io_info_t *io_info, H5D_dset_io_info_t *dset_info,
                                             int mpi_rank, int mpi_size);
static herr_t H5D__multi_chunk_filtered_collective_io(H5D_io_info_t *io_info, H5D_dset_io_info_t *dset_infos,
                                                      size_t num_dset_infos, int mpi_rank, int mpi_size);
static herr_t H5D__link_piece_collective_io(H5D_io_info_t *io_info, int mpi_rank);
static herr_t H5D__link_chunk_filtered_collective_io(H5D_io_info_t *io_info, H5D_dset_io_info_t *dset_infos,
                                                     size_t num_dset_infos, int mpi_rank, int mpi_size);
static herr_t H5D__inter_collective_io(H5D_io_info_t *io_info, const H5D_dset_io_info_t *di,
                                       H5S_t *file_space, H5S_t *mem_space);
static herr_t H5D__final_collective_io(H5D_io_info_t *io_info, hsize_t mpi_buf_count,
                                       MPI_Datatype mpi_file_type, MPI_Datatype mpi_buf_type);
static herr_t H5D__obtain_mpio_mode(H5D_io_info_t *io_info, H5D_dset_io_info_t *di, uint8_t assign_io_mode[],
                                    haddr_t chunk_addr[], int mpi_rank, int mpi_size);
static herr_t H5D__mpio_get_sum_chunk(const H5D_io_info_t *io_info, int *sum_chunkf);
static herr_t H5D__mpio_get_sum_chunk_dset(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info,
                                           int *sum_chunkf);
static herr_t H5D__mpio_collective_filtered_chunk_io_setup(const H5D_io_info_t      *io_info,
                                                           const H5D_dset_io_info_t *di,
                                                           size_t num_dset_infos, int mpi_rank,
                                                           H5D_filtered_collective_io_info_t *chunk_list);
static herr_t H5D__mpio_redistribute_shared_chunks(H5D_filtered_collective_io_info_t *chunk_list,
                                                   const H5D_io_info_t *io_info, int mpi_rank, int mpi_size,
                                                   size_t **rank_chunks_assigned_map);
static herr_t H5D__mpio_redistribute_shared_chunks_int(H5D_filtered_collective_io_info_t *chunk_list,
                                                       size_t *num_chunks_assigned_map,
                                                       bool all_ranks_involved, const H5D_io_info_t *io_info,
                                                       int mpi_rank, int mpi_size);
static herr_t H5D__mpio_share_chunk_modification_data(H5D_filtered_collective_io_info_t *chunk_list,
                                                      H5D_io_info_t *io_info, int mpi_rank,
                                                      int H5_ATTR_NDEBUG_UNUSED mpi_size,
                                                      unsigned char          ***chunk_msg_bufs,
                                                      int                      *chunk_msg_bufs_len);
static herr_t H5D__mpio_collective_filtered_chunk_read(H5D_filtered_collective_io_info_t *chunk_list,
                                                       const H5D_io_info_t *io_info, size_t num_dset_infos,
                                                       int mpi_rank);
static herr_t H5D__mpio_collective_filtered_chunk_update(H5D_filtered_collective_io_info_t *chunk_list,
                                                         unsigned char                    **chunk_msg_bufs,
                                                         int chunk_msg_bufs_len, const H5D_io_info_t *io_info,
                                                         size_t num_dset_infos, int mpi_rank);
static herr_t H5D__mpio_collective_filtered_chunk_reallocate(H5D_filtered_collective_io_info_t *chunk_list,
                                                             size_t        *num_chunks_assigned_map,
                                                             H5D_io_info_t *io_info, size_t num_dset_infos,
                                                             int mpi_rank, int mpi_size);
static herr_t H5D__mpio_collective_filtered_chunk_reinsert(H5D_filtered_collective_io_info_t *chunk_list,
                                                           size_t        *num_chunks_assigned_map,
                                                           H5D_io_info_t *io_info, size_t num_dset_infos,
                                                           int mpi_rank, int mpi_size);
static herr_t H5D__mpio_get_chunk_redistribute_info_types(MPI_Datatype *contig_type,
                                                          bool         *contig_type_derived,
                                                          MPI_Datatype *resized_type,
                                                          bool         *resized_type_derived);
static herr_t H5D__mpio_get_chunk_alloc_info_types(MPI_Datatype *contig_type, bool *contig_type_derived,
                                                   MPI_Datatype *resized_type, bool *resized_type_derived);
static herr_t H5D__mpio_get_chunk_insert_info_types(MPI_Datatype *contig_type, bool *contig_type_derived,
                                                    MPI_Datatype *resized_type, bool *resized_type_derived);
static herr_t H5D__mpio_collective_filtered_vec_io(const H5D_filtered_collective_io_info_t *chunk_list,
                                                   H5F_shared_t *f_sh, H5D_io_op_type_t op_type);
static int    H5D__cmp_piece_addr(const void *chunk_addr_info1, const void *chunk_addr_info2);
static int    H5D__cmp_filtered_collective_io_info_entry(const void *filtered_collective_io_info_entry1,
                                                         const void *filtered_collective_io_info_entry2);
static int    H5D__cmp_chunk_redistribute_info(const void *entry1, const void *entry2);
static int    H5D__cmp_chunk_redistribute_info_orig_owner(const void *entry1, const void *entry2);

#ifdef H5Dmpio_DEBUG
static herr_t H5D__mpio_debug_init(void);
static herr_t H5D__mpio_dump_collective_filtered_chunk_list(H5D_filtered_collective_io_info_t *chunk_list,
                                                            int                                mpi_rank);
#endif

/*********************/
/* Package Variables */
/*********************/

/*******************/
/* Local Variables */
/*******************/

/* Declare extern free list to manage the H5S_sel_iter_t struct */
H5FL_EXTERN(H5S_sel_iter_t);

#ifdef H5Dmpio_DEBUG

/* Flags to control debug actions in this file.
 * (Meant to be indexed by characters)
 *
 * These flags can be set with either (or both) the environment variable
 *      "H5D_mpio_Debug" set to a string containing one or more characters
 *      (flags) or by setting them as a string value for the
 *      "H5D_mpio_debug_key" MPI Info key.
 *
 * Supported characters in 'H5D_mpio_Debug' string:
 *      't' trace function entry and exit
 *      'f' log to file rather than debugging stream
 *      'm' show (rough) memory usage statistics
 *      'c' show critical timing information
 *
 *      To only show output from a particular MPI rank, specify its rank
 *      number as a character, e.g.:
 *
 *      '0' only show output from rank 0
 *
 *      To only show output from a particular range (up to 8 ranks supported
 *      between 0-9) of MPI ranks, specify the start and end ranks separated
 *      by a hyphen, e.g.:
 *
 *      '0-7' only show output from ranks 0 through 7
 *
 */
static int               H5D_mpio_debug_flags_s[256];
static int               H5D_mpio_debug_rank_s[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
static bool              H5D_mpio_debug_inited    = false;
static const char *const trace_in_pre             = "-> ";
static const char *const trace_out_pre            = "<- ";
static int               debug_indent             = 0;
static FILE             *debug_stream             = NULL;

/* Determine if this rank should output debugging info */
#define H5D_MPIO_DEBUG_THIS_RANK(rank)                                                                       \
    (H5D_mpio_debug_rank_s[0] < 0 || rank == H5D_mpio_debug_rank_s[0] || rank == H5D_mpio_debug_rank_s[1] || \
     rank == H5D_mpio_debug_rank_s[2] || rank == H5D_mpio_debug_rank_s[3] ||                                 \
     rank == H5D_mpio_debug_rank_s[4] || rank == H5D_mpio_debug_rank_s[5] ||                                 \
     rank == H5D_mpio_debug_rank_s[6] || rank == H5D_mpio_debug_rank_s[7])

/* Print some debugging string */
#define H5D_MPIO_DEBUG(rank, string)                                                                         \
    do {                                                                                                     \
        if (debug_stream && H5D_MPIO_DEBUG_THIS_RANK(rank)) {                                                \
            fprintf(debug_stream, "%*s(Rank %d) " string "\n", debug_indent, "", rank);                      \
            fflush(debug_stream);                                                                            \
        }                                                                                                    \
    } while (0)

/* Print some debugging string with printf-style arguments */
#define H5D_MPIO_DEBUG_VA(rank, string, ...)                                                                 \
    do {                                                                                                     \
        if (debug_stream && H5D_MPIO_DEBUG_THIS_RANK(rank)) {                                                \
            fprintf(debug_stream, "%*s(Rank %d) " string "\n", debug_indent, "", rank, __VA_ARGS__);         \
            fflush(debug_stream);                                                                            \
        }                                                                                                    \
    } while (0)

#define H5D_MPIO_TRACE_ENTER(rank)                                                                           \
    do {                                                                                                     \
        bool trace_flag = H5D_mpio_debug_flags_s[(int)'t'];                                                  \
                                                                                                             \
        if (trace_flag) {                                                                                    \
            H5D_MPIO_DEBUG_VA(rank, "%s%s", trace_in_pre, __func__);                                         \
            debug_indent += (int)strlen(trace_in_pre);                                                       \
        }                                                                                                    \
    } while (0)

#define H5D_MPIO_TRACE_EXIT(rank)                                                                            \
    do {                                                                                                     \
        bool trace_flag = H5D_mpio_debug_flags_s[(int)'t'];                                                  \
                                                                                                             \
        if (trace_flag) {                                                                                    \
            debug_indent -= (int)strlen(trace_out_pre);                                                      \
            H5D_MPIO_DEBUG_VA(rank, "%s%s", trace_out_pre, __func__);                                        \
        }                                                                                                    \
    } while (0)

#define H5D_MPIO_TIME_START(rank, op_name)                                                                   \
    {                                                                                                        \
        bool              time_flag  = H5D_mpio_debug_flags_s[(int)'c'];                                     \
        double            start_time = 0.0, end_time = 0.0;                                                  \
        const char *const op = op_name;                                                                      \
                                                                                                             \
        if (time_flag) {                                                                                     \
            start_time = MPI_Wtime();                                                                        \
        }

#define H5D_MPIO_TIME_STOP(rank)                                                                             \
    if (time_flag) {                                                                                         \
        end_time = MPI_Wtime();                                                                              \
        H5D_MPIO_DEBUG_VA(rank, "'%s' took %f seconds", op, (end_time - start_time));                        \
    }                                                                                                        \
    }

/*---------------------------------------------------------------------------
 * Function:    H5D__mpio_parse_debug_str
 *
 * Purpose:     Parse a string for H5Dmpio-related debugging flags
 *
 * Returns:     N/A
 *
 *---------------------------------------------------------------------------
 */
static void
H5D__mpio_parse_debug_str(const char *s)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(s);

    while (*s) {
        int c = (int)(*s);

        if (c >= (int)'0' && c <= (int)'9') {
            bool range = false;

            if (*(s + 1) && *(s + 2))
                range = (int)*(s + 1) == '-' && (int)*(s + 2) >= (int)'0' && (int)*(s + 2) <= (int)'9';

            if (range) {
                int start_rank = c - (int)'0';
                int end_rank   = (int)*(s + 2) - '0';
                int num_ranks  = end_rank - start_rank + 1;
                int i;

                if (num_ranks > 8) {
                    end_rank  = start_rank + 7;
                    num_ranks = 8;
                }

                for (i = 0; i < num_ranks; i++)
                    H5D_mpio_debug_rank_s[i] = start_rank++;

                s += 3;
            }
            else
                H5D_mpio_debug_rank_s[0] = c - (int)'0';
        }
        else
            H5D_mpio_debug_flags_s[c]++;

        s++;
    }

    FUNC_LEAVE_NOAPI_VOID
}

static herr_t
H5D__mpio_debug_init(void)
{
    const char *debug_str;
    herr_t      ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_NOERR

    assert(!H5D_mpio_debug_inited);

    /* Clear the debug flag buffer */
    memset(H5D_mpio_debug_flags_s, 0, sizeof(H5D_mpio_debug_flags_s));

    /* Retrieve and parse the H5Dmpio debug string */
    debug_str = getenv("H5D_mpio_Debug");
    if (debug_str)
        H5D__mpio_parse_debug_str(debug_str);

    if (H5DEBUG(D))
        debug_stream = H5DEBUG(D);

    H5D_mpio_debug_inited = true;

    FUNC_LEAVE_NOAPI(ret_value)
}

#endif

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_opt_possible
 *
 * Purpose:     Checks if an direct I/O transfer is possible between memory and
 *              the file.
 *
 *              This was derived from H5D__mpio_opt_possible for
 *              multi-dset work.
 *
 * Return:      Success:   Non-negative: true or false
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5D__mpio_opt_possible(H5D_io_info_t *io_info)
{
    H5FD_mpio_xfer_t io_xfer_mode; /* MPI I/O transfer mode */
    size_t           i;
    H5D_t           *dset;
    const H5S_t     *file_space;
    const H5S_t     *mem_space;
    H5D_type_info_t *type_info;
    unsigned         local_cause[2] = {0, 0}; /* [0] Local reason(s) for breaking collective mode */
                                              /* [1] Flag if dataset is both: H5S_ALL and small */
    unsigned global_cause[2] = {0, 0};        /* Global reason(s) for breaking collective mode */
    htri_t   is_vl_storage;    /* Whether the dataset's datatype is stored in a variable-length form */
    htri_t   ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(io_info);

    for (i = 0; i < io_info->count; i++) {
        assert(io_info->dsets_info[i].file_space);
        assert(io_info->dsets_info[i].mem_space);
    }

    /* For independent I/O, get out quickly and don't try to form consensus */
    if (H5CX_get_io_xfer_mode(&io_xfer_mode) < 0)
        /* Set error flag, but keep going */
        local_cause[0] |= H5D_MPIO_ERROR_WHILE_CHECKING_COLLECTIVE_POSSIBLE;
    if (io_xfer_mode == H5FD_MPIO_INDEPENDENT)
        local_cause[0] |= H5D_MPIO_SET_INDEPENDENT;

    for (i = 0; i < io_info->count; i++) {
        /* Check for skipped I/O */
        if (io_info->dsets_info[i].skip_io)
            continue;

        /* Set convenience pointers */
        dset       = io_info->dsets_info[i].dset;
        file_space = io_info->dsets_info[i].file_space;
        mem_space  = io_info->dsets_info[i].mem_space;
        type_info  = &io_info->dsets_info[i].type_info;

        /* Optimized MPI types flag must be set */
        /* (based on 'HDF5_MPI_OPT_TYPES' environment variable) */
        if (!H5FD_mpi_opt_types_g)
            local_cause[0] |= H5D_MPIO_MPI_OPT_TYPES_ENV_VAR_DISABLED;

        /* Decision on whether to use selection I/O should have been made by now */
        assert(io_info->use_select_io != H5D_SELECTION_IO_MODE_DEFAULT);

        /* Datatype conversions and transformations are allowed with selection I/O.  If the selection I/O mode
         * is auto (default), disable collective for now and re-enable later if we can */
        if (io_info->use_select_io != H5D_SELECTION_IO_MODE_ON) {
            /* Don't allow collective operations if datatype conversions need to happen */
            if (!type_info->is_conv_noop)
                local_cause[0] |= H5D_MPIO_DATATYPE_CONVERSION;

            /* Don't allow collective operations if data transform operations should occur */
            if (!type_info->is_xform_noop)
                local_cause[0] |= H5D_MPIO_DATA_TRANSFORMS;
        }

        /* Check whether these are both simple or scalar dataspaces */
        if (!((H5S_SIMPLE == H5S_GET_EXTENT_TYPE(mem_space) || H5S_SCALAR == H5S_GET_EXTENT_TYPE(mem_space) ||
               H5S_NULL == H5S_GET_EXTENT_TYPE(mem_space)) &&
              (H5S_SIMPLE == H5S_GET_EXTENT_TYPE(file_space) ||
               H5S_SCALAR == H5S_GET_EXTENT_TYPE(file_space))))
            local_cause[0] |= H5D_MPIO_NOT_SIMPLE_OR_SCALAR_DATASPACES;

        /* Dataset storage must be contiguous or chunked */
        if (!(dset->shared->layout.type == H5D_CONTIGUOUS || dset->shared->layout.type == H5D_CHUNKED))
            local_cause[0] |= H5D_MPIO_NOT_CONTIGUOUS_OR_CHUNKED_DATASET;

        /* check if external-file storage is used */
        if (dset->shared->dcpl_cache.efl.nused > 0)
            local_cause[0] |= H5D_MPIO_NOT_CONTIGUOUS_OR_CHUNKED_DATASET;

            /* The handling of memory space is different for chunking and contiguous
             *  storage.  For contiguous storage, mem_space and file_space won't change
             *  when it it is doing disk IO.  For chunking storage, mem_space will
             *  change for different chunks. So for chunking storage, whether we can
             *  use collective IO will defer until each chunk IO is reached.
             */

#ifndef H5_HAVE_PARALLEL_FILTERED_WRITES
        /* Don't allow writes to filtered datasets if the functionality is disabled */
        if (io_info->op_type == H5D_IO_OP_WRITE && dset->shared->dcpl_cache.pline.nused > 0)
            local_cause[0] |= H5D_MPIO_PARALLEL_FILTERED_WRITES_DISABLED;
#endif

        /* Check if we would be able to perform collective if we could use selection I/O.  If so add reasons
         * for not using selection I/O to local_cause[0] */
        if ((io_info->use_select_io == H5D_SELECTION_IO_MODE_OFF) && local_cause[0] &&
            !(local_cause[0] &
              ~((unsigned)H5D_MPIO_DATATYPE_CONVERSION | (unsigned)H5D_MPIO_DATA_TRANSFORMS))) {
            assert(io_info->no_selection_io_cause & H5D_MPIO_NO_SELECTION_IO_CAUSES);
            local_cause[0] |= H5D_MPIO_NO_SELECTION_IO;
        }

        /* Check if we are able to do a MPI_Bcast of the data from one rank
         * instead of having all the processes involved in the collective I/O call.
         */

        /* Check to see if the process is reading the entire dataset */
        if (H5S_GET_SELECT_TYPE(file_space) != H5S_SEL_ALL)
            local_cause[1] |= H5D_MPIO_RANK0_NOT_H5S_ALL;
        /* Only perform this optimization for contiguous datasets, currently */
        else if (H5D_CONTIGUOUS != dset->shared->layout.type)
            /* Flag to do a MPI_Bcast of the data from one proc instead of
             * having all the processes involved in the collective I/O.
             */
            local_cause[1] |= H5D_MPIO_RANK0_NOT_CONTIGUOUS;
        else if ((is_vl_storage = H5T_is_vl_storage(type_info->dset_type)) < 0)
            local_cause[0] |= H5D_MPIO_ERROR_WHILE_CHECKING_COLLECTIVE_POSSIBLE;
        else if (is_vl_storage)
            local_cause[1] |= H5D_MPIO_RANK0_NOT_FIXED_SIZE;
        else {
            size_t type_size; /* Size of dataset's datatype */

            /* Retrieve the size of the dataset's datatype */
            if (0 == (type_size = H5T_GET_SIZE(type_info->dset_type)))
                local_cause[0] |= H5D_MPIO_ERROR_WHILE_CHECKING_COLLECTIVE_POSSIBLE;
            else {
                hssize_t snelmts; /* [Signed] # of elements in dataset's dataspace */

                /* Retrieve the size of the dataset's datatype */
                if ((snelmts = H5S_GET_EXTENT_NPOINTS(file_space)) < 0)
                    local_cause[0] |= H5D_MPIO_ERROR_WHILE_CHECKING_COLLECTIVE_POSSIBLE;
                else {
                    hsize_t dset_size;

                    /* Determine dataset size */
                    dset_size = ((hsize_t)snelmts) * type_size;

                    /* If the size of the dataset is less than 2GB then do an MPI_Bcast
                     * of the data from one process instead of having all the processes
                     * involved in the collective I/O.
                     */
                    if (dset_size > ((hsize_t)(2.0F * H5_GB) - 1))
                        local_cause[1] |= H5D_MPIO_RANK0_GREATER_THAN_2GB;
                } /* end else */
            }     /* end else */
        }         /* end else */
    }             /* end for loop */

    /* Check for independent I/O */
    if (local_cause[0] & H5D_MPIO_SET_INDEPENDENT)
        global_cause[0] = local_cause[0];
    else {
        int mpi_code; /* MPI error code */

        /* Form consensus opinion among all processes about whether to perform
         * collective I/O
         */
        if (MPI_SUCCESS !=
            (mpi_code = MPI_Allreduce(local_cause, global_cause, 2, MPI_UNSIGNED, MPI_BOR, io_info->comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Allreduce failed", mpi_code)
    } /* end else */

    /* Set the local & global values of no-collective-cause in the API context */
    H5CX_set_mpio_local_no_coll_cause(local_cause[0]);
    H5CX_set_mpio_global_no_coll_cause(global_cause[0]);

    /* Set read-with-rank0-and-bcast flag if possible */
    if (global_cause[0] == 0 && global_cause[1] == 0) {
        H5CX_set_mpio_rank0_bcast(true);
#ifdef H5_HAVE_INSTRUMENTED_LIBRARY
        H5CX_test_set_mpio_coll_rank0_bcast(true);
#endif /* H5_HAVE_INSTRUMENTED_LIBRARY */
    }  /* end if */

    /* Set the return value, based on the global cause */
    ret_value = global_cause[0] > 0 ? false : true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__mpio_opt_possible() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_get_no_coll_cause_strings
 *
 * Purpose:     When collective I/O is broken internally, it can be useful
 *              for users to see a representative string for the reason(s)
 *              why it was broken. This routine inspects the current
 *              "cause" flags from the API context and prints strings into
 *              the caller's buffers for the local and global reasons that
 *              collective I/O was broken.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__mpio_get_no_coll_cause_strings(char *local_cause, size_t local_cause_len, char *global_cause,
                                    size_t global_cause_len)
{
    uint32_t local_no_coll_cause;
    uint32_t global_no_coll_cause;
    size_t   local_cause_bytes_written  = 0;
    size_t   global_cause_bytes_written = 0;
    int      nbits;
    herr_t   ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert((local_cause && local_cause_len > 0) || (global_cause && global_cause_len > 0));

    /*
     * Use compile-time assertion so this routine is updated
     * when any new "no collective cause" values are added
     */
    HDcompile_assert(H5D_MPIO_NO_COLLECTIVE_MAX_CAUSE == (H5D_mpio_no_collective_cause_t)0x200);

    /* Initialize output buffers */
    if (local_cause)
        *local_cause = '\0';
    if (global_cause)
        *global_cause = '\0';

    /* Retrieve the local and global cause flags from the API context */
    if (H5CX_get_mpio_local_no_coll_cause(&local_no_coll_cause) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "unable to get local no collective cause value");
    if (H5CX_get_mpio_global_no_coll_cause(&global_no_coll_cause) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "unable to get global no collective cause value");

    /*
     * Append each of the "reason for breaking collective I/O"
     * error messages to the local and global cause string buffers
     */
    nbits = 8 * sizeof(local_no_coll_cause);
    for (int bit_pos = 0; bit_pos < nbits; bit_pos++) {
        H5D_mpio_no_collective_cause_t cur_cause;
        const char                    *cause_str;
        size_t                         buf_space_left;

        cur_cause = (H5D_mpio_no_collective_cause_t)(1 << bit_pos);
        if (cur_cause == H5D_MPIO_NO_COLLECTIVE_MAX_CAUSE)
            break;

        switch (cur_cause) {
            case H5D_MPIO_SET_INDEPENDENT:
                cause_str = "independent I/O was requested";
                break;
            case H5D_MPIO_DATATYPE_CONVERSION:
                cause_str = "datatype conversions were required";
                break;
            case H5D_MPIO_DATA_TRANSFORMS:
                cause_str = "data transforms needed to be applied";
                break;
            case H5D_MPIO_MPI_OPT_TYPES_ENV_VAR_DISABLED:
                cause_str = "optimized MPI types flag wasn't set";
                break;
            case H5D_MPIO_NOT_SIMPLE_OR_SCALAR_DATASPACES:
                cause_str = "one of the dataspaces was neither simple nor scalar";
                break;
            case H5D_MPIO_NOT_CONTIGUOUS_OR_CHUNKED_DATASET:
                cause_str = "dataset was not contiguous or chunked";
                break;
            case H5D_MPIO_PARALLEL_FILTERED_WRITES_DISABLED:
                cause_str = "parallel writes to filtered datasets are disabled";
                break;
            case H5D_MPIO_ERROR_WHILE_CHECKING_COLLECTIVE_POSSIBLE:
                cause_str = "an error occurred while checking if collective I/O was possible";
                break;
            case H5D_MPIO_NO_SELECTION_IO:
                cause_str = "collective I/O may be supported by selection or vector I/O but that feature was "
                            "not possible (see causes via H5Pget_no_selection_io_cause())";
                break;

            case H5D_MPIO_COLLECTIVE:
            case H5D_MPIO_NO_COLLECTIVE_MAX_CAUSE:
            default:
                assert(0 && "invalid no collective cause reason");
                break;
        }

        /*
         * Determine if the local reasons for breaking collective I/O
         * included the current cause
         */
        if (local_cause && (cur_cause & local_no_coll_cause)) {
            buf_space_left = local_cause_len - local_cause_bytes_written;

            /*
             * Check if there were any previous error messages included. If
             * so, prepend a semicolon to separate the messages.
             */
            if (buf_space_left && local_cause_bytes_written) {
                strncat(local_cause, "; ", buf_space_left);
                local_cause_bytes_written += MIN(buf_space_left, 2);
                buf_space_left -= MIN(buf_space_left, 2);
            }

            if (buf_space_left) {
                strncat(local_cause, cause_str, buf_space_left);
                local_cause_bytes_written += MIN(buf_space_left, strlen(cause_str));
            }
        }

        /*
         * Determine if the global reasons for breaking collective I/O
         * included the current cause
         */
        if (global_cause && (cur_cause & global_no_coll_cause)) {
            buf_space_left = global_cause_len - global_cause_bytes_written;

            /*
             * Check if there were any previous error messages included. If
             * so, prepend a semicolon to separate the messages.
             */
            if (buf_space_left && global_cause_bytes_written) {
                strncat(global_cause, "; ", buf_space_left);
                global_cause_bytes_written += MIN(buf_space_left, 2);
                buf_space_left -= MIN(buf_space_left, 2);
            }

            if (buf_space_left) {
                strncat(global_cause, cause_str, buf_space_left);
                global_cause_bytes_written += MIN(buf_space_left, strlen(cause_str));
            }
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_get_no_coll_cause_strings() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_select_read
 *
 * Purpose:     MPI-IO function to read directly from app buffer to file.
 *
 *              This was referred from H5D__mpio_select_read for
 *              multi-dset work.
 *
 * Return:      non-negative on success, negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__mpio_select_read(const H5D_io_info_t *io_info, hsize_t mpi_buf_count, H5S_t H5_ATTR_UNUSED *file_space,
                      H5S_t H5_ATTR_UNUSED *mem_space)
{
    void  *rbuf      = NULL;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* memory addr from a piece with lowest file addr */
    rbuf = io_info->base_maddr.vp;

    /*OKAY: CAST DISCARDS CONST QUALIFIER*/
    H5_CHECK_OVERFLOW(mpi_buf_count, hsize_t, size_t);
    if (H5F_shared_block_read(io_info->f_sh, H5FD_MEM_DRAW, io_info->store_faddr, (size_t)mpi_buf_count,
                              rbuf) < 0)
        HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "can't finish collective parallel read");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_select_read() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_select_write
 *
 * Purpose:     MPI-IO function to write directly from app buffer to file.
 *
 *              This was referred from H5D__mpio_select_write for
 *              multi-dset work.
 *
 * Return:      non-negative on success, negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__mpio_select_write(const H5D_io_info_t *io_info, hsize_t mpi_buf_count, H5S_t H5_ATTR_UNUSED *file_space,
                       H5S_t H5_ATTR_UNUSED *mem_space)
{
    const void *wbuf      = NULL;
    herr_t      ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* memory addr from a piece with lowest file addr */
    wbuf = io_info->base_maddr.cvp;

    /*OKAY: CAST DISCARDS CONST QUALIFIER*/
    H5_CHECK_OVERFLOW(mpi_buf_count, hsize_t, size_t);
    if (H5F_shared_block_write(io_info->f_sh, H5FD_MEM_DRAW, io_info->store_faddr, (size_t)mpi_buf_count,
                               wbuf) < 0)
        HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "can't finish collective parallel write");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_select_write() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_get_sum_chunk
 *
 * Purpose:     Routine for obtaining total number of chunks to cover
 *              hyperslab selection selected by all processors.  Operates
 *              on all datasets in the operation.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_get_sum_chunk(const H5D_io_info_t *io_info, int *sum_chunkf)
{
    int    num_chunkf; /* Number of chunks to iterate over */
    size_t ori_num_chunkf;
    int    mpi_code; /* MPI return code */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Get the number of chunks to perform I/O on */
    num_chunkf     = 0;
    ori_num_chunkf = io_info->pieces_added;
    H5_CHECKED_ASSIGN(num_chunkf, int, ori_num_chunkf, size_t);

    /* Determine the summation of number of chunks for all processes */
    if (MPI_SUCCESS !=
        (mpi_code = MPI_Allreduce(&num_chunkf, sum_chunkf, 1, MPI_INT, MPI_SUM, io_info->comm)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Allreduce failed", mpi_code)

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_get_sum_chunk() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_get_sum_chunk_dset
 *
 * Purpose:     Routine for obtaining total number of chunks to cover
 *              hyperslab selection selected by all processors.  Operates
 *              on a single dataset.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_get_sum_chunk_dset(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info,
                             int *sum_chunkf)
{
    int    num_chunkf; /* Number of chunks to iterate over */
    size_t ori_num_chunkf;
    int    mpi_code; /* MPI return code */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check for non-chunked dataset, in this case we know the number of "chunks"
     * is simply the mpi size */
    assert(dset_info->layout->type == H5D_CHUNKED);

    /* Get the number of chunks to perform I/O on */
    num_chunkf     = 0;
    ori_num_chunkf = H5SL_count(dset_info->layout_io_info.chunk_map->dset_sel_pieces);
    H5_CHECKED_ASSIGN(num_chunkf, int, ori_num_chunkf, size_t);

    /* Determine the summation of number of chunks for all processes */
    if (MPI_SUCCESS !=
        (mpi_code = MPI_Allreduce(&num_chunkf, sum_chunkf, 1, MPI_INT, MPI_SUM, io_info->comm)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Allreduce failed", mpi_code)

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_get_sum_chunk_dset() */

/*-------------------------------------------------------------------------
 * Function:    H5D__piece_io
 *
 * Purpose:     Routine for
 *              1) choose an IO option:
 *                    a) One collective IO defined by one MPI derived datatype to link through all chunks
 *              or    b) multiple chunk IOs,to do MPI-IO for each chunk, the IO mode may be adjusted
 *                       due to the selection pattern for each chunk.
 *              For option a)
 *                      1. Sort the chunk address, obtain chunk info according to the sorted chunk address
 *                      2. Build up MPI derived datatype for each chunk
 *                      3. Build up the final MPI derived datatype
 *                      4. Set up collective IO property list
 *                      5. Do IO
 *              For option b)
 *                      1. Use MPI_gather and MPI_Bcast to obtain information of *collective/independent/none*
 *                         IO mode for each chunk of the selection
 *                      2. Depending on whether the IO mode is collective or independent or none,
 *                         Create either MPI derived datatype for each chunk to do collective IO or
 *                         just do independent IO or independent IO with file set view
 *                      3. Set up collective IO property list for collective mode
 *                      4. DO IO
 *
 * Return:      Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__piece_io(H5D_io_info_t *io_info)
{
    H5FD_mpio_chunk_opt_t chunk_opt_mode;
#ifdef H5Dmpio_DEBUG
    bool  log_file_flag  = false;
    FILE *debug_log_file = NULL;
#endif
    int      io_option        = H5D_MULTI_CHUNK_IO_MORE_OPT;
    bool     recalc_io_option = false;
    bool     use_multi_dset   = false;
    unsigned one_link_chunk_io_threshold; /* Threshold to use single collective I/O for all chunks */
    int      sum_chunk = -1;
    int      mpi_rank;
    int      mpi_size;
    size_t   i;
    herr_t   ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(io_info);
    assert(io_info->using_mpi_vfd);
    assert(io_info->count > 0);

    /* Obtain the current rank of the process and the number of ranks */
    if ((mpi_rank = H5F_mpi_get_rank(io_info->dsets_info[0].dset->oloc.file)) < 0)
        HGOTO_ERROR(H5E_IO, H5E_MPI, FAIL, "unable to obtain MPI rank");
    if ((mpi_size = H5F_mpi_get_size(io_info->dsets_info[0].dset->oloc.file)) < 0)
        HGOTO_ERROR(H5E_IO, H5E_MPI, FAIL, "unable to obtain MPI size");

#ifdef H5Dmpio_DEBUG
    /* Initialize file-level debugging if not initialized */
    if (!H5D_mpio_debug_inited && H5D__mpio_debug_init() < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize H5Dmpio debugging");

    /* Open file for debugging if necessary */
    log_file_flag = H5D_mpio_debug_flags_s[(int)'f'];
    if (log_file_flag) {
        char   debug_log_filename[1024];
        time_t time_now;

        snprintf(debug_log_filename, 1024, "H5Dmpio_debug.rank%d", mpi_rank);

        if (NULL == (debug_log_file = fopen(debug_log_filename, "a")))
            HGOTO_ERROR(H5E_IO, H5E_OPENERROR, FAIL, "couldn't open debugging log file");

        /* Print a short header for this I/O operation */
        time_now = HDtime(NULL);
        fprintf(debug_log_file, "##### %s", HDasctime(HDlocaltime(&time_now)));

        debug_stream = debug_log_file;
    }
#endif

    /* Check the optional property list for the collective chunk IO optimization option.
     * Only set here if it's a static option, if it needs to be calculated using the
     * number of chunks per process delay that calculation until later. */
    if (H5CX_get_mpio_chunk_opt_mode(&chunk_opt_mode) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "couldn't get chunk optimization option");

    if (H5FD_MPIO_CHUNK_ONE_IO == chunk_opt_mode)
        io_option = H5D_ONE_LINK_CHUNK_IO; /*no opt*/
    /* direct request to multi-chunk-io */
    else if (H5FD_MPIO_CHUNK_MULTI_IO == chunk_opt_mode)
        io_option = H5D_MULTI_CHUNK_IO;
    else
        recalc_io_option = true;

    /* Check if we can and should use multi dataset path */
    if (io_info->count > 1 && (io_option == H5D_ONE_LINK_CHUNK_IO || recalc_io_option)) {
        /* Use multi dataset path for now */
        use_multi_dset = true;

        /* Check if this I/O exceeds one linked chunk threshold */
        if (recalc_io_option && use_multi_dset) {
            /* Get the chunk optimization option threshold */
            if (H5CX_get_mpio_chunk_opt_num(&one_link_chunk_io_threshold) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                            "couldn't get chunk optimization option threshold value");

            /* If the threshold is 0, no need to check number of chunks */
            if (one_link_chunk_io_threshold > 0) {
                /* Get number of chunks for all processes */
                if (H5D__mpio_get_sum_chunk(io_info, &sum_chunk) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSWAP, FAIL,
                                "unable to obtain the total chunk number of all processes");

                /* If the average number of chunk per process is less than the threshold, we will do multi
                 * chunk IO.  If this threshold is not exceeded for all datasets, no need to check it again
                 * for each individual dataset. */
                if ((unsigned)sum_chunk / (unsigned)mpi_size < one_link_chunk_io_threshold) {
                    recalc_io_option = false;
                    use_multi_dset   = false;
                }
            }
        }
    }

    /* Perform multi dataset I/O if appropriate */
    if (use_multi_dset) {
#ifdef H5_HAVE_INSTRUMENTED_LIBRARY
        /*** Set collective chunk user-input optimization API. ***/
        if (H5D_ONE_LINK_CHUNK_IO == io_option) {
            if (H5CX_test_set_mpio_coll_chunk_link_hard(0) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set property value");
        } /* end if */
#endif    /* H5_HAVE_INSTRUMENTED_LIBRARY */

        /* Process all the filtered datasets first */
        if (io_info->filtered_count > 0) {
            if (H5D__link_chunk_filtered_collective_io(io_info, io_info->dsets_info, io_info->count, mpi_rank,
                                                       mpi_size) < 0)
                HGOTO_ERROR(H5E_IO, (H5D_IO_OP_READ == io_info->op_type ? H5E_READERROR : H5E_WRITEERROR),
                            FAIL, "couldn't finish filtered linked chunk MPI-IO");
        }

        /* Process all the unfiltered datasets */
        if ((io_info->filtered_count == 0) || (io_info->filtered_count < io_info->count)) {
            /* Perform unfiltered link chunk collective IO */
            if (H5D__link_piece_collective_io(io_info, mpi_rank) < 0)
                HGOTO_ERROR(H5E_IO, (H5D_IO_OP_READ == io_info->op_type ? H5E_READERROR : H5E_WRITEERROR),
                            FAIL, "couldn't finish linked chunk MPI-IO");
        }
    }
    else {
        /* Loop over datasets */
        for (i = 0; i < io_info->count; i++) {
            if (io_info->dsets_info[i].skip_io)
                continue;

            if (io_info->dsets_info[i].layout->type == H5D_CONTIGUOUS) {
                /* Contiguous: call H5D__inter_collective_io() directly */
                H5D_mpio_actual_io_mode_t actual_io_mode = H5D_MPIO_CONTIGUOUS_COLLECTIVE;

                io_info->store_faddr = io_info->dsets_info[i].store->contig.dset_addr;
                io_info->base_maddr  = io_info->dsets_info[i].buf;

                if (H5D__inter_collective_io(io_info, &io_info->dsets_info[i],
                                             io_info->dsets_info[i].file_space,
                                             io_info->dsets_info[i].mem_space) < 0)
                    HGOTO_ERROR(H5E_IO, (H5D_IO_OP_READ == io_info->op_type ? H5E_READERROR : H5E_WRITEERROR),
                                FAIL, "couldn't finish shared collective MPI-IO");

                /* Set the actual I/O mode property. internal_collective_io will not break to
                 * independent I/O, so we set it here.
                 */
                H5CX_set_mpio_actual_io_mode(actual_io_mode);
            }
            else {
                /* Chunked I/O path */
                assert(io_info->dsets_info[i].layout->type == H5D_CHUNKED);

                /* Recalculate io_option if necessary */
                if (recalc_io_option) {
                    /* Get the chunk optimization option threshold */
                    if (H5CX_get_mpio_chunk_opt_num(&one_link_chunk_io_threshold) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                                    "couldn't get chunk optimization option threshold value");

                    /* If the threshold is 0, no need to check number of chunks */
                    if (one_link_chunk_io_threshold == 0) {
                        io_option        = H5D_ONE_LINK_CHUNK_IO_MORE_OPT;
                        recalc_io_option = false;
                    }
                    else {
                        /* Get number of chunks for all processes */
                        if (H5D__mpio_get_sum_chunk_dset(io_info, &io_info->dsets_info[i], &sum_chunk) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSWAP, FAIL,
                                        "unable to obtain the total chunk number of all processes");

                        /* step 1: choose an IO option */
                        /* If the average number of chunk per process is greater than a threshold, we will do
                         * one link chunked IO. */
                        if ((unsigned)sum_chunk / (unsigned)mpi_size >= one_link_chunk_io_threshold)
                            io_option = H5D_ONE_LINK_CHUNK_IO_MORE_OPT;
                        else
                            io_option = H5D_MULTI_CHUNK_IO_MORE_OPT;
                    }
                }

                /* step 2:  Go ahead to do IO.*/
                switch (io_option) {
                    case H5D_ONE_LINK_CHUNK_IO:
                    case H5D_ONE_LINK_CHUNK_IO_MORE_OPT:
                        /* Check if there are any filters in the pipeline */
                        if (io_info->dsets_info[i].dset->shared->dcpl_cache.pline.nused > 0) {
                            if (H5D__link_chunk_filtered_collective_io(io_info, &io_info->dsets_info[i], 1,
                                                                       mpi_rank, mpi_size) < 0)
                                HGOTO_ERROR(
                                    H5E_IO,
                                    (H5D_IO_OP_READ == io_info->op_type ? H5E_READERROR : H5E_WRITEERROR),
                                    FAIL, "couldn't finish filtered linked chunk MPI-IO");
                        } /* end if */
                        else {
                            /* If there is more than one dataset we cannot make the multi dataset call here,
                             * fall back to multi chunk */
                            if (io_info->count > 1) {
                                io_option        = H5D_MULTI_CHUNK_IO_MORE_OPT;
                                recalc_io_option = true;

                                if (H5D__multi_chunk_collective_io(io_info, &io_info->dsets_info[i], mpi_rank,
                                                                   mpi_size) < 0)
                                    HGOTO_ERROR(
                                        H5E_IO,
                                        (H5D_IO_OP_READ == io_info->op_type ? H5E_READERROR : H5E_WRITEERROR),
                                        FAIL, "couldn't finish optimized multiple chunk MPI-IO");
                            }
                            else {
                                /* Perform unfiltered link chunk collective IO */
                                if (H5D__link_piece_collective_io(io_info, mpi_rank) < 0)
                                    HGOTO_ERROR(
                                        H5E_IO,
                                        (H5D_IO_OP_READ == io_info->op_type ? H5E_READERROR : H5E_WRITEERROR),
                                        FAIL, "couldn't finish linked chunk MPI-IO");
                            }
                        }

                        break;

                    case H5D_MULTI_CHUNK_IO: /* direct request to do multi-chunk IO */
                    default:                 /* multiple chunk IO via threshold */
                        /* Check if there are any filters in the pipeline */
                        if (io_info->dsets_info[i].dset->shared->dcpl_cache.pline.nused > 0) {
                            if (H5D__multi_chunk_filtered_collective_io(io_info, &io_info->dsets_info[i], 1,
                                                                        mpi_rank, mpi_size) < 0)
                                HGOTO_ERROR(
                                    H5E_IO,
                                    (H5D_IO_OP_READ == io_info->op_type ? H5E_READERROR : H5E_WRITEERROR),
                                    FAIL, "couldn't finish optimized multiple filtered chunk MPI-IO");
                        } /* end if */
                        else {
                            /* Perform unfiltered multi chunk collective IO */
                            if (H5D__multi_chunk_collective_io(io_info, &io_info->dsets_info[i], mpi_rank,
                                                               mpi_size) < 0)
                                HGOTO_ERROR(
                                    H5E_IO,
                                    (H5D_IO_OP_READ == io_info->op_type ? H5E_READERROR : H5E_WRITEERROR),
                                    FAIL, "couldn't finish optimized multiple chunk MPI-IO");
                        }

                        break;
                } /* end switch */

#ifdef H5_HAVE_INSTRUMENTED_LIBRARY
                {
                    /*** Set collective chunk user-input optimization APIs. ***/
                    if (H5D_ONE_LINK_CHUNK_IO == io_option) {
                        if (H5CX_test_set_mpio_coll_chunk_link_hard(0) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set property value");
                    } /* end if */
                    else if (H5D_MULTI_CHUNK_IO == io_option) {
                        if (H5CX_test_set_mpio_coll_chunk_multi_hard(0) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set property value");
                    } /* end else-if */
                    else if (H5D_ONE_LINK_CHUNK_IO_MORE_OPT == io_option) {
                        if (H5CX_test_set_mpio_coll_chunk_link_num_true(0) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set property value");
                    } /* end if */
                    else if (H5D_MULTI_CHUNK_IO_MORE_OPT == io_option) {
                        if (H5CX_test_set_mpio_coll_chunk_link_num_false(0) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set property value");
                    } /* end if */
                }
#endif /* H5_HAVE_INSTRUMENTED_LIBRARY */
            }
        }
    }

done:
#ifdef H5Dmpio_DEBUG
    /* Close debugging log file */
    if (debug_log_file) {
        fprintf(debug_log_file, "##############\n\n");
        if (EOF == fclose(debug_log_file))
            HDONE_ERROR(H5E_IO, H5E_CLOSEERROR, FAIL, "couldn't close debugging log file");
        debug_stream = H5DEBUG(D);
    }
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__piece_io */

/*-------------------------------------------------------------------------
 * Function:    H5D__collective_read
 *
 * Purpose:     Read directly from pieces (chunks/contig) in file into
 *              application memory using collective I/O.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__collective_read(H5D_io_info_t *io_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Call generic selection operation */
    if (H5D__piece_io(io_info) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_READERROR, FAIL, "read error");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__collective_read() */

/*-------------------------------------------------------------------------
 * Function:    H5D__collective_write
 *
 * Purpose:     Write directly to pieces (chunks/contig) in file into
 *              application memory using collective I/O.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__collective_write(H5D_io_info_t *io_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Call generic selection operation */
    if (H5D__piece_io(io_info) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_WRITEERROR, FAIL, "write error");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__collective_write() */

/*-------------------------------------------------------------------------
 * Function:    H5D__link_piece_collective_io
 *
 * Purpose:     Routine for single collective IO with one MPI derived datatype
 *              to link with all pieces (chunks + contig)
 *
 *              1. Use the piece addresses and piece info sorted in skiplist
 *              2. Build up MPI derived datatype for each chunk
 *              3. Build up the final MPI derived datatype
 *              4. Use common collective IO routine to do MPI-IO
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
#ifdef H5Dmpio_DEBUG
H5D__link_piece_collective_io(H5D_io_info_t *io_info, int mpi_rank)
#else
H5D__link_piece_collective_io(H5D_io_info_t *io_info, int H5_ATTR_UNUSED mpi_rank)
#endif
{
    MPI_Datatype  chunk_final_mtype; /* Final memory MPI datatype for all chunks with selection */
    bool          chunk_final_mtype_is_derived = false;
    MPI_Datatype  chunk_final_ftype; /* Final file MPI datatype for all chunks with selection */
    bool          chunk_final_ftype_is_derived = false;
    H5D_storage_t ctg_store; /* Storage info for "fake" contiguous dataset */
    MPI_Datatype *chunk_mtype           = NULL;
    MPI_Datatype *chunk_ftype           = NULL;
    MPI_Aint     *chunk_file_disp_array = NULL;
    MPI_Aint     *chunk_mem_disp_array  = NULL;
    bool *chunk_mft_is_derived_array = NULL; /* Flags to indicate each chunk's MPI file datatype is derived */
    bool *chunk_mbt_is_derived_array =
        NULL;                          /* Flags to indicate each chunk's MPI memory datatype is derived */
    int *chunk_mpi_file_counts = NULL; /* Count of MPI file datatype for each chunk */
    int *chunk_mpi_mem_counts  = NULL; /* Count of MPI memory datatype for each chunk */
    int  mpi_code;                     /* MPI return code */
    H5D_mpio_actual_chunk_opt_mode_t actual_chunk_opt_mode = H5D_MPIO_LINK_CHUNK;
    H5D_mpio_actual_io_mode_t        actual_io_mode        = 0;
    herr_t                           ret_value             = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* set actual_io_mode */
    for (size_t i = 0; i < io_info->count; i++) {
        /* Skip this dataset if no I/O is being performed */
        if (io_info->dsets_info[i].skip_io)
            continue;

        /* Filtered datasets are processed elsewhere. A contiguous dataset
         * could possibly have filters in the DCPL pipeline, but the library
         * will currently ignore optional filters in that case.
         */
        if ((io_info->dsets_info[i].dset->shared->dcpl_cache.pline.nused > 0) &&
            (io_info->dsets_info[i].layout->type != H5D_CONTIGUOUS))
            continue;

        if (io_info->dsets_info[i].layout->type == H5D_CHUNKED)
            actual_io_mode |= H5D_MPIO_CHUNK_COLLECTIVE;
        else if (io_info->dsets_info[i].layout->type == H5D_CONTIGUOUS)
            actual_io_mode |= H5D_MPIO_CONTIGUOUS_COLLECTIVE;
        else
            HGOTO_ERROR(H5E_IO, H5E_UNSUPPORTED, FAIL, "unsupported storage layout");
    }

    /* Set the actual-chunk-opt-mode property. */
    H5CX_set_mpio_actual_chunk_opt(actual_chunk_opt_mode);

    /* Set the actual-io-mode property.
     * Link chunk I/O does not break to independent, so can set right away */
    H5CX_set_mpio_actual_io_mode(actual_io_mode);

    /* Code block for actual actions (Build a MPI Type, IO) */
    {
        hsize_t mpi_buf_count; /* Number of MPI types */
        size_t  num_chunk;     /* Number of chunks for this process */

        H5D_piece_info_t *piece_info;

        /* local variable for base address for buffer */
        H5_flexible_const_ptr_t base_buf_addr;
        base_buf_addr.cvp = NULL;

        /* Get the number of unfiltered chunks with a selection */
        assert(io_info->filtered_pieces_added <= io_info->pieces_added);
        num_chunk = io_info->pieces_added - io_info->filtered_pieces_added;
        H5_CHECK_OVERFLOW(num_chunk, size_t, int);

#ifdef H5Dmpio_DEBUG
        H5D_MPIO_DEBUG_VA(mpi_rank, "num_chunk = %zu\n", num_chunk);
#endif

        /* Set up MPI datatype for chunks selected */
        if (num_chunk) {
            bool need_sort = false;

            /* Check if sel_pieces array is sorted */
            assert(io_info->sel_pieces[0]->faddr != HADDR_UNDEF);
            for (size_t i = 1; i < io_info->pieces_added; i++) {
                assert(io_info->sel_pieces[i]->faddr != HADDR_UNDEF);

                if (io_info->sel_pieces[i]->faddr < io_info->sel_pieces[i - 1]->faddr) {
                    need_sort = true;
                    break;
                }
            }

            /* Sort sel_pieces if necessary */
            if (need_sort)
                qsort(io_info->sel_pieces, io_info->pieces_added, sizeof(io_info->sel_pieces[0]),
                      H5D__cmp_piece_addr);

            /* Allocate chunking information */
            if (NULL == (chunk_mtype = (MPI_Datatype *)H5MM_malloc(num_chunk * sizeof(MPI_Datatype))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                            "couldn't allocate chunk memory datatype buffer");
            if (NULL == (chunk_ftype = (MPI_Datatype *)H5MM_malloc(num_chunk * sizeof(MPI_Datatype))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate chunk file datatype buffer");
            if (NULL == (chunk_file_disp_array = (MPI_Aint *)H5MM_malloc(num_chunk * sizeof(MPI_Aint))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                            "couldn't allocate chunk file displacement buffer");
            if (NULL == (chunk_mem_disp_array = (MPI_Aint *)H5MM_calloc(num_chunk * sizeof(MPI_Aint))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                            "couldn't allocate chunk memory displacement buffer");
            if (NULL == (chunk_mpi_mem_counts = (int *)H5MM_calloc(num_chunk * sizeof(int))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate chunk memory counts buffer");
            if (NULL == (chunk_mpi_file_counts = (int *)H5MM_calloc(num_chunk * sizeof(int))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate chunk file counts buffer");
            if (NULL == (chunk_mbt_is_derived_array = (bool *)H5MM_calloc(num_chunk * sizeof(bool))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                            "couldn't allocate chunk memory is derived datatype flags buffer");
            if (NULL == (chunk_mft_is_derived_array = (bool *)H5MM_calloc(num_chunk * sizeof(bool))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                            "couldn't allocate chunk file is derived datatype flags buffer");

            /*
             * After sorting sel_pieces according to file address, locate
             * the first unfiltered chunk and save its file address and
             * base memory address for read/write
             */
            ctg_store.contig.dset_addr = HADDR_UNDEF;
            for (size_t i = 0; i < io_info->pieces_added; i++) {
                if (!io_info->sel_pieces[i]->filtered_dset) {
                    ctg_store.contig.dset_addr = io_info->sel_pieces[i]->faddr;
                    base_buf_addr              = io_info->sel_pieces[i]->dset_info->buf;
                    break;
                }
            }
            assert(ctg_store.contig.dset_addr != HADDR_UNDEF);

#ifdef H5Dmpio_DEBUG
            H5D_MPIO_DEBUG(mpi_rank, "before iterate over selected pieces\n");
#endif

            /* Obtain MPI derived datatype from all individual pieces */
            /* Iterate over selected pieces for this process */
            for (size_t i = 0, curr_idx = 0; i < io_info->pieces_added; i++) {
                hsize_t *permute_map = NULL; /* array that holds the mapping from the old,
                                                out-of-order displacements to the in-order
                                                displacements of the MPI datatypes of the
                                                point selection of the file space */
                bool is_permuted = false;

                /* Assign convenience pointer to piece info */
                piece_info = io_info->sel_pieces[i];

                /* Skip over filtered pieces as they are processed elsewhere */
                if (piece_info->filtered_dset)
                    continue;

                /* Obtain disk and memory MPI derived datatype */
                /* NOTE: The permute_map array can be allocated within H5S_mpio_space_type
                 *              and will be fed into the next call to H5S_mpio_space_type
                 *              where it will be freed.
                 */
                if (H5S_mpio_space_type(piece_info->fspace, piece_info->dset_info->type_info.src_type_size,
                                        &chunk_ftype[curr_idx],                  /* OUT: datatype created */
                                        &chunk_mpi_file_counts[curr_idx],        /* OUT */
                                        &(chunk_mft_is_derived_array[curr_idx]), /* OUT */
                                        true,                                    /* this is a file space,
                                                                                    so permute the
                                                                                    datatype if the point
                                                                                    selections are out of
                                                                                    order */
                                        &permute_map,                            /* OUT: a map to indicate the
                                                                                    permutation of points
                                                                                    selected in case they
                                                                                    are out of order */
                                        &is_permuted /* OUT */) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "couldn't create MPI file type");

                /* Sanity check */
                if (is_permuted)
                    assert(permute_map);
                if (H5S_mpio_space_type(piece_info->mspace, piece_info->dset_info->type_info.dst_type_size,
                                        &chunk_mtype[curr_idx], &chunk_mpi_mem_counts[curr_idx],
                                        &(chunk_mbt_is_derived_array[curr_idx]), false, /* this is a memory
                                                                                           space, so if the
                                                                                           file space is not
                                                                                           permuted, there is
                                                                                           no need to permute
                                                                                           the datatype if the
                                                                                           point selections
                                                                                           are out of order */
                                        &permute_map, /* IN: the permutation map
                                                         generated by the
                                                         file_space selection
                                                         and applied to the
                                                         memory selection */
                                        &is_permuted /* IN */) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "couldn't create MPI buf type");
                /* Sanity check */
                if (is_permuted)
                    assert(!permute_map);

                /* Piece address relative to the first piece addr
                 * Assign piece address to MPI displacement
                 * (assume MPI_Aint big enough to hold it) */
                chunk_file_disp_array[curr_idx] =
                    (MPI_Aint)piece_info->faddr - (MPI_Aint)ctg_store.contig.dset_addr;

                if (io_info->op_type == H5D_IO_OP_WRITE) {
                    chunk_mem_disp_array[curr_idx] =
                        (MPI_Aint)piece_info->dset_info->buf.cvp - (MPI_Aint)base_buf_addr.cvp;
                }
                else if (io_info->op_type == H5D_IO_OP_READ) {
                    chunk_mem_disp_array[curr_idx] =
                        (MPI_Aint)piece_info->dset_info->buf.vp - (MPI_Aint)base_buf_addr.vp;
                }

                curr_idx++;
            } /* end for */

            /* Create final MPI derived datatype for the file */
            if (MPI_SUCCESS !=
                (mpi_code = MPI_Type_create_struct((int)num_chunk, chunk_mpi_file_counts,
                                                   chunk_file_disp_array, chunk_ftype, &chunk_final_ftype)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct failed", mpi_code)

            if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(&chunk_final_ftype)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)
            chunk_final_ftype_is_derived = true;

            /* Create final MPI derived datatype for memory */
            if (MPI_SUCCESS !=
                (mpi_code = MPI_Type_create_struct((int)num_chunk, chunk_mpi_mem_counts, chunk_mem_disp_array,
                                                   chunk_mtype, &chunk_final_mtype)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct failed", mpi_code)
            if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(&chunk_final_mtype)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)
            chunk_final_mtype_is_derived = true;

            /* Free the file & memory MPI datatypes for each chunk */
            for (size_t i = 0; i < num_chunk; i++) {
                if (chunk_mbt_is_derived_array[i])
                    if (MPI_SUCCESS != (mpi_code = MPI_Type_free(chunk_mtype + i)))
                        HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)

                if (chunk_mft_is_derived_array[i])
                    if (MPI_SUCCESS != (mpi_code = MPI_Type_free(chunk_ftype + i)))
                        HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
            } /* end for */

            /* We have a single, complicated MPI datatype for both memory & file */
            mpi_buf_count = (hsize_t)1;
        }      /* end if */
        else { /* no selection at all for this process */
            ctg_store.contig.dset_addr = 0;

            /* just provide a valid mem address. no actual IO occur */
            base_buf_addr = io_info->dsets_info[0].buf;

            /* Set the MPI datatype */
            chunk_final_ftype = MPI_BYTE;
            chunk_final_mtype = MPI_BYTE;

            /* No chunks selected for this process */
            mpi_buf_count = (hsize_t)0;
        } /* end else */

#ifdef H5Dmpio_DEBUG
        H5D_MPIO_DEBUG(mpi_rank, "before coming to final collective I/O");
#endif
        /* Set up the base storage address for this piece */
        io_info->store_faddr = ctg_store.contig.dset_addr;
        io_info->base_maddr  = base_buf_addr;

        /* Perform final collective I/O operation */
        if (H5D__final_collective_io(io_info, mpi_buf_count, chunk_final_ftype, chunk_final_mtype) < 0)
            HGOTO_ERROR(H5E_IO, H5E_CANTGET, FAIL, "couldn't finish MPI-IO");
    }

done:
#ifdef H5Dmpio_DEBUG
    H5D_MPIO_DEBUG_VA(mpi_rank, "before freeing memory inside H5D_link_collective_io ret_value = %d",
                      ret_value);
#endif

    if (ret_value < 0)
        H5CX_set_mpio_actual_chunk_opt(H5D_MPIO_NO_CHUNK_OPTIMIZATION);

    /* Release resources */
    if (chunk_mtype)
        H5MM_xfree(chunk_mtype);
    if (chunk_ftype)
        H5MM_xfree(chunk_ftype);
    if (chunk_file_disp_array)
        H5MM_xfree(chunk_file_disp_array);
    if (chunk_mem_disp_array)
        H5MM_xfree(chunk_mem_disp_array);
    if (chunk_mpi_mem_counts)
        H5MM_xfree(chunk_mpi_mem_counts);
    if (chunk_mpi_file_counts)
        H5MM_xfree(chunk_mpi_file_counts);
    if (chunk_mbt_is_derived_array)
        H5MM_xfree(chunk_mbt_is_derived_array);
    if (chunk_mft_is_derived_array)
        H5MM_xfree(chunk_mft_is_derived_array);

    /* Free the MPI buf and file types, if they were derived */
    if (chunk_final_mtype_is_derived && MPI_SUCCESS != (mpi_code = MPI_Type_free(&chunk_final_mtype)))
        HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    if (chunk_final_ftype_is_derived && MPI_SUCCESS != (mpi_code = MPI_Type_free(&chunk_final_ftype)))
        HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__link_piece_collective_io */

/*-------------------------------------------------------------------------
 * Function:    H5D__link_chunk_filtered_collective_io
 *
 * Purpose:     Performs collective I/O on filtered chunks by creating a
 *              single MPI derived datatype to link with all filtered
 *              chunks. The general algorithm is as follows:
 *
 *              1. Construct a list of selected chunks in the collective
 *                 I/O operation
 *              2. If the operation is a read operation
 *                 A. Ensure that the list of chunks is sorted in
 *                    monotonically non-decreasing order of chunk offset
 *                    in the file
 *                 B. Participate in a collective read of chunks from
 *                    the file
 *                 C. Loop through each selected chunk, unfiltering it and
 *                    scattering the data to the application's read buffer
 *              3. If the operation is a write operation
 *                 A. Redistribute any chunks being written by more than 1
 *                    MPI rank, such that the chunk is only owned by 1 MPI
 *                    rank. The rank writing to the chunk which currently
 *                    has the least amount of chunks assigned to it becomes
 *                    the new owner (in the case of ties, the lowest MPI
 *                    rank becomes the new owner)
 *                 B. Participate in a collective read of chunks from the
 *                    file
 *                 C. Loop through each chunk selected in the operation
 *                    and for each chunk:
 *                    I. If we actually read the chunk from the file (if
 *                       a chunk is being fully overwritten, we skip
 *                       reading it), pass the chunk through the filter
 *                       pipeline in reverse order (unfilter the chunk)
 *                    II. Update the chunk data with the modifications from
 *                        the owning MPI rank
 *                    III. Receive any modification data from other
 *                         ranks and update the chunk data with those
 *                         modifications
 *                    IV. Filter the chunk
 *                 D. Contribute the modified chunks to an array gathered
 *                    by all ranks which contains information for
 *                    re-allocating space in the file for every chunk
 *                    modified. Then, each rank collectively re-allocates
 *                    each chunk from the gathered array with their new
 *                    sizes after the filter operation
 *                 E. Proceed with the collective write operation for all
 *                    the modified chunks
 *                 F. Contribute the modified chunks to an array gathered
 *                    by all ranks which contains information for
 *                    re-inserting every chunk modified into the chunk
 *                    index. Then, each rank collectively re-inserts each
 *                    chunk from the gathered array into the chunk index
 *
 *              TODO: Note that steps D. and F. here are both collective
 *                    operations that partially share data from the
 *                    H5D_filtered_collective_chunk_info_t structure. To
 *                    try to conserve on memory a bit, the distributed
 *                    arrays these operations create are discarded after
 *                    each operation is performed. If memory consumption
 *                    here proves to not be an issue, the necessary data
 *                    for both operations could be combined into a single
 *                    structure so that only one collective MPI operation
 *                    is needed to carry out both operations, rather than
 *                    two.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__link_chunk_filtered_collective_io(H5D_io_info_t *io_info, H5D_dset_io_info_t *dset_infos,
                                       size_t num_dset_infos, int mpi_rank, int mpi_size)
{
    H5D_filtered_collective_io_info_t chunk_list               = {0};
    unsigned char                   **chunk_msg_bufs           = NULL;
    size_t                           *rank_chunks_assigned_map = NULL;
    int                               chunk_msg_bufs_len       = 0;
    herr_t                            ret_value                = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(io_info);

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TRACE_ENTER(mpi_rank);
    H5D_MPIO_DEBUG_VA(mpi_rank, "Performing Linked-chunk I/O (%s) with MPI Comm size of %d",
                      io_info->op_type == H5D_IO_OP_WRITE ? "write" : "read", mpi_size);
    H5D_MPIO_TIME_START(mpi_rank, "Linked-chunk I/O");
#endif

    /* Set the actual-chunk-opt-mode property. */
    H5CX_set_mpio_actual_chunk_opt(H5D_MPIO_LINK_CHUNK);

    /* Set the actual-io-mode property.
     * Link chunk filtered I/O does not break to independent, so can set right away
     */
    H5CX_set_mpio_actual_io_mode(H5D_MPIO_CHUNK_COLLECTIVE);

    /* Build a list of selected chunks in the collective io operation */

    if (H5D__mpio_collective_filtered_chunk_io_setup(io_info, dset_infos, num_dset_infos, mpi_rank,
                                                     &chunk_list) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "couldn't construct filtered I/O info list");

    if (io_info->op_type == H5D_IO_OP_READ) { /* Filtered collective read */
        if (H5D__mpio_collective_filtered_chunk_read(&chunk_list, io_info, num_dset_infos, mpi_rank) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "couldn't read filtered chunks");
    }
    else { /* Filtered collective write */
        if (mpi_size > 1) {
            /* Redistribute shared chunks being written to */
            if (H5D__mpio_redistribute_shared_chunks(&chunk_list, io_info, mpi_rank, mpi_size,
                                                     &rank_chunks_assigned_map) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to redistribute shared chunks");

            /* Send any chunk modification messages for chunks this rank no longer owns */
            if (H5D__mpio_share_chunk_modification_data(&chunk_list, io_info, mpi_rank, mpi_size,
                                                        &chunk_msg_bufs, &chunk_msg_bufs_len) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL,
                            "unable to send chunk modification data between MPI ranks");

            /* Make sure the local chunk list was updated correctly */
            assert(chunk_list.num_chunk_infos == rank_chunks_assigned_map[mpi_rank]);
        }

        /* Proceed to update all the chunks this rank owns with its own
         * modification data and data from other ranks, before re-filtering
         * the chunks. As chunk reads are done collectively here, all ranks
         * must participate.
         */
        if (H5D__mpio_collective_filtered_chunk_update(&chunk_list, chunk_msg_bufs, chunk_msg_bufs_len,
                                                       io_info, num_dset_infos, mpi_rank) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "couldn't update modified chunks");

        /* Free up resources used by chunk hash table now that we're done updating chunks */
        HASH_CLEAR(hh, chunk_list.chunk_hash_table);

        /* All ranks now collectively re-allocate file space for all chunks */
        if (H5D__mpio_collective_filtered_chunk_reallocate(&chunk_list, rank_chunks_assigned_map, io_info,
                                                           num_dset_infos, mpi_rank, mpi_size) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL,
                        "couldn't collectively re-allocate file space for chunks");

        /* Perform vector I/O on chunks */
        if (H5D__mpio_collective_filtered_vec_io(&chunk_list, io_info->f_sh, io_info->op_type) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "couldn't perform vector I/O on filtered chunks");

        /* Free up resources in anticipation of following collective operation */
        for (size_t i = 0; i < chunk_list.num_chunk_infos; i++) {
            if (chunk_list.chunk_infos[i].buf) {
                H5MM_free(chunk_list.chunk_infos[i].buf);
                chunk_list.chunk_infos[i].buf = NULL;
            }
        }

        /* Participate in the collective re-insertion of all chunks modified
         * into the chunk index
         */
        if (H5D__mpio_collective_filtered_chunk_reinsert(&chunk_list, rank_chunks_assigned_map, io_info,
                                                         num_dset_infos, mpi_rank, mpi_size) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL,
                        "couldn't collectively re-insert modified chunks into chunk index");
    }

done:
    if (ret_value < 0)
        H5CX_set_mpio_actual_chunk_opt(H5D_MPIO_NO_CHUNK_OPTIMIZATION);

    if (chunk_msg_bufs) {
        for (size_t i = 0; i < (size_t)chunk_msg_bufs_len; i++)
            H5MM_free(chunk_msg_bufs[i]);

        H5MM_free(chunk_msg_bufs);
    }

    HASH_CLEAR(hh, chunk_list.chunk_hash_table);

    if (rank_chunks_assigned_map)
        H5MM_free(rank_chunks_assigned_map);

    /* Free resources used by a rank which had some selection */
    if (chunk_list.chunk_infos) {
        for (size_t i = 0; i < chunk_list.num_chunk_infos; i++)
            if (chunk_list.chunk_infos[i].buf)
                H5MM_free(chunk_list.chunk_infos[i].buf);

        H5MM_free(chunk_list.chunk_infos);
    } /* end if */

    /* Free resources used by cached dataset info */
    if ((num_dset_infos == 1) && (chunk_list.dset_info.single_dset_info)) {
        H5D_mpio_filtered_dset_info_t *curr_dset_info = chunk_list.dset_info.single_dset_info;

        if (curr_dset_info->fb_info_init && H5D__fill_term(&curr_dset_info->fb_info) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't release fill buffer info");
        if (curr_dset_info->fill_space && H5S_close(curr_dset_info->fill_space) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close fill space");

        H5MM_free(chunk_list.dset_info.single_dset_info);
        chunk_list.dset_info.single_dset_info = NULL;
    }
    else if ((num_dset_infos > 1) && (chunk_list.dset_info.dset_info_hash_table)) {
        H5D_mpio_filtered_dset_info_t *curr_dset_info;
        H5D_mpio_filtered_dset_info_t *tmp;

        HASH_ITER(hh, chunk_list.dset_info.dset_info_hash_table, curr_dset_info, tmp)
        {
            HASH_DELETE(hh, chunk_list.dset_info.dset_info_hash_table, curr_dset_info);

            if (curr_dset_info->fb_info_init && H5D__fill_term(&curr_dset_info->fb_info) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't release fill buffer info");
            if (curr_dset_info->fill_space && H5S_close(curr_dset_info->fill_space) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close fill space");

            H5MM_free(curr_dset_info);
            curr_dset_info = NULL;
        }
    }

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TIME_STOP(mpi_rank);
    H5D_MPIO_TRACE_EXIT(mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__link_chunk_filtered_collective_io() */

/*-------------------------------------------------------------------------
 * Function:    H5D__multi_chunk_collective_io
 *
 * Purpose:     To do IO per chunk according to IO mode(collective/independent/none)
 *
 *              1. Use MPI_gather and MPI_Bcast to obtain IO mode in each chunk(collective/independent/none)
 *              2. Depending on whether the IO mode is collective or independent or none,
 *                 Create either MPI derived datatype for each chunk or just do independent IO
 *              3. Use common collective IO routine to do MPI-IO
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__multi_chunk_collective_io(H5D_io_info_t *io_info, H5D_dset_io_info_t *dset_info, int mpi_rank,
                               int mpi_size)
{
    uint8_t                   *chunk_io_option = NULL;
    haddr_t                   *chunk_addr      = NULL;
    H5D_storage_t              store; /* union of EFL and chunk pointer in file space */
    H5FD_mpio_collective_opt_t last_coll_opt_mode =
        H5FD_MPIO_COLLECTIVE_IO; /* Last parallel transfer with independent IO or collective IO with this mode
                                  */
    H5FD_mpio_collective_opt_t orig_coll_opt_mode =
        H5FD_MPIO_COLLECTIVE_IO;           /* Original parallel transfer property on entering this function */
    size_t                    total_chunk; /* Total # of chunks in dataset */
    size_t                    num_chunk;   /* Number of chunks for this process */
    H5SL_node_t              *piece_node      = NULL; /* Current node in chunk skip list */
    H5D_piece_info_t         *next_chunk_info = NULL; /* Chunk info for next selected chunk */
    size_t                    u;                      /* Local index variable */
    H5D_mpio_actual_io_mode_t actual_io_mode =
        H5D_MPIO_NO_COLLECTIVE; /* Local variable for tracking the I/O mode used. */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_TAG(dset_info->dset->oloc.addr)

    assert(dset_info->layout->type == H5D_CHUNKED);

    /* Get the current I/O collective opt mode so we can restore it later */
    if (H5CX_get_mpio_coll_opt(&orig_coll_opt_mode) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get MPI-I/O collective_op property");

    /* Set the actual chunk opt mode property */
    H5CX_set_mpio_actual_chunk_opt(H5D_MPIO_MULTI_CHUNK);

    /* Retrieve total # of chunks in dataset */
    H5_CHECKED_ASSIGN(total_chunk, size_t, dset_info->layout->u.chunk.nchunks, hsize_t);
    assert(total_chunk != 0);

    /* Allocate memories */
    chunk_io_option = (uint8_t *)H5MM_calloc(total_chunk);
    chunk_addr      = (haddr_t *)H5MM_calloc(total_chunk * sizeof(haddr_t));

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_DEBUG_VA(mpi_rank, "total_chunk %zu", total_chunk);
#endif

    /* Obtain IO option for each chunk */
    if (H5D__obtain_mpio_mode(io_info, dset_info, chunk_io_option, chunk_addr, mpi_rank, mpi_size) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTRECV, FAIL, "unable to obtain MPIO mode");

    /* Set memory buffers */
    io_info->base_maddr = dset_info->buf;

    /* Set dataset storage for I/O info */
    dset_info->store = &store;

    /* Get the number of chunks with a selection */
    num_chunk = H5SL_count(dset_info->layout_io_info.chunk_map->dset_sel_pieces);

    if (num_chunk) {
        /* Start at the beginning of the chunk map skiplist.  Since these chunks are
         * stored in index order and since we're iterating in index order we can
         * just check for each chunk being selected in order */
        if (NULL == (piece_node = H5SL_first(dset_info->layout_io_info.chunk_map->dset_sel_pieces)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "couldn't get piece node from skip list");
        if (NULL == (next_chunk_info = (H5D_piece_info_t *)H5SL_item(piece_node)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "couldn't get piece info from skip list");
    }

    /* Loop over _all_ the chunks */
    for (u = 0; u < total_chunk; u++) {
        H5D_piece_info_t *chunk_info; /* Chunk info for current chunk */
        H5S_t            *fspace;     /* Dataspace describing chunk & selection in it */
        H5S_t            *mspace; /* Dataspace describing selection in memory corresponding to this chunk */

#ifdef H5Dmpio_DEBUG
        H5D_MPIO_DEBUG_VA(mpi_rank, "mpi_rank = %d, chunk index = %zu", mpi_rank, u);
#endif

        /* Check if this chunk is the next chunk in the skip list, if there are
         * selected chunks left to process */
        assert(!num_chunk || next_chunk_info);
        assert(!num_chunk || next_chunk_info->index >= u);
        if (num_chunk && next_chunk_info->index == u) {
            /* Next chunk is this chunk */
            chunk_info = next_chunk_info;

            /* One less chunk to process */
            num_chunk--;

            /* Advance next chunk to next node in skip list, if there are more chunks selected */
            if (num_chunk) {
                if (NULL == (piece_node = H5SL_next(piece_node)))
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "chunk skip list terminated early");
                if (NULL == (next_chunk_info = (H5D_piece_info_t *)H5SL_item(piece_node)))
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "couldn't get piece info from skip list");
            }

            /* Pass in chunk's coordinates in a union. */
            store.chunk.scaled = chunk_info->scaled;
        }
        else
            chunk_info = NULL;

        /* Collective IO for this chunk,
         * Note: even there is no selection for this process, the process still
         *      needs to contribute MPI NONE TYPE.
         */
        if (chunk_io_option[u] == H5D_CHUNK_IO_MODE_COL) {
#ifdef H5Dmpio_DEBUG
            H5D_MPIO_DEBUG_VA(mpi_rank, "inside collective chunk IO mpi_rank = %d, chunk index = %zu",
                              mpi_rank, u);
#endif

            /* Set the file & memory dataspaces */
            if (chunk_info) {
                fspace = chunk_info->fspace;
                mspace = chunk_info->mspace;

                /* Update the local variable tracking the actual io mode property.
                 *
                 * Note: H5D_MPIO_COLLECTIVE_MULTI | H5D_MPIO_INDEPENDENT = H5D_MPIO_MIXED
                 *      to ease switching between to mixed I/O without checking the current
                 *      value of the property. You can see the definition in H5Ppublic.h
                 */
                actual_io_mode = (H5D_mpio_actual_io_mode_t)(actual_io_mode | H5D_MPIO_CHUNK_COLLECTIVE);

            } /* end if */
            else {
                fspace = mspace = NULL;
            } /* end else */

            /* Switch back to collective I/O */
            if (last_coll_opt_mode != H5FD_MPIO_COLLECTIVE_IO) {
                if (H5CX_set_mpio_coll_opt(H5FD_MPIO_COLLECTIVE_IO) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't switch to collective I/O");
                last_coll_opt_mode = H5FD_MPIO_COLLECTIVE_IO;
            } /* end if */

            /* Initialize temporary contiguous storage address */
            io_info->store_faddr = chunk_addr[u];

            /* Perform the I/O */
            if (H5D__inter_collective_io(io_info, dset_info, fspace, mspace) < 0)
                HGOTO_ERROR(H5E_IO, H5E_CANTGET, FAIL, "couldn't finish shared collective MPI-IO");
        }      /* end if */
        else { /* possible independent IO for this chunk */
#ifdef H5Dmpio_DEBUG
            H5D_MPIO_DEBUG_VA(mpi_rank, "inside independent IO mpi_rank = %d, chunk index = %zu", mpi_rank,
                              u);
#endif

            assert(chunk_io_option[u] == 0);

            /* Set the file & memory dataspaces */
            if (chunk_info) {
                fspace = chunk_info->fspace;
                mspace = chunk_info->mspace;

                /* Update the local variable tracking the actual io mode. */
                actual_io_mode = (H5D_mpio_actual_io_mode_t)(actual_io_mode | H5D_MPIO_CHUNK_INDEPENDENT);
            } /* end if */
            else {
                fspace = mspace = NULL;
            } /* end else */

            /* Using independent I/O with file setview.*/
            if (last_coll_opt_mode != H5FD_MPIO_INDIVIDUAL_IO) {
                if (H5CX_set_mpio_coll_opt(H5FD_MPIO_INDIVIDUAL_IO) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't switch to individual I/O");
                last_coll_opt_mode = H5FD_MPIO_INDIVIDUAL_IO;
            } /* end if */

            /* Initialize temporary contiguous storage address */
            io_info->store_faddr = chunk_addr[u];

            /* Perform the I/O */
            if (H5D__inter_collective_io(io_info, dset_info, fspace, mspace) < 0)
                HGOTO_ERROR(H5E_IO, H5E_CANTGET, FAIL, "couldn't finish shared collective MPI-IO");
#ifdef H5Dmpio_DEBUG
            H5D_MPIO_DEBUG(mpi_rank, "after inter collective IO");
#endif
        } /* end else */
    }     /* end for */

    /* Write the local value of actual io mode to the API context. */
    H5CX_set_mpio_actual_io_mode(actual_io_mode);

done:
    if (ret_value < 0)
        H5CX_set_mpio_actual_chunk_opt(H5D_MPIO_NO_CHUNK_OPTIMIZATION);

    /* Reset collective opt mode */
    if (H5CX_set_mpio_coll_opt(orig_coll_opt_mode) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't reset MPI-I/O collective_op property");

    /* Free memory */
    if (chunk_io_option)
        H5MM_xfree(chunk_io_option);
    if (chunk_addr)
        H5MM_xfree(chunk_addr);

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__multi_chunk_collective_io */

/*-------------------------------------------------------------------------
 * Function:    H5D__multi_chunk_filtered_collective_io
 *
 * Purpose:     Performs collective I/O on filtered chunks iteratively to
 *              save on memory and potentially get better performance
 *              depending on the average number of chunks per rank. While
 *              linked-chunk I/O will construct and work on a list of all
 *              of the chunks selected in the I/O operation at once, this
 *              function works iteratively on a set of chunks at a time; at
 *              most one chunk per rank per iteration.  The general
 *              algorithm is as follows:
 *
 *              1. Construct a list of selected chunks in the collective
 *                 I/O operation
 *              2. If the operation is a read operation, loop an amount of
 *                 times equal to the maximum number of chunks selected on
 *                 any particular rank and on each iteration:
 *                 A. Participate in a collective read of chunks from
 *                    the file (ranks that run out of chunks still need
 *                    to participate)
 *                 B. Unfilter the chunk that was read (if any)
 *                 C. Scatter the read chunk's data to the application's
 *                    read buffer
 *              3. If the operation is a write operation, redistribute any
 *                 chunks being written to by more than 1 MPI rank, such
 *                 that the chunk is only owned by 1 MPI rank. The rank
 *                 writing to the chunk which currently has the least
 *                 amount of chunks assigned to it becomes the new owner
 *                 (in the case of ties, the lowest MPI rank becomes the
 *                 new owner). Then, loop an amount of times equal to the
 *                 maximum number of chunks selected on any particular
 *                 rank and on each iteration:
 *                 A. Participate in a collective read of chunks from
 *                    the file (ranks that run out of chunks still need
 *                    to participate)
 *                    I. If we actually read a chunk from the file (if
 *                       a chunk is being fully overwritten, we skip
 *                       reading it), pass the chunk through the filter
 *                       pipeline in reverse order (unfilter the chunk)
 *                 B. Update the chunk data with the modifications from
 *                    the owning rank
 *                 C. Receive any modification data from other ranks and
 *                    update the chunk data with those modifications
 *                 D. Filter the chunk
 *                 E. Contribute the chunk to an array gathered by
 *                    all ranks which contains information for
 *                    re-allocating space in the file for every chunk
 *                    modified in this iteration (up to one chunk per
 *                    rank; some ranks may not have a selection/may have
 *                    less chunks to work on than other ranks). Then,
 *                    each rank collectively re-allocates each chunk
 *                    from the gathered array with their new sizes
 *                    after the filter operation
 *                 F. Proceed with the collective write operation
 *                    for the chunks modified on this iteration
 *                 G. Contribute the chunk to an array gathered by
 *                    all ranks which contains information for
 *                    re-inserting every chunk modified on this
 *                    iteration into the chunk index. Then, each rank
 *                    collectively re-inserts each chunk from the
 *                    gathered array into the chunk index
 *
 *              TODO: Note that steps E. and G. here are both collective
 *                    operations that partially share data from the
 *                    H5D_filtered_collective_chunk_info_t structure. To
 *                    try to conserve on memory a bit, the distributed
 *                    arrays these operations create are discarded after
 *                    each operation is performed. If memory consumption
 *                    here proves to not be an issue, the necessary data
 *                    for both operations could be combined into a single
 *                    structure so that only one collective MPI operation
 *                    is needed to carry out both operations, rather than
 *                    two.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__multi_chunk_filtered_collective_io(H5D_io_info_t *io_info, H5D_dset_io_info_t *dset_infos,
                                        size_t num_dset_infos, int mpi_rank, int mpi_size)
{
    H5D_filtered_collective_io_info_t chunk_list     = {0};
    unsigned char                   **chunk_msg_bufs = NULL;
    bool                              have_chunk_to_process;
    size_t                            max_num_chunks;
    int                               chunk_msg_bufs_len = 0;
    int                               mpi_code;
    herr_t                            ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_TAG(dset_infos->dset->oloc.addr)

    assert(io_info);
    assert(num_dset_infos == 1); /* Currently only supported with 1 dataset at a time */

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TRACE_ENTER(mpi_rank);
    H5D_MPIO_DEBUG_VA(mpi_rank, "Performing Multi-chunk I/O (%s) with MPI Comm size of %d",
                      io_info->op_type == H5D_IO_OP_WRITE ? "write" : "read", mpi_size);
    H5D_MPIO_TIME_START(mpi_rank, "Multi-chunk I/O");
#endif

    /* Set the actual chunk opt mode property */
    H5CX_set_mpio_actual_chunk_opt(H5D_MPIO_MULTI_CHUNK);

    /* Set the actual_io_mode property.
     * Multi chunk I/O does not break to independent, so can set right away
     */
    H5CX_set_mpio_actual_io_mode(H5D_MPIO_CHUNK_COLLECTIVE);

    /* Build a list of selected chunks in the collective IO operation */
    if (H5D__mpio_collective_filtered_chunk_io_setup(io_info, dset_infos, 1, mpi_rank, &chunk_list) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "couldn't construct filtered I/O info list");

    /* Retrieve the maximum number of chunks selected for any rank */
    if (MPI_SUCCESS != (mpi_code = MPI_Allreduce(&chunk_list.num_chunk_infos, &max_num_chunks, 1,
                                                 MPI_UNSIGNED_LONG_LONG, MPI_MAX, io_info->comm)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Allreduce failed", mpi_code)

    /* If no one has anything selected at all, end the operation */
    if (0 == max_num_chunks)
        HGOTO_DONE(SUCCEED);

    if (io_info->op_type == H5D_IO_OP_READ) { /* Filtered collective read */
        for (size_t i = 0; i < max_num_chunks; i++) {
            H5D_filtered_collective_io_info_t single_chunk_list = chunk_list;

            /* Check if this rank has a chunk to work on for this iteration */
            have_chunk_to_process = (i < chunk_list.num_chunk_infos);

            /*
             * Setup a chunk list structure for either 1 or 0 chunks, depending
             * on whether this rank has a chunk to work on for this iteration
             */
            if (have_chunk_to_process) {
                single_chunk_list.chunk_infos        = &chunk_list.chunk_infos[i];
                single_chunk_list.num_chunk_infos    = 1;
                single_chunk_list.num_chunks_to_read = chunk_list.chunk_infos[i].need_read ? 1 : 0;
            }
            else {
                single_chunk_list.chunk_infos        = NULL;
                single_chunk_list.num_chunk_infos    = 0;
                single_chunk_list.num_chunks_to_read = 0;
            }

            if (H5D__mpio_collective_filtered_chunk_read(&single_chunk_list, io_info, 1, mpi_rank) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "couldn't read filtered chunks");

            if (have_chunk_to_process && chunk_list.chunk_infos[i].buf) {
                H5MM_free(chunk_list.chunk_infos[i].buf);
                chunk_list.chunk_infos[i].buf = NULL;
            }
        }
    }
    else { /* Filtered collective write */
        if (mpi_size > 1) {
            /* Redistribute shared chunks being written to */
            if (H5D__mpio_redistribute_shared_chunks(&chunk_list, io_info, mpi_rank, mpi_size, NULL) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to redistribute shared chunks");

            /* Send any chunk modification messages for chunks this rank no longer owns */
            if (H5D__mpio_share_chunk_modification_data(&chunk_list, io_info, mpi_rank, mpi_size,
                                                        &chunk_msg_bufs, &chunk_msg_bufs_len) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL,
                            "unable to send chunk modification data between MPI ranks");
        }

        /* Iterate over the max number of chunks among all ranks, as this rank could
         * have no chunks left to work on, but it still needs to participate in the
         * collective re-allocation and re-insertion of chunks modified by other ranks.
         */
        for (size_t i = 0; i < max_num_chunks; i++) {
            H5D_filtered_collective_io_info_t single_chunk_list = chunk_list;

            /* Check if this rank has a chunk to work on for this iteration */
            have_chunk_to_process =
                (i < chunk_list.num_chunk_infos) && (mpi_rank == chunk_list.chunk_infos[i].new_owner);

            /*
             * Setup a chunk list structure for either 1 or 0 chunks, depending
             * on whether this rank has a chunk to work on for this iteration
             */
            if (have_chunk_to_process) {
                single_chunk_list.chunk_infos        = &chunk_list.chunk_infos[i];
                single_chunk_list.num_chunk_infos    = 1;
                single_chunk_list.num_chunks_to_read = chunk_list.chunk_infos[i].need_read ? 1 : 0;
            }
            else {
                single_chunk_list.chunk_infos        = NULL;
                single_chunk_list.num_chunk_infos    = 0;
                single_chunk_list.num_chunks_to_read = 0;
            }

            /* Proceed to update the chunk this rank owns (if any left) with its
             * own modification data and data from other ranks, before re-filtering
             * the chunks. As chunk reads are done collectively here, all ranks
             * must participate.
             */
            if (H5D__mpio_collective_filtered_chunk_update(&single_chunk_list, chunk_msg_bufs,
                                                           chunk_msg_bufs_len, io_info, 1, mpi_rank) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "couldn't update modified chunks");

            /* All ranks now collectively re-allocate file space for all chunks */
            if (H5D__mpio_collective_filtered_chunk_reallocate(&single_chunk_list, NULL, io_info, 1, mpi_rank,
                                                               mpi_size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL,
                            "couldn't collectively re-allocate file space for chunks");

            /* Perform vector I/O on chunks */
            if (H5D__mpio_collective_filtered_vec_io(&single_chunk_list, io_info->f_sh, io_info->op_type) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL,
                            "couldn't perform vector I/O on filtered chunks");

            /* Free up resources in anticipation of following collective operation */
            if (have_chunk_to_process && chunk_list.chunk_infos[i].buf) {
                H5MM_free(chunk_list.chunk_infos[i].buf);
                chunk_list.chunk_infos[i].buf = NULL;
            }

            /* Participate in the collective re-insertion of all chunks modified
             * in this iteration into the chunk index
             */
            if (H5D__mpio_collective_filtered_chunk_reinsert(&single_chunk_list, NULL, io_info, 1, mpi_rank,
                                                             mpi_size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL,
                            "couldn't collectively re-insert modified chunks into chunk index");
        } /* end for */
    }

done:
    if (ret_value < 0)
        H5CX_set_mpio_actual_chunk_opt(H5D_MPIO_NO_CHUNK_OPTIMIZATION);

    if (chunk_msg_bufs) {
        for (size_t i = 0; i < (size_t)chunk_msg_bufs_len; i++)
            H5MM_free(chunk_msg_bufs[i]);

        H5MM_free(chunk_msg_bufs);
    }

    HASH_CLEAR(hh, chunk_list.chunk_hash_table);

    /* Free resources used by a rank which had some selection */
    if (chunk_list.chunk_infos) {
        for (size_t i = 0; i < chunk_list.num_chunk_infos; i++)
            if (chunk_list.chunk_infos[i].buf)
                H5MM_free(chunk_list.chunk_infos[i].buf);

        H5MM_free(chunk_list.chunk_infos);
    } /* end if */

    /* Free resources used by cached dataset info */
    if ((num_dset_infos == 1) && (chunk_list.dset_info.single_dset_info)) {
        H5D_mpio_filtered_dset_info_t *curr_dset_info = chunk_list.dset_info.single_dset_info;

        if (curr_dset_info->fb_info_init && H5D__fill_term(&curr_dset_info->fb_info) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't release fill buffer info");
        if (curr_dset_info->fill_space && H5S_close(curr_dset_info->fill_space) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close fill space");

        H5MM_free(chunk_list.dset_info.single_dset_info);
        chunk_list.dset_info.single_dset_info = NULL;
    }
    else if ((num_dset_infos > 1) && (chunk_list.dset_info.dset_info_hash_table)) {
        H5D_mpio_filtered_dset_info_t *curr_dset_info;
        H5D_mpio_filtered_dset_info_t *tmp;

        HASH_ITER(hh, chunk_list.dset_info.dset_info_hash_table, curr_dset_info, tmp)
        {
            HASH_DELETE(hh, chunk_list.dset_info.dset_info_hash_table, curr_dset_info);

            if (curr_dset_info->fb_info_init && H5D__fill_term(&curr_dset_info->fb_info) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't release fill buffer info");
            if (curr_dset_info->fill_space && H5S_close(curr_dset_info->fill_space) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close fill space");

            H5MM_free(curr_dset_info);
            curr_dset_info = NULL;
        }
    }

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TIME_STOP(mpi_rank);
    H5D_MPIO_TRACE_EXIT(mpi_rank);
#endif

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__multi_chunk_filtered_collective_io() */

/*-------------------------------------------------------------------------
 * Function:    H5D__inter_collective_io
 *
 * Purpose:     Routine for the shared part of collective IO between multiple chunk
 *              collective IO and contiguous collective IO
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__inter_collective_io(H5D_io_info_t *io_info, const H5D_dset_io_info_t *di, H5S_t *file_space,
                         H5S_t *mem_space)
{
    int          mpi_buf_count; /* # of MPI types */
    bool         mbt_is_derived = false;
    bool         mft_is_derived = false;
    MPI_Datatype mpi_file_type, mpi_buf_type;
    int          mpi_code; /* MPI return code */
#ifdef H5Dmpio_DEBUG
    int mpi_rank;
#endif
    herr_t ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

#ifdef H5Dmpio_DEBUG
    mpi_rank = H5F_mpi_get_rank(di->dset->oloc.file);
    H5D_MPIO_TRACE_ENTER(mpi_rank);
    H5D_MPIO_TIME_START(mpi_rank, "Inter collective I/O");
    if (mpi_rank < 0)
        HGOTO_ERROR(H5E_IO, H5E_MPI, FAIL, "unable to obtain MPI rank");
#endif

    assert(io_info);

    if ((file_space != NULL) && (mem_space != NULL)) {
        int      mpi_file_count;     /* Number of file "objects" to transfer */
        hsize_t *permute_map = NULL; /* array that holds the mapping from the old,
                                        out-of-order displacements to the in-order
                                        displacements of the MPI datatypes of the
                                        point selection of the file space */
        bool is_permuted = false;

        assert(di);

        /* Obtain disk and memory MPI derived datatype */
        /* NOTE: The permute_map array can be allocated within H5S_mpio_space_type
         *              and will be fed into the next call to H5S_mpio_space_type
         *              where it will be freed.
         */
        if (H5S_mpio_space_type(file_space, di->type_info.src_type_size, &mpi_file_type, &mpi_file_count,
                                &mft_is_derived, /* OUT: datatype created */
                                true,            /* this is a file space, so
                                                    permute the datatype if the
                                                    point selection is out of
                                                    order */
                                &permute_map,    /* OUT: a map to indicate
                                                    the permutation of
                                                    points selected in
                                                    case they are out of
                                                    order */
                                &is_permuted /* OUT */) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "couldn't create MPI file type");
        /* Sanity check */
        if (is_permuted)
            assert(permute_map);
        if (H5S_mpio_space_type(mem_space, di->type_info.src_type_size, &mpi_buf_type, &mpi_buf_count,
                                &mbt_is_derived, /* OUT: datatype created */
                                false,           /* this is a memory space, so if
                                                    the file space is not
                                                    permuted, there is no need to
                                                    permute the datatype if the
                                                    point selections are out of
                                                    order*/
                                &permute_map     /* IN: the permutation map
                                                    generated by the
                                                    file_space selection
                                                    and applied to the
                                                    memory selection */
                                ,
                                &is_permuted /* IN */) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "couldn't create MPI buffer type");
        /* Sanity check */
        if (is_permuted)
            assert(!permute_map);
    } /* end if */
    else {
        /* For non-selection, participate with a none MPI derived datatype, the count is 0.  */
        mpi_buf_type   = MPI_BYTE;
        mpi_file_type  = MPI_BYTE;
        mpi_buf_count  = 0;
        mbt_is_derived = false;
        mft_is_derived = false;
    } /* end else */

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_DEBUG(mpi_rank, "before final collective I/O");
#endif

    /* Perform final collective I/O operation */
    if (H5D__final_collective_io(io_info, (hsize_t)mpi_buf_count, mpi_file_type, mpi_buf_type) < 0)
        HGOTO_ERROR(H5E_IO, H5E_CANTGET, FAIL, "couldn't finish collective MPI-IO");

done:
    /* Free the MPI buf and file types, if they were derived */
    if (mbt_is_derived && MPI_SUCCESS != (mpi_code = MPI_Type_free(&mpi_buf_type)))
        HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    if (mft_is_derived && MPI_SUCCESS != (mpi_code = MPI_Type_free(&mpi_file_type)))
        HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TIME_STOP(mpi_rank);
    H5D_MPIO_DEBUG_VA(mpi_rank, "before leaving inter_collective_io ret_value = %d", ret_value);
    H5D_MPIO_TRACE_EXIT(mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__inter_collective_io() */

/*-------------------------------------------------------------------------
 * Function:    H5D__final_collective_io
 *
 * Purpose:     Routine for the common part of collective IO with different storages.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__final_collective_io(H5D_io_info_t *io_info, hsize_t mpi_buf_count, MPI_Datatype mpi_file_type,
                         MPI_Datatype mpi_buf_type)
{
#ifdef H5Dmpio_DEBUG
    int mpi_rank;
#endif
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#ifdef H5Dmpio_DEBUG
    mpi_rank = H5F_mpi_get_rank(io_info->dsets_info[0].dset->oloc.file);
    H5D_MPIO_TRACE_ENTER(mpi_rank);
    H5D_MPIO_TIME_START(mpi_rank, "Final collective I/O");
    if (mpi_rank < 0)
        HGOTO_ERROR(H5E_IO, H5E_MPI, FAIL, "unable to obtain MPI rank");
#endif

    /* Pass buf type, file type to the file driver.  */
    if (H5CX_set_mpi_coll_datatypes(mpi_buf_type, mpi_file_type) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set MPI-I/O collective I/O datatypes");

    if (io_info->op_type == H5D_IO_OP_WRITE) {
        if ((io_info->md_io_ops.single_write_md)(io_info, mpi_buf_count, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "optimized write failed");
    } /* end if */
    else {
        if ((io_info->md_io_ops.single_read_md)(io_info, mpi_buf_count, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "optimized read failed");
    } /* end else */

done:
#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TIME_STOP(mpi_rank);
    H5D_MPIO_DEBUG_VA(mpi_rank, "ret_value before leaving final_collective_io=%d", ret_value);
    H5D_MPIO_TRACE_EXIT(mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__final_collective_io */

/*-------------------------------------------------------------------------
 * Function:    H5D__cmp_piece_addr
 *
 * Purpose:     Routine to compare piece addresses
 *
 * Description: Callback for qsort() to compare piece addresses
 *
 * Return:      -1, 0, 1
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__cmp_piece_addr(const void *piece_info1, const void *piece_info2)
{
    haddr_t addr1;
    haddr_t addr2;

    FUNC_ENTER_PACKAGE_NOERR

    addr1 = (*((const H5D_piece_info_t *const *)piece_info1))->faddr;
    addr2 = (*((const H5D_piece_info_t *const *)piece_info2))->faddr;

    FUNC_LEAVE_NOAPI(H5_addr_cmp(addr1, addr2))
} /* end H5D__cmp_chunk_addr() */

/*-------------------------------------------------------------------------
 * Function:    H5D__cmp_filtered_collective_io_info_entry
 *
 * Purpose:     Routine to compare filtered collective chunk io info
 *              entries
 *
 * Description: Callback for qsort() to compare filtered collective chunk
 *              io info entries
 *
 * Return:      -1, 0, 1
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__cmp_filtered_collective_io_info_entry(const void *filtered_collective_io_info_entry1,
                                           const void *filtered_collective_io_info_entry2)
{
    const H5D_filtered_collective_chunk_info_t *entry1;
    const H5D_filtered_collective_chunk_info_t *entry2;
    haddr_t                                     addr1 = HADDR_UNDEF;
    haddr_t                                     addr2 = HADDR_UNDEF;
    int                                         ret_value;

    FUNC_ENTER_PACKAGE_NOERR

    entry1 = (const H5D_filtered_collective_chunk_info_t *)filtered_collective_io_info_entry1;
    entry2 = (const H5D_filtered_collective_chunk_info_t *)filtered_collective_io_info_entry2;

    addr1 = entry1->chunk_new.offset;
    addr2 = entry2->chunk_new.offset;

    /*
     * If both chunk's file addresses are defined, H5_addr_cmp is safe to use.
     * If only one chunk's file address is defined, return the appropriate
     * value based on which is defined. If neither chunk's file address is
     * defined, compare chunk entries based on their dataset object header
     * address, then by their chunk index value.
     */
    if (H5_addr_defined(addr1) && H5_addr_defined(addr2)) {
        ret_value = H5_addr_cmp(addr1, addr2);
    }
    else if (!H5_addr_defined(addr1) && !H5_addr_defined(addr2)) {
        haddr_t oloc_addr1 = entry1->index_info.dset_oloc_addr;
        haddr_t oloc_addr2 = entry2->index_info.dset_oloc_addr;

        if (0 == (ret_value = H5_addr_cmp(oloc_addr1, oloc_addr2))) {
            hsize_t chunk_idx1 = entry1->index_info.chunk_idx;
            hsize_t chunk_idx2 = entry2->index_info.chunk_idx;

            ret_value = (chunk_idx1 > chunk_idx2) - (chunk_idx1 < chunk_idx2);
        }
    }
    else
        ret_value = H5_addr_defined(addr1) ? 1 : -1;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__cmp_filtered_collective_io_info_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5D__cmp_chunk_redistribute_info
 *
 * Purpose:     Routine to compare two H5D_chunk_redistribute_info_t
 *              structures
 *
 * Description: Callback for qsort() to compare two
 *              H5D_chunk_redistribute_info_t structures
 *
 * Return:      -1, 0, 1
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__cmp_chunk_redistribute_info(const void *_entry1, const void *_entry2)
{
    const H5D_chunk_redistribute_info_t *entry1;
    const H5D_chunk_redistribute_info_t *entry2;
    haddr_t                              oloc_addr1;
    haddr_t                              oloc_addr2;
    int                                  ret_value;

    FUNC_ENTER_PACKAGE_NOERR

    entry1 = (const H5D_chunk_redistribute_info_t *)_entry1;
    entry2 = (const H5D_chunk_redistribute_info_t *)_entry2;

    oloc_addr1 = entry1->dset_oloc_addr;
    oloc_addr2 = entry2->dset_oloc_addr;

    /* Sort first by dataset object header address */
    if (0 == (ret_value = H5_addr_cmp(oloc_addr1, oloc_addr2))) {
        hsize_t chunk_index1 = entry1->chunk_idx;
        hsize_t chunk_index2 = entry2->chunk_idx;

        /* Then by chunk index value */
        if (chunk_index1 == chunk_index2) {
            int orig_owner1 = entry1->orig_owner;
            int orig_owner2 = entry2->orig_owner;

            /* And finally by original owning MPI rank for the chunk */

            ret_value = (orig_owner1 > orig_owner2) - (orig_owner1 < orig_owner2);
        }
        else
            ret_value = (chunk_index1 > chunk_index2) - (chunk_index1 < chunk_index2);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__cmp_chunk_redistribute_info() */

/*-------------------------------------------------------------------------
 * Function:    H5D__cmp_chunk_redistribute_info_orig_owner
 *
 * Purpose:     Routine to compare the original owning MPI rank for two
 *              H5D_chunk_redistribute_info_t structures
 *
 * Description: Callback for qsort() to compare the original owning MPI
 *              rank for two H5D_chunk_redistribute_info_t
 *              structures
 *
 * NOTE:        The inner logic used in this sorting callback (inside the
 *              block where the original owners are equal) is intended to
 *              cause the given array of H5D_chunk_redistribute_info_t
 *              structures to be sorted back exactly as it was sorted
 *              before a shared chunks redistribution operation, according
 *              to the logic in H5D__cmp_filtered_collective_io_info_entry.
 *              Since the two sorting callbacks are currently tied directly
 *              to each other, both should be updated in the same way when
 *              changes are made.
 *
 * Return:      -1, 0, 1
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__cmp_chunk_redistribute_info_orig_owner(const void *_entry1, const void *_entry2)
{
    const H5D_chunk_redistribute_info_t *entry1;
    const H5D_chunk_redistribute_info_t *entry2;
    int                                  owner1 = -1;
    int                                  owner2 = -1;
    int                                  ret_value;

    FUNC_ENTER_PACKAGE_NOERR

    entry1 = (const H5D_chunk_redistribute_info_t *)_entry1;
    entry2 = (const H5D_chunk_redistribute_info_t *)_entry2;

    owner1 = entry1->orig_owner;
    owner2 = entry2->orig_owner;

    if (owner1 == owner2) {
        haddr_t addr1 = entry1->chunk_block.offset;
        haddr_t addr2 = entry2->chunk_block.offset;

        /*
         * If both chunk's file addresses are defined, H5_addr_cmp is safe to use.
         * If only one chunk's file address is defined, return the appropriate
         * value based on which is defined. If neither chunk's file address is
         * defined, compare chunk entries based on their dataset object header
         * address, then by their chunk index value.
         */
        if (H5_addr_defined(addr1) && H5_addr_defined(addr2)) {
            ret_value = H5_addr_cmp(addr1, addr2);
        }
        else if (!H5_addr_defined(addr1) && !H5_addr_defined(addr2)) {
            haddr_t oloc_addr1 = entry1->dset_oloc_addr;
            haddr_t oloc_addr2 = entry2->dset_oloc_addr;

            if (0 == (ret_value = H5_addr_cmp(oloc_addr1, oloc_addr2))) {
                hsize_t chunk_idx1 = entry1->chunk_idx;
                hsize_t chunk_idx2 = entry2->chunk_idx;

                ret_value = (chunk_idx1 > chunk_idx2) - (chunk_idx1 < chunk_idx2);
            }
        }
        else
            ret_value = H5_addr_defined(addr1) ? 1 : -1;
    }
    else
        ret_value = (owner1 > owner2) - (owner1 < owner2);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__cmp_chunk_redistribute_info_orig_owner() */

/*-------------------------------------------------------------------------
 * Function:    H5D__obtain_mpio_mode
 *
 * Purpose:     Routine to obtain each io mode(collective,independent or none) for each chunk;
 *              Each chunk address is also obtained.
 *
 * Description:
 *
 *              1) Each process provides two piece of information for all chunks having selection
 *                 a) chunk index
 *                 b) whether this chunk is regular(for MPI derived datatype not working case)
 *
 *              2) Gather all the information to the root process
 *
 *              3) Root process will do the following:
 *                 a) Obtain chunk addresses for all chunks in this dataspace
 *                 b) With the consideration of the user option, calculate IO mode for each chunk
 *                 c) Build MPI derived datatype to combine "chunk address" and "assign_io" information
 *                      in order to do MPI Bcast only once
 *                 d) MPI Bcast the IO mode and chunk address information for each chunk.
 *              4) Each process then retrieves IO mode and chunk address information to assign_io_mode and
 *chunk_addr.
 *
 * Parameters:
 *
 *              Input: H5D_io_info_t* io_info,
 *                      H5D_dset_io_info_t *di,(dataset info struct)
 *              Output: uint8_t assign_io_mode[], : IO mode, collective, independent or none
 *                      haddr_t chunk_addr[],     : chunk address array for each chunk
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__obtain_mpio_mode(H5D_io_info_t *io_info, H5D_dset_io_info_t *di, uint8_t assign_io_mode[],
                      haddr_t chunk_addr[], int mpi_rank, int mpi_size)
{
    size_t                  total_chunks;
    unsigned                percent_nproc_per_chunk, threshold_nproc_per_chunk;
    uint8_t                *io_mode_info      = NULL;
    uint8_t                *recv_io_mode_info = NULL;
    uint8_t                *mergebuf          = NULL;
    uint8_t                *tempbuf;
    H5SL_node_t            *chunk_node;
    H5D_piece_info_t       *chunk_info;
    H5P_coll_md_read_flag_t md_reads_file_flag;
    bool                    md_reads_context_flag;
    bool                    restore_md_reads_state = false;
    MPI_Comm                comm;
    int                     root;
    size_t                  ic;
    int                     mpi_code;
    herr_t                  ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(di->layout->type == H5D_CHUNKED);

    /* Assign the rank 0 to the root */
    root = 0;
    comm = io_info->comm;

    /* Setup parameters */
    H5_CHECKED_ASSIGN(total_chunks, size_t, di->layout->u.chunk.nchunks, hsize_t);
    if (H5CX_get_mpio_chunk_opt_ratio(&percent_nproc_per_chunk) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "couldn't get percent nproc per chunk");
    /* if ratio is 0, perform collective io */
    if (0 == percent_nproc_per_chunk) {
        if (H5D__chunk_addrmap(di->dset, chunk_addr) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get chunk address");
        for (ic = 0; ic < total_chunks; ic++)
            assign_io_mode[ic] = H5D_CHUNK_IO_MODE_COL;

        HGOTO_DONE(SUCCEED);
    } /* end if */

    threshold_nproc_per_chunk = (unsigned)mpi_size * percent_nproc_per_chunk / 100;

    /* Allocate memory */
    if (NULL == (io_mode_info = (uint8_t *)H5MM_calloc(total_chunks)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate I/O mode info buffer");
    if (NULL == (mergebuf = (uint8_t *)H5MM_malloc((sizeof(haddr_t) + 1) * total_chunks)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate mergebuf buffer");
    tempbuf = mergebuf + total_chunks;
    if (mpi_rank == root)
        if (NULL == (recv_io_mode_info = (uint8_t *)H5MM_malloc(total_chunks * (size_t)mpi_size)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate recv I/O mode info buffer");

    /* Obtain the regularity and selection information for all chunks in this process. */
    chunk_node = H5SL_first(di->layout_io_info.chunk_map->dset_sel_pieces);
    while (chunk_node) {
        chunk_info = (H5D_piece_info_t *)H5SL_item(chunk_node);

        io_mode_info[chunk_info->index] = H5D_CHUNK_SELECT_REG; /* this chunk is selected and is "regular" */
        chunk_node                      = H5SL_next(chunk_node);
    } /* end while */

    /* Gather all the information */
    H5_CHECK_OVERFLOW(total_chunks, size_t, int);
    if (MPI_SUCCESS != (mpi_code = MPI_Gather(io_mode_info, (int)total_chunks, MPI_BYTE, recv_io_mode_info,
                                              (int)total_chunks, MPI_BYTE, root, comm)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Gather failed", mpi_code)

    /* Calculate the mode for IO(collective, independent or none) at root process */
    if (mpi_rank == root) {
        size_t    nproc;
        unsigned *nproc_per_chunk;

        /*
         * If enabled, disable collective metadata reads here.
         * Since the chunk address mapping is done on rank 0
         * only here, it will cause problems if collective
         * metadata reads are enabled.
         */
        if (H5F_get_coll_metadata_reads(di->dset->oloc.file)) {
#ifndef NDEBUG
            {
                H5D_chk_idx_info_t idx_info;
                bool               index_is_open;

                idx_info.f       = di->dset->oloc.file;
                idx_info.pline   = &di->dset->shared->dcpl_cache.pline;
                idx_info.layout  = &di->dset->shared->layout.u.chunk;
                idx_info.storage = &di->dset->shared->layout.storage.u.chunk;

                /*
                 * The dataset's chunk index should be open at this point.
                 * Otherwise, we will end up reading it in independently,
                 * which may not be desired.
                 */
                idx_info.storage->ops->is_open(&idx_info, &index_is_open);
                assert(index_is_open);
            }
#endif

            md_reads_file_flag    = H5P_FORCE_FALSE;
            md_reads_context_flag = false;
            H5F_set_coll_metadata_reads(di->dset->oloc.file, &md_reads_file_flag, &md_reads_context_flag);
            restore_md_reads_state = true;
        }

        /* pre-computing: calculate number of processes and
            regularity of the selection occupied in each chunk */
        if (NULL == (nproc_per_chunk = (unsigned *)H5MM_calloc(total_chunks * sizeof(unsigned))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate nproc_per_chunk buffer");

        /* calculating the chunk address */
        if (H5D__chunk_addrmap(di->dset, chunk_addr) < 0) {
            H5MM_free(nproc_per_chunk);
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get chunk address");
        } /* end if */

        /* checking for number of process per chunk and regularity of the selection*/
        for (nproc = 0; nproc < (size_t)mpi_size; nproc++) {
            uint8_t *tmp_recv_io_mode_info = recv_io_mode_info + (nproc * total_chunks);

            /* Calculate the number of process per chunk and adding irregular selection option */
            for (ic = 0; ic < total_chunks; ic++, tmp_recv_io_mode_info++) {
                if (*tmp_recv_io_mode_info != 0) {
                    nproc_per_chunk[ic]++;
                } /* end if */
            }     /* end for */
        }         /* end for */

        /* Calculating MPIO mode for each chunk (collective, independent, none) */
        for (ic = 0; ic < total_chunks; ic++) {
            if (nproc_per_chunk[ic] > MAX(1, threshold_nproc_per_chunk)) {
                assign_io_mode[ic] = H5D_CHUNK_IO_MODE_COL;
            } /* end if */
        }     /* end for */

        /* merge buffer io_mode info and chunk addr into one */
        H5MM_memcpy(mergebuf, assign_io_mode, total_chunks);
        H5MM_memcpy(tempbuf, chunk_addr, sizeof(haddr_t) * total_chunks);

        H5MM_free(nproc_per_chunk);
    } /* end if */

    /* Broadcasting the MPI_IO option info. and chunk address info. */
    if ((sizeof(haddr_t) + 1) * total_chunks > INT_MAX)
        HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "result overflow");
    if (MPI_SUCCESS !=
        (mpi_code = MPI_Bcast(mergebuf, (int)((sizeof(haddr_t) + 1) * total_chunks), MPI_BYTE, root, comm)))
        HMPI_GOTO_ERROR(FAIL, "MPI_BCast failed", mpi_code)

    H5MM_memcpy(assign_io_mode, mergebuf, total_chunks);
    H5MM_memcpy(chunk_addr, tempbuf, sizeof(haddr_t) * total_chunks);

#ifdef H5_HAVE_INSTRUMENTED_LIBRARY
    {
        bool coll_op = false;

        for (ic = 0; ic < total_chunks; ic++)
            if (assign_io_mode[ic] == H5D_CHUNK_IO_MODE_COL) {
                if (H5CX_test_set_mpio_coll_chunk_multi_ratio_coll(0) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set property value");
                coll_op = true;
                break;
            } /* end if */

        if (!coll_op)
            if (H5CX_test_set_mpio_coll_chunk_multi_ratio_ind(0) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set property value");
    }
#endif

done:
    /* Re-enable collective metadata reads if we disabled them */
    if (restore_md_reads_state)
        H5F_set_coll_metadata_reads(di->dset->oloc.file, &md_reads_file_flag, &md_reads_context_flag);

    if (io_mode_info)
        H5MM_free(io_mode_info);
    if (mergebuf)
        H5MM_free(mergebuf);
    if (recv_io_mode_info) {
        assert(mpi_rank == root);
        H5MM_free(recv_io_mode_info);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__obtain_mpio_mode() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_collective_filtered_chunk_io_setup
 *
 * Purpose:     Constructs a list of entries which contain the necessary
 *              information for inter-process communication when performing
 *              collective io on filtered chunks. This list is used by
 *              each MPI rank when performing I/O on locally selected
 *              chunks and also in operations that must be collectively
 *              done on every chunk, such as chunk re-allocation, insertion
 *              of chunks into the chunk index, etc.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_collective_filtered_chunk_io_setup(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *di,
                                             size_t num_dset_infos, int mpi_rank,
                                             H5D_filtered_collective_io_info_t *chunk_list)
{
    H5D_filtered_collective_chunk_info_t *local_info_array    = NULL;
    H5D_mpio_filtered_dset_info_t        *curr_dset_info      = NULL;
    size_t                                num_chunks_selected = 0;
    size_t                                num_chunks_to_read  = 0;
    size_t                                buf_idx             = 0;
    bool                                  need_sort           = false;
    herr_t                                ret_value           = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(io_info);
    assert(di);
    assert(chunk_list);

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TRACE_ENTER(mpi_rank);
    H5D_MPIO_TIME_START(mpi_rank, "Filtered Collective I/O Setup");
#endif

    /* Calculate hash key length for chunk hash table */
    if (num_dset_infos > 1) {
        /* Just in case the structure changes... */
        HDcompile_assert(offsetof(H5D_chunk_index_info_t, dset_oloc_addr) >
                         offsetof(H5D_chunk_index_info_t, chunk_idx));

        /* Calculate key length using uthash compound key example */
        chunk_list->chunk_hash_table_keylen = offsetof(H5D_chunk_index_info_t, dset_oloc_addr) +
                                              sizeof(haddr_t) - offsetof(H5D_chunk_index_info_t, chunk_idx);
    }
    else
        chunk_list->chunk_hash_table_keylen = sizeof(hsize_t);

    chunk_list->all_dset_indices_empty       = true;
    chunk_list->no_dset_index_insert_methods = true;

    /* Calculate size needed for total chunk list */
    for (size_t dset_idx = 0; dset_idx < num_dset_infos; dset_idx++) {
        /* Skip this dataset if no I/O is being performed */
        if (di[dset_idx].skip_io)
            continue;

        /* Only process filtered, chunked datasets. A contiguous dataset
         * could possibly have filters in the DCPL pipeline, but the library
         * will currently ignore optional filters in that case.
         */
        if ((di[dset_idx].dset->shared->dcpl_cache.pline.nused == 0) ||
            (di[dset_idx].layout->type == H5D_CONTIGUOUS))
            continue;

        assert(di[dset_idx].layout->type == H5D_CHUNKED);
        assert(di[dset_idx].layout->storage.type == H5D_CHUNKED);

        num_chunks_selected += H5SL_count(di[dset_idx].layout_io_info.chunk_map->dset_sel_pieces);
    }

    if (num_chunks_selected)
        if (NULL == (local_info_array = H5MM_malloc(num_chunks_selected * sizeof(*local_info_array))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate local io info array buffer");

    for (size_t dset_idx = 0; dset_idx < num_dset_infos; dset_idx++) {
        H5D_chunk_ud_t udata;
        H5O_fill_t    *fill_msg;
        haddr_t        prev_tag = HADDR_UNDEF;

        /* Skip this dataset if no I/O is being performed */
        if (di[dset_idx].skip_io)
            continue;

        /* Only process filtered, chunked datasets. A contiguous dataset
         * could possibly have filters in the DCPL pipeline, but the library
         * will currently ignore optional filters in that case.
         */
        if ((di[dset_idx].dset->shared->dcpl_cache.pline.nused == 0) ||
            (di[dset_idx].layout->type == H5D_CONTIGUOUS))
            continue;

        assert(di[dset_idx].layout->storage.type == H5D_CHUNKED);
        assert(di[dset_idx].layout->storage.u.chunk.idx_type != H5D_CHUNK_IDX_NONE);

        /*
         * To support the multi-dataset I/O case, cache some info (chunk size,
         * fill buffer and fill dataspace, etc.) about each dataset involved
         * in the I/O operation for use when processing chunks. If only one
         * dataset is involved, this information is the same for every chunk
         * processed. Otherwise, if multiple datasets are involved, a hash
         * table is used to quickly match a particular chunk with the cached
         * information pertaining to the dataset it resides in.
         */
        if (NULL == (curr_dset_info = H5MM_malloc(sizeof(H5D_mpio_filtered_dset_info_t))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate space for dataset info");

        memset(&curr_dset_info->fb_info, 0, sizeof(H5D_fill_buf_info_t));

        H5D_MPIO_INIT_CHUNK_IDX_INFO(curr_dset_info->chunk_idx_info, di[dset_idx].dset);

        curr_dset_info->dset_io_info    = &di[dset_idx];
        curr_dset_info->file_chunk_size = di[dset_idx].dset->shared->layout.u.chunk.size;
        curr_dset_info->dset_oloc_addr  = di[dset_idx].dset->oloc.addr;
        curr_dset_info->fill_space      = NULL;
        curr_dset_info->fb_info_init    = false;
        curr_dset_info->index_empty     = false;

        /* Determine if fill values should be written to chunks */
        fill_msg = &di[dset_idx].dset->shared->dcpl_cache.fill;
        curr_dset_info->should_fill =
            (fill_msg->fill_time == H5D_FILL_TIME_ALLOC) ||
            ((fill_msg->fill_time == H5D_FILL_TIME_IFSET) && fill_msg->fill_defined);

        if (curr_dset_info->should_fill) {
            hsize_t chunk_dims[H5S_MAX_RANK];

            assert(di[dset_idx].dset->shared->ndims == di[dset_idx].dset->shared->layout.u.chunk.ndims - 1);
            for (size_t dim_idx = 0; dim_idx < di[dset_idx].dset->shared->layout.u.chunk.ndims - 1; dim_idx++)
                chunk_dims[dim_idx] = (hsize_t)di[dset_idx].dset->shared->layout.u.chunk.dim[dim_idx];

            /* Get a dataspace for filling chunk memory buffers */
            if (NULL == (curr_dset_info->fill_space = H5S_create_simple(
                             di[dset_idx].dset->shared->layout.u.chunk.ndims - 1, chunk_dims, NULL)))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to create chunk fill dataspace");

            /* Initialize fill value buffer */
            if (H5D__fill_init(&curr_dset_info->fb_info, NULL, (H5MM_allocate_t)H5D__chunk_mem_alloc,
                               (void *)&di[dset_idx].dset->shared->dcpl_cache.pline,
                               (H5MM_free_t)H5D__chunk_mem_free,
                               (void *)&di[dset_idx].dset->shared->dcpl_cache.pline,
                               &di[dset_idx].dset->shared->dcpl_cache.fill, di[dset_idx].dset->shared->type,
                               di[dset_idx].dset->shared->type_id, 0, curr_dset_info->file_chunk_size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize fill value buffer");

            curr_dset_info->fb_info_init = true;
        }

        /*
         * If the dataset is incrementally allocated and hasn't been written
         * to yet, the chunk index should be empty. In this case, a collective
         * read of its chunks is essentially a no-op, so we can avoid that read
         * later. If all datasets have empty chunk indices, we can skip the
         * collective read entirely.
         */
        if (fill_msg->alloc_time == H5D_ALLOC_TIME_INCR)
            if (H5D__chunk_index_empty(di[dset_idx].dset, &curr_dset_info->index_empty) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "couldn't determine if chunk index is empty");

        if ((fill_msg->alloc_time != H5D_ALLOC_TIME_INCR) || !curr_dset_info->index_empty)
            chunk_list->all_dset_indices_empty = false;

        if (curr_dset_info->chunk_idx_info.storage->ops->insert)
            chunk_list->no_dset_index_insert_methods = false;

        /*
         * For multi-dataset I/O, use a hash table to keep a mapping between
         * chunks and the cached info for the dataset that they're in. Otherwise,
         * we can just use the info object directly if only one dataset is being
         * worked on.
         */
        if (num_dset_infos > 1) {
            HASH_ADD(hh, chunk_list->dset_info.dset_info_hash_table, dset_oloc_addr, sizeof(haddr_t),
                     curr_dset_info);
        }
        else
            chunk_list->dset_info.single_dset_info = curr_dset_info;
        curr_dset_info = NULL;

        /*
         * Now, each rank builds a local list of info about the chunks
         * they have selected among the chunks in the current dataset
         */

        /* Set metadata tagging with dataset oheader addr */
        H5AC_tag(di[dset_idx].dset->oloc.addr, &prev_tag);

        if (H5SL_count(di[dset_idx].layout_io_info.chunk_map->dset_sel_pieces)) {
            H5SL_node_t *chunk_node;
            bool         filter_partial_edge_chunks;

            /* Determine whether partial edge chunks should be filtered */
            filter_partial_edge_chunks = !(di[dset_idx].dset->shared->layout.u.chunk.flags &
                                           H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS);

            chunk_node = H5SL_first(di[dset_idx].layout_io_info.chunk_map->dset_sel_pieces);
            while (chunk_node) {
                H5D_piece_info_t *chunk_info;
                hsize_t           select_npoints;

                chunk_info = (H5D_piece_info_t *)H5SL_item(chunk_node);
                assert(chunk_info->filtered_dset);

                /* Obtain this chunk's address */
                if (H5D__chunk_lookup(di[dset_idx].dset, chunk_info->scaled, &udata) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");

                /* Initialize rank-local chunk info */
                local_info_array[buf_idx].chunk_info     = chunk_info;
                local_info_array[buf_idx].chunk_buf_size = 0;
                local_info_array[buf_idx].num_writers    = 0;
                local_info_array[buf_idx].orig_owner     = mpi_rank;
                local_info_array[buf_idx].new_owner      = mpi_rank;
                local_info_array[buf_idx].buf            = NULL;

                select_npoints = H5S_GET_SELECT_NPOINTS(chunk_info->fspace);
                local_info_array[buf_idx].io_size =
                    (size_t)select_npoints * di[dset_idx].type_info.dst_type_size;

                /*
                 * Determine whether this chunk will need to be read from the file. If this is
                 * a read operation, the chunk will be read. If this is a write operation, we
                 * generally need to read a filtered chunk from the file before modifying it,
                 * unless the chunk is being fully overwritten.
                 *
                 * TODO: Currently the full overwrite status of a chunk is only obtained on a
                 * per-rank basis. This means that if the total selection in the chunk, as
                 * determined by the combination of selections of all of the ranks interested in
                 * the chunk, covers the entire chunk, the performance optimization of not reading
                 * the chunk from the file is still valid, but is not applied in the current
                 * implementation.
                 *
                 * To implement this case, a few approaches were considered:
                 *
                 *  - Keep a running total (distributed to each rank) of the number of chunk
                 *    elements selected during chunk redistribution and compare that to the total
                 *    number of elements in the chunk once redistribution is finished
                 *
                 *  - Process all incoming chunk messages before doing I/O (these are currently
                 *    processed AFTER doing I/O), combine the owning rank's selection in a chunk
                 *    with the selections received from other ranks and check to see whether that
                 *    combined selection covers the entire chunk
                 *
                 * The first approach will be dangerous if the application performs an overlapping
                 * write to a chunk, as the number of selected elements can equal or exceed the
                 * number of elements in the chunk without the whole chunk selection being covered.
                 * While it might be considered erroneous for an application to do an overlapping
                 * write, we don't explicitly disallow it.
                 *
                 * The second approach contains a bit of complexity in that part of the chunk
                 * messages will be needed before doing I/O and part will be needed after doing I/O.
                 * Since modification data from chunk messages can't be applied until after any I/O
                 * is performed (otherwise, we'll overwrite any applied modification data), chunk
                 * messages are currently entirely processed after I/O. However, in order to determine
                 * if a chunk is being fully overwritten, we need the dataspace portion of the chunk
                 * messages before doing I/O. The naive way to do this is to process chunk messages
                 * twice, using just the relevant information from the message before and after I/O.
                 * The better way would be to avoid processing chunk messages twice by extracting (and
                 * keeping around) the dataspace portion of the message before I/O and processing the
                 * rest of the chunk message after I/O. Note that the dataspace portion of each chunk
                 * message is used to correctly apply chunk modification data from the message, so
                 * must be kept around both before and after I/O in this case.
                 */
                if (io_info->op_type == H5D_IO_OP_READ)
                    local_info_array[buf_idx].need_read = true;
                else {
                    local_info_array[buf_idx].need_read =
                        local_info_array[buf_idx].io_size <
                        (size_t)di[dset_idx].dset->shared->layout.u.chunk.size;
                }

                if (local_info_array[buf_idx].need_read)
                    num_chunks_to_read++;

                local_info_array[buf_idx].skip_filter_pline = false;
                if (!filter_partial_edge_chunks) {
                    /*
                     * If this is a partial edge chunk and the "don't filter partial edge
                     * chunks" flag is set, make sure not to apply filters to the chunk.
                     */
                    if (H5D__chunk_is_partial_edge_chunk(
                            di[dset_idx].dset->shared->ndims, di[dset_idx].dset->shared->layout.u.chunk.dim,
                            chunk_info->scaled, di[dset_idx].dset->shared->curr_dims))
                        local_info_array[buf_idx].skip_filter_pline = true;
                }

                /* Initialize the chunk's shared info */
                local_info_array[buf_idx].chunk_current = udata.chunk_block;
                local_info_array[buf_idx].chunk_new     = udata.chunk_block;

                /*
                 * Check if the list is not in ascending order of offset in the file
                 * or has unallocated chunks. In either case, the list should get
                 * sorted.
                 */
                if (!need_sort && buf_idx) {
                    haddr_t curr_chunk_offset = local_info_array[buf_idx].chunk_current.offset;
                    haddr_t prev_chunk_offset = local_info_array[buf_idx - 1].chunk_current.offset;

                    if (!H5_addr_defined(prev_chunk_offset) || !H5_addr_defined(curr_chunk_offset) ||
                        (curr_chunk_offset < prev_chunk_offset))
                        need_sort = true;
                }

                /* Needed for proper hashing later on */
                memset(&local_info_array[buf_idx].index_info, 0, sizeof(H5D_chunk_index_info_t));

                /*
                 * Extensible arrays may calculate a chunk's index a little differently
                 * than normal when the dataset's unlimited dimension is not the
                 * slowest-changing dimension, so set the index here based on what the
                 * extensible array code calculated instead of what was calculated
                 * in the chunk file mapping.
                 */
                if (di[dset_idx].dset->shared->layout.u.chunk.idx_type == H5D_CHUNK_IDX_EARRAY)
                    local_info_array[buf_idx].index_info.chunk_idx = udata.chunk_idx;
                else
                    local_info_array[buf_idx].index_info.chunk_idx = chunk_info->index;

                assert(H5_addr_defined(di[dset_idx].dset->oloc.addr));
                local_info_array[buf_idx].index_info.dset_oloc_addr = di[dset_idx].dset->oloc.addr;

                local_info_array[buf_idx].index_info.filter_mask = udata.filter_mask;
                local_info_array[buf_idx].index_info.need_insert = false;

                buf_idx++;

                chunk_node = H5SL_next(chunk_node);
            }
        }

        /* Reset metadata tagging */
        H5AC_tag(prev_tag, NULL);
    }

    /* Ensure the chunk list is sorted in ascending order of offset in the file */
    if (local_info_array && need_sort)
        qsort(local_info_array, num_chunks_selected, sizeof(H5D_filtered_collective_chunk_info_t),
              H5D__cmp_filtered_collective_io_info_entry);

    chunk_list->chunk_infos        = local_info_array;
    chunk_list->num_chunk_infos    = num_chunks_selected;
    chunk_list->num_chunks_to_read = num_chunks_to_read;

#ifdef H5Dmpio_DEBUG
    H5D__mpio_dump_collective_filtered_chunk_list(chunk_list, mpi_rank);
#endif

done:
    if (ret_value < 0) {
        /* Free temporary cached dataset info object */
        if (curr_dset_info) {
            if (curr_dset_info->fb_info_init && H5D__fill_term(&curr_dset_info->fb_info) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't release fill buffer info");
            if (curr_dset_info->fill_space && H5S_close(curr_dset_info->fill_space) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close fill space");

            H5MM_free(curr_dset_info);
            curr_dset_info = NULL;

            if (num_dset_infos == 1)
                chunk_list->dset_info.single_dset_info = NULL;
        }

        /* Free resources used by cached dataset info hash table */
        if (num_dset_infos > 1) {
            H5D_mpio_filtered_dset_info_t *tmp;

            HASH_ITER(hh, chunk_list->dset_info.dset_info_hash_table, curr_dset_info, tmp)
            {
                HASH_DELETE(hh, chunk_list->dset_info.dset_info_hash_table, curr_dset_info);
                H5MM_free(curr_dset_info);
                curr_dset_info = NULL;
            }
        }

        if (num_dset_infos == 1)
            chunk_list->dset_info.single_dset_info = NULL;
        else
            chunk_list->dset_info.dset_info_hash_table = NULL;

        H5MM_free(local_info_array);
    }

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TIME_STOP(mpi_rank);
    H5D_MPIO_TRACE_EXIT(mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_collective_filtered_chunk_io_setup() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_redistribute_shared_chunks
 *
 * Purpose:     When performing a parallel write on a chunked Dataset with
 *              filters applied, we must ensure that any particular chunk
 *              is only written to by a single MPI rank in order to avoid
 *              potential data races on the chunk. This function is used to
 *              redistribute (by assigning ownership to a single rank) any
 *              chunks which are selected by more than one MPI rank.
 *
 *              An initial Allgather is performed to determine how many
 *              chunks each rank has selected in the write operation and
 *              then that number is compared against a threshold value to
 *              determine whether chunk redistribution should be done on
 *              MPI rank 0 only, or on all MPI ranks.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_redistribute_shared_chunks(H5D_filtered_collective_io_info_t *chunk_list,
                                     const H5D_io_info_t *io_info, int mpi_rank, int mpi_size,
                                     size_t **rank_chunks_assigned_map)
{
    bool    redistribute_on_all_ranks;
    size_t *num_chunks_map       = NULL;
    size_t  coll_chunk_list_size = 0;
    int     mpi_code;
    herr_t  ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(chunk_list);
    assert(io_info);
    assert(mpi_size > 1); /* No chunk sharing is possible for MPI Comm size of 1 */

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TRACE_ENTER(mpi_rank);
    H5D_MPIO_TIME_START(mpi_rank, "Redistribute shared chunks");
#endif

    /*
     * Allocate an array for each rank to keep track of the number of
     * chunks assigned to any other rank in order to cut down on future
     * MPI communication.
     */
    if (NULL == (num_chunks_map = H5MM_malloc((size_t)mpi_size * sizeof(*num_chunks_map))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "couldn't allocate assigned chunks array");

    /* Perform initial Allgather to determine the collective chunk list size */
    if (MPI_SUCCESS != (mpi_code = MPI_Allgather(&chunk_list->num_chunk_infos, 1, H5_SIZE_T_AS_MPI_TYPE,
                                                 num_chunks_map, 1, H5_SIZE_T_AS_MPI_TYPE, io_info->comm)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Allgather failed", mpi_code)

    for (int curr_rank = 0; curr_rank < mpi_size; curr_rank++)
        coll_chunk_list_size += num_chunks_map[curr_rank];

    /*
     * Determine whether we should perform chunk redistribution on all
     * ranks or just rank 0. For a relatively small number of chunks,
     * we redistribute on all ranks to cut down on MPI communication
     * overhead. For a larger number of chunks, we redistribute on
     * rank 0 only to cut down on memory usage.
     */
    redistribute_on_all_ranks = coll_chunk_list_size < H5D_CHUNK_REDISTRIBUTE_THRES;

    if (H5D__mpio_redistribute_shared_chunks_int(chunk_list, num_chunks_map, redistribute_on_all_ranks,
                                                 io_info, mpi_rank, mpi_size) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTREDISTRIBUTE, FAIL, "can't redistribute shared chunks");

    /*
     * If the caller provided a pointer for the mapping from
     * rank value -> number of chunks assigned, return that
     * mapping here.
     */
    if (rank_chunks_assigned_map) {
        /*
         * If we performed chunk redistribution on rank 0 only, distribute
         * the rank value -> number of chunks assigned mapping back to all
         * ranks.
         */
        if (!redistribute_on_all_ranks) {
            if (MPI_SUCCESS !=
                (mpi_code = MPI_Bcast(num_chunks_map, mpi_size, H5_SIZE_T_AS_MPI_TYPE, 0, io_info->comm)))
                HMPI_GOTO_ERROR(FAIL, "couldn't broadcast chunk mapping to other ranks", mpi_code)
        }

        *rank_chunks_assigned_map = num_chunks_map;
    }

done:
    if (!rank_chunks_assigned_map || (ret_value < 0)) {
        num_chunks_map = H5MM_xfree(num_chunks_map);
    }

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TIME_STOP(mpi_rank);
    H5D_MPIO_TRACE_EXIT(mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_redistribute_shared_chunks() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_redistribute_shared_chunks_int
 *
 * Purpose:     Routine to perform redistribution of shared chunks during
 *              parallel writes to datasets with filters applied.
 *
 *              If `all_ranks_involved` is true, chunk redistribution
 *              occurs on all MPI ranks. This is usually done when there
 *              is a relatively small number of chunks involved in order to
 *              cut down on MPI communication overhead while increasing
 *              total memory usage a bit.
 *
 *              If `all_ranks_involved` is false, only rank 0 will perform
 *              chunk redistribution. This is usually done when there is
 *              a relatively large number of chunks involved in order to
 *              cut down on total memory usage at the cost of increased
 *              overhead from MPI communication.
 *
 *              This implementation is as follows:
 *
 *              - All MPI ranks send their list of selected chunks to the
 *                ranks involved in chunk redistribution. Then, the
 *                involved ranks sort this new list in order of:
 *
 *                  dataset object header address -> chunk index value ->
 *                    original owning MPI rank for chunk
 *
 *              - The involved ranks scan the list looking for matching
 *                runs of (dataset object header address, chunk index value)
 *                pairs (corresponding to a shared chunk which has been
 *                selected by more than one rank in the I/O operation) and
 *                for each shared chunk, redistribute the chunk to the MPI
 *                rank writing to the chunk which currently has the least
 *                amount of chunks assigned to it. This is done by modifying
 *                the "new_owner" field in each of the list entries
 *                corresponding to that chunk. The involved ranks then
 *                re-sort the list in order of original chunk owner so that
 *                each rank's section of contributed chunks is contiguous
 *                in the collective chunk list.
 *
 *              - If chunk redistribution occurred on all ranks, each rank
 *                scans through the collective chunk list to find their
 *                contributed section of chunks and uses that to update
 *                their local chunk list with the newly-updated "new_owner"
 *                and "num_writers" fields. If chunk redistribution
 *                occurred only on rank 0, an MPI_Scatterv operation will
 *                be used to scatter the segments of the collective chunk
 *                list from rank 0 back to the corresponding ranks.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_redistribute_shared_chunks_int(H5D_filtered_collective_io_info_t *chunk_list,
                                         size_t *num_chunks_assigned_map, bool all_ranks_involved,
                                         const H5D_io_info_t *io_info, int mpi_rank, int mpi_size)
{
    MPI_Datatype struct_type;
    MPI_Datatype packed_type;
    bool         struct_type_derived         = false;
    bool         packed_type_derived         = false;
    size_t       coll_chunk_list_num_entries = 0;
    void        *coll_chunk_list             = NULL;
    int         *counts_disps_array          = NULL;
    int         *counts_ptr                  = NULL;
    int         *displacements_ptr           = NULL;
    int          num_chunks_int;
    int          mpi_code;
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(chunk_list);
    assert(num_chunks_assigned_map);
    assert(io_info);
    assert(mpi_size > 1);

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TRACE_ENTER(mpi_rank);
    H5D_MPIO_TIME_START(mpi_rank, "Redistribute shared chunks (internal)");
#endif

    /*
     * Make sure it's safe to cast this rank's number
     * of chunks to be sent into an int for MPI
     */
    H5_CHECKED_ASSIGN(num_chunks_int, int, num_chunks_assigned_map[mpi_rank], size_t);

    /*
     * Phase 1 - Participate in collective gathering of every rank's
     * list of chunks to the ranks which are performing the redistribution
     * operation.
     */

    if (all_ranks_involved || (mpi_rank == 0)) {
        /*
         * Allocate array to store the receive counts of each rank, as well as
         * the displacements into the final array where each rank will place
         * their data. The first half of the array contains the receive counts
         * (in rank order), while the latter half contains the displacements
         * (also in rank order).
         */
        if (NULL == (counts_disps_array = H5MM_malloc(2 * (size_t)mpi_size * sizeof(*counts_disps_array)))) {
            /* Push an error, but still participate in collective gather operation */
            HDONE_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                        "couldn't allocate receive counts and displacements array");
        }
        else {
            /* Set the receive counts from the assigned chunks map */
            counts_ptr = counts_disps_array;

            for (int curr_rank = 0; curr_rank < mpi_size; curr_rank++)
                H5_CHECKED_ASSIGN(counts_ptr[curr_rank], int, num_chunks_assigned_map[curr_rank], size_t);

            /* Set the displacements into the receive buffer for the gather operation */
            displacements_ptr = &counts_disps_array[mpi_size];

            *displacements_ptr = 0;
            for (int curr_rank = 1; curr_rank < mpi_size; curr_rank++)
                displacements_ptr[curr_rank] = displacements_ptr[curr_rank - 1] + counts_ptr[curr_rank - 1];
        }
    }

    /*
     * Construct MPI derived types for extracting information
     * necessary for MPI communication
     */
    if (H5D__mpio_get_chunk_redistribute_info_types(&packed_type, &packed_type_derived, &struct_type,
                                                    &struct_type_derived) < 0) {
        /* Push an error, but still participate in collective gather operation */
        HDONE_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                    "can't create derived datatypes for chunk redistribution info");
    }

    /* Perform gather operation */
    if (H5_mpio_gatherv_alloc(chunk_list->chunk_infos, num_chunks_int, struct_type, counts_ptr,
                              displacements_ptr, packed_type, all_ranks_involved, 0, io_info->comm, mpi_rank,
                              mpi_size, &coll_chunk_list, &coll_chunk_list_num_entries) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGATHER, FAIL,
                    "can't gather chunk redistribution info to involved ranks");

    /*
     * If all ranks are redistributing shared chunks, we no
     * longer need the receive counts and displacements array
     */
    if (all_ranks_involved) {
        counts_disps_array = H5MM_xfree(counts_disps_array);
    }

    /*
     * Phase 2 - Involved ranks now redistribute any shared chunks to new
     * owners as necessary.
     */

    if (all_ranks_involved || (mpi_rank == 0)) {
        H5D_chunk_redistribute_info_t *chunk_entry;

        /* Clear the mapping from rank value -> number of assigned chunks */
        memset(num_chunks_assigned_map, 0, (size_t)mpi_size * sizeof(*num_chunks_assigned_map));

        /*
         * Sort collective chunk list according to:
         *   dataset object header address -> chunk index value -> original owning MPI rank for chunk
         */
        qsort(coll_chunk_list, coll_chunk_list_num_entries, sizeof(H5D_chunk_redistribute_info_t),
              H5D__cmp_chunk_redistribute_info);

        /*
         * Process all chunks in the collective chunk list.
         * Note that the loop counter is incremented by both
         * the outer loop (while processing each entry in
         * the collective chunk list) and the inner loop
         * (while processing duplicate entries for shared
         * chunks).
         */
        chunk_entry = &((H5D_chunk_redistribute_info_t *)coll_chunk_list)[0];
        for (size_t entry_idx = 0; entry_idx < coll_chunk_list_num_entries;) {
            haddr_t curr_oloc_addr;
            hsize_t curr_chunk_idx;
            size_t  set_begin_index;
            bool    keep_processing;
            int     num_writers;
            int     new_chunk_owner;

            /* Set chunk's initial new owner to its original owner */
            new_chunk_owner = chunk_entry->orig_owner;

            /*
             * Set the current dataset object header address and chunk
             * index value so we know when we've processed all duplicate
             * entries for a particular shared chunk
             */
            curr_oloc_addr = chunk_entry->dset_oloc_addr;
            curr_chunk_idx = chunk_entry->chunk_idx;

            /* Reset the initial number of writers to this chunk */
            num_writers = 0;

            /* Set index for the beginning of this section of duplicate chunk entries */
            set_begin_index = entry_idx;

            /*
             * Process each chunk entry in the set for the current
             * (possibly shared) chunk and increment the loop counter
             * while doing so.
             */
            do {
                /*
                 * The new owner of the chunk is determined by the rank
                 * writing to the chunk which currently has the least amount
                 * of chunks assigned to it
                 */
                if (num_chunks_assigned_map[chunk_entry->orig_owner] <
                    num_chunks_assigned_map[new_chunk_owner])
                    new_chunk_owner = chunk_entry->orig_owner;

                /* Update the number of writers to this particular chunk */
                num_writers++;

                chunk_entry++;

                keep_processing =
                    /* Make sure we haven't run out of chunks in the chunk list */
                    (++entry_idx < coll_chunk_list_num_entries) &&
                    /* Make sure the chunk we're looking at is in the same dataset */
                    (H5_addr_eq(chunk_entry->dset_oloc_addr, curr_oloc_addr)) &&
                    /* Make sure the chunk we're looking at is the same chunk */
                    (chunk_entry->chunk_idx == curr_chunk_idx);
            } while (keep_processing);

            /* We should never have more writers to a chunk than the number of MPI ranks */
            assert(num_writers <= mpi_size);

            /* Set all processed chunk entries' "new_owner" and "num_writers" fields */
            for (; set_begin_index < entry_idx; set_begin_index++) {
                H5D_chunk_redistribute_info_t *entry;

                entry = &((H5D_chunk_redistribute_info_t *)coll_chunk_list)[set_begin_index];

                entry->new_owner   = new_chunk_owner;
                entry->num_writers = num_writers;
            }

            /* Update the number of chunks assigned to the MPI rank that now owns this chunk */
            num_chunks_assigned_map[new_chunk_owner]++;
        }

        /*
         * Re-sort the collective chunk list in order of original chunk owner
         * so that each rank's section of contributed chunks is contiguous in
         * the collective chunk list.
         *
         * NOTE: this re-sort is frail in that it needs to sort the collective
         *       chunk list so that each rank's section of contributed chunks
         *       is in the exact order it was contributed in, or things will
         *       be scrambled when each rank's local chunk list is updated.
         *       Therefore, the sorting algorithm here is tied to the one
         *       used during the I/O setup operation. Specifically, chunks
         *       are first sorted by ascending order of offset in the file and
         *       then by chunk index. In the future, a better redistribution
         *       algorithm may be devised that doesn't rely on frail sorting,
         *       but the current implementation is a quick and naive approach.
         */
        qsort(coll_chunk_list, coll_chunk_list_num_entries, sizeof(H5D_chunk_redistribute_info_t),
              H5D__cmp_chunk_redistribute_info_orig_owner);
    }

    if (all_ranks_involved) {
        size_t entry_idx;

        /*
         * If redistribution occurred on all ranks, search for the section
         * in the collective chunk list corresponding to this rank's locally
         * selected chunks and update the local list after redistribution.
         */
        for (entry_idx = 0; entry_idx < coll_chunk_list_num_entries; entry_idx++)
            if (mpi_rank == ((H5D_chunk_redistribute_info_t *)coll_chunk_list)[entry_idx].orig_owner)
                break;

        for (size_t info_idx = 0; info_idx < (size_t)num_chunks_int; info_idx++) {
            H5D_chunk_redistribute_info_t *coll_entry;

            coll_entry = &((H5D_chunk_redistribute_info_t *)coll_chunk_list)[entry_idx++];

            chunk_list->chunk_infos[info_idx].new_owner   = coll_entry->new_owner;
            chunk_list->chunk_infos[info_idx].num_writers = coll_entry->num_writers;

            /*
             * Check if the chunk list struct's `num_chunks_to_read` field
             * needs to be updated
             */
            if (chunk_list->chunk_infos[info_idx].need_read &&
                (chunk_list->chunk_infos[info_idx].new_owner != mpi_rank)) {
                chunk_list->chunk_infos[info_idx].need_read = false;

                assert(chunk_list->num_chunks_to_read > 0);
                chunk_list->num_chunks_to_read--;
            }
        }
    }
    else {
        /*
         * If redistribution occurred only on rank 0, scatter the segments
         * of the collective chunk list back to each rank so that their
         * local chunk lists get updated
         */
        if (MPI_SUCCESS !=
            (mpi_code = MPI_Scatterv(coll_chunk_list, counts_ptr, displacements_ptr, packed_type,
                                     chunk_list->chunk_infos, num_chunks_int, struct_type, 0, io_info->comm)))
            HMPI_GOTO_ERROR(FAIL, "unable to scatter shared chunks info buffer", mpi_code)

        /*
         * Now that chunks have been redistributed, each rank must update
         * their chunk list struct's `num_chunks_to_read` field since it
         * may now be out of date.
         */
        for (size_t info_idx = 0; info_idx < chunk_list->num_chunk_infos; info_idx++) {
            if ((chunk_list->chunk_infos[info_idx].new_owner != mpi_rank) &&
                chunk_list->chunk_infos[info_idx].need_read) {
                chunk_list->chunk_infos[info_idx].need_read = false;

                assert(chunk_list->num_chunks_to_read > 0);
                chunk_list->num_chunks_to_read--;
            }
        }
    }

#ifdef H5Dmpio_DEBUG
    H5D__mpio_dump_collective_filtered_chunk_list(chunk_list, mpi_rank);
#endif

done:
    H5MM_free(coll_chunk_list);

    if (struct_type_derived) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&struct_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    }
    if (packed_type_derived) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&packed_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    }

    H5MM_free(counts_disps_array);

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TIME_STOP(mpi_rank);
    H5D_MPIO_TRACE_EXIT(mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_redistribute_shared_chunks_int() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_share_chunk_modification_data
 *
 * Purpose:     When performing a parallel write on a chunked dataset with
 *              filters applied, we must first ensure that any particular
 *              chunk is only written to by a single MPI rank in order to
 *              avoid potential data races on the chunk. Once dataset
 *              chunks have been redistributed in a suitable manner, each
 *              MPI rank must send its chunk data to other ranks for each
 *              chunk it no longer owns.
 *
 *              The current implementation here follows the Nonblocking
 *              Consensus algorithm described in:
 *              http://unixer.de/publications/img/hoefler-dsde-protocols.pdf
 *
 *              First, each MPI rank scans through its list of selected
 *              chunks and does the following for each chunk:
 *
 *               * If a chunk in the MPI rank's chunk list is still owned
 *                 by that rank, the rank checks how many messages are
 *                 incoming for that chunk and adds that to its running
 *                 total. Then, the rank updates its local chunk list so
 *                 that any previous chunk entries for chunks that are no
 *                 longer owned by the rank get overwritten by chunk
 *                 entries for chunks the rank still owns. Since the data
 *                 for the chunks no longer owned will have already been
 *                 sent, those chunks can effectively be discarded.
 *               * If a chunk in the MPI rank's chunk list is no longer
 *                 owned by that rank, the rank sends the data it wishes to
 *                 update the chunk with to the MPI rank that now has
 *                 ownership of that chunk. To do this, it encodes the
 *                 chunk's index value, the dataset's object header address
 *                 (only for the multi-dataset I/O case), its selection in
 *                 the chunk and its modification data into a buffer and
 *                 then posts a non-blocking MPI_Issend to the owning rank.
 *
 *              Once this step is complete, all MPI ranks allocate arrays
 *              to hold chunk message receive buffers and MPI request
 *              objects for each non-blocking receive they will post for
 *              incoming chunk modification messages. Then, all MPI ranks
 *              enter a loop that alternates between non-blocking
 *              MPI_Iprobe calls to probe for incoming messages and
 *              MPI_Testall calls to see if all send requests have
 *              completed. As chunk modification messages arrive,
 *              non-blocking MPI_Irecv calls will be posted for each
 *              message.
 *
 *              Once all send requests have completed, an MPI_Ibarrier is
 *              posted and the loop then alternates between MPI_Iprobe
 *              calls and MPI_Test calls to check if all ranks have reached
 *              the non-blocking barrier. Once all ranks have reached the
 *              barrier, processing can move on to updating the selected
 *              chunks that are owned in the operation.
 *
 *              Any chunk messages that were received from other ranks
 *              will be returned through the `chunk_msg_bufs` array and
 *              `chunk_msg_bufs_len` will be set appropriately.
 *
 *              NOTE: The use of non-blocking sends and receives of chunk
 *                    data here may contribute to large amounts of memory
 *                    usage/MPI request overhead if the number of shared
 *                    chunks is high. If this becomes a problem, it may be
 *                    useful to split the message receiving loop away so
 *                    that chunk modification messages can be received and
 *                    processed immediately (MPI_Recv) using a single chunk
 *                    message buffer. However, it's possible this may
 *                    degrade performance since the chunk message sends
 *                    are synchronous (MPI_Issend) in the Nonblocking
 *                    Consensus algorithm.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_share_chunk_modification_data(H5D_filtered_collective_io_info_t *chunk_list, H5D_io_info_t *io_info,
                                        int mpi_rank, int H5_ATTR_NDEBUG_UNUSED mpi_size,
                                        unsigned char ***chunk_msg_bufs, int *chunk_msg_bufs_len)
{
#if H5_CHECK_MPI_VERSION(3, 0)
    H5D_filtered_collective_chunk_info_t *chunk_table       = NULL;
    H5S_sel_iter_t                       *mem_iter          = NULL;
    unsigned char                       **msg_send_bufs     = NULL;
    unsigned char                       **msg_recv_bufs     = NULL;
    MPI_Request                          *send_requests     = NULL;
    MPI_Request                          *recv_requests     = NULL;
    MPI_Request                           ibarrier          = MPI_REQUEST_NULL;
    bool                                  mem_iter_init     = false;
    bool                                  ibarrier_posted   = false;
    size_t                                send_bufs_nalloc  = 0;
    size_t                                num_send_requests = 0;
    size_t                                num_recv_requests = 0;
    size_t                                num_msgs_incoming = 0;
    size_t                                hash_keylen       = 0;
    size_t                                last_assigned_idx;
    int                                   mpi_code;
    herr_t                                ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(chunk_list);
    assert(io_info);
    assert(mpi_size > 1);
    assert(chunk_msg_bufs);
    assert(chunk_msg_bufs_len);

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TRACE_ENTER(mpi_rank);
    H5D_MPIO_TIME_START(mpi_rank, "Share chunk modification data");
#endif

    /* Set to latest format for encoding dataspace */
    H5CX_set_libver_bounds(NULL);

    if (chunk_list->num_chunk_infos > 0) {
        hash_keylen = chunk_list->chunk_hash_table_keylen;
        assert(hash_keylen > 0);

        /* Allocate a selection iterator for iterating over chunk dataspaces */
        if (NULL == (mem_iter = H5FL_MALLOC(H5S_sel_iter_t)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate dataspace selection iterator");

        /*
         * Allocate send buffer and MPI_Request arrays for non-blocking
         * sends of outgoing chunk messages
         */
        send_bufs_nalloc = H5D_CHUNK_NUM_SEND_MSGS_INIT;
        if (NULL == (msg_send_bufs = H5MM_malloc(send_bufs_nalloc * sizeof(*msg_send_bufs))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                        "couldn't allocate chunk modification message buffer array");

        if (NULL == (send_requests = H5MM_malloc(send_bufs_nalloc * sizeof(*send_requests))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate send requests array");
    }

    /*
     * For each chunk this rank owns, add to the total number of
     * incoming MPI messages, then update the local chunk list to
     * overwrite any previous chunks no longer owned by this rank.
     * Since the data for those chunks will have already been sent,
     * this rank should no longer be interested in them and they
     * can effectively be discarded. This bookkeeping also makes
     * the code for the collective file space re-allocation and
     * chunk re-insertion operations a bit simpler.
     *
     * For each chunk this rank doesn't own, use non-blocking
     * synchronous sends to send the data this rank is writing to
     * the rank that does own the chunk.
     */
    last_assigned_idx = 0;
    for (size_t info_idx = 0; info_idx < chunk_list->num_chunk_infos; info_idx++) {
        H5D_filtered_collective_chunk_info_t *chunk_entry = &chunk_list->chunk_infos[info_idx];

        if (mpi_rank == chunk_entry->new_owner) {
            num_msgs_incoming += (size_t)(chunk_entry->num_writers - 1);

            /*
             * Overwrite chunk entries this rank doesn't own with entries that it
             * does own, since it has sent the necessary data and is no longer
             * interested in the chunks it doesn't own.
             */
            chunk_list->chunk_infos[last_assigned_idx] = chunk_list->chunk_infos[info_idx];

            /*
             * Since, at large scale, a chunk's index value may be larger than
             * the maximum value that can be stored in an int, we cannot rely
             * on using a chunk's index value as the tag for the MPI messages
             * sent/received for a chunk. Further, to support the multi-dataset
             * I/O case, we can't rely on being able to distinguish between
             * chunks by their chunk index value alone since two chunks from
             * different datasets could have the same chunk index value.
             * Therefore, add this chunk to a hash table with the dataset's
             * object header address + the chunk's index value as a key so that
             * we can quickly find the chunk when processing chunk messages that
             * were received. The message itself will contain the dataset's
             * object header address and the chunk's index value so we can
             * update the correct chunk with the received data.
             */
            HASH_ADD(hh, chunk_table, index_info.chunk_idx, hash_keylen,
                     &chunk_list->chunk_infos[last_assigned_idx]);

            last_assigned_idx++;
        }
        else {
            H5D_piece_info_t *chunk_info = chunk_entry->chunk_info;
            unsigned char    *mod_data_p = NULL;
            hsize_t           iter_nelmts;
            size_t            mod_data_size = 0;
            size_t            space_size    = 0;

            /* Add the size of the chunk hash table key to the encoded size */
            mod_data_size += hash_keylen;

            /* Determine size of serialized chunk file dataspace */
            if (H5S_encode(chunk_info->fspace, &mod_data_p, &space_size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get encoded dataspace size");
            mod_data_size += space_size;

            /* Determine size of data being written */
            iter_nelmts = H5S_GET_SELECT_NPOINTS(chunk_info->mspace);
            H5_CHECK_OVERFLOW(iter_nelmts, hsize_t, size_t);

            mod_data_size += (size_t)iter_nelmts * chunk_info->dset_info->type_info.src_type_size;

            if (NULL == (msg_send_bufs[num_send_requests] = H5MM_malloc(mod_data_size)))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                            "couldn't allocate chunk modification message buffer");

            mod_data_p = msg_send_bufs[num_send_requests];

            /*
             * Add the chunk hash table key (chunk index value + possibly
             * dataset object header address) into the buffer
             */
            H5MM_memcpy(mod_data_p, &chunk_entry->index_info.chunk_idx, hash_keylen);
            mod_data_p += hash_keylen;

            /* Serialize the chunk's file dataspace into the buffer */
            if (H5S_encode(chunk_info->fspace, &mod_data_p, &mod_data_size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, FAIL, "unable to encode dataspace");

            /* Initialize iterator for memory selection */
            if (H5S_select_iter_init(mem_iter, chunk_info->mspace,
                                     chunk_info->dset_info->type_info.src_type_size,
                                     H5S_SEL_ITER_SHARE_WITH_DATASPACE) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                            "unable to initialize memory selection information");
            mem_iter_init = true;

            /* Collect the modification data into the buffer */
            if (0 ==
                H5D__gather_mem(chunk_info->dset_info->buf.cvp, mem_iter, (size_t)iter_nelmts, mod_data_p))
                HGOTO_ERROR(H5E_IO, H5E_CANTGATHER, FAIL, "couldn't gather from write buffer");

            /*
             * Ensure that the size of the chunk data being sent can be
             * safely cast to an int for MPI. Note that this should
             * generally be OK for now (unless a rank is sending a
             * whole 32-bit-sized chunk of data + its encoded selection),
             * but if we allow larger than 32-bit-sized chunks in the
             * future, this may become a problem and derived datatypes
             * will need to be used.
             */
            H5_CHECK_OVERFLOW(mod_data_size, size_t, int);

            /* Send modification data to new owner */
            if (MPI_SUCCESS !=
                (mpi_code = MPI_Issend(msg_send_bufs[num_send_requests], (int)mod_data_size, MPI_BYTE,
                                       chunk_entry->new_owner, H5D_CHUNK_MOD_DATA_TAG, io_info->comm,
                                       &send_requests[num_send_requests])))
                HMPI_GOTO_ERROR(FAIL, "MPI_Issend failed", mpi_code)

            num_send_requests++;

            /* Resize send buffer and send request arrays if necessary */
            if (num_send_requests == send_bufs_nalloc) {
                void *tmp_alloc;

                send_bufs_nalloc = (size_t)((double)send_bufs_nalloc * 1.5);

                if (NULL ==
                    (tmp_alloc = H5MM_realloc(msg_send_bufs, send_bufs_nalloc * sizeof(*msg_send_bufs))))
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                                "couldn't resize chunk modification message buffer array");
                msg_send_bufs = tmp_alloc;

                if (NULL ==
                    (tmp_alloc = H5MM_realloc(send_requests, send_bufs_nalloc * sizeof(*send_requests))))
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't resize send requests array");
                send_requests = tmp_alloc;
            }

            if (H5S_SELECT_ITER_RELEASE(mem_iter) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "couldn't release memory selection iterator");
            mem_iter_init = false;
        }
    }

    /* Check if the number of send or receive requests will overflow an int (MPI requirement) */
    if (num_send_requests > INT_MAX || num_msgs_incoming > INT_MAX)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL,
                    "too many shared chunks in parallel filtered write operation");

    H5_CHECK_OVERFLOW(num_send_requests, size_t, int);
    H5_CHECK_OVERFLOW(num_msgs_incoming, size_t, int);

    /*
     * Allocate receive buffer and MPI_Request arrays for non-blocking
     * receives of incoming chunk messages
     */
    if (num_msgs_incoming) {
        if (NULL == (msg_recv_bufs = H5MM_malloc(num_msgs_incoming * sizeof(*msg_recv_bufs))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                        "couldn't allocate chunk modification message buffer array");

        if (NULL == (recv_requests = H5MM_malloc(num_msgs_incoming * sizeof(*recv_requests))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate receive requests array");
    }

    /* Process any incoming messages until everyone is done */
    do {
        MPI_Status status;
        int        msg_flag;

        /* Probe for an incoming message from any rank */
        if (MPI_SUCCESS != (mpi_code = MPI_Iprobe(MPI_ANY_SOURCE, H5D_CHUNK_MOD_DATA_TAG, io_info->comm,
                                                  &msg_flag, &status)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Iprobe failed", mpi_code)

        /*
         * If a message was found, allocate a buffer for the message and
         * post a non-blocking receive to receive it
         */
        if (msg_flag) {
#if H5_CHECK_MPI_VERSION(3, 0)
            MPI_Count msg_size = 0;

            if (MPI_SUCCESS != (mpi_code = MPI_Get_elements_x(&status, MPI_BYTE, &msg_size)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Get_elements_x failed", mpi_code)

            H5_CHECK_OVERFLOW(msg_size, MPI_Count, int);
#else
            int msg_size = 0;

            if (MPI_SUCCESS != (mpi_code = MPI_Get_elements(&status, MPI_BYTE, &msg_size)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Get_elements failed", mpi_code)
#endif

            if (msg_size <= 0)
                HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "invalid chunk modification message size");

            assert((num_recv_requests + 1) <= num_msgs_incoming);
            if (NULL ==
                (msg_recv_bufs[num_recv_requests] = H5MM_malloc((size_t)msg_size * sizeof(unsigned char))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                            "couldn't allocate chunk modification message receive buffer");

            if (MPI_SUCCESS != (mpi_code = MPI_Irecv(msg_recv_bufs[num_recv_requests], (int)msg_size,
                                                     MPI_BYTE, status.MPI_SOURCE, H5D_CHUNK_MOD_DATA_TAG,
                                                     io_info->comm, &recv_requests[num_recv_requests])))
                HMPI_GOTO_ERROR(FAIL, "MPI_Irecv failed", mpi_code)

            num_recv_requests++;
        }

        if (ibarrier_posted) {
            int ibarrier_completed;

            if (MPI_SUCCESS != (mpi_code = MPI_Test(&ibarrier, &ibarrier_completed, MPI_STATUS_IGNORE)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Test failed", mpi_code)

            if (ibarrier_completed)
                break;
        }
        else {
            int all_sends_completed;

            /* Determine if all send requests have completed
             *
             * gcc 11 complains about passing MPI_STATUSES_IGNORE as an MPI_Status
             * array. See the discussion here:
             *
             * https://github.com/pmodels/mpich/issues/5687
             */
            H5_GCC_DIAG_OFF("stringop-overflow")
            if (MPI_SUCCESS != (mpi_code = MPI_Testall((int)num_send_requests, send_requests,
                                                       &all_sends_completed, MPI_STATUSES_IGNORE)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Testall failed", mpi_code)
            H5_GCC_DIAG_ON("stringop-overflow")

            if (all_sends_completed) {
                /* Post non-blocking barrier */
                if (MPI_SUCCESS != (mpi_code = MPI_Ibarrier(io_info->comm, &ibarrier)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_Ibarrier failed", mpi_code)
                ibarrier_posted = true;

                /*
                 * Now that all send requests have completed, free up the
                 * send buffers used in the non-blocking operations
                 */
                if (msg_send_bufs) {
                    for (size_t i = 0; i < num_send_requests; i++) {
                        if (msg_send_bufs[i])
                            H5MM_free(msg_send_bufs[i]);
                    }

                    msg_send_bufs = H5MM_xfree(msg_send_bufs);
                }
            }
        }
    } while (1);

    /*
     * Ensure all receive requests have completed before moving on.
     * For linked-chunk I/O, more overlap with computation could
     * theoretically be achieved by returning the receive requests
     * array and postponing this wait until during chunk updating
     * when the data is really needed. However, multi-chunk I/O
     * only updates a chunk at a time and the messages may not come
     * in the order that chunks are processed. So, the safest way to
     * support both I/O modes is to simply make sure all messages
     * are available.
     *
     * gcc 11 complains about passing MPI_STATUSES_IGNORE as an MPI_Status
     * array. See the discussion here:
     *
     * https://github.com/pmodels/mpich/issues/5687
     */
    H5_GCC_DIAG_OFF("stringop-overflow")
    if (MPI_SUCCESS != (mpi_code = MPI_Waitall((int)num_recv_requests, recv_requests, MPI_STATUSES_IGNORE)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Waitall failed", mpi_code)
    H5_GCC_DIAG_ON("stringop-overflow")

    /* Set the new number of locally-selected chunks */
    chunk_list->num_chunk_infos = last_assigned_idx;

    /* Set chunk hash table information for future use */
    chunk_list->chunk_hash_table = chunk_table;

    /* Return chunk message buffers if any were received */
    *chunk_msg_bufs     = msg_recv_bufs;
    *chunk_msg_bufs_len = (int)num_recv_requests;

done:
    if (ret_value < 0) {
        /* If this rank failed, make sure to participate in collective barrier */
        if (!ibarrier_posted) {
            if (MPI_SUCCESS != (mpi_code = MPI_Ibarrier(io_info->comm, &ibarrier)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Ibarrier failed", mpi_code)
        }

        if (num_send_requests) {
            for (size_t i = 0; i < num_send_requests; i++) {
                MPI_Cancel(&send_requests[i]);
            }
        }

        if (recv_requests) {
            for (size_t i = 0; i < num_recv_requests; i++) {
                MPI_Cancel(&recv_requests[i]);
            }
        }

        if (msg_recv_bufs) {
            for (size_t i = 0; i < num_recv_requests; i++) {
                H5MM_free(msg_recv_bufs[i]);
            }

            H5MM_free(msg_recv_bufs);
        }

        HASH_CLEAR(hh, chunk_table);
    }

    if (recv_requests)
        H5MM_free(recv_requests);
    if (send_requests)
        H5MM_free(send_requests);

    if (msg_send_bufs) {
        for (size_t i = 0; i < num_send_requests; i++) {
            if (msg_send_bufs[i])
                H5MM_free(msg_send_bufs[i]);
        }

        H5MM_free(msg_send_bufs);
    }

    if (mem_iter) {
        if (mem_iter_init && H5S_SELECT_ITER_RELEASE(mem_iter) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "couldn't release dataspace selection iterator");
        mem_iter = H5FL_FREE(H5S_sel_iter_t, mem_iter);
    }

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TIME_STOP(mpi_rank);
    H5D_MPIO_TRACE_EXIT(mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
#else
    FUNC_ENTER_PACKAGE
    HERROR(
        H5E_DATASET, H5E_WRITEERROR,
        "unable to send chunk modification data between MPI ranks - MPI version < 3 (MPI_Ibarrier missing)")
    FUNC_LEAVE_NOAPI(FAIL)
#endif
} /* end H5D__mpio_share_chunk_modification_data() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_collective_filtered_chunk_read
 *
 * Purpose:     This routine coordinates a collective read across all ranks
 *              of the chunks they have selected. Each rank will then go
 *              and unfilter their read chunks as necessary and scatter
 *              the data into the provided read buffer.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_collective_filtered_chunk_read(H5D_filtered_collective_io_info_t *chunk_list,
                                         const H5D_io_info_t *io_info, size_t num_dset_infos, int mpi_rank)
{
    H5Z_EDC_t err_detect; /* Error detection info */
    H5Z_cb_t  filter_cb;  /* I/O filter callback function */
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(chunk_list);
    assert(io_info);

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TRACE_ENTER(mpi_rank);
    H5D_MPIO_TIME_START(mpi_rank, "Filtered collective chunk read");
#else
    (void)mpi_rank;
#endif

    /*
     * Allocate memory buffers for all chunks being read. Chunk data buffers are of
     * the largest size between the chunk's current filtered size and the chunk's true
     * size, as calculated by the number of elements in the chunk's file space extent
     * multiplied by the datatype size. This tries to ensure that:
     *
     *  * If we're reading the chunk and the filter normally reduces the chunk size,
     *    the unfiltering operation won't need to grow the buffer.
     *  * If we're reading the chunk and the filter normally grows the chunk size,
     *    we make sure to read into a buffer of size equal to the filtered chunk's
     *    size; reading into a (smaller) buffer of size equal to the unfiltered
     *    chunk size would of course be bad.
     */
    for (size_t info_idx = 0; info_idx < chunk_list->num_chunk_infos; info_idx++) {
        H5D_filtered_collective_chunk_info_t *chunk_entry = &chunk_list->chunk_infos[info_idx];
        H5D_mpio_filtered_dset_info_t        *cached_dset_info;
        hsize_t                               file_chunk_size;

        assert(chunk_entry->need_read);

        /* Find the cached dataset info for the dataset this chunk is in */
        if (num_dset_infos > 1) {
            HASH_FIND(hh, chunk_list->dset_info.dset_info_hash_table, &chunk_entry->index_info.dset_oloc_addr,
                      sizeof(haddr_t), cached_dset_info);
            if (cached_dset_info == NULL) {
                if (chunk_list->all_dset_indices_empty)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTFIND, FAIL, "unable to find cached dataset info entry");
                else {
                    /* Push an error, but participate in collective read */
                    HDONE_ERROR(H5E_DATASET, H5E_CANTFIND, FAIL, "unable to find cached dataset info entry");
                    break;
                }
            }
        }
        else
            cached_dset_info = chunk_list->dset_info.single_dset_info;
        assert(cached_dset_info);

        file_chunk_size = cached_dset_info->file_chunk_size;

        chunk_entry->chunk_buf_size = MAX(chunk_entry->chunk_current.length, file_chunk_size);

        if (NULL == (chunk_entry->buf = H5MM_malloc(chunk_entry->chunk_buf_size))) {
            if (chunk_list->all_dset_indices_empty)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate chunk data buffer");
            else {
                /* Push an error, but participate in collective read */
                HDONE_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate chunk data buffer");
                break;
            }
        }

        /*
         * Check whether the chunk needs to be read from the file, based
         * on whether the dataset's chunk index is empty or the chunk has
         * a defined address in the file. If the chunk doesn't need to be
         * read from the file, just fill the chunk buffer with the fill
         * value if necessary.
         */
        if (cached_dset_info->index_empty || !H5_addr_defined(chunk_entry->chunk_current.offset)) {
            chunk_entry->need_read = false;

            /* Update field keeping track of number of chunks to read */
            assert(chunk_list->num_chunks_to_read > 0);
            chunk_list->num_chunks_to_read--;
        }

        if (chunk_entry->need_read) {
            /* Set chunk's new length for eventual filter pipeline calls */
            if (chunk_entry->skip_filter_pline)
                chunk_entry->chunk_new.length = file_chunk_size;
            else
                chunk_entry->chunk_new.length = chunk_entry->chunk_current.length;
        }
        else {
            /* Set chunk's new length for eventual filter pipeline calls */
            chunk_entry->chunk_new.length = file_chunk_size;

            /* Determine if fill values should be "read" for this unallocated chunk */
            if (cached_dset_info->should_fill) {
                assert(cached_dset_info->fb_info_init);
                assert(cached_dset_info->fb_info.fill_buf);

                /* Write fill value to memory buffer */
                if (H5D__fill(cached_dset_info->fb_info.fill_buf,
                              cached_dset_info->dset_io_info->type_info.dset_type, chunk_entry->buf,
                              cached_dset_info->dset_io_info->type_info.mem_type,
                              cached_dset_info->fill_space) < 0) {
                    if (chunk_list->all_dset_indices_empty)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                    "couldn't fill chunk buffer with fill value");
                    else {
                        /* Push an error, but participate in collective read */
                        HDONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                    "couldn't fill chunk buffer with fill value");
                        break;
                    }
                }
            }
        }
    }

    /* Perform collective vector read if necessary */
    if (!chunk_list->all_dset_indices_empty)
        if (H5D__mpio_collective_filtered_vec_io(chunk_list, io_info->f_sh, H5D_IO_OP_READ) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "couldn't perform vector I/O on filtered chunks");

    if (chunk_list->num_chunk_infos) {
        /* Retrieve filter settings from API context */
        if (H5CX_get_err_detect(&err_detect) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get error detection info");
        if (H5CX_get_filter_cb(&filter_cb) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get I/O filter callback function");
    }

    /*
     * Iterate through all the read chunks, unfiltering them and scattering their
     * data out to the application's read buffer.
     */
    for (size_t info_idx = 0; info_idx < chunk_list->num_chunk_infos; info_idx++) {
        H5D_filtered_collective_chunk_info_t *chunk_entry = &chunk_list->chunk_infos[info_idx];
        H5D_piece_info_t                     *chunk_info  = chunk_entry->chunk_info;
        hsize_t                               iter_nelmts;

        /* Unfilter the chunk, unless we didn't read it from the file */
        if (chunk_entry->need_read && !chunk_entry->skip_filter_pline) {
            if (H5Z_pipeline(&chunk_info->dset_info->dset->shared->dcpl_cache.pline, H5Z_FLAG_REVERSE,
                             &(chunk_entry->index_info.filter_mask), err_detect, filter_cb,
                             (size_t *)&chunk_entry->chunk_new.length, &chunk_entry->chunk_buf_size,
                             &chunk_entry->buf) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFILTER, FAIL, "couldn't unfilter chunk for modifying");
        }

        /* Scatter the chunk data to the read buffer */
        iter_nelmts = H5S_GET_SELECT_NPOINTS(chunk_info->fspace);

        if (H5D_select_io_mem(chunk_info->dset_info->buf.vp, chunk_info->mspace, chunk_entry->buf,
                              chunk_info->fspace, chunk_info->dset_info->type_info.src_type_size,
                              (size_t)iter_nelmts) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "couldn't copy chunk data to read buffer");
    }

done:
    /* Free all resources used by entries in the chunk list */
    for (size_t info_idx = 0; info_idx < chunk_list->num_chunk_infos; info_idx++) {
        if (chunk_list->chunk_infos[info_idx].buf) {
            H5MM_free(chunk_list->chunk_infos[info_idx].buf);
            chunk_list->chunk_infos[info_idx].buf = NULL;
        }
    }

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TIME_STOP(mpi_rank);
    H5D_MPIO_TRACE_EXIT(mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_collective_filtered_chunk_read() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_collective_filtered_chunk_update
 *
 * Purpose:     When performing a parallel write on a chunked dataset with
 *              filters applied, all ranks must update their owned chunks
 *              with their own modification data and data from other ranks.
 *              This routine is responsible for coordinating that process.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_collective_filtered_chunk_update(H5D_filtered_collective_io_info_t *chunk_list,
                                           unsigned char **chunk_msg_bufs, int chunk_msg_bufs_len,
                                           const H5D_io_info_t *io_info, size_t num_dset_infos, int mpi_rank)
{
    H5S_sel_iter_t *sel_iter = NULL; /* Dataspace selection iterator for H5D__scatter_mem */
    H5Z_EDC_t       err_detect;      /* Error detection info */
    H5Z_cb_t        filter_cb;       /* I/O filter callback function */
    uint8_t        *key_buf       = NULL;
    H5S_t          *dataspace     = NULL;
    bool            sel_iter_init = false;
    herr_t          ret_value     = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(chunk_list);
    assert((chunk_msg_bufs && chunk_list->chunk_hash_table) || 0 == chunk_msg_bufs_len);
    assert(io_info);

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TRACE_ENTER(mpi_rank);
    H5D_MPIO_TIME_START(mpi_rank, "Filtered collective chunk update");
#endif

    /*
     * Allocate memory buffers for all owned chunks. Chunk data buffers are of the
     * largest size between the chunk's current filtered size and the chunk's true
     * size, as calculated by the number of elements in the chunk's file space extent
     * multiplied by the datatype size. This tries to ensure that:
     *
     *  * If we're fully overwriting the chunk and the filter normally reduces the
     *    chunk size, we simply have the exact buffer size required to hold the
     *    unfiltered chunk data.
     *  * If we're fully overwriting the chunk and the filter normally grows the
     *    chunk size (e.g., fletcher32 filter), the final filtering operation
     *    (hopefully) won't need to grow the buffer.
     *  * If we're reading the chunk and the filter normally reduces the chunk size,
     *    the unfiltering operation won't need to grow the buffer.
     *  * If we're reading the chunk and the filter normally grows the chunk size,
     *    we make sure to read into a buffer of size equal to the filtered chunk's
     *    size; reading into a (smaller) buffer of size equal to the unfiltered
     *    chunk size would of course be bad.
     */
    for (size_t info_idx = 0; info_idx < chunk_list->num_chunk_infos; info_idx++) {
        H5D_filtered_collective_chunk_info_t *chunk_entry = &chunk_list->chunk_infos[info_idx];
        H5D_mpio_filtered_dset_info_t        *cached_dset_info;
        hsize_t                               file_chunk_size;

        assert(mpi_rank == chunk_entry->new_owner);

        /* Find the cached dataset info for the dataset this chunk is in */
        if (num_dset_infos > 1) {
            HASH_FIND(hh, chunk_list->dset_info.dset_info_hash_table, &chunk_entry->index_info.dset_oloc_addr,
                      sizeof(haddr_t), cached_dset_info);
            if (cached_dset_info == NULL) {
                if (chunk_list->all_dset_indices_empty)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTFIND, FAIL, "unable to find cached dataset info entry");
                else {
                    /* Push an error, but participate in collective read */
                    HDONE_ERROR(H5E_DATASET, H5E_CANTFIND, FAIL, "unable to find cached dataset info entry");
                    break;
                }
            }
        }
        else
            cached_dset_info = chunk_list->dset_info.single_dset_info;
        assert(cached_dset_info);

        file_chunk_size = cached_dset_info->file_chunk_size;

        chunk_entry->chunk_buf_size = MAX(chunk_entry->chunk_current.length, file_chunk_size);

        /*
         * If this chunk hasn't been allocated yet and we aren't writing
         * out fill values to it, make sure to 0-fill its memory buffer
         * so we don't use uninitialized memory.
         */
        if (!H5_addr_defined(chunk_entry->chunk_current.offset) && !cached_dset_info->should_fill)
            chunk_entry->buf = H5MM_calloc(chunk_entry->chunk_buf_size);
        else
            chunk_entry->buf = H5MM_malloc(chunk_entry->chunk_buf_size);

        if (NULL == chunk_entry->buf) {
            if (chunk_list->all_dset_indices_empty)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate chunk data buffer");
            else {
                /* Push an error, but participate in collective read */
                HDONE_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate chunk data buffer");
                break;
            }
        }

        if (!chunk_entry->need_read)
            /* Set chunk's new length for eventual filter pipeline calls */
            chunk_entry->chunk_new.length = file_chunk_size;
        else {
            /*
             * Check whether the chunk needs to be read from the file, based
             * on whether the dataset's chunk index is empty or the chunk has
             * a defined address in the file. If the chunk doesn't need to be
             * read from the file, just fill the chunk buffer with the fill
             * value if necessary.
             */
            if (cached_dset_info->index_empty || !H5_addr_defined(chunk_entry->chunk_current.offset)) {
                chunk_entry->need_read = false;

                /* Update field keeping track of number of chunks to read */
                assert(chunk_list->num_chunks_to_read > 0);
                chunk_list->num_chunks_to_read--;
            }

            if (chunk_entry->need_read) {
                /* Set chunk's new length for eventual filter pipeline calls */
                if (chunk_entry->skip_filter_pline)
                    chunk_entry->chunk_new.length = file_chunk_size;
                else
                    chunk_entry->chunk_new.length = chunk_entry->chunk_current.length;
            }
            else {
                /* Set chunk's new length for eventual filter pipeline calls */
                chunk_entry->chunk_new.length = file_chunk_size;

                /* Determine if fill values should be "read" for this unallocated chunk */
                if (cached_dset_info->should_fill) {
                    assert(cached_dset_info->fb_info_init);
                    assert(cached_dset_info->fb_info.fill_buf);

                    /* Write fill value to memory buffer */
                    if (H5D__fill(cached_dset_info->fb_info.fill_buf,
                                  cached_dset_info->dset_io_info->type_info.dset_type, chunk_entry->buf,
                                  cached_dset_info->dset_io_info->type_info.mem_type,
                                  cached_dset_info->fill_space) < 0) {
                        if (chunk_list->all_dset_indices_empty)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                        "couldn't fill chunk buffer with fill value");
                        else {
                            /* Push an error, but participate in collective read */
                            HDONE_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                        "couldn't fill chunk buffer with fill value");
                            break;
                        }
                    }
                }
            }
        }
    }

    /* Perform collective vector read if necessary */
    if (!chunk_list->all_dset_indices_empty)
        if (H5D__mpio_collective_filtered_vec_io(chunk_list, io_info->f_sh, H5D_IO_OP_READ) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "couldn't perform vector I/O on filtered chunks");

    /*
     * Now that all owned chunks have been read, update the chunks
     * with modification data from the owning rank and other ranks.
     */

    if (chunk_list->num_chunk_infos > 0) {
        /* Retrieve filter settings from API context */
        if (H5CX_get_err_detect(&err_detect) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get error detection info");
        if (H5CX_get_filter_cb(&filter_cb) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get I/O filter callback function");
    }

    /* Process all chunks with data from the owning rank first */
    for (size_t info_idx = 0; info_idx < chunk_list->num_chunk_infos; info_idx++) {
        H5D_filtered_collective_chunk_info_t *chunk_entry = &chunk_list->chunk_infos[info_idx];
        H5D_piece_info_t                     *chunk_info  = chunk_entry->chunk_info;
        hsize_t                               iter_nelmts;

        assert(mpi_rank == chunk_entry->new_owner);

        /*
         * If this chunk wasn't being fully overwritten, we read it from
         * the file, so we need to unfilter it
         */
        if (chunk_entry->need_read && !chunk_entry->skip_filter_pline) {
            if (H5Z_pipeline(&chunk_info->dset_info->dset->shared->dcpl_cache.pline, H5Z_FLAG_REVERSE,
                             &(chunk_entry->index_info.filter_mask), err_detect, filter_cb,
                             (size_t *)&chunk_entry->chunk_new.length, &chunk_entry->chunk_buf_size,
                             &chunk_entry->buf) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFILTER, FAIL, "couldn't unfilter chunk for modifying");
        }

        iter_nelmts = H5S_GET_SELECT_NPOINTS(chunk_info->mspace);

        if (H5D_select_io_mem(chunk_entry->buf, chunk_info->fspace, chunk_info->dset_info->buf.cvp,
                              chunk_info->mspace, chunk_info->dset_info->type_info.dst_type_size,
                              (size_t)iter_nelmts) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "couldn't copy chunk data to write buffer");
    }

    /* Allocate iterator for memory selection */
    if (chunk_msg_bufs_len > 0) {
        assert(chunk_list->chunk_hash_table_keylen > 0);
        if (NULL == (key_buf = H5MM_malloc(chunk_list->chunk_hash_table_keylen)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate hash table key buffer");

        if (NULL == (sel_iter = H5FL_MALLOC(H5S_sel_iter_t)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate memory iterator");
    }

    /* Now process all received chunk message buffers */
    for (size_t buf_idx = 0; buf_idx < (size_t)chunk_msg_bufs_len; buf_idx++) {
        H5D_filtered_collective_chunk_info_t *chunk_entry = NULL;
        const unsigned char                  *msg_ptr     = chunk_msg_bufs[buf_idx];

        if (msg_ptr) {
            /* Retrieve the chunk hash table key from the chunk message buffer */
            H5MM_memcpy(key_buf, msg_ptr, chunk_list->chunk_hash_table_keylen);
            msg_ptr += chunk_list->chunk_hash_table_keylen;

            /* Find the chunk entry according to its chunk hash table key */
            HASH_FIND(hh, chunk_list->chunk_hash_table, key_buf, chunk_list->chunk_hash_table_keylen,
                      chunk_entry);
            if (chunk_entry == NULL)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFIND, FAIL, "unable to find chunk entry");
            if (mpi_rank != chunk_entry->new_owner)
                HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "chunk owner set to incorrect MPI rank");

            /*
             * Only process the chunk if its data buffer is allocated.
             * In the case of multi-chunk I/O, we're only working on
             * a chunk at a time, so we need to skip over messages
             * that aren't for the chunk we're currently working on.
             */
            if (!chunk_entry->buf)
                continue;
            else {
                hsize_t iter_nelmts;

                /* Decode the chunk file dataspace from the message */
                if (NULL == (dataspace = H5S_decode(&msg_ptr)))
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTDECODE, FAIL, "unable to decode dataspace");

                if (H5S_select_iter_init(sel_iter, dataspace,
                                         chunk_entry->chunk_info->dset_info->type_info.dst_type_size,
                                         H5S_SEL_ITER_SHARE_WITH_DATASPACE) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                "unable to initialize memory selection information");
                sel_iter_init = true;

                iter_nelmts = H5S_GET_SELECT_NPOINTS(dataspace);

                /* Update the chunk data with the received modification data */
                if (H5D__scatter_mem(msg_ptr, sel_iter, (size_t)iter_nelmts, chunk_entry->buf) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "couldn't scatter to write buffer");

                if (H5S_SELECT_ITER_RELEASE(sel_iter) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "couldn't release selection iterator");
                sel_iter_init = false;

                if (dataspace) {
                    if (H5S_close(dataspace) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "can't close dataspace");
                    dataspace = NULL;
                }

                H5MM_free(chunk_msg_bufs[buf_idx]);
                chunk_msg_bufs[buf_idx] = NULL;
            }
        }
    }

    /* Finally, filter all the chunks */
    for (size_t info_idx = 0; info_idx < chunk_list->num_chunk_infos; info_idx++) {
        if (!chunk_list->chunk_infos[info_idx].skip_filter_pline) {
            if (H5Z_pipeline(
                    &chunk_list->chunk_infos[info_idx].chunk_info->dset_info->dset->shared->dcpl_cache.pline,
                    0, &(chunk_list->chunk_infos[info_idx].index_info.filter_mask), err_detect, filter_cb,
                    (size_t *)&chunk_list->chunk_infos[info_idx].chunk_new.length,
                    &chunk_list->chunk_infos[info_idx].chunk_buf_size,
                    &chunk_list->chunk_infos[info_idx].buf) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_CANTFILTER, FAIL, "output pipeline failed");
        }

#if H5_SIZEOF_SIZE_T > 4
        /* Check for the chunk expanding too much to encode in a 32-bit value */
        if (chunk_list->chunk_infos[info_idx].chunk_new.length > ((size_t)0xffffffff))
            HGOTO_ERROR(H5E_DATASET, H5E_BADRANGE, FAIL, "chunk too large for 32-bit length");
#endif
    }

done:
    if (dataspace && (H5S_close(dataspace) < 0))
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "can't close dataspace");

    if (sel_iter) {
        if (sel_iter_init && H5S_SELECT_ITER_RELEASE(sel_iter) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "couldn't release selection iterator");
        sel_iter = H5FL_FREE(H5S_sel_iter_t, sel_iter);
    }

    H5MM_free(key_buf);

    /* On failure, try to free all resources used by entries in the chunk list */
    if (ret_value < 0) {
        for (size_t info_idx = 0; info_idx < chunk_list->num_chunk_infos; info_idx++) {
            if (chunk_list->chunk_infos[info_idx].buf) {
                H5MM_free(chunk_list->chunk_infos[info_idx].buf);
                chunk_list->chunk_infos[info_idx].buf = NULL;
            }
        }
    }

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TIME_STOP(mpi_rank);
    H5D_MPIO_TRACE_EXIT(mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_collective_filtered_chunk_update() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_collective_filtered_chunk_reallocate
 *
 * Purpose:     When performing a parallel write on a chunked dataset with
 *              filters applied, all ranks must eventually get together and
 *              perform a collective reallocation of space in the file for
 *              all chunks that were modified on all ranks. This routine is
 *              responsible for coordinating that process.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_collective_filtered_chunk_reallocate(H5D_filtered_collective_io_info_t *chunk_list,
                                               size_t *num_chunks_assigned_map, H5D_io_info_t *io_info,
                                               size_t num_dset_infos, int mpi_rank, int mpi_size)
{
    H5D_chunk_alloc_info_t *collective_list = NULL;
    MPI_Datatype            send_type;
    MPI_Datatype            recv_type;
    bool                    send_type_derived          = false;
    bool                    recv_type_derived          = false;
    bool                    need_sort                  = false;
    size_t                  collective_num_entries     = 0;
    size_t                  num_local_chunks_processed = 0;
    void                   *gathered_array             = NULL;
    int                    *counts_disps_array         = NULL;
    int                    *counts_ptr                 = NULL;
    int                    *displacements_ptr          = NULL;
    int                     mpi_code;
    herr_t                  ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(chunk_list);
    assert(io_info);

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TRACE_ENTER(mpi_rank);
    H5D_MPIO_TIME_START(mpi_rank, "Reallocation of chunk file space");
#endif

    /*
     * Make sure it's safe to cast this rank's number
     * of chunks to be sent into an int for MPI
     */
    H5_CHECK_OVERFLOW(chunk_list->num_chunk_infos, size_t, int);

    /* Create derived datatypes for the chunk file space info needed */
    if (H5D__mpio_get_chunk_alloc_info_types(&recv_type, &recv_type_derived, &send_type, &send_type_derived) <
        0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                    "can't create derived datatypes for chunk file space info");

    /*
     * Gather the new chunk sizes to all ranks for a collective reallocation
     * of the chunks in the file.
     */
    if (num_chunks_assigned_map) {
        /*
         * If a mapping between rank value -> number of assigned chunks has
         * been provided (usually during linked-chunk I/O), we can use this
         * to optimize MPI overhead a bit since MPI ranks won't need to
         * first inform each other about how many chunks they're contributing.
         */
        if (NULL == (counts_disps_array = H5MM_malloc(2 * (size_t)mpi_size * sizeof(*counts_disps_array)))) {
            /* Push an error, but still participate in collective gather operation */
            HDONE_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                        "couldn't allocate receive counts and displacements array");
        }
        else {
            /* Set the receive counts from the assigned chunks map */
            counts_ptr = counts_disps_array;

            for (int curr_rank = 0; curr_rank < mpi_size; curr_rank++)
                H5_CHECKED_ASSIGN(counts_ptr[curr_rank], int, num_chunks_assigned_map[curr_rank], size_t);

            /* Set the displacements into the receive buffer for the gather operation */
            displacements_ptr = &counts_disps_array[mpi_size];

            *displacements_ptr = 0;
            for (int curr_rank = 1; curr_rank < mpi_size; curr_rank++)
                displacements_ptr[curr_rank] = displacements_ptr[curr_rank - 1] + counts_ptr[curr_rank - 1];
        }

        /* Perform gather operation */
        if (H5_mpio_gatherv_alloc(chunk_list->chunk_infos, (int)chunk_list->num_chunk_infos, send_type,
                                  counts_ptr, displacements_ptr, recv_type, true, 0, io_info->comm, mpi_rank,
                                  mpi_size, &gathered_array, &collective_num_entries) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGATHER, FAIL,
                        "can't gather chunk file space info to/from ranks");
    }
    else {
        /*
         * If no mapping between rank value -> number of assigned chunks has
         * been provided (usually during multi-chunk I/O), all MPI ranks will
         * need to first inform other ranks about how many chunks they're
         * contributing before performing the actual gather operation. Use
         * the 'simple' MPI_Allgatherv wrapper for this.
         */
        if (H5_mpio_gatherv_alloc_simple(chunk_list->chunk_infos, (int)chunk_list->num_chunk_infos, send_type,
                                         recv_type, true, 0, io_info->comm, mpi_rank, mpi_size,
                                         &gathered_array, &collective_num_entries) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGATHER, FAIL,
                        "can't gather chunk file space info to/from ranks");
    }

    /* Collectively re-allocate the modified chunks (from each rank) in the file */
    collective_list            = (H5D_chunk_alloc_info_t *)gathered_array;
    num_local_chunks_processed = 0;
    for (size_t entry_idx = 0; entry_idx < collective_num_entries; entry_idx++) {
        H5D_mpio_filtered_dset_info_t *cached_dset_info;
        H5D_chunk_alloc_info_t        *coll_entry = &collective_list[entry_idx];
        bool                           need_insert;
        bool                           update_local_chunk;

        /* Find the cached dataset info for the dataset this chunk is in */
        if (num_dset_infos > 1) {
            HASH_FIND(hh, chunk_list->dset_info.dset_info_hash_table, &coll_entry->dset_oloc_addr,
                      sizeof(haddr_t), cached_dset_info);
            if (cached_dset_info == NULL)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFIND, FAIL, "unable to find cached dataset info entry");
        }
        else
            cached_dset_info = chunk_list->dset_info.single_dset_info;
        assert(cached_dset_info);

        if (H5D__chunk_file_alloc(&cached_dset_info->chunk_idx_info, &coll_entry->chunk_current,
                                  &coll_entry->chunk_new, &need_insert, NULL) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "unable to allocate chunk");

        /*
         * If we just re-allocated a chunk that is local to this
         * rank, make sure to update the chunk entry in the local
         * chunk list
         */
        update_local_chunk =
            (num_local_chunks_processed < chunk_list->num_chunk_infos) &&
            (coll_entry->dset_oloc_addr ==
             chunk_list->chunk_infos[num_local_chunks_processed].index_info.dset_oloc_addr) &&
            (coll_entry->chunk_idx ==
             chunk_list->chunk_infos[num_local_chunks_processed].index_info.chunk_idx);

        if (update_local_chunk) {
            H5D_filtered_collective_chunk_info_t *local_chunk;

            local_chunk = &chunk_list->chunk_infos[num_local_chunks_processed];

            /* Sanity check that this chunk is actually local */
            assert(mpi_rank == local_chunk->orig_owner);
            assert(mpi_rank == local_chunk->new_owner);

            local_chunk->chunk_new              = coll_entry->chunk_new;
            local_chunk->index_info.need_insert = need_insert;

            /*
             * Since chunk reallocation can move chunks around, check if
             * the local chunk list is still in ascending offset of order
             * in the file
             */
            if (num_local_chunks_processed) {
                haddr_t curr_chunk_offset = local_chunk->chunk_new.offset;
                haddr_t prev_chunk_offset =
                    chunk_list->chunk_infos[num_local_chunks_processed - 1].chunk_new.offset;

                assert(H5_addr_defined(prev_chunk_offset) && H5_addr_defined(curr_chunk_offset));
                if (curr_chunk_offset < prev_chunk_offset)
                    need_sort = true;
            }

            num_local_chunks_processed++;
        }
    }

    assert(chunk_list->num_chunk_infos == num_local_chunks_processed);

    /*
     * Ensure this rank's local chunk list is sorted in
     * ascending order of offset in the file
     */
    if (need_sort)
        qsort(chunk_list->chunk_infos, chunk_list->num_chunk_infos,
              sizeof(H5D_filtered_collective_chunk_info_t), H5D__cmp_filtered_collective_io_info_entry);

done:
    H5MM_free(gathered_array);
    H5MM_free(counts_disps_array);

    if (send_type_derived) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&send_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    }
    if (recv_type_derived) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&recv_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    }

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TIME_STOP(mpi_rank);
    H5D_MPIO_TRACE_EXIT(mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__mpio_collective_filtered_chunk_reallocate() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_collective_filtered_chunk_reinsert
 *
 * Purpose:     When performing a parallel write on a chunked dataset with
 *              filters applied, all ranks must eventually get together and
 *              perform a collective reinsertion into the dataset's chunk
 *              index of chunks that were modified. This routine is
 *              responsible for coordinating that process.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_collective_filtered_chunk_reinsert(H5D_filtered_collective_io_info_t *chunk_list,
                                             size_t *num_chunks_assigned_map, H5D_io_info_t *io_info,
                                             size_t num_dset_infos, int mpi_rank, int mpi_size)
{
    MPI_Datatype send_type;
    MPI_Datatype recv_type;
    size_t       collective_num_entries = 0;
    bool         send_type_derived      = false;
    bool         recv_type_derived      = false;
    void        *gathered_array         = NULL;
    int         *counts_disps_array     = NULL;
    int         *counts_ptr             = NULL;
    int         *displacements_ptr      = NULL;
    int          mpi_code;
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(chunk_list);
    assert(io_info);

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TRACE_ENTER(mpi_rank);
    H5D_MPIO_TIME_START(mpi_rank, "Reinsertion of modified chunks into chunk index");
#endif

    /*
     * If no datasets involved have a chunk index 'insert'
     * operation, this function is a no-op
     */
    if (chunk_list->no_dset_index_insert_methods)
        HGOTO_DONE(SUCCEED);

    /*
     * Make sure it's safe to cast this rank's number
     * of chunks to be sent into an int for MPI
     */
    H5_CHECK_OVERFLOW(chunk_list->num_chunk_infos, size_t, int);

    /* Create derived datatypes for the chunk re-insertion info needed */
    if (H5D__mpio_get_chunk_insert_info_types(&recv_type, &recv_type_derived, &send_type,
                                              &send_type_derived) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                    "can't create derived datatypes for chunk re-insertion info");

    /*
     * Gather information to all ranks for a collective re-insertion
     * of the modified chunks into the chunk index
     */
    if (num_chunks_assigned_map) {
        /*
         * If a mapping between rank value -> number of assigned chunks has
         * been provided (usually during linked-chunk I/O), we can use this
         * to optimize MPI overhead a bit since MPI ranks won't need to
         * first inform each other about how many chunks they're contributing.
         */
        if (NULL == (counts_disps_array = H5MM_malloc(2 * (size_t)mpi_size * sizeof(*counts_disps_array)))) {
            /* Push an error, but still participate in collective gather operation */
            HDONE_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                        "couldn't allocate receive counts and displacements array");
        }
        else {
            /* Set the receive counts from the assigned chunks map */
            counts_ptr = counts_disps_array;

            for (int curr_rank = 0; curr_rank < mpi_size; curr_rank++)
                H5_CHECKED_ASSIGN(counts_ptr[curr_rank], int, num_chunks_assigned_map[curr_rank], size_t);

            /* Set the displacements into the receive buffer for the gather operation */
            displacements_ptr = &counts_disps_array[mpi_size];

            *displacements_ptr = 0;
            for (int curr_rank = 1; curr_rank < mpi_size; curr_rank++)
                displacements_ptr[curr_rank] = displacements_ptr[curr_rank - 1] + counts_ptr[curr_rank - 1];
        }

        /* Perform gather operation */
        if (H5_mpio_gatherv_alloc(chunk_list->chunk_infos, (int)chunk_list->num_chunk_infos, send_type,
                                  counts_ptr, displacements_ptr, recv_type, true, 0, io_info->comm, mpi_rank,
                                  mpi_size, &gathered_array, &collective_num_entries) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGATHER, FAIL,
                        "can't gather chunk index re-insertion info to/from ranks");
    }
    else {
        /*
         * If no mapping between rank value -> number of assigned chunks has
         * been provided (usually during multi-chunk I/O), all MPI ranks will
         * need to first inform other ranks about how many chunks they're
         * contributing before performing the actual gather operation. Use
         * the 'simple' MPI_Allgatherv wrapper for this.
         */
        if (H5_mpio_gatherv_alloc_simple(chunk_list->chunk_infos, (int)chunk_list->num_chunk_infos, send_type,
                                         recv_type, true, 0, io_info->comm, mpi_rank, mpi_size,
                                         &gathered_array, &collective_num_entries) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGATHER, FAIL,
                        "can't gather chunk index re-insertion info to/from ranks");
    }

    for (size_t entry_idx = 0; entry_idx < collective_num_entries; entry_idx++) {
        H5D_mpio_filtered_dset_info_t *cached_dset_info;
        H5D_chunk_insert_info_t       *coll_entry = &((H5D_chunk_insert_info_t *)gathered_array)[entry_idx];
        H5D_chunk_ud_t                 chunk_ud;
        haddr_t                        prev_tag = HADDR_UNDEF;
        hsize_t                        scaled_coords[H5O_LAYOUT_NDIMS];

        /*
         * We only need to reinsert this chunk if we had to actually
         * allocate or reallocate space in the file for it
         */
        if (!coll_entry->index_info.need_insert)
            continue;

        /* Find the cached dataset info for the dataset this chunk is in */
        if (num_dset_infos > 1) {
            HASH_FIND(hh, chunk_list->dset_info.dset_info_hash_table, &coll_entry->index_info.dset_oloc_addr,
                      sizeof(haddr_t), cached_dset_info);
            if (cached_dset_info == NULL)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFIND, FAIL, "unable to find cached dataset info entry");
        }
        else
            cached_dset_info = chunk_list->dset_info.single_dset_info;
        assert(cached_dset_info);

        chunk_ud.common.layout  = cached_dset_info->chunk_idx_info.layout;
        chunk_ud.common.storage = cached_dset_info->chunk_idx_info.storage;
        chunk_ud.common.scaled  = scaled_coords;

        chunk_ud.chunk_block = coll_entry->chunk_block;
        chunk_ud.chunk_idx   = coll_entry->index_info.chunk_idx;
        chunk_ud.filter_mask = coll_entry->index_info.filter_mask;

        /* Calculate scaled coordinates for the chunk */
        if (cached_dset_info->chunk_idx_info.layout->idx_type == H5D_CHUNK_IDX_EARRAY &&
            cached_dset_info->chunk_idx_info.layout->u.earray.unlim_dim > 0) {
            /*
             * Extensible arrays where the unlimited dimension is not
             * the slowest-changing dimension "swizzle" the coordinates
             * to move the unlimited dimension value to offset 0. Therefore,
             * we use the "swizzled" down chunks to calculate the "swizzled"
             * scaled coordinates and then we undo the "swizzle" operation.
             *
             * TODO: In the future, this is something that should be handled
             *       by the particular chunk index rather than manually
             *       here. Likely, the chunk index ops should get a new
             *       callback that accepts a chunk index and provides the
             *       caller with the scaled coordinates for that chunk.
             */
            H5VM_array_calc_pre(chunk_ud.chunk_idx, cached_dset_info->dset_io_info->dset->shared->ndims,
                                cached_dset_info->chunk_idx_info.layout->u.earray.swizzled_down_chunks,
                                scaled_coords);

            H5VM_unswizzle_coords(hsize_t, scaled_coords,
                                  cached_dset_info->chunk_idx_info.layout->u.earray.unlim_dim);
        }
        else {
            H5VM_array_calc_pre(chunk_ud.chunk_idx, cached_dset_info->dset_io_info->dset->shared->ndims,
                                cached_dset_info->dset_io_info->dset->shared->layout.u.chunk.down_chunks,
                                scaled_coords);
        }

        scaled_coords[cached_dset_info->dset_io_info->dset->shared->ndims] = 0;

#ifndef NDEBUG
        /*
         * If a matching local chunk entry is found, the
         * `chunk_info` structure (which contains the chunk's
         * pre-computed scaled coordinates) will be valid
         * for this rank. Compare those coordinates against
         * the calculated coordinates above to make sure
         * they match.
         */
        for (size_t dbg_idx = 0; dbg_idx < chunk_list->num_chunk_infos; dbg_idx++) {
            bool same_chunk;

            /* Chunks must have the same index and reside in the same dataset */
            same_chunk = (0 == H5_addr_cmp(coll_entry->index_info.dset_oloc_addr,
                                           chunk_list->chunk_infos[dbg_idx].index_info.dset_oloc_addr));
            same_chunk = same_chunk && (coll_entry->index_info.chunk_idx ==
                                        chunk_list->chunk_infos[dbg_idx].index_info.chunk_idx);

            if (same_chunk) {
                bool coords_match =
                    !memcmp(scaled_coords, chunk_list->chunk_infos[dbg_idx].chunk_info->scaled,
                            cached_dset_info->dset_io_info->dset->shared->ndims * sizeof(hsize_t));

                assert(coords_match && "Calculated scaled coordinates for chunk didn't match "
                                       "chunk's actual scaled coordinates!");
                break;
            }
        }
#endif

        /* Set metadata tagging with dataset oheader addr */
        H5AC_tag(cached_dset_info->dset_io_info->dset->oloc.addr, &prev_tag);

        if ((cached_dset_info->chunk_idx_info.storage->ops->insert)(
                &cached_dset_info->chunk_idx_info, &chunk_ud, cached_dset_info->dset_io_info->dset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL, "unable to insert chunk address into index");

        /* Reset metadata tagging */
        H5AC_tag(prev_tag, NULL);
    }

done:
    H5MM_free(gathered_array);
    H5MM_free(counts_disps_array);

    if (send_type_derived) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&send_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    }
    if (recv_type_derived) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&recv_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    }

#ifdef H5Dmpio_DEBUG
    H5D_MPIO_TIME_STOP(mpi_rank);
    H5D_MPIO_TRACE_EXIT(mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_collective_filtered_chunk_reinsert() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_get_chunk_redistribute_info_types
 *
 * Purpose:     Constructs MPI derived datatypes for communicating the
 *              info from a H5D_filtered_collective_chunk_info_t structure
 *              that is necessary for redistributing shared chunks during a
 *              collective write of filtered chunks.
 *
 *              The datatype returned through `contig_type` has an extent
 *              equal to the size of an H5D_chunk_redistribute_info_t
 *              structure and is suitable for communicating that structure
 *              type.
 *
 *              The datatype returned through `resized_type` has an extent
 *              equal to the size of an H5D_filtered_collective_chunk_info_t
 *              structure. This makes it suitable for sending an array of
 *              those structures, while extracting out just the info
 *              necessary for the chunk redistribution operation during
 *              communication.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_get_chunk_redistribute_info_types(MPI_Datatype *contig_type, bool *contig_type_derived,
                                            MPI_Datatype *resized_type, bool *resized_type_derived)
{
    MPI_Datatype struct_type              = MPI_DATATYPE_NULL;
    bool         struct_type_derived      = false;
    MPI_Datatype chunk_block_type         = MPI_DATATYPE_NULL;
    bool         chunk_block_type_derived = false;
    MPI_Datatype types[6];
    MPI_Aint     displacements[6];
    int          block_lengths[6];
    int          field_count;
    int          mpi_code;
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(contig_type);
    assert(contig_type_derived);
    assert(resized_type);
    assert(resized_type_derived);

    *contig_type_derived  = false;
    *resized_type_derived = false;

    /* Create struct type for the inner H5F_block_t structure */
    if (H5F_mpi_get_file_block_type(false, &chunk_block_type, &chunk_block_type_derived) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't create derived type for chunk file description");

    field_count = 6;
    assert(field_count == (sizeof(types) / sizeof(MPI_Datatype)));

    /*
     * Create structure type to pack chunk H5F_block_t structure
     * next to chunk_idx, dset_oloc_addr, orig_owner, new_owner
     * and num_writers fields
     */
    block_lengths[0] = 1;
    block_lengths[1] = 1;
    block_lengths[2] = 1;
    block_lengths[3] = 1;
    block_lengths[4] = 1;
    block_lengths[5] = 1;
    displacements[0] = offsetof(H5D_chunk_redistribute_info_t, chunk_block);
    displacements[1] = offsetof(H5D_chunk_redistribute_info_t, chunk_idx);
    displacements[2] = offsetof(H5D_chunk_redistribute_info_t, dset_oloc_addr);
    displacements[3] = offsetof(H5D_chunk_redistribute_info_t, orig_owner);
    displacements[4] = offsetof(H5D_chunk_redistribute_info_t, new_owner);
    displacements[5] = offsetof(H5D_chunk_redistribute_info_t, num_writers);
    types[0]         = chunk_block_type;
    types[1]         = HSIZE_AS_MPI_TYPE;
    types[2]         = HADDR_AS_MPI_TYPE;
    types[3]         = MPI_INT;
    types[4]         = MPI_INT;
    types[5]         = MPI_INT;
    if (MPI_SUCCESS !=
        (mpi_code = MPI_Type_create_struct(field_count, block_lengths, displacements, types, contig_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct failed", mpi_code)
    *contig_type_derived = true;

    if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(contig_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)

    /* Create struct type to extract the chunk_current, chunk_idx,
     * dset_oloc_addr, orig_owner, new_owner and num_writers fields
     * from a H5D_filtered_collective_chunk_info_t structure
     */
    block_lengths[0] = 1;
    block_lengths[1] = 1;
    block_lengths[2] = 1;
    block_lengths[3] = 1;
    block_lengths[4] = 1;
    block_lengths[5] = 1;
    displacements[0] = offsetof(H5D_filtered_collective_chunk_info_t, chunk_current);
    displacements[1] = offsetof(H5D_filtered_collective_chunk_info_t, index_info.chunk_idx);
    displacements[2] = offsetof(H5D_filtered_collective_chunk_info_t, index_info.dset_oloc_addr);
    displacements[3] = offsetof(H5D_filtered_collective_chunk_info_t, orig_owner);
    displacements[4] = offsetof(H5D_filtered_collective_chunk_info_t, new_owner);
    displacements[5] = offsetof(H5D_filtered_collective_chunk_info_t, num_writers);
    types[0]         = chunk_block_type;
    types[1]         = HSIZE_AS_MPI_TYPE;
    types[2]         = HADDR_AS_MPI_TYPE;
    types[3]         = MPI_INT;
    types[4]         = MPI_INT;
    types[5]         = MPI_INT;
    if (MPI_SUCCESS !=
        (mpi_code = MPI_Type_create_struct(field_count, block_lengths, displacements, types, &struct_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct failed", mpi_code)
    struct_type_derived = true;

    if (MPI_SUCCESS != (mpi_code = MPI_Type_create_resized(
                            struct_type, 0, sizeof(H5D_filtered_collective_chunk_info_t), resized_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_resized failed", mpi_code)
    *resized_type_derived = true;

    if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(resized_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)

done:
    if (struct_type_derived) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&struct_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    }
    if (chunk_block_type_derived) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&chunk_block_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    }

    if (ret_value < 0) {
        if (*resized_type_derived) {
            if (MPI_SUCCESS != (mpi_code = MPI_Type_free(resized_type)))
                HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
            *resized_type_derived = false;
        }
        if (*contig_type_derived) {
            if (MPI_SUCCESS != (mpi_code = MPI_Type_free(contig_type)))
                HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
            *contig_type_derived = false;
        }
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_get_chunk_redistribute_info_types() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_get_chunk_alloc_info_types
 *
 * Purpose:     Constructs MPI derived datatypes for communicating the info
 *              from a H5D_filtered_collective_chunk_info_t structure that
 *              is necessary for re-allocating file space during a
 *              collective write of filtered chunks.
 *
 *              The datatype returned through `contig_type` has an extent
 *              equal to the size of an H5D_chunk_alloc_info_t structure
 *              and is suitable for communicating that structure type.
 *
 *              The datatype returned through `resized_type` has an extent
 *              equal to the size of an H5D_filtered_collective_chunk_info_t
 *              structure. This makes it suitable for sending an array of
 *              those structures, while extracting out just the info
 *              necessary for the chunk file space reallocation operation
 *              during communication.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_get_chunk_alloc_info_types(MPI_Datatype *contig_type, bool *contig_type_derived,
                                     MPI_Datatype *resized_type, bool *resized_type_derived)
{
    MPI_Datatype struct_type              = MPI_DATATYPE_NULL;
    bool         struct_type_derived      = false;
    MPI_Datatype chunk_block_type         = MPI_DATATYPE_NULL;
    bool         chunk_block_type_derived = false;
    MPI_Datatype types[4];
    MPI_Aint     displacements[4];
    int          block_lengths[4];
    int          field_count;
    int          mpi_code;
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(contig_type);
    assert(contig_type_derived);
    assert(resized_type);
    assert(resized_type_derived);

    *contig_type_derived  = false;
    *resized_type_derived = false;

    /* Create struct type for the inner H5F_block_t structure */
    if (H5F_mpi_get_file_block_type(false, &chunk_block_type, &chunk_block_type_derived) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't create derived type for chunk file description");

    field_count = 4;
    assert(field_count == (sizeof(types) / sizeof(MPI_Datatype)));

    /*
     * Create structure type to pack both chunk H5F_block_t structures
     * next to chunk_idx and dset_oloc_addr fields
     */
    block_lengths[0] = 1;
    block_lengths[1] = 1;
    block_lengths[2] = 1;
    block_lengths[3] = 1;
    displacements[0] = offsetof(H5D_chunk_alloc_info_t, chunk_current);
    displacements[1] = offsetof(H5D_chunk_alloc_info_t, chunk_new);
    displacements[2] = offsetof(H5D_chunk_alloc_info_t, chunk_idx);
    displacements[3] = offsetof(H5D_chunk_alloc_info_t, dset_oloc_addr);
    types[0]         = chunk_block_type;
    types[1]         = chunk_block_type;
    types[2]         = HSIZE_AS_MPI_TYPE;
    types[3]         = HADDR_AS_MPI_TYPE;
    if (MPI_SUCCESS !=
        (mpi_code = MPI_Type_create_struct(field_count, block_lengths, displacements, types, contig_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct failed", mpi_code)
    *contig_type_derived = true;

    if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(contig_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)

    /*
     * Create struct type to extract the chunk_current, chunk_new, chunk_idx
     * and dset_oloc_addr fields from a H5D_filtered_collective_chunk_info_t
     * structure
     */
    block_lengths[0] = 1;
    block_lengths[1] = 1;
    block_lengths[2] = 1;
    block_lengths[3] = 1;
    displacements[0] = offsetof(H5D_filtered_collective_chunk_info_t, chunk_current);
    displacements[1] = offsetof(H5D_filtered_collective_chunk_info_t, chunk_new);
    displacements[2] = offsetof(H5D_filtered_collective_chunk_info_t, index_info.chunk_idx);
    displacements[3] = offsetof(H5D_filtered_collective_chunk_info_t, index_info.dset_oloc_addr);
    types[0]         = chunk_block_type;
    types[1]         = chunk_block_type;
    types[2]         = HSIZE_AS_MPI_TYPE;
    types[3]         = HADDR_AS_MPI_TYPE;
    if (MPI_SUCCESS !=
        (mpi_code = MPI_Type_create_struct(field_count, block_lengths, displacements, types, &struct_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct failed", mpi_code)
    struct_type_derived = true;

    if (MPI_SUCCESS != (mpi_code = MPI_Type_create_resized(
                            struct_type, 0, sizeof(H5D_filtered_collective_chunk_info_t), resized_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_resized failed", mpi_code)
    *resized_type_derived = true;

    if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(resized_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)

done:
    if (struct_type_derived) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&struct_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    }
    if (chunk_block_type_derived) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&chunk_block_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    }

    if (ret_value < 0) {
        if (*resized_type_derived) {
            if (MPI_SUCCESS != (mpi_code = MPI_Type_free(resized_type)))
                HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
            *resized_type_derived = false;
        }
        if (*contig_type_derived) {
            if (MPI_SUCCESS != (mpi_code = MPI_Type_free(contig_type)))
                HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
            *contig_type_derived = false;
        }
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_get_chunk_alloc_info_types() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_get_chunk_insert_info_types
 *
 * Purpose:     Constructs MPI derived datatypes for communicating the
 *              information necessary when reinserting chunks into a
 *              dataset's chunk index. This includes the chunk's new offset
 *              and size (H5F_block_t) and the inner `index_info` structure
 *              of a H5D_filtered_collective_chunk_info_t structure.
 *
 *              The datatype returned through `contig_type` has an extent
 *              equal to the size of an H5D_chunk_insert_info_t structure
 *              and is suitable for communicating that structure type.
 *
 *              The datatype returned through `resized_type` has an extent
 *              equal to the size of the encompassing
 *              H5D_filtered_collective_chunk_info_t structure. This makes
 *              it suitable for sending an array of
 *              H5D_filtered_collective_chunk_info_t structures, while
 *              extracting out just the information needed during
 *              communication.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_get_chunk_insert_info_types(MPI_Datatype *contig_type, bool *contig_type_derived,
                                      MPI_Datatype *resized_type, bool *resized_type_derived)
{
    MPI_Datatype struct_type              = MPI_DATATYPE_NULL;
    bool         struct_type_derived      = false;
    MPI_Datatype chunk_block_type         = MPI_DATATYPE_NULL;
    bool         chunk_block_type_derived = false;
    MPI_Aint     contig_type_extent;
    MPI_Datatype types[5];
    MPI_Aint     displacements[5];
    int          block_lengths[5];
    int          field_count;
    int          mpi_code;
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(contig_type);
    assert(contig_type_derived);
    assert(resized_type);
    assert(resized_type_derived);

    *contig_type_derived  = false;
    *resized_type_derived = false;

    /* Create struct type for an H5F_block_t structure */
    if (H5F_mpi_get_file_block_type(false, &chunk_block_type, &chunk_block_type_derived) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't create derived type for chunk file description");

    field_count = 5;
    assert(field_count == (sizeof(types) / sizeof(MPI_Datatype)));

    /*
     * Create struct type to pack information into memory as follows:
     *
     * Chunk's new Offset/Size (H5F_block_t) ->
     * Chunk Index Info (H5D_chunk_index_info_t)
     */
    block_lengths[0] = 1;
    block_lengths[1] = 1;
    block_lengths[2] = 1;
    block_lengths[3] = 1;
    block_lengths[4] = 1;
    displacements[0] = offsetof(H5D_chunk_insert_info_t, chunk_block);
    displacements[1] = offsetof(H5D_chunk_insert_info_t, index_info.chunk_idx);
    displacements[2] = offsetof(H5D_chunk_insert_info_t, index_info.dset_oloc_addr);
    displacements[3] = offsetof(H5D_chunk_insert_info_t, index_info.filter_mask);
    displacements[4] = offsetof(H5D_chunk_insert_info_t, index_info.need_insert);
    types[0]         = chunk_block_type;
    types[1]         = HSIZE_AS_MPI_TYPE;
    types[2]         = HADDR_AS_MPI_TYPE;
    types[3]         = MPI_UNSIGNED;
    types[4]         = MPI_C_BOOL;
    if (MPI_SUCCESS !=
        (mpi_code = MPI_Type_create_struct(field_count, block_lengths, displacements, types, &struct_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct failed", mpi_code)
    struct_type_derived = true;

    contig_type_extent = (MPI_Aint)(sizeof(H5F_block_t) + sizeof(H5D_chunk_index_info_t));

    if (MPI_SUCCESS != (mpi_code = MPI_Type_create_resized(struct_type, 0, contig_type_extent, contig_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_resized failed", mpi_code)
    *contig_type_derived = true;

    if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(contig_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)

    struct_type_derived = false;
    if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&struct_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_free failed", mpi_code)

    /*
     * Create struct type to correctly extract all needed
     * information from a H5D_filtered_collective_chunk_info_t
     * structure.
     */
    displacements[0] = offsetof(H5D_filtered_collective_chunk_info_t, chunk_new);
    displacements[1] = offsetof(H5D_filtered_collective_chunk_info_t, index_info.chunk_idx);
    displacements[2] = offsetof(H5D_filtered_collective_chunk_info_t, index_info.dset_oloc_addr);
    displacements[3] = offsetof(H5D_filtered_collective_chunk_info_t, index_info.filter_mask);
    displacements[4] = offsetof(H5D_filtered_collective_chunk_info_t, index_info.need_insert);
    if (MPI_SUCCESS !=
        (mpi_code = MPI_Type_create_struct(field_count, block_lengths, displacements, types, &struct_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct failed", mpi_code)
    struct_type_derived = true;

    if (MPI_SUCCESS != (mpi_code = MPI_Type_create_resized(
                            struct_type, 0, sizeof(H5D_filtered_collective_chunk_info_t), resized_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_resized failed", mpi_code)
    *resized_type_derived = true;

    if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(resized_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)

done:
    if (struct_type_derived) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&struct_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    }
    if (chunk_block_type_derived) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&chunk_block_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    }

    if (ret_value < 0) {
        if (*resized_type_derived) {
            if (MPI_SUCCESS != (mpi_code = MPI_Type_free(resized_type)))
                HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
            *resized_type_derived = false;
        }
        if (*contig_type_derived) {
            if (MPI_SUCCESS != (mpi_code = MPI_Type_free(contig_type)))
                HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
            *contig_type_derived = false;
        }
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_get_chunk_insert_info_types() */

/*-------------------------------------------------------------------------
 * Function:    H5D__mpio_collective_filtered_vec_io
 *
 * Purpose:     Given a pointer to a H5D_filtered_collective_io_info_t
 *              structure with information about collective filtered chunk
 *              I/O, populates I/O vectors and performs vector I/O on those
 *              chunks.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__mpio_collective_filtered_vec_io(const H5D_filtered_collective_io_info_t *chunk_list, H5F_shared_t *f_sh,
                                     H5D_io_op_type_t op_type)
{
    const void **io_wbufs = NULL;
    void       **io_rbufs = NULL;
    H5FD_mem_t   io_types[2];
    uint32_t     iovec_count = 0;
    haddr_t     *io_addrs    = NULL;
    size_t      *io_sizes    = NULL;
    herr_t       ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(chunk_list);
    assert(f_sh);

    if (op_type == H5D_IO_OP_WRITE)
        iovec_count = (uint32_t)chunk_list->num_chunk_infos;
    else {
        assert(chunk_list->num_chunks_to_read <= chunk_list->num_chunk_infos);
        iovec_count = (uint32_t)chunk_list->num_chunks_to_read;
    }

    if (iovec_count > 0) {
        if (chunk_list->num_chunk_infos > UINT32_MAX)
            HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL,
                        "number of chunk entries in I/O operation exceeds UINT32_MAX");

        if (NULL == (io_addrs = H5MM_malloc(iovec_count * sizeof(*io_addrs))))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                        "couldn't allocate space for I/O addresses vector");
        if (NULL == (io_sizes = H5MM_malloc(iovec_count * sizeof(*io_sizes))))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "couldn't allocate space for I/O sizes vector");

        if (op_type == H5D_IO_OP_WRITE) {
            if (NULL == (io_wbufs = H5MM_malloc(iovec_count * sizeof(*io_wbufs))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "couldn't allocate space for I/O buffers vector");
        }
        else {
            if (NULL == (io_rbufs = H5MM_malloc(iovec_count * sizeof(*io_rbufs))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "couldn't allocate space for I/O buffers vector");
        }

        /*
         * Since all I/O will be raw data, we can save on memory a bit by
         * making use of H5FD_MEM_NOLIST to signal that all the memory types
         * are the same across the I/O vectors
         */
        io_types[0] = H5FD_MEM_DRAW;
        io_types[1] = H5FD_MEM_NOLIST;

        for (size_t i = 0, vec_idx = 0; i < chunk_list->num_chunk_infos; i++) {
            H5F_block_t *chunk_block;

            if (op_type == H5D_IO_OP_READ && !chunk_list->chunk_infos[i].need_read)
                continue;

            /*
             * Check that we aren't going to accidentally try to write past the
             * allocated memory for the I/O vector buffers in case bookkeeping
             * wasn't done properly for the chunk list struct's `num_chunks_to_read`
             * field.
             */
            assert(vec_idx < iovec_count);

            /* Set convenience pointer for current chunk block */
            chunk_block = (op_type == H5D_IO_OP_READ) ? &chunk_list->chunk_infos[i].chunk_current
                                                      : &chunk_list->chunk_infos[i].chunk_new;

            assert(H5_addr_defined(chunk_block->offset));
            io_addrs[vec_idx] = chunk_block->offset;

            /*
             * Ensure the chunk list is sorted in ascending ordering of
             * offset in the file. Note that we only compare that the
             * current address is greater than the previous address and
             * not equal to it; file addresses should only appear in the
             * chunk list once.
             */
#ifndef NDEBUG
            if (vec_idx > 0)
                assert(io_addrs[vec_idx] > io_addrs[vec_idx - 1]);
#endif

            io_sizes[vec_idx] = (size_t)chunk_block->length;

            if (op_type == H5D_IO_OP_WRITE)
                io_wbufs[vec_idx] = chunk_list->chunk_infos[i].buf;
            else
                io_rbufs[vec_idx] = chunk_list->chunk_infos[i].buf;

            vec_idx++;
        }
    }

    if (op_type == H5D_IO_OP_WRITE) {
        if (H5F_shared_vector_write(f_sh, iovec_count, io_types, io_addrs, io_sizes, io_wbufs) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "vector write call failed");
    }
    else {
        if (H5F_shared_vector_read(f_sh, iovec_count, io_types, io_addrs, io_sizes, io_rbufs) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "vector read call failed");
    }

done:
    H5MM_free(io_wbufs);
    H5MM_free(io_rbufs);
    H5MM_free(io_sizes);
    H5MM_free(io_addrs);

    FUNC_LEAVE_NOAPI(ret_value)
}

#ifdef H5Dmpio_DEBUG

static herr_t
H5D__mpio_dump_collective_filtered_chunk_list(H5D_filtered_collective_io_info_t *chunk_list, int mpi_rank)
{
    H5D_filtered_collective_chunk_info_t *chunk_entry;
    size_t                                i;
    herr_t                                ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_NOERR

    H5D_MPIO_DEBUG(mpi_rank, "CHUNK LIST: [");
    for (i = 0; i < chunk_list->num_chunk_infos; i++) {
        unsigned chunk_rank;

        chunk_entry = &chunk_list->chunk_infos[i];

        assert(chunk_entry->chunk_info);
        chunk_rank = (unsigned)H5S_GET_EXTENT_NDIMS(chunk_entry->chunk_info->fspace);

        H5D_MPIO_DEBUG(mpi_rank, " {");
        H5D_MPIO_DEBUG_VA(mpi_rank, "   - Entry %zu -", i);

        H5D_MPIO_DEBUG(mpi_rank, "   - Chunk Fspace Info -");
        H5D_MPIO_DEBUG_VA(mpi_rank,
                          "     Chunk Current Info: { Offset: %" PRIuHADDR ", Length: %" PRIuHADDR " }",
                          chunk_entry->chunk_current.offset, chunk_entry->chunk_current.length);
        H5D_MPIO_DEBUG_VA(mpi_rank, "     Chunk New Info: { Offset: %" PRIuHADDR ", Length: %" PRIuHADDR " }",
                          chunk_entry->chunk_new.offset, chunk_entry->chunk_new.length);

        H5D_MPIO_DEBUG(mpi_rank, "   - Chunk Insert Info -");
        H5D_MPIO_DEBUG_VA(mpi_rank,
                          "     Chunk Scaled Coords (4-d): { %" PRIuHSIZE ", %" PRIuHSIZE ", %" PRIuHSIZE
                          ", %" PRIuHSIZE " }",
                          chunk_rank < 1 ? 0 : chunk_entry->chunk_info->scaled[0],
                          chunk_rank < 2 ? 0 : chunk_entry->chunk_info->scaled[1],
                          chunk_rank < 3 ? 0 : chunk_entry->chunk_info->scaled[2],
                          chunk_rank < 4 ? 0 : chunk_entry->chunk_info->scaled[3]);
        H5D_MPIO_DEBUG_VA(mpi_rank, "     Chunk Index: %" PRIuHSIZE, chunk_entry->index_info.chunk_idx);
        H5D_MPIO_DEBUG_VA(mpi_rank, "     Dataset Object Header Address: %" PRIuHADDR,
                          chunk_entry->index_info.dset_oloc_addr);
        H5D_MPIO_DEBUG_VA(mpi_rank, "     Filter Mask: %u", chunk_entry->index_info.filter_mask);
        H5D_MPIO_DEBUG_VA(mpi_rank, "     Need Insert: %s",
                          chunk_entry->index_info.need_insert ? "YES" : "NO");

        H5D_MPIO_DEBUG(mpi_rank, "   - Other Info -");
        H5D_MPIO_DEBUG_VA(mpi_rank, "     Chunk Info Ptr: %p", (void *)chunk_entry->chunk_info);
        H5D_MPIO_DEBUG_VA(mpi_rank, "     Need Read: %s", chunk_entry->need_read ? "YES" : "NO");
        H5D_MPIO_DEBUG_VA(mpi_rank, "     Chunk I/O Size: %zu", chunk_entry->io_size);
        H5D_MPIO_DEBUG_VA(mpi_rank, "     Chunk Buffer Size: %zu", chunk_entry->chunk_buf_size);
        H5D_MPIO_DEBUG_VA(mpi_rank, "     Original Owner: %d", chunk_entry->orig_owner);
        H5D_MPIO_DEBUG_VA(mpi_rank, "     New Owner: %d", chunk_entry->new_owner);
        H5D_MPIO_DEBUG_VA(mpi_rank, "     # of Writers: %d", chunk_entry->num_writers);
        H5D_MPIO_DEBUG_VA(mpi_rank, "     Chunk Data Buffer Ptr: %p", (void *)chunk_entry->buf);

        H5D_MPIO_DEBUG(mpi_rank, " }");
    }
    H5D_MPIO_DEBUG(mpi_rank, "]");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__mpio_dump_collective_filtered_chunk_list() */

#endif

#endif /* H5_HAVE_PARALLEL */
