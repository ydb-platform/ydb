# cython: language_level=3
# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

from .api_types_ext cimport *

include "config.pxi"

cdef extern from "hdf5.h":
  # Basic types
  ctypedef long int hid_t
  ctypedef int hbool_t
  ctypedef int herr_t
  ctypedef int htri_t
  ctypedef long long hsize_t
  ctypedef signed long long hssize_t
  ctypedef signed long long haddr_t
  ctypedef long int off_t

  ctypedef struct hvl_t:
    size_t len                 # Length of VL data (in base type units)
    void *p                    # Pointer to VL data

  int HADDR_UNDEF

  ctypedef enum H5_iter_order_t:
    H5_ITER_UNKNOWN = -1,       # Unknown order
    H5_ITER_INC,                # Increasing order
    H5_ITER_DEC,                # Decreasing order
    H5_ITER_NATIVE,             # No particular order, whatever is fastest
    H5_ITER_N                   # Number of iteration orders

  ctypedef enum H5_index_t:
    H5_INDEX_UNKNOWN = -1,      # Unknown index type
    H5_INDEX_NAME,              # Index on names
    H5_INDEX_CRT_ORDER,         # Index on creation order
    H5_INDEX_N                  # Number of indices defined

# === H5D - Dataset API =======================================================

  ctypedef enum H5D_layout_t:
      H5D_LAYOUT_ERROR    = -1,
      H5D_COMPACT         = 0,
      H5D_CONTIGUOUS      = 1,
      H5D_CHUNKED         = 2,
      H5D_VIRTUAL         = 3,
      H5D_NLAYOUTS        = 4

  ctypedef enum H5D_vds_view_t:
      H5D_VDS_ERROR           = -1,
      H5D_VDS_FIRST_MISSING   = 0,
      H5D_VDS_LAST_AVAILABLE  = 1

  ctypedef enum H5D_alloc_time_t:
    H5D_ALLOC_TIME_ERROR    =-1,
    H5D_ALLOC_TIME_DEFAULT  =0,
    H5D_ALLOC_TIME_EARLY    =1,
    H5D_ALLOC_TIME_LATE        =2,
    H5D_ALLOC_TIME_INCR        =3

  ctypedef enum H5D_space_status_t:
    H5D_SPACE_STATUS_ERROR            =-1,
    H5D_SPACE_STATUS_NOT_ALLOCATED    =0,
    H5D_SPACE_STATUS_PART_ALLOCATED    =1,
    H5D_SPACE_STATUS_ALLOCATED        =2

  ctypedef enum H5D_fill_time_t:
    H5D_FILL_TIME_ERROR    =-1,
    H5D_FILL_TIME_ALLOC =0,
    H5D_FILL_TIME_NEVER    =1,
    H5D_FILL_TIME_IFSET    =2

  ctypedef enum H5D_fill_value_t:
    H5D_FILL_VALUE_ERROR        =-1,
    H5D_FILL_VALUE_UNDEFINED    =0,
    H5D_FILL_VALUE_DEFAULT      =1,
    H5D_FILL_VALUE_USER_DEFINED =2

  ctypedef  herr_t (*H5D_operator_t)(void *elem, hid_t type_id, unsigned ndim,
                    hsize_t *point, void *operator_data) except -1


  IF HDF5_VERSION >= (1, 12, 3) or (HDF5_VERSION >= (1, 10, 10) and HDF5_VERSION < (1, 10, 99)):
    ctypedef int (*H5D_chunk_iter_op_t)(const hsize_t *offset, unsigned filter_mask,
                                        haddr_t addr, hsize_t size, void *op_data) except -1

# === H5F - File API ==========================================================

  # File constants
  cdef enum:
    H5F_ACC_TRUNC
    H5F_ACC_RDONLY
    H5F_ACC_RDWR
    H5F_ACC_EXCL
    H5F_ACC_DEBUG
    H5F_ACC_CREAT
    H5F_ACC_SWMR_WRITE
    H5F_ACC_SWMR_READ

  cdef enum:
    H5LT_FILE_IMAGE_OPEN_RW
    H5LT_FILE_IMAGE_DONT_COPY
    H5LT_FILE_IMAGE_DONT_RELEASE

  # The difference between a single file and a set of mounted files
  cdef enum H5F_scope_t:
    H5F_SCOPE_LOCAL     = 0,    # specified file handle only
    H5F_SCOPE_GLOBAL    = 1,    # entire virtual file
    H5F_SCOPE_DOWN      = 2     # for internal use only

  cdef enum H5F_close_degree_t:
    H5F_CLOSE_DEFAULT = 0,
    H5F_CLOSE_WEAK    = 1,
    H5F_CLOSE_SEMI    = 2,
    H5F_CLOSE_STRONG  = 3

  cdef enum H5F_fspace_strategy_t:
    H5F_FSPACE_STRATEGY_FSM_AGGR = 0,  # FSM, Aggregators, VFD
    H5F_FSPACE_STRATEGY_PAGE     = 1,  # Paged FSM, VFD
    H5F_FSPACE_STRATEGY_AGGR     = 2,  # Aggregators, VFD
    H5F_FSPACE_STRATEGY_NONE     = 3   # VFD

  int H5F_OBJ_FILE
  int H5F_OBJ_DATASET
  int H5F_OBJ_GROUP
  int H5F_OBJ_DATATYPE
  int H5F_OBJ_ATTR
  int H5F_OBJ_ALL
  int H5F_OBJ_LOCAL
  hsize_t H5F_UNLIMITED

  IF HDF5_VERSION < (1,11,4):
    ctypedef enum H5F_libver_t:
      H5F_LIBVER_EARLIEST = 0,        # Use the earliest possible format for storing objects
      H5F_LIBVER_V18 = 1,
      H5F_LIBVER_V110 = 2,
      H5F_LIBVER_NBOUNDS
    int H5F_LIBVER_LATEST  # Use the latest possible format available for storing objects

  IF HDF5_VERSION >= (1, 11, 4) and HDF5_VERSION < (1, 14, 0):
    ctypedef enum H5F_libver_t:
      H5F_LIBVER_EARLIEST = 0,        # Use the earliest possible format for storing objects
      H5F_LIBVER_V18 = 1,
      H5F_LIBVER_V110 = 2,
      H5F_LIBVER_V112 = 3,
      H5F_LIBVER_NBOUNDS
    int H5F_LIBVER_LATEST  # Use the latest possible format available for storing objects

  IF HDF5_VERSION >= (1, 14, 0):
    ctypedef enum H5F_libver_t:
      H5F_LIBVER_EARLIEST = 0,        # Use the earliest possible format for storing objects
      H5F_LIBVER_V18 = 1,
      H5F_LIBVER_V110 = 2,
      H5F_LIBVER_V112 = 3,
      H5F_LIBVER_V114 = 4,
      H5F_LIBVER_NBOUNDS
    int H5F_LIBVER_LATEST  # Use the latest possible format available for storing objects

# === H5FD - Low-level file descriptor API ====================================

  ctypedef enum H5FD_mem_t:
    H5FD_MEM_NOLIST    = -1,
    H5FD_MEM_DEFAULT    = 0,
    H5FD_MEM_SUPER      = 1,
    H5FD_MEM_BTREE      = 2,
    H5FD_MEM_DRAW       = 3,
    H5FD_MEM_GHEAP      = 4,
    H5FD_MEM_LHEAP      = 5,
    H5FD_MEM_OHDR       = 6,
    H5FD_MEM_NTYPES

  # HDF5 uses a clever scheme wherein these are actually init() calls
  # Hopefully Cython won't have a problem with this.
  # Thankfully they are defined but -1 if unavailable
  hid_t H5FD_CORE
  hid_t H5FD_FAMILY
  hid_t H5FD_LOG
  hid_t H5FD_MPIO
  hid_t H5FD_MULTI
  hid_t H5FD_SEC2
  hid_t H5FD_DIRECT
  hid_t H5FD_STDIO
  IF UNAME_SYSNAME == "Windows":
    hid_t H5FD_WINDOWS
  hid_t H5FD_ROS3

  int H5FD_LOG_LOC_READ   # 0x0001
  int H5FD_LOG_LOC_WRITE  # 0x0002
  int H5FD_LOG_LOC_SEEK   # 0x0004
  int H5FD_LOG_LOC_IO     # (H5FD_LOG_LOC_READ|H5FD_LOG_LOC_WRITE|H5FD_LOG_LOC_SEEK)

  # Flags for tracking number of times each byte is read/written
  int H5FD_LOG_FILE_READ  # 0x0008
  int H5FD_LOG_FILE_WRITE # 0x0010
  int H5FD_LOG_FILE_IO    # (H5FD_LOG_FILE_READ|H5FD_LOG_FILE_WRITE)

  # Flag for tracking "flavor" (type) of information stored at each byte
  int H5FD_LOG_FLAVOR     # 0x0020

  # Flags for tracking total number of reads/writes/seeks
  int H5FD_LOG_NUM_READ   # 0x0040
  int H5FD_LOG_NUM_WRITE  # 0x0080
  int H5FD_LOG_NUM_SEEK   # 0x0100
  int H5FD_LOG_NUM_IO     # (H5FD_LOG_NUM_READ|H5FD_LOG_NUM_WRITE|H5FD_LOG_NUM_SEEK)

  # Flags for tracking time spent in open/read/write/seek/close
  int H5FD_LOG_TIME_OPEN  # 0x0200        # Not implemented yet
  int H5FD_LOG_TIME_READ  # 0x0400        # Not implemented yet
  int H5FD_LOG_TIME_WRITE # 0x0800        # Partially implemented (need to track total time)
  int H5FD_LOG_TIME_SEEK  # 0x1000        # Partially implemented (need to track total time & track time for seeks during reading)
  int H5FD_LOG_TIME_CLOSE # 0x2000        # Fully implemented
  int H5FD_LOG_TIME_IO    # (H5FD_LOG_TIME_OPEN|H5FD_LOG_TIME_READ|H5FD_LOG_TIME_WRITE|H5FD_LOG_TIME_SEEK|H5FD_LOG_TIME_CLOSE)

  # Flag for tracking allocation of space in file
  int H5FD_LOG_ALLOC      # 0x4000
  int H5FD_LOG_ALL        # (H5FD_LOG_ALLOC|H5FD_LOG_TIME_IO|H5FD_LOG_NUM_IO|H5FD_LOG_FLAVOR|H5FD_LOG_FILE_IO|H5FD_LOG_LOC_IO)

  ctypedef enum H5FD_mpio_xfer_t:
    H5FD_MPIO_INDEPENDENT = 0,
    H5FD_MPIO_COLLECTIVE

  # File driver identifier type and values
  IF HDF5_VERSION >= (1, 14, 0):
    ctypedef int H5FD_class_value_t

    H5FD_class_value_t H5_VFD_INVALID      # -1
    H5FD_class_value_t H5_VFD_SEC2         # 0
    H5FD_class_value_t H5_VFD_CORE         # 1
    H5FD_class_value_t H5_VFD_LOG          # 2
    H5FD_class_value_t H5_VFD_FAMILY       # 3
    H5FD_class_value_t H5_VFD_MULTI        # 4
    H5FD_class_value_t H5_VFD_STDIO        # 5
    H5FD_class_value_t H5_VFD_SPLITTER     # 6
    H5FD_class_value_t H5_VFD_MPIO         # 7
    H5FD_class_value_t H5_VFD_DIRECT       # 8
    H5FD_class_value_t H5_VFD_MIRROR       # 9
    H5FD_class_value_t H5_VFD_HDFS         # 10
    H5FD_class_value_t H5_VFD_ROS3         # 11
    H5FD_class_value_t H5_VFD_SUBFILING    # 12
    H5FD_class_value_t H5_VFD_IOC          # 13
    H5FD_class_value_t H5_VFD_ONION        # 14

  # Class information for each file driver
  IF HDF5_VERSION < (1, 14, 0):
    ctypedef struct H5FD_class_t:
      const char *name
      haddr_t maxaddr
      H5F_close_degree_t fc_degree
      herr_t  (*terminate)()
      hsize_t (*sb_size)(H5FD_t *file)
      herr_t  (*sb_encode)(H5FD_t *file, char *name, unsigned char *p)
      herr_t  (*sb_decode)(H5FD_t *f, const char *name, const unsigned char *p)
      size_t  fapl_size
      void *  (*fapl_get)(H5FD_t *file) except *
      void *  (*fapl_copy)(const void *fapl) except *
      herr_t  (*fapl_free)(void *fapl) except -1
      size_t  dxpl_size
      void *  (*dxpl_copy)(const void *dxpl)
      herr_t  (*dxpl_free)(void *dxpl)
      H5FD_t *(*open)(const char *name, unsigned flags, hid_t fapl, haddr_t maxaddr) except *
      herr_t  (*close)(H5FD_t *file) except -1
      int     (*cmp)(const H5FD_t *f1, const H5FD_t *f2)
      herr_t  (*query)(const H5FD_t *f1, unsigned long *flags)
      herr_t  (*get_type_map)(const H5FD_t *file, H5FD_mem_t *type_map)
      haddr_t (*alloc)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, hsize_t size)
      herr_t  (*free)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, hsize_t size)
      haddr_t (*get_eoa)(const H5FD_t *file, H5FD_mem_t type) noexcept
      herr_t  (*set_eoa)(H5FD_t *file, H5FD_mem_t type, haddr_t addr) noexcept
      haddr_t (*get_eof)(const H5FD_t *file, H5FD_mem_t type) except -1
      herr_t  (*get_handle)(H5FD_t *file, hid_t fapl, void**file_handle)
      herr_t  (*read)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl, haddr_t addr, size_t size, void *buffer) except *
      herr_t  (*write)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl, haddr_t addr, size_t size, const void *buffer) except *
      herr_t  (*flush)(H5FD_t *file, hid_t dxpl_id, hbool_t closing) except -1
      herr_t  (*truncate)(H5FD_t *file, hid_t dxpl_id, hbool_t closing) except -1
      herr_t  (*lock)(H5FD_t *file, hbool_t rw)
      herr_t  (*unlock)(H5FD_t *file)
      H5FD_mem_t fl_map[<int>H5FD_MEM_NTYPES]
  ELSE:
    unsigned H5FD_CLASS_VERSION  # File driver struct version

    ctypedef struct H5FD_class_t:
      unsigned version  # File driver class struct version number
      H5FD_class_value_t value
      const char *name
      haddr_t maxaddr
      H5F_close_degree_t fc_degree
      herr_t  (*terminate)()
      hsize_t (*sb_size)(H5FD_t *file)
      herr_t  (*sb_encode)(H5FD_t *file, char *name, unsigned char *p)
      herr_t  (*sb_decode)(H5FD_t *f, const char *name, const unsigned char *p)
      size_t  fapl_size
      void *  (*fapl_get)(H5FD_t *file) except *
      void *  (*fapl_copy)(const void *fapl) except *
      herr_t  (*fapl_free)(void *fapl) except -1
      size_t  dxpl_size
      void *  (*dxpl_copy)(const void *dxpl)
      herr_t  (*dxpl_free)(void *dxpl)
      H5FD_t *(*open)(const char *name, unsigned flags, hid_t fapl, haddr_t maxaddr) except *
      herr_t  (*close)(H5FD_t *file) except -1
      int     (*cmp)(const H5FD_t *f1, const H5FD_t *f2)
      herr_t  (*query)(const H5FD_t *f1, unsigned long *flags)
      herr_t  (*get_type_map)(const H5FD_t *file, H5FD_mem_t *type_map)
      haddr_t (*alloc)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, hsize_t size)
      herr_t  (*free)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, hsize_t size)
      haddr_t (*get_eoa)(const H5FD_t *file, H5FD_mem_t type) noexcept
      herr_t  (*set_eoa)(H5FD_t *file, H5FD_mem_t type, haddr_t addr) noexcept
      haddr_t (*get_eof)(const H5FD_t *file, H5FD_mem_t type) except -1
      herr_t  (*get_handle)(H5FD_t *file, hid_t fapl, void**file_handle)
      herr_t  (*read)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl, haddr_t addr, size_t size, void *buffer) except *
      herr_t  (*write)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl, haddr_t addr, size_t size, const void *buffer) except *
      herr_t  (*flush)(H5FD_t *file, hid_t dxpl_id, hbool_t closing) except -1
      herr_t  (*truncate)(H5FD_t *file, hid_t dxpl_id, hbool_t closing) except -1
      herr_t  (*lock)(H5FD_t *file, hbool_t rw)
      herr_t  (*unlock)(H5FD_t *file)
      H5FD_mem_t fl_map[<int>H5FD_MEM_NTYPES]

  # The main datatype for each driver
  ctypedef struct H5FD_t:
    hid_t driver_id             # driver ID for this file
    const H5FD_class_t cls      # constant class info
    unsigned long fileno        # File 'serial' number
    unsigned access_flags       # File access flags (from create or open)
    unsigned long feature_flags # VFL Driver feature Flags
    haddr_t maxaddr             # For this file, overrides class
    haddr_t base_addr           # Base address for HDF5 data w/in file

    # Space allocation management fields
    hsize_t threshold           # Threshold for alignment
    hsize_t alignment           # Allocation alignment
    hbool_t paged_aggr          # Paged aggregation for file space is enabled or not

  ctypedef struct H5FD_ros3_fapl_t:
    int32_t version
    hbool_t authenticate
    char    aws_region[33]
    char    secret_id[129]
    char    secret_key[129]

  unsigned int H5FD_CURR_ROS3_FAPL_T_VERSION # version of struct

  IF HDF5_VERSION >= (1, 14, 2):
    size_t H5FD_ROS3_MAX_SECRET_TOK_LEN
# === H5G - Groups API ========================================================

  ctypedef enum H5G_link_t:
    H5G_LINK_ERROR      = -1,
    H5G_LINK_HARD       = 0,
    H5G_LINK_SOFT       = 1

  cdef enum H5G_obj_t:
    H5G_UNKNOWN = -1,           # Unknown object type
    H5G_LINK,                   # Object is a symbolic link
    H5G_GROUP,                  # Object is a group
    H5G_DATASET,                # Object is a dataset
    H5G_TYPE,                   # Object is a named data type

  ctypedef struct H5G_stat_t:
    unsigned long fileno[2]
    unsigned long objno[2]
    unsigned int nlink
    H5G_obj_t type              # new in HDF5 1.6
    time_t mtime
    size_t linklen
    #H5O_stat_t ohdr            # Object header information. New in HDF5 1.6

  ctypedef herr_t (*H5G_iterate_t)(hid_t group, char *name, void* op_data) except 2

  ctypedef enum H5G_storage_type_t:
      H5G_STORAGE_TYPE_UNKNOWN = -1,
      H5G_STORAGE_TYPE_SYMBOL_TABLE,
      H5G_STORAGE_TYPE_COMPACT,
      H5G_STORAGE_TYPE_DENSE

  ctypedef struct H5G_info_t:
      H5G_storage_type_t     storage_type
      hsize_t     nlinks
      int64_t     max_corder

# === H5I - Identifier and reflection interface ===============================

  int H5I_INVALID_HID

  IF HDF5_VERSION < VOL_MIN_HDF5_VERSION:
    ctypedef enum H5I_type_t:
      H5I_UNINIT       = -2,  # uninitialized Group
      H5I_BADID        = -1,  # invalid Group
      H5I_FILE        = 1,    # type ID for File objects
      H5I_GROUP,              # type ID for Group objects
      H5I_DATATYPE,           # type ID for Datatype objects
      H5I_DATASPACE,          # type ID for Dataspace objects
      H5I_DATASET,            # type ID for Dataset objects
      H5I_ATTR,               # type ID for Attribute objects
      H5I_REFERENCE,          # type ID for Reference objects
      H5I_VFL,                # type ID for virtual file layer
      H5I_GENPROP_CLS,        # type ID for generic property list classes
      H5I_GENPROP_LST,        # type ID for generic property lists
      H5I_ERROR_CLASS,        # type ID for error classes
      H5I_ERROR_MSG,          # type ID for error messages
      H5I_ERROR_STACK,        # type ID for error stacks
      H5I_NTYPES              # number of valid groups, MUST BE LAST!

  ELSE:
    ctypedef enum H5I_type_t:
      H5I_UNINIT      = -2,     # uninitialized type
      H5I_BADID       = -1,     # invalid Type
      H5I_FILE        = 1,      # type ID for File objects
      H5I_GROUP,                # type ID for Group objects
      H5I_DATATYPE,             # type ID for Datatype objects
      H5I_DATASPACE,            # type ID for Dataspace object
      H5I_DATASET,              # type ID for Dataset object
      H5I_MAP,                  # type ID for Map object
      H5I_ATTR,                 # type ID for Attribute objects
      H5I_VFL,                  # type ID for virtual file layer
      H5I_VOL,                  # type ID for virtual object layer
      H5I_GENPROP_CLS,          # type ID for generic property list classes
      H5I_GENPROP_LST,          # type ID for generic property lists
      H5I_ERROR_CLASS,          # type ID for error classes
      H5I_ERROR_MSG,            # type ID for error messages
      H5I_ERROR_STACK,          # type ID for error stacks
      H5I_SPACE_SEL_ITER,       # type ID for dataspace selection iterator
      H5I_NTYPES                # number of library types, MUST BE LAST!

# === H5L/H5O - Links interface (1.8.X only) ======================================

  unsigned int H5L_MAX_LINK_NAME_LEN #  ((uint32_t) (-1)) (4GB - 1)

  # Link class types.
  # * Values less than 64 are reserved for the HDF5 library's internal use.
  # * Values 64 to 255 are for "user-defined" link class types; these types are
  # * defined by HDF5 but their behavior can be overridden by users.
  # * Users who want to create new classes of links should contact the HDF5
  # * development team at hdfhelp@ncsa.uiuc.edu .
  # * These values can never change because they appear in HDF5 files.
  #
  ctypedef enum H5L_type_t:
    H5L_TYPE_ERROR = (-1),      #  Invalid link type id
    H5L_TYPE_HARD = 0,          #  Hard link id
    H5L_TYPE_SOFT = 1,          #  Soft link id
    H5L_TYPE_EXTERNAL = 64,     #  External link id
    H5L_TYPE_MAX = 255          #  Maximum link type id

  #  Information struct for link (for H5Lget_info/H5Lget_info_by_idx)
  cdef union _add_u:
    haddr_t address   #  Address hard link points to
    size_t val_size   #  Size of a soft link or UD link value

  ctypedef struct H5L_info_t:
    H5L_type_t  type            #  Type of link
    hbool_t     corder_valid    #  Indicate if creation order is valid
    int64_t     corder          #  Creation order
    H5T_cset_t  cset            #  Character set of link name
    _add_u u

  #  Prototype for H5Literate/H5Literate_by_name() operator
  ctypedef herr_t (*H5L_iterate_t) (hid_t group, char *name, H5L_info_t *info,
                    void *op_data) except 2

  ctypedef uint32_t H5O_msg_crt_idx_t

  ctypedef enum H5O_type_t:
    H5O_TYPE_UNKNOWN = -1,      # Unknown object type
    H5O_TYPE_GROUP,             # Object is a group
    H5O_TYPE_DATASET,           # Object is a dataset
    H5O_TYPE_NAMED_DATATYPE,    # Object is a named data type
    H5O_TYPE_NTYPES             # Number of different object types (must be last!)

  unsigned int H5O_COPY_SHALLOW_HIERARCHY_FLAG    # (0x0001u) Copy only immediate members
  unsigned int H5O_COPY_EXPAND_SOFT_LINK_FLAG     # (0x0002u) Expand soft links into new objects
  unsigned int H5O_COPY_EXPAND_EXT_LINK_FLAG      # (0x0004u) Expand external links into new objects
  unsigned int H5O_COPY_EXPAND_REFERENCE_FLAG     # (0x0008u) Copy objects that are pointed by references
  unsigned int H5O_COPY_WITHOUT_ATTR_FLAG         # (0x0010u) Copy object without copying attributes
  unsigned int H5O_COPY_PRESERVE_NULL_FLAG        # (0x0020u) Copy NULL messages (empty space)
  unsigned int H5O_COPY_ALL                       # (0x003Fu) All object copying flags (for internal checking)

  # --- Components for the H5O_info_t struct ----------------------------------

  ctypedef struct space:
    hsize_t total           #  Total space for storing object header in file
    hsize_t meta            #  Space within header for object header metadata information
    hsize_t mesg            #  Space within header for actual message information
    hsize_t free            #  Free space within object header

  ctypedef struct mesg:
    unsigned long present   #  Flags to indicate presence of message type in header
    unsigned long shared    #  Flags to indicate message type is shared in header

  ctypedef struct hdr:
    unsigned version        #  Version number of header format in file
    unsigned nmesgs         #  Number of object header messages
    unsigned nchunks        #  Number of object header chunks
    unsigned flags          #  Object header status flags
    space space
    mesg mesg

  ctypedef struct H5_ih_info_t:
    hsize_t     index_size,  # btree and/or list
    hsize_t     heap_size

  cdef struct meta_size:
    H5_ih_info_t   obj,    #        v1/v2 B-tree & local/fractal heap for groups, B-tree for chunked datasets
    H5_ih_info_t   attr    #        v2 B-tree & heap for attributes

  ctypedef struct H5O_info_t:
    unsigned long   fileno      #  File number that object is located in
    haddr_t         addr        #  Object address in file
    H5O_type_t      type        #  Basic object type (group, dataset, etc.)
    unsigned        rc          #  Reference count of object
    time_t          atime       #  Access time
    time_t          mtime       #  Modification time
    time_t          ctime       #  Change time
    time_t          btime       #  Birth time
    hsize_t         num_attrs   #  # of attributes attached to object
    hdr             hdr
    meta_size       meta_size

  ctypedef herr_t (*H5O_iterate_t)(hid_t obj, char *name, H5O_info_t *info,
                    void *op_data) except -1

# === H5P - Property list API =================================================

  int H5P_DEFAULT

  # Property list classes
  hid_t H5P_NO_CLASS
  hid_t H5P_FILE_CREATE
  hid_t H5P_FILE_ACCESS
  hid_t H5P_DATASET_CREATE
  hid_t H5P_DATASET_ACCESS
  hid_t H5P_DATASET_XFER

  hid_t H5P_OBJECT_CREATE
  hid_t H5P_OBJECT_COPY
  hid_t H5P_LINK_CREATE
  hid_t H5P_LINK_ACCESS
  hid_t H5P_GROUP_CREATE
  hid_t H5P_DATATYPE_CREATE
  hid_t H5P_CRT_ORDER_TRACKED
  hid_t H5P_CRT_ORDER_INDEXED

# === H5R - Reference API =====================================================

  size_t H5R_DSET_REG_REF_BUF_SIZE
  size_t H5R_OBJ_REF_BUF_SIZE

  ctypedef enum H5R_type_t:
    H5R_BADTYPE = (-1),
    H5R_OBJECT,
    H5R_DATASET_REGION,
    H5R_INTERNAL,
    H5R_MAXTYPE

# === H5S - Dataspaces ========================================================

  int H5S_ALL, H5S_MAX_RANK
  hsize_t H5S_UNLIMITED

  # Codes for defining selections
  ctypedef enum H5S_seloper_t:
    H5S_SELECT_NOOP      = -1,
    H5S_SELECT_SET       = 0,
    H5S_SELECT_OR,
    H5S_SELECT_AND,
    H5S_SELECT_XOR,
    H5S_SELECT_NOTB,
    H5S_SELECT_NOTA,
    H5S_SELECT_APPEND,
    H5S_SELECT_PREPEND,
    H5S_SELECT_INVALID    # Must be the last one

  ctypedef enum H5S_class_t:
    H5S_NO_CLASS         = -1,  #/*error
    H5S_SCALAR           = 0,   #/*scalar variable
    H5S_SIMPLE           = 1,   #/*simple data space
    H5S_NULL             = 2,   # NULL data space

  ctypedef enum H5S_sel_type:
    H5S_SEL_ERROR    = -1,         #Error
    H5S_SEL_NONE    = 0,        #Nothing selected
    H5S_SEL_POINTS    = 1,        #Sequence of points selected
    H5S_SEL_HYPERSLABS  = 2,    #"New-style" hyperslab selection defined
    H5S_SEL_ALL        = 3,        #Entire extent selected
    H5S_SEL_N        = 4            #/*THIS MUST BE LAST

# === H5T - Datatypes =========================================================

  # --- Enumerated constants --------------------------------------------------

  # Byte orders
  ctypedef enum H5T_order_t:
    H5T_ORDER_ERROR      = -1,  # error
    H5T_ORDER_LE         = 0,   # little endian
    H5T_ORDER_BE         = 1,   # bit endian
    H5T_ORDER_VAX        = 2,   # VAX mixed endian
    H5T_ORDER_NONE       = 3    # no particular order (strings, bits,..)

  # HDF5 signed enums
  ctypedef enum H5T_sign_t:
    H5T_SGN_ERROR        = -1,  # error
    H5T_SGN_NONE         = 0,   # this is an unsigned type
    H5T_SGN_2            = 1,   # two's complement
    H5T_NSGN             = 2    # this must be last!

  ctypedef enum H5T_norm_t:
    H5T_NORM_ERROR       = -1,
    H5T_NORM_IMPLIED     = 0,
    H5T_NORM_MSBSET      = 1,
    H5T_NORM_NONE        = 2

  ctypedef enum H5T_cset_t:
    H5T_CSET_ERROR       = -1,  # error
    H5T_CSET_ASCII       = 0,   # US ASCII
    H5T_CSET_UTF8        = 1,   # UTF-8 Unicode encoding

  ctypedef enum H5T_str_t:
    H5T_STR_ERROR        = -1,
    H5T_STR_NULLTERM     = 0,
    H5T_STR_NULLPAD      = 1,
    H5T_STR_SPACEPAD     = 2

  # Atomic datatype padding
  ctypedef enum H5T_pad_t:
    H5T_PAD_ZERO        = 0,
    H5T_PAD_ONE         = 1,
    H5T_PAD_BACKGROUND  = 2

  # HDF5 type classes
  cdef enum H5T_class_t:
    H5T_NO_CLASS         = -1,  # error
    H5T_INTEGER          = 0,   # integer types
    H5T_FLOAT            = 1,   # floating-point types
    H5T_TIME             = 2,   # date and time types
    H5T_STRING           = 3,   # character string types
    H5T_BITFIELD         = 4,   # bit field types
    H5T_OPAQUE           = 5,   # opaque types
    H5T_COMPOUND         = 6,   # compound types
    H5T_REFERENCE        = 7,   # reference types
    H5T_ENUM             = 8,   # enumeration types
    H5T_VLEN             = 9,   # variable-length types
    H5T_ARRAY            = 10,  # array types
    H5T_NCLASSES                # this must be last

  # Native search direction
  cdef enum H5T_direction_t:
    H5T_DIR_DEFAULT,
    H5T_DIR_ASCEND,
    H5T_DIR_DESCEND

  # For vlen strings
  cdef size_t H5T_VARIABLE

  # --- Predefined datatypes --------------------------------------------------

  cdef hid_t H5T_NATIVE_B8
  cdef hid_t H5T_NATIVE_B16
  cdef hid_t H5T_NATIVE_B32
  cdef hid_t H5T_NATIVE_B64
  cdef hid_t H5T_NATIVE_CHAR
  cdef hid_t H5T_NATIVE_SCHAR
  cdef hid_t H5T_NATIVE_UCHAR
  cdef hid_t H5T_NATIVE_SHORT
  cdef hid_t H5T_NATIVE_USHORT
  cdef hid_t H5T_NATIVE_INT
  cdef hid_t H5T_NATIVE_UINT
  cdef hid_t H5T_NATIVE_LONG
  cdef hid_t H5T_NATIVE_ULONG
  cdef hid_t H5T_NATIVE_LLONG
  cdef hid_t H5T_NATIVE_ULLONG
  cdef hid_t H5T_NATIVE_FLOAT
  cdef hid_t H5T_NATIVE_DOUBLE
  cdef hid_t H5T_NATIVE_LDOUBLE
  IF HDF5_VERSION > (1, 14, 3):
    cdef hid_t H5T_NATIVE_FLOAT16

  # "Standard" types
  cdef hid_t H5T_STD_I8LE
  cdef hid_t H5T_STD_I16LE
  cdef hid_t H5T_STD_I32LE
  cdef hid_t H5T_STD_I64LE
  cdef hid_t H5T_STD_U8LE
  cdef hid_t H5T_STD_U16LE
  cdef hid_t H5T_STD_U32LE
  cdef hid_t H5T_STD_U64LE
  cdef hid_t H5T_STD_B8LE
  cdef hid_t H5T_STD_B16LE
  cdef hid_t H5T_STD_B32LE
  cdef hid_t H5T_STD_B64LE
  cdef hid_t H5T_IEEE_F32LE
  cdef hid_t H5T_IEEE_F64LE
  cdef hid_t H5T_STD_I8BE
  cdef hid_t H5T_STD_I16BE
  cdef hid_t H5T_STD_I32BE
  cdef hid_t H5T_STD_I64BE
  cdef hid_t H5T_STD_U8BE
  cdef hid_t H5T_STD_U16BE
  cdef hid_t H5T_STD_U32BE
  cdef hid_t H5T_STD_U64BE
  cdef hid_t H5T_STD_B8BE
  cdef hid_t H5T_STD_B16BE
  cdef hid_t H5T_STD_B32BE
  cdef hid_t H5T_STD_B64BE
  cdef hid_t H5T_IEEE_F32BE
  cdef hid_t H5T_IEEE_F64BE
  IF HDF5_VERSION > (1, 14, 3):
    cdef hid_t H5T_IEEE_F16BE
    cdef hid_t H5T_IEEE_F16LE


  cdef hid_t H5T_NATIVE_INT8
  cdef hid_t H5T_NATIVE_UINT8
  cdef hid_t H5T_NATIVE_INT16
  cdef hid_t H5T_NATIVE_UINT16
  cdef hid_t H5T_NATIVE_INT32
  cdef hid_t H5T_NATIVE_UINT32
  cdef hid_t H5T_NATIVE_INT64
  cdef hid_t H5T_NATIVE_UINT64

  # Unix time types
  cdef hid_t H5T_UNIX_D32LE
  cdef hid_t H5T_UNIX_D64LE
  cdef hid_t H5T_UNIX_D32BE
  cdef hid_t H5T_UNIX_D64BE

  # String types
  cdef hid_t H5T_FORTRAN_S1
  cdef hid_t H5T_C_S1

  # References
  cdef hid_t H5T_STD_REF_OBJ
  cdef hid_t H5T_STD_REF_DSETREG

  # Type-conversion infrastructure

  ctypedef enum H5T_pers_t:
    H5T_PERS_DONTCARE	= -1,
    H5T_PERS_HARD	= 0,	    # /*hard conversion function		     */
    H5T_PERS_SOFT	= 1 	    # /*soft conversion function		     */

  ctypedef enum H5T_cmd_t:
    H5T_CONV_INIT	= 0,	#/*query and/or initialize private data	     */
    H5T_CONV_CONV	= 1, 	#/*convert data from source to dest datatype */
    H5T_CONV_FREE	= 2	    #/*function is being removed from path	     */

  ctypedef enum H5T_bkg_t:
    H5T_BKG_NO		= 0, 	#/*background buffer is not needed, send NULL */
    H5T_BKG_TEMP	= 1,	#/*bkg buffer used as temp storage only       */
    H5T_BKG_YES		= 2	    #/*init bkg buf with data before conversion   */

  ctypedef struct H5T_cdata_t:
    H5T_cmd_t		command     # /*what should the conversion function do?    */
    H5T_bkg_t		need_bkg   #/*is the background buffer needed?	     */
    hbool_t		recalc	        # /*recalculate private data		     */
    void		*priv	        # /*private data				     */

  ctypedef struct hvl_t:
      size_t len # /* Length of VL data (in base type units) */
      void *p    #/* Pointer to VL data */

  ctypedef herr_t (*H5T_conv_t)(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
      size_t nelmts, size_t buf_stride, size_t bkg_stride, void *buf,
      void *bkg, hid_t dset_xfer_plist) except -1

# === H5Z - Filters ===========================================================

  ctypedef int H5Z_filter_t

  int H5Z_CLASS_T_VERS
  int H5Z_FILTER_ERROR
  int H5Z_FILTER_NONE
  int H5Z_FILTER_ALL
  int H5Z_FILTER_DEFLATE
  int H5Z_FILTER_SHUFFLE
  int H5Z_FILTER_FLETCHER32
  int H5Z_FILTER_SZIP
  int H5Z_FILTER_NBIT
  int H5Z_FILTER_SCALEOFFSET
  int H5Z_FILTER_RESERVED
  int H5Z_FILTER_MAX
  int H5Z_MAX_NFILTERS

  int H5Z_FLAG_DEFMASK
  int H5Z_FLAG_MANDATORY
  int H5Z_FLAG_OPTIONAL

  int H5Z_FLAG_INVMASK
  int H5Z_FLAG_REVERSE
  int H5Z_FLAG_SKIP_EDC

  int H5_SZIP_ALLOW_K13_OPTION_MASK   #1
  int H5_SZIP_CHIP_OPTION_MASK        #2
  int H5_SZIP_EC_OPTION_MASK          #4
  int H5_SZIP_NN_OPTION_MASK          #32
  int H5_SZIP_MAX_PIXELS_PER_BLOCK    #32

  int H5Z_SO_INT_MINBITS_DEFAULT

  int H5Z_FILTER_CONFIG_ENCODE_ENABLED #(0x0001)
  int H5Z_FILTER_CONFIG_DECODE_ENABLED #(0x0002)

  cdef enum H5Z_EDC_t:
      H5Z_ERROR_EDC       = -1,
      H5Z_DISABLE_EDC     = 0,
      H5Z_ENABLE_EDC      = 1,
      H5Z_NO_EDC          = 2

  cdef enum H5Z_SO_scale_type_t:
      H5Z_SO_FLOAT_DSCALE = 0,
      H5Z_SO_FLOAT_ESCALE = 1,
      H5Z_SO_INT          = 2

# === H5A - Attributes API ====================================================

  ctypedef herr_t (*H5A_operator_t)(hid_t loc_id, char *attr_name, void* operator_data) except 2

  ctypedef struct H5A_info_t:
    hbool_t corder_valid          # Indicate if creation order is valid
    H5O_msg_crt_idx_t corder      # Creation order
    H5T_cset_t        cset        # Character set of attribute name
    hsize_t           data_size   # Size of raw data

  ctypedef herr_t (*H5A_operator2_t)(hid_t location_id, char *attr_name,
          H5A_info_t *ainfo, void *op_data) except 2



#  === H5AC - Attribute Cache configuration API ================================


  unsigned int H5AC__CURR_CACHE_CONFIG_VERSION  # 	1
  # I don't really understand why this works, but
  # https://groups.google.com/forum/?fromgroups#!topic/cython-users/-fLG08E5lYM
  # suggests it and it _does_ work
  enum: H5AC__MAX_TRACE_FILE_NAME_LEN	#	1024

  unsigned int H5AC_METADATA_WRITE_STRATEGY__PROCESS_0_ONLY   # 0
  unsigned int H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED      # 1


  cdef extern from "H5Cpublic.h":
  # === H5C - Cache configuration API ================================
    cdef enum H5C_cache_incr_mode:
      H5C_incr__off,
      H5C_incr__threshold,


    cdef enum H5C_cache_flash_incr_mode:
      H5C_flash_incr__off,
      H5C_flash_incr__add_space


    cdef enum H5C_cache_decr_mode:
      H5C_decr__off,
      H5C_decr__threshold,
      H5C_decr__age_out,
      H5C_decr__age_out_with_threshold

    ctypedef struct H5AC_cache_config_t:
      #     /* general configuration fields: */
      int version
      hbool_t rpt_fcn_enabled
      hbool_t evictions_enabled
      hbool_t set_initial_size
      size_t initial_size
      double min_clean_fraction
      size_t max_size
      size_t min_size
      long int epoch_length
      #    /* size increase control fields: */
      H5C_cache_incr_mode incr_mode
      double lower_hr_threshold
      double increment
      hbool_t apply_max_increment
      size_t max_increment
      H5C_cache_flash_incr_mode flash_incr_mode
      double flash_multiple
      double flash_threshold
      # /* size decrease control fields: */
      H5C_cache_decr_mode decr_mode
      double upper_hr_threshold
      double decrement
      hbool_t apply_max_decrement
      size_t max_decrement
      int epochs_before_eviction
      hbool_t apply_empty_reserve
      double empty_reserve
      # /* parallel configuration fields: */
      int dirty_bytes_threshold
      #  int metadata_write_strategy # present in 1.8.6 and higher




cdef extern from "hdf5_hl.h":
# === H5DS - Dimension Scales API =============================================

  ctypedef herr_t  (*H5DS_iterate_t)(hid_t dset, unsigned dim, hid_t scale, void *visitor_data) except 2
