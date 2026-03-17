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

/* Generated automatically by bin/make_err -- do not edit */
/* Add new errors to H5err.txt file */


#ifndef H5Eterm_H
#define H5Eterm_H

/* Reset major error IDs */
    
H5E_FUNC_g=    
H5E_FILE_g=    
H5E_VOL_g=    
H5E_SOHM_g=    
H5E_SYM_g=    
H5E_PLUGIN_g=    
H5E_VFL_g=    
H5E_INTERNAL_g=    
H5E_BTREE_g=    
H5E_REFERENCE_g=    
H5E_DATASPACE_g=    
H5E_RESOURCE_g=    
H5E_EVENTSET_g=    
H5E_ID_g=    
H5E_RS_g=    
H5E_FARRAY_g=    
H5E_HEAP_g=    
H5E_MAP_g=    
H5E_ATTR_g=    
H5E_IO_g=    
H5E_EFL_g=    
H5E_TST_g=    
H5E_LIB_g=    
H5E_PAGEBUF_g=    
H5E_FSPACE_g=    
H5E_DATASET_g=    
H5E_STORAGE_g=    
H5E_LINK_g=    
H5E_PLIST_g=    
H5E_DATATYPE_g=    
H5E_OHDR_g=    
H5E_NONE_MAJOR_g=    
H5E_SLIST_g=    
H5E_ARGS_g=    
H5E_CONTEXT_g=    
H5E_EARRAY_g=    
H5E_PLINE_g=    
H5E_ERROR_g=    
H5E_CACHE_g= (-1);

/* Reset minor error IDs */


/* Object ID related errors */    
H5E_BADID_g=    
H5E_BADGROUP_g=    
H5E_CANTREGISTER_g=    
H5E_CANTINC_g=    
H5E_CANTDEC_g=    
H5E_NOIDS_g=

/* Generic low-level file I/O errors */    
H5E_SEEKERROR_g=    
H5E_READERROR_g=    
H5E_WRITEERROR_g=    
H5E_CLOSEERROR_g=    
H5E_OVERFLOW_g=    
H5E_FCNTL_g=

/* Resource errors */    
H5E_NOSPACE_g=    
H5E_CANTALLOC_g=    
H5E_CANTCOPY_g=    
H5E_CANTFREE_g=    
H5E_ALREADYEXISTS_g=    
H5E_CANTLOCK_g=    
H5E_CANTUNLOCK_g=    
H5E_CANTGC_g=    
H5E_CANTGETSIZE_g=    
H5E_OBJOPEN_g=

/* Heap errors */    
H5E_CANTRESTORE_g=    
H5E_CANTCOMPUTE_g=    
H5E_CANTEXTEND_g=    
H5E_CANTATTACH_g=    
H5E_CANTUPDATE_g=    
H5E_CANTOPERATE_g=

/* Map related errors */    
H5E_CANTPUT_g=

/* Function entry/exit interface errors */    
H5E_CANTINIT_g=    
H5E_ALREADYINIT_g=    
H5E_CANTRELEASE_g=

/* Property list errors */    
H5E_CANTGET_g=    
H5E_CANTSET_g=    
H5E_DUPCLASS_g=    
H5E_SETDISALLOWED_g=

/* Asynchronous operation errors */    
H5E_CANTWAIT_g=    
H5E_CANTCANCEL_g=

/* Free space errors */    
H5E_CANTMERGE_g=    
H5E_CANTREVIVE_g=    
H5E_CANTSHRINK_g=

/* Object header related errors */    
H5E_LINKCOUNT_g=    
H5E_VERSION_g=    
H5E_ALIGNMENT_g=    
H5E_BADMESG_g=    
H5E_CANTDELETE_g=    
H5E_BADITER_g=    
H5E_CANTPACK_g=    
H5E_CANTRESET_g=    
H5E_CANTRENAME_g=

/* System level errors */    
H5E_SYSERRSTR_g=

/* I/O pipeline errors */    
H5E_NOFILTER_g=    
H5E_CALLBACK_g=    
H5E_CANAPPLY_g=    
H5E_SETLOCAL_g=    
H5E_NOENCODER_g=    
H5E_CANTFILTER_g=

/* Group related errors */    
H5E_CANTOPENOBJ_g=    
H5E_CANTCLOSEOBJ_g=    
H5E_COMPLEN_g=    
H5E_PATH_g=

/* No error */    
H5E_NONE_MINOR_g=

/* Plugin errors */    
H5E_OPENERROR_g=

/* File accessibility errors */    
H5E_FILEEXISTS_g=    
H5E_FILEOPEN_g=    
H5E_CANTCREATE_g=    
H5E_CANTOPENFILE_g=    
H5E_CANTCLOSEFILE_g=    
H5E_NOTHDF5_g=    
H5E_BADFILE_g=    
H5E_TRUNCATED_g=    
H5E_MOUNT_g=    
H5E_UNMOUNT_g=    
H5E_CANTDELETEFILE_g=    
H5E_CANTLOCKFILE_g=    
H5E_CANTUNLOCKFILE_g=

/* Cache related errors */    
H5E_CANTFLUSH_g=    
H5E_CANTUNSERIALIZE_g=    
H5E_CANTSERIALIZE_g=    
H5E_CANTTAG_g=    
H5E_CANTLOAD_g=    
H5E_PROTECT_g=    
H5E_NOTCACHED_g=    
H5E_SYSTEM_g=    
H5E_CANTINS_g=    
H5E_CANTPROTECT_g=    
H5E_CANTUNPROTECT_g=    
H5E_CANTPIN_g=    
H5E_CANTUNPIN_g=    
H5E_CANTMARKDIRTY_g=    
H5E_CANTMARKCLEAN_g=    
H5E_CANTMARKUNSERIALIZED_g=    
H5E_CANTMARKSERIALIZED_g=    
H5E_CANTDIRTY_g=    
H5E_CANTCLEAN_g=    
H5E_CANTEXPUNGE_g=    
H5E_CANTRESIZE_g=    
H5E_CANTDEPEND_g=    
H5E_CANTUNDEPEND_g=    
H5E_CANTNOTIFY_g=    
H5E_LOGGING_g=    
H5E_CANTCORK_g=    
H5E_CANTUNCORK_g=

/* Link related errors */    
H5E_TRAVERSE_g=    
H5E_NLINKS_g=    
H5E_NOTREGISTERED_g=    
H5E_CANTMOVE_g=    
H5E_CANTSORT_g=

/* Parallel MPI errors */    
H5E_MPI_g=    
H5E_MPIERRSTR_g=    
H5E_CANTRECV_g=    
H5E_CANTGATHER_g=    
H5E_NO_INDEPENDENT_g=

/* Dataspace errors */    
H5E_CANTCLIP_g=    
H5E_CANTCOUNT_g=    
H5E_CANTSELECT_g=    
H5E_CANTNEXT_g=    
H5E_BADSELECT_g=    
H5E_CANTCOMPARE_g=    
H5E_INCONSISTENTSTATE_g=    
H5E_CANTAPPEND_g=

/* Argument errors */    
H5E_UNINITIALIZED_g=    
H5E_UNSUPPORTED_g=    
H5E_BADTYPE_g=    
H5E_BADRANGE_g=    
H5E_BADVALUE_g=

/* B-tree related errors */    
H5E_NOTFOUND_g=    
H5E_EXISTS_g=    
H5E_CANTENCODE_g=    
H5E_CANTDECODE_g=    
H5E_CANTSPLIT_g=    
H5E_CANTREDISTRIBUTE_g=    
H5E_CANTSWAP_g=    
H5E_CANTINSERT_g=    
H5E_CANTLIST_g=    
H5E_CANTMODIFY_g=    
H5E_CANTREMOVE_g=    
H5E_CANTFIND_g=

/* Datatype conversion errors */    
H5E_CANTCONVERT_g=    
H5E_BADSIZE_g= (-1);

#endif /* H5Eterm_H */
