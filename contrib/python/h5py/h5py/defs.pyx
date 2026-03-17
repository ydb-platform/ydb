# cython: language_level=3
#
# Warning: this file is auto-generated from api_gen.py. DO NOT EDIT!
#

include "config.pxi"
from .api_types_ext cimport *
from .api_types_hdf5 cimport *

from . cimport _hdf5

from ._errors cimport set_exception, set_default_error_handler

cdef herr_t H5open() except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5open()
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5open (return value <0)")
    return r

cdef herr_t H5close() except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5close()
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5close (return value <0)")
    return r

cdef herr_t H5get_libversion(unsigned *majnum, unsigned *minnum, unsigned *relnum) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5get_libversion(majnum, minnum, relnum)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5get_libversion (return value <0)")
    return r

cdef herr_t H5check_version(unsigned majnum, unsigned minnum, unsigned relnum ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5check_version(majnum, minnum, relnum)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5check_version (return value <0)")
    return r

cdef herr_t H5free_memory(void *mem) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5free_memory(mem)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5free_memory (return value <0)")
    return r

cdef hid_t H5Acreate(hid_t loc_id, char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Acreate(loc_id, name, type_id, space_id, acpl_id, aapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Acreate (return value <0)")
    return r

cdef hid_t H5Aopen_idx(hid_t loc_id, unsigned int idx) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Aopen_idx(loc_id, idx)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aopen_idx (return value <0)")
    return r

cdef hid_t H5Aopen_name(hid_t loc_id, char *name) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Aopen_name(loc_id, name)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aopen_name (return value <0)")
    return r

cdef herr_t H5Aclose(hid_t attr_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Aclose(attr_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aclose (return value <0)")
    return r

cdef herr_t H5Adelete(hid_t loc_id, char *name) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Adelete(loc_id, name)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Adelete (return value <0)")
    return r

cdef herr_t H5Aread(hid_t attr_id, hid_t mem_type_id, void *buf) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Aread(attr_id, mem_type_id, buf)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aread (return value <0)")
    return r

cdef herr_t H5Awrite(hid_t attr_id, hid_t mem_type_id, void *buf ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Awrite(attr_id, mem_type_id, buf)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Awrite (return value <0)")
    return r

cdef int H5Aget_num_attrs(hid_t loc_id) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Aget_num_attrs(loc_id)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Aget_num_attrs (return value <0)")
    return r

cdef ssize_t H5Aget_name(hid_t attr_id, size_t buf_size, char *buf) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5Aget_name(attr_id, buf_size, buf)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aget_name (return value <0)")
    return r

cdef hid_t H5Aget_space(hid_t attr_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Aget_space(attr_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aget_space (return value <0)")
    return r

cdef hid_t H5Aget_type(hid_t attr_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Aget_type(attr_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aget_type (return value <0)")
    return r

cdef herr_t H5Adelete_by_name(hid_t loc_id, char *obj_name, char *attr_name, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Adelete_by_name(loc_id, obj_name, attr_name, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Adelete_by_name (return value <0)")
    return r

cdef herr_t H5Adelete_by_idx(hid_t loc_id, char *obj_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Adelete_by_idx(loc_id, obj_name, idx_type, order, n, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Adelete_by_idx (return value <0)")
    return r

cdef hid_t H5Acreate_by_name(hid_t loc_id, char *obj_name, char *attr_name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t lapl_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Acreate_by_name(loc_id, obj_name, attr_name, type_id, space_id, acpl_id, aapl_id, lapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Acreate_by_name (return value <0)")
    return r

cdef hid_t H5Aopen(hid_t obj_id, char *attr_name, hid_t aapl_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Aopen(obj_id, attr_name, aapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aopen (return value <0)")
    return r

cdef hid_t H5Aopen_by_name( hid_t loc_id, char *obj_name, char *attr_name, hid_t aapl_id, hid_t lapl_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Aopen_by_name(loc_id, obj_name, attr_name, aapl_id, lapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aopen_by_name (return value <0)")
    return r

cdef hid_t H5Aopen_by_idx(hid_t loc_id, char *obj_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, hid_t aapl_id, hid_t lapl_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Aopen_by_idx(loc_id, obj_name, idx_type, order, n, aapl_id, lapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aopen_by_idx (return value <0)")
    return r

cdef htri_t H5Aexists_by_name( hid_t loc_id, char *obj_name, char *attr_name, hid_t lapl_id) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Aexists_by_name(loc_id, obj_name, attr_name, lapl_id)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aexists_by_name (return value <0)")
    return r

cdef htri_t H5Aexists(hid_t obj_id, char *attr_name) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Aexists(obj_id, attr_name)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aexists (return value <0)")
    return r

cdef herr_t H5Arename(hid_t loc_id, char *old_attr_name, char *new_attr_name) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Arename(loc_id, old_attr_name, new_attr_name)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Arename (return value <0)")
    return r

cdef herr_t H5Arename_by_name(hid_t loc_id, char *obj_name, char *old_attr_name, char *new_attr_name, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Arename_by_name(loc_id, obj_name, old_attr_name, new_attr_name, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Arename_by_name (return value <0)")
    return r

cdef herr_t H5Aget_info( hid_t attr_id, H5A_info_t *ainfo) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Aget_info(attr_id, ainfo)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aget_info (return value <0)")
    return r

cdef herr_t H5Aget_info_by_name(hid_t loc_id, char *obj_name, char *attr_name, H5A_info_t *ainfo, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Aget_info_by_name(loc_id, obj_name, attr_name, ainfo, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aget_info_by_name (return value <0)")
    return r

cdef herr_t H5Aget_info_by_idx(hid_t loc_id, char *obj_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, H5A_info_t *ainfo, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Aget_info_by_idx(loc_id, obj_name, idx_type, order, n, ainfo, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aget_info_by_idx (return value <0)")
    return r

cdef herr_t H5Aiterate(hid_t obj_id, H5_index_t idx_type, H5_iter_order_t order, hsize_t *n, H5A_operator2_t op, void *op_data) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Aiterate(obj_id, idx_type, order, n, op, op_data)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Aiterate (return value <0)")
    return r

cdef hsize_t H5Aget_storage_size(hid_t attr_id) except <hsize_t>0:
    cdef hsize_t r
    set_default_error_handler()
    r = _hdf5.H5Aget_storage_size(attr_id)
    if r==0:
        if set_exception():
            return <hsize_t>0
        else:
            raise RuntimeError("Unspecified error in H5Aget_storage_size (return value ==0)")
    return r

cdef hid_t H5Dcreate(hid_t loc_id, char *name, hid_t type_id, hid_t space_id, hid_t lcpl_id, hid_t dcpl_id, hid_t dapl_id) except <hid_t>-1:
    cdef hid_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Dcreate(loc_id, name, type_id, space_id, lcpl_id, dcpl_id, dapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dcreate (return value <0)")
    return r

cdef hid_t H5Dcreate_anon(hid_t file_id, hid_t type_id, hid_t space_id, hid_t plist_id, hid_t dapl_id) except <hid_t>-1:
    cdef hid_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Dcreate_anon(file_id, type_id, space_id, plist_id, dapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dcreate_anon (return value <0)")
    return r

cdef hid_t H5Dopen(hid_t loc_id, char *name, hid_t dapl_id ) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Dopen(loc_id, name, dapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dopen (return value <0)")
    return r

cdef herr_t H5Dclose(hid_t dset_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Dclose(dset_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dclose (return value <0)")
    return r

cdef hid_t H5Dget_space(hid_t dset_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Dget_space(dset_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dget_space (return value <0)")
    return r

cdef herr_t H5Dget_space_status(hid_t dset_id, H5D_space_status_t *status) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Dget_space_status(dset_id, status)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dget_space_status (return value <0)")
    return r

cdef hid_t H5Dget_type(hid_t dset_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Dget_type(dset_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dget_type (return value <0)")
    return r

cdef hid_t H5Dget_create_plist(hid_t dataset_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Dget_create_plist(dataset_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dget_create_plist (return value <0)")
    return r

cdef hid_t H5Dget_access_plist(hid_t dataset_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Dget_access_plist(dataset_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dget_access_plist (return value <0)")
    return r

cdef haddr_t H5Dget_offset(hid_t dset_id) except <haddr_t>0:
    cdef haddr_t r
    set_default_error_handler()
    r = _hdf5.H5Dget_offset(dset_id)
    if r==0:
        if set_exception():
            return <haddr_t>0
        else:
            raise RuntimeError("Unspecified error in H5Dget_offset (return value ==0)")
    return r

cdef hsize_t H5Dget_storage_size(hid_t dset_id) except? <hsize_t>0:
    cdef hsize_t r
    set_default_error_handler()
    r = _hdf5.H5Dget_storage_size(dset_id)
    if r==0:
        if set_exception():
            return <hsize_t>0
    return r

cdef herr_t H5Dread(hid_t dset_id, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Dread(dset_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dread (return value <0)")
    return r

cdef herr_t H5Dwrite(hid_t dset_id, hid_t mem_type, hid_t mem_space, hid_t file_space, hid_t xfer_plist, void* buf) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Dwrite(dset_id, mem_type, mem_space, file_space, xfer_plist, buf)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dwrite (return value <0)")
    return r

cdef herr_t H5Dextend(hid_t dataset_id, hsize_t *size) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Dextend(dataset_id, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dextend (return value <0)")
    return r

cdef herr_t H5Dfill(void *fill, hid_t fill_type_id, void *buf,  hid_t buf_type_id, hid_t space_id) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Dfill(fill, fill_type_id, buf, buf_type_id, space_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dfill (return value <0)")
    return r

cdef herr_t H5Dvlen_get_buf_size(hid_t dset_id, hid_t type_id, hid_t space_id, hsize_t *size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Dvlen_get_buf_size(dset_id, type_id, space_id, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dvlen_get_buf_size (return value <0)")
    return r

cdef herr_t H5Dvlen_reclaim(hid_t type_id, hid_t space_id,  hid_t plist, void *buf) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Dvlen_reclaim(type_id, space_id, plist, buf)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dvlen_reclaim (return value <0)")
    return r

cdef herr_t H5Diterate(void *buf, hid_t type_id, hid_t space_id,  H5D_operator_t op, void* operator_data) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Diterate(buf, type_id, space_id, op, operator_data)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Diterate (return value <0)")
    return r

cdef herr_t H5Dset_extent(hid_t dset_id, hsize_t* size) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Dset_extent(dset_id, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dset_extent (return value <0)")
    return r

cdef herr_t H5Dflush(hid_t dataset_id) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Dflush(dataset_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dflush (return value <0)")
    return r

cdef herr_t H5Drefresh(hid_t dataset_id) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Drefresh(dataset_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Drefresh (return value <0)")
    return r

cdef herr_t H5DOwrite_chunk(hid_t dset_id, hid_t dxpl_id, uint32_t filters, const hsize_t *offset, size_t data_size, const void *buf) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5DOwrite_chunk(dset_id, dxpl_id, filters, offset, data_size, buf)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5DOwrite_chunk (return value <0)")
    return r

cdef herr_t H5Dget_chunk_storage_size(hid_t dset_id, const hsize_t *offset, hsize_t *chunk_nbytes) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Dget_chunk_storage_size(dset_id, offset, chunk_nbytes)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dget_chunk_storage_size (return value <0)")
    return r

cdef herr_t H5Dread_chunk(hid_t dset_id, hid_t dxpl_id, const hsize_t *offset, uint32_t *filters, void *buf) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Dread_chunk(dset_id, dxpl_id, offset, filters, buf)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dread_chunk (return value <0)")
    return r

cdef herr_t H5Dget_num_chunks(hid_t dset_id, hid_t fspace_id, hsize_t *nchunks) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Dget_num_chunks(dset_id, fspace_id, nchunks)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dget_num_chunks (return value <0)")
    return r

cdef herr_t H5Dget_chunk_info(hid_t dset_id, hid_t fspace_id, hsize_t chk_idx, hsize_t *offset, unsigned *filter_mask, haddr_t *addr, hsize_t *size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Dget_chunk_info(dset_id, fspace_id, chk_idx, offset, filter_mask, addr, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dget_chunk_info (return value <0)")
    return r

cdef herr_t H5Dget_chunk_info_by_coord(hid_t dset_id, const hsize_t *offset, unsigned *filter_mask, haddr_t *addr, hsize_t *size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Dget_chunk_info_by_coord(dset_id, offset, filter_mask, addr, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dget_chunk_info_by_coord (return value <0)")
    return r

cdef herr_t H5Dchunk_iter(hid_t dset_id, hid_t dxpl_id, H5D_chunk_iter_op_t cb, void *op_data) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Dchunk_iter(dset_id, dxpl_id, cb, op_data)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Dchunk_iter (return value <0)")
    return r

cdef hid_t H5Fcreate(char *filename, unsigned int flags, hid_t create_plist, hid_t access_plist) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Fcreate(filename, flags, create_plist, access_plist)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fcreate (return value <0)")
    return r

cdef hid_t H5Fopen(char *name, unsigned flags, hid_t access_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Fopen(name, flags, access_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fopen (return value <0)")
    return r

cdef herr_t H5Fclose(hid_t file_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Fclose(file_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fclose (return value <0)")
    return r

cdef htri_t H5Fis_hdf5(char *name) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Fis_hdf5(name)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fis_hdf5 (return value <0)")
    return r

cdef herr_t H5Fflush(hid_t object_id, H5F_scope_t scope) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Fflush(object_id, scope)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fflush (return value <0)")
    return r

cdef hid_t H5Freopen(hid_t file_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Freopen(file_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Freopen (return value <0)")
    return r

cdef herr_t H5Fmount(hid_t loc_id, char *name, hid_t child_id, hid_t plist_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Fmount(loc_id, name, child_id, plist_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fmount (return value <0)")
    return r

cdef herr_t H5Funmount(hid_t loc_id, char *name) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Funmount(loc_id, name)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Funmount (return value <0)")
    return r

cdef herr_t H5Fget_filesize(hid_t file_id, hsize_t *size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Fget_filesize(file_id, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_filesize (return value <0)")
    return r

cdef hid_t H5Fget_create_plist(hid_t file_id ) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Fget_create_plist(file_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_create_plist (return value <0)")
    return r

cdef hid_t H5Fget_access_plist(hid_t file_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Fget_access_plist(file_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_access_plist (return value <0)")
    return r

cdef hssize_t H5Fget_freespace(hid_t file_id) except <hssize_t>-1:
    cdef hssize_t r
    set_default_error_handler()
    r = _hdf5.H5Fget_freespace(file_id)
    if r<0:
        if set_exception():
            return <hssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_freespace (return value <0)")
    return r

cdef ssize_t H5Fget_name(hid_t obj_id, char *name, size_t size) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5Fget_name(obj_id, name, size)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_name (return value <0)")
    return r

cdef int H5Fget_obj_count(hid_t file_id, unsigned int types) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Fget_obj_count(file_id, types)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_obj_count (return value <0)")
    return r

cdef int H5Fget_obj_ids(hid_t file_id, unsigned int types, int max_objs, hid_t *obj_id_list) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Fget_obj_ids(file_id, types, max_objs, obj_id_list)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_obj_ids (return value <0)")
    return r

cdef herr_t H5Fget_vfd_handle(hid_t file_id, hid_t fapl_id, void **file_handle) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Fget_vfd_handle(file_id, fapl_id, file_handle)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_vfd_handle (return value <0)")
    return r

cdef herr_t H5Fget_intent(hid_t file_id, unsigned int *intent) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Fget_intent(file_id, intent)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_intent (return value <0)")
    return r

cdef herr_t H5Fget_mdc_config(hid_t file_id, H5AC_cache_config_t *config_ptr) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Fget_mdc_config(file_id, config_ptr)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_mdc_config (return value <0)")
    return r

cdef herr_t H5Fget_mdc_hit_rate(hid_t file_id, double *hit_rate_ptr) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Fget_mdc_hit_rate(file_id, hit_rate_ptr)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_mdc_hit_rate (return value <0)")
    return r

cdef herr_t H5Fget_mdc_size(hid_t file_id, size_t *max_size_ptr, size_t *min_clean_size_ptr, size_t *cur_size_ptr, int *cur_num_entries_ptr) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Fget_mdc_size(file_id, max_size_ptr, min_clean_size_ptr, cur_size_ptr, cur_num_entries_ptr)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_mdc_size (return value <0)")
    return r

cdef herr_t H5Freset_mdc_hit_rate_stats(hid_t file_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Freset_mdc_hit_rate_stats(file_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Freset_mdc_hit_rate_stats (return value <0)")
    return r

cdef herr_t H5Fset_mdc_config(hid_t file_id, H5AC_cache_config_t *config_ptr) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Fset_mdc_config(file_id, config_ptr)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fset_mdc_config (return value <0)")
    return r

cdef ssize_t H5Fget_file_image(hid_t file_id, void *buf_ptr, size_t buf_len) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5Fget_file_image(file_id, buf_ptr, buf_len)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_file_image (return value <0)")
    return r

cdef hid_t H5LTopen_file_image(void *buf_ptr, size_t buf_len, unsigned int flags) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5LTopen_file_image(buf_ptr, buf_len, flags)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5LTopen_file_image (return value <0)")
    return r

cdef herr_t H5Fstart_swmr_write(hid_t file_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Fstart_swmr_write(file_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fstart_swmr_write (return value <0)")
    return r

cdef herr_t H5Freset_page_buffering_stats(hid_t file_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Freset_page_buffering_stats(file_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Freset_page_buffering_stats (return value <0)")
    return r

cdef herr_t H5Fget_page_buffering_stats(hid_t file_id, unsigned *accesses, unsigned *hits, unsigned *misses, unsigned *evictions, unsigned *bypasses) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Fget_page_buffering_stats(file_id, accesses, hits, misses, evictions, bypasses)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Fget_page_buffering_stats (return value <0)")
    return r

cdef hid_t H5FDregister(H5FD_class_t *cls_ptr) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5FDregister(cls_ptr)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5FDregister (return value <0)")
    return r

cdef herr_t H5FDunregister(hid_t driver_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5FDunregister(driver_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5FDunregister (return value <0)")
    return r

cdef herr_t H5Gclose(hid_t group_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Gclose(group_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Gclose (return value <0)")
    return r

cdef herr_t H5Glink2( hid_t curr_loc_id, char *current_name, H5G_link_t link_type, hid_t new_loc_id, char *new_name) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Glink2(curr_loc_id, current_name, link_type, new_loc_id, new_name)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Glink2 (return value <0)")
    return r

cdef herr_t H5Gunlink(hid_t file_id, char *name) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Gunlink(file_id, name)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Gunlink (return value <0)")
    return r

cdef herr_t H5Gmove2(hid_t src_loc_id, char *src_name, hid_t dst_loc_id, char *dst_name) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Gmove2(src_loc_id, src_name, dst_loc_id, dst_name)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Gmove2 (return value <0)")
    return r

cdef herr_t H5Gget_num_objs(hid_t loc_id, hsize_t*  num_obj) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Gget_num_objs(loc_id, num_obj)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Gget_num_objs (return value <0)")
    return r

cdef int H5Gget_objname_by_idx(hid_t loc_id, hsize_t idx, char *name, size_t size) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Gget_objname_by_idx(loc_id, idx, name, size)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Gget_objname_by_idx (return value <0)")
    return r

cdef int H5Gget_objtype_by_idx(hid_t loc_id, hsize_t idx) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Gget_objtype_by_idx(loc_id, idx)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Gget_objtype_by_idx (return value <0)")
    return r

cdef herr_t H5Giterate(hid_t loc_id, char *name, int *idx, H5G_iterate_t op, void* data) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Giterate(loc_id, name, idx, op, data)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Giterate (return value <0)")
    return r

cdef herr_t H5Gget_objinfo(hid_t loc_id, char* name, int follow_link, H5G_stat_t *statbuf) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Gget_objinfo(loc_id, name, follow_link, statbuf)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Gget_objinfo (return value <0)")
    return r

cdef herr_t H5Gget_linkval(hid_t loc_id, char *name, size_t size, char *value) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Gget_linkval(loc_id, name, size, value)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Gget_linkval (return value <0)")
    return r

cdef herr_t H5Gset_comment(hid_t loc_id, char *name, char *comment) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Gset_comment(loc_id, name, comment)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Gset_comment (return value <0)")
    return r

cdef int H5Gget_comment(hid_t loc_id, char *name, size_t bufsize, char *comment) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Gget_comment(loc_id, name, bufsize, comment)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Gget_comment (return value <0)")
    return r

cdef hid_t H5Gcreate_anon( hid_t loc_id, hid_t gcpl_id, hid_t gapl_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Gcreate_anon(loc_id, gcpl_id, gapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Gcreate_anon (return value <0)")
    return r

cdef hid_t H5Gcreate(hid_t loc_id, char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Gcreate(loc_id, name, lcpl_id, gcpl_id, gapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Gcreate (return value <0)")
    return r

cdef hid_t H5Gopen( hid_t loc_id, char * name, hid_t gapl_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Gopen(loc_id, name, gapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Gopen (return value <0)")
    return r

cdef herr_t H5Gget_info( hid_t group_id, H5G_info_t *group_info) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Gget_info(group_id, group_info)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Gget_info (return value <0)")
    return r

cdef herr_t H5Gget_info_by_name( hid_t loc_id, char *group_name, H5G_info_t *group_info, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Gget_info_by_name(loc_id, group_name, group_info, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Gget_info_by_name (return value <0)")
    return r

cdef hid_t H5Gget_create_plist(hid_t group_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Gget_create_plist(group_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Gget_create_plist (return value <0)")
    return r

cdef H5I_type_t H5Iget_type(hid_t obj_id) except <H5I_type_t>-1:
    cdef H5I_type_t r
    set_default_error_handler()
    r = _hdf5.H5Iget_type(obj_id)
    if r<0:
        if set_exception():
            return <H5I_type_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Iget_type (return value <0)")
    return r

cdef ssize_t H5Iget_name( hid_t obj_id, char *name, size_t size) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5Iget_name(obj_id, name, size)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Iget_name (return value <0)")
    return r

cdef hid_t H5Iget_file_id(hid_t obj_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Iget_file_id(obj_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Iget_file_id (return value <0)")
    return r

cdef int H5Idec_ref(hid_t obj_id) except <int>-1:
    cdef int r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Idec_ref(obj_id)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Idec_ref (return value <0)")
    return r

cdef int H5Iget_ref(hid_t obj_id) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Iget_ref(obj_id)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Iget_ref (return value <0)")
    return r

cdef int H5Iinc_ref(hid_t obj_id) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Iinc_ref(obj_id)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Iinc_ref (return value <0)")
    return r

cdef htri_t H5Iis_valid( hid_t obj_id ) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Iis_valid(obj_id)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Iis_valid (return value <0)")
    return r

cdef herr_t H5Lmove(hid_t src_loc, char *src_name, hid_t dst_loc, char *dst_name, hid_t lcpl_id, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Lmove(src_loc, src_name, dst_loc, dst_name, lcpl_id, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lmove (return value <0)")
    return r

cdef herr_t H5Lcopy(hid_t src_loc, char *src_name, hid_t dst_loc, char *dst_name, hid_t lcpl_id, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Lcopy(src_loc, src_name, dst_loc, dst_name, lcpl_id, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lcopy (return value <0)")
    return r

cdef herr_t H5Lcreate_hard(hid_t cur_loc, char *cur_name, hid_t dst_loc, char *dst_name, hid_t lcpl_id, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Lcreate_hard(cur_loc, cur_name, dst_loc, dst_name, lcpl_id, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lcreate_hard (return value <0)")
    return r

cdef herr_t H5Lcreate_soft(char *link_target, hid_t link_loc_id, char *link_name, hid_t lcpl_id, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Lcreate_soft(link_target, link_loc_id, link_name, lcpl_id, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lcreate_soft (return value <0)")
    return r

cdef herr_t H5Ldelete(hid_t loc_id, char *name, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Ldelete(loc_id, name, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Ldelete (return value <0)")
    return r

cdef herr_t H5Ldelete_by_idx(hid_t loc_id, char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Ldelete_by_idx(loc_id, group_name, idx_type, order, n, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Ldelete_by_idx (return value <0)")
    return r

cdef herr_t H5Lget_val(hid_t loc_id, char *name, void *bufout, size_t size, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Lget_val(loc_id, name, bufout, size, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lget_val (return value <0)")
    return r

cdef herr_t H5Lget_val_by_idx(hid_t loc_id, char *group_name,  H5_index_t idx_type, H5_iter_order_t order, hsize_t n, void *bufout, size_t size, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Lget_val_by_idx(loc_id, group_name, idx_type, order, n, bufout, size, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lget_val_by_idx (return value <0)")
    return r

cdef htri_t H5Lexists(hid_t loc_id, char *name, hid_t lapl_id) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Lexists(loc_id, name, lapl_id)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lexists (return value <0)")
    return r

cdef herr_t H5Lget_info(hid_t loc_id, char *name, H5L_info_t *linfo, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Lget_info(loc_id, name, linfo, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lget_info (return value <0)")
    return r

cdef herr_t H5Lget_info_by_idx(hid_t loc_id, char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, H5L_info_t *linfo, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Lget_info_by_idx(loc_id, group_name, idx_type, order, n, linfo, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lget_info_by_idx (return value <0)")
    return r

cdef ssize_t H5Lget_name_by_idx(hid_t loc_id, char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, char *name, size_t size, hid_t lapl_id) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5Lget_name_by_idx(loc_id, group_name, idx_type, order, n, name, size, lapl_id)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lget_name_by_idx (return value <0)")
    return r

cdef herr_t H5Literate(hid_t grp_id, H5_index_t idx_type, H5_iter_order_t order, hsize_t *idx, H5L_iterate_t op, void *op_data) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Literate(grp_id, idx_type, order, idx, op, op_data)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Literate (return value <0)")
    return r

cdef herr_t H5Literate_by_name(hid_t loc_id, char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t *idx, H5L_iterate_t op, void *op_data, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Literate_by_name(loc_id, group_name, idx_type, order, idx, op, op_data, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Literate_by_name (return value <0)")
    return r

cdef herr_t H5Lvisit(hid_t grp_id, H5_index_t idx_type, H5_iter_order_t order, H5L_iterate_t op, void *op_data) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Lvisit(grp_id, idx_type, order, op, op_data)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lvisit (return value <0)")
    return r

cdef herr_t H5Lvisit_by_name(hid_t loc_id, char *group_name, H5_index_t idx_type, H5_iter_order_t order, H5L_iterate_t op, void *op_data, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Lvisit_by_name(loc_id, group_name, idx_type, order, op, op_data, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lvisit_by_name (return value <0)")
    return r

cdef herr_t H5Lunpack_elink_val(void *ext_linkval, size_t link_size, unsigned *flags, const char **filename, const char **obj_path) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Lunpack_elink_val(ext_linkval, link_size, flags, filename, obj_path)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lunpack_elink_val (return value <0)")
    return r

cdef herr_t H5Lcreate_external(char *file_name, char *obj_name, hid_t link_loc_id, char *link_name, hid_t lcpl_id, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Lcreate_external(file_name, obj_name, link_loc_id, link_name, lcpl_id, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Lcreate_external (return value <0)")
    return r

cdef hid_t H5Oopen(hid_t loc_id, char *name, hid_t lapl_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Oopen(loc_id, name, lapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Oopen (return value <0)")
    return r

cdef hid_t H5Oopen_by_addr(hid_t loc_id, haddr_t addr) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Oopen_by_addr(loc_id, addr)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Oopen_by_addr (return value <0)")
    return r

cdef hid_t H5Oopen_by_idx(hid_t loc_id, char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, hid_t lapl_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Oopen_by_idx(loc_id, group_name, idx_type, order, n, lapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Oopen_by_idx (return value <0)")
    return r

cdef herr_t H5Oget_info(hid_t loc_id, H5O_info_t *oinfo) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Oget_info(loc_id, oinfo)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Oget_info (return value <0)")
    return r

cdef herr_t H5Oget_info_by_name(hid_t loc_id, char *name, H5O_info_t *oinfo, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Oget_info_by_name(loc_id, name, oinfo, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Oget_info_by_name (return value <0)")
    return r

cdef herr_t H5Oget_info_by_idx(hid_t loc_id, char *group_name,  H5_index_t idx_type, H5_iter_order_t order, hsize_t n, H5O_info_t *oinfo, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Oget_info_by_idx(loc_id, group_name, idx_type, order, n, oinfo, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Oget_info_by_idx (return value <0)")
    return r

cdef herr_t H5Olink(hid_t obj_id, hid_t new_loc_id, char *new_name, hid_t lcpl_id, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Olink(obj_id, new_loc_id, new_name, lcpl_id, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Olink (return value <0)")
    return r

cdef herr_t H5Ocopy(hid_t src_loc_id, char *src_name, hid_t dst_loc_id,  char *dst_name, hid_t ocpypl_id, hid_t lcpl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Ocopy(src_loc_id, src_name, dst_loc_id, dst_name, ocpypl_id, lcpl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Ocopy (return value <0)")
    return r

cdef herr_t H5Oincr_refcount(hid_t object_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Oincr_refcount(object_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Oincr_refcount (return value <0)")
    return r

cdef herr_t H5Odecr_refcount(hid_t object_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Odecr_refcount(object_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Odecr_refcount (return value <0)")
    return r

cdef herr_t H5Oset_comment(hid_t obj_id, char *comment) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Oset_comment(obj_id, comment)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Oset_comment (return value <0)")
    return r

cdef herr_t H5Oset_comment_by_name(hid_t loc_id, char *name,  char *comment, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Oset_comment_by_name(loc_id, name, comment, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Oset_comment_by_name (return value <0)")
    return r

cdef ssize_t H5Oget_comment(hid_t obj_id, char *comment, size_t bufsize) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5Oget_comment(obj_id, comment, bufsize)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Oget_comment (return value <0)")
    return r

cdef ssize_t H5Oget_comment_by_name(hid_t loc_id, char *name, char *comment, size_t bufsize, hid_t lapl_id) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5Oget_comment_by_name(loc_id, name, comment, bufsize, lapl_id)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Oget_comment_by_name (return value <0)")
    return r

cdef herr_t H5Ovisit(hid_t obj_id, H5_index_t idx_type, H5_iter_order_t order,  H5O_iterate_t op, void *op_data) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Ovisit(obj_id, idx_type, order, op, op_data)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Ovisit (return value <0)")
    return r

cdef herr_t H5Ovisit_by_name(hid_t loc_id, char *obj_name, H5_index_t idx_type, H5_iter_order_t order, H5O_iterate_t op, void *op_data, hid_t lapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Ovisit_by_name(loc_id, obj_name, idx_type, order, op, op_data, lapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Ovisit_by_name (return value <0)")
    return r

cdef herr_t H5Oclose(hid_t object_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Oclose(object_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Oclose (return value <0)")
    return r

cdef htri_t H5Oexists_by_name(hid_t loc_id, char * name, hid_t lapl_id ) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Oexists_by_name(loc_id, name, lapl_id)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Oexists_by_name (return value <0)")
    return r

cdef hid_t H5Pcreate(hid_t plist_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Pcreate(plist_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pcreate (return value <0)")
    return r

cdef hid_t H5Pcopy(hid_t plist_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Pcopy(plist_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pcopy (return value <0)")
    return r

cdef hid_t H5Pget_class(hid_t plist_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_class(plist_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_class (return value <0)")
    return r

cdef herr_t H5Pclose(hid_t plist_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pclose(plist_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pclose (return value <0)")
    return r

cdef htri_t H5Pequal( hid_t id1, hid_t id2 ) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Pequal(id1, id2)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pequal (return value <0)")
    return r

cdef herr_t H5Pclose_class(hid_t id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pclose_class(id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pclose_class (return value <0)")
    return r

cdef herr_t H5Pget_version(hid_t plist, unsigned int *super_, unsigned int* freelist,  unsigned int *stab, unsigned int *shhdr) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_version(plist, super_, freelist, stab, shhdr)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_version (return value <0)")
    return r

cdef herr_t H5Pset_userblock(hid_t plist, hsize_t size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_userblock(plist, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_userblock (return value <0)")
    return r

cdef herr_t H5Pget_userblock(hid_t plist, hsize_t * size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_userblock(plist, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_userblock (return value <0)")
    return r

cdef herr_t H5Pset_sizes(hid_t plist, size_t sizeof_addr, size_t sizeof_size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_sizes(plist, sizeof_addr, sizeof_size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_sizes (return value <0)")
    return r

cdef herr_t H5Pget_sizes(hid_t plist, size_t *sizeof_addr, size_t *sizeof_size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_sizes(plist, sizeof_addr, sizeof_size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_sizes (return value <0)")
    return r

cdef herr_t H5Pset_sym_k(hid_t plist, unsigned int ik, unsigned int lk) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_sym_k(plist, ik, lk)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_sym_k (return value <0)")
    return r

cdef herr_t H5Pget_sym_k(hid_t plist, unsigned int *ik, unsigned int *lk) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_sym_k(plist, ik, lk)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_sym_k (return value <0)")
    return r

cdef herr_t H5Pset_istore_k(hid_t plist, unsigned int ik) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_istore_k(plist, ik)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_istore_k (return value <0)")
    return r

cdef herr_t H5Pget_istore_k(hid_t plist, unsigned int *ik) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_istore_k(plist, ik)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_istore_k (return value <0)")
    return r

cdef herr_t H5Pset_file_space_strategy(hid_t fcpl, H5F_fspace_strategy_t strategy, hbool_t persist, hsize_t threshold) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_file_space_strategy(fcpl, strategy, persist, threshold)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_file_space_strategy (return value <0)")
    return r

cdef herr_t H5Pget_file_space_strategy(hid_t fcpl, H5F_fspace_strategy_t *strategy, hbool_t *persist, hsize_t *threshold) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_file_space_strategy(fcpl, strategy, persist, threshold)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_file_space_strategy (return value <0)")
    return r

cdef herr_t H5Pset_file_space_page_size(hid_t plist_id, hsize_t fsp_size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_file_space_page_size(plist_id, fsp_size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_file_space_page_size (return value <0)")
    return r

cdef herr_t H5Pget_file_space_page_size(hid_t plist_id, hsize_t *fsp_size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_file_space_page_size(plist_id, fsp_size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_file_space_page_size (return value <0)")
    return r

cdef herr_t H5Pset_fclose_degree(hid_t fapl_id, H5F_close_degree_t fc_degree) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_fclose_degree(fapl_id, fc_degree)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_fclose_degree (return value <0)")
    return r

cdef herr_t H5Pget_fclose_degree(hid_t fapl_id, H5F_close_degree_t *fc_degree) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_fclose_degree(fapl_id, fc_degree)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_fclose_degree (return value <0)")
    return r

cdef herr_t H5Pset_fapl_core( hid_t fapl_id, size_t increment, hbool_t backing_store) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_fapl_core(fapl_id, increment, backing_store)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_fapl_core (return value <0)")
    return r

cdef herr_t H5Pget_fapl_core( hid_t fapl_id, size_t *increment, hbool_t *backing_store) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_fapl_core(fapl_id, increment, backing_store)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_fapl_core (return value <0)")
    return r

cdef herr_t H5Pset_fapl_family( hid_t fapl_id,  hsize_t memb_size, hid_t memb_fapl_id ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_fapl_family(fapl_id, memb_size, memb_fapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_fapl_family (return value <0)")
    return r

cdef herr_t H5Pget_fapl_family( hid_t fapl_id, hsize_t *memb_size, hid_t *memb_fapl_id ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_fapl_family(fapl_id, memb_size, memb_fapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_fapl_family (return value <0)")
    return r

cdef herr_t H5Pset_family_offset( hid_t fapl_id, hsize_t offset) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_family_offset(fapl_id, offset)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_family_offset (return value <0)")
    return r

cdef herr_t H5Pget_family_offset( hid_t fapl_id, hsize_t *offset) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_family_offset(fapl_id, offset)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_family_offset (return value <0)")
    return r

cdef herr_t H5Pset_fapl_log(hid_t fapl_id, char *logfile, unsigned int flags, size_t buf_size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_fapl_log(fapl_id, logfile, flags, buf_size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_fapl_log (return value <0)")
    return r

cdef herr_t H5Pset_fapl_multi(hid_t fapl_id, H5FD_mem_t *memb_map, hid_t *memb_fapl, const char * const *memb_name, haddr_t *memb_addr, hbool_t relax) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_fapl_multi(fapl_id, memb_map, memb_fapl, memb_name, memb_addr, relax)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_fapl_multi (return value <0)")
    return r

cdef herr_t H5Pset_cache(hid_t plist_id, int mdc_nelmts, int rdcc_nelmts,  size_t rdcc_nbytes, double rdcc_w0) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_cache(plist_id, mdc_nelmts, rdcc_nelmts, rdcc_nbytes, rdcc_w0)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_cache (return value <0)")
    return r

cdef herr_t H5Pget_cache(hid_t plist_id, int *mdc_nelmts, size_t *rdcc_nelmts, size_t *rdcc_nbytes, double *rdcc_w0) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_cache(plist_id, mdc_nelmts, rdcc_nelmts, rdcc_nbytes, rdcc_w0)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_cache (return value <0)")
    return r

cdef herr_t H5Pset_fapl_sec2(hid_t fapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_fapl_sec2(fapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_fapl_sec2 (return value <0)")
    return r

cdef herr_t H5Pset_fapl_stdio(hid_t fapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_fapl_stdio(fapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_fapl_stdio (return value <0)")
    return r

cdef herr_t H5Pset_fapl_split(hid_t fapl_id, const char *meta_ext, hid_t meta_plist_id, const char *raw_ext, hid_t raw_plist_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_fapl_split(fapl_id, meta_ext, meta_plist_id, raw_ext, raw_plist_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_fapl_split (return value <0)")
    return r

cdef herr_t H5Pset_driver(hid_t plist_id, hid_t driver_id, void *driver_info) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_driver(plist_id, driver_id, driver_info)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_driver (return value <0)")
    return r

cdef hid_t H5Pget_driver(hid_t fapl_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_driver(fapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_driver (return value <0)")
    return r

cdef const void* H5Pget_driver_info(hid_t plist_id) except <void*>NULL:
    cdef const void* r
    set_default_error_handler()
    r = _hdf5.H5Pget_driver_info(plist_id)
    if r==NULL:
        if set_exception():
            return <void*>NULL
        else:
            raise RuntimeError("Unspecified error in H5Pget_driver_info (return value ==NULL)")
    return r

cdef herr_t H5Pget_mdc_config(hid_t plist_id, H5AC_cache_config_t *config_ptr) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_mdc_config(plist_id, config_ptr)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_mdc_config (return value <0)")
    return r

cdef herr_t H5Pset_mdc_config(hid_t plist_id, H5AC_cache_config_t *config_ptr) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_mdc_config(plist_id, config_ptr)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_mdc_config (return value <0)")
    return r

cdef herr_t H5Pset_meta_block_size(hid_t fapl_id, hsize_t size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_meta_block_size(fapl_id, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_meta_block_size (return value <0)")
    return r

cdef herr_t H5Pget_meta_block_size(hid_t fapl_id, hsize_t * size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_meta_block_size(fapl_id, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_meta_block_size (return value <0)")
    return r

cdef herr_t H5Pset_file_image(hid_t plist_id, void *buf_ptr, size_t buf_len) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_file_image(plist_id, buf_ptr, buf_len)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_file_image (return value <0)")
    return r

cdef herr_t H5Pset_page_buffer_size(hid_t plist_id, size_t buf_size, unsigned min_meta_per, unsigned min_raw_per) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_page_buffer_size(plist_id, buf_size, min_meta_per, min_raw_per)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_page_buffer_size (return value <0)")
    return r

cdef herr_t H5Pget_page_buffer_size(hid_t plist_id, size_t *buf_size, unsigned *min_meta_per, unsigned *min_raw_per) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_page_buffer_size(plist_id, buf_size, min_meta_per, min_raw_per)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_page_buffer_size (return value <0)")
    return r

cdef herr_t H5Pget_file_locking(hid_t fapl_id, hbool_t *use_file_locking, hbool_t *ignore_when_disabled) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_file_locking(fapl_id, use_file_locking, ignore_when_disabled)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_file_locking (return value <0)")
    return r

cdef herr_t H5Pset_file_locking(hid_t fapl_id, hbool_t use_file_locking, hbool_t ignore_when_disabled) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_file_locking(fapl_id, use_file_locking, ignore_when_disabled)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_file_locking (return value <0)")
    return r

cdef herr_t H5Pset_layout(hid_t plist, int layout) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_layout(plist, layout)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_layout (return value <0)")
    return r

cdef H5D_layout_t H5Pget_layout(hid_t plist) except <H5D_layout_t>-1:
    cdef H5D_layout_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_layout(plist)
    if r<0:
        if set_exception():
            return <H5D_layout_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_layout (return value <0)")
    return r

cdef herr_t H5Pset_chunk(hid_t plist, int ndims, hsize_t * dim) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_chunk(plist, ndims, dim)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_chunk (return value <0)")
    return r

cdef int H5Pget_chunk(hid_t plist, int max_ndims, hsize_t * dims ) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Pget_chunk(plist, max_ndims, dims)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_chunk (return value <0)")
    return r

cdef herr_t H5Pset_deflate( hid_t plist, int level) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_deflate(plist, level)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_deflate (return value <0)")
    return r

cdef herr_t H5Pset_fill_value(hid_t plist_id, hid_t type_id, void *value ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_fill_value(plist_id, type_id, value)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_fill_value (return value <0)")
    return r

cdef herr_t H5Pget_fill_value(hid_t plist_id, hid_t type_id, void *value ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_fill_value(plist_id, type_id, value)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_fill_value (return value <0)")
    return r

cdef herr_t H5Pfill_value_defined(hid_t plist_id, H5D_fill_value_t *status ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pfill_value_defined(plist_id, status)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pfill_value_defined (return value <0)")
    return r

cdef herr_t H5Pset_fill_time(hid_t plist_id, H5D_fill_time_t fill_time ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_fill_time(plist_id, fill_time)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_fill_time (return value <0)")
    return r

cdef herr_t H5Pget_fill_time(hid_t plist_id, H5D_fill_time_t *fill_time ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_fill_time(plist_id, fill_time)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_fill_time (return value <0)")
    return r

cdef herr_t H5Pset_alloc_time(hid_t plist_id, H5D_alloc_time_t alloc_time ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_alloc_time(plist_id, alloc_time)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_alloc_time (return value <0)")
    return r

cdef herr_t H5Pget_alloc_time(hid_t plist_id, H5D_alloc_time_t *alloc_time ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_alloc_time(plist_id, alloc_time)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_alloc_time (return value <0)")
    return r

cdef herr_t H5Pset_filter(hid_t plist, H5Z_filter_t filter, unsigned int flags, size_t cd_nelmts, unsigned int* cd_values ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_filter(plist, filter, flags, cd_nelmts, cd_values)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_filter (return value <0)")
    return r

cdef htri_t H5Pall_filters_avail(hid_t dcpl_id) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Pall_filters_avail(dcpl_id)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pall_filters_avail (return value <0)")
    return r

cdef int H5Pget_nfilters(hid_t plist) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Pget_nfilters(plist)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_nfilters (return value <0)")
    return r

cdef H5Z_filter_t H5Pget_filter(hid_t plist, unsigned int filter_number,   unsigned int *flags, size_t *cd_nelmts,  unsigned int* cd_values, size_t namelen, char* name, unsigned int* filter_config) except <H5Z_filter_t>-1:
    cdef H5Z_filter_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_filter(plist, filter_number, flags, cd_nelmts, cd_values, namelen, name, filter_config)
    if r<0:
        if set_exception():
            return <H5Z_filter_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_filter (return value <0)")
    return r

cdef herr_t H5Pget_filter_by_id( hid_t plist_id, H5Z_filter_t filter,  unsigned int *flags, size_t *cd_nelmts,  unsigned int* cd_values, size_t namelen, char* name, unsigned int* filter_config) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_filter_by_id(plist_id, filter, flags, cd_nelmts, cd_values, namelen, name, filter_config)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_filter_by_id (return value <0)")
    return r

cdef herr_t H5Pmodify_filter(hid_t plist, H5Z_filter_t filter, unsigned int flags, size_t cd_nelmts, unsigned int *cd_values) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pmodify_filter(plist, filter, flags, cd_nelmts, cd_values)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pmodify_filter (return value <0)")
    return r

cdef herr_t H5Premove_filter(hid_t plist, H5Z_filter_t filter ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Premove_filter(plist, filter)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Premove_filter (return value <0)")
    return r

cdef herr_t H5Pset_fletcher32(hid_t plist) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_fletcher32(plist)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_fletcher32 (return value <0)")
    return r

cdef herr_t H5Pset_shuffle(hid_t plist_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_shuffle(plist_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_shuffle (return value <0)")
    return r

cdef herr_t H5Pset_szip(hid_t plist, unsigned int options_mask, unsigned int pixels_per_block) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_szip(plist, options_mask, pixels_per_block)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_szip (return value <0)")
    return r

cdef herr_t H5Pset_scaleoffset(hid_t plist, H5Z_SO_scale_type_t scale_type, int scale_factor) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_scaleoffset(plist, scale_type, scale_factor)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_scaleoffset (return value <0)")
    return r

cdef herr_t H5Pset_external(hid_t plist_id, const char *name, off_t offset, hsize_t size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_external(plist_id, name, offset, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_external (return value <0)")
    return r

cdef int H5Pget_external_count(hid_t plist_id) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Pget_external_count(plist_id)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_external_count (return value <0)")
    return r

cdef herr_t H5Pget_external(hid_t plist, unsigned idx, size_t name_size, char *name, off_t *offset, hsize_t *size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_external(plist, idx, name_size, name, offset, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_external (return value <0)")
    return r

cdef ssize_t H5Pget_virtual_dsetname(hid_t dcpl_id, size_t index, char *name, size_t size) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_virtual_dsetname(dcpl_id, index, name, size)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_virtual_dsetname (return value <0)")
    return r

cdef ssize_t H5Pget_virtual_filename(hid_t dcpl_id, size_t index, char *name, size_t size) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_virtual_filename(dcpl_id, index, name, size)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_virtual_filename (return value <0)")
    return r

cdef herr_t H5Pget_virtual_count(hid_t dcpl_id, size_t *count) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_virtual_count(dcpl_id, count)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_virtual_count (return value <0)")
    return r

cdef herr_t H5Pset_virtual(hid_t dcpl_id, hid_t vspace_id, const char *src_file_name, const char *src_dset_name, hid_t src_space_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_virtual(dcpl_id, vspace_id, src_file_name, src_dset_name, src_space_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_virtual (return value <0)")
    return r

cdef hid_t H5Pget_virtual_vspace(hid_t dcpl_id, size_t index) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_virtual_vspace(dcpl_id, index)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_virtual_vspace (return value <0)")
    return r

cdef hid_t H5Pget_virtual_srcspace(hid_t dcpl_id, size_t index) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_virtual_srcspace(dcpl_id, index)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_virtual_srcspace (return value <0)")
    return r

cdef herr_t H5Pset_edc_check(hid_t plist, H5Z_EDC_t check) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_edc_check(plist, check)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_edc_check (return value <0)")
    return r

cdef H5Z_EDC_t H5Pget_edc_check(hid_t plist) except <H5Z_EDC_t>-1:
    cdef H5Z_EDC_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_edc_check(plist)
    if r<0:
        if set_exception():
            return <H5Z_EDC_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_edc_check (return value <0)")
    return r

cdef herr_t H5Pset_chunk_cache( hid_t dapl_id, size_t rdcc_nslots, size_t rdcc_nbytes, double rdcc_w0 ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_chunk_cache(dapl_id, rdcc_nslots, rdcc_nbytes, rdcc_w0)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_chunk_cache (return value <0)")
    return r

cdef herr_t H5Pget_chunk_cache( hid_t dapl_id, size_t *rdcc_nslots, size_t *rdcc_nbytes, double *rdcc_w0 ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_chunk_cache(dapl_id, rdcc_nslots, rdcc_nbytes, rdcc_w0)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_chunk_cache (return value <0)")
    return r

cdef herr_t H5Pget_efile_prefix(hid_t dapl_id, char *prefix, ssize_t size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_efile_prefix(dapl_id, prefix, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_efile_prefix (return value <0)")
    return r

cdef herr_t H5Pset_efile_prefix(hid_t dapl_id, char *prefix) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_efile_prefix(dapl_id, prefix)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_efile_prefix (return value <0)")
    return r

cdef herr_t H5Pset_virtual_view(hid_t plist_id, H5D_vds_view_t view) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_virtual_view(plist_id, view)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_virtual_view (return value <0)")
    return r

cdef herr_t H5Pget_virtual_view(hid_t plist_id, H5D_vds_view_t *view) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_virtual_view(plist_id, view)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_virtual_view (return value <0)")
    return r

cdef herr_t H5Pset_virtual_printf_gap(hid_t plist_id, hsize_t gap_size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_virtual_printf_gap(plist_id, gap_size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_virtual_printf_gap (return value <0)")
    return r

cdef herr_t H5Pget_virtual_printf_gap(hid_t plist_id, hsize_t *gap_size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_virtual_printf_gap(plist_id, gap_size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_virtual_printf_gap (return value <0)")
    return r

cdef ssize_t H5Pget_virtual_prefix(hid_t dapl_id, char *prefix, ssize_t size) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_virtual_prefix(dapl_id, prefix, size)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_virtual_prefix (return value <0)")
    return r

cdef herr_t H5Pset_virtual_prefix(hid_t dapl_id, char *prefix) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_virtual_prefix(dapl_id, prefix)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_virtual_prefix (return value <0)")
    return r

cdef herr_t H5Pset_sieve_buf_size(hid_t fapl_id, size_t size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_sieve_buf_size(fapl_id, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_sieve_buf_size (return value <0)")
    return r

cdef herr_t H5Pget_sieve_buf_size(hid_t fapl_id, size_t *size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_sieve_buf_size(fapl_id, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_sieve_buf_size (return value <0)")
    return r

cdef herr_t H5Pset_nlinks(hid_t plist_id, size_t nlinks) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_nlinks(plist_id, nlinks)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_nlinks (return value <0)")
    return r

cdef herr_t H5Pget_nlinks(hid_t plist_id, size_t *nlinks) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_nlinks(plist_id, nlinks)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_nlinks (return value <0)")
    return r

cdef herr_t H5Pset_elink_prefix(hid_t plist_id, char *prefix) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_elink_prefix(plist_id, prefix)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_elink_prefix (return value <0)")
    return r

cdef ssize_t H5Pget_elink_prefix(hid_t plist_id, char *prefix, size_t size) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_elink_prefix(plist_id, prefix, size)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_elink_prefix (return value <0)")
    return r

cdef hid_t H5Pget_elink_fapl(hid_t lapl_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_elink_fapl(lapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_elink_fapl (return value <0)")
    return r

cdef herr_t H5Pset_elink_fapl(hid_t lapl_id, hid_t fapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_elink_fapl(lapl_id, fapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_elink_fapl (return value <0)")
    return r

cdef herr_t H5Pget_elink_acc_flags(hid_t lapl_id, unsigned int *flags) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_elink_acc_flags(lapl_id, flags)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_elink_acc_flags (return value <0)")
    return r

cdef herr_t H5Pset_elink_acc_flags(hid_t lapl_id, unsigned int flags) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_elink_acc_flags(lapl_id, flags)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_elink_acc_flags (return value <0)")
    return r

cdef herr_t H5Pset_create_intermediate_group(hid_t plist_id, unsigned crt_intmd) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_create_intermediate_group(plist_id, crt_intmd)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_create_intermediate_group (return value <0)")
    return r

cdef herr_t H5Pget_create_intermediate_group(hid_t plist_id, unsigned *crt_intmd) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_create_intermediate_group(plist_id, crt_intmd)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_create_intermediate_group (return value <0)")
    return r

cdef herr_t H5Pset_copy_object(hid_t plist_id, unsigned crt_intmd) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_copy_object(plist_id, crt_intmd)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_copy_object (return value <0)")
    return r

cdef herr_t H5Pget_copy_object(hid_t plist_id, unsigned *crt_intmd) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_copy_object(plist_id, crt_intmd)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_copy_object (return value <0)")
    return r

cdef herr_t H5Pset_char_encoding(hid_t plist_id, H5T_cset_t encoding) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_char_encoding(plist_id, encoding)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_char_encoding (return value <0)")
    return r

cdef herr_t H5Pget_char_encoding(hid_t plist_id, H5T_cset_t *encoding) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_char_encoding(plist_id, encoding)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_char_encoding (return value <0)")
    return r

cdef herr_t H5Pset_attr_creation_order(hid_t plist_id, unsigned crt_order_flags) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_attr_creation_order(plist_id, crt_order_flags)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_attr_creation_order (return value <0)")
    return r

cdef herr_t H5Pget_attr_creation_order(hid_t plist_id, unsigned *crt_order_flags) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_attr_creation_order(plist_id, crt_order_flags)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_attr_creation_order (return value <0)")
    return r

cdef herr_t H5Pset_obj_track_times( hid_t ocpl_id, hbool_t track_times ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_obj_track_times(ocpl_id, track_times)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_obj_track_times (return value <0)")
    return r

cdef herr_t H5Pget_obj_track_times( hid_t ocpl_id, hbool_t *track_times ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_obj_track_times(ocpl_id, track_times)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_obj_track_times (return value <0)")
    return r

cdef herr_t H5Pset_local_heap_size_hint(hid_t plist_id, size_t size_hint) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_local_heap_size_hint(plist_id, size_hint)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_local_heap_size_hint (return value <0)")
    return r

cdef herr_t H5Pget_local_heap_size_hint(hid_t plist_id, size_t *size_hint) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_local_heap_size_hint(plist_id, size_hint)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_local_heap_size_hint (return value <0)")
    return r

cdef herr_t H5Pset_link_phase_change(hid_t plist_id, unsigned max_compact, unsigned min_dense) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_link_phase_change(plist_id, max_compact, min_dense)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_link_phase_change (return value <0)")
    return r

cdef herr_t H5Pget_link_phase_change(hid_t plist_id, unsigned *max_compact , unsigned *min_dense) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_link_phase_change(plist_id, max_compact, min_dense)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_link_phase_change (return value <0)")
    return r

cdef herr_t H5Pset_attr_phase_change(hid_t ocpl_id, unsigned max_compact, unsigned min_dense) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_attr_phase_change(ocpl_id, max_compact, min_dense)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_attr_phase_change (return value <0)")
    return r

cdef herr_t H5Pget_attr_phase_change(hid_t ocpl_id, unsigned *max_compact , unsigned *min_dense) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_attr_phase_change(ocpl_id, max_compact, min_dense)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_attr_phase_change (return value <0)")
    return r

cdef herr_t H5Pset_est_link_info(hid_t plist_id, unsigned est_num_entries, unsigned est_name_len) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_est_link_info(plist_id, est_num_entries, est_name_len)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_est_link_info (return value <0)")
    return r

cdef herr_t H5Pget_est_link_info(hid_t plist_id, unsigned *est_num_entries , unsigned *est_name_len) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_est_link_info(plist_id, est_num_entries, est_name_len)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_est_link_info (return value <0)")
    return r

cdef herr_t H5Pset_link_creation_order(hid_t plist_id, unsigned crt_order_flags) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_link_creation_order(plist_id, crt_order_flags)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_link_creation_order (return value <0)")
    return r

cdef herr_t H5Pget_link_creation_order(hid_t plist_id, unsigned *crt_order_flags) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_link_creation_order(plist_id, crt_order_flags)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_link_creation_order (return value <0)")
    return r

cdef herr_t H5Pset_libver_bounds(hid_t fapl_id, H5F_libver_t libver_low, H5F_libver_t libver_high) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_libver_bounds(fapl_id, libver_low, libver_high)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_libver_bounds (return value <0)")
    return r

cdef herr_t H5Pget_libver_bounds(hid_t fapl_id, H5F_libver_t *libver_low, H5F_libver_t *libver_high) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_libver_bounds(fapl_id, libver_low, libver_high)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_libver_bounds (return value <0)")
    return r

cdef herr_t H5Pset_alignment(hid_t plist_id, hsize_t threshold, hsize_t alignment) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pset_alignment(plist_id, threshold, alignment)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pset_alignment (return value <0)")
    return r

cdef herr_t H5Pget_alignment(hid_t plist_id, hsize_t *threshold, hsize_t *alignment) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Pget_alignment(plist_id, threshold, alignment)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Pget_alignment (return value <0)")
    return r

cdef herr_t H5PLappend(const char *search_path) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5PLappend(search_path)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5PLappend (return value <0)")
    return r

cdef herr_t H5PLprepend(const char *search_path) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5PLprepend(search_path)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5PLprepend (return value <0)")
    return r

cdef herr_t H5PLreplace(const char *search_path, unsigned int index) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5PLreplace(search_path, index)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5PLreplace (return value <0)")
    return r

cdef herr_t H5PLinsert(const char *search_path, unsigned int index) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5PLinsert(search_path, index)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5PLinsert (return value <0)")
    return r

cdef herr_t H5PLremove(unsigned int index) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5PLremove(index)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5PLremove (return value <0)")
    return r

cdef ssize_t H5PLget(unsigned int index, char *path_buf, size_t buf_size) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5PLget(index, path_buf, buf_size)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5PLget (return value <0)")
    return r

cdef herr_t H5PLsize(unsigned int *num_paths) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5PLsize(num_paths)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5PLsize (return value <0)")
    return r

cdef herr_t H5Rcreate(void *ref, hid_t loc_id, char *name, H5R_type_t ref_type,  hid_t space_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Rcreate(ref, loc_id, name, ref_type, space_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Rcreate (return value <0)")
    return r

cdef hid_t H5Rdereference(hid_t obj_id, hid_t oapl_id, H5R_type_t ref_type, void *ref) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Rdereference(obj_id, oapl_id, ref_type, ref)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Rdereference (return value <0)")
    return r

cdef hid_t H5Rget_region(hid_t dataset, H5R_type_t ref_type, void *ref) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Rget_region(dataset, ref_type, ref)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Rget_region (return value <0)")
    return r

cdef herr_t H5Rget_obj_type(hid_t id, H5R_type_t ref_type, void *ref, H5O_type_t *obj_type) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Rget_obj_type(id, ref_type, ref, obj_type)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Rget_obj_type (return value <0)")
    return r

cdef ssize_t H5Rget_name(hid_t loc_id, H5R_type_t ref_type, void *ref, char *name, size_t size) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5Rget_name(loc_id, ref_type, ref, name, size)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Rget_name (return value <0)")
    return r

cdef hid_t H5Screate(H5S_class_t type) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Screate(type)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Screate (return value <0)")
    return r

cdef hid_t H5Scopy(hid_t space_id ) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Scopy(space_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Scopy (return value <0)")
    return r

cdef herr_t H5Sclose(hid_t space_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Sclose(space_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sclose (return value <0)")
    return r

cdef hid_t H5Screate_simple(int rank, hsize_t *dims, hsize_t *maxdims) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Screate_simple(rank, dims, maxdims)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Screate_simple (return value <0)")
    return r

cdef htri_t H5Sis_simple(hid_t space_id) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Sis_simple(space_id)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sis_simple (return value <0)")
    return r

cdef herr_t H5Soffset_simple(hid_t space_id, hssize_t *offset ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Soffset_simple(space_id, offset)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Soffset_simple (return value <0)")
    return r

cdef int H5Sget_simple_extent_ndims(hid_t space_id) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Sget_simple_extent_ndims(space_id)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Sget_simple_extent_ndims (return value <0)")
    return r

cdef int H5Sget_simple_extent_dims(hid_t space_id, hsize_t *dims, hsize_t *maxdims) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Sget_simple_extent_dims(space_id, dims, maxdims)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Sget_simple_extent_dims (return value <0)")
    return r

cdef hssize_t H5Sget_simple_extent_npoints(hid_t space_id) except <hssize_t>-1:
    cdef hssize_t r
    set_default_error_handler()
    r = _hdf5.H5Sget_simple_extent_npoints(space_id)
    if r<0:
        if set_exception():
            return <hssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sget_simple_extent_npoints (return value <0)")
    return r

cdef H5S_class_t H5Sget_simple_extent_type(hid_t space_id) except <H5S_class_t>-1:
    cdef H5S_class_t r
    set_default_error_handler()
    r = _hdf5.H5Sget_simple_extent_type(space_id)
    if r<0:
        if set_exception():
            return <H5S_class_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sget_simple_extent_type (return value <0)")
    return r

cdef herr_t H5Sextent_copy(hid_t dest_space_id, hid_t source_space_id ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Sextent_copy(dest_space_id, source_space_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sextent_copy (return value <0)")
    return r

cdef herr_t H5Sset_extent_simple(hid_t space_id, int rank, hsize_t *current_size, hsize_t *maximum_size ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Sset_extent_simple(space_id, rank, current_size, maximum_size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sset_extent_simple (return value <0)")
    return r

cdef herr_t H5Sset_extent_none(hid_t space_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Sset_extent_none(space_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sset_extent_none (return value <0)")
    return r

cdef H5S_sel_type H5Sget_select_type(hid_t space_id) except <H5S_sel_type>-1:
    cdef H5S_sel_type r
    set_default_error_handler()
    r = _hdf5.H5Sget_select_type(space_id)
    if r<0:
        if set_exception():
            return <H5S_sel_type>-1
        else:
            raise RuntimeError("Unspecified error in H5Sget_select_type (return value <0)")
    return r

cdef hssize_t H5Sget_select_npoints(hid_t space_id) except <hssize_t>-1:
    cdef hssize_t r
    set_default_error_handler()
    r = _hdf5.H5Sget_select_npoints(space_id)
    if r<0:
        if set_exception():
            return <hssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sget_select_npoints (return value <0)")
    return r

cdef herr_t H5Sget_select_bounds(hid_t space_id, hsize_t *start, hsize_t *end) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Sget_select_bounds(space_id, start, end)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sget_select_bounds (return value <0)")
    return r

cdef herr_t H5Sselect_all(hid_t space_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Sselect_all(space_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sselect_all (return value <0)")
    return r

cdef herr_t H5Sselect_none(hid_t space_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Sselect_none(space_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sselect_none (return value <0)")
    return r

cdef htri_t H5Sselect_valid(hid_t space_id) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Sselect_valid(space_id)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sselect_valid (return value <0)")
    return r

cdef hssize_t H5Sget_select_elem_npoints(hid_t space_id) except <hssize_t>-1:
    cdef hssize_t r
    set_default_error_handler()
    r = _hdf5.H5Sget_select_elem_npoints(space_id)
    if r<0:
        if set_exception():
            return <hssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sget_select_elem_npoints (return value <0)")
    return r

cdef herr_t H5Sget_select_elem_pointlist(hid_t space_id, hsize_t startpoint,  hsize_t numpoints, hsize_t *buf) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Sget_select_elem_pointlist(space_id, startpoint, numpoints, buf)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sget_select_elem_pointlist (return value <0)")
    return r

cdef herr_t H5Sselect_elements(hid_t space_id, H5S_seloper_t op,  size_t num_elements, const hsize_t *coord) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Sselect_elements(space_id, op, num_elements, coord)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sselect_elements (return value <0)")
    return r

cdef hssize_t H5Sget_select_hyper_nblocks(hid_t space_id ) except <hssize_t>-1:
    cdef hssize_t r
    set_default_error_handler()
    r = _hdf5.H5Sget_select_hyper_nblocks(space_id)
    if r<0:
        if set_exception():
            return <hssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sget_select_hyper_nblocks (return value <0)")
    return r

cdef herr_t H5Sget_select_hyper_blocklist(hid_t space_id,  hsize_t startblock, hsize_t numblocks, hsize_t *buf ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Sget_select_hyper_blocklist(space_id, startblock, numblocks, buf)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sget_select_hyper_blocklist (return value <0)")
    return r

cdef herr_t H5Sselect_hyperslab(hid_t space_id, H5S_seloper_t op,  hsize_t *start, hsize_t *_stride, hsize_t *count, hsize_t *_block) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Sselect_hyperslab(space_id, op, start, _stride, count, _block)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sselect_hyperslab (return value <0)")
    return r

cdef herr_t H5Sencode(hid_t obj_id, void *buf, size_t *nalloc) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Sencode(obj_id, buf, nalloc)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sencode (return value <0)")
    return r

cdef hid_t H5Sdecode(void *buf) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Sdecode(buf)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sdecode (return value <0)")
    return r

cdef htri_t H5Sis_regular_hyperslab(hid_t spaceid) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Sis_regular_hyperslab(spaceid)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sis_regular_hyperslab (return value <0)")
    return r

cdef htri_t H5Sget_regular_hyperslab(hid_t spaceid, hsize_t* start, hsize_t* stride, hsize_t* count, hsize_t* block) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Sget_regular_hyperslab(spaceid, start, stride, count, block)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sget_regular_hyperslab (return value <0)")
    return r

cdef htri_t H5Sselect_shape_same(hid_t space1_id, hid_t space2_id) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Sselect_shape_same(space1_id, space2_id)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Sselect_shape_same (return value <0)")
    return r

cdef hid_t H5Tcreate(H5T_class_t type, size_t size) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Tcreate(type, size)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tcreate (return value <0)")
    return r

cdef hid_t H5Topen(hid_t loc, char* name, hid_t tapl_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Topen(loc, name, tapl_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Topen (return value <0)")
    return r

cdef htri_t H5Tcommitted(hid_t type) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Tcommitted(type)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tcommitted (return value <0)")
    return r

cdef hid_t H5Tcopy(hid_t type_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Tcopy(type_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tcopy (return value <0)")
    return r

cdef htri_t H5Tequal(hid_t type_id1, hid_t type_id2 ) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Tequal(type_id1, type_id2)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tequal (return value <0)")
    return r

cdef herr_t H5Tlock(hid_t type_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tlock(type_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tlock (return value <0)")
    return r

cdef H5T_class_t H5Tget_class(hid_t type_id) except <H5T_class_t>-1:
    cdef H5T_class_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_class(type_id)
    if r<0:
        if set_exception():
            return <H5T_class_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_class (return value <0)")
    return r

cdef size_t H5Tget_size(hid_t type_id) except <size_t>0:
    cdef size_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_size(type_id)
    if r==0:
        if set_exception():
            return <size_t>0
        else:
            raise RuntimeError("Unspecified error in H5Tget_size (return value ==0)")
    return r

cdef hid_t H5Tget_super(hid_t type) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_super(type)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_super (return value <0)")
    return r

cdef htri_t H5Tdetect_class(hid_t type_id, H5T_class_t dtype_class) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Tdetect_class(type_id, dtype_class)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tdetect_class (return value <0)")
    return r

cdef herr_t H5Tclose(hid_t type_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tclose(type_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tclose (return value <0)")
    return r

cdef hid_t H5Tget_native_type(hid_t type_id, H5T_direction_t direction) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_native_type(type_id, direction)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_native_type (return value <0)")
    return r

cdef herr_t H5Tcommit(hid_t loc_id, char *name, hid_t dtype_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tcommit(loc_id, name, dtype_id, lcpl_id, tcpl_id, tapl_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tcommit (return value <0)")
    return r

cdef hid_t H5Tget_create_plist(hid_t dtype_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_create_plist(dtype_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_create_plist (return value <0)")
    return r

cdef hid_t H5Tdecode(unsigned char *buf) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Tdecode(buf)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tdecode (return value <0)")
    return r

cdef herr_t H5Tencode(hid_t obj_id, unsigned char *buf, size_t *nalloc) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tencode(obj_id, buf, nalloc)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tencode (return value <0)")
    return r

cdef H5T_conv_t H5Tfind(hid_t src_id, hid_t dst_id, H5T_cdata_t **pcdata) except <H5T_conv_t>NULL:
    cdef H5T_conv_t r
    set_default_error_handler()
    r = _hdf5.H5Tfind(src_id, dst_id, pcdata)
    if r==NULL:
        if set_exception():
            return <H5T_conv_t>NULL
        else:
            raise RuntimeError("Unspecified error in H5Tfind (return value ==NULL)")
    return r

cdef herr_t H5Tconvert(hid_t src_id, hid_t dst_id, size_t nelmts, void *buf, void *background, hid_t plist_id) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Tconvert(src_id, dst_id, nelmts, buf, background, plist_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tconvert (return value <0)")
    return r

cdef herr_t H5Tregister(H5T_pers_t pers, char *name, hid_t src_id, hid_t dst_id, H5T_conv_t func) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Tregister(pers, name, src_id, dst_id, func)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tregister (return value <0)")
    return r

cdef herr_t H5Tunregister(H5T_pers_t pers, char *name, hid_t src_id, hid_t dst_id, H5T_conv_t func) except <herr_t>-1:
    cdef herr_t r
    with nogil:
        set_default_error_handler()
        r = _hdf5.H5Tunregister(pers, name, src_id, dst_id, func)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tunregister (return value <0)")
    return r

cdef herr_t H5Tset_size(hid_t type_id, size_t size) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tset_size(type_id, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tset_size (return value <0)")
    return r

cdef H5T_order_t H5Tget_order(hid_t type_id) except <H5T_order_t>-1:
    cdef H5T_order_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_order(type_id)
    if r<0:
        if set_exception():
            return <H5T_order_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_order (return value <0)")
    return r

cdef herr_t H5Tset_order(hid_t type_id, H5T_order_t order) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tset_order(type_id, order)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tset_order (return value <0)")
    return r

cdef hsize_t H5Tget_precision(hid_t type_id) except <hsize_t>0:
    cdef hsize_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_precision(type_id)
    if r==0:
        if set_exception():
            return <hsize_t>0
        else:
            raise RuntimeError("Unspecified error in H5Tget_precision (return value ==0)")
    return r

cdef herr_t H5Tset_precision(hid_t type_id, size_t prec) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tset_precision(type_id, prec)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tset_precision (return value <0)")
    return r

cdef int H5Tget_offset(hid_t type_id) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Tget_offset(type_id)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_offset (return value <0)")
    return r

cdef herr_t H5Tset_offset(hid_t type_id, size_t offset) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tset_offset(type_id, offset)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tset_offset (return value <0)")
    return r

cdef herr_t H5Tget_pad(hid_t type_id, H5T_pad_t * lsb, H5T_pad_t * msb ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_pad(type_id, lsb, msb)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_pad (return value <0)")
    return r

cdef herr_t H5Tset_pad(hid_t type_id, H5T_pad_t lsb, H5T_pad_t msb ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tset_pad(type_id, lsb, msb)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tset_pad (return value <0)")
    return r

cdef H5T_sign_t H5Tget_sign(hid_t type_id) except <H5T_sign_t>-1:
    cdef H5T_sign_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_sign(type_id)
    if r<0:
        if set_exception():
            return <H5T_sign_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_sign (return value <0)")
    return r

cdef herr_t H5Tset_sign(hid_t type_id, H5T_sign_t sign) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tset_sign(type_id, sign)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tset_sign (return value <0)")
    return r

cdef herr_t H5Tget_fields(hid_t type_id, size_t *spos, size_t *epos, size_t *esize, size_t *mpos, size_t *msize ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_fields(type_id, spos, epos, esize, mpos, msize)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_fields (return value <0)")
    return r

cdef herr_t H5Tset_fields(hid_t type_id, size_t spos, size_t epos, size_t esize, size_t mpos, size_t msize ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tset_fields(type_id, spos, epos, esize, mpos, msize)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tset_fields (return value <0)")
    return r

cdef size_t H5Tget_ebias(hid_t type_id) except <size_t>0:
    cdef size_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_ebias(type_id)
    if r==0:
        if set_exception():
            return <size_t>0
        else:
            raise RuntimeError("Unspecified error in H5Tget_ebias (return value ==0)")
    return r

cdef herr_t H5Tset_ebias(hid_t type_id, size_t ebias) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tset_ebias(type_id, ebias)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tset_ebias (return value <0)")
    return r

cdef H5T_norm_t H5Tget_norm(hid_t type_id) except <H5T_norm_t>-1:
    cdef H5T_norm_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_norm(type_id)
    if r<0:
        if set_exception():
            return <H5T_norm_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_norm (return value <0)")
    return r

cdef herr_t H5Tset_norm(hid_t type_id, H5T_norm_t norm) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tset_norm(type_id, norm)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tset_norm (return value <0)")
    return r

cdef H5T_pad_t H5Tget_inpad(hid_t type_id) except <H5T_pad_t>-1:
    cdef H5T_pad_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_inpad(type_id)
    if r<0:
        if set_exception():
            return <H5T_pad_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_inpad (return value <0)")
    return r

cdef herr_t H5Tset_inpad(hid_t type_id, H5T_pad_t inpad) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tset_inpad(type_id, inpad)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tset_inpad (return value <0)")
    return r

cdef H5T_cset_t H5Tget_cset(hid_t type_id) except <H5T_cset_t>-1:
    cdef H5T_cset_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_cset(type_id)
    if r<0:
        if set_exception():
            return <H5T_cset_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_cset (return value <0)")
    return r

cdef herr_t H5Tset_cset(hid_t type_id, H5T_cset_t cset) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tset_cset(type_id, cset)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tset_cset (return value <0)")
    return r

cdef H5T_str_t H5Tget_strpad(hid_t type_id) except <H5T_str_t>-1:
    cdef H5T_str_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_strpad(type_id)
    if r<0:
        if set_exception():
            return <H5T_str_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_strpad (return value <0)")
    return r

cdef herr_t H5Tset_strpad(hid_t type_id, H5T_str_t strpad) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tset_strpad(type_id, strpad)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tset_strpad (return value <0)")
    return r

cdef hid_t H5Tvlen_create(hid_t base_type_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Tvlen_create(base_type_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tvlen_create (return value <0)")
    return r

cdef htri_t H5Tis_variable_str(hid_t dtype_id) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Tis_variable_str(dtype_id)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tis_variable_str (return value <0)")
    return r

cdef int H5Tget_nmembers(hid_t type_id) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Tget_nmembers(type_id)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_nmembers (return value <0)")
    return r

cdef H5T_class_t H5Tget_member_class(hid_t type_id, int member_no) except <H5T_class_t>-1:
    cdef H5T_class_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_member_class(type_id, member_no)
    if r<0:
        if set_exception():
            return <H5T_class_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_member_class (return value <0)")
    return r

cdef char* H5Tget_member_name(hid_t type_id, unsigned membno) except <char*>NULL:
    cdef char* r
    set_default_error_handler()
    r = _hdf5.H5Tget_member_name(type_id, membno)
    if r==NULL:
        if set_exception():
            return <char*>NULL
        else:
            raise RuntimeError("Unspecified error in H5Tget_member_name (return value ==NULL)")
    return r

cdef hid_t H5Tget_member_type(hid_t type_id, unsigned membno) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_member_type(type_id, membno)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_member_type (return value <0)")
    return r

cdef int H5Tget_member_offset(hid_t type_id, int membno) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Tget_member_offset(type_id, membno)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_member_offset (return value <0)")
    return r

cdef int H5Tget_member_index(hid_t type_id, char* name) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Tget_member_index(type_id, name)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_member_index (return value <0)")
    return r

cdef herr_t H5Tinsert(hid_t parent_id, char *name, size_t offset, hid_t member_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tinsert(parent_id, name, offset, member_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tinsert (return value <0)")
    return r

cdef herr_t H5Tpack(hid_t type_id) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tpack(type_id)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tpack (return value <0)")
    return r

cdef hid_t H5Tenum_create(hid_t base_id) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Tenum_create(base_id)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tenum_create (return value <0)")
    return r

cdef herr_t H5Tenum_insert(hid_t type, char *name, void *value) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tenum_insert(type, name, value)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tenum_insert (return value <0)")
    return r

cdef herr_t H5Tenum_nameof( hid_t type, void *value, char *name, size_t size ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tenum_nameof(type, value, name, size)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tenum_nameof (return value <0)")
    return r

cdef herr_t H5Tenum_valueof( hid_t type, char *name, void *value ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tenum_valueof(type, name, value)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tenum_valueof (return value <0)")
    return r

cdef herr_t H5Tget_member_value(hid_t type,  unsigned int memb_no, void *value ) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tget_member_value(type, memb_no, value)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_member_value (return value <0)")
    return r

cdef hid_t H5Tarray_create(hid_t base_id, int ndims, hsize_t *dims) except <hid_t>-1:
    cdef hid_t r
    set_default_error_handler()
    r = _hdf5.H5Tarray_create(base_id, ndims, dims)
    if r<0:
        if set_exception():
            return <hid_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tarray_create (return value <0)")
    return r

cdef int H5Tget_array_ndims(hid_t type_id) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Tget_array_ndims(type_id)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_array_ndims (return value <0)")
    return r

cdef int H5Tget_array_dims(hid_t type_id, hsize_t *dims) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5Tget_array_dims(type_id, dims)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5Tget_array_dims (return value <0)")
    return r

cdef herr_t H5Tset_tag(hid_t type_id, char* tag) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Tset_tag(type_id, tag)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Tset_tag (return value <0)")
    return r

cdef char* H5Tget_tag(hid_t type_id) except <char*>NULL:
    cdef char* r
    set_default_error_handler()
    r = _hdf5.H5Tget_tag(type_id)
    if r==NULL:
        if set_exception():
            return <char*>NULL
        else:
            raise RuntimeError("Unspecified error in H5Tget_tag (return value ==NULL)")
    return r

cdef htri_t H5Zfilter_avail(H5Z_filter_t id_) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5Zfilter_avail(id_)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Zfilter_avail (return value <0)")
    return r

cdef herr_t H5Zget_filter_info(H5Z_filter_t filter_, unsigned int *filter_config_flags) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Zget_filter_info(filter_, filter_config_flags)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Zget_filter_info (return value <0)")
    return r

cdef herr_t H5Zregister(const void *cls) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Zregister(cls)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Zregister (return value <0)")
    return r

cdef herr_t H5Zunregister(H5Z_filter_t id_) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5Zunregister(id_)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5Zunregister (return value <0)")
    return r

cdef herr_t H5DSattach_scale(hid_t did, hid_t dsid, unsigned int idx) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5DSattach_scale(did, dsid, idx)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5DSattach_scale (return value <0)")
    return r

cdef herr_t H5DSdetach_scale(hid_t did, hid_t dsid, unsigned int idx) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5DSdetach_scale(did, dsid, idx)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5DSdetach_scale (return value <0)")
    return r

cdef htri_t H5DSis_attached(hid_t did, hid_t dsid, unsigned int idx) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5DSis_attached(did, dsid, idx)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5DSis_attached (return value <0)")
    return r

cdef herr_t H5DSset_scale(hid_t dsid, char *dimname) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5DSset_scale(dsid, dimname)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5DSset_scale (return value <0)")
    return r

cdef int H5DSget_num_scales(hid_t did, unsigned int dim) except <int>-1:
    cdef int r
    set_default_error_handler()
    r = _hdf5.H5DSget_num_scales(did, dim)
    if r<0:
        if set_exception():
            return <int>-1
        else:
            raise RuntimeError("Unspecified error in H5DSget_num_scales (return value <0)")
    return r

cdef herr_t H5DSset_label(hid_t did, unsigned int idx, char *label) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5DSset_label(did, idx, label)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5DSset_label (return value <0)")
    return r

cdef ssize_t H5DSget_label(hid_t did, unsigned int idx, char *label, size_t size) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5DSget_label(did, idx, label, size)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5DSget_label (return value <0)")
    return r

cdef ssize_t H5DSget_scale_name(hid_t did, char *name, size_t size) except <ssize_t>-1:
    cdef ssize_t r
    set_default_error_handler()
    r = _hdf5.H5DSget_scale_name(did, name, size)
    if r<0:
        if set_exception():
            return <ssize_t>-1
        else:
            raise RuntimeError("Unspecified error in H5DSget_scale_name (return value <0)")
    return r

cdef htri_t H5DSis_scale(hid_t did) except <htri_t>-1:
    cdef htri_t r
    set_default_error_handler()
    r = _hdf5.H5DSis_scale(did)
    if r<0:
        if set_exception():
            return <htri_t>-1
        else:
            raise RuntimeError("Unspecified error in H5DSis_scale (return value <0)")
    return r

cdef herr_t H5DSiterate_scales(hid_t did, unsigned int dim, int *idx, H5DS_iterate_t visitor, void *visitor_data) except <herr_t>-1:
    cdef herr_t r
    set_default_error_handler()
    r = _hdf5.H5DSiterate_scales(did, dim, idx, visitor, visitor_data)
    if r<0:
        if set_exception():
            return <herr_t>-1
        else:
            raise RuntimeError("Unspecified error in H5DSiterate_scales (return value <0)")
    return r

