# cython: language_level=3
#
# Warning: this file is auto-generated from api_gen.py. DO NOT EDIT!
#

include "config.pxi"

from .api_types_hdf5 cimport *
from .api_types_ext cimport *

cdef herr_t H5open() except <herr_t>-1
cdef herr_t H5close() except <herr_t>-1
cdef herr_t H5get_libversion(unsigned *majnum, unsigned *minnum, unsigned *relnum) except <herr_t>-1
cdef herr_t H5check_version(unsigned majnum, unsigned minnum, unsigned relnum ) except <herr_t>-1
cdef herr_t H5free_memory(void *mem) except <herr_t>-1
cdef hid_t H5Acreate(hid_t loc_id, char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id) except <hid_t>-1
cdef hid_t H5Aopen_idx(hid_t loc_id, unsigned int idx) except <hid_t>-1
cdef hid_t H5Aopen_name(hid_t loc_id, char *name) except <hid_t>-1
cdef herr_t H5Aclose(hid_t attr_id) except <herr_t>-1
cdef herr_t H5Adelete(hid_t loc_id, char *name) except <herr_t>-1
cdef herr_t H5Aread(hid_t attr_id, hid_t mem_type_id, void *buf) except <herr_t>-1
cdef herr_t H5Awrite(hid_t attr_id, hid_t mem_type_id, void *buf ) except <herr_t>-1
cdef int H5Aget_num_attrs(hid_t loc_id) except <int>-1
cdef ssize_t H5Aget_name(hid_t attr_id, size_t buf_size, char *buf) except <ssize_t>-1
cdef hid_t H5Aget_space(hid_t attr_id) except <hid_t>-1
cdef hid_t H5Aget_type(hid_t attr_id) except <hid_t>-1
cdef herr_t H5Adelete_by_name(hid_t loc_id, char *obj_name, char *attr_name, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Adelete_by_idx(hid_t loc_id, char *obj_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, hid_t lapl_id) except <herr_t>-1
cdef hid_t H5Acreate_by_name(hid_t loc_id, char *obj_name, char *attr_name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t lapl_id) except <hid_t>-1
cdef hid_t H5Aopen(hid_t obj_id, char *attr_name, hid_t aapl_id) except <hid_t>-1
cdef hid_t H5Aopen_by_name( hid_t loc_id, char *obj_name, char *attr_name, hid_t aapl_id, hid_t lapl_id) except <hid_t>-1
cdef hid_t H5Aopen_by_idx(hid_t loc_id, char *obj_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, hid_t aapl_id, hid_t lapl_id) except <hid_t>-1
cdef htri_t H5Aexists_by_name( hid_t loc_id, char *obj_name, char *attr_name, hid_t lapl_id) except <htri_t>-1
cdef htri_t H5Aexists(hid_t obj_id, char *attr_name) except <htri_t>-1
cdef herr_t H5Arename(hid_t loc_id, char *old_attr_name, char *new_attr_name) except <herr_t>-1
cdef herr_t H5Arename_by_name(hid_t loc_id, char *obj_name, char *old_attr_name, char *new_attr_name, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Aget_info( hid_t attr_id, H5A_info_t *ainfo) except <herr_t>-1
cdef herr_t H5Aget_info_by_name(hid_t loc_id, char *obj_name, char *attr_name, H5A_info_t *ainfo, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Aget_info_by_idx(hid_t loc_id, char *obj_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, H5A_info_t *ainfo, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Aiterate(hid_t obj_id, H5_index_t idx_type, H5_iter_order_t order, hsize_t *n, H5A_operator2_t op, void *op_data) except <herr_t>-1
cdef hsize_t H5Aget_storage_size(hid_t attr_id) except <hsize_t>0
cdef hid_t H5Dcreate(hid_t loc_id, char *name, hid_t type_id, hid_t space_id, hid_t lcpl_id, hid_t dcpl_id, hid_t dapl_id) except <hid_t>-1
cdef hid_t H5Dcreate_anon(hid_t file_id, hid_t type_id, hid_t space_id, hid_t plist_id, hid_t dapl_id) except <hid_t>-1
cdef hid_t H5Dopen(hid_t loc_id, char *name, hid_t dapl_id ) except <hid_t>-1
cdef herr_t H5Dclose(hid_t dset_id) except <herr_t>-1
cdef hid_t H5Dget_space(hid_t dset_id) except <hid_t>-1
cdef herr_t H5Dget_space_status(hid_t dset_id, H5D_space_status_t *status) except <herr_t>-1
cdef hid_t H5Dget_type(hid_t dset_id) except <hid_t>-1
cdef hid_t H5Dget_create_plist(hid_t dataset_id) except <hid_t>-1
cdef hid_t H5Dget_access_plist(hid_t dataset_id) except <hid_t>-1
cdef haddr_t H5Dget_offset(hid_t dset_id) except <haddr_t>0
cdef hsize_t H5Dget_storage_size(hid_t dset_id) except? <hsize_t>0
cdef herr_t H5Dread(hid_t dset_id, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf) except <herr_t>-1
cdef herr_t H5Dwrite(hid_t dset_id, hid_t mem_type, hid_t mem_space, hid_t file_space, hid_t xfer_plist, void* buf) except <herr_t>-1
cdef herr_t H5Dextend(hid_t dataset_id, hsize_t *size) except <herr_t>-1
cdef herr_t H5Dfill(void *fill, hid_t fill_type_id, void *buf,  hid_t buf_type_id, hid_t space_id) except <herr_t>-1
cdef herr_t H5Dvlen_get_buf_size(hid_t dset_id, hid_t type_id, hid_t space_id, hsize_t *size) except <herr_t>-1
cdef herr_t H5Dvlen_reclaim(hid_t type_id, hid_t space_id,  hid_t plist, void *buf) except <herr_t>-1
cdef herr_t H5Diterate(void *buf, hid_t type_id, hid_t space_id,  H5D_operator_t op, void* operator_data) except <herr_t>-1
cdef herr_t H5Dset_extent(hid_t dset_id, hsize_t* size) except <herr_t>-1
cdef herr_t H5Dflush(hid_t dataset_id) except <herr_t>-1
cdef herr_t H5Drefresh(hid_t dataset_id) except <herr_t>-1
cdef herr_t H5DOwrite_chunk(hid_t dset_id, hid_t dxpl_id, uint32_t filters, const hsize_t *offset, size_t data_size, const void *buf) except <herr_t>-1
cdef herr_t H5Dget_chunk_storage_size(hid_t dset_id, const hsize_t *offset, hsize_t *chunk_nbytes) except <herr_t>-1
cdef herr_t H5Dread_chunk(hid_t dset_id, hid_t dxpl_id, const hsize_t *offset, uint32_t *filters, void *buf) except <herr_t>-1
cdef herr_t H5Dget_num_chunks(hid_t dset_id, hid_t fspace_id, hsize_t *nchunks) except <herr_t>-1
cdef herr_t H5Dget_chunk_info(hid_t dset_id, hid_t fspace_id, hsize_t chk_idx, hsize_t *offset, unsigned *filter_mask, haddr_t *addr, hsize_t *size) except <herr_t>-1
cdef herr_t H5Dget_chunk_info_by_coord(hid_t dset_id, const hsize_t *offset, unsigned *filter_mask, haddr_t *addr, hsize_t *size) except <herr_t>-1
cdef herr_t H5Dchunk_iter(hid_t dset_id, hid_t dxpl_id, H5D_chunk_iter_op_t cb, void *op_data) except <herr_t>-1
cdef hid_t H5Fcreate(char *filename, unsigned int flags, hid_t create_plist, hid_t access_plist) except <hid_t>-1
cdef hid_t H5Fopen(char *name, unsigned flags, hid_t access_id) except <hid_t>-1
cdef herr_t H5Fclose(hid_t file_id) except <herr_t>-1
cdef htri_t H5Fis_hdf5(char *name) except <htri_t>-1
cdef herr_t H5Fflush(hid_t object_id, H5F_scope_t scope) except <herr_t>-1
cdef hid_t H5Freopen(hid_t file_id) except <hid_t>-1
cdef herr_t H5Fmount(hid_t loc_id, char *name, hid_t child_id, hid_t plist_id) except <herr_t>-1
cdef herr_t H5Funmount(hid_t loc_id, char *name) except <herr_t>-1
cdef herr_t H5Fget_filesize(hid_t file_id, hsize_t *size) except <herr_t>-1
cdef hid_t H5Fget_create_plist(hid_t file_id ) except <hid_t>-1
cdef hid_t H5Fget_access_plist(hid_t file_id) except <hid_t>-1
cdef hssize_t H5Fget_freespace(hid_t file_id) except <hssize_t>-1
cdef ssize_t H5Fget_name(hid_t obj_id, char *name, size_t size) except <ssize_t>-1
cdef int H5Fget_obj_count(hid_t file_id, unsigned int types) except <int>-1
cdef int H5Fget_obj_ids(hid_t file_id, unsigned int types, int max_objs, hid_t *obj_id_list) except <int>-1
cdef herr_t H5Fget_vfd_handle(hid_t file_id, hid_t fapl_id, void **file_handle) except <herr_t>-1
cdef herr_t H5Fget_intent(hid_t file_id, unsigned int *intent) except <herr_t>-1
cdef herr_t H5Fget_mdc_config(hid_t file_id, H5AC_cache_config_t *config_ptr) except <herr_t>-1
cdef herr_t H5Fget_mdc_hit_rate(hid_t file_id, double *hit_rate_ptr) except <herr_t>-1
cdef herr_t H5Fget_mdc_size(hid_t file_id, size_t *max_size_ptr, size_t *min_clean_size_ptr, size_t *cur_size_ptr, int *cur_num_entries_ptr) except <herr_t>-1
cdef herr_t H5Freset_mdc_hit_rate_stats(hid_t file_id) except <herr_t>-1
cdef herr_t H5Fset_mdc_config(hid_t file_id, H5AC_cache_config_t *config_ptr) except <herr_t>-1
cdef ssize_t H5Fget_file_image(hid_t file_id, void *buf_ptr, size_t buf_len) except <ssize_t>-1
cdef hid_t H5LTopen_file_image(void *buf_ptr, size_t buf_len, unsigned int flags) except <hid_t>-1
cdef herr_t H5Fstart_swmr_write(hid_t file_id) except <herr_t>-1
cdef herr_t H5Freset_page_buffering_stats(hid_t file_id) except <herr_t>-1
cdef herr_t H5Fget_page_buffering_stats(hid_t file_id, unsigned *accesses, unsigned *hits, unsigned *misses, unsigned *evictions, unsigned *bypasses) except <herr_t>-1
cdef hid_t H5FDregister(H5FD_class_t *cls_ptr) except <hid_t>-1
cdef herr_t H5FDunregister(hid_t driver_id) except <herr_t>-1
cdef herr_t H5Gclose(hid_t group_id) except <herr_t>-1
cdef herr_t H5Glink2( hid_t curr_loc_id, char *current_name, H5G_link_t link_type, hid_t new_loc_id, char *new_name) except <herr_t>-1
cdef herr_t H5Gunlink(hid_t file_id, char *name) except <herr_t>-1
cdef herr_t H5Gmove2(hid_t src_loc_id, char *src_name, hid_t dst_loc_id, char *dst_name) except <herr_t>-1
cdef herr_t H5Gget_num_objs(hid_t loc_id, hsize_t*  num_obj) except <herr_t>-1
cdef int H5Gget_objname_by_idx(hid_t loc_id, hsize_t idx, char *name, size_t size) except <int>-1
cdef int H5Gget_objtype_by_idx(hid_t loc_id, hsize_t idx) except <int>-1
cdef herr_t H5Giterate(hid_t loc_id, char *name, int *idx, H5G_iterate_t op, void* data) except <herr_t>-1
cdef herr_t H5Gget_objinfo(hid_t loc_id, char* name, int follow_link, H5G_stat_t *statbuf) except <herr_t>-1
cdef herr_t H5Gget_linkval(hid_t loc_id, char *name, size_t size, char *value) except <herr_t>-1
cdef herr_t H5Gset_comment(hid_t loc_id, char *name, char *comment) except <herr_t>-1
cdef int H5Gget_comment(hid_t loc_id, char *name, size_t bufsize, char *comment) except <int>-1
cdef hid_t H5Gcreate_anon( hid_t loc_id, hid_t gcpl_id, hid_t gapl_id) except <hid_t>-1
cdef hid_t H5Gcreate(hid_t loc_id, char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id) except <hid_t>-1
cdef hid_t H5Gopen( hid_t loc_id, char * name, hid_t gapl_id) except <hid_t>-1
cdef herr_t H5Gget_info( hid_t group_id, H5G_info_t *group_info) except <herr_t>-1
cdef herr_t H5Gget_info_by_name( hid_t loc_id, char *group_name, H5G_info_t *group_info, hid_t lapl_id) except <herr_t>-1
cdef hid_t H5Gget_create_plist(hid_t group_id) except <hid_t>-1
cdef H5I_type_t H5Iget_type(hid_t obj_id) except <H5I_type_t>-1
cdef ssize_t H5Iget_name( hid_t obj_id, char *name, size_t size) except <ssize_t>-1
cdef hid_t H5Iget_file_id(hid_t obj_id) except <hid_t>-1
cdef int H5Idec_ref(hid_t obj_id) except <int>-1
cdef int H5Iget_ref(hid_t obj_id) except <int>-1
cdef int H5Iinc_ref(hid_t obj_id) except <int>-1
cdef htri_t H5Iis_valid( hid_t obj_id ) except <htri_t>-1
cdef herr_t H5Lmove(hid_t src_loc, char *src_name, hid_t dst_loc, char *dst_name, hid_t lcpl_id, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Lcopy(hid_t src_loc, char *src_name, hid_t dst_loc, char *dst_name, hid_t lcpl_id, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Lcreate_hard(hid_t cur_loc, char *cur_name, hid_t dst_loc, char *dst_name, hid_t lcpl_id, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Lcreate_soft(char *link_target, hid_t link_loc_id, char *link_name, hid_t lcpl_id, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Ldelete(hid_t loc_id, char *name, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Ldelete_by_idx(hid_t loc_id, char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Lget_val(hid_t loc_id, char *name, void *bufout, size_t size, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Lget_val_by_idx(hid_t loc_id, char *group_name,  H5_index_t idx_type, H5_iter_order_t order, hsize_t n, void *bufout, size_t size, hid_t lapl_id) except <herr_t>-1
cdef htri_t H5Lexists(hid_t loc_id, char *name, hid_t lapl_id) except <htri_t>-1
cdef herr_t H5Lget_info(hid_t loc_id, char *name, H5L_info_t *linfo, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Lget_info_by_idx(hid_t loc_id, char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, H5L_info_t *linfo, hid_t lapl_id) except <herr_t>-1
cdef ssize_t H5Lget_name_by_idx(hid_t loc_id, char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, char *name, size_t size, hid_t lapl_id) except <ssize_t>-1
cdef herr_t H5Literate(hid_t grp_id, H5_index_t idx_type, H5_iter_order_t order, hsize_t *idx, H5L_iterate_t op, void *op_data) except <herr_t>-1
cdef herr_t H5Literate_by_name(hid_t loc_id, char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t *idx, H5L_iterate_t op, void *op_data, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Lvisit(hid_t grp_id, H5_index_t idx_type, H5_iter_order_t order, H5L_iterate_t op, void *op_data) except <herr_t>-1
cdef herr_t H5Lvisit_by_name(hid_t loc_id, char *group_name, H5_index_t idx_type, H5_iter_order_t order, H5L_iterate_t op, void *op_data, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Lunpack_elink_val(void *ext_linkval, size_t link_size, unsigned *flags, const char **filename, const char **obj_path) except <herr_t>-1
cdef herr_t H5Lcreate_external(char *file_name, char *obj_name, hid_t link_loc_id, char *link_name, hid_t lcpl_id, hid_t lapl_id) except <herr_t>-1
cdef hid_t H5Oopen(hid_t loc_id, char *name, hid_t lapl_id) except <hid_t>-1
cdef hid_t H5Oopen_by_addr(hid_t loc_id, haddr_t addr) except <hid_t>-1
cdef hid_t H5Oopen_by_idx(hid_t loc_id, char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n, hid_t lapl_id) except <hid_t>-1
cdef herr_t H5Oget_info(hid_t loc_id, H5O_info_t *oinfo) except <herr_t>-1
cdef herr_t H5Oget_info_by_name(hid_t loc_id, char *name, H5O_info_t *oinfo, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Oget_info_by_idx(hid_t loc_id, char *group_name,  H5_index_t idx_type, H5_iter_order_t order, hsize_t n, H5O_info_t *oinfo, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Olink(hid_t obj_id, hid_t new_loc_id, char *new_name, hid_t lcpl_id, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Ocopy(hid_t src_loc_id, char *src_name, hid_t dst_loc_id,  char *dst_name, hid_t ocpypl_id, hid_t lcpl_id) except <herr_t>-1
cdef herr_t H5Oincr_refcount(hid_t object_id) except <herr_t>-1
cdef herr_t H5Odecr_refcount(hid_t object_id) except <herr_t>-1
cdef herr_t H5Oset_comment(hid_t obj_id, char *comment) except <herr_t>-1
cdef herr_t H5Oset_comment_by_name(hid_t loc_id, char *name,  char *comment, hid_t lapl_id) except <herr_t>-1
cdef ssize_t H5Oget_comment(hid_t obj_id, char *comment, size_t bufsize) except <ssize_t>-1
cdef ssize_t H5Oget_comment_by_name(hid_t loc_id, char *name, char *comment, size_t bufsize, hid_t lapl_id) except <ssize_t>-1
cdef herr_t H5Ovisit(hid_t obj_id, H5_index_t idx_type, H5_iter_order_t order,  H5O_iterate_t op, void *op_data) except <herr_t>-1
cdef herr_t H5Ovisit_by_name(hid_t loc_id, char *obj_name, H5_index_t idx_type, H5_iter_order_t order, H5O_iterate_t op, void *op_data, hid_t lapl_id) except <herr_t>-1
cdef herr_t H5Oclose(hid_t object_id) except <herr_t>-1
cdef htri_t H5Oexists_by_name(hid_t loc_id, char * name, hid_t lapl_id ) except <htri_t>-1
cdef hid_t H5Pcreate(hid_t plist_id) except <hid_t>-1
cdef hid_t H5Pcopy(hid_t plist_id) except <hid_t>-1
cdef hid_t H5Pget_class(hid_t plist_id) except <hid_t>-1
cdef herr_t H5Pclose(hid_t plist_id) except <herr_t>-1
cdef htri_t H5Pequal( hid_t id1, hid_t id2 ) except <htri_t>-1
cdef herr_t H5Pclose_class(hid_t id) except <herr_t>-1
cdef herr_t H5Pget_version(hid_t plist, unsigned int *super_, unsigned int* freelist,  unsigned int *stab, unsigned int *shhdr) except <herr_t>-1
cdef herr_t H5Pset_userblock(hid_t plist, hsize_t size) except <herr_t>-1
cdef herr_t H5Pget_userblock(hid_t plist, hsize_t * size) except <herr_t>-1
cdef herr_t H5Pset_sizes(hid_t plist, size_t sizeof_addr, size_t sizeof_size) except <herr_t>-1
cdef herr_t H5Pget_sizes(hid_t plist, size_t *sizeof_addr, size_t *sizeof_size) except <herr_t>-1
cdef herr_t H5Pset_sym_k(hid_t plist, unsigned int ik, unsigned int lk) except <herr_t>-1
cdef herr_t H5Pget_sym_k(hid_t plist, unsigned int *ik, unsigned int *lk) except <herr_t>-1
cdef herr_t H5Pset_istore_k(hid_t plist, unsigned int ik) except <herr_t>-1
cdef herr_t H5Pget_istore_k(hid_t plist, unsigned int *ik) except <herr_t>-1
cdef herr_t H5Pset_file_space_strategy(hid_t fcpl, H5F_fspace_strategy_t strategy, hbool_t persist, hsize_t threshold) except <herr_t>-1
cdef herr_t H5Pget_file_space_strategy(hid_t fcpl, H5F_fspace_strategy_t *strategy, hbool_t *persist, hsize_t *threshold) except <herr_t>-1
cdef herr_t H5Pset_file_space_page_size(hid_t plist_id, hsize_t fsp_size) except <herr_t>-1
cdef herr_t H5Pget_file_space_page_size(hid_t plist_id, hsize_t *fsp_size) except <herr_t>-1
cdef herr_t H5Pset_fclose_degree(hid_t fapl_id, H5F_close_degree_t fc_degree) except <herr_t>-1
cdef herr_t H5Pget_fclose_degree(hid_t fapl_id, H5F_close_degree_t *fc_degree) except <herr_t>-1
cdef herr_t H5Pset_fapl_core( hid_t fapl_id, size_t increment, hbool_t backing_store) except <herr_t>-1
cdef herr_t H5Pget_fapl_core( hid_t fapl_id, size_t *increment, hbool_t *backing_store) except <herr_t>-1
cdef herr_t H5Pset_fapl_family( hid_t fapl_id,  hsize_t memb_size, hid_t memb_fapl_id ) except <herr_t>-1
cdef herr_t H5Pget_fapl_family( hid_t fapl_id, hsize_t *memb_size, hid_t *memb_fapl_id ) except <herr_t>-1
cdef herr_t H5Pset_family_offset( hid_t fapl_id, hsize_t offset) except <herr_t>-1
cdef herr_t H5Pget_family_offset( hid_t fapl_id, hsize_t *offset) except <herr_t>-1
cdef herr_t H5Pset_fapl_log(hid_t fapl_id, char *logfile, unsigned int flags, size_t buf_size) except <herr_t>-1
cdef herr_t H5Pset_fapl_multi(hid_t fapl_id, H5FD_mem_t *memb_map, hid_t *memb_fapl, const char * const *memb_name, haddr_t *memb_addr, hbool_t relax) except <herr_t>-1
cdef herr_t H5Pset_cache(hid_t plist_id, int mdc_nelmts, int rdcc_nelmts,  size_t rdcc_nbytes, double rdcc_w0) except <herr_t>-1
cdef herr_t H5Pget_cache(hid_t plist_id, int *mdc_nelmts, size_t *rdcc_nelmts, size_t *rdcc_nbytes, double *rdcc_w0) except <herr_t>-1
cdef herr_t H5Pset_fapl_sec2(hid_t fapl_id) except <herr_t>-1
cdef herr_t H5Pset_fapl_stdio(hid_t fapl_id) except <herr_t>-1
cdef herr_t H5Pset_fapl_split(hid_t fapl_id, const char *meta_ext, hid_t meta_plist_id, const char *raw_ext, hid_t raw_plist_id) except <herr_t>-1
cdef herr_t H5Pset_driver(hid_t plist_id, hid_t driver_id, void *driver_info) except <herr_t>-1
cdef hid_t H5Pget_driver(hid_t fapl_id) except <hid_t>-1
cdef void* H5Pget_driver_info(hid_t plist_id) except <void*>NULL
cdef herr_t H5Pget_mdc_config(hid_t plist_id, H5AC_cache_config_t *config_ptr) except <herr_t>-1
cdef herr_t H5Pset_mdc_config(hid_t plist_id, H5AC_cache_config_t *config_ptr) except <herr_t>-1
cdef herr_t H5Pset_meta_block_size(hid_t fapl_id, hsize_t size) except <herr_t>-1
cdef herr_t H5Pget_meta_block_size(hid_t fapl_id, hsize_t * size) except <herr_t>-1
cdef herr_t H5Pset_file_image(hid_t plist_id, void *buf_ptr, size_t buf_len) except <herr_t>-1
cdef herr_t H5Pset_page_buffer_size(hid_t plist_id, size_t buf_size, unsigned min_meta_per, unsigned min_raw_per) except <herr_t>-1
cdef herr_t H5Pget_page_buffer_size(hid_t plist_id, size_t *buf_size, unsigned *min_meta_per, unsigned *min_raw_per) except <herr_t>-1
cdef herr_t H5Pget_file_locking(hid_t fapl_id, hbool_t *use_file_locking, hbool_t *ignore_when_disabled) except <herr_t>-1
cdef herr_t H5Pset_file_locking(hid_t fapl_id, hbool_t use_file_locking, hbool_t ignore_when_disabled) except <herr_t>-1
cdef herr_t H5Pset_layout(hid_t plist, int layout) except <herr_t>-1
cdef H5D_layout_t H5Pget_layout(hid_t plist) except <H5D_layout_t>-1
cdef herr_t H5Pset_chunk(hid_t plist, int ndims, hsize_t * dim) except <herr_t>-1
cdef int H5Pget_chunk(hid_t plist, int max_ndims, hsize_t * dims ) except <int>-1
cdef herr_t H5Pset_deflate( hid_t plist, int level) except <herr_t>-1
cdef herr_t H5Pset_fill_value(hid_t plist_id, hid_t type_id, void *value ) except <herr_t>-1
cdef herr_t H5Pget_fill_value(hid_t plist_id, hid_t type_id, void *value ) except <herr_t>-1
cdef herr_t H5Pfill_value_defined(hid_t plist_id, H5D_fill_value_t *status ) except <herr_t>-1
cdef herr_t H5Pset_fill_time(hid_t plist_id, H5D_fill_time_t fill_time ) except <herr_t>-1
cdef herr_t H5Pget_fill_time(hid_t plist_id, H5D_fill_time_t *fill_time ) except <herr_t>-1
cdef herr_t H5Pset_alloc_time(hid_t plist_id, H5D_alloc_time_t alloc_time ) except <herr_t>-1
cdef herr_t H5Pget_alloc_time(hid_t plist_id, H5D_alloc_time_t *alloc_time ) except <herr_t>-1
cdef herr_t H5Pset_filter(hid_t plist, H5Z_filter_t filter, unsigned int flags, size_t cd_nelmts, unsigned int* cd_values ) except <herr_t>-1
cdef htri_t H5Pall_filters_avail(hid_t dcpl_id) except <htri_t>-1
cdef int H5Pget_nfilters(hid_t plist) except <int>-1
cdef H5Z_filter_t H5Pget_filter(hid_t plist, unsigned int filter_number,   unsigned int *flags, size_t *cd_nelmts,  unsigned int* cd_values, size_t namelen, char* name, unsigned int* filter_config) except <H5Z_filter_t>-1
cdef herr_t H5Pget_filter_by_id( hid_t plist_id, H5Z_filter_t filter,  unsigned int *flags, size_t *cd_nelmts,  unsigned int* cd_values, size_t namelen, char* name, unsigned int* filter_config) except <herr_t>-1
cdef herr_t H5Pmodify_filter(hid_t plist, H5Z_filter_t filter, unsigned int flags, size_t cd_nelmts, unsigned int *cd_values) except <herr_t>-1
cdef herr_t H5Premove_filter(hid_t plist, H5Z_filter_t filter ) except <herr_t>-1
cdef herr_t H5Pset_fletcher32(hid_t plist) except <herr_t>-1
cdef herr_t H5Pset_shuffle(hid_t plist_id) except <herr_t>-1
cdef herr_t H5Pset_szip(hid_t plist, unsigned int options_mask, unsigned int pixels_per_block) except <herr_t>-1
cdef herr_t H5Pset_scaleoffset(hid_t plist, H5Z_SO_scale_type_t scale_type, int scale_factor) except <herr_t>-1
cdef herr_t H5Pset_external(hid_t plist_id, const char *name, off_t offset, hsize_t size) except <herr_t>-1
cdef int H5Pget_external_count(hid_t plist_id) except <int>-1
cdef herr_t H5Pget_external(hid_t plist, unsigned idx, size_t name_size, char *name, off_t *offset, hsize_t *size) except <herr_t>-1
cdef ssize_t H5Pget_virtual_dsetname(hid_t dcpl_id, size_t index, char *name, size_t size) except <ssize_t>-1
cdef ssize_t H5Pget_virtual_filename(hid_t dcpl_id, size_t index, char *name, size_t size) except <ssize_t>-1
cdef herr_t H5Pget_virtual_count(hid_t dcpl_id, size_t *count) except <herr_t>-1
cdef herr_t H5Pset_virtual(hid_t dcpl_id, hid_t vspace_id, const char *src_file_name, const char *src_dset_name, hid_t src_space_id) except <herr_t>-1
cdef hid_t H5Pget_virtual_vspace(hid_t dcpl_id, size_t index) except <hid_t>-1
cdef hid_t H5Pget_virtual_srcspace(hid_t dcpl_id, size_t index) except <hid_t>-1
cdef herr_t H5Pset_edc_check(hid_t plist, H5Z_EDC_t check) except <herr_t>-1
cdef H5Z_EDC_t H5Pget_edc_check(hid_t plist) except <H5Z_EDC_t>-1
cdef herr_t H5Pset_chunk_cache( hid_t dapl_id, size_t rdcc_nslots, size_t rdcc_nbytes, double rdcc_w0 ) except <herr_t>-1
cdef herr_t H5Pget_chunk_cache( hid_t dapl_id, size_t *rdcc_nslots, size_t *rdcc_nbytes, double *rdcc_w0 ) except <herr_t>-1
cdef herr_t H5Pget_efile_prefix(hid_t dapl_id, char *prefix, ssize_t size) except <herr_t>-1
cdef herr_t H5Pset_efile_prefix(hid_t dapl_id, char *prefix) except <herr_t>-1
cdef herr_t H5Pset_virtual_view(hid_t plist_id, H5D_vds_view_t view) except <herr_t>-1
cdef herr_t H5Pget_virtual_view(hid_t plist_id, H5D_vds_view_t *view) except <herr_t>-1
cdef herr_t H5Pset_virtual_printf_gap(hid_t plist_id, hsize_t gap_size) except <herr_t>-1
cdef herr_t H5Pget_virtual_printf_gap(hid_t plist_id, hsize_t *gap_size) except <herr_t>-1
cdef ssize_t H5Pget_virtual_prefix(hid_t dapl_id, char *prefix, ssize_t size) except <ssize_t>-1
cdef herr_t H5Pset_virtual_prefix(hid_t dapl_id, char *prefix) except <herr_t>-1
cdef herr_t H5Pset_sieve_buf_size(hid_t fapl_id, size_t size) except <herr_t>-1
cdef herr_t H5Pget_sieve_buf_size(hid_t fapl_id, size_t *size) except <herr_t>-1
cdef herr_t H5Pset_nlinks(hid_t plist_id, size_t nlinks) except <herr_t>-1
cdef herr_t H5Pget_nlinks(hid_t plist_id, size_t *nlinks) except <herr_t>-1
cdef herr_t H5Pset_elink_prefix(hid_t plist_id, char *prefix) except <herr_t>-1
cdef ssize_t H5Pget_elink_prefix(hid_t plist_id, char *prefix, size_t size) except <ssize_t>-1
cdef hid_t H5Pget_elink_fapl(hid_t lapl_id) except <hid_t>-1
cdef herr_t H5Pset_elink_fapl(hid_t lapl_id, hid_t fapl_id) except <herr_t>-1
cdef herr_t H5Pget_elink_acc_flags(hid_t lapl_id, unsigned int *flags) except <herr_t>-1
cdef herr_t H5Pset_elink_acc_flags(hid_t lapl_id, unsigned int flags) except <herr_t>-1
cdef herr_t H5Pset_create_intermediate_group(hid_t plist_id, unsigned crt_intmd) except <herr_t>-1
cdef herr_t H5Pget_create_intermediate_group(hid_t plist_id, unsigned *crt_intmd) except <herr_t>-1
cdef herr_t H5Pset_copy_object(hid_t plist_id, unsigned crt_intmd) except <herr_t>-1
cdef herr_t H5Pget_copy_object(hid_t plist_id, unsigned *crt_intmd) except <herr_t>-1
cdef herr_t H5Pset_char_encoding(hid_t plist_id, H5T_cset_t encoding) except <herr_t>-1
cdef herr_t H5Pget_char_encoding(hid_t plist_id, H5T_cset_t *encoding) except <herr_t>-1
cdef herr_t H5Pset_attr_creation_order(hid_t plist_id, unsigned crt_order_flags) except <herr_t>-1
cdef herr_t H5Pget_attr_creation_order(hid_t plist_id, unsigned *crt_order_flags) except <herr_t>-1
cdef herr_t H5Pset_obj_track_times( hid_t ocpl_id, hbool_t track_times ) except <herr_t>-1
cdef herr_t H5Pget_obj_track_times( hid_t ocpl_id, hbool_t *track_times ) except <herr_t>-1
cdef herr_t H5Pset_local_heap_size_hint(hid_t plist_id, size_t size_hint) except <herr_t>-1
cdef herr_t H5Pget_local_heap_size_hint(hid_t plist_id, size_t *size_hint) except <herr_t>-1
cdef herr_t H5Pset_link_phase_change(hid_t plist_id, unsigned max_compact, unsigned min_dense) except <herr_t>-1
cdef herr_t H5Pget_link_phase_change(hid_t plist_id, unsigned *max_compact , unsigned *min_dense) except <herr_t>-1
cdef herr_t H5Pset_attr_phase_change(hid_t ocpl_id, unsigned max_compact, unsigned min_dense) except <herr_t>-1
cdef herr_t H5Pget_attr_phase_change(hid_t ocpl_id, unsigned *max_compact , unsigned *min_dense) except <herr_t>-1
cdef herr_t H5Pset_est_link_info(hid_t plist_id, unsigned est_num_entries, unsigned est_name_len) except <herr_t>-1
cdef herr_t H5Pget_est_link_info(hid_t plist_id, unsigned *est_num_entries , unsigned *est_name_len) except <herr_t>-1
cdef herr_t H5Pset_link_creation_order(hid_t plist_id, unsigned crt_order_flags) except <herr_t>-1
cdef herr_t H5Pget_link_creation_order(hid_t plist_id, unsigned *crt_order_flags) except <herr_t>-1
cdef herr_t H5Pset_libver_bounds(hid_t fapl_id, H5F_libver_t libver_low, H5F_libver_t libver_high) except <herr_t>-1
cdef herr_t H5Pget_libver_bounds(hid_t fapl_id, H5F_libver_t *libver_low, H5F_libver_t *libver_high) except <herr_t>-1
cdef herr_t H5Pset_alignment(hid_t plist_id, hsize_t threshold, hsize_t alignment) except <herr_t>-1
cdef herr_t H5Pget_alignment(hid_t plist_id, hsize_t *threshold, hsize_t *alignment) except <herr_t>-1
cdef herr_t H5PLappend(const char *search_path) except <herr_t>-1
cdef herr_t H5PLprepend(const char *search_path) except <herr_t>-1
cdef herr_t H5PLreplace(const char *search_path, unsigned int index) except <herr_t>-1
cdef herr_t H5PLinsert(const char *search_path, unsigned int index) except <herr_t>-1
cdef herr_t H5PLremove(unsigned int index) except <herr_t>-1
cdef ssize_t H5PLget(unsigned int index, char *path_buf, size_t buf_size) except <ssize_t>-1
cdef herr_t H5PLsize(unsigned int *num_paths) except <herr_t>-1
cdef herr_t H5Rcreate(void *ref, hid_t loc_id, char *name, H5R_type_t ref_type,  hid_t space_id) except <herr_t>-1
cdef hid_t H5Rdereference(hid_t obj_id, hid_t oapl_id, H5R_type_t ref_type, void *ref) except <hid_t>-1
cdef hid_t H5Rget_region(hid_t dataset, H5R_type_t ref_type, void *ref) except <hid_t>-1
cdef herr_t H5Rget_obj_type(hid_t id, H5R_type_t ref_type, void *ref, H5O_type_t *obj_type) except <herr_t>-1
cdef ssize_t H5Rget_name(hid_t loc_id, H5R_type_t ref_type, void *ref, char *name, size_t size) except <ssize_t>-1
cdef hid_t H5Screate(H5S_class_t type) except <hid_t>-1
cdef hid_t H5Scopy(hid_t space_id ) except <hid_t>-1
cdef herr_t H5Sclose(hid_t space_id) except <herr_t>-1
cdef hid_t H5Screate_simple(int rank, hsize_t *dims, hsize_t *maxdims) except <hid_t>-1
cdef htri_t H5Sis_simple(hid_t space_id) except <htri_t>-1
cdef herr_t H5Soffset_simple(hid_t space_id, hssize_t *offset ) except <herr_t>-1
cdef int H5Sget_simple_extent_ndims(hid_t space_id) except <int>-1
cdef int H5Sget_simple_extent_dims(hid_t space_id, hsize_t *dims, hsize_t *maxdims) except <int>-1
cdef hssize_t H5Sget_simple_extent_npoints(hid_t space_id) except <hssize_t>-1
cdef H5S_class_t H5Sget_simple_extent_type(hid_t space_id) except <H5S_class_t>-1
cdef herr_t H5Sextent_copy(hid_t dest_space_id, hid_t source_space_id ) except <herr_t>-1
cdef herr_t H5Sset_extent_simple(hid_t space_id, int rank, hsize_t *current_size, hsize_t *maximum_size ) except <herr_t>-1
cdef herr_t H5Sset_extent_none(hid_t space_id) except <herr_t>-1
cdef H5S_sel_type H5Sget_select_type(hid_t space_id) except <H5S_sel_type>-1
cdef hssize_t H5Sget_select_npoints(hid_t space_id) except <hssize_t>-1
cdef herr_t H5Sget_select_bounds(hid_t space_id, hsize_t *start, hsize_t *end) except <herr_t>-1
cdef herr_t H5Sselect_all(hid_t space_id) except <herr_t>-1
cdef herr_t H5Sselect_none(hid_t space_id) except <herr_t>-1
cdef htri_t H5Sselect_valid(hid_t space_id) except <htri_t>-1
cdef hssize_t H5Sget_select_elem_npoints(hid_t space_id) except <hssize_t>-1
cdef herr_t H5Sget_select_elem_pointlist(hid_t space_id, hsize_t startpoint,  hsize_t numpoints, hsize_t *buf) except <herr_t>-1
cdef herr_t H5Sselect_elements(hid_t space_id, H5S_seloper_t op,  size_t num_elements, const hsize_t *coord) except <herr_t>-1
cdef hssize_t H5Sget_select_hyper_nblocks(hid_t space_id ) except <hssize_t>-1
cdef herr_t H5Sget_select_hyper_blocklist(hid_t space_id,  hsize_t startblock, hsize_t numblocks, hsize_t *buf ) except <herr_t>-1
cdef herr_t H5Sselect_hyperslab(hid_t space_id, H5S_seloper_t op,  hsize_t *start, hsize_t *_stride, hsize_t *count, hsize_t *_block) except <herr_t>-1
cdef herr_t H5Sencode(hid_t obj_id, void *buf, size_t *nalloc) except <herr_t>-1
cdef hid_t H5Sdecode(void *buf) except <hid_t>-1
cdef htri_t H5Sis_regular_hyperslab(hid_t spaceid) except <htri_t>-1
cdef htri_t H5Sget_regular_hyperslab(hid_t spaceid, hsize_t* start, hsize_t* stride, hsize_t* count, hsize_t* block) except <htri_t>-1
cdef htri_t H5Sselect_shape_same(hid_t space1_id, hid_t space2_id) except <htri_t>-1
cdef hid_t H5Tcreate(H5T_class_t type, size_t size) except <hid_t>-1
cdef hid_t H5Topen(hid_t loc, char* name, hid_t tapl_id) except <hid_t>-1
cdef htri_t H5Tcommitted(hid_t type) except <htri_t>-1
cdef hid_t H5Tcopy(hid_t type_id) except <hid_t>-1
cdef htri_t H5Tequal(hid_t type_id1, hid_t type_id2 ) except <htri_t>-1
cdef herr_t H5Tlock(hid_t type_id) except <herr_t>-1
cdef H5T_class_t H5Tget_class(hid_t type_id) except <H5T_class_t>-1
cdef size_t H5Tget_size(hid_t type_id) except <size_t>0
cdef hid_t H5Tget_super(hid_t type) except <hid_t>-1
cdef htri_t H5Tdetect_class(hid_t type_id, H5T_class_t dtype_class) except <htri_t>-1
cdef herr_t H5Tclose(hid_t type_id) except <herr_t>-1
cdef hid_t H5Tget_native_type(hid_t type_id, H5T_direction_t direction) except <hid_t>-1
cdef herr_t H5Tcommit(hid_t loc_id, char *name, hid_t dtype_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id) except <herr_t>-1
cdef hid_t H5Tget_create_plist(hid_t dtype_id) except <hid_t>-1
cdef hid_t H5Tdecode(unsigned char *buf) except <hid_t>-1
cdef herr_t H5Tencode(hid_t obj_id, unsigned char *buf, size_t *nalloc) except <herr_t>-1
cdef H5T_conv_t H5Tfind(hid_t src_id, hid_t dst_id, H5T_cdata_t **pcdata) except <H5T_conv_t>NULL
cdef herr_t H5Tconvert(hid_t src_id, hid_t dst_id, size_t nelmts, void *buf, void *background, hid_t plist_id) except <herr_t>-1
cdef herr_t H5Tregister(H5T_pers_t pers, char *name, hid_t src_id, hid_t dst_id, H5T_conv_t func) except <herr_t>-1
cdef herr_t H5Tunregister(H5T_pers_t pers, char *name, hid_t src_id, hid_t dst_id, H5T_conv_t func) except <herr_t>-1
cdef herr_t H5Tset_size(hid_t type_id, size_t size) except <herr_t>-1
cdef H5T_order_t H5Tget_order(hid_t type_id) except <H5T_order_t>-1
cdef herr_t H5Tset_order(hid_t type_id, H5T_order_t order) except <herr_t>-1
cdef hsize_t H5Tget_precision(hid_t type_id) except <hsize_t>0
cdef herr_t H5Tset_precision(hid_t type_id, size_t prec) except <herr_t>-1
cdef int H5Tget_offset(hid_t type_id) except <int>-1
cdef herr_t H5Tset_offset(hid_t type_id, size_t offset) except <herr_t>-1
cdef herr_t H5Tget_pad(hid_t type_id, H5T_pad_t * lsb, H5T_pad_t * msb ) except <herr_t>-1
cdef herr_t H5Tset_pad(hid_t type_id, H5T_pad_t lsb, H5T_pad_t msb ) except <herr_t>-1
cdef H5T_sign_t H5Tget_sign(hid_t type_id) except <H5T_sign_t>-1
cdef herr_t H5Tset_sign(hid_t type_id, H5T_sign_t sign) except <herr_t>-1
cdef herr_t H5Tget_fields(hid_t type_id, size_t *spos, size_t *epos, size_t *esize, size_t *mpos, size_t *msize ) except <herr_t>-1
cdef herr_t H5Tset_fields(hid_t type_id, size_t spos, size_t epos, size_t esize, size_t mpos, size_t msize ) except <herr_t>-1
cdef size_t H5Tget_ebias(hid_t type_id) except <size_t>0
cdef herr_t H5Tset_ebias(hid_t type_id, size_t ebias) except <herr_t>-1
cdef H5T_norm_t H5Tget_norm(hid_t type_id) except <H5T_norm_t>-1
cdef herr_t H5Tset_norm(hid_t type_id, H5T_norm_t norm) except <herr_t>-1
cdef H5T_pad_t H5Tget_inpad(hid_t type_id) except <H5T_pad_t>-1
cdef herr_t H5Tset_inpad(hid_t type_id, H5T_pad_t inpad) except <herr_t>-1
cdef H5T_cset_t H5Tget_cset(hid_t type_id) except <H5T_cset_t>-1
cdef herr_t H5Tset_cset(hid_t type_id, H5T_cset_t cset) except <herr_t>-1
cdef H5T_str_t H5Tget_strpad(hid_t type_id) except <H5T_str_t>-1
cdef herr_t H5Tset_strpad(hid_t type_id, H5T_str_t strpad) except <herr_t>-1
cdef hid_t H5Tvlen_create(hid_t base_type_id) except <hid_t>-1
cdef htri_t H5Tis_variable_str(hid_t dtype_id) except <htri_t>-1
cdef int H5Tget_nmembers(hid_t type_id) except <int>-1
cdef H5T_class_t H5Tget_member_class(hid_t type_id, int member_no) except <H5T_class_t>-1
cdef char* H5Tget_member_name(hid_t type_id, unsigned membno) except <char*>NULL
cdef hid_t H5Tget_member_type(hid_t type_id, unsigned membno) except <hid_t>-1
cdef int H5Tget_member_offset(hid_t type_id, int membno) except <int>-1
cdef int H5Tget_member_index(hid_t type_id, char* name) except <int>-1
cdef herr_t H5Tinsert(hid_t parent_id, char *name, size_t offset, hid_t member_id) except <herr_t>-1
cdef herr_t H5Tpack(hid_t type_id) except <herr_t>-1
cdef hid_t H5Tenum_create(hid_t base_id) except <hid_t>-1
cdef herr_t H5Tenum_insert(hid_t type, char *name, void *value) except <herr_t>-1
cdef herr_t H5Tenum_nameof( hid_t type, void *value, char *name, size_t size ) except <herr_t>-1
cdef herr_t H5Tenum_valueof( hid_t type, char *name, void *value ) except <herr_t>-1
cdef herr_t H5Tget_member_value(hid_t type,  unsigned int memb_no, void *value ) except <herr_t>-1
cdef hid_t H5Tarray_create(hid_t base_id, int ndims, hsize_t *dims) except <hid_t>-1
cdef int H5Tget_array_ndims(hid_t type_id) except <int>-1
cdef int H5Tget_array_dims(hid_t type_id, hsize_t *dims) except <int>-1
cdef herr_t H5Tset_tag(hid_t type_id, char* tag) except <herr_t>-1
cdef char* H5Tget_tag(hid_t type_id) except <char*>NULL
cdef htri_t H5Zfilter_avail(H5Z_filter_t id_) except <htri_t>-1
cdef herr_t H5Zget_filter_info(H5Z_filter_t filter_, unsigned int *filter_config_flags) except <herr_t>-1
cdef herr_t H5Zregister(const void *cls) except <herr_t>-1
cdef herr_t H5Zunregister(H5Z_filter_t id_) except <herr_t>-1
cdef herr_t H5DSattach_scale(hid_t did, hid_t dsid, unsigned int idx) except <herr_t>-1
cdef herr_t H5DSdetach_scale(hid_t did, hid_t dsid, unsigned int idx) except <herr_t>-1
cdef htri_t H5DSis_attached(hid_t did, hid_t dsid, unsigned int idx) except <htri_t>-1
cdef herr_t H5DSset_scale(hid_t dsid, char *dimname) except <herr_t>-1
cdef int H5DSget_num_scales(hid_t did, unsigned int dim) except <int>-1
cdef herr_t H5DSset_label(hid_t did, unsigned int idx, char *label) except <herr_t>-1
cdef ssize_t H5DSget_label(hid_t did, unsigned int idx, char *label, size_t size) except <ssize_t>-1
cdef ssize_t H5DSget_scale_name(hid_t did, char *name, size_t size) except <ssize_t>-1
cdef htri_t H5DSis_scale(hid_t did) except <htri_t>-1
cdef herr_t H5DSiterate_scales(hid_t did, unsigned int dim, int *idx, H5DS_iterate_t visitor, void *visitor_data) except <herr_t>-1
