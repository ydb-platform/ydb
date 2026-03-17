cdef tuple get_ogr_vsimem_write_path(object path_or_fp, str driver)
cdef str read_buffer_to_vsimem(bytes bytes_buffer)
cdef read_vsimem_to_buffer(str path, object out_buffer)
cpdef vsimem_rmtree_toplevel(str path)
