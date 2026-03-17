/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_shmem_mmap_component;
extern const mca_base_component_t mca_shmem_posix_component;
extern const mca_base_component_t mca_shmem_sysv_component;

const mca_base_component_t *mca_shmem_base_static_components[] = {
  &mca_shmem_mmap_component, 
  &mca_shmem_posix_component, 
  &mca_shmem_sysv_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

