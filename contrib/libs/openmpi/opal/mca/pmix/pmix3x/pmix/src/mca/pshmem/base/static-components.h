/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const pmix_mca_base_component_t mca_pshmem_mmap_component;

const pmix_mca_base_component_t *mca_pshmem_base_static_components[] = {
  &mca_pshmem_mmap_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

