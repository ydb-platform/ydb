/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_allocator_basic_component;
extern const mca_base_component_t mca_allocator_bucket_component;

const mca_base_component_t *mca_allocator_base_static_components[] = {
  &mca_allocator_basic_component, 
  &mca_allocator_bucket_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

