/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const pmix_mca_base_component_t mca_preg_native_component;

const pmix_mca_base_component_t *mca_preg_base_static_components[] = {
  &mca_preg_native_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

