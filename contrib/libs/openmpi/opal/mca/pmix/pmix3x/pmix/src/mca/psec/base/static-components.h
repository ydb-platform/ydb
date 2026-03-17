/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const pmix_mca_base_component_t mca_psec_native_component;
extern const pmix_mca_base_component_t mca_psec_none_component;

const pmix_mca_base_component_t *mca_psec_base_static_components[] = {
  &mca_psec_native_component, 
  &mca_psec_none_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

