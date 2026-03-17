/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const pmix_mca_base_component_t mca_pdl_pdlopen_component;

const pmix_mca_base_component_t *mca_pdl_base_static_components[] = {
  &mca_pdl_pdlopen_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

