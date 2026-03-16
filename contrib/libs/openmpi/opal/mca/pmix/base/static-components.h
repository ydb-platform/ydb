/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_pmix_isolated_component;
extern const mca_base_component_t mca_pmix_pmix3x_component;

const mca_base_component_t *mca_pmix_base_static_components[] = {
  &mca_pmix_isolated_component, 
  &mca_pmix_pmix3x_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

