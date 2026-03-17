/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const pmix_mca_base_component_t mca_bfrops_v12_component;
extern const pmix_mca_base_component_t mca_bfrops_v20_component;
extern const pmix_mca_base_component_t mca_bfrops_v21_component;
extern const pmix_mca_base_component_t mca_bfrops_v3_component;

const pmix_mca_base_component_t *mca_bfrops_base_static_components[] = {
  &mca_bfrops_v12_component, 
  &mca_bfrops_v20_component, 
  &mca_bfrops_v21_component, 
  &mca_bfrops_v3_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

