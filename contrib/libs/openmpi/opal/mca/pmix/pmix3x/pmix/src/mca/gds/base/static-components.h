/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const pmix_mca_base_component_t mca_gds_ds12_component;
extern const pmix_mca_base_component_t mca_gds_ds21_component;
extern const pmix_mca_base_component_t mca_gds_hash_component;

const pmix_mca_base_component_t *mca_gds_base_static_components[] = {
  &mca_gds_ds12_component, 
  &mca_gds_ds21_component, 
  &mca_gds_hash_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

