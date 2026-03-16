/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const pmix_mca_base_component_t mca_psensor_file_component;
extern const pmix_mca_base_component_t mca_psensor_heartbeat_component;

const pmix_mca_base_component_t *mca_psensor_base_static_components[] = {
  &mca_psensor_file_component, 
  &mca_psensor_heartbeat_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

