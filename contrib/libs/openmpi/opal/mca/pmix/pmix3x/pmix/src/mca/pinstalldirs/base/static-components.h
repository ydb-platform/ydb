/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const pmix_mca_base_component_t mca_pinstalldirs_env_component;
extern const pmix_mca_base_component_t mca_pinstalldirs_config_component;

const pmix_mca_base_component_t *mca_pinstalldirs_base_static_components[] = {
  &mca_pinstalldirs_env_component, 
  &mca_pinstalldirs_config_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

