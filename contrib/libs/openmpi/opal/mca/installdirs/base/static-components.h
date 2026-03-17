/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_installdirs_env_component;
extern const mca_base_component_t mca_installdirs_config_component;

const mca_base_component_t *mca_installdirs_base_static_components[] = {
  &mca_installdirs_env_component, 
  &mca_installdirs_config_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

