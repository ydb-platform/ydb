/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_ess_env_component;
extern const mca_base_component_t mca_ess_hnp_component;
extern const mca_base_component_t mca_ess_pmi_component;
extern const mca_base_component_t mca_ess_singleton_component;
extern const mca_base_component_t mca_ess_tool_component;
extern const mca_base_component_t mca_ess_slurm_component;

const mca_base_component_t *mca_ess_base_static_components[] = {
  &mca_ess_env_component, 
  &mca_ess_hnp_component, 
  &mca_ess_pmi_component, 
  &mca_ess_singleton_component, 
  &mca_ess_tool_component, 
  &mca_ess_slurm_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

