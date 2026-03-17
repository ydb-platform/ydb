/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_ras_simulator_component;
extern const mca_base_component_t mca_ras_slurm_component;

const mca_base_component_t *mca_ras_base_static_components[] = {
  &mca_ras_simulator_component, 
  &mca_ras_slurm_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

