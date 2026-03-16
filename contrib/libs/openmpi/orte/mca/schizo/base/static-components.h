/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_schizo_flux_component;
extern const mca_base_component_t mca_schizo_ompi_component;
extern const mca_base_component_t mca_schizo_orte_component;
extern const mca_base_component_t mca_schizo_slurm_component;

const mca_base_component_t *mca_schizo_base_static_components[] = {
  &mca_schizo_flux_component, 
  &mca_schizo_ompi_component, 
  &mca_schizo_orte_component, 
  &mca_schizo_slurm_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

