/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_fcoll_dynamic_component;
extern const mca_base_component_t mca_fcoll_dynamic_gen2_component;
extern const mca_base_component_t mca_fcoll_individual_component;
extern const mca_base_component_t mca_fcoll_two_phase_component;
extern const mca_base_component_t mca_fcoll_vulcan_component;

const mca_base_component_t *mca_fcoll_base_static_components[] = {
  &mca_fcoll_dynamic_component, 
  &mca_fcoll_dynamic_gen2_component, 
  &mca_fcoll_individual_component, 
  &mca_fcoll_two_phase_component, 
  &mca_fcoll_vulcan_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

