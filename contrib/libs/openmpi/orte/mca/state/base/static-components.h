/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_state_app_component;
extern const mca_base_component_t mca_state_hnp_component;
extern const mca_base_component_t mca_state_novm_component;
extern const mca_base_component_t mca_state_orted_component;
extern const mca_base_component_t mca_state_tool_component;

const mca_base_component_t *mca_state_base_static_components[] = {
  &mca_state_app_component, 
  &mca_state_hnp_component, 
  &mca_state_novm_component, 
  &mca_state_orted_component, 
  &mca_state_tool_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

