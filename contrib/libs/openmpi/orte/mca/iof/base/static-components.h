/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_iof_hnp_component;
extern const mca_base_component_t mca_iof_orted_component;
extern const mca_base_component_t mca_iof_tool_component;

const mca_base_component_t *mca_iof_base_static_components[] = {
  &mca_iof_hnp_component, 
  &mca_iof_orted_component, 
  &mca_iof_tool_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

