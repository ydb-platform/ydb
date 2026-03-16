/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_regx_fwd_component;
extern const mca_base_component_t mca_regx_reverse_component;

const mca_base_component_t *mca_regx_base_static_components[] = {
  &mca_regx_fwd_component, 
  &mca_regx_reverse_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

