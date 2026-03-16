/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_btl_self_component;
extern const mca_base_component_t mca_btl_sm_component;
extern const mca_base_component_t mca_btl_smcuda_component;
extern const mca_base_component_t mca_btl_tcp_component;
extern const mca_base_component_t mca_btl_vader_component;

const mca_base_component_t *mca_btl_base_static_components[] = {
  &mca_btl_self_component, 
  &mca_btl_sm_component, 
  &mca_btl_smcuda_component, 
  &mca_btl_tcp_component, 
  &mca_btl_vader_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

