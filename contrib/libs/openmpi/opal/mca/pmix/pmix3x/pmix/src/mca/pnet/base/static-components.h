/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const pmix_mca_base_component_t mca_pnet_tcp_component;
extern const pmix_mca_base_component_t mca_pnet_test_component;

const pmix_mca_base_component_t *mca_pnet_base_static_components[] = {
  &mca_pnet_tcp_component, 
  &mca_pnet_test_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

