/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const pmix_mca_base_component_t mca_ptl_tcp_component;
extern const pmix_mca_base_component_t mca_ptl_usock_component;

const pmix_mca_base_component_t *mca_ptl_base_static_components[] = {
  &mca_ptl_tcp_component, 
  &mca_ptl_usock_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

