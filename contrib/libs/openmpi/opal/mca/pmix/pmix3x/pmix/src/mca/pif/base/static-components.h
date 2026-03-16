/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const pmix_mca_base_component_t mca_pif_linux_ipv6_component;
extern const pmix_mca_base_component_t mca_pif_posix_ipv4_component;

const pmix_mca_base_component_t *mca_pif_base_static_components[] = {
  &mca_pif_linux_ipv6_component, 
  &mca_pif_posix_ipv4_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

