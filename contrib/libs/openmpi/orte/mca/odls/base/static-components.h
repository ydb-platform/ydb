/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_odls_default_component;
extern const mca_base_component_t mca_odls_pspawn_component;

const mca_base_component_t *mca_odls_base_static_components[] = {
  &mca_odls_default_component, 
  &mca_odls_pspawn_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

