/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_patcher_overwrite_component;

const mca_base_component_t *mca_patcher_base_static_components[] = {
  &mca_patcher_overwrite_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

