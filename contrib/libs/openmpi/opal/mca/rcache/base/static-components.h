/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_rcache_grdma_component;
extern const mca_base_component_t mca_rcache_gpusm_component;
extern const mca_base_component_t mca_rcache_rgpusm_component;

const mca_base_component_t *mca_rcache_base_static_components[] = {
  &mca_rcache_grdma_component, 
  &mca_rcache_gpusm_component, 
  &mca_rcache_rgpusm_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

