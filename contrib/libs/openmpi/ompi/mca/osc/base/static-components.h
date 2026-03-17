/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_osc_sm_component;
extern const mca_base_component_t mca_osc_pt2pt_component;
extern const mca_base_component_t mca_osc_rdma_component;

const mca_base_component_t *mca_osc_base_static_components[] = {
  &mca_osc_sm_component, 
  &mca_osc_pt2pt_component, 
  &mca_osc_rdma_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

