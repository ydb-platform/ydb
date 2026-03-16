/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_rmaps_mindist_component;
extern const mca_base_component_t mca_rmaps_ppr_component;
extern const mca_base_component_t mca_rmaps_rank_file_component;
extern const mca_base_component_t mca_rmaps_resilient_component;
extern const mca_base_component_t mca_rmaps_round_robin_component;
extern const mca_base_component_t mca_rmaps_seq_component;

const mca_base_component_t *mca_rmaps_base_static_components[] = {
  &mca_rmaps_mindist_component, 
  &mca_rmaps_ppr_component, 
  &mca_rmaps_rank_file_component, 
  &mca_rmaps_resilient_component, 
  &mca_rmaps_round_robin_component, 
  &mca_rmaps_seq_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

