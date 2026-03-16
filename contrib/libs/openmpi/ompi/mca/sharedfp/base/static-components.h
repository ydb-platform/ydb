/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_sharedfp_individual_component;
extern const mca_base_component_t mca_sharedfp_lockedfile_component;
extern const mca_base_component_t mca_sharedfp_sm_component;

const mca_base_component_t *mca_sharedfp_base_static_components[] = {
  &mca_sharedfp_individual_component, 
  &mca_sharedfp_lockedfile_component, 
  &mca_sharedfp_sm_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

