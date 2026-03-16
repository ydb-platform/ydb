/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_pml_cm_component;
extern const mca_base_component_t mca_pml_ob1_component;
extern const mca_base_component_t mca_pml_v_component;

const mca_base_component_t *mca_pml_base_static_components[] = {
  &mca_pml_cm_component, 
  &mca_pml_ob1_component, 
  &mca_pml_v_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

