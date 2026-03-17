/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_dfs_app_component;
extern const mca_base_component_t mca_dfs_orted_component;
extern const mca_base_component_t mca_dfs_test_component;

const mca_base_component_t *mca_dfs_base_static_components[] = {
  &mca_dfs_app_component, 
  &mca_dfs_orted_component, 
  &mca_dfs_test_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

