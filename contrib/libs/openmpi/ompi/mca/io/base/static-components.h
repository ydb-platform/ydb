/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_io_ompio_component;

const mca_base_component_t *mca_io_base_static_components[] = {
  &mca_io_ompio_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

