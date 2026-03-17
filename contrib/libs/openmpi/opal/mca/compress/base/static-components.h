/*
 * $HEADER$
 */
#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

extern const mca_base_component_t mca_compress_bzip_component;
extern const mca_base_component_t mca_compress_gzip_component;

const mca_base_component_t *mca_compress_base_static_components[] = {
  &mca_compress_bzip_component, 
  &mca_compress_gzip_component, 
  NULL
};

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

