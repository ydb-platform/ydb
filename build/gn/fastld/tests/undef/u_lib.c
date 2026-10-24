/* v1 (build time) defines gone(); v2 (installed over it after build) does not. */
const char *present(void) { return "present"; }
#ifdef V1
const char *gone(void) { return "gone"; }
#endif
