const char *common_fn(void) { return "A"; }
__attribute__((weak)) const char *weak_fn(void) { return "A"; }
int common_var = 1;
const char *prot_fn(void) { return "A"; }
const char *a_calls_common(void) { return common_fn(); }
