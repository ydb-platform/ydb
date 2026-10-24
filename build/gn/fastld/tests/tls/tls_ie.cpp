// Initial-exec TLS in a .so (like jemalloc in the real build: DF_STATIC_TLS, TPOFF64).
__attribute__((tls_model("initial-exec"))) thread_local int ie_tls = 909;
int ie_get() { return ie_tls; }
void ie_inc() { ++ie_tls; }
