// dlopen'ed later: its TLS block is dynamic (not in static TLS).
thread_local int p_tls = 404;
thread_local char p_big[4096] = {1};
extern "C" int p_get() { return p_tls + p_big[0]; }
extern "C" void p_inc() { ++p_tls; }
