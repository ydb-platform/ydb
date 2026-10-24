extern const char *gone(void) __attribute__((weak));
int weak_gone_is_null(void) { return gone == 0; }
