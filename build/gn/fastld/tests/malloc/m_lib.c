#include <stdlib.h>
#include <string.h>
void *m_lib_malloc(void) { return malloc(24); }
char *m_lib_strdup(void) { return strdup("from lib"); }
void m_lib_free(void *p) { free(p); }
