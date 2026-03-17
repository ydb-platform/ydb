#ifndef YTLS_H
#define YTLS_H

#include "Python.h"
#include "pythread.h"

#if PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION >= 7
    #define USE_NEW_TSS_API
#endif

#ifdef USE_NEW_TSS_API
    #define YPY_KEY_TYPE Py_tss_t*
#else
    #define YPY_KEY_TYPE int
#endif

typedef struct {
    YPY_KEY_TYPE _key;
} tls_key_t;


tls_key_t* create_tls_key(void);
int set_tls_key_value(tls_key_t* key, void* value);
void* get_tls_key_value(tls_key_t* key);
void delete_tls_key(tls_key_t* key);
#endif
