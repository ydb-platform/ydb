#include "tls.h"
#include "mem.h"

tls_key_t *
create_tls_key()
{

    tls_key_t* key;
    YPY_KEY_TYPE py_key;

    key = ymalloc(sizeof(tls_key_t));
    if(!key) {
        return NULL;
    }

#ifdef USE_NEW_TSS_API
    py_key = PyThread_tss_alloc();
    if (!py_key) {
        goto error;
    }

    if (PyThread_tss_create(py_key) != 0) {
        PyThread_tss_free(py_key);
        goto error;
    }

#else
    py_key = PyThread_create_key();
    if (py_key == -1) {
        goto error;
    }

#endif

    key->_key = py_key;
    return key;

error:
    yfree(key);
    return NULL;
}

int
set_tls_key_value(tls_key_t* key, void* value)
{
#ifdef USE_NEW_TSS_API
    return PyThread_tss_set(key->_key, value);
#else
    PyThread_delete_key_value(key->_key);
    return PyThread_set_key_value(key->_key, value);
#endif

}

void*
get_tls_key_value(tls_key_t* key)
{
    void* res;
#ifdef USE_NEW_TSS_API
    res = PyThread_tss_get(key->_key);
#else
    res = PyThread_get_key_value(key->_key);
#endif
    return res;
}

void
delete_tls_key(tls_key_t* key)
{
#ifdef USE_NEW_TSS_API
    PyThread_tss_delete(key->_key);
    PyThread_tss_free(key->_key);
#else
    PyThread_delete_key(key->_key);
#endif
    yfree(key);
}