#ifndef _MULTIDICT_C_H
#define _MULTIDICT_C_H

#ifdef __cplusplus
extern "C" {
#endif

#include "pythoncapi_compat.h"
#include "pair_list.h"

#if PY_VERSION_HEX >= 0x030c00f0
#define MANAGED_WEAKREFS
#endif


typedef struct {  // 16 or 24 for GC prefix
    PyObject_HEAD  // 16
#ifndef MANAGED_WEAKREFS
    PyObject *weaklist;
#endif
    pair_list_t pairs;
} MultiDictObject;

typedef struct {
    PyObject_HEAD
#ifndef MANAGED_WEAKREFS
    PyObject *weaklist;
#endif
    MultiDictObject *md;
} MultiDictProxyObject;


#ifdef __cplusplus
}
#endif

#endif
