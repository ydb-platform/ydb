#ifndef PLYVEL_COMPARATOR_H
#define PLYVEL_COMPARATOR_H

#include <leveldb/comparator.h>

leveldb::Comparator* NewPlyvelCallbackComparator(const char* name, PyObject* comparator);

#endif
