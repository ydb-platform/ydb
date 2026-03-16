#pragma once

#include "../../Automaton.h"

#define SAVEBUFFER_DEFAULT_SIZE (32 * 1024lu)

typedef struct SaveBuffer {
    KeysStore   store;
    FILE*       file;
    char*       buffer;
    size_t      size;
    size_t      capacity;

    PyObject*   serializer;
    size_t      nodes_count;    ///< the total number of stored nodes
} SaveBuffer;

bool
savebuffer_init(SaveBuffer* save, PyObject* serializer, KeysStore store, const char* path, size_t capacity);

void
savebuffer_flush(SaveBuffer* save);

char*
savebuffer_acquire(SaveBuffer* save, size_t request);

void
savebuffer_store(SaveBuffer* save, const char* data, size_t size);

void
savebuffer_store_pointer(SaveBuffer* save, void* ptr);

void
savebuffer_finalize(SaveBuffer* save);
