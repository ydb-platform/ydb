#include "savebuffer.h"

bool
savebuffer_init(SaveBuffer* output, PyObject* serializer, KeysStore store, const char* path, size_t capacity) {

    output->store       = store;
    output->file        = NULL;
    output->buffer      = NULL;
    output->size        = 0;
    output->capacity    = capacity;
    output->serializer  = serializer;
    output->nodes_count = 0;

    if (PICKLE_SIZE_T_SIZE < sizeof(PyObject*)) {
        // XXX: this must be reworked, likely moved to module level
        PyErr_SetString(PyExc_SystemError, "unable to save data due to technical reasons");
        return false;
    }

    if (UNLIKELY(store == STORE_ANY && serializer == NULL)) {
        PyErr_SetString(PyExc_ValueError, "for automatons with STORE_ANY serializer must be given");
        return false;
    }

    output->buffer = (char*)memory_alloc(capacity);
    if (UNLIKELY(output->buffer == NULL)) {
        PyErr_NoMemory();
        return false;
    }

    output->file = fopen(path, "wb");
    if (output->file == NULL) {
        memory_free(output->buffer);
        output->buffer = NULL;
        PyErr_SetFromErrno(PyExc_IOError);
        return false;
    }

    return true;
}


void
savebuffer_flush(SaveBuffer* output) {
    if (output->size != fwrite(output->buffer, 1, output->size, output->file)) {
        PyErr_SetFromErrno(PyExc_IOError);
    }

    output->size = 0;
}


char*
savebuffer_acquire(SaveBuffer* output, size_t request) {

    char* ptr;

    if (UNLIKELY(request > output->capacity)) {
        return NULL;
    }

    if (UNLIKELY(output->size + request > output->capacity)) {
        savebuffer_flush(output);
    }

    ptr = output->buffer + output->size;
    output->size += request;

    return ptr;
}


void
savebuffer_store(SaveBuffer* output, const char* data, size_t size) {

    if (UNLIKELY(size > output->capacity)) {
        savebuffer_flush(output);
        if (fwrite(data, 1, size, output->file) != size) {
            PyErr_SetFromErrno(PyExc_IOError);
        }
        return;
    }

    if (UNLIKELY(output->size + size >= output->capacity)) {
        savebuffer_flush(output);
    }

    memcpy(output->buffer + output->size, data, size);
    output->size += size;
}


void
savebuffer_store_pointer(SaveBuffer* save, void* ptr) {
    char* buf;

    buf = savebuffer_acquire(save, sizeof(void*));
    *((void**)buf) = ptr;
}


void
savebuffer_finalize(SaveBuffer* output) {

    if (output->buffer != NULL && output->file != NULL && output->size > 0) {
        savebuffer_flush(output);
    }

    memory_safefree(output->buffer);

    if (output->file != NULL) {
        fclose(output->file);
    }
}
