#include "loadbuffer.h"


int
loadbuffer_open(LoadBuffer* input, const char* path, PyObject* deserializer) {

    ASSERT(input != NULL);
    ASSERT(path != NULL);

    input->file         = NULL;
    input->lookup       = NULL;
    input->size         = 0;
    input->capacity     = 0;
    input->deserializer = deserializer;

    input->file = fopen(path, "rb");
    if (UNLIKELY(input->file == NULL)) {
        PyErr_SetFromErrno(PyExc_IOError);
        return 0;
    }

    return 1;
}

int
loadbuffer_load(LoadBuffer* input, char* buffer, size_t size) {

    size_t read;

    ASSERT(input != NULL);
    ASSERT(buffer != NULL);

    if (UNLIKELY(size == 0)) {
        PyErr_SetString(PyExc_ValueError, "logic error: tried to read 0 bytes");
        return 0;
    }

    read = fread(buffer, 1, size, input->file);
    if (read != size) {
        PyErr_SetFromErrno(PyExc_IOError);
        return 0;
    }

    return 1;
}

int
loadbuffer_init(LoadBuffer* input, CustompickleHeader* header, CustompickleFooter* footer) {

    long pos;
    int ret;

    ASSERT(input != NULL);
    ASSERT(header != NULL);
    ASSERT(footer != NULL);

    ret = loadbuffer_loadinto(input, header, CustompickleHeader);
    if (UNLIKELY(!ret)) {
        return 0;
    }

    pos = ftell(input->file);
    if (UNLIKELY(pos < 0)) {
        PyErr_SetFromErrno(PyExc_IOError);
        return 0;
    }

    ret = fseek(input->file, -(long int)sizeof(CustompickleFooter), SEEK_END);
    if (UNLIKELY(ret < 0)) {
        PyErr_SetFromErrno(PyExc_IOError);
        return 0;
    }

    ret = loadbuffer_loadinto(input, footer, CustompickleFooter);
    if (UNLIKELY(!ret)) {
        return 0;
    }

    ret = fseek(input->file, pos, SEEK_SET);
    if (UNLIKELY(ret < 0)) {
        PyErr_SetFromErrno(PyExc_IOError);
        return 0;
    }

    if (UNLIKELY(!custompickle_validate_header(header))) {
        PyErr_Format(PyExc_ValueError, "invalid header");
        return 0;
    }

    if (UNLIKELY(!custompickle_validate_footer(footer))) {
        PyErr_Format(PyExc_ValueError, "invalid footer");
        return 0;
    }

    input->store    = header->data.store;
    input->kind     = header->data.kind;
    input->size     = 0;
    input->capacity = footer->nodes_count;
    input->lookup   = (AddressPair*)memory_alloc(sizeof(AddressPair) * input->capacity);
    if (UNLIKELY(input->lookup == NULL)) {
        PyErr_NoMemory();
        return 0;
    }

    return 1;
}

void
loadbuffer_invalidate(LoadBuffer* input) {

    ASSERT(input != NULL);

    input->size = 0;
}

void
loadbuffer_close(LoadBuffer* input) {

    TrieNode* node;
    size_t i;

    if (input->file != NULL) {
        fclose(input->file);
    }

    if (input->lookup) {
        for (i=0; i < input->size; i++) {
            node = input->lookup[i].current;

            if (node->eow && input->store == STORE_ANY) {
                Py_DECREF(node->output.object);
            }

            trienode_free(node);
        }

        memory_free(input->lookup);
    }
}


void
loadbuffer_dump(LoadBuffer* input, FILE* out) {

    AddressPair* pair;
    size_t i;

    for (i=0; i < input->size; i++) {
        pair = &(input->lookup[i]);
        fprintf(out, "%p -> %p\n", pair->original, pair->current);
    }
}
