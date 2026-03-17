#pragma once

#include <stdio.h>

#include "../../trienode.h"
#include "../custompickle.h"

typedef struct AddressPair {
    TrieNode* original;
    TrieNode* current;
} AddressPair;


typedef struct LoadBuffer {
    PyObject*     deserializer;
    FILE*         file;
    KeysStore     store;
    AutomatonKind kind;
    AddressPair*  lookup;
    size_t        size;
    size_t        capacity;
} LoadBuffer;

int
loadbuffer_open(LoadBuffer* input, const char* path, PyObject* deserializer);

int
loadbuffer_load(LoadBuffer* input, char* output, size_t size);

#define loadbuffer_loadinto(input, variable, type) \
    loadbuffer_load(input, (char*)(variable), sizeof(type))

int
loadbuffer_init(LoadBuffer* input, CustompickleHeader* header, CustompickleFooter* footer);

void
loadbuffer_invalidate(LoadBuffer* input);

void
loadbuffer_close(LoadBuffer* input);

void
loadbuffer_dump(LoadBuffer* input, FILE* out);
