#pragma once

#include "config.h"
#include <stdlib.h>

struct vector_t {
    size_t size;
    size_t capacity;
    size_t *values;
};

struct vector_t*  vector_create();
void vector_destroy(struct vector_t* vec);
size_t vector_size(struct vector_t* vec);
void vector_push_back(struct vector_t *vec, size_t offset);
size_t vector_at(struct vector_t *vector, size_t idx);
int vector_equal(struct vector_t *vec1, struct vector_t *vec2);
size_t* vector_data(struct vector_t *vec);
