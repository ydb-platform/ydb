#include "vector.h"
#include <stdio.h>

#define VECTOR_INITIAL_CAPACITY 128
#define VECTOR_GROWTH_FACTOR 2

#define VECTOR_FATAL_ERROR(...) \
  { \
    fprintf(stderr, "Fatal error in %s at line %d: Exiting", __FILE__, __LINE__); \
    exit(1); \
  }

struct vector_t* vector_create()
{
  struct vector_t *vec = malloc(sizeof(struct vector_t));
  if (vec == NULL)
    VECTOR_FATAL_ERROR("Failed to allocate memory for vector\n");

  vec->capacity = VECTOR_INITIAL_CAPACITY;
  vec->size = 0;
  vec->values = malloc(sizeof(*vec->values) * vec->capacity);
  if (vec->values == NULL)
    VECTOR_FATAL_ERROR("Failed to allocate memory for vector values\n");
  return vec;
}

size_t vector_size(struct vector_t* vec)
{
  return vec->size;
}

void vector_destroy(struct vector_t *vec)
{
  free(vec->values);
  free(vec);
}

int vector_equal(struct vector_t *vec1, struct vector_t *vec2)
{
  if (vec1->size != vec2->size)
    return 0;
  for (size_t i = 0; i < vec1->size; i++)
    if (vec1->values[i] != vec2->values[i])
      return 0;
  return 1;
}

size_t vector_at(struct vector_t *vector, size_t idx)
{
  return vector->values[idx];
}

void vector_push_back(struct vector_t *vec, size_t value)
{
  if (vec->size == vec->capacity) {
    vec->capacity *= VECTOR_GROWTH_FACTOR;
    vec->values = realloc(vec->values, sizeof(*vec->values) * vec->capacity);
    if (vec->values == NULL)
      VECTOR_FATAL_ERROR("Failed to reallocate memory for vector values\n");
  }
  vec->values[vec->size] = value;
  vec->size++;
}

size_t* vector_data(struct vector_t *vec)
{
  return vec->values;
}
