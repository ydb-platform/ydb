#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

// declare function here, linker will find this when linked to
// libx86simdsortcpp.so
void keyvalue_qsort_float_sizet(float *, size_t *, size_t);
void keyvalue_qsort_float_uint32(float *, uint32_t *, uint32_t);
void keyvalue_qsort_sizet_sizet(size_t *, size_t *, size_t);
void keyvalue_qsort_sizet_uint32(size_t *, uint32_t *, uint32_t);
void keyvalue_qsort_uint32_sizet(uint32_t *, size_t *, size_t);
void keyvalue_qsort_uint32_uint32(uint32_t *, uint32_t *, uint32_t);
void keyvalue_qsort_int32_sizet(int32_t *, size_t *, size_t);
void keyvalue_qsort_int32_uint32(int32_t *, uint32_t *, uint32_t);

// struct definition, we will sort an array of these:
struct Point {
    int x;
    int y;
    float distance;
    size_t metric;
};

#define SWAP(a, b, type) \
    { \
        type temp = a; \
        a = b; \
        b = temp; \
    }

// Function to sort an array of objects:
void object_qsort(struct Point *arr, size_t size)
{
    /* (1) Create and initialize arrays of key and value  */
    size_t *key = malloc(size * sizeof(size_t));
    size_t *arg = malloc(size * sizeof(size_t));
    bool *done = malloc(size * sizeof(bool));
    for (size_t ii = 0; ii < size; ++ii) {
        key[ii] = arr[ii].metric;
        arg[ii] = ii;
        done[ii] = false;
    }

    /* (2) IndexSort using the keyvalue_qsort */
    keyvalue_qsort_sizet_sizet(key, arg, size);

    /* (3) Permute obj array in-place */
    for (size_t ii = 0; ii < size; ++ii) {
        if (done[ii]) { continue; }
        done[ii] = true;
        size_t prev_j = ii;
        size_t jj = arg[ii];
        while (ii != jj) {
            SWAP(arr[prev_j], arr[jj], struct Point);
            done[jj] = true;
            prev_j = jj;
            jj = arg[jj];
        }
    }
    free(key);
    free(arg);
    free(done);
}

int main()
{
    const size_t size = 10;
    struct Point arr[size];

    // Initialize:
    for (size_t ii = 0; ii < size; ++ii) {
        arr[ii].distance = (float)rand() / RAND_MAX;
        arr[ii].metric = rand() % 100;
    }

    // sort:
    object_qsort(arr, size);

    // check if it is sorted:
    printf("arr = ");
    for (size_t ii = 0; ii < size; ++ii) {
        printf("%ld, ", arr[ii].metric);
    }
    printf("\n");
    return 0;
}
