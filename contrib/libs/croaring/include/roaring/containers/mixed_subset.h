/*
 * mixed_subset.h
 *
 */

#ifndef CONTAINERS_MIXED_SUBSET_H_
#define CONTAINERS_MIXED_SUBSET_H_

#include <roaring/containers/array.h>
#include <roaring/containers/bitset.h>
#include <roaring/containers/run.h>

#ifdef __cplusplus
extern "C" {
namespace roaring {
namespace internal {
#endif

/**
 * Return true if container1 is a subset of container2.
 */
bool array_container_is_subset_bitset(const array_container_t* container1,
                                      const bitset_container_t* container2);

/**
 * Return true if container1 is a subset of container2.
 */
bool run_container_is_subset_array(const run_container_t* container1,
                                   const array_container_t* container2);

/**
 * Return true if container1 is a subset of container2.
 */
bool array_container_is_subset_run(const array_container_t* container1,
                                   const run_container_t* container2);

/**
 * Return true if container1 is a subset of container2.
 */
bool run_container_is_subset_bitset(const run_container_t* container1,
                                    const bitset_container_t* container2);

/**
 * Return true if container1 is a subset of container2.
 */
bool bitset_container_is_subset_run(const bitset_container_t* container1,
                                    const run_container_t* container2);

#ifdef __cplusplus
}
}
}  // extern "C" { namespace roaring { namespace internal {
#endif

#endif /* CONTAINERS_MIXED_SUBSET_H_ */
