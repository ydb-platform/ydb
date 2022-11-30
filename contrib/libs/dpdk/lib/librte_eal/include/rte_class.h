/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2018 Gaëtan Rivet
 */

#ifndef _RTE_CLASS_H_
#define _RTE_CLASS_H_

/**
 * @file
 *
 * DPDK device class interface.
 *
 * This file describes the interface of the device class
 * abstraction layer.
 *
 * A device class defines the type of function a device
 * will be used for e.g.: Ethernet adapter (eth),
 * cryptographic co-processor (crypto), etc.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/queue.h>

#include <rte_dev.h>

/** Double linked list of classes */
TAILQ_HEAD(rte_class_list, rte_class);

/**
 * A structure describing a generic device class.
 */
struct rte_class {
	TAILQ_ENTRY(rte_class) next; /**< Next device class in linked list */
	const char *name; /**< Name of the class */
	rte_dev_iterate_t dev_iterate; /**< Device iterator. */
};

/**
 * Class comparison function.
 *
 * @param cls
 *	Class under test.
 *
 * @param data
 *	Data to compare against.
 *
 * @return
 *	0 if the class matches the data.
 *	!0 if the class does not match.
 *	<0 if ordering is possible and the class is lower than the data.
 *	>0 if ordering is possible and the class is greater than the data.
 */
typedef int (*rte_class_cmp_t)(const struct rte_class *cls, const void *data);

/**
 * Class iterator to find a particular class.
 *
 * This function compares each registered class to find one that matches
 * the data passed as parameter.
 *
 * If the comparison function returns zero this function will stop iterating
 * over any more classes. To continue a search the class of a previous search
 * can be passed via the start parameter.
 *
 * @param start
 *	Starting point for the iteration.
 *
 * @param cmp
 *	Comparison function.
 *
 * @param data
 *	 Data to pass to comparison function.
 *
 * @return
 *	 A pointer to a rte_class structure or NULL in case no class matches
 */
__rte_experimental
struct rte_class *
rte_class_find(const struct rte_class *start, rte_class_cmp_t cmp,
	       const void *data);

/**
 * Find the registered class for a given name.
 */
__rte_experimental
struct rte_class *
rte_class_find_by_name(const char *name);

/**
 * Register a Class handle.
 *
 * @param cls
 *   A pointer to a rte_class structure describing the class
 *   to be registered.
 */
__rte_experimental
void rte_class_register(struct rte_class *cls);

/**
 * Unregister a Class handle.
 *
 * @param cls
 *   A pointer to a rte_class structure describing the class
 *   to be unregistered.
 */
__rte_experimental
void rte_class_unregister(struct rte_class *cls);

/**
 * Helper for Class registration.
 * The constructor has lower priority than Bus constructors.
 * The constructor has higher priority than PMD constructors.
 */
#define RTE_REGISTER_CLASS(nm, cls) \
RTE_INIT_PRIO(classinitfn_ ##nm, CLASS) \
{\
	(cls).name = RTE_STR(nm); \
	rte_class_register(&cls); \
}

#define RTE_UNREGISTER_CLASS(nm, cls) \
RTE_FINI_PRIO(classfinifn_ ##nm, CLASS) \
{ \
	rte_class_unregister(&cls); \
}

#ifdef __cplusplus
}
#endif

#endif /* _RTE_CLASS_H_ */
