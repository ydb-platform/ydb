/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2014 6WIND S.A.
 */

#ifndef _RTE_DEV_H_
#define _RTE_DEV_H_

/**
 * @file
 *
 * RTE PMD Driver Registration Interface
 *
 * This file manages the list of device drivers.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <sys/queue.h>

#include <rte_config.h>
#include <rte_compat.h>
#include <rte_log.h>

/**
 * The device event type.
 */
enum rte_dev_event_type {
	RTE_DEV_EVENT_ADD,	/**< device being added */
	RTE_DEV_EVENT_REMOVE,	/**< device being removed */
	RTE_DEV_EVENT_MAX	/**< max value of this enum */
};

typedef void (*rte_dev_event_cb_fn)(const char *device_name,
					enum rte_dev_event_type event,
					void *cb_arg);

/* Macros to check for invalid function pointers */
#define RTE_FUNC_PTR_OR_ERR_RET(func, retval) do { \
	if ((func) == NULL) \
		return retval; \
} while (0)

#define RTE_FUNC_PTR_OR_RET(func) do { \
	if ((func) == NULL) \
		return; \
} while (0)

/**
 * Device policies.
 */
enum rte_dev_policy {
	RTE_DEV_ALLOWED,
	RTE_DEV_BLOCKED,
};

/* Backwards compatibility will be removed */
#define RTE_DEV_WHITELISTED \
	RTE_DEPRECATED(RTE_DEV_WHITELISTED) RTE_DEV_ALLOWED
#define RTE_DEV_BLACKLISTED \
	RTE_DEPRECATED(RTE_DEV_BLACKLISTED) RTE_DEV_BLOCKED

/**
 * A generic memory resource representation.
 */
struct rte_mem_resource {
	uint64_t phys_addr; /**< Physical address, 0 if not resource. */
	uint64_t len;       /**< Length of the resource. */
	void *addr;         /**< Virtual address, NULL when not mapped. */
};

/**
 * A structure describing a device driver.
 */
struct rte_driver {
	TAILQ_ENTRY(rte_driver) next;  /**< Next in list. */
	const char *name;                   /**< Driver name. */
	const char *alias;              /**< Driver alias. */
};

/*
 * Internal identifier length
 * Sufficiently large to allow for UUID or PCI address
 */
#define RTE_DEV_NAME_MAX_LEN 64

/**
 * A structure describing a generic device.
 */
struct rte_device {
	TAILQ_ENTRY(rte_device) next; /**< Next device */
	const char *name;             /**< Device name */
	const struct rte_driver *driver; /**< Driver assigned after probing */
	const struct rte_bus *bus;    /**< Bus handle assigned on scan */
	int numa_node;                /**< NUMA node connection */
	struct rte_devargs *devargs;  /**< Arguments for latest probing */
};

/**
 * Query status of a device.
 *
 * @param dev
 *   Generic device pointer.
 * @return
 *   (int)true if already probed successfully, 0 otherwise.
 */
int rte_dev_is_probed(const struct rte_device *dev);

/**
 * Hotplug add a given device to a specific bus.
 *
 * In multi-process, it will request other processes to add the same device.
 * A failure, in any process, will rollback the action
 *
 * @param busname
 *   The bus name the device is added to.
 * @param devname
 *   The device name. Based on this device name, eal will identify a driver
 *   capable of handling it and pass it to the driver probing function.
 * @param drvargs
 *   Device arguments to be passed to the driver.
 * @return
 *   0 on success, negative on error.
 */
int rte_eal_hotplug_add(const char *busname, const char *devname,
			const char *drvargs);

/**
 * Add matching devices.
 *
 * In multi-process, it will request other processes to add the same device.
 * A failure, in any process, will rollback the action
 *
 * @param devargs
 *   Device arguments including bus, class and driver properties.
 * @return
 *   0 on success, negative on error.
 */
int rte_dev_probe(const char *devargs);

/**
 * Hotplug remove a given device from a specific bus.
 *
 * In multi-process, it will request other processes to remove the same device.
 * A failure, in any process, will rollback the action
 *
 * @param busname
 *   The bus name the device is removed from.
 * @param devname
 *   The device name being removed.
 * @return
 *   0 on success, negative on error.
 */
int rte_eal_hotplug_remove(const char *busname, const char *devname);

/**
 * Remove one device.
 *
 * In multi-process, it will request other processes to remove the same device.
 * A failure, in any process, will rollback the action
 *
 * @param dev
 *   Data structure of the device to remove.
 * @return
 *   0 on success, negative on error.
 */
int rte_dev_remove(struct rte_device *dev);

/**
 * Device comparison function.
 *
 * This type of function is used to compare an rte_device with arbitrary
 * data.
 *
 * @param dev
 *   Device handle.
 *
 * @param data
 *   Data to compare against. The type of this parameter is determined by
 *   the kind of comparison performed by the function.
 *
 * @return
 *   0 if the device matches the data.
 *   !0 if the device does not match.
 *   <0 if ordering is possible and the device is lower than the data.
 *   >0 if ordering is possible and the device is greater than the data.
 */
typedef int (*rte_dev_cmp_t)(const struct rte_device *dev, const void *data);

#define RTE_PMD_EXPORT_NAME_ARRAY(n, idx) n##idx[]

#define RTE_PMD_EXPORT_NAME(name, idx) \
static const char RTE_PMD_EXPORT_NAME_ARRAY(this_pmd_name, idx) \
__rte_used = RTE_STR(name)

#define DRV_EXP_TAG(name, tag) __##name##_##tag

#define RTE_PMD_REGISTER_PCI_TABLE(name, table) \
static const char DRV_EXP_TAG(name, pci_tbl_export)[] __rte_used = \
RTE_STR(table)

#define RTE_PMD_REGISTER_PARAM_STRING(name, str) \
static const char DRV_EXP_TAG(name, param_string_export)[] \
__rte_used = str

/**
 * Advertise the list of kernel modules required to run this driver
 *
 * This string lists the kernel modules required for the devices
 * associated to a PMD. The format of each line of the string is:
 * "<device-pattern> <kmod-expression>".
 *
 * The possible formats for the device pattern are:
 *   "*"                     all devices supported by this driver
 *   "pci:*"                 all PCI devices supported by this driver
 *   "pci:v8086:d*:sv*:sd*"  all PCI devices supported by this driver
 *                           whose vendor id is 0x8086.
 *
 * The format of the kernel modules list is a parenthesized expression
 * containing logical-and (&) and logical-or (|).
 *
 * The device pattern and the kmod expression are separated by a space.
 *
 * Example:
 * - "* igb_uio | uio_pci_generic | vfio"
 */
#define RTE_PMD_REGISTER_KMOD_DEP(name, str) \
static const char DRV_EXP_TAG(name, kmod_dep_export)[] \
__rte_used = str

/**
 * Iteration context.
 *
 * This context carries over the current iteration state.
 */
struct rte_dev_iterator {
	const char *dev_str; /**< device string. */
	const char *bus_str; /**< bus-related part of device string. */
	const char *cls_str; /**< class-related part of device string. */
	struct rte_bus *bus; /**< bus handle. */
	struct rte_class *cls; /**< class handle. */
	struct rte_device *device; /**< current position. */
	void *class_device; /**< additional specialized context. */
};

/**
 * Device iteration function.
 *
 * Find the next device matching properties passed in parameters.
 * The function takes an additional ``start`` parameter, that is
 * used as starting context when relevant.
 *
 * The function returns the current element in the iteration.
 * This return value will potentially be used as a start parameter
 * in subsequent calls to the function.
 *
 * The additional iterator parameter is only there if a specific
 * implementation needs additional context. It must not be modified by
 * the iteration function itself.
 *
 * @param start
 *   Starting iteration context.
 *
 * @param devstr
 *   Device description string.
 *
 * @param it
 *   Device iterator.
 *
 * @return
 *   The address of the current element matching the device description
 *   string.
 */
typedef void *(*rte_dev_iterate_t)(const void *start,
				   const char *devstr,
				   const struct rte_dev_iterator *it);

/**
 * Initializes a device iterator.
 *
 * This iterator allows accessing a list of devices matching a criteria.
 * The device matching is made among all buses and classes currently registered,
 * filtered by the device description given as parameter.
 *
 * This function will not allocate any memory. It is safe to stop the
 * iteration at any moment and let the iterator go out of context.
 *
 * @param it
 *   Device iterator handle.
 *
 * @param str
 *   Device description string.
 *
 * @return
 *   0 on successful initialization.
 *   <0 on error.
 */
__rte_experimental
int
rte_dev_iterator_init(struct rte_dev_iterator *it, const char *str);

/**
 * Iterates on a device iterator.
 *
 * Generates a new rte_device handle corresponding to the next element
 * in the list described in comprehension by the iterator.
 *
 * The next object is returned, and the iterator is updated.
 *
 * @param it
 *   Device iterator handle.
 *
 * @return
 *   An rte_device handle if found.
 *   NULL if an error occurred (rte_errno is set).
 *   NULL if no device could be found (rte_errno is not set).
 */
__rte_experimental
struct rte_device *
rte_dev_iterator_next(struct rte_dev_iterator *it);

#define RTE_DEV_FOREACH(dev, devstr, it) \
	for (rte_dev_iterator_init(it, devstr), \
	     dev = rte_dev_iterator_next(it); \
	     dev != NULL; \
	     dev = rte_dev_iterator_next(it))

#ifdef __cplusplus
}
#endif

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * It registers the callback for the specific device.
 * Multiple callbacks can be registered at the same time.
 *
 * @param device_name
 *  The device name, that is the param name of the struct rte_device,
 *  null value means for all devices.
 * @param cb_fn
 *  callback address.
 * @param cb_arg
 *  address of parameter for callback.
 *
 * @return
 *  - On success, zero.
 *  - On failure, a negative value.
 */
__rte_experimental
int
rte_dev_event_callback_register(const char *device_name,
				rte_dev_event_cb_fn cb_fn,
				void *cb_arg);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * It unregisters the callback according to the specified device.
 *
 * @param device_name
 *  The device name, that is the param name of the struct rte_device,
 *  null value means for all devices and their callbacks.
 * @param cb_fn
 *  callback address.
 * @param cb_arg
 *  address of parameter for callback, (void *)-1 means to remove all
 *  registered which has the same callback address.
 *
 * @return
 *  - On success, return the number of callback entities removed.
 *  - On failure, a negative value.
 */
__rte_experimental
int
rte_dev_event_callback_unregister(const char *device_name,
				  rte_dev_event_cb_fn cb_fn,
				  void *cb_arg);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Executes all the user application registered callbacks for
 * the specific device.
 *
 * @param device_name
 *  The device name.
 * @param event
 *  the device event type.
 */
__rte_experimental
void
rte_dev_event_callback_process(const char *device_name,
			       enum rte_dev_event_type event);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Start the device event monitoring.
 *
 * @return
 *   - On success, zero.
 *   - On failure, a negative value.
 */
__rte_experimental
int
rte_dev_event_monitor_start(void);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Stop the device event monitoring.
 *
 * @return
 *   - On success, zero.
 *   - On failure, a negative value.
 */
__rte_experimental
int
rte_dev_event_monitor_stop(void);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Enable hotplug handling for devices.
 *
 * @return
 *   - On success, zero.
 *   - On failure, a negative value.
 */
__rte_experimental
int
rte_dev_hotplug_handle_enable(void);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Disable hotplug handling for devices.
 *
 * @return
 *   - On success, zero.
 *   - On failure, a negative value.
 */
__rte_experimental
int
rte_dev_hotplug_handle_disable(void);

/**
 * Device level DMA map function.
 * After a successful call, the memory segment will be mapped to the
 * given device.
 *
 * @note: Memory must be registered in advance using rte_extmem_* APIs.
 *
 * @param dev
 *	Device pointer.
 * @param addr
 *	Virtual address to map.
 * @param iova
 *	IOVA address to map.
 * @param len
 *	Length of the memory segment being mapped.
 *
 * @return
 *	0 if mapping was successful.
 *	Negative value and rte_errno is set otherwise.
 */
__rte_experimental
int
rte_dev_dma_map(struct rte_device *dev, void *addr, uint64_t iova, size_t len);

/**
 * Device level DMA unmap function.
 * After a successful call, the memory segment will no longer be
 * accessible by the given device.
 *
 * @note: Memory must be registered in advance using rte_extmem_* APIs.
 *
 * @param dev
 *	Device pointer.
 * @param addr
 *	Virtual address to unmap.
 * @param iova
 *	IOVA address to unmap.
 * @param len
 *	Length of the memory segment being mapped.
 *
 * @return
 *	0 if un-mapping was successful.
 *	Negative value and rte_errno is set otherwise.
 */
__rte_experimental
int
rte_dev_dma_unmap(struct rte_device *dev, void *addr, uint64_t iova,
		  size_t len);

#endif /* _RTE_DEV_H_ */
