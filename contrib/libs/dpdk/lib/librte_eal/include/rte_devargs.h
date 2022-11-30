/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2014 6WIND S.A.
 */

#ifndef _RTE_DEVARGS_H_
#define _RTE_DEVARGS_H_

/**
 * @file
 *
 * RTE devargs: list of devices and their user arguments
 *
 * This file stores a list of devices and their arguments given by
 * the user when a DPDK application is started. These devices can be PCI
 * devices or virtual devices. These devices are stored at startup in a
 * list of rte_devargs structures.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <sys/queue.h>
#include <rte_compat.h>
#include <rte_bus.h>

/**
 * Type of generic device
 */
enum rte_devtype {
	RTE_DEVTYPE_ALLOWED,
	RTE_DEVTYPE_BLOCKED,
	RTE_DEVTYPE_VIRTUAL,
};

/* Backwards compatibility will be removed later */
#define RTE_DEVTYPE_WHITELISTED_PCI \
	RTE_DEPRECATED(RTE_DEVTYPE_WHITELISTED_PCI) RTE_DEVTYPE_ALLOWED
#define RTE_DEVTYPE_BLACKLISTED_PCI \
	RTE_DEPRECATED(RTE_DEVTYPE_BLACKLISTED_PCI) RTE_DEVTYPE_BLOCKED

/**
 * Structure that stores a device given by the user with its arguments
 *
 * A user device is a physical or a virtual device given by the user to
 * the DPDK application at startup through command line arguments.
 *
 * The structure stores the configuration of the device, its PCI
 * identifier if it's a PCI device or the driver name if it's a virtual
 * device.
 */
struct rte_devargs {
	/** Next in list. */
	TAILQ_ENTRY(rte_devargs) next;
	/** Type of device. */
	enum rte_devtype type;
	/** Device policy. */
	enum rte_dev_policy policy;
	/** Name of the device. */
	char name[RTE_DEV_NAME_MAX_LEN];
	RTE_STD_C11
	union {
	/** Arguments string as given by user or "" for no argument. */
		char *args;
		const char *drv_str;
	};
	struct rte_bus *bus; /**< bus handle. */
	struct rte_class *cls; /**< class handle. */
	const char *bus_str; /**< bus-related part of device string. */
	const char *cls_str; /**< class-related part of device string. */
	const char *data; /**< Device string storage. */
};

/**
 * Parse a device string.
 *
 * Verify that a bus is capable of handling the device passed
 * in argument. Store which bus will handle the device, its name
 * and the eventual device parameters.
 *
 * The syntax is:
 *
 *     bus:device_identifier,arg1=val1,arg2=val2
 *
 * where "bus:" is the bus name followed by any character separator.
 * The bus name is optional. If no bus name is specified, each bus
 * will attempt to recognize the device identifier. The first one
 * to succeed will be used.
 *
 * Examples:
 *
 *     pci:0000:05.00.0,arg=val
 *     05.00.0,arg=val
 *     vdev:net_ring0
 *
 * @param da
 *   The devargs structure holding the device information.
 *
 * @param dev
 *   String describing a device.
 *
 * @return
 *   - 0 on success.
 *   - Negative errno on error.
 */
int
rte_devargs_parse(struct rte_devargs *da, const char *dev);

/**
 * Parse a device string.
 *
 * Verify that a bus is capable of handling the device passed
 * in argument. Store which bus will handle the device, its name
 * and the eventual device parameters.
 *
 * The device string is built with a printf-like syntax.
 *
 * The syntax is:
 *
 *     bus:device_identifier,arg1=val1,arg2=val2
 *
 * where "bus:" is the bus name followed by any character separator.
 * The bus name is optional. If no bus name is specified, each bus
 * will attempt to recognize the device identifier. The first one
 * to succeed will be used.
 *
 * Examples:
 *
 *     pci:0000:05.00.0,arg=val
 *     05.00.0,arg=val
 *     vdev:net_ring0
 *
 * @param da
 *   The devargs structure holding the device information.
 * @param format
 *   Format string describing a device.
 *
 * @return
 *   - 0 on success.
 *   - Negative errno on error.
 */
int
rte_devargs_parsef(struct rte_devargs *da,
		   const char *format, ...)
__rte_format_printf(2, 0);

/**
 * Insert an rte_devargs in the global list.
 *
 * @param da
 *  The devargs structure to insert.
 *  If a devargs for the same device is already inserted,
 *  it will be updated and returned. It means *da pointer can change.
 *
 * @return
 *   - 0 on success
 *   - Negative on error.
 */
int
rte_devargs_insert(struct rte_devargs **da);

/**
 * Add a device to the user device list
 * See rte_devargs_parse() for details.
 *
 * @param devtype
 *   The type of the device.
 * @param devargs_str
 *   The arguments as given by the user.
 *
 * @return
 *   - 0 on success
 *   - A negative value on error
 */
int rte_devargs_add(enum rte_devtype devtype, const char *devargs_str);

/**
 * Remove a device from the user device list.
 * Its resources are freed.
 * If the devargs cannot be found, nothing happens.
 *
 * @param devargs
 *   The instance or a copy of devargs to remove.
 *
 * @return
 *   0 on success.
 *   <0 on error.
 *   >0 if the devargs was not within the user device list.
 */
int rte_devargs_remove(struct rte_devargs *devargs);

/**
 * Count the number of user devices of a specified type
 *
 * @param devtype
 *   The type of the devices to counted.
 *
 * @return
 *   The number of devices.
 */
unsigned int
rte_devargs_type_count(enum rte_devtype devtype);

/**
 * This function dumps the list of user device and their arguments.
 *
 * @param f
 *   A pointer to a file for output
 */
void rte_devargs_dump(FILE *f);

/**
 * Find next rte_devargs matching the provided bus name.
 *
 * @param busname
 *   Limit the iteration to devargs related to buses
 *   matching this name.
 *   Will return any next rte_devargs if NULL.
 *
 * @param start
 *   Starting iteration point. The iteration will start at
 *   the first rte_devargs if NULL.
 *
 * @return
 *   Next rte_devargs entry matching the requested bus,
 *   NULL if there is none.
 */
struct rte_devargs *
rte_devargs_next(const char *busname, const struct rte_devargs *start);

/**
 * Iterate over all rte_devargs for a specific bus.
 */
#define RTE_EAL_DEVARGS_FOREACH(busname, da) \
	for (da = rte_devargs_next(busname, NULL); \
	     da != NULL; \
	     da = rte_devargs_next(busname, da)) \

#ifdef __cplusplus
}
#endif

#endif /* _RTE_DEVARGS_H_ */
