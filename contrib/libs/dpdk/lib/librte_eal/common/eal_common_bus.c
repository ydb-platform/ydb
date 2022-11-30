#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2016 NXP
 */

#include <stdio.h>
#include <string.h>
#include <sys/queue.h>

#include <rte_bus.h>
#include <rte_debug.h>
#include <rte_string_fns.h>
#include <rte_errno.h>

#include "eal_private.h"

static struct rte_bus_list rte_bus_list =
	TAILQ_HEAD_INITIALIZER(rte_bus_list);

void
rte_bus_register(struct rte_bus *bus)
{
	RTE_VERIFY(bus);
	RTE_VERIFY(bus->name && strlen(bus->name));
	/* A bus should mandatorily have the scan implemented */
	RTE_VERIFY(bus->scan);
	RTE_VERIFY(bus->probe);
	RTE_VERIFY(bus->find_device);
	/* Buses supporting driver plug also require unplug. */
	RTE_VERIFY(!bus->plug || bus->unplug);

	TAILQ_INSERT_TAIL(&rte_bus_list, bus, next);
	RTE_LOG(DEBUG, EAL, "Registered [%s] bus.\n", bus->name);
}

void
rte_bus_unregister(struct rte_bus *bus)
{
	TAILQ_REMOVE(&rte_bus_list, bus, next);
	RTE_LOG(DEBUG, EAL, "Unregistered [%s] bus.\n", bus->name);
}

/* Scan all the buses for registered devices */
int
rte_bus_scan(void)
{
	int ret;
	struct rte_bus *bus = NULL;

	TAILQ_FOREACH(bus, &rte_bus_list, next) {
		ret = bus->scan();
		if (ret)
			RTE_LOG(ERR, EAL, "Scan for (%s) bus failed.\n",
				bus->name);
	}

	return 0;
}

/* Probe all devices of all buses */
int
rte_bus_probe(void)
{
	int ret;
	struct rte_bus *bus, *vbus = NULL;

	TAILQ_FOREACH(bus, &rte_bus_list, next) {
		if (!strcmp(bus->name, "vdev")) {
			vbus = bus;
			continue;
		}

		ret = bus->probe();
		if (ret)
			RTE_LOG(ERR, EAL, "Bus (%s) probe failed.\n",
				bus->name);
	}

	if (vbus) {
		ret = vbus->probe();
		if (ret)
			RTE_LOG(ERR, EAL, "Bus (%s) probe failed.\n",
				vbus->name);
	}

	return 0;
}

/* Dump information of a single bus */
static int
bus_dump_one(FILE *f, struct rte_bus *bus)
{
	int ret;

	/* For now, dump only the bus name */
	ret = fprintf(f, " %s\n", bus->name);

	/* Error in case of inability in writing to stream */
	if (ret < 0)
		return ret;

	return 0;
}

void
rte_bus_dump(FILE *f)
{
	int ret;
	struct rte_bus *bus;

	TAILQ_FOREACH(bus, &rte_bus_list, next) {
		ret = bus_dump_one(f, bus);
		if (ret) {
			RTE_LOG(ERR, EAL, "Unable to write to stream (%d)\n",
				ret);
			break;
		}
	}
}

struct rte_bus *
rte_bus_find(const struct rte_bus *start, rte_bus_cmp_t cmp,
	     const void *data)
{
	struct rte_bus *bus;

	if (start != NULL)
		bus = TAILQ_NEXT(start, next);
	else
		bus = TAILQ_FIRST(&rte_bus_list);
	while (bus != NULL) {
		if (cmp(bus, data) == 0)
			break;
		bus = TAILQ_NEXT(bus, next);
	}
	return bus;
}

static int
cmp_rte_device(const struct rte_device *dev1, const void *_dev2)
{
	const struct rte_device *dev2 = _dev2;

	return dev1 != dev2;
}

static int
bus_find_device(const struct rte_bus *bus, const void *_dev)
{
	struct rte_device *dev;

	dev = bus->find_device(NULL, cmp_rte_device, _dev);
	return dev == NULL;
}

struct rte_bus *
rte_bus_find_by_device(const struct rte_device *dev)
{
	return rte_bus_find(NULL, bus_find_device, (const void *)dev);
}

static int
cmp_bus_name(const struct rte_bus *bus, const void *_name)
{
	const char *name = _name;

	return strcmp(bus->name, name);
}

struct rte_bus *
rte_bus_find_by_name(const char *busname)
{
	return rte_bus_find(NULL, cmp_bus_name, (const void *)busname);
}

static int
bus_can_parse(const struct rte_bus *bus, const void *_name)
{
	const char *name = _name;

	return !(bus->parse && bus->parse(name, NULL) == 0);
}

struct rte_bus *
rte_bus_find_by_device_name(const char *str)
{
	char name[RTE_DEV_NAME_MAX_LEN];
	char *c;

	strlcpy(name, str, sizeof(name));
	c = strchr(name, ',');
	if (c != NULL)
		c[0] = '\0';
	return rte_bus_find(NULL, bus_can_parse, name);
}


/*
 * Get iommu class of devices on the bus.
 */
enum rte_iova_mode
rte_bus_get_iommu_class(void)
{
	enum rte_iova_mode mode = RTE_IOVA_DC;
	bool buses_want_va = false;
	bool buses_want_pa = false;
	struct rte_bus *bus;

	TAILQ_FOREACH(bus, &rte_bus_list, next) {
		enum rte_iova_mode bus_iova_mode;

		if (bus->get_iommu_class == NULL)
			continue;

		bus_iova_mode = bus->get_iommu_class();
		RTE_LOG(DEBUG, EAL, "Bus %s wants IOVA as '%s'\n",
			bus->name,
			bus_iova_mode == RTE_IOVA_DC ? "DC" :
			(bus_iova_mode == RTE_IOVA_PA ? "PA" : "VA"));
		if (bus_iova_mode == RTE_IOVA_PA)
			buses_want_pa = true;
		else if (bus_iova_mode == RTE_IOVA_VA)
			buses_want_va = true;
	}
	if (buses_want_va && !buses_want_pa) {
		mode = RTE_IOVA_VA;
	} else if (buses_want_pa && !buses_want_va) {
		mode = RTE_IOVA_PA;
	} else {
		mode = RTE_IOVA_DC;
		if (buses_want_va) {
			RTE_LOG(WARNING, EAL, "Some buses want 'VA' but forcing 'DC' because other buses want 'PA'.\n");
			RTE_LOG(WARNING, EAL, "Depending on the final decision by the EAL, not all buses may be able to initialize.\n");
		}
	}

	return mode;
}

static int
bus_handle_sigbus(const struct rte_bus *bus,
			const void *failure_addr)
{
	int ret;

	if (!bus->sigbus_handler)
		return -1;

	ret = bus->sigbus_handler(failure_addr);

	/* find bus but handle failed, keep the errno be set. */
	if (ret < 0 && rte_errno == 0)
		rte_errno = ENOTSUP;

	return ret > 0;
}

int
rte_bus_sigbus_handler(const void *failure_addr)
{
	struct rte_bus *bus;

	int ret = 0;
	int old_errno = rte_errno;

	rte_errno = 0;

	bus = rte_bus_find(NULL, bus_handle_sigbus, failure_addr);
	/* can not find bus. */
	if (!bus)
		return 1;
	/* find bus but handle failed, pass on the new errno. */
	else if (rte_errno != 0)
		return -1;

	/* restore the old errno. */
	rte_errno = old_errno;

	return ret;
}
