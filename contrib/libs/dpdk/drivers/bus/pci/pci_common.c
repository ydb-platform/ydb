#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation.
 * Copyright 2013-2014 6WIND S.A.
 */

#include <string.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/queue.h>
#include <rte_errno.h>
#include <rte_interrupts.h>
#include <rte_log.h>
#include <rte_bus.h>
#include <rte_pci.h>
#include <rte_bus_pci.h>
#include <rte_per_lcore.h>
#include <rte_memory.h>
#include <rte_eal.h>
#include <rte_eal_paging.h>
#include <rte_string_fns.h>
#include <rte_common.h>
#include <rte_devargs.h>
#include <rte_vfio.h>

#include "private.h"


#define SYSFS_PCI_DEVICES "/sys/bus/pci/devices"

const char *rte_pci_get_sysfs_path(void)
{
	const char *path = NULL;

#ifdef RTE_EXEC_ENV_LINUX
	path = getenv("SYSFS_PCI_DEVICES");
	if (path == NULL)
		return SYSFS_PCI_DEVICES;
#endif

	return path;
}

static struct rte_devargs *
pci_devargs_lookup(const struct rte_pci_addr *pci_addr)
{
	struct rte_devargs *devargs;
	struct rte_pci_addr addr;

	RTE_EAL_DEVARGS_FOREACH("pci", devargs) {
		devargs->bus->parse(devargs->name, &addr);
		if (!rte_pci_addr_cmp(pci_addr, &addr))
			return devargs;
	}
	return NULL;
}

void
pci_name_set(struct rte_pci_device *dev)
{
	struct rte_devargs *devargs;

	/* Each device has its internal, canonical name set. */
	rte_pci_device_name(&dev->addr,
			dev->name, sizeof(dev->name));
	devargs = pci_devargs_lookup(&dev->addr);
	dev->device.devargs = devargs;

	/* When using a blocklist, only blocked devices will have
	 * an rte_devargs. Allowed devices won't have one.
	 */
	if (devargs != NULL)
		/* If an rte_devargs exists, the generic rte_device uses the
		 * given name as its name.
		 */
		dev->device.name = dev->device.devargs->name;
	else
		/* Otherwise, it uses the internal, canonical form. */
		dev->device.name = dev->name;
}

/* map a particular resource from a file */
void *
pci_map_resource(void *requested_addr, int fd, off_t offset, size_t size,
		 int additional_flags)
{
	void *mapaddr;

	/* Map the PCI memory resource of device */
	mapaddr = rte_mem_map(requested_addr, size,
		RTE_PROT_READ | RTE_PROT_WRITE,
		RTE_MAP_SHARED | additional_flags, fd, offset);
	if (mapaddr == NULL) {
		RTE_LOG(ERR, EAL,
			"%s(): cannot map resource(%d, %p, 0x%zx, 0x%llx): %s (%p)\n",
			__func__, fd, requested_addr, size,
			(unsigned long long)offset,
			rte_strerror(rte_errno), mapaddr);
	} else
		RTE_LOG(DEBUG, EAL, "  PCI memory mapped at %p\n", mapaddr);

	return mapaddr;
}

/* unmap a particular resource */
void
pci_unmap_resource(void *requested_addr, size_t size)
{
	if (requested_addr == NULL)
		return;

	/* Unmap the PCI memory resource of device */
	if (rte_mem_unmap(requested_addr, size)) {
		RTE_LOG(ERR, EAL, "%s(): cannot mem unmap(%p, %#zx): %s\n",
			__func__, requested_addr, size,
			rte_strerror(rte_errno));
	} else
		RTE_LOG(DEBUG, EAL, "  PCI memory unmapped at %p\n",
				requested_addr);
}
/*
 * Match the PCI Driver and Device using the ID Table
 */
int
rte_pci_match(const struct rte_pci_driver *pci_drv,
	      const struct rte_pci_device *pci_dev)
{
	const struct rte_pci_id *id_table;

	for (id_table = pci_drv->id_table; id_table->vendor_id != 0;
	     id_table++) {
		/* check if device's identifiers match the driver's ones */
		if (id_table->vendor_id != pci_dev->id.vendor_id &&
				id_table->vendor_id != PCI_ANY_ID)
			continue;
		if (id_table->device_id != pci_dev->id.device_id &&
				id_table->device_id != PCI_ANY_ID)
			continue;
		if (id_table->subsystem_vendor_id !=
		    pci_dev->id.subsystem_vendor_id &&
		    id_table->subsystem_vendor_id != PCI_ANY_ID)
			continue;
		if (id_table->subsystem_device_id !=
		    pci_dev->id.subsystem_device_id &&
		    id_table->subsystem_device_id != PCI_ANY_ID)
			continue;
		if (id_table->class_id != pci_dev->id.class_id &&
				id_table->class_id != RTE_CLASS_ANY_ID)
			continue;

		return 1;
	}

	return 0;
}

/*
 * If vendor/device ID match, call the probe() function of the
 * driver.
 */
static int
rte_pci_probe_one_driver(struct rte_pci_driver *dr,
			 struct rte_pci_device *dev)
{
	int ret;
	bool already_probed;
	struct rte_pci_addr *loc;

	if ((dr == NULL) || (dev == NULL))
		return -EINVAL;

	loc = &dev->addr;

	/* The device is not blocked; Check if driver supports it */
	if (!rte_pci_match(dr, dev))
		/* Match of device and driver failed */
		return 1;

	RTE_LOG(DEBUG, EAL, "PCI device "PCI_PRI_FMT" on NUMA socket %i\n",
			loc->domain, loc->bus, loc->devid, loc->function,
			dev->device.numa_node);

	/* no initialization when marked as blocked, return without error */
	if (dev->device.devargs != NULL &&
		dev->device.devargs->policy == RTE_DEV_BLOCKED) {
		RTE_LOG(INFO, EAL, "  Device is blocked, not initializing\n");
		return 1;
	}

	if (dev->device.numa_node < 0) {
		RTE_LOG(WARNING, EAL, "  Invalid NUMA socket, default to 0\n");
		dev->device.numa_node = 0;
	}

	already_probed = rte_dev_is_probed(&dev->device);
	if (already_probed && !(dr->drv_flags & RTE_PCI_DRV_PROBE_AGAIN)) {
		RTE_LOG(DEBUG, EAL, "Device %s is already probed\n",
				dev->device.name);
		return -EEXIST;
	}

	RTE_LOG(DEBUG, EAL, "  probe driver: %x:%x %s\n", dev->id.vendor_id,
		dev->id.device_id, dr->driver.name);

	/*
	 * reference driver structure
	 * This needs to be before rte_pci_map_device(), as it enables to use
	 * driver flags for adjusting configuration.
	 */
	if (!already_probed) {
		enum rte_iova_mode dev_iova_mode;
		enum rte_iova_mode iova_mode;

		dev_iova_mode = pci_device_iova_mode(dr, dev);
		iova_mode = rte_eal_iova_mode();
		if (dev_iova_mode != RTE_IOVA_DC &&
		    dev_iova_mode != iova_mode) {
			RTE_LOG(ERR, EAL, "  Expecting '%s' IOVA mode but current mode is '%s', not initializing\n",
				dev_iova_mode == RTE_IOVA_PA ? "PA" : "VA",
				iova_mode == RTE_IOVA_PA ? "PA" : "VA");
			return -EINVAL;
		}

		dev->driver = dr;
	}

	if (!already_probed && (dr->drv_flags & RTE_PCI_DRV_NEED_MAPPING)) {
		/* map resources for devices that use igb_uio */
		ret = rte_pci_map_device(dev);
		if (ret != 0) {
			dev->driver = NULL;
			return ret;
		}
	}

	RTE_LOG(INFO, EAL, "Probe PCI driver: %s (%x:%x) device: "PCI_PRI_FMT" (socket %i)\n",
			dr->driver.name, dev->id.vendor_id, dev->id.device_id,
			loc->domain, loc->bus, loc->devid, loc->function,
			dev->device.numa_node);
	/* call the driver probe() function */
	ret = dr->probe(dr, dev);
	if (already_probed)
		return ret; /* no rollback if already succeeded earlier */
	if (ret) {
		dev->driver = NULL;
		if ((dr->drv_flags & RTE_PCI_DRV_NEED_MAPPING) &&
			/* Don't unmap if device is unsupported and
			 * driver needs mapped resources.
			 */
			!(ret > 0 &&
				(dr->drv_flags & RTE_PCI_DRV_KEEP_MAPPED_RES)))
			rte_pci_unmap_device(dev);
	} else {
		dev->device.driver = &dr->driver;
	}

	return ret;
}

/*
 * If vendor/device ID match, call the remove() function of the
 * driver.
 */
static int
rte_pci_detach_dev(struct rte_pci_device *dev)
{
	struct rte_pci_addr *loc;
	struct rte_pci_driver *dr;
	int ret = 0;

	if (dev == NULL)
		return -EINVAL;

	dr = dev->driver;
	loc = &dev->addr;

	RTE_LOG(DEBUG, EAL, "PCI device "PCI_PRI_FMT" on NUMA socket %i\n",
			loc->domain, loc->bus, loc->devid,
			loc->function, dev->device.numa_node);

	RTE_LOG(DEBUG, EAL, "  remove driver: %x:%x %s\n", dev->id.vendor_id,
			dev->id.device_id, dr->driver.name);

	if (dr->remove) {
		ret = dr->remove(dev);
		if (ret < 0)
			return ret;
	}

	/* clear driver structure */
	dev->driver = NULL;
	dev->device.driver = NULL;

	if (dr->drv_flags & RTE_PCI_DRV_NEED_MAPPING)
		/* unmap resources for devices that use igb_uio */
		rte_pci_unmap_device(dev);

	return 0;
}

/*
 * If vendor/device ID match, call the probe() function of all
 * registered driver for the given device. Return < 0 if initialization
 * failed, return 1 if no driver is found for this device.
 */
static int
pci_probe_all_drivers(struct rte_pci_device *dev)
{
	struct rte_pci_driver *dr = NULL;
	int rc = 0;

	if (dev == NULL)
		return -EINVAL;

	FOREACH_DRIVER_ON_PCIBUS(dr) {
		rc = rte_pci_probe_one_driver(dr, dev);
		if (rc < 0)
			/* negative value is an error */
			return rc;
		if (rc > 0)
			/* positive value means driver doesn't support it */
			continue;
		return 0;
	}
	return 1;
}

/*
 * Scan the content of the PCI bus, and call the probe() function for
 * all registered drivers that have a matching entry in its id_table
 * for discovered devices.
 */
static int
pci_probe(void)
{
	struct rte_pci_device *dev = NULL;
	size_t probed = 0, failed = 0;
	int ret = 0;

	FOREACH_DEVICE_ON_PCIBUS(dev) {
		probed++;

		ret = pci_probe_all_drivers(dev);
		if (ret < 0) {
			if (ret != -EEXIST) {
				RTE_LOG(ERR, EAL, "Requested device "
					PCI_PRI_FMT " cannot be used\n",
					dev->addr.domain, dev->addr.bus,
					dev->addr.devid, dev->addr.function);
				rte_errno = errno;
				failed++;
			}
			ret = 0;
		}
	}

	return (probed && probed == failed) ? -1 : 0;
}

/* dump one device */
static int
pci_dump_one_device(FILE *f, struct rte_pci_device *dev)
{
	int i;

	fprintf(f, PCI_PRI_FMT, dev->addr.domain, dev->addr.bus,
	       dev->addr.devid, dev->addr.function);
	fprintf(f, " - vendor:%x device:%x\n", dev->id.vendor_id,
	       dev->id.device_id);

	for (i = 0; i != sizeof(dev->mem_resource) /
		sizeof(dev->mem_resource[0]); i++) {
		fprintf(f, "   %16.16"PRIx64" %16.16"PRIx64"\n",
			dev->mem_resource[i].phys_addr,
			dev->mem_resource[i].len);
	}
	return 0;
}

/* dump devices on the bus */
void
rte_pci_dump(FILE *f)
{
	struct rte_pci_device *dev = NULL;

	FOREACH_DEVICE_ON_PCIBUS(dev) {
		pci_dump_one_device(f, dev);
	}
}

static int
pci_parse(const char *name, void *addr)
{
	struct rte_pci_addr *out = addr;
	struct rte_pci_addr pci_addr;
	bool parse;

	parse = (rte_pci_addr_parse(name, &pci_addr) == 0);
	if (parse && addr != NULL)
		*out = pci_addr;
	return parse == false;
}

/* register a driver */
void
rte_pci_register(struct rte_pci_driver *driver)
{
	TAILQ_INSERT_TAIL(&rte_pci_bus.driver_list, driver, next);
	driver->bus = &rte_pci_bus;
}

/* unregister a driver */
void
rte_pci_unregister(struct rte_pci_driver *driver)
{
	TAILQ_REMOVE(&rte_pci_bus.driver_list, driver, next);
	driver->bus = NULL;
}

/* Add a device to PCI bus */
void
rte_pci_add_device(struct rte_pci_device *pci_dev)
{
	TAILQ_INSERT_TAIL(&rte_pci_bus.device_list, pci_dev, next);
}

/* Insert a device into a predefined position in PCI bus */
void
rte_pci_insert_device(struct rte_pci_device *exist_pci_dev,
		      struct rte_pci_device *new_pci_dev)
{
	TAILQ_INSERT_BEFORE(exist_pci_dev, new_pci_dev, next);
}

/* Remove a device from PCI bus */
static void
rte_pci_remove_device(struct rte_pci_device *pci_dev)
{
	TAILQ_REMOVE(&rte_pci_bus.device_list, pci_dev, next);
}

static struct rte_device *
pci_find_device(const struct rte_device *start, rte_dev_cmp_t cmp,
		const void *data)
{
	const struct rte_pci_device *pstart;
	struct rte_pci_device *pdev;

	if (start != NULL) {
		pstart = RTE_DEV_TO_PCI_CONST(start);
		pdev = TAILQ_NEXT(pstart, next);
	} else {
		pdev = TAILQ_FIRST(&rte_pci_bus.device_list);
	}
	while (pdev != NULL) {
		if (cmp(&pdev->device, data) == 0)
			return &pdev->device;
		pdev = TAILQ_NEXT(pdev, next);
	}
	return NULL;
}

/*
 * find the device which encounter the failure, by iterate over all device on
 * PCI bus to check if the memory failure address is located in the range
 * of the BARs of the device.
 */
static struct rte_pci_device *
pci_find_device_by_addr(const void *failure_addr)
{
	struct rte_pci_device *pdev = NULL;
	uint64_t check_point, start, end, len;
	int i;

	check_point = (uint64_t)(uintptr_t)failure_addr;

	FOREACH_DEVICE_ON_PCIBUS(pdev) {
		for (i = 0; i != RTE_DIM(pdev->mem_resource); i++) {
			start = (uint64_t)(uintptr_t)pdev->mem_resource[i].addr;
			len = pdev->mem_resource[i].len;
			end = start + len;
			if (check_point >= start && check_point < end) {
				RTE_LOG(DEBUG, EAL, "Failure address %16.16"
					PRIx64" belongs to device %s!\n",
					check_point, pdev->device.name);
				return pdev;
			}
		}
	}
	return NULL;
}

static int
pci_hot_unplug_handler(struct rte_device *dev)
{
	struct rte_pci_device *pdev = NULL;
	int ret = 0;

	pdev = RTE_DEV_TO_PCI(dev);
	if (!pdev)
		return -1;

	switch (pdev->kdrv) {
#ifdef HAVE_VFIO_DEV_REQ_INTERFACE
	case RTE_PCI_KDRV_VFIO:
		/*
		 * vfio kernel module guaranty the pci device would not be
		 * deleted until the user space release the resource, so no
		 * need to remap BARs resource here, just directly notify
		 * the req event to the user space to handle it.
		 */
		rte_dev_event_callback_process(dev->name,
					       RTE_DEV_EVENT_REMOVE);
		break;
#endif
	case RTE_PCI_KDRV_IGB_UIO:
	case RTE_PCI_KDRV_UIO_GENERIC:
	case RTE_PCI_KDRV_NIC_UIO:
		/* BARs resource is invalid, remap it to be safe. */
		ret = pci_uio_remap_resource(pdev);
		break;
	default:
		RTE_LOG(DEBUG, EAL,
			"Not managed by a supported kernel driver, skipped\n");
		ret = -1;
		break;
	}

	return ret;
}

static int
pci_sigbus_handler(const void *failure_addr)
{
	struct rte_pci_device *pdev = NULL;
	int ret = 0;

	pdev = pci_find_device_by_addr(failure_addr);
	if (!pdev) {
		/* It is a generic sigbus error, no bus would handle it. */
		ret = 1;
	} else {
		/* The sigbus error is caused of hot-unplug. */
		ret = pci_hot_unplug_handler(&pdev->device);
		if (ret) {
			RTE_LOG(ERR, EAL,
				"Failed to handle hot-unplug for device %s",
				pdev->name);
			ret = -1;
		}
	}
	return ret;
}

static int
pci_plug(struct rte_device *dev)
{
	return pci_probe_all_drivers(RTE_DEV_TO_PCI(dev));
}

static int
pci_unplug(struct rte_device *dev)
{
	struct rte_pci_device *pdev;
	int ret;

	pdev = RTE_DEV_TO_PCI(dev);
	ret = rte_pci_detach_dev(pdev);
	if (ret == 0) {
		rte_pci_remove_device(pdev);
		rte_devargs_remove(dev->devargs);
		free(pdev);
	}
	return ret;
}

static int
pci_dma_map(struct rte_device *dev, void *addr, uint64_t iova, size_t len)
{
	struct rte_pci_device *pdev = RTE_DEV_TO_PCI(dev);

	if (!pdev || !pdev->driver) {
		rte_errno = EINVAL;
		return -1;
	}
	if (pdev->driver->dma_map)
		return pdev->driver->dma_map(pdev, addr, iova, len);
	/**
	 *  In case driver don't provides any specific mapping
	 *  try fallback to VFIO.
	 */
	if (pdev->kdrv == RTE_PCI_KDRV_VFIO)
		return rte_vfio_container_dma_map
				(RTE_VFIO_DEFAULT_CONTAINER_FD, (uintptr_t)addr,
				 iova, len);
	rte_errno = ENOTSUP;
	return -1;
}

static int
pci_dma_unmap(struct rte_device *dev, void *addr, uint64_t iova, size_t len)
{
	struct rte_pci_device *pdev = RTE_DEV_TO_PCI(dev);

	if (!pdev || !pdev->driver) {
		rte_errno = EINVAL;
		return -1;
	}
	if (pdev->driver->dma_unmap)
		return pdev->driver->dma_unmap(pdev, addr, iova, len);
	/**
	 *  In case driver don't provides any specific mapping
	 *  try fallback to VFIO.
	 */
	if (pdev->kdrv == RTE_PCI_KDRV_VFIO)
		return rte_vfio_container_dma_unmap
				(RTE_VFIO_DEFAULT_CONTAINER_FD, (uintptr_t)addr,
				 iova, len);
	rte_errno = ENOTSUP;
	return -1;
}

bool
rte_pci_ignore_device(const struct rte_pci_addr *pci_addr)
{
	struct rte_devargs *devargs = pci_devargs_lookup(pci_addr);

	switch (rte_pci_bus.bus.conf.scan_mode) {
	case RTE_BUS_SCAN_ALLOWLIST:
		if (devargs && devargs->policy == RTE_DEV_ALLOWED)
			return false;
		break;
	case RTE_BUS_SCAN_UNDEFINED:
	case RTE_BUS_SCAN_BLOCKLIST:
		if (devargs == NULL || devargs->policy != RTE_DEV_BLOCKED)
			return false;
		break;
	}
	return true;
}

enum rte_iova_mode
rte_pci_get_iommu_class(void)
{
	enum rte_iova_mode iova_mode = RTE_IOVA_DC;
	const struct rte_pci_device *dev;
	const struct rte_pci_driver *drv;
	bool devices_want_va = false;
	bool devices_want_pa = false;
	int iommu_no_va = -1;

	FOREACH_DEVICE_ON_PCIBUS(dev) {
		/*
		 * We can check this only once, because the IOMMU hardware is
		 * the same for all of them.
		 */
		if (iommu_no_va == -1)
			iommu_no_va = pci_device_iommu_support_va(dev)
					? 0 : 1;

		if (dev->kdrv == RTE_PCI_KDRV_UNKNOWN ||
		    dev->kdrv == RTE_PCI_KDRV_NONE)
			continue;
		FOREACH_DRIVER_ON_PCIBUS(drv) {
			enum rte_iova_mode dev_iova_mode;

			if (!rte_pci_match(drv, dev))
				continue;

			dev_iova_mode = pci_device_iova_mode(drv, dev);
			RTE_LOG(DEBUG, EAL, "PCI driver %s for device "
				PCI_PRI_FMT " wants IOVA as '%s'\n",
				drv->driver.name,
				dev->addr.domain, dev->addr.bus,
				dev->addr.devid, dev->addr.function,
				dev_iova_mode == RTE_IOVA_DC ? "DC" :
				(dev_iova_mode == RTE_IOVA_PA ? "PA" : "VA"));
			if (dev_iova_mode == RTE_IOVA_PA)
				devices_want_pa = true;
			else if (dev_iova_mode == RTE_IOVA_VA)
				devices_want_va = true;
		}
	}
	if (iommu_no_va == 1) {
		iova_mode = RTE_IOVA_PA;
		if (devices_want_va) {
			RTE_LOG(WARNING, EAL, "Some devices want 'VA' but IOMMU does not support 'VA'.\n");
			RTE_LOG(WARNING, EAL, "The devices that want 'VA' won't initialize.\n");
		}
	} else if (devices_want_va && !devices_want_pa) {
		iova_mode = RTE_IOVA_VA;
	} else if (devices_want_pa && !devices_want_va) {
		iova_mode = RTE_IOVA_PA;
	} else {
		iova_mode = RTE_IOVA_DC;
		if (devices_want_va) {
			RTE_LOG(WARNING, EAL, "Some devices want 'VA' but forcing 'DC' because other devices want 'PA'.\n");
			RTE_LOG(WARNING, EAL, "Depending on the final decision by the EAL, not all devices may be able to initialize.\n");
		}
	}
	return iova_mode;
}

off_t
rte_pci_find_ext_capability(struct rte_pci_device *dev, uint32_t cap)
{
	off_t offset = RTE_PCI_CFG_SPACE_SIZE;
	uint32_t header;
	int ttl;

	/* minimum 8 bytes per capability */
	ttl = (RTE_PCI_CFG_SPACE_EXP_SIZE - RTE_PCI_CFG_SPACE_SIZE) / 8;

	if (rte_pci_read_config(dev, &header, 4, offset) < 0) {
		RTE_LOG(ERR, EAL, "error in reading extended capabilities\n");
		return -1;
	}

	/*
	 * If we have no capabilities, this is indicated by cap ID,
	 * cap version and next pointer all being 0.
	 */
	if (header == 0)
		return 0;

	while (ttl != 0) {
		if (RTE_PCI_EXT_CAP_ID(header) == cap)
			return offset;

		offset = RTE_PCI_EXT_CAP_NEXT(header);

		if (offset < RTE_PCI_CFG_SPACE_SIZE)
			break;

		if (rte_pci_read_config(dev, &header, 4, offset) < 0) {
			RTE_LOG(ERR, EAL,
				"error in reading extended capabilities\n");
			return -1;
		}

		ttl--;
	}

	return 0;
}

struct rte_pci_bus rte_pci_bus = {
	.bus = {
		.scan = rte_pci_scan,
		.probe = pci_probe,
		.find_device = pci_find_device,
		.plug = pci_plug,
		.unplug = pci_unplug,
		.parse = pci_parse,
		.dma_map = pci_dma_map,
		.dma_unmap = pci_dma_unmap,
		.get_iommu_class = rte_pci_get_iommu_class,
		.dev_iterate = rte_pci_dev_iterate,
		.hot_unplug_handler = pci_hot_unplug_handler,
		.sigbus_handler = pci_sigbus_handler,
	},
	.device_list = TAILQ_HEAD_INITIALIZER(rte_pci_bus.device_list),
	.driver_list = TAILQ_HEAD_INITIALIZER(rte_pci_bus.driver_list),
};

RTE_REGISTER_BUS(pci, rte_pci_bus.bus);
