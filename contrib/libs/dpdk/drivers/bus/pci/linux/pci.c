#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <string.h>
#include <dirent.h>

#include <rte_log.h>
#include <rte_bus.h>
#include <rte_pci.h>
#include <rte_bus_pci.h>
#include <rte_malloc.h>
#include <rte_devargs.h>
#include <rte_memcpy.h>
#include <rte_vfio.h>

#include "eal_filesystem.h"

#include "private.h"
#include "pci_init.h"

/**
 * @file
 * PCI probing under linux
 *
 * This code is used to simulate a PCI probe by parsing information in sysfs.
 * When a registered device matches a driver, it is then initialized with
 * IGB_UIO driver (or doesn't initialize, if the device wasn't bound to it).
 */

extern struct rte_pci_bus rte_pci_bus;

static int
pci_get_kernel_driver_by_path(const char *filename, char *dri_name,
			      size_t len)
{
	int count;
	char path[PATH_MAX];
	char *name;

	if (!filename || !dri_name)
		return -1;

	count = readlink(filename, path, PATH_MAX);
	if (count >= PATH_MAX)
		return -1;

	/* For device does not have a driver */
	if (count < 0)
		return 1;

	path[count] = '\0';

	name = strrchr(path, '/');
	if (name) {
		strlcpy(dri_name, name + 1, len);
		return 0;
	}

	return -1;
}

/* Map pci device */
int
rte_pci_map_device(struct rte_pci_device *dev)
{
	int ret = -1;

	/* try mapping the NIC resources using VFIO if it exists */
	switch (dev->kdrv) {
	case RTE_PCI_KDRV_VFIO:
#ifdef VFIO_PRESENT
		if (pci_vfio_is_enabled())
			ret = pci_vfio_map_resource(dev);
#endif
		break;
	case RTE_PCI_KDRV_IGB_UIO:
	case RTE_PCI_KDRV_UIO_GENERIC:
		if (rte_eal_using_phys_addrs()) {
			/* map resources for devices that use uio */
			ret = pci_uio_map_resource(dev);
		}
		break;
	default:
		RTE_LOG(DEBUG, EAL,
			"  Not managed by a supported kernel driver, skipped\n");
		ret = 1;
		break;
	}

	return ret;
}

/* Unmap pci device */
void
rte_pci_unmap_device(struct rte_pci_device *dev)
{
	/* try unmapping the NIC resources using VFIO if it exists */
	switch (dev->kdrv) {
	case RTE_PCI_KDRV_VFIO:
#ifdef VFIO_PRESENT
		if (pci_vfio_is_enabled())
			pci_vfio_unmap_resource(dev);
#endif
		break;
	case RTE_PCI_KDRV_IGB_UIO:
	case RTE_PCI_KDRV_UIO_GENERIC:
		/* unmap resources for devices that use uio */
		pci_uio_unmap_resource(dev);
		break;
	default:
		RTE_LOG(DEBUG, EAL,
			"  Not managed by a supported kernel driver, skipped\n");
		break;
	}
}

static int
find_max_end_va(const struct rte_memseg_list *msl, void *arg)
{
	size_t sz = msl->len;
	void *end_va = RTE_PTR_ADD(msl->base_va, sz);
	void **max_va = arg;

	if (*max_va < end_va)
		*max_va = end_va;
	return 0;
}

void *
pci_find_max_end_va(void)
{
	void *va = NULL;

	rte_memseg_list_walk(find_max_end_va, &va);
	return va;
}


/* parse one line of the "resource" sysfs file (note that the 'line'
 * string is modified)
 */
int
pci_parse_one_sysfs_resource(char *line, size_t len, uint64_t *phys_addr,
	uint64_t *end_addr, uint64_t *flags)
{
	union pci_resource_info {
		struct {
			char *phys_addr;
			char *end_addr;
			char *flags;
		};
		char *ptrs[PCI_RESOURCE_FMT_NVAL];
	} res_info;

	if (rte_strsplit(line, len, res_info.ptrs, 3, ' ') != 3) {
		RTE_LOG(ERR, EAL,
			"%s(): bad resource format\n", __func__);
		return -1;
	}
	errno = 0;
	*phys_addr = strtoull(res_info.phys_addr, NULL, 16);
	*end_addr = strtoull(res_info.end_addr, NULL, 16);
	*flags = strtoull(res_info.flags, NULL, 16);
	if (errno != 0) {
		RTE_LOG(ERR, EAL,
			"%s(): bad resource format\n", __func__);
		return -1;
	}

	return 0;
}

/* parse the "resource" sysfs file */
static int
pci_parse_sysfs_resource(const char *filename, struct rte_pci_device *dev)
{
	FILE *f;
	char buf[BUFSIZ];
	int i;
	uint64_t phys_addr, end_addr, flags;

	f = fopen(filename, "r");
	if (f == NULL) {
		RTE_LOG(ERR, EAL, "Cannot open sysfs resource\n");
		return -1;
	}

	for (i = 0; i<PCI_MAX_RESOURCE; i++) {

		if (fgets(buf, sizeof(buf), f) == NULL) {
			RTE_LOG(ERR, EAL,
				"%s(): cannot read resource\n", __func__);
			goto error;
		}
		if (pci_parse_one_sysfs_resource(buf, sizeof(buf), &phys_addr,
				&end_addr, &flags) < 0)
			goto error;

		if (flags & IORESOURCE_MEM) {
			dev->mem_resource[i].phys_addr = phys_addr;
			dev->mem_resource[i].len = end_addr - phys_addr + 1;
			/* not mapped for now */
			dev->mem_resource[i].addr = NULL;
		}
	}
	fclose(f);
	return 0;

error:
	fclose(f);
	return -1;
}

/* Scan one pci sysfs entry, and fill the devices list from it. */
static int
pci_scan_one(const char *dirname, const struct rte_pci_addr *addr)
{
	char filename[PATH_MAX];
	unsigned long tmp;
	struct rte_pci_device *dev;
	char driver[PATH_MAX];
	int ret;

	dev = malloc(sizeof(*dev));
	if (dev == NULL)
		return -1;

	memset(dev, 0, sizeof(*dev));
	dev->device.bus = &rte_pci_bus.bus;
	dev->addr = *addr;

	/* get vendor id */
	snprintf(filename, sizeof(filename), "%s/vendor", dirname);
	if (eal_parse_sysfs_value(filename, &tmp) < 0) {
		free(dev);
		return -1;
	}
	dev->id.vendor_id = (uint16_t)tmp;

	/* get device id */
	snprintf(filename, sizeof(filename), "%s/device", dirname);
	if (eal_parse_sysfs_value(filename, &tmp) < 0) {
		free(dev);
		return -1;
	}
	dev->id.device_id = (uint16_t)tmp;

	/* get subsystem_vendor id */
	snprintf(filename, sizeof(filename), "%s/subsystem_vendor",
		 dirname);
	if (eal_parse_sysfs_value(filename, &tmp) < 0) {
		free(dev);
		return -1;
	}
	dev->id.subsystem_vendor_id = (uint16_t)tmp;

	/* get subsystem_device id */
	snprintf(filename, sizeof(filename), "%s/subsystem_device",
		 dirname);
	if (eal_parse_sysfs_value(filename, &tmp) < 0) {
		free(dev);
		return -1;
	}
	dev->id.subsystem_device_id = (uint16_t)tmp;

	/* get class_id */
	snprintf(filename, sizeof(filename), "%s/class",
		 dirname);
	if (eal_parse_sysfs_value(filename, &tmp) < 0) {
		free(dev);
		return -1;
	}
	/* the least 24 bits are valid: class, subclass, program interface */
	dev->id.class_id = (uint32_t)tmp & RTE_CLASS_ANY_ID;

	/* get max_vfs */
	dev->max_vfs = 0;
	snprintf(filename, sizeof(filename), "%s/max_vfs", dirname);
	if (!access(filename, F_OK) &&
	    eal_parse_sysfs_value(filename, &tmp) == 0)
		dev->max_vfs = (uint16_t)tmp;
	else {
		/* for non igb_uio driver, need kernel version >= 3.8 */
		snprintf(filename, sizeof(filename),
			 "%s/sriov_numvfs", dirname);
		if (!access(filename, F_OK) &&
		    eal_parse_sysfs_value(filename, &tmp) == 0)
			dev->max_vfs = (uint16_t)tmp;
	}

	/* get numa node, default to 0 if not present */
	snprintf(filename, sizeof(filename), "%s/numa_node",
		 dirname);

	if (access(filename, F_OK) != -1) {
		if (eal_parse_sysfs_value(filename, &tmp) == 0)
			dev->device.numa_node = tmp;
		else
			dev->device.numa_node = -1;
	} else {
		dev->device.numa_node = 0;
	}

	pci_name_set(dev);

	/* parse resources */
	snprintf(filename, sizeof(filename), "%s/resource", dirname);
	if (pci_parse_sysfs_resource(filename, dev) < 0) {
		RTE_LOG(ERR, EAL, "%s(): cannot parse resource\n", __func__);
		free(dev);
		return -1;
	}

	/* parse driver */
	snprintf(filename, sizeof(filename), "%s/driver", dirname);
	ret = pci_get_kernel_driver_by_path(filename, driver, sizeof(driver));
	if (ret < 0) {
		RTE_LOG(ERR, EAL, "Fail to get kernel driver\n");
		free(dev);
		return -1;
	}

	if (!ret) {
		if (!strcmp(driver, "vfio-pci"))
			dev->kdrv = RTE_PCI_KDRV_VFIO;
		else if (!strcmp(driver, "igb_uio"))
			dev->kdrv = RTE_PCI_KDRV_IGB_UIO;
		else if (!strcmp(driver, "uio_pci_generic"))
			dev->kdrv = RTE_PCI_KDRV_UIO_GENERIC;
		else
			dev->kdrv = RTE_PCI_KDRV_UNKNOWN;
	} else {
		dev->kdrv = RTE_PCI_KDRV_NONE;
		return 0;
	}
	/* device is valid, add in list (sorted) */
	if (TAILQ_EMPTY(&rte_pci_bus.device_list)) {
		rte_pci_add_device(dev);
	} else {
		struct rte_pci_device *dev2;
		int ret;

		TAILQ_FOREACH(dev2, &rte_pci_bus.device_list, next) {
			ret = rte_pci_addr_cmp(&dev->addr, &dev2->addr);
			if (ret > 0)
				continue;

			if (ret < 0) {
				rte_pci_insert_device(dev2, dev);
			} else { /* already registered */
				if (!rte_dev_is_probed(&dev2->device)) {
					dev2->kdrv = dev->kdrv;
					dev2->max_vfs = dev->max_vfs;
					dev2->id = dev->id;
					pci_name_set(dev2);
					memmove(dev2->mem_resource,
						dev->mem_resource,
						sizeof(dev->mem_resource));
				} else {
					/**
					 * If device is plugged and driver is
					 * probed already, (This happens when
					 * we call rte_dev_probe which will
					 * scan all device on the bus) we don't
					 * need to do anything here unless...
					 **/
					if (dev2->kdrv != dev->kdrv ||
						dev2->max_vfs != dev->max_vfs ||
						memcmp(&dev2->id, &dev->id, sizeof(dev2->id)))
						/*
						 * This should not happens.
						 * But it is still possible if
						 * we unbind a device from
						 * vfio or uio before hotplug
						 * remove and rebind it with
						 * a different configure.
						 * So we just print out the
						 * error as an alarm.
						 */
						RTE_LOG(ERR, EAL, "Unexpected device scan at %s!\n",
							filename);
					else if (dev2->device.devargs !=
						 dev->device.devargs) {
						rte_devargs_remove(dev2->device.devargs);
						pci_name_set(dev2);
					}
				}
				free(dev);
			}
			return 0;
		}

		rte_pci_add_device(dev);
	}

	return 0;
}

/*
 * split up a pci address into its constituent parts.
 */
static int
parse_pci_addr_format(const char *buf, int bufsize, struct rte_pci_addr *addr)
{
	/* first split on ':' */
	union splitaddr {
		struct {
			char *domain;
			char *bus;
			char *devid;
			char *function;
		};
		char *str[PCI_FMT_NVAL]; /* last element-separator is "." not ":" */
	} splitaddr;

	char *buf_copy = strndup(buf, bufsize);
	if (buf_copy == NULL)
		return -1;

	if (rte_strsplit(buf_copy, bufsize, splitaddr.str, PCI_FMT_NVAL, ':')
			!= PCI_FMT_NVAL - 1)
		goto error;
	/* final split is on '.' between devid and function */
	splitaddr.function = strchr(splitaddr.devid,'.');
	if (splitaddr.function == NULL)
		goto error;
	*splitaddr.function++ = '\0';

	/* now convert to int values */
	errno = 0;
	addr->domain = strtoul(splitaddr.domain, NULL, 16);
	addr->bus = strtoul(splitaddr.bus, NULL, 16);
	addr->devid = strtoul(splitaddr.devid, NULL, 16);
	addr->function = strtoul(splitaddr.function, NULL, 10);
	if (errno != 0)
		goto error;

	free(buf_copy); /* free the copy made with strdup */
	return 0;
error:
	free(buf_copy);
	return -1;
}

/*
 * Scan the content of the PCI bus, and the devices in the devices
 * list
 */
int
rte_pci_scan(void)
{
	struct dirent *e;
	DIR *dir;
	char dirname[PATH_MAX];
	struct rte_pci_addr addr;

	/* for debug purposes, PCI can be disabled */
	if (!rte_eal_has_pci())
		return 0;

#ifdef VFIO_PRESENT
	if (!pci_vfio_is_enabled())
		RTE_LOG(DEBUG, EAL, "VFIO PCI modules not loaded\n");
#endif

	dir = opendir(rte_pci_get_sysfs_path());
	if (dir == NULL) {
		RTE_LOG(ERR, EAL, "%s(): opendir failed: %s\n",
			__func__, strerror(errno));
		return -1;
	}

	while ((e = readdir(dir)) != NULL) {
		if (e->d_name[0] == '.')
			continue;

		if (parse_pci_addr_format(e->d_name, sizeof(e->d_name), &addr) != 0)
			continue;

		if (rte_pci_ignore_device(&addr))
			continue;

		snprintf(dirname, sizeof(dirname), "%s/%s",
				rte_pci_get_sysfs_path(), e->d_name);

		if (pci_scan_one(dirname, &addr) < 0)
			goto error;
	}
	closedir(dir);
	return 0;

error:
	closedir(dir);
	return -1;
}

#if defined(RTE_ARCH_X86)
bool
pci_device_iommu_support_va(const struct rte_pci_device *dev)
{
#define VTD_CAP_MGAW_SHIFT	16
#define VTD_CAP_MGAW_MASK	(0x3fULL << VTD_CAP_MGAW_SHIFT)
	const struct rte_pci_addr *addr = &dev->addr;
	char filename[PATH_MAX];
	FILE *fp;
	uint64_t mgaw, vtd_cap_reg = 0;

	snprintf(filename, sizeof(filename),
		 "%s/" PCI_PRI_FMT "/iommu/intel-iommu/cap",
		 rte_pci_get_sysfs_path(), addr->domain, addr->bus, addr->devid,
		 addr->function);

	fp = fopen(filename, "r");
	if (fp == NULL) {
		/* We don't have an Intel IOMMU, assume VA supported */
		if (errno == ENOENT)
			return true;

		RTE_LOG(ERR, EAL, "%s(): can't open %s: %s\n",
			__func__, filename, strerror(errno));
		return false;
	}

	/* We have an Intel IOMMU */
	if (fscanf(fp, "%" PRIx64, &vtd_cap_reg) != 1) {
		RTE_LOG(ERR, EAL, "%s(): can't read %s\n", __func__, filename);
		fclose(fp);
		return false;
	}

	fclose(fp);

	mgaw = ((vtd_cap_reg & VTD_CAP_MGAW_MASK) >> VTD_CAP_MGAW_SHIFT) + 1;

	/*
	 * Assuming there is no limitation by now. We can not know at this point
	 * because the memory has not been initialized yet. Setting the dma mask
	 * will force a check once memory initialization is done. We can not do
	 * a fallback to IOVA PA now, but if the dma check fails, the error
	 * message should advice for using '--iova-mode pa' if IOVA VA is the
	 * current mode.
	 */
	rte_mem_set_dma_mask(mgaw);
	return true;
}
#elif defined(RTE_ARCH_PPC_64)
bool
pci_device_iommu_support_va(__rte_unused const struct rte_pci_device *dev)
{
	/*
	 * IOMMU is always present on a PowerNV host (IOMMUv2).
	 * IOMMU is also present in a KVM/QEMU VM (IOMMUv1) but is not
	 * currently supported by DPDK. Test for our current environment
	 * and report VA support as appropriate.
	 */

	char *line = NULL;
	size_t len = 0;
	char filename[PATH_MAX] = "/proc/cpuinfo";
	FILE *fp = fopen(filename, "r");
	bool ret = false;

	if (fp == NULL) {
		RTE_LOG(ERR, EAL, "%s(): can't open %s: %s\n",
			__func__, filename, strerror(errno));
		return ret;
	}

	/* Check for a PowerNV platform */
	while (getline(&line, &len, fp) != -1) {
		if (strstr(line, "platform") != NULL)
			continue;

		if (strstr(line, "PowerNV") != NULL) {
			RTE_LOG(DEBUG, EAL, "Running on a PowerNV system\n");
			ret = true;
			break;
		}
	}

	free(line);
	fclose(fp);
	return ret;
}
#else
bool
pci_device_iommu_support_va(__rte_unused const struct rte_pci_device *dev)
{
	return true;
}
#endif

enum rte_iova_mode
pci_device_iova_mode(const struct rte_pci_driver *pdrv,
		     const struct rte_pci_device *pdev)
{
	enum rte_iova_mode iova_mode = RTE_IOVA_DC;

	switch (pdev->kdrv) {
	case RTE_PCI_KDRV_VFIO: {
#ifdef VFIO_PRESENT
		static int is_vfio_noiommu_enabled = -1;

		if (is_vfio_noiommu_enabled == -1) {
			if (rte_vfio_noiommu_is_enabled() == 1)
				is_vfio_noiommu_enabled = 1;
			else
				is_vfio_noiommu_enabled = 0;
		}
		if (is_vfio_noiommu_enabled != 0)
			iova_mode = RTE_IOVA_PA;
		else if ((pdrv->drv_flags & RTE_PCI_DRV_NEED_IOVA_AS_VA) != 0)
			iova_mode = RTE_IOVA_VA;
#endif
		break;
	}

	case RTE_PCI_KDRV_IGB_UIO:
	case RTE_PCI_KDRV_UIO_GENERIC:
		iova_mode = RTE_IOVA_PA;
		break;

	default:
		if ((pdrv->drv_flags & RTE_PCI_DRV_NEED_IOVA_AS_VA) != 0)
			iova_mode = RTE_IOVA_VA;
		break;
	}
	return iova_mode;
}

/* Read PCI config space. */
int rte_pci_read_config(const struct rte_pci_device *device,
		void *buf, size_t len, off_t offset)
{
	char devname[RTE_DEV_NAME_MAX_LEN] = "";
	const struct rte_intr_handle *intr_handle = &device->intr_handle;

	switch (device->kdrv) {
	case RTE_PCI_KDRV_IGB_UIO:
	case RTE_PCI_KDRV_UIO_GENERIC:
		return pci_uio_read_config(intr_handle, buf, len, offset);
#ifdef VFIO_PRESENT
	case RTE_PCI_KDRV_VFIO:
		return pci_vfio_read_config(intr_handle, buf, len, offset);
#endif
	default:
		rte_pci_device_name(&device->addr, devname,
				    RTE_DEV_NAME_MAX_LEN);
		RTE_LOG(ERR, EAL,
			"Unknown driver type for %s\n", devname);
		return -1;
	}
}

/* Write PCI config space. */
int rte_pci_write_config(const struct rte_pci_device *device,
		const void *buf, size_t len, off_t offset)
{
	char devname[RTE_DEV_NAME_MAX_LEN] = "";
	const struct rte_intr_handle *intr_handle = &device->intr_handle;

	switch (device->kdrv) {
	case RTE_PCI_KDRV_IGB_UIO:
	case RTE_PCI_KDRV_UIO_GENERIC:
		return pci_uio_write_config(intr_handle, buf, len, offset);
#ifdef VFIO_PRESENT
	case RTE_PCI_KDRV_VFIO:
		return pci_vfio_write_config(intr_handle, buf, len, offset);
#endif
	default:
		rte_pci_device_name(&device->addr, devname,
				    RTE_DEV_NAME_MAX_LEN);
		RTE_LOG(ERR, EAL,
			"Unknown driver type for %s\n", devname);
		return -1;
	}
}

#if defined(RTE_ARCH_X86)
static int
pci_ioport_map(struct rte_pci_device *dev, int bar __rte_unused,
		struct rte_pci_ioport *p)
{
	uint16_t start, end;
	FILE *fp;
	char *line = NULL;
	char pci_id[16];
	int found = 0;
	size_t linesz;

	if (rte_eal_iopl_init() != 0) {
		RTE_LOG(ERR, EAL, "%s(): insufficient ioport permissions for PCI device %s\n",
			__func__, dev->name);
		return -1;
	}

	snprintf(pci_id, sizeof(pci_id), PCI_PRI_FMT,
		 dev->addr.domain, dev->addr.bus,
		 dev->addr.devid, dev->addr.function);

	fp = fopen("/proc/ioports", "r");
	if (fp == NULL) {
		RTE_LOG(ERR, EAL, "%s(): can't open ioports\n", __func__);
		return -1;
	}

	while (getdelim(&line, &linesz, '\n', fp) > 0) {
		char *ptr = line;
		char *left;
		int n;

		n = strcspn(ptr, ":");
		ptr[n] = 0;
		left = &ptr[n + 1];

		while (*left && isspace(*left))
			left++;

		if (!strncmp(left, pci_id, strlen(pci_id))) {
			found = 1;

			while (*ptr && isspace(*ptr))
				ptr++;

			sscanf(ptr, "%04hx-%04hx", &start, &end);

			break;
		}
	}

	free(line);
	fclose(fp);

	if (!found)
		return -1;

	p->base = start;
	RTE_LOG(DEBUG, EAL, "PCI Port IO found start=0x%x\n", start);

	return 0;
}
#endif

int
rte_pci_ioport_map(struct rte_pci_device *dev, int bar,
		struct rte_pci_ioport *p)
{
	int ret = -1;

	switch (dev->kdrv) {
#ifdef VFIO_PRESENT
	case RTE_PCI_KDRV_VFIO:
		if (pci_vfio_is_enabled())
			ret = pci_vfio_ioport_map(dev, bar, p);
		break;
#endif
	case RTE_PCI_KDRV_IGB_UIO:
		ret = pci_uio_ioport_map(dev, bar, p);
		break;
	case RTE_PCI_KDRV_UIO_GENERIC:
#if defined(RTE_ARCH_X86)
		ret = pci_ioport_map(dev, bar, p);
#else
		ret = pci_uio_ioport_map(dev, bar, p);
#endif
		break;
	default:
		break;
	}

	if (!ret)
		p->dev = dev;

	return ret;
}

void
rte_pci_ioport_read(struct rte_pci_ioport *p,
		void *data, size_t len, off_t offset)
{
	switch (p->dev->kdrv) {
#ifdef VFIO_PRESENT
	case RTE_PCI_KDRV_VFIO:
		pci_vfio_ioport_read(p, data, len, offset);
		break;
#endif
	case RTE_PCI_KDRV_IGB_UIO:
		pci_uio_ioport_read(p, data, len, offset);
		break;
	case RTE_PCI_KDRV_UIO_GENERIC:
		pci_uio_ioport_read(p, data, len, offset);
		break;
	default:
		break;
	}
}

void
rte_pci_ioport_write(struct rte_pci_ioport *p,
		const void *data, size_t len, off_t offset)
{
	switch (p->dev->kdrv) {
#ifdef VFIO_PRESENT
	case RTE_PCI_KDRV_VFIO:
		pci_vfio_ioport_write(p, data, len, offset);
		break;
#endif
	case RTE_PCI_KDRV_IGB_UIO:
		pci_uio_ioport_write(p, data, len, offset);
		break;
	case RTE_PCI_KDRV_UIO_GENERIC:
		pci_uio_ioport_write(p, data, len, offset);
		break;
	default:
		break;
	}
}

int
rte_pci_ioport_unmap(struct rte_pci_ioport *p)
{
	int ret = -1;

	switch (p->dev->kdrv) {
#ifdef VFIO_PRESENT
	case RTE_PCI_KDRV_VFIO:
		if (pci_vfio_is_enabled())
			ret = pci_vfio_ioport_unmap(p);
		break;
#endif
	case RTE_PCI_KDRV_IGB_UIO:
		ret = pci_uio_ioport_unmap(p);
		break;
	case RTE_PCI_KDRV_UIO_GENERIC:
#if defined(RTE_ARCH_X86)
		ret = 0;
#else
		ret = pci_uio_ioport_unmap(p);
#endif
		break;
	default:
		break;
	}

	return ret;
}
