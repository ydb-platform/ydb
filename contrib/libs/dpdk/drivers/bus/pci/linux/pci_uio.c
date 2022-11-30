#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/sysmacros.h>
#include <linux/pci_regs.h>

#if defined(RTE_ARCH_X86)
#include <sys/io.h>
#endif

#include <rte_string_fns.h>
#include <rte_log.h>
#include <rte_pci.h>
#include <rte_bus_pci.h>
#include <rte_common.h>
#include <rte_malloc.h>

#include "eal_filesystem.h"
#include "pci_init.h"
#include "private.h"

void *pci_map_addr = NULL;

#define OFF_MAX              ((uint64_t)(off_t)-1)

int
pci_uio_read_config(const struct rte_intr_handle *intr_handle,
		    void *buf, size_t len, off_t offset)
{
	return pread(intr_handle->uio_cfg_fd, buf, len, offset);
}

int
pci_uio_write_config(const struct rte_intr_handle *intr_handle,
		     const void *buf, size_t len, off_t offset)
{
	return pwrite(intr_handle->uio_cfg_fd, buf, len, offset);
}

static int
pci_uio_set_bus_master(int dev_fd)
{
	uint16_t reg;
	int ret;

	ret = pread(dev_fd, &reg, sizeof(reg), PCI_COMMAND);
	if (ret != sizeof(reg)) {
		RTE_LOG(ERR, EAL,
			"Cannot read command from PCI config space!\n");
		return -1;
	}

	/* return if bus mastering is already on */
	if (reg & PCI_COMMAND_MASTER)
		return 0;

	reg |= PCI_COMMAND_MASTER;

	ret = pwrite(dev_fd, &reg, sizeof(reg), PCI_COMMAND);
	if (ret != sizeof(reg)) {
		RTE_LOG(ERR, EAL,
			"Cannot write command to PCI config space!\n");
		return -1;
	}

	return 0;
}

static int
pci_mknod_uio_dev(const char *sysfs_uio_path, unsigned uio_num)
{
	FILE *f;
	char filename[PATH_MAX];
	int ret;
	unsigned major, minor;
	dev_t dev;

	/* get the name of the sysfs file that contains the major and minor
	 * of the uio device and read its content */
	snprintf(filename, sizeof(filename), "%s/dev", sysfs_uio_path);

	f = fopen(filename, "r");
	if (f == NULL) {
		RTE_LOG(ERR, EAL, "%s(): cannot open sysfs to get major:minor\n",
			__func__);
		return -1;
	}

	ret = fscanf(f, "%u:%u", &major, &minor);
	if (ret != 2) {
		RTE_LOG(ERR, EAL, "%s(): cannot parse sysfs to get major:minor\n",
			__func__);
		fclose(f);
		return -1;
	}
	fclose(f);

	/* create the char device "mknod /dev/uioX c major minor" */
	snprintf(filename, sizeof(filename), "/dev/uio%u", uio_num);
	dev = makedev(major, minor);
	ret = mknod(filename, S_IFCHR | S_IRUSR | S_IWUSR, dev);
	if (ret != 0) {
		RTE_LOG(ERR, EAL, "%s(): mknod() failed %s\n",
			__func__, strerror(errno));
		return -1;
	}

	return ret;
}

/*
 * Return the uioX char device used for a pci device. On success, return
 * the UIO number and fill dstbuf string with the path of the device in
 * sysfs. On error, return a negative value. In this case dstbuf is
 * invalid.
 */
static int
pci_get_uio_dev(struct rte_pci_device *dev, char *dstbuf,
			   unsigned int buflen, int create)
{
	struct rte_pci_addr *loc = &dev->addr;
	int uio_num = -1;
	struct dirent *e;
	DIR *dir;
	char dirname[PATH_MAX];

	/* depending on kernel version, uio can be located in uio/uioX
	 * or uio:uioX */

	snprintf(dirname, sizeof(dirname),
			"%s/" PCI_PRI_FMT "/uio", rte_pci_get_sysfs_path(),
			loc->domain, loc->bus, loc->devid, loc->function);

	dir = opendir(dirname);
	if (dir == NULL) {
		/* retry with the parent directory */
		snprintf(dirname, sizeof(dirname),
				"%s/" PCI_PRI_FMT, rte_pci_get_sysfs_path(),
				loc->domain, loc->bus, loc->devid, loc->function);
		dir = opendir(dirname);

		if (dir == NULL) {
			RTE_LOG(ERR, EAL, "Cannot opendir %s\n", dirname);
			return -1;
		}
	}

	/* take the first file starting with "uio" */
	while ((e = readdir(dir)) != NULL) {
		/* format could be uio%d ...*/
		int shortprefix_len = sizeof("uio") - 1;
		/* ... or uio:uio%d */
		int longprefix_len = sizeof("uio:uio") - 1;
		char *endptr;

		if (strncmp(e->d_name, "uio", 3) != 0)
			continue;

		/* first try uio%d */
		errno = 0;
		uio_num = strtoull(e->d_name + shortprefix_len, &endptr, 10);
		if (errno == 0 && endptr != (e->d_name + shortprefix_len)) {
			snprintf(dstbuf, buflen, "%s/uio%u", dirname, uio_num);
			break;
		}

		/* then try uio:uio%d */
		errno = 0;
		uio_num = strtoull(e->d_name + longprefix_len, &endptr, 10);
		if (errno == 0 && endptr != (e->d_name + longprefix_len)) {
			snprintf(dstbuf, buflen, "%s/uio:uio%u", dirname, uio_num);
			break;
		}
	}
	closedir(dir);

	/* No uio resource found */
	if (e == NULL)
		return -1;

	/* create uio device if we've been asked to */
	if (rte_eal_create_uio_dev() && create &&
			pci_mknod_uio_dev(dstbuf, uio_num) < 0)
		RTE_LOG(WARNING, EAL, "Cannot create /dev/uio%u\n", uio_num);

	return uio_num;
}

void
pci_uio_free_resource(struct rte_pci_device *dev,
		struct mapped_pci_resource *uio_res)
{
	rte_free(uio_res);

	if (dev->intr_handle.uio_cfg_fd >= 0) {
		close(dev->intr_handle.uio_cfg_fd);
		dev->intr_handle.uio_cfg_fd = -1;
	}
	if (dev->intr_handle.fd >= 0) {
		close(dev->intr_handle.fd);
		dev->intr_handle.fd = -1;
		dev->intr_handle.type = RTE_INTR_HANDLE_UNKNOWN;
	}
}

int
pci_uio_alloc_resource(struct rte_pci_device *dev,
		struct mapped_pci_resource **uio_res)
{
	char dirname[PATH_MAX];
	char cfgname[PATH_MAX];
	char devname[PATH_MAX]; /* contains the /dev/uioX */
	int uio_num;
	struct rte_pci_addr *loc;

	loc = &dev->addr;

	/* find uio resource */
	uio_num = pci_get_uio_dev(dev, dirname, sizeof(dirname), 1);
	if (uio_num < 0) {
		RTE_LOG(WARNING, EAL, "  "PCI_PRI_FMT" not managed by UIO driver, "
				"skipping\n", loc->domain, loc->bus, loc->devid, loc->function);
		return 1;
	}
	snprintf(devname, sizeof(devname), "/dev/uio%u", uio_num);

	/* save fd if in primary process */
	dev->intr_handle.fd = open(devname, O_RDWR);
	if (dev->intr_handle.fd < 0) {
		RTE_LOG(ERR, EAL, "Cannot open %s: %s\n",
			devname, strerror(errno));
		goto error;
	}

	snprintf(cfgname, sizeof(cfgname),
			"/sys/class/uio/uio%u/device/config", uio_num);
	dev->intr_handle.uio_cfg_fd = open(cfgname, O_RDWR);
	if (dev->intr_handle.uio_cfg_fd < 0) {
		RTE_LOG(ERR, EAL, "Cannot open %s: %s\n",
			cfgname, strerror(errno));
		goto error;
	}

	if (dev->kdrv == RTE_PCI_KDRV_IGB_UIO)
		dev->intr_handle.type = RTE_INTR_HANDLE_UIO;
	else {
		dev->intr_handle.type = RTE_INTR_HANDLE_UIO_INTX;

		/* set bus master that is not done by uio_pci_generic */
		if (pci_uio_set_bus_master(dev->intr_handle.uio_cfg_fd)) {
			RTE_LOG(ERR, EAL, "Cannot set up bus mastering!\n");
			goto error;
		}
	}

	/* allocate the mapping details for secondary processes*/
	*uio_res = rte_zmalloc("UIO_RES", sizeof(**uio_res), 0);
	if (*uio_res == NULL) {
		RTE_LOG(ERR, EAL,
			"%s(): cannot store uio mmap details\n", __func__);
		goto error;
	}

	strlcpy((*uio_res)->path, devname, sizeof((*uio_res)->path));
	memcpy(&(*uio_res)->pci_addr, &dev->addr, sizeof((*uio_res)->pci_addr));

	return 0;

error:
	pci_uio_free_resource(dev, *uio_res);
	return -1;
}

int
pci_uio_map_resource_by_index(struct rte_pci_device *dev, int res_idx,
		struct mapped_pci_resource *uio_res, int map_idx)
{
	int fd = -1;
	char devname[PATH_MAX];
	void *mapaddr;
	struct rte_pci_addr *loc;
	struct pci_map *maps;
	int wc_activate = 0;

	if (dev->driver != NULL)
		wc_activate = dev->driver->drv_flags & RTE_PCI_DRV_WC_ACTIVATE;

	loc = &dev->addr;
	maps = uio_res->maps;

	/* allocate memory to keep path */
	maps[map_idx].path = rte_malloc(NULL, sizeof(devname), 0);
	if (maps[map_idx].path == NULL) {
		RTE_LOG(ERR, EAL, "Cannot allocate memory for path: %s\n",
				strerror(errno));
		return -1;
	}

	/*
	 * open resource file, to mmap it
	 */
	if (wc_activate) {
		/* update devname for mmap  */
		snprintf(devname, sizeof(devname),
			"%s/" PCI_PRI_FMT "/resource%d_wc",
			rte_pci_get_sysfs_path(),
			loc->domain, loc->bus, loc->devid,
			loc->function, res_idx);

		fd = open(devname, O_RDWR);
		if (fd < 0 && errno != ENOENT) {
			RTE_LOG(INFO, EAL, "%s cannot be mapped. "
				"Fall-back to non prefetchable mode.\n",
				devname);
		}
	}

	if (!wc_activate || fd < 0) {
		snprintf(devname, sizeof(devname),
			"%s/" PCI_PRI_FMT "/resource%d",
			rte_pci_get_sysfs_path(),
			loc->domain, loc->bus, loc->devid,
			loc->function, res_idx);

		/* then try to map resource file */
		fd = open(devname, O_RDWR);
		if (fd < 0) {
			RTE_LOG(ERR, EAL, "Cannot open %s: %s\n",
				devname, strerror(errno));
			goto error;
		}
	}

	/* try mapping somewhere close to the end of hugepages */
	if (pci_map_addr == NULL)
		pci_map_addr = pci_find_max_end_va();

	mapaddr = pci_map_resource(pci_map_addr, fd, 0,
			(size_t)dev->mem_resource[res_idx].len, 0);
	close(fd);
	if (mapaddr == NULL)
		goto error;

	pci_map_addr = RTE_PTR_ADD(mapaddr,
			(size_t)dev->mem_resource[res_idx].len);

	pci_map_addr = RTE_PTR_ALIGN(pci_map_addr, sysconf(_SC_PAGE_SIZE));

	maps[map_idx].phaddr = dev->mem_resource[res_idx].phys_addr;
	maps[map_idx].size = dev->mem_resource[res_idx].len;
	maps[map_idx].addr = mapaddr;
	maps[map_idx].offset = 0;
	strcpy(maps[map_idx].path, devname);
	dev->mem_resource[res_idx].addr = mapaddr;

	return 0;

error:
	rte_free(maps[map_idx].path);
	return -1;
}

#if defined(RTE_ARCH_X86)
int
pci_uio_ioport_map(struct rte_pci_device *dev, int bar,
		   struct rte_pci_ioport *p)
{
	char dirname[PATH_MAX];
	char filename[PATH_MAX];
	int uio_num;
	unsigned long start;

	if (rte_eal_iopl_init() != 0) {
		RTE_LOG(ERR, EAL, "%s(): insufficient ioport permissions for PCI device %s\n",
			__func__, dev->name);
		return -1;
	}

	uio_num = pci_get_uio_dev(dev, dirname, sizeof(dirname), 0);
	if (uio_num < 0)
		return -1;

	/* get portio start */
	snprintf(filename, sizeof(filename),
		 "%s/portio/port%d/start", dirname, bar);
	if (eal_parse_sysfs_value(filename, &start) < 0) {
		RTE_LOG(ERR, EAL, "%s(): cannot parse portio start\n",
			__func__);
		return -1;
	}
	/* ensure we don't get anything funny here, read/write will cast to
	 * uin16_t */
	if (start > UINT16_MAX)
		return -1;

	/* FIXME only for primary process ? */
	if (dev->intr_handle.type == RTE_INTR_HANDLE_UNKNOWN) {

		snprintf(filename, sizeof(filename), "/dev/uio%u", uio_num);
		dev->intr_handle.fd = open(filename, O_RDWR);
		if (dev->intr_handle.fd < 0) {
			RTE_LOG(ERR, EAL, "Cannot open %s: %s\n",
				filename, strerror(errno));
			return -1;
		}
		dev->intr_handle.type = RTE_INTR_HANDLE_UIO;
	}

	RTE_LOG(DEBUG, EAL, "PCI Port IO found start=0x%lx\n", start);

	p->base = start;
	p->len = 0;
	return 0;
}
#else
int
pci_uio_ioport_map(struct rte_pci_device *dev, int bar,
		   struct rte_pci_ioport *p)
{
	FILE *f;
	char buf[BUFSIZ];
	char filename[PATH_MAX];
	uint64_t phys_addr, end_addr, flags;
	int fd, i;
	void *addr;

	/* open and read addresses of the corresponding resource in sysfs */
	snprintf(filename, sizeof(filename), "%s/" PCI_PRI_FMT "/resource",
		rte_pci_get_sysfs_path(), dev->addr.domain, dev->addr.bus,
		dev->addr.devid, dev->addr.function);
	f = fopen(filename, "r");
	if (f == NULL) {
		RTE_LOG(ERR, EAL, "Cannot open sysfs resource: %s\n",
			strerror(errno));
		return -1;
	}
	for (i = 0; i < bar + 1; i++) {
		if (fgets(buf, sizeof(buf), f) == NULL) {
			RTE_LOG(ERR, EAL, "Cannot read sysfs resource\n");
			goto error;
		}
	}
	if (pci_parse_one_sysfs_resource(buf, sizeof(buf), &phys_addr,
			&end_addr, &flags) < 0)
		goto error;
	if ((flags & IORESOURCE_IO) == 0) {
		RTE_LOG(ERR, EAL, "BAR %d is not an IO resource\n", bar);
		goto error;
	}
	snprintf(filename, sizeof(filename), "%s/" PCI_PRI_FMT "/resource%d",
		rte_pci_get_sysfs_path(), dev->addr.domain, dev->addr.bus,
		dev->addr.devid, dev->addr.function, bar);

	/* mmap the pci resource */
	fd = open(filename, O_RDWR);
	if (fd < 0) {
		RTE_LOG(ERR, EAL, "Cannot open %s: %s\n", filename,
			strerror(errno));
		goto error;
	}
	addr = mmap(NULL, end_addr + 1, PROT_READ | PROT_WRITE,
		MAP_SHARED, fd, 0);
	close(fd);
	if (addr == MAP_FAILED) {
		RTE_LOG(ERR, EAL, "Cannot mmap IO port resource: %s\n",
			strerror(errno));
		goto error;
	}

	/* strangely, the base address is mmap addr + phys_addr */
	p->base = (uintptr_t)addr + phys_addr;
	p->len = end_addr + 1;
	RTE_LOG(DEBUG, EAL, "PCI Port IO found start=0x%"PRIx64"\n", p->base);
	fclose(f);

	return 0;

error:
	fclose(f);
	return -1;
}
#endif

void
pci_uio_ioport_read(struct rte_pci_ioport *p,
		    void *data, size_t len, off_t offset)
{
	uint8_t *d;
	int size;
	uintptr_t reg = p->base + offset;

	for (d = data; len > 0; d += size, reg += size, len -= size) {
		if (len >= 4) {
			size = 4;
#if defined(RTE_ARCH_X86)
			*(uint32_t *)d = inl(reg);
#else
			*(uint32_t *)d = *(volatile uint32_t *)reg;
#endif
		} else if (len >= 2) {
			size = 2;
#if defined(RTE_ARCH_X86)
			*(uint16_t *)d = inw(reg);
#else
			*(uint16_t *)d = *(volatile uint16_t *)reg;
#endif
		} else {
			size = 1;
#if defined(RTE_ARCH_X86)
			*d = inb(reg);
#else
			*d = *(volatile uint8_t *)reg;
#endif
		}
	}
}

void
pci_uio_ioport_write(struct rte_pci_ioport *p,
		     const void *data, size_t len, off_t offset)
{
	const uint8_t *s;
	int size;
	uintptr_t reg = p->base + offset;

	for (s = data; len > 0; s += size, reg += size, len -= size) {
		if (len >= 4) {
			size = 4;
#if defined(RTE_ARCH_X86)
			outl_p(*(const uint32_t *)s, reg);
#else
			*(volatile uint32_t *)reg = *(const uint32_t *)s;
#endif
		} else if (len >= 2) {
			size = 2;
#if defined(RTE_ARCH_X86)
			outw_p(*(const uint16_t *)s, reg);
#else
			*(volatile uint16_t *)reg = *(const uint16_t *)s;
#endif
		} else {
			size = 1;
#if defined(RTE_ARCH_X86)
			outb_p(*s, reg);
#else
			*(volatile uint8_t *)reg = *s;
#endif
		}
	}
}

int
pci_uio_ioport_unmap(struct rte_pci_ioport *p)
{
#if defined(RTE_ARCH_X86)
	RTE_SET_USED(p);
	/* FIXME close intr fd ? */
	return 0;
#else
	return munmap((void *)(uintptr_t)p->base, p->len);
#endif
}
