/* Support for specifying IO affinity by various means.
   Copyright 2010 Intel Corporation
   Author: Andi Kleen

   libnuma is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; version
   2.1.

   libnuma is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should find a copy of v2.1 of the GNU Lesser General Public License
   somewhere on your Linux system; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA */

/* Notebook:
   - Separate real errors from no NUMA with fallback
   - Infiniband
   - FCoE?
   - Support for other special IO devices
   - Specifying cpu subsets inside the IO node?
   - Handle multiple IO nodes (needs kernel changes)
   - Better support for multi-path IO?
 */
#define _GNU_SOURCE 1
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <netdb.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <dirent.h>
#include <linux/rtnetlink.h>
#include <linux/netlink.h>
#include <sys/types.h>
#include <sys/sysmacros.h>
#include <ctype.h>
#include <assert.h>
#include <regex.h>
#include <sys/sysmacros.h>
#include "numa.h"
#include "numaint.h"
#include "sysfs.h"
#include "affinity.h"
#include "rtnetlink.h"

static int badchar(const char *s)
{
	if (strpbrk(s, "/."))
		return 1;
	return 0;
}

static int node_parse_failure(int ret, char *cls, const char *dev)
{
	if (!cls)
		cls = "";
	if (ret == -2)
		numa_warn(W_node_parse1,
			  "Kernel does not know node mask for%s%s device `%s'",
				*cls ? " " : "", cls, dev);
	else
		numa_warn(W_node_parse2,
			  "Cannot read node mask for %s device `%s'",
			  cls, dev);
	return -1;
}

/* Generic sysfs class lookup */
static int
affinity_class(struct bitmask *mask, char *cls, const char *dev)
{
	int ret;
	while (isspace(*dev))
		dev++;
	if (badchar(dev)) {
		numa_warn(W_badchar, "Illegal characters in `%s' specification",
			  dev);
		return -1;
	}

	/* Somewhat hackish: extract device from symlink path.
	   Better would be a direct backlink. This knows slightly too
	   much about the actual sysfs layout. */
	char path[1024];
	char *fn = NULL;
	if (asprintf(&fn, "/sys/class/%s/%s", cls, dev) > 0 &&
	    readlink(fn, path, sizeof path) > 0) {
		regex_t re;
		regmatch_t match[2];
		char *p;

		regcomp(&re, "(/devices/pci[0-9a-fA-F:/]+\\.[0-9]+)/",
			REG_EXTENDED);
		ret = regexec(&re, path, 2, match, 0);
		regfree(&re);
		if (ret == 0) {
			free(fn);
			assert(match[0].rm_so > 0);
			assert(match[0].rm_eo > 0);
			path[match[1].rm_eo + 1] = 0;
			p = path + match[0].rm_so;
			ret = sysfs_node_read(mask, "/sys/%s/numa_node", p);
			if (ret < 0)
				return node_parse_failure(ret, NULL, p);
			return ret;
		}
	}
	free(fn);

	ret = sysfs_node_read(mask, "/sys/class/%s/%s/device/numa_node",
			      cls, dev);
	if (ret < 0)
		return node_parse_failure(ret, cls, dev);
	return 0;
}

/* Turn file (or device node) into class name */
static int affinity_file(struct bitmask *mask, char *cls, const char *file)
{
	struct stat st;
	DIR *dir;
	int n;
	unsigned maj = 0, min = 0;
	dev_t d;
	struct dirent *dep;

	cls = "block";
	char fn[sizeof("/sys/class/") + strlen(cls)];
	if (stat(file, &st) < 0) {
		numa_warn(W_blockdev1, "Cannot stat file %s", file);
		return -1;
	}
	d = st.st_dev;
	if (S_ISCHR(st.st_mode)) {
		/* Better choice than misc? Most likely misc will not work
		   anyways unless the kernel is fixed. */
		cls = "misc";
		d = st.st_rdev;
	} else if (S_ISBLK(st.st_mode))
		d = st.st_rdev;

	sprintf(fn, "/sys/class/%s", cls);
	dir = opendir(fn);
	if (!dir) {
		numa_warn(W_blockdev2, "Cannot enumerate %s devices in sysfs",
			  cls);
		return -1;
	}
	while ((dep = readdir(dir)) != NULL) {
		char *name = dep->d_name;
		int ret;

		if (*name == '.')
			continue;
		char *dev;
		char fn2[sizeof("/sys/class/block//dev") + strlen(name)];

		n = -1;
		if (sprintf(fn2, "/sys/class/block/%s/dev", name) < 0)
			break;
		dev = sysfs_read(fn2);
		if (dev) {
			n = sscanf(dev, "%u:%u", &maj, &min);
			free(dev);
		}
		if (n != 2) {
			numa_warn(W_blockdev3, "Cannot parse sysfs device %s",
				  name);
			continue;
		}

		if (major(d) != maj || minor(d) != min)
			continue;

		ret = affinity_class(mask, "block", name);
		closedir(dir);
		return ret;
	}
	closedir(dir);
	numa_warn(W_blockdev5, "Cannot find block device %x:%x in sysfs for `%s'",
		  maj, min, file);
	return -1;
}

/* Look up interface of route using rtnetlink. */
static int find_route(struct sockaddr *dst, int *iifp)
{
	struct rtattr *rta;
	const int hdrlen = NLMSG_LENGTH(sizeof(struct rtmsg));
	struct {
		struct nlmsghdr msg;
		struct rtmsg rt;
		char buf[256];
	} req = {
		.msg = {
			.nlmsg_len = hdrlen,
			.nlmsg_type = RTM_GETROUTE,
			.nlmsg_flags = NLM_F_REQUEST,
		},
		.rt = {
			.rtm_family = dst->sa_family,
		},
	};
	struct sockaddr_nl adr = {
		.nl_family = AF_NETLINK,
	};

	if (rta_put_address(&req.msg, RTA_DST, dst) < 0) {
		numa_warn(W_netlink1, "Cannot handle network family %x",
			  dst->sa_family);
		return -1;
	}

	if (rtnetlink_request(&req.msg, sizeof req, &adr) < 0) {
		numa_warn(W_netlink2, "Cannot request rtnetlink route: %s",
			  strerror(errno));
		return -1;
	}

	/* Fish the interface out of the netlink soup. */
	rta = NULL;
	while ((rta = rta_get(&req.msg, rta, hdrlen)) != NULL) {
		if (rta->rta_type == RTA_OIF) {
			memcpy(iifp, RTA_DATA(rta), sizeof(int));
			return 0;
		}
	}

	numa_warn(W_netlink3, "rtnetlink query did not return interface");
	return -1;
}

static int iif_to_name(int iif, struct ifreq *ifr)
{
	int n;
	int sk = socket(PF_INET, SOCK_DGRAM, 0);
	if (sk < 0)
		return -1;
	ifr->ifr_ifindex = iif;
	n = ioctl(sk, SIOCGIFNAME, ifr);
	close(sk);
	return n;
}

/* Resolve an IP address to the nodes of a network device.
   This generally only attempts to handle simple cases:
   no multi-path, no bounding etc. In these cases only
   the first interface or none is chosen. */
static int affinity_ip(struct bitmask *mask, char *cls, const char *id)
{
	struct addrinfo *ai;
	int n;
	int iif;
	struct ifreq ifr;

	if ((n = getaddrinfo(id, NULL, NULL, &ai)) != 0) {
		numa_warn(W_net1, "Cannot resolve %s: %s",
			  id, gai_strerror(n));
		return -1;
	}

	if (find_route(&ai->ai_addr[0], &iif) < 0)
		goto out_ai;

	if (iif_to_name(iif, &ifr) < 0) {
		numa_warn(W_net2, "Cannot resolve network interface %d", iif);
		goto out_ai;
	}

	freeaddrinfo(ai);
	return affinity_class(mask, "net", ifr.ifr_name);

out_ai:
	freeaddrinfo(ai);
	return -1;
}

/* Look up affinity for a PCI device */
static int affinity_pci(struct bitmask *mask, char *cls, const char *id)
{
	unsigned seg, bus, dev, func;
	int n, ret;

	/* Func is optional. */
	if ((n = sscanf(id, "%x:%x:%x.%x",&seg,&bus,&dev,&func)) == 4 || n == 3) {
		if (n == 3)
			func = 0;
	}
	/* Segment is optional too */
	else if ((n = sscanf(id, "%x:%x.%x",&bus,&dev,&func)) == 3 || n == 2) {
		seg = 0;
		if (n == 2)
			func = 0;
	} else {
		numa_warn(W_pci1, "Cannot parse PCI device `%s'", id);
		return -1;
	}
	ret = sysfs_node_read(mask,
			"/sys/devices/pci%04x:%02x/%04x:%02x:%02x.%x/numa_node",
			      seg, bus, seg, bus, dev, func);
	if (ret < 0)
		return node_parse_failure(ret, cls, id);
	return 0;
}

static struct handler {
	char first;
	char *name;
	char *cls;
	int (*handler)(struct bitmask *mask, char *cls, const char *desc);
} handlers[] = {
	{ 'n', "netdev:", "net",   affinity_class },
	{ 'i', "ip:",     NULL,    affinity_ip    },
	{ 'f', "file:",   NULL,    affinity_file  },
	{ 'b', "block:",  "block", affinity_class },
	{ 'p', "pci:",    NULL,	   affinity_pci   },
	{}
};

hidden int resolve_affinity(const char *id, struct bitmask *mask)
{
	struct handler *h;

	for (h = &handlers[0]; h->first; h++) {
		int len;
		if (id[0] != h->first)
			continue;
		len = strlen(h->name);
		if (!strncmp(id, h->name, len)) {
			int ret = h->handler(mask, h->cls, id + len);
			if (ret == -2) {
				numa_warn(W_nonode, "Kernel does not know node for %s\n",
					  id + len);
			}
			return ret;
		}
	}
	return NO_IO_AFFINITY;
}
