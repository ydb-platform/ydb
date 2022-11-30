#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"
#include "spdk/string.h"

#include "spdk/log.h"
#include "spdk/env.h"

#ifdef __linux__

#include <linux/netlink.h>

#define SPDK_UEVENT_MSG_LEN 4096
#define SPDK_UEVENT_RECVBUF_SIZE 1024 * 1024

int
spdk_pci_event_listen(void)
{
	struct sockaddr_nl addr;
	int netlink_fd;
	int size = SPDK_UEVENT_RECVBUF_SIZE;
	int flag, rc;

	memset(&addr, 0, sizeof(addr));
	addr.nl_family = AF_NETLINK;
	addr.nl_pid = 0;
	addr.nl_groups = 0xffffffff;

	netlink_fd = socket(PF_NETLINK, SOCK_DGRAM, NETLINK_KOBJECT_UEVENT);
	if (netlink_fd < 0) {
		SPDK_ERRLOG("Failed to create netlink socket\n");
		return netlink_fd;
	}

	if (setsockopt(netlink_fd, SOL_SOCKET, SO_RCVBUFFORCE, &size, sizeof(size)) < 0) {
		rc = errno;
		SPDK_ERRLOG("Failed to set socket option\n");
		close(netlink_fd);
		return -rc;
	}

	flag = fcntl(netlink_fd, F_GETFL);
	if (flag < 0) {
		rc = errno;
		SPDK_ERRLOG("Failed to get socket flag, fd: %d\n", netlink_fd);
		close(netlink_fd);
		return -rc;
	}

	if (fcntl(netlink_fd, F_SETFL, flag | O_NONBLOCK) < 0) {
		rc = errno;
		SPDK_ERRLOG("Fcntl can't set nonblocking mode for socket, fd: %d\n", netlink_fd);
		close(netlink_fd);
		return -rc;
	}

	if (bind(netlink_fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
		rc = errno;
		SPDK_ERRLOG("Failed to bind the netlink\n");
		close(netlink_fd);
		return -rc;
	}

	return netlink_fd;
}

/* Note: We parse the event from uio and vfio subsystem and will ignore
 *       all the event from other subsystem. the event from uio subsystem
 *       as below:
 *       action: "add" or "remove"
 *       subsystem: "uio"
 *       dev_path: "/devices/pci0000:80/0000:80:01.0/0000:81:00.0/uio/uio0"
 *       VFIO subsystem add event:
 *       ACTION=bind
 *       DRIVER=vfio-pci
 *       PCI_SLOT_NAME=0000:d8:00.0
 */
static int
parse_subsystem_event(const char *buf, struct spdk_pci_event *event)
{
	char subsystem[SPDK_UEVENT_MSG_LEN];
	char action[SPDK_UEVENT_MSG_LEN];
	char dev_path[SPDK_UEVENT_MSG_LEN];
	char driver[SPDK_UEVENT_MSG_LEN];
	char vfio_pci_addr[SPDK_UEVENT_MSG_LEN];
	char *pci_address, *tmp;
	int rc;

	memset(subsystem, 0, SPDK_UEVENT_MSG_LEN);
	memset(action, 0, SPDK_UEVENT_MSG_LEN);
	memset(dev_path, 0, SPDK_UEVENT_MSG_LEN);
	memset(driver, 0, SPDK_UEVENT_MSG_LEN);
	memset(vfio_pci_addr, 0, SPDK_UEVENT_MSG_LEN);

	while (*buf) {
		if (!strncmp(buf, "SUBSYSTEM=", 10)) {
			buf += 10;
			snprintf(subsystem, sizeof(subsystem), "%s", buf);
		} else if (!strncmp(buf, "ACTION=", 7)) {
			buf += 7;
			snprintf(action, sizeof(action), "%s", buf);
		} else if (!strncmp(buf, "DEVPATH=", 8)) {
			buf += 8;
			snprintf(dev_path, sizeof(dev_path), "%s", buf);
		} else if (!strncmp(buf, "DRIVER=", 7)) {
			buf += 7;
			snprintf(driver, sizeof(driver), "%s", buf);
		} else if (!strncmp(buf, "PCI_SLOT_NAME=", 14)) {
			buf += 14;
			snprintf(vfio_pci_addr, sizeof(vfio_pci_addr), "%s", buf);
		}

		while (*buf++)
			;
	}

	if (!strncmp(subsystem, "uio", 3)) {
		if (!strncmp(action, "remove", 6)) {
			event->action = SPDK_UEVENT_REMOVE;
		} else if (!strncmp(action, "add", 3)) {
			/* Support the ADD UEVENT for the device allow */
			event->action = SPDK_UEVENT_ADD;
		} else {
			return 0;
		}

		tmp = strstr(dev_path, "/uio/");
		if (!tmp) {
			SPDK_ERRLOG("Invalid format of uevent: %s\n", dev_path);
			return -EBADMSG;
		}
		memset(tmp, 0, SPDK_UEVENT_MSG_LEN - (tmp - dev_path));

		pci_address = strrchr(dev_path, '/');
		if (!pci_address) {
			SPDK_ERRLOG("Not found PCI device BDF in uevent: %s\n", dev_path);
			return -EBADMSG;
		}
		pci_address++;

		rc = spdk_pci_addr_parse(&event->traddr, pci_address);
		if (rc != 0) {
			SPDK_ERRLOG("Invalid format for PCI device BDF: %s\n", pci_address);
			return rc;
		}

		return 1;
	}

	if (!strncmp(driver, "vfio-pci", 8)) {
		if (!strncmp(action, "bind", 4)) {
			/* Support the ADD UEVENT for the device allow */
			event->action = SPDK_UEVENT_ADD;
		} else {
			/* Only need to support add event.
			 * VFIO hotplug interface is "pci.c:pci_device_rte_dev_event".
			 * VFIO informs the userspace hotplug through vfio req notifier interrupt.
			 * The app needs to free the device userspace driver resource first then
			 * the OS remove the device VFIO driver and boardcast the VFIO uevent.
			 */
			return 0;
		}

		rc = spdk_pci_addr_parse(&event->traddr, vfio_pci_addr);
		if (rc != 0) {
			SPDK_ERRLOG("Invalid format for PCI device BDF: %s\n", vfio_pci_addr);
			return rc;
		}

		return 1;
	}

	return 0;
}

int
spdk_pci_get_event(int fd, struct spdk_pci_event *event)
{
	int ret;
	char buf[SPDK_UEVENT_MSG_LEN];

	memset(buf, 0, SPDK_UEVENT_MSG_LEN);
	memset(event, 0, sizeof(*event));

	ret = recv(fd, buf, SPDK_UEVENT_MSG_LEN - 1, MSG_DONTWAIT);
	if (ret > 0) {
		return parse_subsystem_event(buf, event);
	} else if (ret < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			return 0;
		} else {
			ret = errno;
			SPDK_ERRLOG("Socket read error %d\n", errno);
			return -ret;
		}
	} else {
		/* connection closed */
		return -ENOTCONN;
	}

	return 0;
}

#else /* Not Linux */

int
spdk_pci_event_listen(void)
{
	SPDK_ERRLOG("Non-Linux does not support this operation\n");
	return -ENOTSUP;
}

int
spdk_pci_get_event(int fd, struct spdk_pci_event *event)
{
	SPDK_ERRLOG("Non-Linux does not support this operation\n");
	return -ENOTSUP;
}
#endif
