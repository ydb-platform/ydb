#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2018 Intel Corporation
 */

#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <linux/netlink.h>

#include <rte_string_fns.h>
#include <rte_log.h>
#include <rte_compat.h>
#include <rte_dev.h>
#include <rte_malloc.h>
#include <rte_interrupts.h>
#include <rte_alarm.h>
#include <rte_bus.h>
#include <rte_eal.h>
#include <rte_spinlock.h>
#include <rte_errno.h>

#include "eal_private.h"

static struct rte_intr_handle intr_handle = {.fd = -1 };
static bool monitor_started;
static bool hotplug_handle;

#define EAL_UEV_MSG_LEN 4096
#define EAL_UEV_MSG_ELEM_LEN 128

/*
 * spinlock for device hot-unplug failure handling. If it try to access bus or
 * device, such as handle sigbus on bus or handle memory failure for device
 * just need to use this lock. It could protect the bus and the device to avoid
 * race condition.
 */
static rte_spinlock_t failure_handle_lock = RTE_SPINLOCK_INITIALIZER;

static struct sigaction sigbus_action_old;

static int sigbus_need_recover;

static void dev_uev_handler(__rte_unused void *param);

/* identify the system layer which reports this event. */
enum eal_dev_event_subsystem {
	EAL_DEV_EVENT_SUBSYSTEM_PCI, /* PCI bus device event */
	EAL_DEV_EVENT_SUBSYSTEM_UIO, /* UIO driver device event */
	EAL_DEV_EVENT_SUBSYSTEM_VFIO, /* VFIO driver device event */
	EAL_DEV_EVENT_SUBSYSTEM_MAX
};

static void
sigbus_action_recover(void)
{
	if (sigbus_need_recover) {
		sigaction(SIGBUS, &sigbus_action_old, NULL);
		sigbus_need_recover = 0;
	}
}

static void sigbus_handler(int signum, siginfo_t *info,
				void *ctx __rte_unused)
{
	int ret;

	RTE_LOG(DEBUG, EAL, "Thread catch SIGBUS, fault address:%p\n",
		info->si_addr);

	rte_spinlock_lock(&failure_handle_lock);
	ret = rte_bus_sigbus_handler(info->si_addr);
	rte_spinlock_unlock(&failure_handle_lock);
	if (ret == -1) {
		rte_exit(EXIT_FAILURE,
			 "Failed to handle SIGBUS for hot-unplug, "
			 "(rte_errno: %s)!", strerror(rte_errno));
	} else if (ret == 1) {
		if (sigbus_action_old.sa_flags == SA_SIGINFO
		    && sigbus_action_old.sa_sigaction) {
			(*(sigbus_action_old.sa_sigaction))(signum,
							    info, ctx);
		} else if (sigbus_action_old.sa_flags != SA_SIGINFO
			   && sigbus_action_old.sa_handler) {
			(*(sigbus_action_old.sa_handler))(signum);
		} else {
			rte_exit(EXIT_FAILURE,
				 "Failed to handle generic SIGBUS!");
		}
	}

	RTE_LOG(DEBUG, EAL, "Success to handle SIGBUS for hot-unplug!\n");
}

static int cmp_dev_name(const struct rte_device *dev,
	const void *_name)
{
	const char *name = _name;

	return strcmp(dev->name, name);
}

static int
dev_uev_socket_fd_create(void)
{
	struct sockaddr_nl addr;
	int ret;

	intr_handle.fd = socket(PF_NETLINK, SOCK_RAW | SOCK_CLOEXEC |
			SOCK_NONBLOCK,
			NETLINK_KOBJECT_UEVENT);
	if (intr_handle.fd < 0) {
		RTE_LOG(ERR, EAL, "create uevent fd failed.\n");
		return -1;
	}

	memset(&addr, 0, sizeof(addr));
	addr.nl_family = AF_NETLINK;
	addr.nl_pid = 0;
	addr.nl_groups = 0xffffffff;

	ret = bind(intr_handle.fd, (struct sockaddr *) &addr, sizeof(addr));
	if (ret < 0) {
		RTE_LOG(ERR, EAL, "Failed to bind uevent socket.\n");
		goto err;
	}

	return 0;
err:
	close(intr_handle.fd);
	intr_handle.fd = -1;
	return ret;
}

struct rte_dev_event {
	enum rte_dev_event_type type;	/**< device event type */
	int subsystem;			/**< subsystem id */
	char *devname;			/**< device name */
};

static int
dev_uev_parse(const char *buf, struct rte_dev_event *event, int length)
{
	char action[EAL_UEV_MSG_ELEM_LEN];
	char subsystem[EAL_UEV_MSG_ELEM_LEN];
	char pci_slot_name[EAL_UEV_MSG_ELEM_LEN];
	int i = 0;

	memset(action, 0, EAL_UEV_MSG_ELEM_LEN);
	memset(subsystem, 0, EAL_UEV_MSG_ELEM_LEN);
	memset(pci_slot_name, 0, EAL_UEV_MSG_ELEM_LEN);

	while (i < length) {
		for (; i < length; i++) {
			if (*buf)
				break;
			buf++;
		}
		/**
		 * check device uevent from kernel side, no need to check
		 * uevent from udev.
		 */
		if (!strncmp(buf, "libudev", 7)) {
			buf += 7;
			i += 7;
			return -1;
		}
		if (!strncmp(buf, "ACTION=", 7)) {
			buf += 7;
			i += 7;
			strlcpy(action, buf, sizeof(action));
		} else if (!strncmp(buf, "SUBSYSTEM=", 10)) {
			buf += 10;
			i += 10;
			strlcpy(subsystem, buf, sizeof(subsystem));
		} else if (!strncmp(buf, "PCI_SLOT_NAME=", 14)) {
			buf += 14;
			i += 14;
			strlcpy(pci_slot_name, buf, sizeof(subsystem));
			event->devname = strdup(pci_slot_name);
		}
		for (; i < length; i++) {
			if (*buf == '\0')
				break;
			buf++;
		}
	}

	/* parse the subsystem layer */
	if (!strncmp(subsystem, "uio", 3))
		event->subsystem = EAL_DEV_EVENT_SUBSYSTEM_UIO;
	else if (!strncmp(subsystem, "pci", 3))
		event->subsystem = EAL_DEV_EVENT_SUBSYSTEM_PCI;
	else if (!strncmp(subsystem, "vfio", 4))
		event->subsystem = EAL_DEV_EVENT_SUBSYSTEM_VFIO;
	else
		goto err;

	/* parse the action type */
	if (!strncmp(action, "add", 3))
		event->type = RTE_DEV_EVENT_ADD;
	else if (!strncmp(action, "remove", 6))
		event->type = RTE_DEV_EVENT_REMOVE;
	else
		goto err;
	return 0;
err:
	free(event->devname);
	return -1;
}

static void
dev_delayed_unregister(void *param)
{
	rte_intr_callback_unregister(&intr_handle, dev_uev_handler, param);
	close(intr_handle.fd);
	intr_handle.fd = -1;
}

static void
dev_uev_handler(__rte_unused void *param)
{
	struct rte_dev_event uevent;
	int ret;
	char buf[EAL_UEV_MSG_LEN];
	struct rte_bus *bus;
	struct rte_device *dev;
	const char *busname = "";

	memset(&uevent, 0, sizeof(struct rte_dev_event));
	memset(buf, 0, EAL_UEV_MSG_LEN);

	ret = recv(intr_handle.fd, buf, EAL_UEV_MSG_LEN, MSG_DONTWAIT);
	if (ret < 0 && errno == EAGAIN)
		return;
	else if (ret <= 0) {
		/* connection is closed or broken, can not up again. */
		RTE_LOG(ERR, EAL, "uevent socket connection is broken.\n");
		rte_eal_alarm_set(1, dev_delayed_unregister, NULL);
		return;
	}

	ret = dev_uev_parse(buf, &uevent, EAL_UEV_MSG_LEN);
	if (ret < 0) {
		RTE_LOG(DEBUG, EAL, "Ignoring uevent '%s'\n", buf);
		return;
	}

	RTE_LOG(DEBUG, EAL, "receive uevent(name:%s, type:%d, subsystem:%d)\n",
		uevent.devname, uevent.type, uevent.subsystem);

	switch (uevent.subsystem) {
	case EAL_DEV_EVENT_SUBSYSTEM_PCI:
	case EAL_DEV_EVENT_SUBSYSTEM_UIO:
		busname = "pci";
		break;
	default:
		break;
	}

	if (uevent.devname) {
		if (uevent.type == RTE_DEV_EVENT_REMOVE && hotplug_handle) {
			rte_spinlock_lock(&failure_handle_lock);
			bus = rte_bus_find_by_name(busname);
			if (bus == NULL) {
				RTE_LOG(ERR, EAL, "Cannot find bus (%s)\n",
					busname);
				goto failure_handle_err;
			}

			dev = bus->find_device(NULL, cmp_dev_name,
					       uevent.devname);
			if (dev == NULL) {
				RTE_LOG(ERR, EAL, "Cannot find device (%s) on "
					"bus (%s)\n", uevent.devname, busname);
				goto failure_handle_err;
			}

			ret = bus->hot_unplug_handler(dev);
			if (ret) {
				RTE_LOG(ERR, EAL, "Can not handle hot-unplug "
					"for device (%s)\n", dev->name);
			}
			rte_spinlock_unlock(&failure_handle_lock);
		}
		rte_dev_event_callback_process(uevent.devname, uevent.type);
		free(uevent.devname);
	}

	return;

failure_handle_err:
	rte_spinlock_unlock(&failure_handle_lock);
	free(uevent.devname);
}

int
rte_dev_event_monitor_start(void)
{
	int ret;

	if (monitor_started)
		return 0;

	ret = dev_uev_socket_fd_create();
	if (ret) {
		RTE_LOG(ERR, EAL, "error create device event fd.\n");
		return -1;
	}

	intr_handle.type = RTE_INTR_HANDLE_DEV_EVENT;
	ret = rte_intr_callback_register(&intr_handle, dev_uev_handler, NULL);

	if (ret) {
		RTE_LOG(ERR, EAL, "fail to register uevent callback.\n");
		return -1;
	}

	monitor_started = true;

	return 0;
}

int
rte_dev_event_monitor_stop(void)
{
	int ret;

	if (!monitor_started)
		return 0;

	ret = rte_intr_callback_unregister(&intr_handle, dev_uev_handler,
					   (void *)-1);
	if (ret < 0) {
		RTE_LOG(ERR, EAL, "fail to unregister uevent callback.\n");
		return ret;
	}

	close(intr_handle.fd);
	intr_handle.fd = -1;
	monitor_started = false;

	return 0;
}

int
dev_sigbus_handler_register(void)
{
	sigset_t mask;
	struct sigaction action;

	rte_errno = 0;

	if (sigbus_need_recover)
		return 0;

	sigemptyset(&mask);
	sigaddset(&mask, SIGBUS);
	action.sa_flags = SA_SIGINFO;
	action.sa_mask = mask;
	action.sa_sigaction = sigbus_handler;
	sigbus_need_recover = !sigaction(SIGBUS, &action, &sigbus_action_old);

	return rte_errno;
}

int
dev_sigbus_handler_unregister(void)
{
	rte_errno = 0;

	sigbus_action_recover();

	return rte_errno;
}

int
rte_dev_hotplug_handle_enable(void)
{
	int ret = 0;

	ret = dev_sigbus_handler_register();
	if (ret < 0)
		RTE_LOG(ERR, EAL,
			"fail to register sigbus handler for devices.\n");

	hotplug_handle = true;

	return ret;
}

int
rte_dev_hotplug_handle_disable(void)
{
	int ret = 0;

	ret = dev_sigbus_handler_unregister();
	if (ret < 0)
		RTE_LOG(ERR, EAL,
			"fail to unregister sigbus handler for devices.\n");

	hotplug_handle = false;

	return ret;
}
