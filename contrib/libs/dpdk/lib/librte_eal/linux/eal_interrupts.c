#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/queue.h>
#include <stdarg.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/ioctl.h>
#include <sys/eventfd.h>
#include <assert.h>
#include <stdbool.h>

#include <rte_common.h>
#include <rte_interrupts.h>
#include <rte_memory.h>
#include <rte_launch.h>
#include <rte_eal.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_branch_prediction.h>
#include <rte_debug.h>
#include <rte_log.h>
#include <rte_errno.h>
#include <rte_spinlock.h>
#include <rte_pause.h>
#include <rte_vfio.h>
#include <rte_eal_trace.h>

#include "eal_private.h"
#include "eal_vfio.h"
#include "eal_thread.h"

#define EAL_INTR_EPOLL_WAIT_FOREVER (-1)
#define NB_OTHER_INTR               1

static RTE_DEFINE_PER_LCORE(int, _epfd) = -1; /**< epoll fd per thread */

/**
 * union for pipe fds.
 */
union intr_pipefds{
	struct {
		int pipefd[2];
	};
	struct {
		int readfd;
		int writefd;
	};
};

/**
 * union buffer for reading on different devices
 */
union rte_intr_read_buffer {
	int uio_intr_count;              /* for uio device */
#ifdef VFIO_PRESENT
	uint64_t vfio_intr_count;        /* for vfio device */
#endif
	uint64_t timerfd_num;            /* for timerfd */
	char charbuf[16];                /* for others */
};

TAILQ_HEAD(rte_intr_cb_list, rte_intr_callback);
TAILQ_HEAD(rte_intr_source_list, rte_intr_source);

struct rte_intr_callback {
	TAILQ_ENTRY(rte_intr_callback) next;
	rte_intr_callback_fn cb_fn;  /**< callback address */
	void *cb_arg;                /**< parameter for callback */
	uint8_t pending_delete;      /**< delete after callback is called */
	rte_intr_unregister_callback_fn ucb_fn; /**< fn to call before cb is deleted */
};

struct rte_intr_source {
	TAILQ_ENTRY(rte_intr_source) next;
	struct rte_intr_handle intr_handle; /**< interrupt handle */
	struct rte_intr_cb_list callbacks;  /**< user callbacks */
	uint32_t active;
};

/* global spinlock for interrupt data operation */
static rte_spinlock_t intr_lock = RTE_SPINLOCK_INITIALIZER;

/* union buffer for pipe read/write */
static union intr_pipefds intr_pipe;

/* interrupt sources list */
static struct rte_intr_source_list intr_sources;

/* interrupt handling thread */
static pthread_t intr_thread;

/* VFIO interrupts */
#ifdef VFIO_PRESENT

#define IRQ_SET_BUF_LEN  (sizeof(struct vfio_irq_set) + sizeof(int))
/* irq set buffer length for queue interrupts and LSC interrupt */
#define MSIX_IRQ_SET_BUF_LEN (sizeof(struct vfio_irq_set) + \
			      sizeof(int) * (RTE_MAX_RXTX_INTR_VEC_ID + 1))

/* enable legacy (INTx) interrupts */
static int
vfio_enable_intx(const struct rte_intr_handle *intr_handle) {
	struct vfio_irq_set *irq_set;
	char irq_set_buf[IRQ_SET_BUF_LEN];
	int len, ret;
	int *fd_ptr;

	len = sizeof(irq_set_buf);

	/* enable INTx */
	irq_set = (struct vfio_irq_set *) irq_set_buf;
	irq_set->argsz = len;
	irq_set->count = 1;
	irq_set->flags = VFIO_IRQ_SET_DATA_EVENTFD | VFIO_IRQ_SET_ACTION_TRIGGER;
	irq_set->index = VFIO_PCI_INTX_IRQ_INDEX;
	irq_set->start = 0;
	fd_ptr = (int *) &irq_set->data;
	*fd_ptr = intr_handle->fd;

	ret = ioctl(intr_handle->vfio_dev_fd, VFIO_DEVICE_SET_IRQS, irq_set);

	if (ret) {
		RTE_LOG(ERR, EAL, "Error enabling INTx interrupts for fd %d\n",
						intr_handle->fd);
		return -1;
	}

	/* unmask INTx after enabling */
	memset(irq_set, 0, len);
	len = sizeof(struct vfio_irq_set);
	irq_set->argsz = len;
	irq_set->count = 1;
	irq_set->flags = VFIO_IRQ_SET_DATA_NONE | VFIO_IRQ_SET_ACTION_UNMASK;
	irq_set->index = VFIO_PCI_INTX_IRQ_INDEX;
	irq_set->start = 0;

	ret = ioctl(intr_handle->vfio_dev_fd, VFIO_DEVICE_SET_IRQS, irq_set);

	if (ret) {
		RTE_LOG(ERR, EAL, "Error unmasking INTx interrupts for fd %d\n",
						intr_handle->fd);
		return -1;
	}
	return 0;
}

/* disable legacy (INTx) interrupts */
static int
vfio_disable_intx(const struct rte_intr_handle *intr_handle) {
	struct vfio_irq_set *irq_set;
	char irq_set_buf[IRQ_SET_BUF_LEN];
	int len, ret;

	len = sizeof(struct vfio_irq_set);

	/* mask interrupts before disabling */
	irq_set = (struct vfio_irq_set *) irq_set_buf;
	irq_set->argsz = len;
	irq_set->count = 1;
	irq_set->flags = VFIO_IRQ_SET_DATA_NONE | VFIO_IRQ_SET_ACTION_MASK;
	irq_set->index = VFIO_PCI_INTX_IRQ_INDEX;
	irq_set->start = 0;

	ret = ioctl(intr_handle->vfio_dev_fd, VFIO_DEVICE_SET_IRQS, irq_set);

	if (ret) {
		RTE_LOG(ERR, EAL, "Error masking INTx interrupts for fd %d\n",
						intr_handle->fd);
		return -1;
	}

	/* disable INTx*/
	memset(irq_set, 0, len);
	irq_set->argsz = len;
	irq_set->count = 0;
	irq_set->flags = VFIO_IRQ_SET_DATA_NONE | VFIO_IRQ_SET_ACTION_TRIGGER;
	irq_set->index = VFIO_PCI_INTX_IRQ_INDEX;
	irq_set->start = 0;

	ret = ioctl(intr_handle->vfio_dev_fd, VFIO_DEVICE_SET_IRQS, irq_set);

	if (ret) {
		RTE_LOG(ERR, EAL,
			"Error disabling INTx interrupts for fd %d\n", intr_handle->fd);
		return -1;
	}
	return 0;
}

/* unmask/ack legacy (INTx) interrupts */
static int
vfio_ack_intx(const struct rte_intr_handle *intr_handle)
{
	struct vfio_irq_set irq_set;

	/* unmask INTx */
	memset(&irq_set, 0, sizeof(irq_set));
	irq_set.argsz = sizeof(irq_set);
	irq_set.count = 1;
	irq_set.flags = VFIO_IRQ_SET_DATA_NONE | VFIO_IRQ_SET_ACTION_UNMASK;
	irq_set.index = VFIO_PCI_INTX_IRQ_INDEX;
	irq_set.start = 0;

	if (ioctl(intr_handle->vfio_dev_fd, VFIO_DEVICE_SET_IRQS, &irq_set)) {
		RTE_LOG(ERR, EAL, "Error unmasking INTx interrupts for fd %d\n",
			intr_handle->fd);
		return -1;
	}
	return 0;
}

/* enable MSI interrupts */
static int
vfio_enable_msi(const struct rte_intr_handle *intr_handle) {
	int len, ret;
	char irq_set_buf[IRQ_SET_BUF_LEN];
	struct vfio_irq_set *irq_set;
	int *fd_ptr;

	len = sizeof(irq_set_buf);

	irq_set = (struct vfio_irq_set *) irq_set_buf;
	irq_set->argsz = len;
	irq_set->count = 1;
	irq_set->flags = VFIO_IRQ_SET_DATA_EVENTFD | VFIO_IRQ_SET_ACTION_TRIGGER;
	irq_set->index = VFIO_PCI_MSI_IRQ_INDEX;
	irq_set->start = 0;
	fd_ptr = (int *) &irq_set->data;
	*fd_ptr = intr_handle->fd;

	ret = ioctl(intr_handle->vfio_dev_fd, VFIO_DEVICE_SET_IRQS, irq_set);

	if (ret) {
		RTE_LOG(ERR, EAL, "Error enabling MSI interrupts for fd %d\n",
						intr_handle->fd);
		return -1;
	}
	return 0;
}

/* disable MSI interrupts */
static int
vfio_disable_msi(const struct rte_intr_handle *intr_handle) {
	struct vfio_irq_set *irq_set;
	char irq_set_buf[IRQ_SET_BUF_LEN];
	int len, ret;

	len = sizeof(struct vfio_irq_set);

	irq_set = (struct vfio_irq_set *) irq_set_buf;
	irq_set->argsz = len;
	irq_set->count = 0;
	irq_set->flags = VFIO_IRQ_SET_DATA_NONE | VFIO_IRQ_SET_ACTION_TRIGGER;
	irq_set->index = VFIO_PCI_MSI_IRQ_INDEX;
	irq_set->start = 0;

	ret = ioctl(intr_handle->vfio_dev_fd, VFIO_DEVICE_SET_IRQS, irq_set);

	if (ret)
		RTE_LOG(ERR, EAL,
			"Error disabling MSI interrupts for fd %d\n", intr_handle->fd);

	return ret;
}

/* enable MSI-X interrupts */
static int
vfio_enable_msix(const struct rte_intr_handle *intr_handle) {
	int len, ret;
	char irq_set_buf[MSIX_IRQ_SET_BUF_LEN];
	struct vfio_irq_set *irq_set;
	int *fd_ptr;

	len = sizeof(irq_set_buf);

	irq_set = (struct vfio_irq_set *) irq_set_buf;
	irq_set->argsz = len;
	/* 0 < irq_set->count < RTE_MAX_RXTX_INTR_VEC_ID + 1 */
	irq_set->count = intr_handle->max_intr ?
		(intr_handle->max_intr > RTE_MAX_RXTX_INTR_VEC_ID + 1 ?
		RTE_MAX_RXTX_INTR_VEC_ID + 1 : intr_handle->max_intr) : 1;
	irq_set->flags = VFIO_IRQ_SET_DATA_EVENTFD | VFIO_IRQ_SET_ACTION_TRIGGER;
	irq_set->index = VFIO_PCI_MSIX_IRQ_INDEX;
	irq_set->start = 0;
	fd_ptr = (int *) &irq_set->data;
	/* INTR vector offset 0 reserve for non-efds mapping */
	fd_ptr[RTE_INTR_VEC_ZERO_OFFSET] = intr_handle->fd;
	memcpy(&fd_ptr[RTE_INTR_VEC_RXTX_OFFSET], intr_handle->efds,
		sizeof(*intr_handle->efds) * intr_handle->nb_efd);

	ret = ioctl(intr_handle->vfio_dev_fd, VFIO_DEVICE_SET_IRQS, irq_set);

	if (ret) {
		RTE_LOG(ERR, EAL, "Error enabling MSI-X interrupts for fd %d\n",
						intr_handle->fd);
		return -1;
	}

	return 0;
}

/* disable MSI-X interrupts */
static int
vfio_disable_msix(const struct rte_intr_handle *intr_handle) {
	struct vfio_irq_set *irq_set;
	char irq_set_buf[MSIX_IRQ_SET_BUF_LEN];
	int len, ret;

	len = sizeof(struct vfio_irq_set);

	irq_set = (struct vfio_irq_set *) irq_set_buf;
	irq_set->argsz = len;
	irq_set->count = 0;
	irq_set->flags = VFIO_IRQ_SET_DATA_NONE | VFIO_IRQ_SET_ACTION_TRIGGER;
	irq_set->index = VFIO_PCI_MSIX_IRQ_INDEX;
	irq_set->start = 0;

	ret = ioctl(intr_handle->vfio_dev_fd, VFIO_DEVICE_SET_IRQS, irq_set);

	if (ret)
		RTE_LOG(ERR, EAL,
			"Error disabling MSI-X interrupts for fd %d\n", intr_handle->fd);

	return ret;
}

#ifdef HAVE_VFIO_DEV_REQ_INTERFACE
/* enable req notifier */
static int
vfio_enable_req(const struct rte_intr_handle *intr_handle)
{
	int len, ret;
	char irq_set_buf[IRQ_SET_BUF_LEN];
	struct vfio_irq_set *irq_set;
	int *fd_ptr;

	len = sizeof(irq_set_buf);

	irq_set = (struct vfio_irq_set *) irq_set_buf;
	irq_set->argsz = len;
	irq_set->count = 1;
	irq_set->flags = VFIO_IRQ_SET_DATA_EVENTFD |
			 VFIO_IRQ_SET_ACTION_TRIGGER;
	irq_set->index = VFIO_PCI_REQ_IRQ_INDEX;
	irq_set->start = 0;
	fd_ptr = (int *) &irq_set->data;
	*fd_ptr = intr_handle->fd;

	ret = ioctl(intr_handle->vfio_dev_fd, VFIO_DEVICE_SET_IRQS, irq_set);

	if (ret) {
		RTE_LOG(ERR, EAL, "Error enabling req interrupts for fd %d\n",
						intr_handle->fd);
		return -1;
	}

	return 0;
}

/* disable req notifier */
static int
vfio_disable_req(const struct rte_intr_handle *intr_handle)
{
	struct vfio_irq_set *irq_set;
	char irq_set_buf[IRQ_SET_BUF_LEN];
	int len, ret;

	len = sizeof(struct vfio_irq_set);

	irq_set = (struct vfio_irq_set *) irq_set_buf;
	irq_set->argsz = len;
	irq_set->count = 0;
	irq_set->flags = VFIO_IRQ_SET_DATA_NONE | VFIO_IRQ_SET_ACTION_TRIGGER;
	irq_set->index = VFIO_PCI_REQ_IRQ_INDEX;
	irq_set->start = 0;

	ret = ioctl(intr_handle->vfio_dev_fd, VFIO_DEVICE_SET_IRQS, irq_set);

	if (ret)
		RTE_LOG(ERR, EAL, "Error disabling req interrupts for fd %d\n",
			intr_handle->fd);

	return ret;
}
#endif
#endif

static int
uio_intx_intr_disable(const struct rte_intr_handle *intr_handle)
{
	unsigned char command_high;

	/* use UIO config file descriptor for uio_pci_generic */
	if (pread(intr_handle->uio_cfg_fd, &command_high, 1, 5) != 1) {
		RTE_LOG(ERR, EAL,
			"Error reading interrupts status for fd %d\n",
			intr_handle->uio_cfg_fd);
		return -1;
	}
	/* disable interrupts */
	command_high |= 0x4;
	if (pwrite(intr_handle->uio_cfg_fd, &command_high, 1, 5) != 1) {
		RTE_LOG(ERR, EAL,
			"Error disabling interrupts for fd %d\n",
			intr_handle->uio_cfg_fd);
		return -1;
	}

	return 0;
}

static int
uio_intx_intr_enable(const struct rte_intr_handle *intr_handle)
{
	unsigned char command_high;

	/* use UIO config file descriptor for uio_pci_generic */
	if (pread(intr_handle->uio_cfg_fd, &command_high, 1, 5) != 1) {
		RTE_LOG(ERR, EAL,
			"Error reading interrupts status for fd %d\n",
			intr_handle->uio_cfg_fd);
		return -1;
	}
	/* enable interrupts */
	command_high &= ~0x4;
	if (pwrite(intr_handle->uio_cfg_fd, &command_high, 1, 5) != 1) {
		RTE_LOG(ERR, EAL,
			"Error enabling interrupts for fd %d\n",
			intr_handle->uio_cfg_fd);
		return -1;
	}

	return 0;
}

static int
uio_intr_disable(const struct rte_intr_handle *intr_handle)
{
	const int value = 0;

	if (write(intr_handle->fd, &value, sizeof(value)) < 0) {
		RTE_LOG(ERR, EAL,
			"Error disabling interrupts for fd %d (%s)\n",
			intr_handle->fd, strerror(errno));
		return -1;
	}
	return 0;
}

static int
uio_intr_enable(const struct rte_intr_handle *intr_handle)
{
	const int value = 1;

	if (write(intr_handle->fd, &value, sizeof(value)) < 0) {
		RTE_LOG(ERR, EAL,
			"Error enabling interrupts for fd %d (%s)\n",
			intr_handle->fd, strerror(errno));
		return -1;
	}
	return 0;
}

int
rte_intr_callback_register(const struct rte_intr_handle *intr_handle,
			rte_intr_callback_fn cb, void *cb_arg)
{
	int ret, wake_thread;
	struct rte_intr_source *src;
	struct rte_intr_callback *callback;

	wake_thread = 0;

	/* first do parameter checking */
	if (intr_handle == NULL || intr_handle->fd < 0 || cb == NULL) {
		RTE_LOG(ERR, EAL,
			"Registering with invalid input parameter\n");
		return -EINVAL;
	}

	/* allocate a new interrupt callback entity */
	callback = calloc(1, sizeof(*callback));
	if (callback == NULL) {
		RTE_LOG(ERR, EAL, "Can not allocate memory\n");
		return -ENOMEM;
	}
	callback->cb_fn = cb;
	callback->cb_arg = cb_arg;
	callback->pending_delete = 0;
	callback->ucb_fn = NULL;

	rte_spinlock_lock(&intr_lock);

	/* check if there is at least one callback registered for the fd */
	TAILQ_FOREACH(src, &intr_sources, next) {
		if (src->intr_handle.fd == intr_handle->fd) {
			/* we had no interrupts for this */
			if (TAILQ_EMPTY(&src->callbacks))
				wake_thread = 1;

			TAILQ_INSERT_TAIL(&(src->callbacks), callback, next);
			ret = 0;
			break;
		}
	}

	/* no existing callbacks for this - add new source */
	if (src == NULL) {
		src = calloc(1, sizeof(*src));
		if (src == NULL) {
			RTE_LOG(ERR, EAL, "Can not allocate memory\n");
			free(callback);
			ret = -ENOMEM;
		} else {
			src->intr_handle = *intr_handle;
			TAILQ_INIT(&src->callbacks);
			TAILQ_INSERT_TAIL(&(src->callbacks), callback, next);
			TAILQ_INSERT_TAIL(&intr_sources, src, next);
			wake_thread = 1;
			ret = 0;
		}
	}

	rte_spinlock_unlock(&intr_lock);

	/**
	 * check if need to notify the pipe fd waited by epoll_wait to
	 * rebuild the wait list.
	 */
	if (wake_thread)
		if (write(intr_pipe.writefd, "1", 1) < 0)
			ret = -EPIPE;

	rte_eal_trace_intr_callback_register(intr_handle, cb, cb_arg, ret);
	return ret;
}

int
rte_intr_callback_unregister_pending(const struct rte_intr_handle *intr_handle,
				rte_intr_callback_fn cb_fn, void *cb_arg,
				rte_intr_unregister_callback_fn ucb_fn)
{
	int ret;
	struct rte_intr_source *src;
	struct rte_intr_callback *cb, *next;

	/* do parameter checking first */
	if (intr_handle == NULL || intr_handle->fd < 0) {
		RTE_LOG(ERR, EAL,
		"Unregistering with invalid input parameter\n");
		return -EINVAL;
	}

	rte_spinlock_lock(&intr_lock);

	/* check if the insterrupt source for the fd is existent */
	TAILQ_FOREACH(src, &intr_sources, next)
		if (src->intr_handle.fd == intr_handle->fd)
			break;

	/* No interrupt source registered for the fd */
	if (src == NULL) {
		ret = -ENOENT;

	/* only usable if the source is active */
	} else if (src->active == 0) {
		ret = -EAGAIN;

	} else {
		ret = 0;

		/* walk through the callbacks and mark all that match. */
		for (cb = TAILQ_FIRST(&src->callbacks); cb != NULL; cb = next) {
			next = TAILQ_NEXT(cb, next);
			if (cb->cb_fn == cb_fn && (cb_arg == (void *)-1 ||
					cb->cb_arg == cb_arg)) {
				cb->pending_delete = 1;
				cb->ucb_fn = ucb_fn;
				ret++;
			}
		}
	}

	rte_spinlock_unlock(&intr_lock);

	return ret;
}

int
rte_intr_callback_unregister(const struct rte_intr_handle *intr_handle,
			rte_intr_callback_fn cb_fn, void *cb_arg)
{
	int ret;
	struct rte_intr_source *src;
	struct rte_intr_callback *cb, *next;

	/* do parameter checking first */
	if (intr_handle == NULL || intr_handle->fd < 0) {
		RTE_LOG(ERR, EAL,
		"Unregistering with invalid input parameter\n");
		return -EINVAL;
	}

	rte_spinlock_lock(&intr_lock);

	/* check if the insterrupt source for the fd is existent */
	TAILQ_FOREACH(src, &intr_sources, next)
		if (src->intr_handle.fd == intr_handle->fd)
			break;

	/* No interrupt source registered for the fd */
	if (src == NULL) {
		ret = -ENOENT;

	/* interrupt source has some active callbacks right now. */
	} else if (src->active != 0) {
		ret = -EAGAIN;

	/* ok to remove. */
	} else {
		ret = 0;

		/*walk through the callbacks and remove all that match. */
		for (cb = TAILQ_FIRST(&src->callbacks); cb != NULL; cb = next) {

			next = TAILQ_NEXT(cb, next);

			if (cb->cb_fn == cb_fn && (cb_arg == (void *)-1 ||
					cb->cb_arg == cb_arg)) {
				TAILQ_REMOVE(&src->callbacks, cb, next);
				free(cb);
				ret++;
			}
		}

		/* all callbacks for that source are removed. */
		if (TAILQ_EMPTY(&src->callbacks)) {
			TAILQ_REMOVE(&intr_sources, src, next);
			free(src);
		}
	}

	rte_spinlock_unlock(&intr_lock);

	/* notify the pipe fd waited by epoll_wait to rebuild the wait list */
	if (ret >= 0 && write(intr_pipe.writefd, "1", 1) < 0) {
		ret = -EPIPE;
	}

	rte_eal_trace_intr_callback_unregister(intr_handle, cb_fn, cb_arg,
		ret);
	return ret;
}

int
rte_intr_enable(const struct rte_intr_handle *intr_handle)
{
	int rc = 0;

	if (intr_handle == NULL)
		return -1;

	if (intr_handle->type == RTE_INTR_HANDLE_VDEV) {
		rc = 0;
		goto out;
	}

	if (intr_handle->fd < 0 || intr_handle->uio_cfg_fd < 0) {
		rc = -1;
		goto out;
	}

	switch (intr_handle->type){
	/* write to the uio fd to enable the interrupt */
	case RTE_INTR_HANDLE_UIO:
		if (uio_intr_enable(intr_handle))
			rc = -1;
		break;
	case RTE_INTR_HANDLE_UIO_INTX:
		if (uio_intx_intr_enable(intr_handle))
			rc = -1;
		break;
	/* not used at this moment */
	case RTE_INTR_HANDLE_ALARM:
		rc = -1;
		break;
#ifdef VFIO_PRESENT
	case RTE_INTR_HANDLE_VFIO_MSIX:
		if (vfio_enable_msix(intr_handle))
			rc = -1;
		break;
	case RTE_INTR_HANDLE_VFIO_MSI:
		if (vfio_enable_msi(intr_handle))
			rc = -1;
		break;
	case RTE_INTR_HANDLE_VFIO_LEGACY:
		if (vfio_enable_intx(intr_handle))
			rc = -1;
		break;
#ifdef HAVE_VFIO_DEV_REQ_INTERFACE
	case RTE_INTR_HANDLE_VFIO_REQ:
		if (vfio_enable_req(intr_handle))
			rc = -1;
		break;
#endif
#endif
	/* not used at this moment */
	case RTE_INTR_HANDLE_DEV_EVENT:
		rc = -1;
		break;
	/* unknown handle type */
	default:
		RTE_LOG(ERR, EAL,
			"Unknown handle type of fd %d\n",
					intr_handle->fd);
		rc = -1;
		break;
	}
out:
	rte_eal_trace_intr_enable(intr_handle, rc);
	return rc;
}

/**
 * PMD generally calls this function at the end of its IRQ callback.
 * Internally, it unmasks the interrupt if possible.
 *
 * For INTx, unmasking is required as the interrupt is auto-masked prior to
 * invoking callback.
 *
 * For MSI/MSI-X, unmasking is typically not needed as the interrupt is not
 * auto-masked. In fact, for interrupt handle types VFIO_MSIX and VFIO_MSI,
 * this function is no-op.
 */
int
rte_intr_ack(const struct rte_intr_handle *intr_handle)
{
	if (intr_handle && intr_handle->type == RTE_INTR_HANDLE_VDEV)
		return 0;

	if (!intr_handle || intr_handle->fd < 0 || intr_handle->uio_cfg_fd < 0)
		return -1;

	switch (intr_handle->type) {
	/* Both acking and enabling are same for UIO */
	case RTE_INTR_HANDLE_UIO:
		if (uio_intr_enable(intr_handle))
			return -1;
		break;
	case RTE_INTR_HANDLE_UIO_INTX:
		if (uio_intx_intr_enable(intr_handle))
			return -1;
		break;
	/* not used at this moment */
	case RTE_INTR_HANDLE_ALARM:
		return -1;
#ifdef VFIO_PRESENT
	/* VFIO MSI* is implicitly acked unlike INTx, nothing to do */
	case RTE_INTR_HANDLE_VFIO_MSIX:
	case RTE_INTR_HANDLE_VFIO_MSI:
		return 0;
	case RTE_INTR_HANDLE_VFIO_LEGACY:
		if (vfio_ack_intx(intr_handle))
			return -1;
		break;
#ifdef HAVE_VFIO_DEV_REQ_INTERFACE
	case RTE_INTR_HANDLE_VFIO_REQ:
		return -1;
#endif
#endif
	/* not used at this moment */
	case RTE_INTR_HANDLE_DEV_EVENT:
		return -1;
	/* unknown handle type */
	default:
		RTE_LOG(ERR, EAL, "Unknown handle type of fd %d\n",
			intr_handle->fd);
		return -1;
	}

	return 0;
}

int
rte_intr_disable(const struct rte_intr_handle *intr_handle)
{
	int rc = 0;

	if (intr_handle == NULL)
		return -1;

	if (intr_handle->type == RTE_INTR_HANDLE_VDEV) {
		rc = 0;
		goto out;
	}

	if (intr_handle->fd < 0 || intr_handle->uio_cfg_fd < 0) {
		rc = -1;
		goto out;
	}

	switch (intr_handle->type){
	/* write to the uio fd to disable the interrupt */
	case RTE_INTR_HANDLE_UIO:
		if (uio_intr_disable(intr_handle))
			rc = -1;
		break;
	case RTE_INTR_HANDLE_UIO_INTX:
		if (uio_intx_intr_disable(intr_handle))
			rc = -1;
		break;
	/* not used at this moment */
	case RTE_INTR_HANDLE_ALARM:
		rc = -1;
		break;
#ifdef VFIO_PRESENT
	case RTE_INTR_HANDLE_VFIO_MSIX:
		if (vfio_disable_msix(intr_handle))
			rc = -1;
		break;
	case RTE_INTR_HANDLE_VFIO_MSI:
		if (vfio_disable_msi(intr_handle))
			rc = -1;
		break;
	case RTE_INTR_HANDLE_VFIO_LEGACY:
		if (vfio_disable_intx(intr_handle))
			rc = -1;
		break;
#ifdef HAVE_VFIO_DEV_REQ_INTERFACE
	case RTE_INTR_HANDLE_VFIO_REQ:
		if (vfio_disable_req(intr_handle))
			rc = -1;
		break;
#endif
#endif
	/* not used at this moment */
	case RTE_INTR_HANDLE_DEV_EVENT:
		rc = -1;
		break;
	/* unknown handle type */
	default:
		RTE_LOG(ERR, EAL,
			"Unknown handle type of fd %d\n",
					intr_handle->fd);
		rc = -1;
		break;
	}
out:
	rte_eal_trace_intr_disable(intr_handle, rc);
	return rc;
}

static int
eal_intr_process_interrupts(struct epoll_event *events, int nfds)
{
	bool call = false;
	int n, bytes_read, rv;
	struct rte_intr_source *src;
	struct rte_intr_callback *cb, *next;
	union rte_intr_read_buffer buf;
	struct rte_intr_callback active_cb;

	for (n = 0; n < nfds; n++) {

		/**
		 * if the pipe fd is ready to read, return out to
		 * rebuild the wait list.
		 */
		if (events[n].data.fd == intr_pipe.readfd){
			int r = read(intr_pipe.readfd, buf.charbuf,
					sizeof(buf.charbuf));
			RTE_SET_USED(r);
			return -1;
		}
		rte_spinlock_lock(&intr_lock);
		TAILQ_FOREACH(src, &intr_sources, next)
			if (src->intr_handle.fd ==
					events[n].data.fd)
				break;
		if (src == NULL){
			rte_spinlock_unlock(&intr_lock);
			continue;
		}

		/* mark this interrupt source as active and release the lock. */
		src->active = 1;
		rte_spinlock_unlock(&intr_lock);

		/* set the length to be read dor different handle type */
		switch (src->intr_handle.type) {
		case RTE_INTR_HANDLE_UIO:
		case RTE_INTR_HANDLE_UIO_INTX:
			bytes_read = sizeof(buf.uio_intr_count);
			break;
		case RTE_INTR_HANDLE_ALARM:
			bytes_read = sizeof(buf.timerfd_num);
			break;
#ifdef VFIO_PRESENT
		case RTE_INTR_HANDLE_VFIO_MSIX:
		case RTE_INTR_HANDLE_VFIO_MSI:
		case RTE_INTR_HANDLE_VFIO_LEGACY:
			bytes_read = sizeof(buf.vfio_intr_count);
			break;
#ifdef HAVE_VFIO_DEV_REQ_INTERFACE
		case RTE_INTR_HANDLE_VFIO_REQ:
			bytes_read = 0;
			call = true;
			break;
#endif
#endif
		case RTE_INTR_HANDLE_VDEV:
		case RTE_INTR_HANDLE_EXT:
			bytes_read = 0;
			call = true;
			break;
		case RTE_INTR_HANDLE_DEV_EVENT:
			bytes_read = 0;
			call = true;
			break;
		default:
			bytes_read = 1;
			break;
		}

		if (bytes_read > 0) {
			/**
			 * read out to clear the ready-to-be-read flag
			 * for epoll_wait.
			 */
			bytes_read = read(events[n].data.fd, &buf, bytes_read);
			if (bytes_read < 0) {
				if (errno == EINTR || errno == EWOULDBLOCK)
					continue;

				RTE_LOG(ERR, EAL, "Error reading from file "
					"descriptor %d: %s\n",
					events[n].data.fd,
					strerror(errno));
				/*
				 * The device is unplugged or buggy, remove
				 * it as an interrupt source and return to
				 * force the wait list to be rebuilt.
				 */
				rte_spinlock_lock(&intr_lock);
				TAILQ_REMOVE(&intr_sources, src, next);
				rte_spinlock_unlock(&intr_lock);

				for (cb = TAILQ_FIRST(&src->callbacks); cb;
							cb = next) {
					next = TAILQ_NEXT(cb, next);
					TAILQ_REMOVE(&src->callbacks, cb, next);
					free(cb);
				}
				free(src);
				return -1;
			} else if (bytes_read == 0)
				RTE_LOG(ERR, EAL, "Read nothing from file "
					"descriptor %d\n", events[n].data.fd);
			else
				call = true;
		}

		/* grab a lock, again to call callbacks and update status. */
		rte_spinlock_lock(&intr_lock);

		if (call) {

			/* Finally, call all callbacks. */
			TAILQ_FOREACH(cb, &src->callbacks, next) {

				/* make a copy and unlock. */
				active_cb = *cb;
				rte_spinlock_unlock(&intr_lock);

				/* call the actual callback */
				active_cb.cb_fn(active_cb.cb_arg);

				/*get the lock back. */
				rte_spinlock_lock(&intr_lock);
			}
		}
		/* we done with that interrupt source, release it. */
		src->active = 0;

		rv = 0;

		/* check if any callback are supposed to be removed */
		for (cb = TAILQ_FIRST(&src->callbacks); cb != NULL; cb = next) {
			next = TAILQ_NEXT(cb, next);
			if (cb->pending_delete) {
				TAILQ_REMOVE(&src->callbacks, cb, next);
				if (cb->ucb_fn)
					cb->ucb_fn(&src->intr_handle, cb->cb_arg);
				free(cb);
				rv++;
			}
		}

		/* all callbacks for that source are removed. */
		if (TAILQ_EMPTY(&src->callbacks)) {
			TAILQ_REMOVE(&intr_sources, src, next);
			free(src);
		}

		/* notify the pipe fd waited by epoll_wait to rebuild the wait list */
		if (rv > 0 && write(intr_pipe.writefd, "1", 1) < 0) {
			rte_spinlock_unlock(&intr_lock);
			return -EPIPE;
		}

		rte_spinlock_unlock(&intr_lock);
	}

	return 0;
}

/**
 * It handles all the interrupts.
 *
 * @param pfd
 *  epoll file descriptor.
 * @param totalfds
 *  The number of file descriptors added in epoll.
 *
 * @return
 *  void
 */
static void
eal_intr_handle_interrupts(int pfd, unsigned totalfds)
{
	struct epoll_event events[totalfds];
	int nfds = 0;

	for(;;) {
		nfds = epoll_wait(pfd, events, totalfds,
			EAL_INTR_EPOLL_WAIT_FOREVER);
		/* epoll_wait fail */
		if (nfds < 0) {
			if (errno == EINTR)
				continue;
			RTE_LOG(ERR, EAL,
				"epoll_wait returns with fail\n");
			return;
		}
		/* epoll_wait timeout, will never happens here */
		else if (nfds == 0)
			continue;
		/* epoll_wait has at least one fd ready to read */
		if (eal_intr_process_interrupts(events, nfds) < 0)
			return;
	}
}

/**
 * It builds/rebuilds up the epoll file descriptor with all the
 * file descriptors being waited on. Then handles the interrupts.
 *
 * @param arg
 *  pointer. (unused)
 *
 * @return
 *  never return;
 */
static __rte_noreturn void *
eal_intr_thread_main(__rte_unused void *arg)
{
	/* host thread, never break out */
	for (;;) {
		/* build up the epoll fd with all descriptors we are to
		 * wait on then pass it to the handle_interrupts function
		 */
		static struct epoll_event pipe_event = {
			.events = EPOLLIN | EPOLLPRI,
		};
		struct rte_intr_source *src;
		unsigned numfds = 0;

		/* create epoll fd */
		int pfd = epoll_create(1);
		if (pfd < 0)
			rte_panic("Cannot create epoll instance\n");

		pipe_event.data.fd = intr_pipe.readfd;
		/**
		 * add pipe fd into wait list, this pipe is used to
		 * rebuild the wait list.
		 */
		if (epoll_ctl(pfd, EPOLL_CTL_ADD, intr_pipe.readfd,
						&pipe_event) < 0) {
			rte_panic("Error adding fd to %d epoll_ctl, %s\n",
					intr_pipe.readfd, strerror(errno));
		}
		numfds++;

		rte_spinlock_lock(&intr_lock);

		TAILQ_FOREACH(src, &intr_sources, next) {
			struct epoll_event ev;

			if (src->callbacks.tqh_first == NULL)
				continue; /* skip those with no callbacks */
			memset(&ev, 0, sizeof(ev));
			ev.events = EPOLLIN | EPOLLPRI | EPOLLRDHUP | EPOLLHUP;
			ev.data.fd = src->intr_handle.fd;

			/**
			 * add all the uio device file descriptor
			 * into wait list.
			 */
			if (epoll_ctl(pfd, EPOLL_CTL_ADD,
					src->intr_handle.fd, &ev) < 0){
				rte_panic("Error adding fd %d epoll_ctl, %s\n",
					src->intr_handle.fd, strerror(errno));
			}
			else
				numfds++;
		}
		rte_spinlock_unlock(&intr_lock);
		/* serve the interrupt */
		eal_intr_handle_interrupts(pfd, numfds);

		/**
		 * when we return, we need to rebuild the
		 * list of fds to monitor.
		 */
		close(pfd);
	}
}

int
rte_eal_intr_init(void)
{
	int ret = 0;

	/* init the global interrupt source head */
	TAILQ_INIT(&intr_sources);

	/**
	 * create a pipe which will be waited by epoll and notified to
	 * rebuild the wait list of epoll.
	 */
	if (pipe(intr_pipe.pipefd) < 0) {
		rte_errno = errno;
		return -1;
	}

	/* create the host thread to wait/handle the interrupt */
	ret = rte_ctrl_thread_create(&intr_thread, "eal-intr-thread", NULL,
			eal_intr_thread_main, NULL);
	if (ret != 0) {
		rte_errno = -ret;
		RTE_LOG(ERR, EAL,
			"Failed to create thread for interrupt handling\n");
	}

	return ret;
}

static void
eal_intr_proc_rxtx_intr(int fd, const struct rte_intr_handle *intr_handle)
{
	union rte_intr_read_buffer buf;
	int bytes_read = 0;
	int nbytes;

	switch (intr_handle->type) {
	case RTE_INTR_HANDLE_UIO:
	case RTE_INTR_HANDLE_UIO_INTX:
		bytes_read = sizeof(buf.uio_intr_count);
		break;
#ifdef VFIO_PRESENT
	case RTE_INTR_HANDLE_VFIO_MSIX:
	case RTE_INTR_HANDLE_VFIO_MSI:
	case RTE_INTR_HANDLE_VFIO_LEGACY:
		bytes_read = sizeof(buf.vfio_intr_count);
		break;
#endif
	case RTE_INTR_HANDLE_VDEV:
		bytes_read = intr_handle->efd_counter_size;
		/* For vdev, number of bytes to read is set by driver */
		break;
	case RTE_INTR_HANDLE_EXT:
		return;
	default:
		bytes_read = 1;
		RTE_LOG(INFO, EAL, "unexpected intr type\n");
		break;
	}

	/**
	 * read out to clear the ready-to-be-read flag
	 * for epoll_wait.
	 */
	if (bytes_read == 0)
		return;
	do {
		nbytes = read(fd, &buf, bytes_read);
		if (nbytes < 0) {
			if (errno == EINTR || errno == EWOULDBLOCK ||
			    errno == EAGAIN)
				continue;
			RTE_LOG(ERR, EAL,
				"Error reading from fd %d: %s\n",
				fd, strerror(errno));
		} else if (nbytes == 0)
			RTE_LOG(ERR, EAL, "Read nothing from fd %d\n", fd);
		return;
	} while (1);
}

static int
eal_epoll_process_event(struct epoll_event *evs, unsigned int n,
			struct rte_epoll_event *events)
{
	unsigned int i, count = 0;
	struct rte_epoll_event *rev;
	uint32_t valid_status;

	for (i = 0; i < n; i++) {
		rev = evs[i].data.ptr;
		valid_status =  RTE_EPOLL_VALID;
		/* ACQUIRE memory ordering here pairs with RELEASE
		 * ordering below acting as a lock to synchronize
		 * the event data updating.
		 */
		if (!rev || !__atomic_compare_exchange_n(&rev->status,
				    &valid_status, RTE_EPOLL_EXEC, 0,
				    __ATOMIC_ACQUIRE, __ATOMIC_RELAXED))
			continue;

		events[count].status        = RTE_EPOLL_VALID;
		events[count].fd            = rev->fd;
		events[count].epfd          = rev->epfd;
		events[count].epdata.event  = rev->epdata.event;
		events[count].epdata.data   = rev->epdata.data;
		if (rev->epdata.cb_fun)
			rev->epdata.cb_fun(rev->fd,
					   rev->epdata.cb_arg);

		/* the status update should be observed after
		 * the other fields change.
		 */
		__atomic_store_n(&rev->status, RTE_EPOLL_VALID,
				__ATOMIC_RELEASE);
		count++;
	}
	return count;
}

static inline int
eal_init_tls_epfd(void)
{
	int pfd = epoll_create(255);

	if (pfd < 0) {
		RTE_LOG(ERR, EAL,
			"Cannot create epoll instance\n");
		return -1;
	}
	return pfd;
}

int
rte_intr_tls_epfd(void)
{
	if (RTE_PER_LCORE(_epfd) == -1)
		RTE_PER_LCORE(_epfd) = eal_init_tls_epfd();

	return RTE_PER_LCORE(_epfd);
}

static int
eal_epoll_wait(int epfd, struct rte_epoll_event *events,
	       int maxevents, int timeout, bool interruptible)
{
	struct epoll_event evs[maxevents];
	int rc;

	if (!events) {
		RTE_LOG(ERR, EAL, "rte_epoll_event can't be NULL\n");
		return -1;
	}

	/* using per thread epoll fd */
	if (epfd == RTE_EPOLL_PER_THREAD)
		epfd = rte_intr_tls_epfd();

	while (1) {
		rc = epoll_wait(epfd, evs, maxevents, timeout);
		if (likely(rc > 0)) {
			/* epoll_wait has at least one fd ready to read */
			rc = eal_epoll_process_event(evs, rc, events);
			break;
		} else if (rc < 0) {
			if (errno == EINTR) {
				if (interruptible)
					return -1;
				else
					continue;
			}
			/* epoll_wait fail */
			RTE_LOG(ERR, EAL, "epoll_wait returns with fail %s\n",
				strerror(errno));
			rc = -1;
			break;
		} else {
			/* rc == 0, epoll_wait timed out */
			break;
		}
	}

	return rc;
}

int
rte_epoll_wait(int epfd, struct rte_epoll_event *events,
	       int maxevents, int timeout)
{
	return eal_epoll_wait(epfd, events, maxevents, timeout, false);
}

int
rte_epoll_wait_interruptible(int epfd, struct rte_epoll_event *events,
			     int maxevents, int timeout)
{
	return eal_epoll_wait(epfd, events, maxevents, timeout, true);
}

static inline void
eal_epoll_data_safe_free(struct rte_epoll_event *ev)
{
	uint32_t valid_status = RTE_EPOLL_VALID;

	while (!__atomic_compare_exchange_n(&ev->status, &valid_status,
		    RTE_EPOLL_INVALID, 0, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
		while (__atomic_load_n(&ev->status,
				__ATOMIC_RELAXED) != RTE_EPOLL_VALID)
			rte_pause();
		valid_status = RTE_EPOLL_VALID;
	}
	memset(&ev->epdata, 0, sizeof(ev->epdata));
	ev->fd = -1;
	ev->epfd = -1;
}

int
rte_epoll_ctl(int epfd, int op, int fd,
	      struct rte_epoll_event *event)
{
	struct epoll_event ev;

	if (!event) {
		RTE_LOG(ERR, EAL, "rte_epoll_event can't be NULL\n");
		return -1;
	}

	/* using per thread epoll fd */
	if (epfd == RTE_EPOLL_PER_THREAD)
		epfd = rte_intr_tls_epfd();

	if (op == EPOLL_CTL_ADD) {
		__atomic_store_n(&event->status, RTE_EPOLL_VALID,
				__ATOMIC_RELAXED);
		event->fd = fd;  /* ignore fd in event */
		event->epfd = epfd;
		ev.data.ptr = (void *)event;
	}

	ev.events = event->epdata.event;
	if (epoll_ctl(epfd, op, fd, &ev) < 0) {
		RTE_LOG(ERR, EAL, "Error op %d fd %d epoll_ctl, %s\n",
			op, fd, strerror(errno));
		if (op == EPOLL_CTL_ADD)
			/* rollback status when CTL_ADD fail */
			__atomic_store_n(&event->status, RTE_EPOLL_INVALID,
					__ATOMIC_RELAXED);
		return -1;
	}

	if (op == EPOLL_CTL_DEL && __atomic_load_n(&event->status,
			__ATOMIC_RELAXED) != RTE_EPOLL_INVALID)
		eal_epoll_data_safe_free(event);

	return 0;
}

int
rte_intr_rx_ctl(struct rte_intr_handle *intr_handle, int epfd,
		int op, unsigned int vec, void *data)
{
	struct rte_epoll_event *rev;
	struct rte_epoll_data *epdata;
	int epfd_op;
	unsigned int efd_idx;
	int rc = 0;

	efd_idx = (vec >= RTE_INTR_VEC_RXTX_OFFSET) ?
		(vec - RTE_INTR_VEC_RXTX_OFFSET) : vec;

	if (!intr_handle || intr_handle->nb_efd == 0 ||
	    efd_idx >= intr_handle->nb_efd) {
		RTE_LOG(ERR, EAL, "Wrong intr vector number.\n");
		return -EPERM;
	}

	switch (op) {
	case RTE_INTR_EVENT_ADD:
		epfd_op = EPOLL_CTL_ADD;
		rev = &intr_handle->elist[efd_idx];
		if (__atomic_load_n(&rev->status,
				__ATOMIC_RELAXED) != RTE_EPOLL_INVALID) {
			RTE_LOG(INFO, EAL, "Event already been added.\n");
			return -EEXIST;
		}

		/* attach to intr vector fd */
		epdata = &rev->epdata;
		epdata->event  = EPOLLIN | EPOLLPRI | EPOLLET;
		epdata->data   = data;
		epdata->cb_fun = (rte_intr_event_cb_t)eal_intr_proc_rxtx_intr;
		epdata->cb_arg = (void *)intr_handle;
		rc = rte_epoll_ctl(epfd, epfd_op,
				   intr_handle->efds[efd_idx], rev);
		if (!rc)
			RTE_LOG(DEBUG, EAL,
				"efd %d associated with vec %d added on epfd %d"
				"\n", rev->fd, vec, epfd);
		else
			rc = -EPERM;
		break;
	case RTE_INTR_EVENT_DEL:
		epfd_op = EPOLL_CTL_DEL;
		rev = &intr_handle->elist[efd_idx];
		if (__atomic_load_n(&rev->status,
				__ATOMIC_RELAXED) == RTE_EPOLL_INVALID) {
			RTE_LOG(INFO, EAL, "Event does not exist.\n");
			return -EPERM;
		}

		rc = rte_epoll_ctl(rev->epfd, epfd_op, rev->fd, rev);
		if (rc)
			rc = -EPERM;
		break;
	default:
		RTE_LOG(ERR, EAL, "event op type mismatch\n");
		rc = -EPERM;
	}

	return rc;
}

void
rte_intr_free_epoll_fd(struct rte_intr_handle *intr_handle)
{
	uint32_t i;
	struct rte_epoll_event *rev;

	for (i = 0; i < intr_handle->nb_efd; i++) {
		rev = &intr_handle->elist[i];
		if (__atomic_load_n(&rev->status,
				__ATOMIC_RELAXED) == RTE_EPOLL_INVALID)
			continue;
		if (rte_epoll_ctl(rev->epfd, EPOLL_CTL_DEL, rev->fd, rev)) {
			/* force free if the entry valid */
			eal_epoll_data_safe_free(rev);
		}
	}
}

int
rte_intr_efd_enable(struct rte_intr_handle *intr_handle, uint32_t nb_efd)
{
	uint32_t i;
	int fd;
	uint32_t n = RTE_MIN(nb_efd, (uint32_t)RTE_MAX_RXTX_INTR_VEC_ID);

	assert(nb_efd != 0);

	if (intr_handle->type == RTE_INTR_HANDLE_VFIO_MSIX) {
		for (i = 0; i < n; i++) {
			fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
			if (fd < 0) {
				RTE_LOG(ERR, EAL,
					"can't setup eventfd, error %i (%s)\n",
					errno, strerror(errno));
				return -errno;
			}
			intr_handle->efds[i] = fd;
		}
		intr_handle->nb_efd   = n;
		intr_handle->max_intr = NB_OTHER_INTR + n;
	} else if (intr_handle->type == RTE_INTR_HANDLE_VDEV) {
		/* only check, initialization would be done in vdev driver.*/
		if (intr_handle->efd_counter_size >
		    sizeof(union rte_intr_read_buffer)) {
			RTE_LOG(ERR, EAL, "the efd_counter_size is oversized");
			return -EINVAL;
		}
	} else {
		intr_handle->efds[0]  = intr_handle->fd;
		intr_handle->nb_efd   = RTE_MIN(nb_efd, 1U);
		intr_handle->max_intr = NB_OTHER_INTR;
	}

	return 0;
}

void
rte_intr_efd_disable(struct rte_intr_handle *intr_handle)
{
	uint32_t i;

	rte_intr_free_epoll_fd(intr_handle);
	if (intr_handle->max_intr > intr_handle->nb_efd) {
		for (i = 0; i < intr_handle->nb_efd; i++)
			close(intr_handle->efds[i]);
	}
	intr_handle->nb_efd = 0;
	intr_handle->max_intr = 0;
}

int
rte_intr_dp_is_en(struct rte_intr_handle *intr_handle)
{
	return !(!intr_handle->nb_efd);
}

int
rte_intr_allow_others(struct rte_intr_handle *intr_handle)
{
	if (!rte_intr_dp_is_en(intr_handle))
		return 1;
	else
		return !!(intr_handle->max_intr - intr_handle->nb_efd);
}

int
rte_intr_cap_multiple(struct rte_intr_handle *intr_handle)
{
	if (intr_handle->type == RTE_INTR_HANDLE_VFIO_MSIX)
		return 1;

	if (intr_handle->type == RTE_INTR_HANDLE_VDEV)
		return 1;

	return 0;
}

int rte_thread_is_intr(void)
{
	return pthread_equal(intr_thread, pthread_self());
}
