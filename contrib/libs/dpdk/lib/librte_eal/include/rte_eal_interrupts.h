/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_INTERRUPTS_H_
#error "don't include this file directly, please include generic <rte_interrupts.h>"
#endif

/**
 * @file rte_eal_interrupts.h
 * @internal
 *
 * Contains function prototypes exposed by the EAL for interrupt handling by
 * drivers and other DPDK internal consumers.
 */

#ifndef _RTE_EAL_INTERRUPTS_H_
#define _RTE_EAL_INTERRUPTS_H_

#define RTE_MAX_RXTX_INTR_VEC_ID      512
#define RTE_INTR_VEC_ZERO_OFFSET      0
#define RTE_INTR_VEC_RXTX_OFFSET      1

/**
 * The interrupt source type, e.g. UIO, VFIO, ALARM etc.
 */
enum rte_intr_handle_type {
	RTE_INTR_HANDLE_UNKNOWN = 0,  /**< generic unknown handle */
	RTE_INTR_HANDLE_UIO,          /**< uio device handle */
	RTE_INTR_HANDLE_UIO_INTX,     /**< uio generic handle */
	RTE_INTR_HANDLE_VFIO_LEGACY,  /**< vfio device handle (legacy) */
	RTE_INTR_HANDLE_VFIO_MSI,     /**< vfio device handle (MSI) */
	RTE_INTR_HANDLE_VFIO_MSIX,    /**< vfio device handle (MSIX) */
	RTE_INTR_HANDLE_ALARM,        /**< alarm handle */
	RTE_INTR_HANDLE_EXT,          /**< external handler */
	RTE_INTR_HANDLE_VDEV,         /**< virtual device */
	RTE_INTR_HANDLE_DEV_EVENT,    /**< device event handle */
	RTE_INTR_HANDLE_VFIO_REQ,     /**< VFIO request handle */
	RTE_INTR_HANDLE_MAX           /**< count of elements */
};

#define RTE_INTR_EVENT_ADD            1UL
#define RTE_INTR_EVENT_DEL            2UL

typedef void (*rte_intr_event_cb_t)(int fd, void *arg);

struct rte_epoll_data {
	uint32_t event;               /**< event type */
	void *data;                   /**< User data */
	rte_intr_event_cb_t cb_fun;   /**< IN: callback fun */
	void *cb_arg;	              /**< IN: callback arg */
};

enum {
	RTE_EPOLL_INVALID = 0,
	RTE_EPOLL_VALID,
	RTE_EPOLL_EXEC,
};

/** interrupt epoll event obj, taken by epoll_event.ptr */
struct rte_epoll_event {
	uint32_t status;           /**< OUT: event status */
	int fd;                    /**< OUT: event fd */
	int epfd;       /**< OUT: epoll instance the ev associated with */
	struct rte_epoll_data epdata;
};

/** Handle for interrupts. */
struct rte_intr_handle {
	RTE_STD_C11
	union {
		struct {
			RTE_STD_C11
			union {
				/** VFIO device file descriptor */
				int vfio_dev_fd;
				/** UIO cfg file desc for uio_pci_generic */
				int uio_cfg_fd;
			};
			int fd;	/**< interrupt event file descriptor */
		};
		void *handle; /**< device driver handle (Windows) */
	};
	enum rte_intr_handle_type type;  /**< handle type */
	uint32_t max_intr;             /**< max interrupt requested */
	uint32_t nb_efd;               /**< number of available efd(event fd) */
	uint8_t efd_counter_size;      /**< size of efd counter, used for vdev */
	int efds[RTE_MAX_RXTX_INTR_VEC_ID];  /**< intr vectors/efds mapping */
	struct rte_epoll_event elist[RTE_MAX_RXTX_INTR_VEC_ID];
				       /**< intr vector epoll event */
	int *intr_vec;                 /**< intr vector number array */
};

#define RTE_EPOLL_PER_THREAD        -1  /**< to hint using per thread epfd */

/**
 * It waits for events on the epoll instance.
 * Retries if signal received.
 *
 * @param epfd
 *   Epoll instance fd on which the caller wait for events.
 * @param events
 *   Memory area contains the events that will be available for the caller.
 * @param maxevents
 *   Up to maxevents are returned, must greater than zero.
 * @param timeout
 *   Specifying a timeout of -1 causes a block indefinitely.
 *   Specifying a timeout equal to zero cause to return immediately.
 * @return
 *   - On success, returns the number of available event.
 *   - On failure, a negative value.
 */
int
rte_epoll_wait(int epfd, struct rte_epoll_event *events,
	       int maxevents, int timeout);

/**
 * It waits for events on the epoll instance.
 * Does not retry if signal received.
 *
 * @param epfd
 *   Epoll instance fd on which the caller wait for events.
 * @param events
 *   Memory area contains the events that will be available for the caller.
 * @param maxevents
 *   Up to maxevents are returned, must greater than zero.
 * @param timeout
 *   Specifying a timeout of -1 causes a block indefinitely.
 *   Specifying a timeout equal to zero cause to return immediately.
 * @return
 *   - On success, returns the number of available event.
 *   - On failure, a negative value.
 */
__rte_experimental
int
rte_epoll_wait_interruptible(int epfd, struct rte_epoll_event *events,
	       int maxevents, int timeout);

/**
 * It performs control operations on epoll instance referred by the epfd.
 * It requests that the operation op be performed for the target fd.
 *
 * @param epfd
 *   Epoll instance fd on which the caller perform control operations.
 * @param op
 *   The operation be performed for the target fd.
 * @param fd
 *   The target fd on which the control ops perform.
 * @param event
 *   Describes the object linked to the fd.
 *   Note: The caller must take care the object deletion after CTL_DEL.
 * @return
 *   - On success, zero.
 *   - On failure, a negative value.
 */
int
rte_epoll_ctl(int epfd, int op, int fd,
	      struct rte_epoll_event *event);

/**
 * The function returns the per thread epoll instance.
 *
 * @return
 *   epfd the epoll instance referred to.
 */
int
rte_intr_tls_epfd(void);

/**
 * @param intr_handle
 *   Pointer to the interrupt handle.
 * @param epfd
 *   Epoll instance fd which the intr vector associated to.
 * @param op
 *   The operation be performed for the vector.
 *   Operation type of {ADD, DEL}.
 * @param vec
 *   RX intr vector number added to the epoll instance wait list.
 * @param data
 *   User raw data.
 * @return
 *   - On success, zero.
 *   - On failure, a negative value.
 */
int
rte_intr_rx_ctl(struct rte_intr_handle *intr_handle,
		int epfd, int op, unsigned int vec, void *data);

/**
 * It deletes registered eventfds.
 *
 * @param intr_handle
 *   Pointer to the interrupt handle.
 */
void
rte_intr_free_epoll_fd(struct rte_intr_handle *intr_handle);

/**
 * It enables the packet I/O interrupt event if it's necessary.
 * It creates event fd for each interrupt vector when MSIX is used,
 * otherwise it multiplexes a single event fd.
 *
 * @param intr_handle
 *   Pointer to the interrupt handle.
 * @param nb_efd
 *   Number of interrupt vector trying to enable.
 *   The value 0 is not allowed.
 * @return
 *   - On success, zero.
 *   - On failure, a negative value.
 */
int
rte_intr_efd_enable(struct rte_intr_handle *intr_handle, uint32_t nb_efd);

/**
 * It disables the packet I/O interrupt event.
 * It deletes registered eventfds and closes the open fds.
 *
 * @param intr_handle
 *   Pointer to the interrupt handle.
 */
void
rte_intr_efd_disable(struct rte_intr_handle *intr_handle);

/**
 * The packet I/O interrupt on datapath is enabled or not.
 *
 * @param intr_handle
 *   Pointer to the interrupt handle.
 */
int
rte_intr_dp_is_en(struct rte_intr_handle *intr_handle);

/**
 * The interrupt handle instance allows other causes or not.
 * Other causes stand for any none packet I/O interrupts.
 *
 * @param intr_handle
 *   Pointer to the interrupt handle.
 */
int
rte_intr_allow_others(struct rte_intr_handle *intr_handle);

/**
 * The multiple interrupt vector capability of interrupt handle instance.
 * It returns zero if no multiple interrupt vector support.
 *
 * @param intr_handle
 *   Pointer to the interrupt handle.
 */
int
rte_intr_cap_multiple(struct rte_intr_handle *intr_handle);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * @internal
 * Check if currently executing in interrupt context
 *
 * @return
 *  - non zero in case of interrupt context
 *  - zero in case of process context
 */
__rte_experimental
int
rte_thread_is_intr(void);

#endif /* _RTE_EAL_INTERRUPTS_H_ */
