/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation. All rights reserved.
 *   Copyright (c) 2020, 2021 Mellanox Technologies LTD. All rights reserved.
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

#ifndef __NVME_INTERNAL_H__
#define __NVME_INTERNAL_H__

#include "spdk/config.h"
#include "spdk/likely.h"
#include "spdk/stdinc.h"

#include "spdk/nvme.h"

#if defined(__i386__) || defined(__x86_64__)
#include <x86intrin.h>
#endif

#include "spdk/queue.h"
#include "spdk/barrier.h"
#include "spdk/bit_array.h"
#include "spdk/mmio.h"
#include "spdk/pci_ids.h"
#include "spdk/util.h"
#include "spdk/memory.h"
#include "spdk/nvme_intel.h"
#include "spdk/nvmf_spec.h"
#include "spdk/uuid.h"

#include "spdk_internal/assert.h"
#include "spdk/log.h"

extern pid_t g_spdk_nvme_pid;

/*
 * Some Intel devices support vendor-unique read latency log page even
 * though the log page directory says otherwise.
 */
#define NVME_INTEL_QUIRK_READ_LATENCY 0x1

/*
 * Some Intel devices support vendor-unique write latency log page even
 * though the log page directory says otherwise.
 */
#define NVME_INTEL_QUIRK_WRITE_LATENCY 0x2

/*
 * The controller needs a delay before starts checking the device
 * readiness, which is done by reading the NVME_CSTS_RDY bit.
 */
#define NVME_QUIRK_DELAY_BEFORE_CHK_RDY	0x4

/*
 * The controller performs best when I/O is split on particular
 * LBA boundaries.
 */
#define NVME_INTEL_QUIRK_STRIPING 0x8

/*
 * The controller needs a delay after allocating an I/O queue pair
 * before it is ready to accept I/O commands.
 */
#define NVME_QUIRK_DELAY_AFTER_QUEUE_ALLOC 0x10

/*
 * Earlier NVMe devices do not indicate whether unmapped blocks
 * will read all zeroes or not. This define indicates that the
 * device does in fact read all zeroes after an unmap event
 */
#define NVME_QUIRK_READ_ZERO_AFTER_DEALLOCATE 0x20

/*
 * The controller doesn't handle Identify value others than 0 or 1 correctly.
 */
#define NVME_QUIRK_IDENTIFY_CNS 0x40

/*
 * The controller supports Open Channel command set if matching additional
 * condition, like the first byte (value 0x1) in the vendor specific
 * bits of the namespace identify structure is set.
 */
#define NVME_QUIRK_OCSSD 0x80

/*
 * The controller has an Intel vendor ID but does not support Intel vendor-specific
 * log pages.  This is primarily for QEMU emulated SSDs which report an Intel vendor
 * ID but do not support these log pages.
 */
#define NVME_INTEL_QUIRK_NO_LOG_PAGES 0x100

/*
 * The controller does not set SHST_COMPLETE in a reasonable amount of time.  This
 * is primarily seen in virtual VMWare NVMe SSDs.  This quirk merely adds an additional
 * error message that on VMWare NVMe SSDs, the shutdown timeout may be expected.
 */
#define NVME_QUIRK_SHST_COMPLETE 0x200

/*
 * The controller requires an extra delay before starting the initialization process
 * during attach.
 */
#define NVME_QUIRK_DELAY_BEFORE_INIT 0x400

/*
 * Some SSDs exhibit poor performance with the default SPDK NVMe IO queue size.
 * This quirk will increase the default to 1024 which matches other operating
 * systems, at the cost of some extra memory usage.  Users can still override
 * the increased default by changing the spdk_nvme_io_qpair_opts when allocating
 * a new queue pair.
 */
#define NVME_QUIRK_MINIMUM_IO_QUEUE_SIZE 0x800

/**
 * The maximum access width to PCI memory space is 8 Bytes, don't use AVX2 or
 * SSE instructions to optimize the memory access(memcpy or memset) larger than
 * 8 Bytes.
 */
#define NVME_QUIRK_MAXIMUM_PCI_ACCESS_WIDTH 0x1000

/**
 * The SSD does not support OPAL even through it sets the security bit in OACS.
 */
#define NVME_QUIRK_OACS_SECURITY 0x2000

/**
 * Intel P55XX SSDs can't support Dataset Management command with SGL format,
 * so use PRP with DSM command.
 */
#define NVME_QUIRK_NO_SGL_FOR_DSM 0x4000

/**
 * Maximum Data Transfer Size(MDTS) excludes interleaved metadata.
 */
#define NVME_QUIRK_MDTS_EXCLUDE_MD 0x8000

#define NVME_MAX_ASYNC_EVENTS	(8)

#define NVME_MAX_ADMIN_TIMEOUT_IN_SECS	(30)

/* Maximum log page size to fetch for AERs. */
#define NVME_MAX_AER_LOG_SIZE		(4096)

/*
 * NVME_MAX_IO_QUEUES in nvme_spec.h defines the 64K spec-limit, but this
 *  define specifies the maximum number of queues this driver will actually
 *  try to configure, if available.
 */
#define DEFAULT_MAX_IO_QUEUES		(1024)
#define DEFAULT_ADMIN_QUEUE_SIZE	(32)
#define DEFAULT_IO_QUEUE_SIZE		(256)
#define DEFAULT_IO_QUEUE_SIZE_FOR_QUIRK	(1024) /* Matches Linux kernel driver */

#define DEFAULT_IO_QUEUE_REQUESTS	(512)

#define SPDK_NVME_DEFAULT_RETRY_COUNT	(4)

#define SPDK_NVME_TRANSPORT_ACK_TIMEOUT_DISABLED	(0)
#define SPDK_NVME_DEFAULT_TRANSPORT_ACK_TIMEOUT	SPDK_NVME_TRANSPORT_ACK_TIMEOUT_DISABLED

#define MIN_KEEP_ALIVE_TIMEOUT_IN_MS	(10000)

/* We want to fit submission and completion rings each in a single 2MB
 * hugepage to ensure physical address contiguity.
 */
#define MAX_IO_QUEUE_ENTRIES		(VALUE_2MB / spdk_max( \
						sizeof(struct spdk_nvme_cmd), \
						sizeof(struct spdk_nvme_cpl)))

/* Default timeout for fabrics connect commands. */
#ifdef DEBUG
#define NVME_FABRIC_CONNECT_COMMAND_TIMEOUT 0
#else
/* 500 millisecond timeout. */
#define NVME_FABRIC_CONNECT_COMMAND_TIMEOUT 500000
#endif

/* This value indicates that a read from a PCIe register is invalid. This can happen when a device is no longer present */
#define SPDK_NVME_INVALID_REGISTER_VALUE 0xFFFFFFFFu

enum nvme_payload_type {
	NVME_PAYLOAD_TYPE_INVALID = 0,

	/** nvme_request::u.payload.contig_buffer is valid for this request */
	NVME_PAYLOAD_TYPE_CONTIG,

	/** nvme_request::u.sgl is valid for this request */
	NVME_PAYLOAD_TYPE_SGL,
};

/**
 * Descriptor for a request data payload.
 */
struct nvme_payload {
	/**
	 * Functions for retrieving physical addresses for scattered payloads.
	 */
	spdk_nvme_req_reset_sgl_cb reset_sgl_fn;
	spdk_nvme_req_next_sge_cb next_sge_fn;

	/**
	 * If reset_sgl_fn == NULL, this is a contig payload, and contig_or_cb_arg contains the
	 * virtual memory address of a single virtually contiguous buffer.
	 *
	 * If reset_sgl_fn != NULL, this is a SGL payload, and contig_or_cb_arg contains the
	 * cb_arg that will be passed to the SGL callback functions.
	 */
	void *contig_or_cb_arg;

	/** Virtual memory address of a single virtually contiguous metadata buffer */
	void *md;
};

#define NVME_PAYLOAD_CONTIG(contig_, md_) \
	(struct nvme_payload) { \
		.reset_sgl_fn = NULL, \
		.next_sge_fn = NULL, \
		.contig_or_cb_arg = (contig_), \
		.md = (md_), \
	}

#define NVME_PAYLOAD_SGL(reset_sgl_fn_, next_sge_fn_, cb_arg_, md_) \
	(struct nvme_payload) { \
		.reset_sgl_fn = (reset_sgl_fn_), \
		.next_sge_fn = (next_sge_fn_), \
		.contig_or_cb_arg = (cb_arg_), \
		.md = (md_), \
	}

static inline enum nvme_payload_type
nvme_payload_type(const struct nvme_payload *payload) {
	return payload->reset_sgl_fn ? NVME_PAYLOAD_TYPE_SGL : NVME_PAYLOAD_TYPE_CONTIG;
}

struct nvme_error_cmd {
	bool				do_not_submit;
	uint64_t			timeout_tsc;
	uint32_t			err_count;
	uint8_t				opc;
	struct spdk_nvme_status		status;
	TAILQ_ENTRY(nvme_error_cmd)	link;
};

struct nvme_request {
	struct spdk_nvme_cmd		cmd;

	uint8_t				retries;

	uint8_t				timed_out : 1;

	/**
	 * True if the request is in the queued_req list.
	 */
	uint8_t				queued : 1;
	uint8_t				reserved : 6;

	/**
	 * Number of children requests still outstanding for this
	 *  request which was split into multiple child requests.
	 */
	uint16_t			num_children;

	/**
	 * Offset in bytes from the beginning of payload for this request.
	 * This is used for I/O commands that are split into multiple requests.
	 */
	uint32_t			payload_offset;
	uint32_t			md_offset;

	uint32_t			payload_size;

	/**
	 * Timeout ticks for error injection requests, can be extended in future
	 * to support per-request timeout feature.
	 */
	uint64_t			timeout_tsc;

	/**
	 * Data payload for this request's command.
	 */
	struct nvme_payload		payload;

	spdk_nvme_cmd_cb		cb_fn;
	void				*cb_arg;
	STAILQ_ENTRY(nvme_request)	stailq;

	struct spdk_nvme_qpair		*qpair;

	/*
	 * The value of spdk_get_ticks() when the request was submitted to the hardware.
	 * Only set if ctrlr->timeout_enabled is true.
	 */
	uint64_t			submit_tick;

	/**
	 * The active admin request can be moved to a per process pending
	 *  list based on the saved pid to tell which process it belongs
	 *  to. The cpl saves the original completion information which
	 *  is used in the completion callback.
	 * NOTE: these below two fields are only used for admin request.
	 */
	pid_t				pid;
	struct spdk_nvme_cpl		cpl;

	uint32_t			md_size;

	/**
	 * The following members should not be reordered with members
	 *  above.  These members are only needed when splitting
	 *  requests which is done rarely, and the driver is careful
	 *  to not touch the following fields until a split operation is
	 *  needed, to avoid touching an extra cacheline.
	 */

	/**
	 * Points to the outstanding child requests for a parent request.
	 *  Only valid if a request was split into multiple children
	 *  requests, and is not initialized for non-split requests.
	 */
	TAILQ_HEAD(, nvme_request)	children;

	/**
	 * Linked-list pointers for a child request in its parent's list.
	 */
	TAILQ_ENTRY(nvme_request)	child_tailq;

	/**
	 * Points to a parent request if part of a split request,
	 *   NULL otherwise.
	 */
	struct nvme_request		*parent;

	/**
	 * Completion status for a parent request.  Initialized to all 0's
	 *  (SUCCESS) before child requests are submitted.  If a child
	 *  request completes with error, the error status is copied here,
	 *  to ensure that the parent request is also completed with error
	 *  status once all child requests are completed.
	 */
	struct spdk_nvme_cpl		parent_status;

	/**
	 * The user_cb_fn and user_cb_arg fields are used for holding the original
	 * callback data when using nvme_allocate_request_user_copy.
	 */
	spdk_nvme_cmd_cb		user_cb_fn;
	void				*user_cb_arg;
	void				*user_buffer;
};

struct nvme_completion_poll_status {
	struct spdk_nvme_cpl	cpl;
	bool			done;
	/* This flag indicates that the request has been timed out and the memory
	   must be freed in a completion callback */
	bool			timed_out;
};

struct nvme_async_event_request {
	struct spdk_nvme_ctrlr		*ctrlr;
	struct nvme_request		*req;
	struct spdk_nvme_cpl		cpl;
};

enum nvme_qpair_state {
	NVME_QPAIR_DISCONNECTED,
	NVME_QPAIR_DISCONNECTING,
	NVME_QPAIR_CONNECTING,
	NVME_QPAIR_CONNECTED,
	NVME_QPAIR_ENABLING,
	NVME_QPAIR_ENABLED,
	NVME_QPAIR_DESTROYING,
};

struct spdk_nvme_qpair {
	struct spdk_nvme_ctrlr			*ctrlr;

	uint16_t				id;

	uint8_t					qprio;

	uint8_t					state : 3;

	/*
	 * Members for handling IO qpair deletion inside of a completion context.
	 * These are specifically defined as single bits, so that they do not
	 *  push this data structure out to another cacheline.
	 */
	uint8_t					in_completion_context : 1;
	uint8_t					delete_after_completion_context: 1;

	/*
	 * Set when no deletion notification is needed. For example, the process
	 * which allocated this qpair exited unexpectedly.
	 */
	uint8_t					no_deletion_notification_needed: 1;

	uint8_t					first_fused_submitted: 1;

	enum spdk_nvme_transport_type		trtype;

	STAILQ_HEAD(, nvme_request)		free_req;
	STAILQ_HEAD(, nvme_request)		queued_req;
	STAILQ_HEAD(, nvme_request)		aborting_queued_req;

	/* List entry for spdk_nvme_transport_poll_group::qpairs */
	STAILQ_ENTRY(spdk_nvme_qpair)		poll_group_stailq;

	/** Commands opcode in this list will return error */
	TAILQ_HEAD(, nvme_error_cmd)		err_cmd_head;
	/** Requests in this list will return error */
	STAILQ_HEAD(, nvme_request)		err_req_head;

	/* List entry for spdk_nvme_ctrlr::active_io_qpairs */
	TAILQ_ENTRY(spdk_nvme_qpair)		tailq;

	/* List entry for spdk_nvme_ctrlr_process::allocated_io_qpairs */
	TAILQ_ENTRY(spdk_nvme_qpair)		per_process_tailq;

	struct spdk_nvme_ctrlr_process		*active_proc;

	struct spdk_nvme_transport_poll_group	*poll_group;

	void					*poll_group_tailq_head;

	void					*req_buf;

	const struct spdk_nvme_transport	*transport;

	uint8_t					transport_failure_reason: 2;
};

struct spdk_nvme_poll_group {
	void						*ctx;
	struct spdk_nvme_accel_fn_table			accel_fn_table;
	STAILQ_HEAD(, spdk_nvme_transport_poll_group)	tgroups;
};

struct spdk_nvme_transport_poll_group {
	struct spdk_nvme_poll_group			*group;
	const struct spdk_nvme_transport		*transport;
	STAILQ_HEAD(, spdk_nvme_qpair)			connected_qpairs;
	STAILQ_HEAD(, spdk_nvme_qpair)			disconnected_qpairs;
	STAILQ_ENTRY(spdk_nvme_transport_poll_group)	link;
	bool						in_completion_context;
	uint64_t					num_qpairs_to_delete;
};

struct spdk_nvme_ns {
	struct spdk_nvme_ctrlr		*ctrlr;
	uint32_t			sector_size;

	/*
	 * Size of data transferred as part of each block,
	 * including metadata if FLBAS indicates the metadata is transferred
	 * as part of the data buffer at the end of each LBA.
	 */
	uint32_t			extended_lba_size;

	uint32_t			md_size;
	uint32_t			pi_type;
	uint32_t			sectors_per_max_io;
	uint32_t			sectors_per_max_io_no_md;
	uint32_t			sectors_per_stripe;
	uint32_t			id;
	uint16_t			flags;

	/* Command Set Identifier */
	enum spdk_nvme_csi		csi;

	/* Namespace Identification Descriptor List (CNS = 03h) */
	uint8_t				id_desc_list[4096];

	uint32_t			ana_group_id;
	enum spdk_nvme_ana_state	ana_state;

	/* Identify Namespace data. */
	struct spdk_nvme_ns_data	nsdata;

	/* Zoned Namespace Command Set Specific Identify Namespace data. */
	struct spdk_nvme_zns_ns_data	*nsdata_zns;
};

/**
 * State of struct spdk_nvme_ctrlr (in particular, during initialization).
 */
enum nvme_ctrlr_state {
	/**
	 * Wait before initializing the controller.
	 */
	NVME_CTRLR_STATE_INIT_DELAY,

	/**
	 * Controller has not been initialized yet.
	 */
	NVME_CTRLR_STATE_INIT,

	/**
	 * Waiting for CSTS.RDY to transition from 0 to 1 so that CC.EN may be set to 0.
	 */
	NVME_CTRLR_STATE_DISABLE_WAIT_FOR_READY_1,

	/**
	 * Waiting for CSTS.RDY to transition from 1 to 0 so that CC.EN may be set to 1.
	 */
	NVME_CTRLR_STATE_DISABLE_WAIT_FOR_READY_0,

	/**
	 * Enable the controller by writing CC.EN to 1
	 */
	NVME_CTRLR_STATE_ENABLE,

	/**
	 * Waiting for CSTS.RDY to transition from 0 to 1 after enabling the controller.
	 */
	NVME_CTRLR_STATE_ENABLE_WAIT_FOR_READY_1,

	/**
	 * Reset the Admin queue of the controller.
	 */
	NVME_CTRLR_STATE_RESET_ADMIN_QUEUE,

	/**
	 * Identify Controller command will be sent to then controller.
	 */
	NVME_CTRLR_STATE_IDENTIFY,

	/**
	 * Waiting for Identify Controller command be completed.
	 */
	NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY,

	/**
	 * Get Identify I/O Command Set Specific Controller data structure.
	 */
	NVME_CTRLR_STATE_IDENTIFY_IOCS_SPECIFIC,

	/**
	 * Waiting for Identify I/O Command Set Specific Controller command to be completed.
	 */
	NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_IOCS_SPECIFIC,

	/**
	 * Get Commands Supported and Effects log page for the Zoned Namespace Command Set.
	 */
	NVME_CTRLR_STATE_GET_ZNS_CMD_EFFECTS_LOG,

	/**
	 * Waiting for the Get Log Page command to be completed.
	 */
	NVME_CTRLR_STATE_WAIT_FOR_GET_ZNS_CMD_EFFECTS_LOG,

	/**
	 * Set Number of Queues of the controller.
	 */
	NVME_CTRLR_STATE_SET_NUM_QUEUES,

	/**
	 * Waiting for Set Num of Queues command to be completed.
	 */
	NVME_CTRLR_STATE_WAIT_FOR_SET_NUM_QUEUES,

	/**
	 * Construct Namespace data structures of the controller.
	 */
	NVME_CTRLR_STATE_CONSTRUCT_NS,

	/**
	 * Get active Namespace list of the controller.
	 */
	NVME_CTRLR_STATE_IDENTIFY_ACTIVE_NS,

	/**
	 * Waiting for the Identify Active Namespace commands to be completed.
	 */
	NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_ACTIVE_NS,

	/**
	 * Get Identify Namespace Data structure for each NS.
	 */
	NVME_CTRLR_STATE_IDENTIFY_NS,

	/**
	 * Waiting for the Identify Namespace commands to be completed.
	 */
	NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_NS,

	/**
	 * Get Identify Namespace Identification Descriptors.
	 */
	NVME_CTRLR_STATE_IDENTIFY_ID_DESCS,

	/**
	 * Get Identify I/O Command Set Specific Namespace data structure for each NS.
	 */
	NVME_CTRLR_STATE_IDENTIFY_NS_IOCS_SPECIFIC,

	/**
	 * Waiting for the Identify I/O Command Set Specific Namespace commands to be completed.
	 */
	NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_NS_IOCS_SPECIFIC,

	/**
	 * Waiting for the Identify Namespace Identification
	 * Descriptors to be completed.
	 */
	NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_ID_DESCS,

	/**
	 * Configure AER of the controller.
	 */
	NVME_CTRLR_STATE_CONFIGURE_AER,

	/**
	 * Waiting for the Configure AER to be completed.
	 */
	NVME_CTRLR_STATE_WAIT_FOR_CONFIGURE_AER,

	/**
	 * Set supported log pages of the controller.
	 */
	NVME_CTRLR_STATE_SET_SUPPORTED_LOG_PAGES,

	/**
	 * Set supported features of the controller.
	 */
	NVME_CTRLR_STATE_SET_SUPPORTED_FEATURES,

	/**
	 * Set Doorbell Buffer Config of the controller.
	 */
	NVME_CTRLR_STATE_SET_DB_BUF_CFG,

	/**
	 * Waiting for Doorbell Buffer Config to be completed.
	 */
	NVME_CTRLR_STATE_WAIT_FOR_DB_BUF_CFG,

	/**
	 * Set Keep Alive Timeout of the controller.
	 */
	NVME_CTRLR_STATE_SET_KEEP_ALIVE_TIMEOUT,

	/**
	 * Waiting for Set Keep Alive Timeout to be completed.
	 */
	NVME_CTRLR_STATE_WAIT_FOR_KEEP_ALIVE_TIMEOUT,

	/**
	 * Set Host ID of the controller.
	 */
	NVME_CTRLR_STATE_SET_HOST_ID,

	/**
	 * Waiting for Set Host ID to be completed.
	 */
	NVME_CTRLR_STATE_WAIT_FOR_HOST_ID,

	/**
	 * Controller initialization has completed and the controller is ready.
	 */
	NVME_CTRLR_STATE_READY,

	/**
	 * Controller inilialization has an error.
	 */
	NVME_CTRLR_STATE_ERROR
};

#define NVME_TIMEOUT_INFINITE	0

/*
 * Used to track properties for all processes accessing the controller.
 */
struct spdk_nvme_ctrlr_process {
	/** Whether it is the primary process  */
	bool						is_primary;

	/** Process ID */
	pid_t						pid;

	/** Active admin requests to be completed */
	STAILQ_HEAD(, nvme_request)			active_reqs;

	TAILQ_ENTRY(spdk_nvme_ctrlr_process)		tailq;

	/** Per process PCI device handle */
	struct spdk_pci_device				*devhandle;

	/** Reference to track the number of attachment to this controller. */
	int						ref;

	/** Allocated IO qpairs */
	TAILQ_HEAD(, spdk_nvme_qpair)			allocated_io_qpairs;

	spdk_nvme_aer_cb				aer_cb_fn;
	void						*aer_cb_arg;

	/**
	 * A function pointer to timeout callback function
	 */
	spdk_nvme_timeout_cb		timeout_cb_fn;
	void				*timeout_cb_arg;
	uint64_t			timeout_ticks;
};

struct spdk_nvme_ctrlr_aer_completion_list {
	struct spdk_nvme_cpl	cpl;
	STAILQ_ENTRY(spdk_nvme_ctrlr_aer_completion_list) link;
};

/*
 * One of these per allocated PCI device.
 */
struct spdk_nvme_ctrlr {
	/* Hot data (accessed in I/O path) starts here. */

	/** Array of namespaces indexed by nsid - 1 */
	struct spdk_nvme_ns		*ns;

	uint32_t			num_ns;

	bool				is_removed;

	bool				is_resetting;

	bool				is_failed;

	bool				is_destructed;

	bool				timeout_enabled;

	uint16_t			max_sges;

	uint16_t			cntlid;

	/** Controller support flags */
	uint64_t			flags;

	/** NVMEoF in-capsule data size in bytes */
	uint32_t			ioccsz_bytes;

	/** NVMEoF in-capsule data offset in 16 byte units */
	uint16_t			icdoff;

	/* Cold data (not accessed in normal I/O path) is after this point. */

	struct spdk_nvme_transport_id	trid;

	union spdk_nvme_cap_register	cap;
	union spdk_nvme_vs_register	vs;

	int				state;
	uint64_t			state_timeout_tsc;

	uint64_t			next_keep_alive_tick;
	uint64_t			keep_alive_interval_ticks;

	TAILQ_ENTRY(spdk_nvme_ctrlr)	tailq;

	/** All the log pages supported */
	bool				log_page_supported[256];

	/** All the features supported */
	bool				feature_supported[256];

	/** maximum i/o size in bytes */
	uint32_t			max_xfer_size;

	/** minimum page size supported by this controller in bytes */
	uint32_t			min_page_size;

	/** selected memory page size for this controller in bytes */
	uint32_t			page_size;

	uint32_t			num_aers;
	struct nvme_async_event_request	aer[NVME_MAX_ASYNC_EVENTS];

	/** guards access to the controller itself, including admin queues */
	pthread_mutex_t			ctrlr_lock;

	struct spdk_nvme_qpair		*adminq;

	/** shadow doorbell buffer */
	uint32_t			*shadow_doorbell;
	/** eventidx buffer */
	uint32_t			*eventidx;

	/**
	 * Identify Controller data.
	 */
	struct spdk_nvme_ctrlr_data	cdata;

	/**
	 * Zoned Namespace Command Set Specific Identify Controller data.
	 */
	struct spdk_nvme_zns_ctrlr_data	*cdata_zns;

	/**
	 * Keep track of active namespaces
	 */
	uint32_t			*active_ns_list;

	struct spdk_bit_array		*free_io_qids;
	TAILQ_HEAD(, spdk_nvme_qpair)	active_io_qpairs;

	struct spdk_nvme_ctrlr_opts	opts;

	uint64_t			quirks;

	/* Extra sleep time during controller initialization */
	uint64_t			sleep_timeout_tsc;

	/** Track all the processes manage this controller */
	TAILQ_HEAD(, spdk_nvme_ctrlr_process)	active_procs;


	STAILQ_HEAD(, nvme_request)	queued_aborts;
	uint32_t			outstanding_aborts;

	/* CB to notify the user when the ctrlr is removed/failed. */
	spdk_nvme_remove_cb			remove_cb;
	void					*cb_ctx;

	struct spdk_nvme_qpair		*external_io_msgs_qpair;
	pthread_mutex_t			external_io_msgs_lock;
	struct spdk_ring		*external_io_msgs;

	STAILQ_HEAD(, nvme_io_msg_producer) io_producers;

	struct spdk_nvme_ana_page	*ana_log_page;
	uint32_t			ana_log_page_size;

	/* scratchpad pointer that can be used to send data between two NVME_CTRLR_STATEs */
	void				*tmp_ptr;

	/* maximum zone append size in bytes */
	uint32_t			max_zone_append_size;

	STAILQ_HEAD(, spdk_nvme_ctrlr_aer_completion_list)      async_events;

	/* PMR size in bytes */
	uint64_t			pmr_size;
};

struct spdk_nvme_probe_ctx {
	struct spdk_nvme_transport_id		trid;
	void					*cb_ctx;
	spdk_nvme_probe_cb			probe_cb;
	spdk_nvme_attach_cb			attach_cb;
	spdk_nvme_remove_cb			remove_cb;
	TAILQ_HEAD(, spdk_nvme_ctrlr)		init_ctrlrs;
};

typedef void (*nvme_ctrlr_detach_cb)(struct spdk_nvme_ctrlr *ctrlr);

struct nvme_ctrlr_detach_ctx {
	struct spdk_nvme_ctrlr			*ctrlr;
	nvme_ctrlr_detach_cb			cb_fn;
	uint64_t				shutdown_start_tsc;
	uint32_t				shutdown_timeout_ms;
	bool					shutdown_complete;
	TAILQ_ENTRY(nvme_ctrlr_detach_ctx)	link;
};

struct spdk_nvme_detach_ctx {
	TAILQ_HEAD(, nvme_ctrlr_detach_ctx)	head;
	bool					polling_started;
};

struct nvme_driver {
	pthread_mutex_t			lock;

	/** Multi-process shared attached controller list */
	TAILQ_HEAD(, spdk_nvme_ctrlr)	shared_attached_ctrlrs;

	bool				initialized;
	struct spdk_uuid		default_extended_host_id;

	/** netlink socket fd for hotplug messages */
	int				hotplug_fd;
};

extern struct nvme_driver *g_spdk_nvme_driver;

int nvme_driver_init(void);

#define nvme_delay		usleep

static inline bool
nvme_qpair_is_admin_queue(struct spdk_nvme_qpair *qpair)
{
	return qpair->id == 0;
}

static inline bool
nvme_qpair_is_io_queue(struct spdk_nvme_qpair *qpair)
{
	return qpair->id != 0;
}

static inline int
nvme_robust_mutex_lock(pthread_mutex_t *mtx)
{
	int rc = pthread_mutex_lock(mtx);

#ifndef __FreeBSD__
	if (rc == EOWNERDEAD) {
		rc = pthread_mutex_consistent(mtx);
	}
#endif

	return rc;
}

static inline int
nvme_robust_mutex_unlock(pthread_mutex_t *mtx)
{
	return pthread_mutex_unlock(mtx);
}

/* Poll group management functions. */
int nvme_poll_group_connect_qpair(struct spdk_nvme_qpair *qpair);
int nvme_poll_group_disconnect_qpair(struct spdk_nvme_qpair *qpair);

/* Admin functions */
int	nvme_ctrlr_cmd_identify(struct spdk_nvme_ctrlr *ctrlr,
				uint8_t cns, uint16_t cntid, uint32_t nsid,
				uint8_t csi, void *payload, size_t payload_size,
				spdk_nvme_cmd_cb cb_fn, void *cb_arg);
int	nvme_ctrlr_cmd_set_num_queues(struct spdk_nvme_ctrlr *ctrlr,
				      uint32_t num_queues, spdk_nvme_cmd_cb cb_fn,
				      void *cb_arg);
int	nvme_ctrlr_cmd_get_num_queues(struct spdk_nvme_ctrlr *ctrlr,
				      spdk_nvme_cmd_cb cb_fn, void *cb_arg);
int	nvme_ctrlr_cmd_set_async_event_config(struct spdk_nvme_ctrlr *ctrlr,
		union spdk_nvme_feat_async_event_configuration config,
		spdk_nvme_cmd_cb cb_fn, void *cb_arg);
int	nvme_ctrlr_cmd_set_host_id(struct spdk_nvme_ctrlr *ctrlr, void *host_id, uint32_t host_id_size,
				   spdk_nvme_cmd_cb cb_fn, void *cb_arg);
int	nvme_ctrlr_cmd_attach_ns(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
				 struct spdk_nvme_ctrlr_list *payload, spdk_nvme_cmd_cb cb_fn, void *cb_arg);
int	nvme_ctrlr_cmd_detach_ns(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
				 struct spdk_nvme_ctrlr_list *payload, spdk_nvme_cmd_cb cb_fn, void *cb_arg);
int	nvme_ctrlr_cmd_create_ns(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_ns_data *payload,
				 spdk_nvme_cmd_cb cb_fn, void *cb_arg);
int	nvme_ctrlr_cmd_doorbell_buffer_config(struct spdk_nvme_ctrlr *ctrlr,
		uint64_t prp1, uint64_t prp2,
		spdk_nvme_cmd_cb cb_fn, void *cb_arg);
int	nvme_ctrlr_cmd_delete_ns(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid, spdk_nvme_cmd_cb cb_fn,
				 void *cb_arg);
int	nvme_ctrlr_cmd_format(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
			      struct spdk_nvme_format *format, spdk_nvme_cmd_cb cb_fn, void *cb_arg);
int	nvme_ctrlr_cmd_fw_commit(struct spdk_nvme_ctrlr *ctrlr,
				 const struct spdk_nvme_fw_commit *fw_commit,
				 spdk_nvme_cmd_cb cb_fn, void *cb_arg);
int	nvme_ctrlr_cmd_fw_image_download(struct spdk_nvme_ctrlr *ctrlr,
		uint32_t size, uint32_t offset, void *payload,
		spdk_nvme_cmd_cb cb_fn, void *cb_arg);
int	nvme_ctrlr_cmd_sanitize(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
				struct spdk_nvme_sanitize *sanitize, uint32_t cdw11,
				spdk_nvme_cmd_cb cb_fn, void *cb_arg);
void	nvme_completion_poll_cb(void *arg, const struct spdk_nvme_cpl *cpl);
int	nvme_wait_for_completion(struct spdk_nvme_qpair *qpair,
				 struct nvme_completion_poll_status *status);
int	nvme_wait_for_completion_robust_lock(struct spdk_nvme_qpair *qpair,
		struct nvme_completion_poll_status *status,
		pthread_mutex_t *robust_mutex);
int	nvme_wait_for_completion_timeout(struct spdk_nvme_qpair *qpair,
		struct nvme_completion_poll_status *status,
		uint64_t timeout_in_usecs);
int	nvme_wait_for_completion_robust_lock_timeout(struct spdk_nvme_qpair *qpair,
		struct nvme_completion_poll_status *status,
		pthread_mutex_t *robust_mutex,
		uint64_t timeout_in_usecs);

struct spdk_nvme_ctrlr_process *nvme_ctrlr_get_process(struct spdk_nvme_ctrlr *ctrlr,
		pid_t pid);
struct spdk_nvme_ctrlr_process *nvme_ctrlr_get_current_process(struct spdk_nvme_ctrlr *ctrlr);
int	nvme_ctrlr_add_process(struct spdk_nvme_ctrlr *ctrlr, void *devhandle);
void	nvme_ctrlr_free_processes(struct spdk_nvme_ctrlr *ctrlr);
struct spdk_pci_device *nvme_ctrlr_proc_get_devhandle(struct spdk_nvme_ctrlr *ctrlr);

int	nvme_ctrlr_probe(const struct spdk_nvme_transport_id *trid,
			 struct spdk_nvme_probe_ctx *probe_ctx, void *devhandle);

int	nvme_ctrlr_construct(struct spdk_nvme_ctrlr *ctrlr);
void	nvme_ctrlr_destruct_finish(struct spdk_nvme_ctrlr *ctrlr);
void	nvme_ctrlr_destruct(struct spdk_nvme_ctrlr *ctrlr);
void	nvme_ctrlr_destruct_async(struct spdk_nvme_ctrlr *ctrlr,
				  struct nvme_ctrlr_detach_ctx *ctx);
int	nvme_ctrlr_destruct_poll_async(struct spdk_nvme_ctrlr *ctrlr,
				       struct nvme_ctrlr_detach_ctx *ctx);
void	nvme_ctrlr_fail(struct spdk_nvme_ctrlr *ctrlr, bool hot_remove);
int	nvme_ctrlr_reset(struct spdk_nvme_ctrlr *ctrlr);
int	nvme_ctrlr_process_init(struct spdk_nvme_ctrlr *ctrlr);
void	nvme_ctrlr_connected(struct spdk_nvme_probe_ctx *probe_ctx,
			     struct spdk_nvme_ctrlr *ctrlr);

int	nvme_ctrlr_submit_admin_request(struct spdk_nvme_ctrlr *ctrlr,
					struct nvme_request *req);
int	nvme_ctrlr_get_cap(struct spdk_nvme_ctrlr *ctrlr, union spdk_nvme_cap_register *cap);
int	nvme_ctrlr_get_vs(struct spdk_nvme_ctrlr *ctrlr, union spdk_nvme_vs_register *vs);
int	nvme_ctrlr_get_cmbsz(struct spdk_nvme_ctrlr *ctrlr, union spdk_nvme_cmbsz_register *cmbsz);
int	nvme_ctrlr_get_pmrcap(struct spdk_nvme_ctrlr *ctrlr, union spdk_nvme_pmrcap_register *pmrcap);
bool	nvme_ctrlr_multi_iocs_enabled(struct spdk_nvme_ctrlr *ctrlr);
void	nvme_ctrlr_init_cap(struct spdk_nvme_ctrlr *ctrlr, const union spdk_nvme_cap_register *cap,
			    const union spdk_nvme_vs_register *vs);
void    nvme_ctrlr_process_async_event(struct spdk_nvme_ctrlr *ctrlr,
				       const struct spdk_nvme_cpl *cpl);
void nvme_ctrlr_disconnect_qpair(struct spdk_nvme_qpair *qpair);
void nvme_ctrlr_complete_queued_async_events(struct spdk_nvme_ctrlr *ctrlr);
int nvme_qpair_init(struct spdk_nvme_qpair *qpair, uint16_t id,
		    struct spdk_nvme_ctrlr *ctrlr,
		    enum spdk_nvme_qprio qprio,
		    uint32_t num_requests);
void	nvme_qpair_deinit(struct spdk_nvme_qpair *qpair);
void	nvme_qpair_complete_error_reqs(struct spdk_nvme_qpair *qpair);
int	nvme_qpair_submit_request(struct spdk_nvme_qpair *qpair,
				  struct nvme_request *req);
void	nvme_qpair_abort_reqs(struct spdk_nvme_qpair *qpair, uint32_t dnr);
uint32_t nvme_qpair_abort_queued_reqs(struct spdk_nvme_qpair *qpair, void *cmd_cb_arg);
void	nvme_qpair_resubmit_requests(struct spdk_nvme_qpair *qpair, uint32_t num_requests);

int	nvme_ctrlr_identify_active_ns(struct spdk_nvme_ctrlr *ctrlr);
void	nvme_ns_set_identify_data(struct spdk_nvme_ns *ns);
void	nvme_ns_set_id_desc_list_data(struct spdk_nvme_ns *ns);
void	nvme_ns_free_zns_specific_data(struct spdk_nvme_ns *ns);
void	nvme_ns_free_iocs_specific_data(struct spdk_nvme_ns *ns);
bool	nvme_ns_has_supported_iocs_specific_data(struct spdk_nvme_ns *ns);
int	nvme_ns_construct(struct spdk_nvme_ns *ns, uint32_t id,
			  struct spdk_nvme_ctrlr *ctrlr);
void	nvme_ns_destruct(struct spdk_nvme_ns *ns);
int	nvme_ns_update(struct spdk_nvme_ns *ns);
int	nvme_ns_cmd_zone_append_with_md(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
					void *buffer, void *metadata, uint64_t zslba,
					uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg,
					uint32_t io_flags, uint16_t apptag_mask, uint16_t apptag);
int nvme_ns_cmd_zone_appendv_with_md(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
				     uint64_t zslba, uint32_t lba_count,
				     spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t io_flags,
				     spdk_nvme_req_reset_sgl_cb reset_sgl_fn,
				     spdk_nvme_req_next_sge_cb next_sge_fn, void *metadata,
				     uint16_t apptag_mask, uint16_t apptag);

int	nvme_fabric_ctrlr_set_reg_4(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint32_t value);
int	nvme_fabric_ctrlr_set_reg_8(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint64_t value);
int	nvme_fabric_ctrlr_get_reg_4(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint32_t *value);
int	nvme_fabric_ctrlr_scan(struct spdk_nvme_probe_ctx *probe_ctx, bool direct_connect);
int	nvme_fabric_ctrlr_get_reg_8(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint64_t *value);
int	nvme_fabric_ctrlr_discover(struct spdk_nvme_ctrlr *ctrlr,
				   struct spdk_nvme_probe_ctx *probe_ctx);
int	nvme_fabric_qpair_connect(struct spdk_nvme_qpair *qpair, uint32_t num_entries);

typedef int (*spdk_nvme_parse_ana_log_page_cb)(
	const struct spdk_nvme_ana_group_descriptor *desc, void *cb_arg);
int	nvme_ctrlr_parse_ana_log_page(struct spdk_nvme_ctrlr *ctrlr,
				      spdk_nvme_parse_ana_log_page_cb cb_fn, void *cb_arg);

static inline struct nvme_request *
nvme_allocate_request(struct spdk_nvme_qpair *qpair,
		      const struct nvme_payload *payload, uint32_t payload_size, uint32_t md_size,
		      spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;

	req = STAILQ_FIRST(&qpair->free_req);
	if (req == NULL) {
		return req;
	}

	STAILQ_REMOVE_HEAD(&qpair->free_req, stailq);

	/*
	 * Only memset/zero fields that need it.  All other fields
	 *  will be initialized appropriately either later in this
	 *  function, or before they are needed later in the
	 *  submission patch.  For example, the children
	 *  TAILQ_ENTRY and following members are
	 *  only used as part of I/O splitting so we avoid
	 *  memsetting them until it is actually needed.
	 *  They will be initialized in nvme_request_add_child()
	 *  if the request is split.
	 */
	memset(req, 0, offsetof(struct nvme_request, payload_size));

	req->cb_fn = cb_fn;
	req->cb_arg = cb_arg;
	req->payload = *payload;
	req->payload_size = payload_size;
	req->md_size = md_size;
	req->pid = g_spdk_nvme_pid;
	req->submit_tick = 0;

	return req;
}

static inline struct nvme_request *
nvme_allocate_request_contig(struct spdk_nvme_qpair *qpair,
			     void *buffer, uint32_t payload_size,
			     spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_payload payload;

	payload = NVME_PAYLOAD_CONTIG(buffer, NULL);

	return nvme_allocate_request(qpair, &payload, payload_size, 0, cb_fn, cb_arg);
}

static inline struct nvme_request *
nvme_allocate_request_null(struct spdk_nvme_qpair *qpair, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	return nvme_allocate_request_contig(qpair, NULL, 0, cb_fn, cb_arg);
}

struct nvme_request *nvme_allocate_request_user_copy(struct spdk_nvme_qpair *qpair,
		void *buffer, uint32_t payload_size,
		spdk_nvme_cmd_cb cb_fn, void *cb_arg, bool host_to_controller);

static inline void
nvme_complete_request(spdk_nvme_cmd_cb cb_fn, void *cb_arg, struct spdk_nvme_qpair *qpair,
		      struct nvme_request *req, struct spdk_nvme_cpl *cpl)
{
	struct spdk_nvme_cpl            err_cpl;
	struct nvme_error_cmd           *cmd;

	/* error injection at completion path,
	 * only inject for successful completed commands
	 */
	if (spdk_unlikely(!TAILQ_EMPTY(&qpair->err_cmd_head) &&
			  !spdk_nvme_cpl_is_error(cpl))) {
		TAILQ_FOREACH(cmd, &qpair->err_cmd_head, link) {

			if (cmd->do_not_submit) {
				continue;
			}

			if ((cmd->opc == req->cmd.opc) && cmd->err_count) {

				err_cpl = *cpl;
				err_cpl.status.sct = cmd->status.sct;
				err_cpl.status.sc = cmd->status.sc;

				cpl = &err_cpl;
				cmd->err_count--;
				break;
			}
		}
	}

	if (cb_fn) {
		cb_fn(cb_arg, cpl);
	}
}

static inline void
nvme_free_request(struct nvme_request *req)
{
	assert(req != NULL);
	assert(req->num_children == 0);
	assert(req->qpair != NULL);

	STAILQ_INSERT_HEAD(&req->qpair->free_req, req, stailq);
}

static inline void
nvme_qpair_set_state(struct spdk_nvme_qpair *qpair, enum nvme_qpair_state state)
{
	qpair->state = state;
}

static inline enum nvme_qpair_state
nvme_qpair_get_state(struct spdk_nvme_qpair *qpair) {
	return qpair->state;
}

static inline void
nvme_qpair_free_request(struct spdk_nvme_qpair *qpair, struct nvme_request *req)
{
	assert(req != NULL);
	assert(req->num_children == 0);

	STAILQ_INSERT_HEAD(&qpair->free_req, req, stailq);
}

static inline void
nvme_request_remove_child(struct nvme_request *parent, struct nvme_request *child)
{
	assert(parent != NULL);
	assert(child != NULL);
	assert(child->parent == parent);
	assert(parent->num_children != 0);

	parent->num_children--;
	child->parent = NULL;
	TAILQ_REMOVE(&parent->children, child, child_tailq);
}

static inline void
nvme_cb_complete_child(void *child_arg, const struct spdk_nvme_cpl *cpl)
{
	struct nvme_request *child = child_arg;
	struct nvme_request *parent = child->parent;

	nvme_request_remove_child(parent, child);

	if (spdk_nvme_cpl_is_error(cpl)) {
		memcpy(&parent->parent_status, cpl, sizeof(*cpl));
	}

	if (parent->num_children == 0) {
		nvme_complete_request(parent->cb_fn, parent->cb_arg, parent->qpair,
				      parent, &parent->parent_status);
		nvme_free_request(parent);
	}
}

static inline void
nvme_request_add_child(struct nvme_request *parent, struct nvme_request *child)
{
	assert(parent->num_children != UINT16_MAX);

	if (parent->num_children == 0) {
		/*
		 * Defer initialization of the children TAILQ since it falls
		 *  on a separate cacheline.  This ensures we do not touch this
		 *  cacheline except on request splitting cases, which are
		 *  relatively rare.
		 */
		TAILQ_INIT(&parent->children);
		parent->parent = NULL;
		memset(&parent->parent_status, 0, sizeof(struct spdk_nvme_cpl));
	}

	parent->num_children++;
	TAILQ_INSERT_TAIL(&parent->children, child, child_tailq);
	child->parent = parent;
	child->cb_fn = nvme_cb_complete_child;
	child->cb_arg = child;
}

static inline void
nvme_request_free_children(struct nvme_request *req)
{
	struct nvme_request *child, *tmp;

	if (req->num_children == 0) {
		return;
	}

	/* free all child nvme_request */
	TAILQ_FOREACH_SAFE(child, &req->children, child_tailq, tmp) {
		nvme_request_remove_child(req, child);
		nvme_request_free_children(child);
		nvme_free_request(child);
	}
}

int	nvme_request_check_timeout(struct nvme_request *req, uint16_t cid,
				   struct spdk_nvme_ctrlr_process *active_proc, uint64_t now_tick);
uint64_t nvme_get_quirks(const struct spdk_pci_id *id);

int	nvme_robust_mutex_init_shared(pthread_mutex_t *mtx);
int	nvme_robust_mutex_init_recursive_shared(pthread_mutex_t *mtx);

bool	nvme_completion_is_retry(const struct spdk_nvme_cpl *cpl);

struct spdk_nvme_ctrlr *nvme_get_ctrlr_by_trid_unsafe(
	const struct spdk_nvme_transport_id *trid);

const struct spdk_nvme_transport *nvme_get_transport(const char *transport_name);
const struct spdk_nvme_transport *nvme_get_first_transport(void);
const struct spdk_nvme_transport *nvme_get_next_transport(const struct spdk_nvme_transport
		*transport);

/* Transport specific functions */
struct spdk_nvme_ctrlr *nvme_transport_ctrlr_construct(const struct spdk_nvme_transport_id *trid,
		const struct spdk_nvme_ctrlr_opts *opts,
		void *devhandle);
int nvme_transport_ctrlr_destruct(struct spdk_nvme_ctrlr *ctrlr);
int nvme_transport_ctrlr_scan(struct spdk_nvme_probe_ctx *probe_ctx, bool direct_connect);
int nvme_transport_ctrlr_enable(struct spdk_nvme_ctrlr *ctrlr);
int nvme_transport_ctrlr_set_reg_4(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint32_t value);
int nvme_transport_ctrlr_set_reg_8(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint64_t value);
int nvme_transport_ctrlr_get_reg_4(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint32_t *value);
int nvme_transport_ctrlr_get_reg_8(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint64_t *value);
uint32_t nvme_transport_ctrlr_get_max_xfer_size(struct spdk_nvme_ctrlr *ctrlr);
uint16_t nvme_transport_ctrlr_get_max_sges(struct spdk_nvme_ctrlr *ctrlr);
struct spdk_nvme_qpair *nvme_transport_ctrlr_create_io_qpair(struct spdk_nvme_ctrlr *ctrlr,
		uint16_t qid, const struct spdk_nvme_io_qpair_opts *opts);
int nvme_transport_ctrlr_reserve_cmb(struct spdk_nvme_ctrlr *ctrlr);
void *nvme_transport_ctrlr_map_cmb(struct spdk_nvme_ctrlr *ctrlr, size_t *size);
int nvme_transport_ctrlr_unmap_cmb(struct spdk_nvme_ctrlr *ctrlr);
int nvme_transport_ctrlr_enable_pmr(struct spdk_nvme_ctrlr *ctrlr);
int nvme_transport_ctrlr_disable_pmr(struct spdk_nvme_ctrlr *ctrlr);
void *nvme_transport_ctrlr_map_pmr(struct spdk_nvme_ctrlr *ctrlr, size_t *size);
int nvme_transport_ctrlr_unmap_pmr(struct spdk_nvme_ctrlr *ctrlr);
int nvme_transport_ctrlr_delete_io_qpair(struct spdk_nvme_ctrlr *ctrlr,
		struct spdk_nvme_qpair *qpair);
int nvme_transport_ctrlr_connect_qpair(struct spdk_nvme_ctrlr *ctrlr,
				       struct spdk_nvme_qpair *qpair);
void nvme_transport_ctrlr_disconnect_qpair(struct spdk_nvme_ctrlr *ctrlr,
		struct spdk_nvme_qpair *qpair);
void nvme_transport_qpair_abort_reqs(struct spdk_nvme_qpair *qpair, uint32_t dnr);
int nvme_transport_qpair_reset(struct spdk_nvme_qpair *qpair);
int nvme_transport_qpair_submit_request(struct spdk_nvme_qpair *qpair, struct nvme_request *req);
int32_t nvme_transport_qpair_process_completions(struct spdk_nvme_qpair *qpair,
		uint32_t max_completions);
void nvme_transport_admin_qpair_abort_aers(struct spdk_nvme_qpair *qpair);
int nvme_transport_qpair_iterate_requests(struct spdk_nvme_qpair *qpair,
		int (*iter_fn)(struct nvme_request *req, void *arg),
		void *arg);

struct spdk_nvme_transport_poll_group *nvme_transport_poll_group_create(
	const struct spdk_nvme_transport *transport);
struct spdk_nvme_transport_poll_group *nvme_transport_qpair_get_optimal_poll_group(
	const struct spdk_nvme_transport *transport,
	struct spdk_nvme_qpair *qpair);
int nvme_transport_poll_group_add(struct spdk_nvme_transport_poll_group *tgroup,
				  struct spdk_nvme_qpair *qpair);
int nvme_transport_poll_group_remove(struct spdk_nvme_transport_poll_group *tgroup,
				     struct spdk_nvme_qpair *qpair);
int nvme_transport_poll_group_disconnect_qpair(struct spdk_nvme_qpair *qpair);
int nvme_transport_poll_group_connect_qpair(struct spdk_nvme_qpair *qpair);
int64_t nvme_transport_poll_group_process_completions(struct spdk_nvme_transport_poll_group *tgroup,
		uint32_t completions_per_qpair, spdk_nvme_disconnected_qpair_cb disconnected_qpair_cb);
int nvme_transport_poll_group_destroy(struct spdk_nvme_transport_poll_group *tgroup);
int nvme_transport_poll_group_get_stats(struct spdk_nvme_transport_poll_group *tgroup,
					struct spdk_nvme_transport_poll_group_stat **stats);
void nvme_transport_poll_group_free_stats(struct spdk_nvme_transport_poll_group *tgroup,
		struct spdk_nvme_transport_poll_group_stat *stats);
enum spdk_nvme_transport_type nvme_transport_get_trtype(const struct spdk_nvme_transport
		*transport);
/*
 * Below ref related functions must be called with the global
 *  driver lock held for the multi-process condition.
 *  Within these functions, the per ctrlr ctrlr_lock is also
 *  acquired for the multi-thread condition.
 */
void	nvme_ctrlr_proc_get_ref(struct spdk_nvme_ctrlr *ctrlr);
void	nvme_ctrlr_proc_put_ref(struct spdk_nvme_ctrlr *ctrlr);
int	nvme_ctrlr_get_ref_count(struct spdk_nvme_ctrlr *ctrlr);

static inline bool
_is_page_aligned(uint64_t address, uint64_t page_size)
{
	return (address & (page_size - 1)) == 0;
}

#endif /* __NVME_INTERNAL_H__ */
