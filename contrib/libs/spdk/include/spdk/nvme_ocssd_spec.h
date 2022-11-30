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

/**
 * \file
 * Open-Channel specification definitions
 */

#ifndef SPDK_NVME_OCSSD_SPEC_H
#define SPDK_NVME_OCSSD_SPEC_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "spdk/assert.h"
#include "spdk/nvme_spec.h"

/** A maximum number of LBAs that can be issued by vector I/O commands */
#define SPDK_NVME_OCSSD_MAX_LBAL_ENTRIES	64

struct spdk_ocssd_dev_lba_fmt {
	/**  Contiguous number of bits assigned to Group addressing */
	uint8_t grp_len;

	/** Contiguous number of bits assigned to PU addressing */
	uint8_t pu_len;

	/** Contiguous number of bits assigned to Chunk addressing */
	uint8_t chk_len;

	/** Contiguous number of bits assigned to logical blocks within Chunk */
	uint8_t lbk_len;

	uint8_t reserved[4];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_ocssd_dev_lba_fmt) == 8, "Incorrect size");

struct spdk_ocssd_geometry_data {
	/** Major Version Number */
	uint8_t		mjr;

	/** Minor Version Number */
	uint8_t		mnr;

	uint8_t		reserved1[6];

	/** LBA format */
	struct spdk_ocssd_dev_lba_fmt	lbaf;

	/** Media and Controller Capabilities */
	struct {
		/* Supports the Vector Chunk Copy I/O Command */
		uint32_t	vec_chk_cpy	: 1;

		/* Supports multiple resets when a chunk is in its free state */
		uint32_t	multi_reset	: 1;

		uint32_t	reserved	: 30;
	} mccap;

	uint8_t		reserved2[12];

	/** Wear-level Index Delta Threshold */
	uint8_t		wit;

	uint8_t		reserved3[31];

	/** Number of Groups */
	uint16_t	num_grp;

	/** Number of parallel units per group */
	uint16_t	num_pu;

	/** Number of chunks per parallel unit */
	uint32_t	num_chk;

	/** Chunk Size */
	uint32_t	clba;

	uint8_t		reserved4[52];

	/** Minimum Write Size */
	uint32_t	ws_min;

	/** Optimal Write Size */
	uint32_t	ws_opt;

	/** Cache Minimum Write Size Units */
	uint32_t	mw_cunits;

	/** Maximum Open Chunks */
	uint32_t	maxoc;

	/** Maximum Open Chunks per PU */
	uint32_t	maxocpu;

	uint8_t		reserved5[44];

	/** tRD Typical */
	uint32_t	trdt;

	/** tRD Max */
	uint32_t	trdm;

	/** tWR Typical */
	uint32_t	twrt;

	/** tWR Max */
	uint32_t	twrm;

	/** tCRS Typical */
	uint32_t	tcrst;

	/** tCRS Max */
	uint32_t	tcrsm;

	/** bytes 216-255: reserved for performance related metrics */
	uint8_t		reserved6[40];

	uint8_t		reserved7[3071 - 255];

	/** bytes 3072-4095: Vendor Specific */
	uint8_t		vs[4095 - 3071];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_ocssd_geometry_data) == 4096, "Incorrect size");

struct spdk_ocssd_chunk_information_entry {
	/** Chunk State */
	struct {
		/** if set to 1 chunk is free */
		uint8_t free		: 1;

		/** if set to 1 chunk is closed */
		uint8_t closed		: 1;

		/** if set to 1 chunk is open */
		uint8_t open		: 1;

		/** if set to 1 chunk is offline */
		uint8_t offline		: 1;

		uint8_t reserved	: 4;
	} cs;

	/** Chunk Type */
	struct {
		/** If set to 1 chunk must be written sequentially */
		uint8_t seq_write		: 1;

		/** If set to 1 chunk allows random writes */
		uint8_t rnd_write		: 1;

		uint8_t reserved1		: 2;

		/**
		 * If set to 1 chunk deviates from the chunk size reported
		 * in identify geometry command.
		 */
		uint8_t size_deviate		: 1;

		uint8_t reserved2		: 3;
	} ct;

	/** Wear-level Index */
	uint8_t wli;

	uint8_t reserved[5];

	/** Starting LBA */
	uint64_t slba;

	/** Number of blocks in chunk */
	uint64_t cnlb;

	/** Write Pointer */
	uint64_t wp;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_ocssd_chunk_information_entry) == 32, "Incorrect size");

struct spdk_ocssd_chunk_notification_entry {

	/**
	 * This is a 64-bit incrementing notification count, indicating a
	 * unique identifier for this notification. The counter begins at 1h
	 * and is incremented for each unique event
	 */
	uint64_t		nc;

	/** This field points to the chunk that has its state updated */
	uint64_t		lba;

	/**
	 * This field indicates the namespace id that the event is associated
	 * with
	 */
	uint32_t		nsid;

	/** Field that indicate the state of the block */
	struct {

		/**
		 * If set to 1, then the error rate of the chunk has been
		 * changed to low
		 */
		uint8_t error_rate_low : 1;

		/**
		 * If set to 1, then the error rate of the chunk has been
		 * changed to medium
		 */
		uint8_t error_rate_medium : 1;

		/**
		 * If set to 1, then the error rate of the chunk has been
		 * changed to high
		 */
		uint8_t error_rate_high : 1;

		/**
		 * If set to 1, then the error rate of the chunk has been
		 * changed to unrecoverable
		 */
		uint8_t unrecoverable : 1;

		/**
		 * If set to 1, then the chunk has been refreshed by the
		 * device
		 */
		uint8_t refreshed : 1;

		uint8_t rsvd : 3;

		/**
		 * If set to 1 then the chunk's wear-level index is outside
		 * the average wear-level index threshold defined by the
		 * controller
		 */
		uint8_t wit_exceeded : 1;

		uint8_t rsvd2 : 7;
	} state;

	/**
	 * The address provided is covering either logical block, chunk, or
	 * parallel unit
	 */
	struct {

		/** If set to 1, the LBA covers the logical block */
		uint8_t lblk : 1;

		/** If set to 1, the LBA covers the respecting chunk */
		uint8_t chunk : 1;

		/**
		 * If set to 1, the LBA covers the respecting parallel unit
		 * including all chunks
		 */
		uint8_t pu : 1;

		uint8_t rsvd : 5;
	} mask;

	uint8_t			rsvd[9];

	/**
	 * This field indicates the number of logical chunks to be written.
	 * This is a 0's based value. This field is only valid if mask bit 0 is
	 * set. The number of blocks addressed shall not be outside the boundary
	 * of the specified chunk.
	 */
	uint16_t		nlb;

	uint8_t			rsvd2[30];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_ocssd_chunk_notification_entry) == 64, "Incorrect size");

/**
 * Vector completion queue entry
 */
struct spdk_ocssd_vector_cpl {
	/* dword 0,1 */
	uint64_t		lba_status;	/* completion status bit array */

	/* dword 2 */
	uint16_t		sqhd;	/* submission queue head pointer */
	uint16_t		sqid;	/* submission queue identifier */

	/* dword 3 */
	uint16_t		cid;	/* command identifier */
	struct spdk_nvme_status	status;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_ocssd_vector_cpl) == 16, "Incorrect size");

/**
 * OCSSD admin command set opcodes
 */
enum spdk_ocssd_admin_opcode {
	SPDK_OCSSD_OPC_GEOMETRY	= 0xE2
};

/**
 * OCSSD I/O command set opcodes
 */
enum spdk_ocssd_io_opcode {
	SPDK_OCSSD_OPC_VECTOR_RESET	= 0x90,
	SPDK_OCSSD_OPC_VECTOR_WRITE	= 0x91,
	SPDK_OCSSD_OPC_VECTOR_READ	= 0x92,
	SPDK_OCSSD_OPC_VECTOR_COPY	= 0x93
};

/**
 * Log page identifiers for SPDK_NVME_OPC_GET_LOG_PAGE
 */
enum spdk_ocssd_log_page {
	/** Chunk Information */
	SPDK_OCSSD_LOG_CHUNK_INFO		= 0xCA,

	/** Chunk Notification Log */
	SPDK_OCSSD_LOG_CHUNK_NOTIFICATION	= 0xD0,
};

/**
 * OCSSD feature identifiers
 * Defines OCSSD specific features that may be configured with Set Features and
 * retrieved with Get Features.
 */
enum spdk_ocssd_feat {
	/**  Media Feedback feature identifier */
	SPDK_OCSSD_FEAT_MEDIA_FEEDBACK	= 0xCA
};

/**
 * OCSSD media error status codes extension.
 * Additional error codes for status code type “2h” (media errors)
 */
enum spdk_ocssd_media_error_status_code {
	/**
	 * The chunk was either marked offline by the reset or the state
	 * of the chunk is already offline.
	 */
	SPDK_OCSSD_SC_OFFLINE_CHUNK			= 0xC0,

	/**
	 * Invalid reset if chunk state is either “Free” or “Open”
	 */
	SPDK_OCSSD_SC_INVALID_RESET			= 0xC1,

	/**
	 * Write failed, chunk remains open.
	 * Host should proceed to write to next write unit.
	 */
	SPDK_OCSSD_SC_WRITE_FAIL_WRITE_NEXT_UNIT	= 0xF0,

	/**
	 * The writes ended prematurely. The chunk state is set to closed.
	 * The host can read up to the value of the write pointer.
	 */
	SPDK_OCSSD_SC_WRITE_FAIL_CHUNK_EARLY_CLOSE	= 0xF1,

	/**
	 * The write corresponds to a write out of order within an open
	 * chunk or the write is to a closed or offline chunk.
	 */
	SPDK_OCSSD_SC_OUT_OF_ORDER_WRITE		= 0xF2,

	/**
	 * The data retrieved is nearing its limit for reading.
	 * The limit is vendor specific, and only provides a hint
	 * to the host that should refresh its data in the future.
	 */
	SPDK_OCSSD_SC_READ_HIGH_ECC			= 0xD0,
};

#define SPDK_OCSSD_IO_FLAGS_LIMITED_RETRY (1U << 31)

#ifdef __cplusplus
}
#endif

#endif
