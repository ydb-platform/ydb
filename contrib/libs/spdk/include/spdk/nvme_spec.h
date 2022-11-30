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
 * NVMe specification definitions
 */

#ifndef SPDK_NVME_SPEC_H
#define SPDK_NVME_SPEC_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "spdk/assert.h"

/**
 * Use to mark a command to apply to all namespaces, or to retrieve global
 *  log pages.
 */
#define SPDK_NVME_GLOBAL_NS_TAG		((uint32_t)0xFFFFFFFF)

#define SPDK_NVME_MAX_IO_QUEUES		(65535)

#define SPDK_NVME_ADMIN_QUEUE_MIN_ENTRIES	2
#define SPDK_NVME_ADMIN_QUEUE_MAX_ENTRIES	4096

#define SPDK_NVME_IO_QUEUE_MIN_ENTRIES		2
#define SPDK_NVME_IO_QUEUE_MAX_ENTRIES		65536

/**
 * Indicates the maximum number of range sets that may be specified
 *  in the dataset management command.
 */
#define SPDK_NVME_DATASET_MANAGEMENT_MAX_RANGES	256

/**
 * Maximum number of blocks that may be specified in a single dataset management range.
 */
#define SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS	0xFFFFFFFFu

union spdk_nvme_cap_register {
	uint64_t	raw;
	struct {
		/** maximum queue entries supported */
		uint32_t mqes		: 16;

		/** contiguous queues required */
		uint32_t cqr		: 1;

		/** arbitration mechanism supported */
		uint32_t ams		: 2;

		uint32_t reserved1	: 5;

		/** timeout */
		uint32_t to		: 8;

		/** doorbell stride */
		uint32_t dstrd		: 4;

		/** NVM subsystem reset supported */
		uint32_t nssrs		: 1;

		/** command sets supported */
		uint32_t css		: 8;

		/** boot partition support */
		uint32_t bps		: 1;

		uint32_t reserved2	: 2;

		/** memory page size minimum */
		uint32_t mpsmin		: 4;

		/** memory page size maximum */
		uint32_t mpsmax		: 4;

		/** persistent memory region supported */
		uint32_t pmrs		: 1;

		uint32_t reserved3	: 7;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_cap_register) == 8, "Incorrect size");

/**
 * I/O Command Set Selected
 *
 * Only a single command set is defined as of NVMe 1.3 (NVM). Later, it became
 * possible to disable I/O Command Sets, that is, configuring it to only use the
 * Admin Command Set. With 1.4c and Namespace Types, additional I/O Command Sets
 * are available.
 */
enum spdk_nvme_cc_css {
	SPDK_NVME_CC_CSS_NVM		= 0x0,	/**< NVM command set */
	SPDK_NVME_CC_CSS_IOCS		= 0x6,	/**< One or more I/O command sets */
	SPDK_NVME_CC_CSS_NOIO		= 0x7,	/**< No I/O, only admin */
};

#define SPDK_NVME_CAP_CSS_NVM (1u << SPDK_NVME_CC_CSS_NVM) /**< NVM command set supported */
#define SPDK_NVME_CAP_CSS_IOCS (1u << SPDK_NVME_CC_CSS_IOCS) /**< One or more I/O Command sets supported */
#define SPDK_NVME_CAP_CSS_NOIO (1u << SPDK_NVME_CC_CSS_NOIO) /**< No I/O, only admin */

union spdk_nvme_cc_register {
	uint32_t	raw;
	struct {
		/** enable */
		uint32_t en		: 1;

		uint32_t reserved1	: 3;

		/** i/o command set selected */
		uint32_t css		: 3;

		/** memory page size */
		uint32_t mps		: 4;

		/** arbitration mechanism selected */
		uint32_t ams		: 3;

		/** shutdown notification */
		uint32_t shn		: 2;

		/** i/o submission queue entry size */
		uint32_t iosqes		: 4;

		/** i/o completion queue entry size */
		uint32_t iocqes		: 4;

		uint32_t reserved2	: 8;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_cc_register) == 4, "Incorrect size");

enum spdk_nvme_shn_value {
	SPDK_NVME_SHN_NORMAL		= 0x1,
	SPDK_NVME_SHN_ABRUPT		= 0x2,
};

union spdk_nvme_csts_register {
	uint32_t	raw;
	struct {
		/** ready */
		uint32_t rdy		: 1;

		/** controller fatal status */
		uint32_t cfs		: 1;

		/** shutdown status */
		uint32_t shst		: 2;

		/** NVM subsystem reset occurred */
		uint32_t nssro		: 1;

		/** Processing paused */
		uint32_t pp		: 1;

		uint32_t reserved1	: 26;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_csts_register) == 4, "Incorrect size");

enum spdk_nvme_shst_value {
	SPDK_NVME_SHST_NORMAL		= 0x0,
	SPDK_NVME_SHST_OCCURRING	= 0x1,
	SPDK_NVME_SHST_COMPLETE		= 0x2,
};

union spdk_nvme_aqa_register {
	uint32_t	raw;
	struct {
		/** admin submission queue size */
		uint32_t asqs		: 12;

		uint32_t reserved1	: 4;

		/** admin completion queue size */
		uint32_t acqs		: 12;

		uint32_t reserved2	: 4;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_aqa_register) == 4, "Incorrect size");

union spdk_nvme_vs_register {
	uint32_t	raw;
	struct {
		/** indicates the tertiary version */
		uint32_t ter		: 8;
		/** indicates the minor version */
		uint32_t mnr		: 8;
		/** indicates the major version */
		uint32_t mjr		: 16;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_vs_register) == 4, "Incorrect size");

/** Generate raw version in the same format as \ref spdk_nvme_vs_register for comparison. */
#define SPDK_NVME_VERSION(mjr, mnr, ter) \
	(((uint32_t)(mjr) << 16) | \
	((uint32_t)(mnr) << 8) | \
	(uint32_t)(ter))

/* Test that the shifts are correct */
SPDK_STATIC_ASSERT(SPDK_NVME_VERSION(1, 0, 0) == 0x00010000, "version macro error");
SPDK_STATIC_ASSERT(SPDK_NVME_VERSION(1, 2, 1) == 0x00010201, "version macro error");

union spdk_nvme_cmbloc_register {
	uint32_t	raw;
	struct {
		/** indicator of BAR which contains controller memory buffer(CMB) */
		uint32_t bir		: 3;
		uint32_t reserved1	: 9;
		/** offset of CMB in multiples of the size unit */
		uint32_t ofst		: 20;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_cmbloc_register) == 4, "Incorrect size");

union spdk_nvme_cmbsz_register {
	uint32_t	raw;
	struct {
		/** support submission queues in CMB */
		uint32_t sqs		: 1;
		/** support completion queues in CMB */
		uint32_t cqs		: 1;
		/** support PRP and SGLs lists in CMB */
		uint32_t lists		: 1;
		/** support read data and metadata in CMB */
		uint32_t rds		: 1;
		/** support write data and metadata in CMB */
		uint32_t wds		: 1;
		uint32_t reserved1	: 3;
		/** indicates the granularity of the size unit */
		uint32_t szu		: 4;
		/** size of CMB in multiples of the size unit */
		uint32_t sz		: 20;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_cmbsz_register) == 4, "Incorrect size");

union spdk_nvme_cmbmsc_register {
	uint64_t	raw;
	struct {
		/** capability registers enabled */
		uint64_t cre		: 1;

		/** controller memory space enable */
		uint64_t cmse		: 1;

		uint64_t reserved	: 10;

		/** controller base address */
		uint64_t cba		: 52;
	} bits;

};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_cmbmsc_register) == 8, "Incorrect size");

union spdk_nvme_cmbsts_register {
	uint32_t	raw;
	struct {
		/** controller base address invalid */
		uint32_t cbai		: 1;

		uint32_t reserved	: 31;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_cmbsts_register) == 4, "Incorrect size");

union spdk_nvme_pmrcap_register {
	uint32_t	raw;
	struct {
		uint32_t reserved1	: 3;

		/** read data support */
		uint32_t rds		: 1;

		/** write data support */
		uint32_t wds		: 1;

		/** base indicator register */
		uint32_t bir		: 3;

		/**
		 * persistent memory region time units
		 * 00b: 500 milliseconds
		 * 01b: minutes
		 */
		uint32_t pmrtu		: 2;

		/** persistent memory region write barrier mechanisms */
		uint32_t pmrwbm		: 4;

		uint32_t reserved2	: 2;

		/** persistent memory region timeout */
		uint32_t pmrto		: 8;

		/** controller memory space supported */
		uint32_t cmss		: 1;

		uint32_t reserved3	: 7;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_pmrcap_register) == 4, "Incorrect size");

union spdk_nvme_pmrctl_register {
	uint32_t	raw;
	struct {
		/** enable */
		uint32_t en		: 1;

		uint32_t reserved	: 31;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_pmrctl_register) == 4, "Incorrect size");

union spdk_nvme_pmrsts_register {
	uint32_t	raw;
	struct {
		/** err */
		uint32_t err		: 8;

		/** not ready */
		uint32_t nrdy		: 1;

		/**
		 * health status
		 * 000b: Normal Operation
		 * 001b: Restore Error
		 * 010b: Read Only
		 * 011b: Unreliable
		 */
		uint32_t hsts		: 3;

		/** controller base address invalid */
		uint32_t cbai		: 1;

		uint32_t reserved	: 19;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_pmrsts_register) == 4, "Incorrect size");

union spdk_nvme_pmrebs_register {
	uint32_t	raw;
	struct {
		/**
		 * pmr elasicity buffer size units
		 * 0h: Bytes
		 * 1h: 1 KiB
		 * 2h: 1 MiB
		 * 3h: 1 GiB
		 */
		uint32_t pmrszu		: 4;

		/** read bypass behavior */
		uint32_t rbb		: 1;

		uint32_t reserved	: 3;

		/** pmr elasticity buffer size base */
		uint32_t pmrwbz		: 24;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_pmrebs_register) == 4, "Incorrect size");

union spdk_nvme_pmrswtp_register {
	uint32_t	raw;
	struct {
		/**
		 * pmr sustained write throughput units
		 * 0h: Bytes per second
		 * 1h: 1 KiB / s
		 * 2h: 1 MiB / s
		 * 3h: 1 GiB / s
		 */
		uint32_t pmrswtu	: 4;

		uint32_t reserved	: 4;

		/** pmr sustained write throughput */
		uint32_t pmrswtv	: 24;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_pmrswtp_register) == 4, "Incorrect size");

union spdk_nvme_pmrmscl_register {
	uint32_t	raw;
	struct {
		uint32_t reserved1	: 1;

		/** controller memory space enable */
		uint32_t cmse		: 1;

		uint32_t reserved2	: 10;

		/** controller base address */
		uint32_t cba		: 20;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_pmrmscl_register) == 4, "Incorrect size");

/** Boot partition information */
union spdk_nvme_bpinfo_register	{
	uint32_t	raw;
	struct {
		/** Boot partition size in 128KB multiples */
		uint32_t bpsz		: 15;

		uint32_t reserved1	: 9;

		/**
		 * Boot read status
		 * 00b: No Boot Partition read operation requested
		 * 01b: Boot Partition read in progress
		 * 10b: Boot Partition read completed successfully
		 * 11b: Error completing Boot Partition read
		 */
		uint32_t brs		: 2;

		uint32_t reserved2	: 5;

		/** Active Boot Partition ID */
		uint32_t abpid		: 1;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_bpinfo_register) == 4, "Incorrect size");

/** Boot partition read select */
union spdk_nvme_bprsel_register {
	uint32_t	raw;
	struct {
		/** Boot partition read size in multiples of 4KB */
		uint32_t bprsz		: 10;

		/** Boot partition read offset in multiples of 4KB */
		uint32_t bprof		: 20;

		uint32_t reserved	: 1;

		/** Boot Partition Identifier */
		uint32_t bpid		: 1;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_bprsel_register) == 4, "Incorrect size");

/** Value to write to NSSR to indicate a NVM subsystem reset ("NVMe") */
#define SPDK_NVME_NSSR_VALUE	0x4E564D65

struct spdk_nvme_registers {
	/** controller capabilities */
	union spdk_nvme_cap_register	cap;

	/** version of NVMe specification */
	union spdk_nvme_vs_register	vs;
	uint32_t			intms; /* interrupt mask set */
	uint32_t			intmc; /* interrupt mask clear */

	/** controller configuration */
	union spdk_nvme_cc_register	cc;

	uint32_t			reserved1;
	union spdk_nvme_csts_register	csts; /* controller status */
	uint32_t			nssr; /* NVM subsystem reset */

	/** admin queue attributes */
	union spdk_nvme_aqa_register	aqa;

	uint64_t			asq; /* admin submission queue base addr */
	uint64_t			acq; /* admin completion queue base addr */
	/** controller memory buffer location */
	union spdk_nvme_cmbloc_register	cmbloc;
	/** controller memory buffer size */
	union spdk_nvme_cmbsz_register	cmbsz;

	/** boot partition information */
	union spdk_nvme_bpinfo_register	bpinfo;

	/** boot partition read select */
	union spdk_nvme_bprsel_register	bprsel;

	/** boot partition memory buffer location (must be 4KB aligned) */
	uint64_t			bpmbl;

	/** controller memory buffer memory space control */
	union spdk_nvme_cmbmsc_register	cmbmsc;

	/** controller memory buffer status */
	union spdk_nvme_cmbsts_register	cmbsts;

	uint32_t			reserved2[0x369];

	/** persistent memory region capabilities */
	union spdk_nvme_pmrcap_register	pmrcap;

	/** persistent memory region control */
	union spdk_nvme_pmrctl_register	pmrctl;

	/** persistent memory region status */
	union spdk_nvme_pmrsts_register	pmrsts;

	/** persistent memory region elasticity buffer size */
	union spdk_nvme_pmrebs_register	pmrebs;

	/** persistent memory region sustained write throughput */
	union spdk_nvme_pmrswtp_register	pmrswtp;

	/** persistent memory region memory space control lower */
	union spdk_nvme_pmrmscl_register	pmrmscl;

	uint32_t			pmrmscu; /* persistent memory region memory space control upper */

	uint32_t			reserved3[0x79];

	struct {
		uint32_t	sq_tdbl;	/* submission queue tail doorbell */
		uint32_t	cq_hdbl;	/* completion queue head doorbell */
	} doorbell[1];
};

/* NVMe controller register space offsets */
SPDK_STATIC_ASSERT(0x00 == offsetof(struct spdk_nvme_registers, cap),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0x08 == offsetof(struct spdk_nvme_registers, vs), "Incorrect register offset");
SPDK_STATIC_ASSERT(0x0C == offsetof(struct spdk_nvme_registers, intms),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0x10 == offsetof(struct spdk_nvme_registers, intmc),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0x14 == offsetof(struct spdk_nvme_registers, cc), "Incorrect register offset");
SPDK_STATIC_ASSERT(0x1C == offsetof(struct spdk_nvme_registers, csts), "Incorrect register offset");
SPDK_STATIC_ASSERT(0x20 == offsetof(struct spdk_nvme_registers, nssr), "Incorrect register offset");
SPDK_STATIC_ASSERT(0x24 == offsetof(struct spdk_nvme_registers, aqa), "Incorrect register offset");
SPDK_STATIC_ASSERT(0x28 == offsetof(struct spdk_nvme_registers, asq), "Incorrect register offset");
SPDK_STATIC_ASSERT(0x30 == offsetof(struct spdk_nvme_registers, acq), "Incorrect register offset");
SPDK_STATIC_ASSERT(0x38 == offsetof(struct spdk_nvme_registers, cmbloc),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0x3C == offsetof(struct spdk_nvme_registers, cmbsz),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0x40 == offsetof(struct spdk_nvme_registers, bpinfo),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0x44 == offsetof(struct spdk_nvme_registers, bprsel),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0x48 == offsetof(struct spdk_nvme_registers, bpmbl),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0x50 == offsetof(struct spdk_nvme_registers, cmbmsc),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0x58 == offsetof(struct spdk_nvme_registers, cmbsts),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0xE00 == offsetof(struct spdk_nvme_registers, pmrcap),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0xE04 == offsetof(struct spdk_nvme_registers, pmrctl),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0xE08 == offsetof(struct spdk_nvme_registers, pmrsts),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0xE0C == offsetof(struct spdk_nvme_registers, pmrebs),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0xE10 == offsetof(struct spdk_nvme_registers, pmrswtp),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0xE14 == offsetof(struct spdk_nvme_registers, pmrmscl),
		   "Incorrect register offset");
SPDK_STATIC_ASSERT(0xE18 == offsetof(struct spdk_nvme_registers, pmrmscu),
		   "Incorrect register offset");

enum spdk_nvme_sgl_descriptor_type {
	SPDK_NVME_SGL_TYPE_DATA_BLOCK		= 0x0,
	SPDK_NVME_SGL_TYPE_BIT_BUCKET		= 0x1,
	SPDK_NVME_SGL_TYPE_SEGMENT		= 0x2,
	SPDK_NVME_SGL_TYPE_LAST_SEGMENT		= 0x3,
	SPDK_NVME_SGL_TYPE_KEYED_DATA_BLOCK	= 0x4,
	SPDK_NVME_SGL_TYPE_TRANSPORT_DATA_BLOCK	= 0x5,
	/* 0x6 - 0xE reserved */
	SPDK_NVME_SGL_TYPE_VENDOR_SPECIFIC	= 0xF
};

enum spdk_nvme_sgl_descriptor_subtype {
	SPDK_NVME_SGL_SUBTYPE_ADDRESS		= 0x0,
	SPDK_NVME_SGL_SUBTYPE_OFFSET		= 0x1,
	SPDK_NVME_SGL_SUBTYPE_TRANSPORT		= 0xa,
};

struct __attribute__((packed)) spdk_nvme_sgl_descriptor {
	uint64_t address;
	union {
		struct {
			uint8_t reserved[7];
			uint8_t subtype	: 4;
			uint8_t type	: 4;
		} generic;

		struct {
			uint32_t length;
			uint8_t reserved[3];
			uint8_t subtype	: 4;
			uint8_t type	: 4;
		} unkeyed;

		struct {
			uint64_t length		: 24;
			uint64_t key		: 32;
			uint64_t subtype	: 4;
			uint64_t type		: 4;
		} keyed;
	};
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_sgl_descriptor) == 16, "Incorrect size");

enum spdk_nvme_psdt_value {
	SPDK_NVME_PSDT_PRP		= 0x0,
	SPDK_NVME_PSDT_SGL_MPTR_CONTIG	= 0x1,
	SPDK_NVME_PSDT_SGL_MPTR_SGL	= 0x2,
	SPDK_NVME_PSDT_RESERVED		= 0x3
};

/**
 * Submission queue priority values for Create I/O Submission Queue Command.
 *
 * Only valid for weighted round robin arbitration method.
 */
enum spdk_nvme_qprio {
	SPDK_NVME_QPRIO_URGENT		= 0x0,
	SPDK_NVME_QPRIO_HIGH		= 0x1,
	SPDK_NVME_QPRIO_MEDIUM		= 0x2,
	SPDK_NVME_QPRIO_LOW		= 0x3
};

#define SPDK_NVME_CREATE_IO_SQ_QPRIO_MASK	0x3

/**
 * Optional Arbitration Mechanism Supported by the controller.
 *
 * Two bits for CAP.AMS (18:17) field are set to '1' when the controller supports.
 * There is no bit for AMS_RR where all controllers support and set to 0x0 by default.
 */
enum spdk_nvme_cap_ams {
	SPDK_NVME_CAP_AMS_WRR		= 0x1,	/**< weighted round robin */
	SPDK_NVME_CAP_AMS_VS		= 0x2,	/**< vendor specific */
};

/**
 * Arbitration Mechanism Selected to the controller.
 *
 * Value 0x2 to 0x6 is reserved.
 */
enum spdk_nvme_cc_ams {
	SPDK_NVME_CC_AMS_RR		= 0x0,	/**< default round robin */
	SPDK_NVME_CC_AMS_WRR		= 0x1,	/**< weighted round robin */
	SPDK_NVME_CC_AMS_VS		= 0x7,	/**< vendor specific */
};

/**
 * Fused Operation
 */
enum spdk_nvme_cmd_fuse {
	SPDK_NVME_CMD_FUSE_NONE		= 0x0,	/**< normal operation */
	SPDK_NVME_CMD_FUSE_FIRST	= 0x1,	/**< fused operation, first command */
	SPDK_NVME_CMD_FUSE_SECOND	= 0x2,	/**< fused operation, second command */
	SPDK_NVME_CMD_FUSE_MASK		= 0x3,  /**< fused operation flags mask */
};

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_ARBITRATION
 */
union spdk_nvme_feat_arbitration {
	uint32_t raw;
	struct {
		/** Arbitration Burst */
		uint32_t ab : 3;

		uint32_t reserved : 5;

		/** Low Priority Weight */
		uint32_t lpw : 8;

		/** Medium Priority Weight */
		uint32_t mpw : 8;

		/** High Priority Weight */
		uint32_t hpw : 8;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_arbitration) == 4, "Incorrect size");

#define SPDK_NVME_ARBITRATION_BURST_UNLIMITED	0x7

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_POWER_MANAGEMENT
 */
union spdk_nvme_feat_power_management {
	uint32_t raw;
	struct {
		/** Power State */
		uint32_t ps : 5;

		/** Workload Hint */
		uint32_t wh : 3;

		uint32_t reserved : 24;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_power_management) == 4, "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_LBA_RANGE_TYPE
 */
union spdk_nvme_feat_lba_range_type {
	uint32_t raw;
	struct {
		/** Number of LBA Ranges */
		uint32_t num : 6;

		uint32_t reserved : 26;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_lba_range_type) == 4, "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_TEMPERATURE_THRESHOLD
 */
union spdk_nvme_feat_temperature_threshold {
	uint32_t raw;
	struct {
		/** Temperature Threshold */
		uint32_t tmpth : 16;

		/** Threshold Temperature Select */
		uint32_t tmpsel : 4;

		/** Threshold Type Select */
		uint32_t thsel : 2;

		uint32_t reserved : 10;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_temperature_threshold) == 4, "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_ERROR_RECOVERY
 */
union spdk_nvme_feat_error_recovery {
	uint32_t raw;
	struct {
		/** Time Limited Error Recovery */
		uint32_t tler : 16;

		/** Deallocated or Unwritten Logical Block Error Enable */
		uint32_t dulbe : 1;

		uint32_t reserved : 15;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_error_recovery) == 4, "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_VOLATILE_WRITE_CACHE
 */
union spdk_nvme_feat_volatile_write_cache {
	uint32_t raw;
	struct {
		/** Volatile Write Cache Enable */
		uint32_t wce : 1;

		uint32_t reserved : 31;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_volatile_write_cache) == 4, "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_NUMBER_OF_QUEUES
 */
union spdk_nvme_feat_number_of_queues {
	uint32_t raw;
	struct {
		/** Number of I/O Submission Queues Requested */
		uint32_t nsqr : 16;

		/** Number of I/O Completion Queues Requested */
		uint32_t ncqr : 16;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_number_of_queues) == 4, "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_INTERRUPT_COALESCING
 */
union spdk_nvme_feat_interrupt_coalescing {
	uint32_t raw;
	struct {
		/** Aggregation Threshold */
		uint32_t thr : 8;

		/** Aggregration time */
		uint32_t time : 8;

		uint32_t reserved : 16;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_interrupt_coalescing) == 4, "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_INTERRUPT_VECTOR_CONFIGURATION
 */
union spdk_nvme_feat_interrupt_vector_configuration {
	uint32_t raw;
	struct {
		/** Interrupt Vector */
		uint32_t iv : 16;

		/** Coalescing Disable */
		uint32_t cd : 1;

		uint32_t reserved : 15;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_interrupt_vector_configuration) == 4,
		   "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_WRITE_ATOMICITY
 */
union spdk_nvme_feat_write_atomicity {
	uint32_t raw;
	struct {
		/** Disable Normal */
		uint32_t dn : 1;

		uint32_t reserved : 31;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_write_atomicity) == 4, "Incorrect size");

union spdk_nvme_critical_warning_state {
	uint8_t		raw;

	struct {
		uint8_t	available_spare		: 1;
		uint8_t	temperature		: 1;
		uint8_t	device_reliability	: 1;
		uint8_t	read_only		: 1;
		uint8_t	volatile_memory_backup	: 1;
		uint8_t	reserved		: 3;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_critical_warning_state) == 1, "Incorrect size");

/**
 * Data used by Set Features / Get Features \ref SPDK_NVME_FEAT_ASYNC_EVENT_CONFIGURATION
 */
union spdk_nvme_feat_async_event_configuration {
	uint32_t raw;
	struct {
		union spdk_nvme_critical_warning_state crit_warn;
		uint8_t ns_attr_notice		: 1;
		uint8_t fw_activation_notice	: 1;
		uint8_t telemetry_log_notice	: 1;
		uint8_t ana_change_notice	: 1;
		uint8_t reserved1		: 4;
		uint16_t reserved2		: 15;
		/** Discovery log change (refer to the NVMe over Fabrics specification) */
		uint16_t discovery_log_change_notice	: 1;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_async_event_configuration) == 4, "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_AUTONOMOUS_POWER_STATE_TRANSITION
 */
union spdk_nvme_feat_autonomous_power_state_transition {
	uint32_t raw;
	struct {
		/** Autonomous Power State Transition Enable */
		uint32_t apste : 1;

		uint32_t reserved : 31;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_autonomous_power_state_transition) == 4,
		   "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_HOST_MEM_BUFFER
 */
union spdk_nvme_feat_host_mem_buffer {
	uint32_t raw;
	struct {
		/** Enable Host Memory */
		uint32_t ehm : 1;

		/** Memory Return */
		uint32_t mr : 1;

		uint32_t reserved : 30;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_host_mem_buffer) == 4, "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_KEEP_ALIVE_TIMER
 */
union spdk_nvme_feat_keep_alive_timer {
	uint32_t raw;
	struct {
		/** Keep Alive Timeout */
		uint32_t kato : 32;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_keep_alive_timer) == 4, "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_HOST_CONTROLLED_THERMAL_MANAGEMENT
 */
union spdk_nvme_feat_host_controlled_thermal_management {
	uint32_t raw;
	struct {
		/** Thermal Management Temperature 2 */
		uint32_t tmt2 : 16;

		/** Thermal Management Temperature 1 */
		uint32_t tmt1 : 16;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_host_controlled_thermal_management) == 4,
		   "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_NON_OPERATIONAL_POWER_STATE_CONFIG
 */
union spdk_nvme_feat_non_operational_power_state_config {
	uint32_t raw;
	struct {
		/** Non-Operational Power State Permissive Mode Enable */
		uint32_t noppme : 1;

		uint32_t reserved : 31;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_non_operational_power_state_config) == 4,
		   "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_SOFTWARE_PROGRESS_MARKER
 */
union spdk_nvme_feat_software_progress_marker {
	uint32_t raw;
	struct {
		/** Pre-boot Software Load Count */
		uint32_t pbslc : 8;

		uint32_t reserved : 24;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_software_progress_marker) == 4, "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_HOST_IDENTIFIER
 */
union spdk_nvme_feat_host_identifier {
	uint32_t raw;
	struct {
		/** Enable Extended Host Identifier */
		uint32_t exhid : 1;

		uint32_t reserved : 31;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_host_identifier) == 4, "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_HOST_RESERVE_MASK
 */
union spdk_nvme_feat_reservation_notification_mask {
	uint32_t raw;
	struct {
		uint32_t reserved1 : 1;
		/* Mask Registration Preempted Notification */
		uint32_t regpre    : 1;
		/* Mask Reservation Released Notification */
		uint32_t resrel    : 1;
		/* Mask Reservation Preempted Notification */
		uint32_t respre    : 1;
		uint32_t reserved2 : 28;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_reservation_notification_mask) == 4,
		   "Incorrect size");

/**
 * Data used by Set Features/Get Features \ref SPDK_NVME_FEAT_HOST_RESERVE_PERSIST
 */
union spdk_nvme_feat_reservation_persistence {
	uint32_t raw;
	struct {
		/* Persist Through Power Loss */
		uint32_t ptpl      : 1;
		uint32_t reserved  : 31;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_feat_reservation_persistence) == 4, "Incorrect size");

union spdk_nvme_cmd_cdw10 {
	uint32_t raw;
	struct {
		/* Controller or Namespace Structure */
		uint32_t cns       : 8;
		uint32_t reserved  : 8;
		/* Controller Identifier */
		uint32_t cntid     : 16;
	} identify;

	struct {
		/* Log Page Identifier */
		uint32_t lid       : 8;
		/* Log Specific Field */
		uint32_t lsp       : 4;
		uint32_t reserved  : 3;
		/* Retain Asynchronous Event */
		uint32_t rae       : 1;
		/* Number of Dwords Lower */
		uint32_t numdl     : 16;
	} get_log_page;

	struct {
		/* Submission Queue Identifier */
		uint32_t sqid      : 16;
		/* Command Identifier */
		uint32_t cid       : 16;
	} abort;

	struct {
		/* NVMe Security Specific Field */
		uint32_t nssf      : 8;
		/* SP Specific 0 */
		uint32_t spsp0     : 8;
		/* SP Specific 1 */
		uint32_t spsp1     : 8;
		/* Security Protocol */
		uint32_t secp      : 8;
	} sec_send_recv;

	struct {
		/* Queue Identifier */
		uint32_t qid       : 16;
		/* Queue Size */
		uint32_t qsize     : 16;
	} create_io_q;

	struct {
		/* Queue Identifier */
		uint32_t qid       : 16;
		uint32_t reserved  : 16;
	} delete_io_q;

	struct {
		/* Feature Identifier */
		uint32_t fid       : 8;
		/* Select */
		uint32_t sel       : 3;
		uint32_t reserved  : 21;
	} get_features;

	struct {
		/* Feature Identifier */
		uint32_t fid       : 8;
		uint32_t reserved  : 23;
		/* Save */
		uint32_t sv        : 1;
	} set_features;

	struct {
		/* Select */
		uint32_t sel      : 4;
		uint32_t reserved : 28;
	} ns_attach;

	struct {
		/* Select */
		uint32_t sel      : 4;
		uint32_t reserved : 28;
	} ns_manage;

	struct {
		/* Number of Ranges */
		uint32_t nr       : 8;
		uint32_t reserved : 24;
	} dsm;

	struct {
		/* Reservation Register Action */
		uint32_t rrega     : 3;
		/* Ignore Existing Key */
		uint32_t iekey     : 1;
		uint32_t reserved  : 26;
		/* Change Persist Through Power Loss State */
		uint32_t cptpl     : 2;
	} resv_register;

	struct {
		/* Reservation Release Action */
		uint32_t rrela     : 3;
		/* Ignore Existing Key */
		uint32_t iekey     : 1;
		uint32_t reserved1 : 4;
		/* Reservation Type */
		uint32_t rtype     : 8;
		uint32_t reserved2 : 16;
	} resv_release;

	struct {
		/* Reservation Acquire Action */
		uint32_t racqa     : 3;
		/* Ignore Existing Key */
		uint32_t iekey     : 1;
		uint32_t reserved1 : 4;
		/* Reservation Type */
		uint32_t rtype     : 8;
		uint32_t reserved2 : 16;
	} resv_acquire;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_cmd_cdw10) == 4, "Incorrect size");

union spdk_nvme_cmd_cdw11 {
	uint32_t raw;

	struct {
		/* NVM Set Identifier */
		uint32_t nvmsetid  : 16;
		uint32_t reserved  : 8;
		/* Command Set Identifier */
		uint32_t csi       : 8;
	} identify;

	struct {
		/* Physically Contiguous */
		uint32_t pc       : 1;
		/* Queue Priority */
		uint32_t qprio    : 2;
		uint32_t reserved : 13;
		/* Completion Queue Identifier */
		uint32_t cqid     : 16;
	} create_io_sq;

	struct {
		/* Physically Contiguous */
		uint32_t pc       : 1;
		/* Interrupts Enabled */
		uint32_t ien      : 1;
		uint32_t reserved : 14;
		/* Interrupt Vector */
		uint32_t iv       : 16;
	} create_io_cq;

	struct {
		/* Directive Operation */
		uint32_t doper    : 8;
		/* Directive Type */
		uint32_t dtype    : 8;
		/* Directive Specific */
		uint32_t dspec    : 16;
	} directive;

	struct {
		/* Number of Dwords */
		uint32_t numdu    : 16;
		/* Log Specific Identifier */
		uint32_t lsid     : 16;
	} get_log_page;

	struct {
		/* Extended Data Structure */
		uint32_t eds      : 1;
		uint32_t reserved : 31;
	} resv_report;

	union spdk_nvme_feat_arbitration feat_arbitration;
	union spdk_nvme_feat_power_management feat_power_management;
	union spdk_nvme_feat_lba_range_type feat_lba_range_type;
	union spdk_nvme_feat_temperature_threshold feat_temp_threshold;
	union spdk_nvme_feat_error_recovery feat_error_recovery;
	union spdk_nvme_feat_volatile_write_cache feat_volatile_write_cache;
	union spdk_nvme_feat_number_of_queues feat_num_of_queues;
	union spdk_nvme_feat_interrupt_coalescing feat_interrupt_coalescing;
	union spdk_nvme_feat_interrupt_vector_configuration feat_interrupt_vector_configuration;
	union spdk_nvme_feat_write_atomicity feat_write_atomicity;
	union spdk_nvme_feat_async_event_configuration feat_async_event_cfg;
	union spdk_nvme_feat_keep_alive_timer feat_keep_alive_timer;
	union spdk_nvme_feat_host_identifier feat_host_identifier;
	union spdk_nvme_feat_reservation_notification_mask feat_rsv_notification_mask;
	union spdk_nvme_feat_reservation_persistence feat_rsv_persistence;

	struct {
		/* Attribute – Integral Dataset for Read */
		uint32_t idr      : 1;
		/* Attribute – Integral Dataset for Write */
		uint32_t idw      : 1;
		/* Attribute – Deallocate */
		uint32_t ad       : 1;
		uint32_t reserved : 29;
	} dsm;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_cmd_cdw11) == 4, "Incorrect size");

struct spdk_nvme_cmd {
	/* dword 0 */
	uint16_t opc	:  8;	/* opcode */
	uint16_t fuse	:  2;	/* fused operation */
	uint16_t rsvd1	:  4;
	uint16_t psdt	:  2;
	uint16_t cid;		/* command identifier */

	/* dword 1 */
	uint32_t nsid;		/* namespace identifier */

	/* dword 2-3 */
	uint32_t rsvd2;
	uint32_t rsvd3;

	/* dword 4-5 */
	uint64_t mptr;		/* metadata pointer */

	/* dword 6-9: data pointer */
	union {
		struct {
			uint64_t prp1;		/* prp entry 1 */
			uint64_t prp2;		/* prp entry 2 */
		} prp;

		struct spdk_nvme_sgl_descriptor sgl1;
	} dptr;

	/* command-specific */
	union {
		uint32_t cdw10;
		union spdk_nvme_cmd_cdw10 cdw10_bits;
	};
	/* command-specific */
	union {
		uint32_t cdw11;
		union spdk_nvme_cmd_cdw11 cdw11_bits;
	};
	/* dword 12-15 */
	uint32_t cdw12;		/* command-specific */
	uint32_t cdw13;		/* command-specific */
	uint32_t cdw14;		/* command-specific */
	uint32_t cdw15;		/* command-specific */
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_cmd) == 64, "Incorrect size");

struct spdk_nvme_status {
	uint16_t p	:  1;	/* phase tag */
	uint16_t sc	:  8;	/* status code */
	uint16_t sct	:  3;	/* status code type */
	uint16_t crd	:  2;   /* command retry delay */
	uint16_t m	:  1;	/* more */
	uint16_t dnr	:  1;	/* do not retry */
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_status) == 2, "Incorrect size");

/**
 * Completion queue entry
 */
struct spdk_nvme_cpl {
	/* dword 0 */
	uint32_t		cdw0;	/* command-specific */

	/* dword 1 */
	uint32_t		cdw1;	/* command-specific */

	/* dword 2 */
	uint16_t		sqhd;	/* submission queue head pointer */
	uint16_t		sqid;	/* submission queue identifier */

	/* dword 3 */
	uint16_t		cid;	/* command identifier */
	union {
		uint16_t                status_raw;
		struct spdk_nvme_status	status;
	};
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_cpl) == 16, "Incorrect size");

/**
 * Dataset Management range
 */
struct spdk_nvme_dsm_range {
	union {
		struct {
			uint32_t af		: 4; /**< access frequencey */
			uint32_t al		: 2; /**< access latency */
			uint32_t reserved0	: 2;

			uint32_t sr		: 1; /**< sequential read range */
			uint32_t sw		: 1; /**< sequential write range */
			uint32_t wp		: 1; /**< write prepare */
			uint32_t reserved1	: 13;

			uint32_t access_size	: 8; /**< command access size */
		} bits;

		uint32_t raw;
	} attributes;

	uint32_t length;
	uint64_t starting_lba;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_dsm_range) == 16, "Incorrect size");

/**
 * Status code types
 */
enum spdk_nvme_status_code_type {
	SPDK_NVME_SCT_GENERIC		= 0x0,
	SPDK_NVME_SCT_COMMAND_SPECIFIC	= 0x1,
	SPDK_NVME_SCT_MEDIA_ERROR	= 0x2,
	SPDK_NVME_SCT_PATH		= 0x3,
	/* 0x4-0x6 - reserved */
	SPDK_NVME_SCT_VENDOR_SPECIFIC	= 0x7,
};

/**
 * Generic command status codes
 */
enum spdk_nvme_generic_command_status_code {
	SPDK_NVME_SC_SUCCESS				= 0x00,
	SPDK_NVME_SC_INVALID_OPCODE			= 0x01,
	SPDK_NVME_SC_INVALID_FIELD			= 0x02,
	SPDK_NVME_SC_COMMAND_ID_CONFLICT		= 0x03,
	SPDK_NVME_SC_DATA_TRANSFER_ERROR		= 0x04,
	SPDK_NVME_SC_ABORTED_POWER_LOSS			= 0x05,
	SPDK_NVME_SC_INTERNAL_DEVICE_ERROR		= 0x06,
	SPDK_NVME_SC_ABORTED_BY_REQUEST			= 0x07,
	SPDK_NVME_SC_ABORTED_SQ_DELETION		= 0x08,
	SPDK_NVME_SC_ABORTED_FAILED_FUSED		= 0x09,
	SPDK_NVME_SC_ABORTED_MISSING_FUSED		= 0x0a,
	SPDK_NVME_SC_INVALID_NAMESPACE_OR_FORMAT	= 0x0b,
	SPDK_NVME_SC_COMMAND_SEQUENCE_ERROR		= 0x0c,
	SPDK_NVME_SC_INVALID_SGL_SEG_DESCRIPTOR		= 0x0d,
	SPDK_NVME_SC_INVALID_NUM_SGL_DESCIRPTORS	= 0x0e,
	SPDK_NVME_SC_DATA_SGL_LENGTH_INVALID		= 0x0f,
	SPDK_NVME_SC_METADATA_SGL_LENGTH_INVALID	= 0x10,
	SPDK_NVME_SC_SGL_DESCRIPTOR_TYPE_INVALID	= 0x11,
	SPDK_NVME_SC_INVALID_CONTROLLER_MEM_BUF		= 0x12,
	SPDK_NVME_SC_INVALID_PRP_OFFSET			= 0x13,
	SPDK_NVME_SC_ATOMIC_WRITE_UNIT_EXCEEDED		= 0x14,
	SPDK_NVME_SC_OPERATION_DENIED			= 0x15,
	SPDK_NVME_SC_INVALID_SGL_OFFSET			= 0x16,
	/* 0x17 - reserved */
	SPDK_NVME_SC_HOSTID_INCONSISTENT_FORMAT		= 0x18,
	SPDK_NVME_SC_KEEP_ALIVE_EXPIRED			= 0x19,
	SPDK_NVME_SC_KEEP_ALIVE_INVALID			= 0x1a,
	SPDK_NVME_SC_ABORTED_PREEMPT			= 0x1b,
	SPDK_NVME_SC_SANITIZE_FAILED			= 0x1c,
	SPDK_NVME_SC_SANITIZE_IN_PROGRESS		= 0x1d,
	SPDK_NVME_SC_SGL_DATA_BLOCK_GRANULARITY_INVALID	= 0x1e,
	SPDK_NVME_SC_COMMAND_INVALID_IN_CMB		= 0x1f,

	SPDK_NVME_SC_LBA_OUT_OF_RANGE			= 0x80,
	SPDK_NVME_SC_CAPACITY_EXCEEDED			= 0x81,
	SPDK_NVME_SC_NAMESPACE_NOT_READY		= 0x82,
	SPDK_NVME_SC_RESERVATION_CONFLICT               = 0x83,
	SPDK_NVME_SC_FORMAT_IN_PROGRESS                 = 0x84,
};

/**
 * Command specific status codes
 */
enum spdk_nvme_command_specific_status_code {
	SPDK_NVME_SC_COMPLETION_QUEUE_INVALID		= 0x00,
	SPDK_NVME_SC_INVALID_QUEUE_IDENTIFIER		= 0x01,
	SPDK_NVME_SC_INVALID_QUEUE_SIZE			= 0x02,
	SPDK_NVME_SC_ABORT_COMMAND_LIMIT_EXCEEDED	= 0x03,
	/* 0x04 - reserved */
	SPDK_NVME_SC_ASYNC_EVENT_REQUEST_LIMIT_EXCEEDED = 0x05,
	SPDK_NVME_SC_INVALID_FIRMWARE_SLOT		= 0x06,
	SPDK_NVME_SC_INVALID_FIRMWARE_IMAGE		= 0x07,
	SPDK_NVME_SC_INVALID_INTERRUPT_VECTOR		= 0x08,
	SPDK_NVME_SC_INVALID_LOG_PAGE			= 0x09,
	SPDK_NVME_SC_INVALID_FORMAT			= 0x0a,
	SPDK_NVME_SC_FIRMWARE_REQ_CONVENTIONAL_RESET    = 0x0b,
	SPDK_NVME_SC_INVALID_QUEUE_DELETION             = 0x0c,
	SPDK_NVME_SC_FEATURE_ID_NOT_SAVEABLE            = 0x0d,
	SPDK_NVME_SC_FEATURE_NOT_CHANGEABLE             = 0x0e,
	SPDK_NVME_SC_FEATURE_NOT_NAMESPACE_SPECIFIC     = 0x0f,
	SPDK_NVME_SC_FIRMWARE_REQ_NVM_RESET             = 0x10,
	SPDK_NVME_SC_FIRMWARE_REQ_RESET                 = 0x11,
	SPDK_NVME_SC_FIRMWARE_REQ_MAX_TIME_VIOLATION    = 0x12,
	SPDK_NVME_SC_FIRMWARE_ACTIVATION_PROHIBITED     = 0x13,
	SPDK_NVME_SC_OVERLAPPING_RANGE                  = 0x14,
	SPDK_NVME_SC_NAMESPACE_INSUFFICIENT_CAPACITY    = 0x15,
	SPDK_NVME_SC_NAMESPACE_ID_UNAVAILABLE           = 0x16,
	/* 0x17 - reserved */
	SPDK_NVME_SC_NAMESPACE_ALREADY_ATTACHED         = 0x18,
	SPDK_NVME_SC_NAMESPACE_IS_PRIVATE               = 0x19,
	SPDK_NVME_SC_NAMESPACE_NOT_ATTACHED             = 0x1a,
	SPDK_NVME_SC_THINPROVISIONING_NOT_SUPPORTED     = 0x1b,
	SPDK_NVME_SC_CONTROLLER_LIST_INVALID            = 0x1c,
	SPDK_NVME_SC_DEVICE_SELF_TEST_IN_PROGRESS	= 0x1d,
	SPDK_NVME_SC_BOOT_PARTITION_WRITE_PROHIBITED	= 0x1e,
	SPDK_NVME_SC_INVALID_CTRLR_ID			= 0x1f,
	SPDK_NVME_SC_INVALID_SECONDARY_CTRLR_STATE	= 0x20,
	SPDK_NVME_SC_INVALID_NUM_CTRLR_RESOURCES	= 0x21,
	SPDK_NVME_SC_INVALID_RESOURCE_ID		= 0x22,

	SPDK_NVME_SC_IOCS_NOT_SUPPORTED			= 0x29,
	SPDK_NVME_SC_IOCS_NOT_ENABLED			= 0x2a,
	SPDK_NVME_SC_IOCS_COMBINATION_REJECTED		= 0x2b,
	SPDK_NVME_SC_INVALID_IOCS			= 0x2c,

	SPDK_NVME_SC_STREAM_RESOURCE_ALLOCATION_FAILED  = 0x7f,
	SPDK_NVME_SC_CONFLICTING_ATTRIBUTES		= 0x80,
	SPDK_NVME_SC_INVALID_PROTECTION_INFO		= 0x81,
	SPDK_NVME_SC_ATTEMPTED_WRITE_TO_RO_RANGE	= 0x82,
};

/**
 * Media error status codes
 */
enum spdk_nvme_media_error_status_code {
	SPDK_NVME_SC_WRITE_FAULTS			= 0x80,
	SPDK_NVME_SC_UNRECOVERED_READ_ERROR		= 0x81,
	SPDK_NVME_SC_GUARD_CHECK_ERROR			= 0x82,
	SPDK_NVME_SC_APPLICATION_TAG_CHECK_ERROR	= 0x83,
	SPDK_NVME_SC_REFERENCE_TAG_CHECK_ERROR		= 0x84,
	SPDK_NVME_SC_COMPARE_FAILURE			= 0x85,
	SPDK_NVME_SC_ACCESS_DENIED			= 0x86,
	SPDK_NVME_SC_DEALLOCATED_OR_UNWRITTEN_BLOCK     = 0x87,
};

/**
 * Path related status codes
 */
enum spdk_nvme_path_status_code {
	SPDK_NVME_SC_INTERNAL_PATH_ERROR		= 0x00,
	SPDK_NVME_SC_ASYMMETRIC_ACCESS_PERSISTENT_LOSS	= 0x01,
	SPDK_NVME_SC_ASYMMETRIC_ACCESS_INACCESSIBLE	= 0x02,
	SPDK_NVME_SC_ASYMMETRIC_ACCESS_TRANSITION	= 0x03,

	SPDK_NVME_SC_CONTROLLER_PATH_ERROR		= 0x60,

	SPDK_NVME_SC_HOST_PATH_ERROR			= 0x70,
	SPDK_NVME_SC_ABORTED_BY_HOST			= 0x71,
};

#define SPDK_NVME_MAX_OPC 0xff

/**
 * Admin opcodes
 */
enum spdk_nvme_admin_opcode {
	SPDK_NVME_OPC_DELETE_IO_SQ			= 0x00,
	SPDK_NVME_OPC_CREATE_IO_SQ			= 0x01,
	SPDK_NVME_OPC_GET_LOG_PAGE			= 0x02,
	/* 0x03 - reserved */
	SPDK_NVME_OPC_DELETE_IO_CQ			= 0x04,
	SPDK_NVME_OPC_CREATE_IO_CQ			= 0x05,
	SPDK_NVME_OPC_IDENTIFY				= 0x06,
	/* 0x07 - reserved */
	SPDK_NVME_OPC_ABORT				= 0x08,
	SPDK_NVME_OPC_SET_FEATURES			= 0x09,
	SPDK_NVME_OPC_GET_FEATURES			= 0x0a,
	/* 0x0b - reserved */
	SPDK_NVME_OPC_ASYNC_EVENT_REQUEST		= 0x0c,
	SPDK_NVME_OPC_NS_MANAGEMENT			= 0x0d,
	/* 0x0e-0x0f - reserved */
	SPDK_NVME_OPC_FIRMWARE_COMMIT			= 0x10,
	SPDK_NVME_OPC_FIRMWARE_IMAGE_DOWNLOAD		= 0x11,

	SPDK_NVME_OPC_DEVICE_SELF_TEST			= 0x14,
	SPDK_NVME_OPC_NS_ATTACHMENT			= 0x15,

	SPDK_NVME_OPC_KEEP_ALIVE			= 0x18,
	SPDK_NVME_OPC_DIRECTIVE_SEND			= 0x19,
	SPDK_NVME_OPC_DIRECTIVE_RECEIVE			= 0x1a,

	SPDK_NVME_OPC_VIRTUALIZATION_MANAGEMENT		= 0x1c,
	SPDK_NVME_OPC_NVME_MI_SEND			= 0x1d,
	SPDK_NVME_OPC_NVME_MI_RECEIVE			= 0x1e,

	SPDK_NVME_OPC_DOORBELL_BUFFER_CONFIG		= 0x7c,

	SPDK_NVME_OPC_FORMAT_NVM			= 0x80,
	SPDK_NVME_OPC_SECURITY_SEND			= 0x81,
	SPDK_NVME_OPC_SECURITY_RECEIVE			= 0x82,

	SPDK_NVME_OPC_SANITIZE				= 0x84,

	SPDK_NVME_OPC_GET_LBA_STATUS			= 0x86,
};

/**
 * NVM command set opcodes
 */
enum spdk_nvme_nvm_opcode {
	SPDK_NVME_OPC_FLUSH				= 0x00,
	SPDK_NVME_OPC_WRITE				= 0x01,
	SPDK_NVME_OPC_READ				= 0x02,
	/* 0x03 - reserved */
	SPDK_NVME_OPC_WRITE_UNCORRECTABLE		= 0x04,
	SPDK_NVME_OPC_COMPARE				= 0x05,
	/* 0x06-0x07 - reserved */
	SPDK_NVME_OPC_WRITE_ZEROES			= 0x08,
	SPDK_NVME_OPC_DATASET_MANAGEMENT		= 0x09,

	SPDK_NVME_OPC_RESERVATION_REGISTER		= 0x0d,
	SPDK_NVME_OPC_RESERVATION_REPORT		= 0x0e,

	SPDK_NVME_OPC_RESERVATION_ACQUIRE		= 0x11,
	SPDK_NVME_OPC_RESERVATION_RELEASE		= 0x15,
};

/**
 * Zoned Namespace command set opcodes
 *
 * In addition to the opcodes of the NVM command set, the Zoned Namespace
 * command set supports the following opcodes.
 */
enum spdk_nvme_zns_opcode {
	SPDK_NVME_OPC_ZONE_MGMT_SEND			= 0x79,
	SPDK_NVME_OPC_ZONE_MGMT_RECV			= 0x7a,
	SPDK_NVME_OPC_ZONE_APPEND			= 0x7d,
};

/**
 * Data transfer (bits 1:0) of an NVMe opcode.
 *
 * \sa spdk_nvme_opc_get_data_transfer
 */
enum spdk_nvme_data_transfer {
	/** Opcode does not transfer data */
	SPDK_NVME_DATA_NONE				= 0,
	/** Opcode transfers data from host to controller (e.g. Write) */
	SPDK_NVME_DATA_HOST_TO_CONTROLLER		= 1,
	/** Opcode transfers data from controller to host (e.g. Read) */
	SPDK_NVME_DATA_CONTROLLER_TO_HOST		= 2,
	/** Opcode transfers data both directions */
	SPDK_NVME_DATA_BIDIRECTIONAL			= 3
};

/**
 * Extract the Data Transfer bits from an NVMe opcode.
 *
 * This determines whether a command requires a data buffer and
 * which direction (host to controller or controller to host) it is
 * transferred.
 */
static inline enum spdk_nvme_data_transfer spdk_nvme_opc_get_data_transfer(uint8_t opc)
{
	return (enum spdk_nvme_data_transfer)(opc & 3);
}

static inline uint32_t
spdk_nvme_bytes_to_numd(uint32_t len)
{
	return (len >> 2) - 1;
}

struct __attribute__((packed)) spdk_nvme_host_behavior {
	uint8_t acre;
	uint8_t reserved[511];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_host_behavior) == 512, "Incorrect size");

enum spdk_nvme_feat {
	/* 0x00 - reserved */

	/** cdw11 layout defined by \ref spdk_nvme_feat_arbitration */
	SPDK_NVME_FEAT_ARBITRATION				= 0x01,
	/** cdw11 layout defined by \ref spdk_nvme_feat_power_management */
	SPDK_NVME_FEAT_POWER_MANAGEMENT				= 0x02,
	/** cdw11 layout defined by \ref spdk_nvme_feat_lba_range_type */
	SPDK_NVME_FEAT_LBA_RANGE_TYPE				= 0x03,
	/** cdw11 layout defined by \ref spdk_nvme_feat_temperature_threshold */
	SPDK_NVME_FEAT_TEMPERATURE_THRESHOLD			= 0x04,
	/** cdw11 layout defined by \ref spdk_nvme_feat_error_recovery */
	SPDK_NVME_FEAT_ERROR_RECOVERY				= 0x05,
	/** cdw11 layout defined by \ref spdk_nvme_feat_volatile_write_cache */
	SPDK_NVME_FEAT_VOLATILE_WRITE_CACHE			= 0x06,
	/** cdw11 layout defined by \ref spdk_nvme_feat_number_of_queues */
	SPDK_NVME_FEAT_NUMBER_OF_QUEUES				= 0x07,
	/** cdw11 layout defined by \ref spdk_nvme_feat_interrupt_coalescing */
	SPDK_NVME_FEAT_INTERRUPT_COALESCING			= 0x08,
	/** cdw11 layout defined by \ref spdk_nvme_feat_interrupt_vector_configuration */
	SPDK_NVME_FEAT_INTERRUPT_VECTOR_CONFIGURATION		= 0x09,
	/** cdw11 layout defined by \ref spdk_nvme_feat_write_atomicity */
	SPDK_NVME_FEAT_WRITE_ATOMICITY				= 0x0A,
	/** cdw11 layout defined by \ref spdk_nvme_feat_async_event_configuration */
	SPDK_NVME_FEAT_ASYNC_EVENT_CONFIGURATION		= 0x0B,
	/** cdw11 layout defined by \ref spdk_nvme_feat_autonomous_power_state_transition */
	SPDK_NVME_FEAT_AUTONOMOUS_POWER_STATE_TRANSITION	= 0x0C,
	/** cdw11 layout defined by \ref spdk_nvme_feat_host_mem_buffer */
	SPDK_NVME_FEAT_HOST_MEM_BUFFER				= 0x0D,
	SPDK_NVME_FEAT_TIMESTAMP				= 0x0E,
	/** cdw11 layout defined by \ref spdk_nvme_feat_keep_alive_timer */
	SPDK_NVME_FEAT_KEEP_ALIVE_TIMER				= 0x0F,
	/** cdw11 layout defined by \ref spdk_nvme_feat_host_controlled_thermal_management */
	SPDK_NVME_FEAT_HOST_CONTROLLED_THERMAL_MANAGEMENT	= 0x10,
	/** cdw11 layout defined by \ref spdk_nvme_feat_non_operational_power_state_config */
	SPDK_NVME_FEAT_NON_OPERATIONAL_POWER_STATE_CONFIG	= 0x11,

	SPDK_NVME_FEAT_READ_RECOVERY_LEVEL_CONFIG		= 0x12,
	SPDK_NVME_FEAT_PREDICTABLE_LATENCY_MODE_CONFIG		= 0x13,
	SPDK_NVME_FEAT_PREDICTABLE_LATENCY_MODE_WINDOW		= 0x14,
	SPDK_NVME_FEAT_LBA_STATUS_INFORMATION_ATTRIBUTES	= 0x15,
	/** data buffer layout  defined by \ref spdk_nvme_host_behavior */
	SPDK_NVME_FEAT_HOST_BEHAVIOR_SUPPORT			= 0x16,
	SPDK_NVME_FEAT_SANITIZE_CONFIG				= 0x17,
	SPDK_NVME_FEAT_ENDURANCE_GROUP_EVENT			= 0x18,
	/* 0x12-0x77 - reserved */

	/* 0x78-0x7F - NVMe-MI features */

	/** cdw11 layout defined by \ref spdk_nvme_feat_software_progress_marker */
	SPDK_NVME_FEAT_SOFTWARE_PROGRESS_MARKER			= 0x80,

	/** cdw11 layout defined by \ref spdk_nvme_feat_host_identifier */
	SPDK_NVME_FEAT_HOST_IDENTIFIER				= 0x81,
	/** cdw11 layout defined by \ref spdk_nvme_feat_reservation_notification_mask */
	SPDK_NVME_FEAT_HOST_RESERVE_MASK			= 0x82,
	/** cdw11 layout defined by \ref spdk_nvme_feat_reservation_persistence */
	SPDK_NVME_FEAT_HOST_RESERVE_PERSIST			= 0x83,

	/* 0x84-0xBF - command set specific (reserved) */

	/* 0xC0-0xFF - vendor specific */
};

/** Bit set of attributes for DATASET MANAGEMENT commands. */
enum spdk_nvme_dsm_attribute {
	SPDK_NVME_DSM_ATTR_INTEGRAL_READ		= 0x1,
	SPDK_NVME_DSM_ATTR_INTEGRAL_WRITE		= 0x2,
	SPDK_NVME_DSM_ATTR_DEALLOCATE			= 0x4,
};

struct spdk_nvme_power_state {
	uint16_t mp;				/* bits 15:00: maximum power */

	uint8_t reserved1;

	uint8_t mps		: 1;		/* bit 24: max power scale */
	uint8_t nops		: 1;		/* bit 25: non-operational state */
	uint8_t reserved2	: 6;

	uint32_t enlat;				/* bits 63:32: entry latency in microseconds */
	uint32_t exlat;				/* bits 95:64: exit latency in microseconds */

	uint8_t rrt		: 5;		/* bits 100:96: relative read throughput */
	uint8_t reserved3	: 3;

	uint8_t rrl		: 5;		/* bits 108:104: relative read latency */
	uint8_t reserved4	: 3;

	uint8_t rwt		: 5;		/* bits 116:112: relative write throughput */
	uint8_t reserved5	: 3;

	uint8_t rwl		: 5;		/* bits 124:120: relative write latency */
	uint8_t reserved6	: 3;

	uint8_t reserved7[16];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_power_state) == 32, "Incorrect size");

/** Identify command CNS value */
enum spdk_nvme_identify_cns {
	/** Identify namespace indicated in CDW1.NSID */
	SPDK_NVME_IDENTIFY_NS				= 0x00,

	/** Identify controller */
	SPDK_NVME_IDENTIFY_CTRLR			= 0x01,

	/** List active NSIDs greater than CDW1.NSID */
	SPDK_NVME_IDENTIFY_ACTIVE_NS_LIST		= 0x02,

	/** List namespace identification descriptors */
	SPDK_NVME_IDENTIFY_NS_ID_DESCRIPTOR_LIST	= 0x03,

	/** Identify namespace indicated in CDW1.NSID, specific to CWD11.CSI */
	SPDK_NVME_IDENTIFY_NS_IOCS			= 0x05,

	/** Identify controller, specific to CWD11.CSI */
	SPDK_NVME_IDENTIFY_CTRLR_IOCS			= 0x06,

	/** List active NSIDs greater than CDW1.NSID, specific to CWD11.CSI */
	SPDK_NVME_IDENTIFY_ACTIVE_NS_LIST_IOCS		= 0x07,

	/** List allocated NSIDs greater than CDW1.NSID */
	SPDK_NVME_IDENTIFY_ALLOCATED_NS_LIST		= 0x10,

	/** Identify namespace if CDW1.NSID is allocated */
	SPDK_NVME_IDENTIFY_NS_ALLOCATED			= 0x11,

	/** Get list of controllers starting at CDW10.CNTID that are attached to CDW1.NSID */
	SPDK_NVME_IDENTIFY_NS_ATTACHED_CTRLR_LIST	= 0x12,

	/** Get list of controllers starting at CDW10.CNTID */
	SPDK_NVME_IDENTIFY_CTRLR_LIST			= 0x13,

	/** Get primary controller capabilities structure */
	SPDK_NVME_IDENTIFY_PRIMARY_CTRLR_CAP		= 0x14,

	/** Get secondary controller list */
	SPDK_NVME_IDENTIFY_SECONDARY_CTRLR_LIST		= 0x15,

	/** List allocated NSIDs greater than CDW1.NSID, specific to CWD11.CSI */
	SPDK_NVME_IDENTIFY_ALLOCATED_NS_LIST_IOCS	= 0x1a,

	/** Identify namespace if CDW1.NSID is allocated, specific to CDWD11.CSI */
	SPDK_NVME_IDENTIFY_NS_ALLOCATED_IOCS		= 0x1b,

	/** Identify I/O Command Sets */
	SPDK_NVME_IDENTIFY_IOCS				= 0x1c,
};

/** NVMe over Fabrics controller model */
enum spdk_nvmf_ctrlr_model {
	/** NVM subsystem uses dynamic controller model */
	SPDK_NVMF_CTRLR_MODEL_DYNAMIC			= 0,

	/** NVM subsystem uses static controller model */
	SPDK_NVMF_CTRLR_MODEL_STATIC			= 1,
};

#define SPDK_NVME_CTRLR_SN_LEN	20
#define SPDK_NVME_CTRLR_MN_LEN	40
#define SPDK_NVME_CTRLR_FR_LEN	8

/** Identify Controller data sgls.supported values */
enum spdk_nvme_sgls_supported {
	/** SGLs are not supported */
	SPDK_NVME_SGLS_NOT_SUPPORTED			= 0,

	/** SGLs are supported with no alignment or granularity requirement. */
	SPDK_NVME_SGLS_SUPPORTED			= 1,

	/** SGLs are supported with a DWORD alignment and granularity requirement. */
	SPDK_NVME_SGLS_SUPPORTED_DWORD_ALIGNED		= 2,
};

/** Identify Controller data vwc.flush_broadcast values */
enum spdk_nvme_flush_broadcast {
	/** Support for NSID=FFFFFFFFh with Flush is not indicated. */
	SPDK_NVME_FLUSH_BROADCAST_NOT_INDICATED		= 0,

	/* 01b: Reserved */

	/** Flush does not support NSID set to FFFFFFFFh. */
	SPDK_NVME_FLUSH_BROADCAST_NOT_SUPPORTED		= 2,

	/** Flush supports NSID set to FFFFFFFFh. */
	SPDK_NVME_FLUSH_BROADCAST_SUPPORTED		= 3
};

#define SPDK_NVME_NQN_FIELD_SIZE 256

/** Identify Controller data NVMe over Fabrics-specific fields */
struct spdk_nvme_cdata_nvmf_specific {
	/** I/O queue command capsule supported size (16-byte units) */
	uint32_t	ioccsz;

	/** I/O queue response capsule supported size (16-byte units) */
	uint32_t	iorcsz;

	/** In-capsule data offset (16-byte units) */
	uint16_t	icdoff;

	/** Controller attributes */
	struct {
		/** Controller model: \ref spdk_nvmf_ctrlr_model */
		uint8_t	ctrlr_model : 1;
		uint8_t reserved : 7;
	} ctrattr;

	/** Maximum SGL block descriptors (0 = no limit) */
	uint8_t		msdbd;

	uint8_t		reserved[244];
};

/** Identify Controller data SGL support */
struct spdk_nvme_cdata_sgls {
	uint32_t	supported : 2;
	uint32_t	keyed_sgl : 1;
	uint32_t	reserved1 : 13;
	uint32_t	bit_bucket_descriptor : 1;
	uint32_t	metadata_pointer : 1;
	uint32_t	oversized_sgl : 1;
	uint32_t	metadata_address : 1;
	uint32_t	sgl_offset : 1;
	uint32_t	transport_sgl : 1;
	uint32_t	reserved2 : 10;
};

struct __attribute__((packed)) __attribute__((aligned)) spdk_nvme_ctrlr_data {
	/* bytes 0-255: controller capabilities and features */

	/** pci vendor id */
	uint16_t		vid;

	/** pci subsystem vendor id */
	uint16_t		ssvid;

	/** serial number */
	int8_t			sn[SPDK_NVME_CTRLR_SN_LEN];

	/** model number */
	int8_t			mn[SPDK_NVME_CTRLR_MN_LEN];

	/** firmware revision */
	uint8_t			fr[SPDK_NVME_CTRLR_FR_LEN];

	/** recommended arbitration burst */
	uint8_t			rab;

	/** ieee oui identifier */
	uint8_t			ieee[3];

	/** controller multi-path I/O and namespace sharing capabilities */
	struct {
		uint8_t multi_port	: 1;
		uint8_t multi_host	: 1;
		uint8_t sr_iov		: 1;
		uint8_t ana_reporting	: 1;
		uint8_t reserved	: 4;
	} cmic;

	/** maximum data transfer size */
	uint8_t			mdts;

	/** controller id */
	uint16_t		cntlid;

	/** version */
	union spdk_nvme_vs_register	ver;

	/** RTD3 resume latency */
	uint32_t		rtd3r;

	/** RTD3 entry latency */
	uint32_t		rtd3e;

	/** optional asynchronous events supported */
	struct {
		uint32_t	reserved1 : 8;

		/** Supports sending Namespace Attribute Notices. */
		uint32_t	ns_attribute_notices : 1;

		/** Supports sending Firmware Activation Notices. */
		uint32_t	fw_activation_notices : 1;

		uint32_t	reserved2 : 1;

		/** Supports Asymmetric Namespace Access Change Notices. */
		uint32_t	ana_change_notices : 1;

		uint32_t	reserved3 : 19;

		/** Supports Discovery log change notices (refer to the NVMe over Fabrics specification) */
		uint32_t	discovery_log_change_notices : 1;

	} oaes;

	/** controller attributes */
	struct {
		/** Supports 128-bit host identifier */
		uint32_t	host_id_exhid_supported: 1;

		/** Supports non-operational power state permissive mode */
		uint32_t	non_operational_power_state_permissive_mode: 1;

		uint32_t	reserved: 30;
	} ctratt;

	uint8_t			reserved_100[12];

	/** FRU globally unique identifier */
	uint8_t			fguid[16];

	/** Command Retry Delay Time 1, 2 and 3 */
	uint16_t		crdt[3];

	uint8_t			reserved_122[122];

	/* bytes 256-511: admin command set attributes */

	/** optional admin command support */
	struct {
		/* supports security send/receive commands */
		uint16_t	security  : 1;

		/* supports format nvm command */
		uint16_t	format    : 1;

		/* supports firmware activate/download commands */
		uint16_t	firmware  : 1;

		/* supports ns manage/ns attach commands */
		uint16_t	ns_manage  : 1;

		/** Supports device self-test command (SPDK_NVME_OPC_DEVICE_SELF_TEST) */
		uint16_t	device_self_test : 1;

		/** Supports SPDK_NVME_OPC_DIRECTIVE_SEND and SPDK_NVME_OPC_DIRECTIVE_RECEIVE */
		uint16_t	directives : 1;

		/** Supports NVMe-MI (SPDK_NVME_OPC_NVME_MI_SEND, SPDK_NVME_OPC_NVME_MI_RECEIVE) */
		uint16_t	nvme_mi : 1;

		/** Supports SPDK_NVME_OPC_VIRTUALIZATION_MANAGEMENT */
		uint16_t	virtualization_management : 1;

		/** Supports SPDK_NVME_OPC_DOORBELL_BUFFER_CONFIG */
		uint16_t	doorbell_buffer_config : 1;

		/** Supports SPDK_NVME_OPC_GET_LBA_STATUS */
		uint16_t	get_lba_status : 1;

		uint16_t	oacs_rsvd : 6;
	} oacs;

	/** abort command limit */
	uint8_t			acl;

	/** asynchronous event request limit */
	uint8_t			aerl;

	/** firmware updates */
	struct {
		/* first slot is read-only */
		uint8_t		slot1_ro  : 1;

		/* number of firmware slots */
		uint8_t		num_slots : 3;

		/* support activation without reset */
		uint8_t		activation_without_reset : 1;

		uint8_t		frmw_rsvd : 3;
	} frmw;

	/** log page attributes */
	struct {
		/* per namespace smart/health log page */
		uint8_t		ns_smart : 1;
		/* command effects log page */
		uint8_t		celp : 1;
		/* extended data for get log page */
		uint8_t		edlp: 1;
		/** telemetry log pages and notices */
		uint8_t		telemetry : 1;
		uint8_t		lpa_rsvd : 4;
	} lpa;

	/** error log page entries */
	uint8_t			elpe;

	/** number of power states supported */
	uint8_t			npss;

	/** admin vendor specific command configuration */
	struct {
		/* admin vendor specific commands use disk format */
		uint8_t		spec_format : 1;

		uint8_t		avscc_rsvd  : 7;
	} avscc;

	/** autonomous power state transition attributes */
	struct {
		/** controller supports autonomous power state transitions */
		uint8_t		supported  : 1;

		uint8_t		apsta_rsvd : 7;
	} apsta;

	/** warning composite temperature threshold */
	uint16_t		wctemp;

	/** critical composite temperature threshold */
	uint16_t		cctemp;

	/** maximum time for firmware activation */
	uint16_t		mtfa;

	/** host memory buffer preferred size */
	uint32_t		hmpre;

	/** host memory buffer minimum size */
	uint32_t		hmmin;

	/** total NVM capacity */
	uint64_t		tnvmcap[2];

	/** unallocated NVM capacity */
	uint64_t		unvmcap[2];

	/** replay protected memory block support */
	struct {
		uint8_t		num_rpmb_units	: 3;
		uint8_t		auth_method	: 3;
		uint8_t		reserved1	: 2;

		uint8_t		reserved2;

		uint8_t		total_size;
		uint8_t		access_size;
	} rpmbs;

	/** extended device self-test time (in minutes) */
	uint16_t		edstt;

	/** device self-test options */
	union {
		uint8_t	raw;
		struct {
			/** Device supports only one device self-test operation at a time */
			uint8_t	one_only : 1;

			uint8_t	reserved : 7;
		} bits;
	} dsto;

	/**
	 * Firmware update granularity
	 *
	 * 4KB units
	 * 0x00 = no information provided
	 * 0xFF = no restriction
	 */
	uint8_t			fwug;

	/**
	 * Keep Alive Support
	 *
	 * Granularity of keep alive timer in 100 ms units
	 * 0 = keep alive not supported
	 */
	uint16_t		kas;

	/** Host controlled thermal management attributes */
	union {
		uint16_t		raw;
		struct {
			uint16_t	supported : 1;
			uint16_t	reserved : 15;
		} bits;
	} hctma;

	/** Minimum thermal management temperature */
	uint16_t		mntmt;

	/** Maximum thermal management temperature */
	uint16_t		mxtmt;

	/** Sanitize capabilities */
	union {
		uint32_t	raw;
		struct {
			uint32_t	crypto_erase : 1;
			uint32_t	block_erase : 1;
			uint32_t	overwrite : 1;
			uint32_t	reserved : 29;
		} bits;
	} sanicap;

	/* bytes 332-342 */
	uint8_t			reserved3[10];

	/** ANA transition time */
	uint8_t			anatt;

	/* bytes 343: Asymmetric namespace access capabilities */
	struct {
		uint8_t		ana_optimized_state : 1;
		uint8_t		ana_non_optimized_state : 1;
		uint8_t		ana_inaccessible_state : 1;
		uint8_t		ana_persistent_loss_state : 1;
		uint8_t		ana_change_state : 1;
		uint8_t		reserved : 1;
		uint8_t		no_change_anagrpid : 1;
		uint8_t		non_zero_anagrpid : 1;
	} anacap;

	/* bytes 344-347: ANA group identifier maximum */
	uint32_t		anagrpmax;
	/* bytes 348-351: number of ANA group identifiers */
	uint32_t		nanagrpid;

	/* bytes 352-511 */
	uint8_t			reserved352[160];

	/* bytes 512-703: nvm command set attributes */

	/** submission queue entry size */
	struct {
		uint8_t		min : 4;
		uint8_t		max : 4;
	} sqes;

	/** completion queue entry size */
	struct {
		uint8_t		min : 4;
		uint8_t		max : 4;
	} cqes;

	uint16_t		maxcmd;

	/** number of namespaces */
	uint32_t		nn;

	/** optional nvm command support */
	struct {
		uint16_t	compare : 1;
		uint16_t	write_unc : 1;
		uint16_t	dsm: 1;
		uint16_t	write_zeroes: 1;
		uint16_t	set_features_save: 1;
		uint16_t	reservations: 1;
		uint16_t	timestamp: 1;
		uint16_t	reserved: 9;
	} oncs;

	/** fused operation support */
	struct {
		uint16_t	compare_and_write : 1;
		uint16_t	reserved : 15;
	} fuses;

	/** format nvm attributes */
	struct {
		uint8_t		format_all_ns: 1;
		uint8_t		erase_all_ns: 1;
		uint8_t		crypto_erase_supported: 1;
		uint8_t		reserved: 5;
	} fna;

	/** volatile write cache */
	struct {
		uint8_t		present : 1;
		uint8_t		flush_broadcast : 2;
		uint8_t		reserved : 5;
	} vwc;

	/** atomic write unit normal */
	uint16_t		awun;

	/** atomic write unit power fail */
	uint16_t		awupf;

	/** NVM vendor specific command configuration */
	uint8_t			nvscc;

	uint8_t			reserved531;

	/** atomic compare & write unit */
	uint16_t		acwu;

	uint16_t		reserved534;

	struct spdk_nvme_cdata_sgls sgls;

	/* maximum number of allowed namespaces */
	uint32_t		mnan;

	uint8_t			reserved4[224];

	uint8_t			subnqn[SPDK_NVME_NQN_FIELD_SIZE];

	uint8_t			reserved5[768];

	struct spdk_nvme_cdata_nvmf_specific nvmf_specific;

	/* bytes 2048-3071: power state descriptors */
	struct spdk_nvme_power_state	psd[32];

	/* bytes 3072-4095: vendor specific */
	uint8_t			vs[1024];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_ctrlr_data) == 4096, "Incorrect size");

struct spdk_nvme_zns_ctrlr_data {
	/** zone append size limit */
	uint8_t			zasl;

	uint8_t			reserved1[4095];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_zns_ctrlr_data) == 4096, "Incorrect size");

struct __attribute__((packed)) spdk_nvme_primary_ctrl_capabilities {
	/**  controller id */
	uint16_t		cntlid;
	/**  port identifier */
	uint16_t		portid;
	/**  controller resource types */
	struct {
		uint8_t vq_supported	: 1;
		uint8_t vi_supported	: 1;
		uint8_t reserved	: 6;
	} crt;
	uint8_t			reserved[27];
	/** total number of VQ flexible resources */
	uint32_t		vqfrt;
	/** total number of VQ flexible resources assigned to secondary controllers */
	uint32_t		vqrfa;
	/** total number of VQ flexible resources allocated to primary controller */
	uint16_t		vqrfap;
	/** total number of VQ Private resources for the primary controller */
	uint16_t		vqprt;
	/** max number of VQ flexible Resources that may be assigned to a secondary controller */
	uint16_t		vqfrsm;
	/** preferred granularity of assigning and removing VQ Flexible Resources */
	uint16_t		vqgran;
	uint8_t			reserved1[16];
	/** total number of VI flexible resources for the primary and its secondary controllers */
	uint32_t		vifrt;
	/** total number of VI flexible resources assigned to the secondary controllers */
	uint32_t		virfa;
	/** total number of VI flexible resources currently allocated to the primary controller */
	uint16_t		virfap;
	/** total number of VI private resources for the primary controller */
	uint16_t		viprt;
	/** max number of VI flexible resources that may be assigned to a secondary controller */
	uint16_t		vifrsm;
	/** preferred granularity of assigning and removing VI flexible resources */
	uint16_t		vigran;
	uint8_t			reserved2[4016];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_primary_ctrl_capabilities) == 4096, "Incorrect size");

struct __attribute__((packed)) spdk_nvme_secondary_ctrl_entry {
	/** controller identifier of the secondary controller */
	uint16_t		scid;
	/** controller identifier of the associated primary controller */
	uint16_t		pcid;
	/** indicates the state of the secondary controller */
	struct {
		uint8_t is_online	: 1;
		uint8_t reserved	: 7;
	} scs;
	uint8_t	reserved[3];
	/** VF number if the secondary controller is an SR-IOV VF */
	uint16_t		vfn;
	/** number of VQ flexible resources assigned to the indicated secondary controller */
	uint16_t		nvq;
	/** number of VI flexible resources assigned to the indicated secondary controller */
	uint16_t		nvi;
	uint8_t			reserved1[18];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_secondary_ctrl_entry) == 32, "Incorrect size");

struct __attribute__((packed)) spdk_nvme_secondary_ctrl_list {
	/** number of Secondary controller entries in the list */
	uint8_t					number;
	uint8_t					reserved[31];
	struct spdk_nvme_secondary_ctrl_entry	entries[127];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_secondary_ctrl_list) == 4096, "Incorrect size");

struct spdk_nvme_ns_data {
	/** namespace size */
	uint64_t		nsze;

	/** namespace capacity */
	uint64_t		ncap;

	/** namespace utilization */
	uint64_t		nuse;

	/** namespace features */
	struct {
		/** thin provisioning */
		uint8_t		thin_prov : 1;

		/** NAWUN, NAWUPF, and NACWU are defined for this namespace */
		uint8_t		ns_atomic_write_unit : 1;

		/** Supports Deallocated or Unwritten LBA error for this namespace */
		uint8_t		dealloc_or_unwritten_error : 1;

		/** Non-zero NGUID and EUI64 for namespace are never reused */
		uint8_t		guid_never_reused : 1;

		/** Optimal Performance field */
		uint8_t		optperf : 1;

		uint8_t		reserved1 : 3;
	} nsfeat;

	/** number of lba formats */
	uint8_t			nlbaf;

	/** formatted lba size */
	struct {
		uint8_t		format    : 4;
		uint8_t		extended  : 1;
		uint8_t		reserved2 : 3;
	} flbas;

	/** metadata capabilities */
	struct {
		/** metadata can be transferred as part of data prp list */
		uint8_t		extended  : 1;

		/** metadata can be transferred with separate metadata pointer */
		uint8_t		pointer   : 1;

		/** reserved */
		uint8_t		reserved3 : 6;
	} mc;

	/** end-to-end data protection capabilities */
	struct {
		/** protection information type 1 */
		uint8_t		pit1     : 1;

		/** protection information type 2 */
		uint8_t		pit2     : 1;

		/** protection information type 3 */
		uint8_t		pit3     : 1;

		/** first eight bytes of metadata */
		uint8_t		md_start : 1;

		/** last eight bytes of metadata */
		uint8_t		md_end   : 1;
	} dpc;

	/** end-to-end data protection type settings */
	struct {
		/** protection information type */
		uint8_t		pit       : 3;

		/** 1 == protection info transferred at start of metadata */
		/** 0 == protection info transferred at end of metadata */
		uint8_t		md_start  : 1;

		uint8_t		reserved4 : 4;
	} dps;

	/** namespace multi-path I/O and namespace sharing capabilities */
	struct {
		uint8_t		can_share : 1;
		uint8_t		reserved : 7;
	} nmic;

	/** reservation capabilities */
	union {
		struct {
			/** supports persist through power loss */
			uint8_t		persist : 1;

			/** supports write exclusive */
			uint8_t		write_exclusive : 1;

			/** supports exclusive access */
			uint8_t		exclusive_access : 1;

			/** supports write exclusive - registrants only */
			uint8_t		write_exclusive_reg_only : 1;

			/** supports exclusive access - registrants only */
			uint8_t		exclusive_access_reg_only : 1;

			/** supports write exclusive - all registrants */
			uint8_t		write_exclusive_all_reg : 1;

			/** supports exclusive access - all registrants */
			uint8_t		exclusive_access_all_reg : 1;

			/** supports ignore existing key */
			uint8_t		ignore_existing_key : 1;
		} rescap;
		uint8_t		raw;
	} nsrescap;
	/** format progress indicator */
	struct {
		uint8_t		percentage_remaining : 7;
		uint8_t		fpi_supported : 1;
	} fpi;

	/** deallocate logical features */
	union {
		uint8_t		raw;
		struct {
			/**
			 * Value read from deallocated blocks
			 *
			 * 000b = not reported
			 * 001b = all bytes 0x00
			 * 010b = all bytes 0xFF
			 *
			 * \ref spdk_nvme_dealloc_logical_block_read_value
			 */
			uint8_t	read_value : 3;

			/** Supports Deallocate bit in Write Zeroes */
			uint8_t	write_zero_deallocate : 1;

			/**
			 * Guard field behavior for deallocated logical blocks
			 * 0: contains 0xFFFF
			 * 1: contains CRC for read value
			 */
			uint8_t	guard_value : 1;

			uint8_t	reserved : 3;
		} bits;
	} dlfeat;

	/** namespace atomic write unit normal */
	uint16_t		nawun;

	/** namespace atomic write unit power fail */
	uint16_t		nawupf;

	/** namespace atomic compare & write unit */
	uint16_t		nacwu;

	/** namespace atomic boundary size normal */
	uint16_t		nabsn;

	/** namespace atomic boundary offset */
	uint16_t		nabo;

	/** namespace atomic boundary size power fail */
	uint16_t		nabspf;

	/** namespace optimal I/O boundary in logical blocks */
	uint16_t		noiob;

	/** NVM capacity */
	uint64_t		nvmcap[2];

	/** Namespace Preferred Write Granularity */
	uint16_t		npwg;

	/** Namespace Preferred Write Alignment */
	uint16_t                npwa;

	/** Namespace Preferred Deallocate Granularity */
	uint16_t                npdg;

	/** Namespace Preferred Deallocate Alignment */
	uint16_t                npda;

	/** Namespace Optimal Write Size */
	uint16_t                nows;

	uint8_t			reserved64[18];

	/** ANA group identifier */
	uint32_t		anagrpid;

	uint8_t			reserved96[8];

	/** namespace globally unique identifier */
	uint8_t			nguid[16];

	/** IEEE extended unique identifier */
	uint64_t		eui64;

	/** lba format support */
	struct {
		/** metadata size */
		uint32_t	ms	  : 16;

		/** lba data size */
		uint32_t	lbads	  : 8;

		/** relative performance */
		uint32_t	rp	  : 2;

		uint32_t	reserved6 : 6;
	} lbaf[16];

	uint8_t			reserved6[192];

	uint8_t			vendor_specific[3712];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_ns_data) == 4096, "Incorrect size");

struct spdk_nvme_zns_ns_data {
	/** zone operation characteristics */
	struct {
		uint16_t	variable_zone_capacity : 1;
		uint16_t	zone_active_excursions : 1;
		uint16_t	reserved0 : 14;
	} zoc;

	/** optional zoned command support */
	struct {
		uint16_t	read_across_zone_boundaries : 1;
		uint16_t	reserved0 : 15;
	} ozcs;

	/** maximum active resources */
	uint32_t		mar;

	/** maximum open resources */
	uint32_t		mor;

	/** reset recommended limit */
	uint32_t		rrl;

	/** finish recommended limit */
	uint32_t		frl;

	uint8_t			reserved20[2796];

	/** zns lba format extension support */
	struct {
		/** zone size */
		uint64_t	zsze;

		/** zone descriptor extension size */
		uint64_t	zdes : 8;

		uint64_t	reserved15 : 56;
	} lbafe[16];

	uint8_t			reserved3072[768];

	uint8_t			vendor_specific[256];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_zns_ns_data) == 4096, "Incorrect size");

/**
 * Deallocated logical block features - read value
 */
enum spdk_nvme_dealloc_logical_block_read_value {
	/** Not reported */
	SPDK_NVME_DEALLOC_NOT_REPORTED	= 0,

	/** Deallocated blocks read 0x00 */
	SPDK_NVME_DEALLOC_READ_00	= 1,

	/** Deallocated blocks read 0xFF */
	SPDK_NVME_DEALLOC_READ_FF	= 2,
};

/**
 * Reservation Type Encoding
 */
enum spdk_nvme_reservation_type {
	/* 0x00 - reserved */

	/* Write Exclusive Reservation */
	SPDK_NVME_RESERVE_WRITE_EXCLUSIVE		= 0x1,

	/* Exclusive Access Reservation */
	SPDK_NVME_RESERVE_EXCLUSIVE_ACCESS		= 0x2,

	/* Write Exclusive - Registrants Only Reservation */
	SPDK_NVME_RESERVE_WRITE_EXCLUSIVE_REG_ONLY	= 0x3,

	/* Exclusive Access - Registrants Only Reservation */
	SPDK_NVME_RESERVE_EXCLUSIVE_ACCESS_REG_ONLY	= 0x4,

	/* Write Exclusive - All Registrants Reservation */
	SPDK_NVME_RESERVE_WRITE_EXCLUSIVE_ALL_REGS	= 0x5,

	/* Exclusive Access - All Registrants Reservation */
	SPDK_NVME_RESERVE_EXCLUSIVE_ACCESS_ALL_REGS	= 0x6,

	/* 0x7-0xFF - Reserved */
};

struct spdk_nvme_reservation_acquire_data {
	/** current reservation key */
	uint64_t		crkey;
	/** preempt reservation key */
	uint64_t		prkey;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_reservation_acquire_data) == 16, "Incorrect size");

/**
 * Reservation Acquire action
 */
enum spdk_nvme_reservation_acquire_action {
	SPDK_NVME_RESERVE_ACQUIRE		= 0x0,
	SPDK_NVME_RESERVE_PREEMPT		= 0x1,
	SPDK_NVME_RESERVE_PREEMPT_ABORT		= 0x2,
};

struct __attribute__((packed)) spdk_nvme_reservation_status_data {
	/** reservation action generation counter */
	uint32_t		gen;
	/** reservation type */
	uint8_t			rtype;
	/** number of registered controllers */
	uint16_t		regctl;
	uint16_t		reserved1;
	/** persist through power loss state */
	uint8_t			ptpls;
	uint8_t			reserved[14];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_reservation_status_data) == 24, "Incorrect size");

struct __attribute__((packed)) spdk_nvme_reservation_status_extended_data {
	struct spdk_nvme_reservation_status_data	data;
	uint8_t						reserved[40];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_reservation_status_extended_data) == 64,
		   "Incorrect size");

struct __attribute__((packed)) spdk_nvme_registered_ctrlr_data {
	/** controller id */
	uint16_t		cntlid;
	/** reservation status */
	struct {
		uint8_t		status    : 1;
		uint8_t		reserved1 : 7;
	} rcsts;
	uint8_t			reserved2[5];
	/** 64-bit host identifier */
	uint64_t		hostid;
	/** reservation key */
	uint64_t		rkey;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_registered_ctrlr_data) == 24, "Incorrect size");

struct __attribute__((packed)) spdk_nvme_registered_ctrlr_extended_data {
	/** controller id */
	uint16_t		cntlid;
	/** reservation status */
	struct {
		uint8_t		status    : 1;
		uint8_t		reserved1 : 7;
	} rcsts;
	uint8_t			reserved2[5];
	/** reservation key */
	uint64_t		rkey;
	/** 128-bit host identifier */
	uint8_t			hostid[16];
	uint8_t			reserved3[32];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_registered_ctrlr_extended_data) == 64, "Incorrect size");

/**
 * Change persist through power loss state for
 *  Reservation Register command
 */
enum spdk_nvme_reservation_register_cptpl {
	SPDK_NVME_RESERVE_PTPL_NO_CHANGES		= 0x0,
	SPDK_NVME_RESERVE_PTPL_CLEAR_POWER_ON		= 0x2,
	SPDK_NVME_RESERVE_PTPL_PERSIST_POWER_LOSS	= 0x3,
};

/**
 * Registration action for Reservation Register command
 */
enum spdk_nvme_reservation_register_action {
	SPDK_NVME_RESERVE_REGISTER_KEY		= 0x0,
	SPDK_NVME_RESERVE_UNREGISTER_KEY	= 0x1,
	SPDK_NVME_RESERVE_REPLACE_KEY		= 0x2,
};

struct spdk_nvme_reservation_register_data {
	/** current reservation key */
	uint64_t		crkey;
	/** new reservation key */
	uint64_t		nrkey;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_reservation_register_data) == 16, "Incorrect size");

struct spdk_nvme_reservation_key_data {
	/** current reservation key */
	uint64_t		crkey;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_reservation_key_data) == 8, "Incorrect size");

/**
 * Reservation Release action
 */
enum spdk_nvme_reservation_release_action {
	SPDK_NVME_RESERVE_RELEASE		= 0x0,
	SPDK_NVME_RESERVE_CLEAR			= 0x1,
};

/**
 * Reservation notification log page type
 */
enum spdk_nvme_reservation_notification_log_page_type {
	SPDK_NVME_RESERVATION_LOG_PAGE_EMPTY	= 0x0,
	SPDK_NVME_REGISTRATION_PREEMPTED	= 0x1,
	SPDK_NVME_RESERVATION_RELEASED		= 0x2,
	SPDK_NVME_RESERVATION_PREEMPTED		= 0x3,
};

/**
 * Reservation notification log
 */
struct spdk_nvme_reservation_notification_log {
	/** 64-bit incrementing reservation notification log page count */
	uint64_t	log_page_count;
	/** Reservation notification log page type */
	uint8_t		type;
	/** Number of additional available reservation notification log pages */
	uint8_t		num_avail_log_pages;
	uint8_t		reserved[2];
	uint32_t	nsid;
	uint8_t		reserved1[48];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_reservation_notification_log) == 64, "Incorrect size");

/* Mask Registration Preempted Notificaton */
#define SPDK_NVME_REGISTRATION_PREEMPTED_MASK	(1U << 1)
/* Mask Reservation Released Notification */
#define SPDK_NVME_RESERVATION_RELEASED_MASK	(1U << 2)
/* Mask Reservation Preempted Notification */
#define SPDK_NVME_RESERVATION_PREEMPTED_MASK	(1U << 3)

/**
 * Log page identifiers for SPDK_NVME_OPC_GET_LOG_PAGE
 */
enum spdk_nvme_log_page {
	/* 0x00 - reserved */

	/** Error information (mandatory) - \ref spdk_nvme_error_information_entry */
	SPDK_NVME_LOG_ERROR			= 0x01,

	/** SMART / health information (mandatory) - \ref spdk_nvme_health_information_page */
	SPDK_NVME_LOG_HEALTH_INFORMATION	= 0x02,

	/** Firmware slot information (mandatory) - \ref spdk_nvme_firmware_page */
	SPDK_NVME_LOG_FIRMWARE_SLOT		= 0x03,

	/** Changed namespace list (optional) */
	SPDK_NVME_LOG_CHANGED_NS_LIST	= 0x04,

	/** Command effects log (optional) */
	SPDK_NVME_LOG_COMMAND_EFFECTS_LOG	= 0x05,

	/** Device self test (optional) */
	SPDK_NVME_LOG_DEVICE_SELF_TEST	= 0x06,

	/** Host initiated telemetry log (optional) */
	SPDK_NVME_LOG_TELEMETRY_HOST_INITIATED	= 0x07,

	/** Controller initiated telemetry log (optional) */
	SPDK_NVME_LOG_TELEMETRY_CTRLR_INITIATED	= 0x08,

	/* 0x09-0x0B - reserved */

	/** Asymmetric namespace access log (optional) */
	SPDK_NVME_LOG_ASYMMETRIC_NAMESPACE_ACCESS = 0x0C,

	/* 0x0D-0x6F - reserved */

	/** Discovery(refer to the NVMe over Fabrics specification) */
	SPDK_NVME_LOG_DISCOVERY		= 0x70,

	/* 0x71-0x7f - reserved for NVMe over Fabrics */

	/** Reservation notification (optional) */
	SPDK_NVME_LOG_RESERVATION_NOTIFICATION	= 0x80,

	/** Sanitize status (optional) */
	SPDK_NVME_LOG_SANITIZE_STATUS = 0x81,

	/* 0x81-0xBF - I/O command set specific */

	/* 0xC0-0xFF - vendor specific */
};

/**
 * Error information log page (\ref SPDK_NVME_LOG_ERROR)
 */
struct spdk_nvme_error_information_entry {
	uint64_t		error_count;
	uint16_t		sqid;
	uint16_t		cid;
	struct spdk_nvme_status	status;
	uint16_t		error_location;
	uint64_t		lba;
	uint32_t		nsid;
	uint8_t			vendor_specific;
	uint8_t			trtype;
	uint8_t			reserved30[2];
	uint64_t		command_specific;
	uint16_t		trtype_specific;
	uint8_t			reserved42[22];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_error_information_entry) == 64, "Incorrect size");

/**
 * SMART / health information page (\ref SPDK_NVME_LOG_HEALTH_INFORMATION)
 */
struct __attribute__((packed)) __attribute__((aligned)) spdk_nvme_health_information_page {
	union spdk_nvme_critical_warning_state	critical_warning;

	uint16_t		temperature;
	uint8_t			available_spare;
	uint8_t			available_spare_threshold;
	uint8_t			percentage_used;

	uint8_t			reserved[26];

	/*
	 * Note that the following are 128-bit values, but are
	 *  defined as an array of 2 64-bit values.
	 */
	/* Data Units Read is always in 512-byte units. */
	uint64_t		data_units_read[2];
	/* Data Units Written is always in 512-byte units. */
	uint64_t		data_units_written[2];
	/* For NVM command set, this includes Compare commands. */
	uint64_t		host_read_commands[2];
	uint64_t		host_write_commands[2];
	/* Controller Busy Time is reported in minutes. */
	uint64_t		controller_busy_time[2];
	uint64_t		power_cycles[2];
	uint64_t		power_on_hours[2];
	uint64_t		unsafe_shutdowns[2];
	uint64_t		media_errors[2];
	uint64_t		num_error_info_log_entries[2];
	/* Controller temperature related. */
	uint32_t		warning_temp_time;
	uint32_t		critical_temp_time;
	uint16_t		temp_sensor[8];

	uint8_t			reserved2[296];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_health_information_page) == 512, "Incorrect size");

/* Commands Supported and Effects Data Structure */
struct spdk_nvme_cmds_and_effect_entry {
	/** Command Supported */
	uint16_t csupp : 1;

	/** Logic Block Content Change  */
	uint16_t lbcc  : 1;

	/** Namespace Capability Change */
	uint16_t ncc   : 1;

	/** Namespace Inventory Change */
	uint16_t nic   : 1;

	/** Controller Capability Change */
	uint16_t ccc   : 1;

	uint16_t reserved1 : 11;

	/* Command Submission and Execution recommendation
	 * 000 - No command submission or execution restriction
	 * 001 - Submitted when there is no outstanding command to same NS
	 * 010 - Submitted when there is no outstanding command to any NS
	 * others - Reserved
	 * \ref command_submission_and_execution in section 5.14.1.5 NVMe Revision 1.3
	 */
	uint16_t cse : 3;

	uint16_t reserved2 : 13;
};

/* Commands Supported and Effects Log Page */
struct spdk_nvme_cmds_and_effect_log_page {
	/** Commands Supported and Effects Data Structure for the Admin Commands */
	struct spdk_nvme_cmds_and_effect_entry admin_cmds_supported[256];

	/** Commands Supported and Effects Data Structure for the IO Commands */
	struct spdk_nvme_cmds_and_effect_entry io_cmds_supported[256];

	uint8_t reserved0[2048];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_cmds_and_effect_log_page) == 4096, "Incorrect size");

/*
 * Get Log Page – Telemetry Host/Controller Initiated Log (Log Identifiers 07h/08h)
 */
struct spdk_nvme_telemetry_log_page_hdr {
	/* Log page identifier */
	uint8_t    lpi;
	uint8_t    rsvd[4];
	uint8_t    ieee_oui[3];
	/* Data area 1 last block */
	uint16_t   dalb1;
	/* Data area 2 last block */
	uint16_t   dalb2;
	/* Data area 3 last block */
	uint16_t   dalb3;
	uint8_t    rsvd1[368];
	/* Controller initiated data avail */
	uint8_t    ctrlr_avail;
	/* Controller initiated telemetry data generation */
	uint8_t    ctrlr_gen;
	/* Reason identifier */
	uint8_t    rsnident[128];
	uint8_t    telemetry_datablock[0];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_telemetry_log_page_hdr) == 512, "Incorrect size");

/**
 * Sanitize Status Type
 */
enum spdk_nvme_sanitize_status_type {
	SPDK_NVME_NEVER_BEEN_SANITIZED		= 0x0,
	SPDK_NVME_RECENT_SANITIZE_SUCCESSFUL	= 0x1,
	SPDK_NVME_SANITIZE_IN_PROGRESS		= 0x2,
	SPDK_NVME_SANITIZE_FAILED		= 0x3,
};

/**
 * Sanitize status sstat field
 */
struct spdk_nvme_sanitize_status_sstat {
	uint16_t status			: 3;
	uint16_t complete_pass		: 5;
	uint16_t global_data_erase	: 1;
	uint16_t reserved		: 7;
};

/**
 * Sanitize log page
 */
struct spdk_nvme_sanitize_status_log_page {
	/* Sanitize progress */
	uint16_t				sprog;
	/* Sanitize status */
	struct spdk_nvme_sanitize_status_sstat	sstat;
	/* CDW10 of sanitize command */
	uint32_t				scdw10;
	/* Estimated overwrite time in seconds */
	uint32_t				et_overwrite;
	/* Estimated block erase time in seconds */
	uint32_t				et_block_erase;
	/* Estimated crypto erase time in seconds */
	uint32_t				et_crypto_erase;
	uint8_t					reserved[492];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_sanitize_status_log_page) == 512, "Incorrect size");

/**
 * Asynchronous Event Type
 */
enum spdk_nvme_async_event_type {
	/* Error Status */
	SPDK_NVME_ASYNC_EVENT_TYPE_ERROR	= 0x0,
	/* SMART/Health Status */
	SPDK_NVME_ASYNC_EVENT_TYPE_SMART	= 0x1,
	/* Notice */
	SPDK_NVME_ASYNC_EVENT_TYPE_NOTICE	= 0x2,
	/* 0x3 - 0x5 Reserved */

	/* I/O Command Set Specific Status */
	SPDK_NVME_ASYNC_EVENT_TYPE_IO		= 0x6,
	/* Vendor Specific */
	SPDK_NVME_ASYNC_EVENT_TYPE_VENDOR	= 0x7,
};

/**
 * Asynchronous Event Information for Error Status
 */
enum spdk_nvme_async_event_info_error {
	/* Write to Invalid Doorbell Register */
	SPDK_NVME_ASYNC_EVENT_WRITE_INVALID_DB		= 0x0,
	/* Invalid Doorbell Register Write Value */
	SPDK_NVME_ASYNC_EVENT_INVALID_DB_WRITE		= 0x1,
	/* Diagnostic Failure */
	SPDK_NVME_ASYNC_EVENT_DIAGNOSTIC_FAILURE	= 0x2,
	/* Persistent Internal Error */
	SPDK_NVME_ASYNC_EVENT_PERSISTENT_INTERNAL	= 0x3,
	/* Transient Internal Error */
	SPDK_NVME_ASYNC_EVENT_TRANSIENT_INTERNAL	= 0x4,
	/* Firmware Image Load Error */
	SPDK_NVME_ASYNC_EVENT_FW_IMAGE_LOAD		= 0x5,

	/* 0x6 - 0xFF Reserved */
};

/**
 * Asynchronous Event Information for SMART/Health Status
 */
enum spdk_nvme_async_event_info_smart {
	/* NVM Subsystem Reliability */
	SPDK_NVME_ASYNC_EVENT_SUBSYSTEM_RELIABILITY	= 0x0,
	/* Temperature Threshold */
	SPDK_NVME_ASYNC_EVENT_TEMPERATURE_THRESHOLD	= 0x1,
	/* Spare Below Threshold */
	SPDK_NVME_ASYNC_EVENT_SPARE_BELOW_THRESHOLD	= 0x2,

	/* 0x3 - 0xFF Reserved */
};

/**
 * Asynchronous Event Information for Notice
 */
enum spdk_nvme_async_event_info_notice {
	/* Namespace Attribute Changed */
	SPDK_NVME_ASYNC_EVENT_NS_ATTR_CHANGED		= 0x0,
	/* Firmware Activation Starting */
	SPDK_NVME_ASYNC_EVENT_FW_ACTIVATION_START	= 0x1,
	/* Telemetry Log Changed */
	SPDK_NVME_ASYNC_EVENT_TELEMETRY_LOG_CHANGED	= 0x2,
	/* Asymmetric Namespace Access Change */
	SPDK_NVME_ASYNC_EVENT_ANA_CHANGE		= 0x3,

	/* 0x4 - 0xEF Reserved */

	/** Discovery log change event(refer to the NVMe over Fabrics specification) */
	SPDK_NVME_ASYNC_EVENT_DISCOVERY_LOG_CHANGE	= 0xF0,

	/* 0xF1 - 0xFF Reserved */
};

/**
 * Asynchronous Event Information for NVM Command Set Specific Status
 */
enum spdk_nvme_async_event_info_nvm_command_set {
	/* Reservation Log Page Avaiable */
	SPDK_NVME_ASYNC_EVENT_RESERVATION_LOG_AVAIL	= 0x0,
	/* Sanitize Operation Completed */
	SPDK_NVME_ASYNC_EVENT_SANITIZE_COMPLETED	= 0x1,

	/* 0x2 - 0xFF Reserved */
};

/**
 * Asynchronous Event Request Completion
 */
union spdk_nvme_async_event_completion {
	uint32_t raw;
	struct {
		uint32_t async_event_type	: 3;
		uint32_t reserved1		: 5;
		uint32_t async_event_info	: 8;
		uint32_t log_page_identifier	: 8;
		uint32_t reserved2		: 8;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_async_event_completion) == 4, "Incorrect size");

/**
 * Firmware slot information page (\ref SPDK_NVME_LOG_FIRMWARE_SLOT)
 */
struct spdk_nvme_firmware_page {
	struct {
		uint8_t	active_slot	: 3; /**< Slot for current FW */
		uint8_t	reserved3	: 1;
		uint8_t	next_reset_slot	: 3; /**< Slot that will be active at next controller reset */
		uint8_t	reserved7	: 1;
	} afi;

	uint8_t			reserved[7];
	uint8_t			revision[7][8]; /** Revisions for 7 slots (ASCII strings) */
	uint8_t			reserved2[448];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_firmware_page) == 512, "Incorrect size");

/**
 * Asymmetric Namespace Acccess page (\ref SPDK_NVME_LOG_ASYMMETRIC_NAMESPACE_ACCESS)
 */
struct spdk_nvme_ana_page {
	uint64_t change_count;
	uint16_t num_ana_group_desc;
	uint8_t reserved[6];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_ana_page) == 16, "Incorrect size");

/* Asymmetric namespace access state */
enum spdk_nvme_ana_state {
	SPDK_NVME_ANA_OPTIMIZED_STATE		= 0x1,
	SPDK_NVME_ANA_NON_OPTIMIZED_STATE	= 0x2,
	SPDK_NVME_ANA_INACCESSIBLE_STATE	= 0x3,
	SPDK_NVME_ANA_PERSISTENT_LOSS_STATE	= 0x4,
	SPDK_NVME_ANA_CHANGE_STATE		= 0xF,
};

/* ANA group descriptor */
struct spdk_nvme_ana_group_descriptor {
	uint32_t ana_group_id;
	uint32_t num_of_nsid;
	uint64_t change_count;

	uint8_t ana_state : 4;
	uint8_t reserved0 : 4;

	uint8_t reserved1[15];

	uint32_t nsid[];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_ana_group_descriptor) == 32, "Incorrect size");

/**
 * Namespace attachment Type Encoding
 */
enum spdk_nvme_ns_attach_type {
	/* Controller attach */
	SPDK_NVME_NS_CTRLR_ATTACH	= 0x0,

	/* Controller detach */
	SPDK_NVME_NS_CTRLR_DETACH	= 0x1,

	/* 0x2-0xF - Reserved */
};

/**
 * Namespace management Type Encoding
 */
enum spdk_nvme_ns_management_type {
	/* Create */
	SPDK_NVME_NS_MANAGEMENT_CREATE	= 0x0,

	/* Delete */
	SPDK_NVME_NS_MANAGEMENT_DELETE	= 0x1,

	/* 0x2-0xF - Reserved */
};

struct spdk_nvme_ns_list {
	uint32_t ns_list[1024];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_ns_list) == 4096, "Incorrect size");

/**
 * Namespace identification descriptor type
 *
 * \sa spdk_nvme_ns_id_desc
 */
enum spdk_nvme_nidt {
	/** IEEE Extended Unique Identifier */
	SPDK_NVME_NIDT_EUI64		= 0x01,

	/** Namespace GUID */
	SPDK_NVME_NIDT_NGUID		= 0x02,

	/** Namespace UUID */
	SPDK_NVME_NIDT_UUID		= 0x03,

	/** Namespace Command Set Identifier */
	SPDK_NVME_NIDT_CSI		= 0x04,
};

struct spdk_nvme_ns_id_desc {
	/** Namespace identifier type */
	uint8_t nidt;

	/** Namespace identifier length (length of nid field) */
	uint8_t nidl;

	uint8_t reserved2;
	uint8_t reserved3;

	/** Namespace identifier */
	uint8_t nid[];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_ns_id_desc) == 4, "Incorrect size");

struct spdk_nvme_ctrlr_list {
	uint16_t ctrlr_count;
	uint16_t ctrlr_list[2047];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_ctrlr_list) == 4096, "Incorrect size");

enum spdk_nvme_csi {
	SPDK_NVME_CSI_NVM	= 0x0,
	SPDK_NVME_CSI_KV	= 0x1,
	SPDK_NVME_CSI_ZNS	= 0x2,
};

enum spdk_nvme_secure_erase_setting {
	SPDK_NVME_FMT_NVM_SES_NO_SECURE_ERASE	= 0x0,
	SPDK_NVME_FMT_NVM_SES_USER_DATA_ERASE	= 0x1,
	SPDK_NVME_FMT_NVM_SES_CRYPTO_ERASE	= 0x2,
};

enum spdk_nvme_pi_location {
	SPDK_NVME_FMT_NVM_PROTECTION_AT_TAIL	= 0x0,
	SPDK_NVME_FMT_NVM_PROTECTION_AT_HEAD	= 0x1,
};

enum spdk_nvme_pi_type {
	SPDK_NVME_FMT_NVM_PROTECTION_DISABLE		= 0x0,
	SPDK_NVME_FMT_NVM_PROTECTION_TYPE1		= 0x1,
	SPDK_NVME_FMT_NVM_PROTECTION_TYPE2		= 0x2,
	SPDK_NVME_FMT_NVM_PROTECTION_TYPE3		= 0x3,
};

enum spdk_nvme_metadata_setting {
	SPDK_NVME_FMT_NVM_METADATA_TRANSFER_AS_BUFFER	= 0x0,
	SPDK_NVME_FMT_NVM_METADATA_TRANSFER_AS_LBA	= 0x1,
};

struct spdk_nvme_format {
	uint32_t	lbaf		: 4;
	uint32_t	ms		: 1;
	uint32_t	pi		: 3;
	uint32_t	pil		: 1;
	uint32_t	ses		: 3;
	uint32_t	reserved	: 20;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_format) == 4, "Incorrect size");

struct spdk_nvme_protection_info {
	uint16_t	guard;
	uint16_t	app_tag;
	uint32_t	ref_tag;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_protection_info) == 8, "Incorrect size");

/* Data structures for sanitize command */
/* Sanitize - Command Dword 10 */
struct spdk_nvme_sanitize {
	/* Sanitize Action (SANACT) */
	uint32_t sanact	: 3;
	/* Allow Unrestricted Sanitize Exit (AUSE) */
	uint32_t ause	: 1;
	/* Overwrite Pass Count (OWPASS) */
	uint32_t owpass	: 4;
	/* Overwrite Invert Pattern Between Passes */
	uint32_t oipbp	: 1;
	/* No Deallocate after sanitize (NDAS) */
	uint32_t ndas	: 1;
	/* reserved */
	uint32_t reserved	: 22;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_sanitize) == 4, "Incorrect size");

/* Sanitize Action */
enum spdk_sanitize_action {
	/* Exit Failure Mode */
	SPDK_NVME_SANITIZE_EXIT_FAILURE_MODE	= 0x1,
	/* Start a Block Erase sanitize operation */
	SPDK_NVME_SANITIZE_BLOCK_ERASE		= 0x2,
	/* Start an Overwrite sanitize operation */
	SPDK_NVME_SANITIZE_OVERWRITE		= 0x3,
	/* Start a Crypto Erase sanitize operation */
	SPDK_NVME_SANITIZE_CRYPTO_ERASE		= 0x4,
};

/** Parameters for SPDK_NVME_OPC_FIRMWARE_COMMIT cdw10: commit action */
enum spdk_nvme_fw_commit_action {
	/**
	 * Downloaded image replaces the image specified by
	 * the Firmware Slot field. This image is not activated.
	 */
	SPDK_NVME_FW_COMMIT_REPLACE_IMG			= 0x0,
	/**
	 * Downloaded image replaces the image specified by
	 * the Firmware Slot field. This image is activated at the next reset.
	 */
	SPDK_NVME_FW_COMMIT_REPLACE_AND_ENABLE_IMG	= 0x1,
	/**
	 * The image specified by the Firmware Slot field is
	 * activated at the next reset.
	 */
	SPDK_NVME_FW_COMMIT_ENABLE_IMG			= 0x2,
	/**
	 * The image specified by the Firmware Slot field is
	 * requested to be activated immediately without reset.
	 */
	SPDK_NVME_FW_COMMIT_RUN_IMG			= 0x3,
};

/** Parameters for SPDK_NVME_OPC_FIRMWARE_COMMIT cdw10 */
struct spdk_nvme_fw_commit {
	/**
	 * Firmware Slot. Specifies the firmware slot that shall be used for the
	 * Commit Action. The controller shall choose the firmware slot (slot 1 - 7)
	 * to use for the operation if the value specified is 0h.
	 */
	uint32_t	fs		: 3;
	/**
	 * Commit Action. Specifies the action that is taken on the image downloaded
	 * with the Firmware Image Download command or on a previously downloaded and
	 * placed image.
	 */
	uint32_t	ca		: 3;
	uint32_t	reserved	: 26;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_fw_commit) == 4, "Incorrect size");

/* ZNS Zone Send Action (ZSA) cdw13 */
enum spdk_nvme_zns_zone_send_action {
	SPDK_NVME_ZONE_CLOSE	= 0x1,
	SPDK_NVME_ZONE_FINISH	= 0x2,
	SPDK_NVME_ZONE_OPEN	= 0x3,
	SPDK_NVME_ZONE_RESET	= 0x4,
	SPDK_NVME_ZONE_OFFLINE	= 0x5,
};

/* ZNS Zone Receive Action (ZRA) cdw13 */
enum spdk_nvme_zns_zone_receive_action {
	SPDK_NVME_ZONE_REPORT	= 0x0,
};

enum spdk_nvme_zns_zra_report_opts {
	SPDK_NVME_ZRA_LIST_ALL	= 0x0,
	SPDK_NVME_ZRA_LIST_ZSE	= 0x1,
	SPDK_NVME_ZRA_LIST_ZSIO	= 0x2,
	SPDK_NVME_ZRA_LIST_ZSEO	= 0x3,
	SPDK_NVME_ZRA_LIST_ZSC	= 0x4,
	SPDK_NVME_ZRA_LIST_ZSF	= 0x5,
	SPDK_NVME_ZRA_LIST_ZSRO	= 0x6,
	SPDK_NVME_ZRA_LIST_ZSO	= 0x7,
};

enum spdk_nvme_zns_zone_type {
	SPDK_NVME_ZONE_TYPE_SEQWR = 0x2,
};

enum spdk_nvme_zns_zone_state {
	SPDK_NVME_ZONE_STATE_EMPTY	= 0x1,
	SPDK_NVME_ZONE_STATE_IOPEN	= 0x2,
	SPDK_NVME_ZONE_STATE_EOPEN	= 0x3,
	SPDK_NVME_ZONE_STATE_CLOSED	= 0x4,
	SPDK_NVME_ZONE_STATE_RONLY	= 0xD,
	SPDK_NVME_ZONE_STATE_FULL	= 0xE,
	SPDK_NVME_ZONE_STATE_OFFLINE	= 0xF,
};

struct spdk_nvme_zns_zone_desc {
	/** Zone Type */
	uint8_t zt		: 4;

	uint8_t rsvd0		: 4;

	uint8_t rsvd1		: 4;

	/** Zone State */
	uint8_t zs		: 4;

	/**
	 * Zone Attributes
	 */
	union {
		uint8_t raw;

		struct {
			/** Zone Finished by controller */
			uint8_t zfc: 1;

			/** Zone Finish Recommended */
			uint8_t zfr: 1;

			/** Reset Zone Recommended */
			uint8_t rzr: 1;

			uint8_t rsvd3 : 4;

			/** Zone Descriptor Valid */
			uint8_t zdev: 1;
		} bits;
	} za;

	uint8_t reserved[5];

	/** Zone Capacity (in number of LBAs) */
	uint64_t zcap;

	/** Zone Start LBA */
	uint64_t zslba;

	/** Write Pointer (LBA) */
	uint64_t wp;

	uint8_t reserved32[32];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_zns_zone_desc) == 64, "Incorrect size");

struct spdk_nvme_zns_zone_report {
	uint64_t nr_zones;
	uint8_t reserved8[56];
	struct spdk_nvme_zns_zone_desc descs[];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_zns_zone_report) == 64, "Incorrect size");

/* Directives field */
enum spdk_nvme_directive_type {
	SPDK_NVME_DIRECTIVE_TYPE_IDENTIFY = 0x0,
	SPDK_NVME_DIRECTIVE_TYPE_STREAMS = 0x1,
};

enum spdk_nvme_identify_directive_send_operation {
	SPDK_NVME_IDENTIFY_DIRECTIVE_SEND_ENABLED = 0x1,
};

enum spdk_nvme_identify_directive_receive_operation {
	SPDK_NVME_IDENTIFY_DIRECTIVE_RECEIVE_RETURN_PARAM = 0x1,
};

struct spdk_nvme_ns_identify_directive_param {
	struct {
		/* set to 1b to indicate that the Identify Directive is supported */
		uint8_t identify	: 1;
		/* set to 1b if the Streams Directive is supported */
		uint8_t streams		: 1;
		uint8_t reserved1	: 6;
		uint8_t reserved2[31];
	} directives_supported;
	struct {
		/* set to 1b to indicate that the Identify Directive is enabled */
		uint8_t identify	: 1;
		/* set to 1b if the Streams Directive is enabled */
		uint8_t streams		: 1;
		uint8_t reserved1	: 6;
		uint8_t reserved2[31];
	} directives_enabled;

	uint32_t reserved[1008];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_ns_identify_directive_param) == 4096, "Incorrect size");

enum spdk_nvme_streams_directive_receive_operation {
	SPDK_NVME_STREAMS_DIRECTIVE_RECEIVE_RETURN_PARAM = 0x1,
	SPDK_NVME_STREAMS_DIRECTIVE_RECEIVE_GET_STATUS = 0x2,
	SPDK_NVME_STREAMS_DIRECTIVE_RECEIVE_ALLOCATE_RESOURCE = 0x3,
};

enum spdk_nvme_streams_directive_send_operation {
	SPDK_NVME_STREAMS_DIRECTIVE_SEND_RELEASE_ID = 0x1,
	SPDK_NVME_STREAMS_DIRECTIVE_SEND_RELEASE_RESOURCE = 0x2,
};

struct spdk_nvme_ns_streams_data {
	/* MAX Streams Limit */
	uint16_t msl;
	/* NVM Subsystem Streams Available */
	uint16_t nssa;
	/* NVM Subsystem Streams Open */
	uint16_t nsso;
	/* NVM Subsystem Stream Capability */
	struct {
		/* Stream ID may be shared by multiple host IDs if set to 1. */
		uint8_t ssid		: 1;
		uint8_t reserved	: 7;
	} nssc;
	uint8_t reserved1[9];
	/* Namespace Specific Fields
	 * Stream Write Size */
	uint32_t sws;
	/* Stream Granularity Size */
	uint16_t sgs;
	/* Namespace and Host Identifier Specific Fields
	 * Namespace Streams Allocated */
	uint16_t nsa;
	/* Namespace Streams Open */
	uint16_t nso;
	uint8_t reserved2[6];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_ns_streams_data) == 32, "Incorrect size");

struct spdk_nvme_ns_streams_status {
	/* Open Stream Count, this field specifies the number of streams that are currently open */
	uint16_t open_streams_count;

	/* Stream Identifier, this field specifies the open stream identifier */
	uint16_t stream_id[65535];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_ns_streams_status) == 131072, "Incorrect size");

#define spdk_nvme_cpl_is_error(cpl)			\
	((cpl)->status.sc != SPDK_NVME_SC_SUCCESS ||	\
	 (cpl)->status.sct != SPDK_NVME_SCT_GENERIC)

#define spdk_nvme_cpl_is_success(cpl)	(!spdk_nvme_cpl_is_error(cpl))

#define spdk_nvme_cpl_is_pi_error(cpl)						\
	((cpl)->status.sct == SPDK_NVME_SCT_MEDIA_ERROR &&			\
	 ((cpl)->status.sc == SPDK_NVME_SC_GUARD_CHECK_ERROR ||			\
	  (cpl)->status.sc == SPDK_NVME_SC_APPLICATION_TAG_CHECK_ERROR ||	\
	  (cpl)->status.sc == SPDK_NVME_SC_REFERENCE_TAG_CHECK_ERROR))

#define spdk_nvme_cpl_is_abort_success(cpl)	\
	(spdk_nvme_cpl_is_success(cpl) && !((cpl)->cdw0 & 1U))

/** Set fused operation */
#define SPDK_NVME_IO_FLAGS_FUSE_FIRST (SPDK_NVME_CMD_FUSE_FIRST << 0)
#define SPDK_NVME_IO_FLAGS_FUSE_SECOND (SPDK_NVME_CMD_FUSE_SECOND << 0)
#define SPDK_NVME_IO_FLAGS_FUSE_MASK (SPDK_NVME_CMD_FUSE_MASK << 0)
/** Enable Directive type as streams */
#define SPDK_NVME_IO_FLAGS_STREAMS_DIRECTIVE (1U << 20)
/** Zone append specific, determines the contents of the reference tag written to the media */
#define SPDK_NVME_IO_FLAGS_ZONE_APPEND_PIREMAP (1U << 25)
/** Enable protection information checking of the Logical Block Reference Tag field */
#define SPDK_NVME_IO_FLAGS_PRCHK_REFTAG (1U << 26)
/** Enable protection information checking of the Application Tag field */
#define SPDK_NVME_IO_FLAGS_PRCHK_APPTAG (1U << 27)
/** Enable protection information checking of the Guard field */
#define SPDK_NVME_IO_FLAGS_PRCHK_GUARD (1U << 28)
/** The protection information is stripped or inserted when set this bit */
#define SPDK_NVME_IO_FLAGS_PRACT (1U << 29)
#define SPDK_NVME_IO_FLAGS_FORCE_UNIT_ACCESS (1U << 30)
#define SPDK_NVME_IO_FLAGS_LIMITED_RETRY (1U << 31)

/** Mask of valid io flags mask */
#define SPDK_NVME_IO_FLAGS_VALID_MASK 0xFFFF0003
#define SPDK_NVME_IO_FLAGS_CDW12_MASK 0xFFFF0000

#ifdef __cplusplus
}
#endif

#endif
