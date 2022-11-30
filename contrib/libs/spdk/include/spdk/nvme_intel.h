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
 * Intel NVMe vendor-specific definitions
 *
 * Reference:
 * http://www.intel.com/content/dam/www/public/us/en/documents/product-specifications/ssd-dc-p3700-spec.pdf
 */

#ifndef SPDK_NVME_INTEL_H
#define SPDK_NVME_INTEL_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "spdk/assert.h"

enum spdk_nvme_intel_feat {
	SPDK_NVME_INTEL_FEAT_MAX_LBA				= 0xC1,
	SPDK_NVME_INTEL_FEAT_NATIVE_MAX_LBA			= 0xC2,
	SPDK_NVME_INTEL_FEAT_POWER_GOVERNOR_SETTING		= 0xC6,
	SPDK_NVME_INTEL_FEAT_SMBUS_ADDRESS			= 0xC8,
	SPDK_NVME_INTEL_FEAT_LED_PATTERN			= 0xC9,
	SPDK_NVME_INTEL_FEAT_RESET_TIMED_WORKLOAD_COUNTERS	= 0xD5,
	SPDK_NVME_INTEL_FEAT_LATENCY_TRACKING			= 0xE2,
};

enum spdk_nvme_intel_set_max_lba_command_status_code {
	SPDK_NVME_INTEL_EXCEEDS_AVAILABLE_CAPACITY		= 0xC0,
	SPDK_NVME_INTEL_SMALLER_THAN_MIN_LIMIT			= 0xC1,
	SPDK_NVME_INTEL_SMALLER_THAN_NS_REQUIREMENTS		= 0xC2,
};

enum spdk_nvme_intel_log_page {
	SPDK_NVME_INTEL_LOG_PAGE_DIRECTORY			= 0xC0,
	SPDK_NVME_INTEL_LOG_READ_CMD_LATENCY			= 0xC1,
	SPDK_NVME_INTEL_LOG_WRITE_CMD_LATENCY			= 0xC2,
	SPDK_NVME_INTEL_LOG_TEMPERATURE				= 0xC5,
	SPDK_NVME_INTEL_LOG_SMART				= 0xCA,
	SPDK_NVME_INTEL_MARKETING_DESCRIPTION			= 0xDD,
};

enum spdk_nvme_intel_smart_attribute_code {
	SPDK_NVME_INTEL_SMART_PROGRAM_FAIL_COUNT		= 0xAB,
	SPDK_NVME_INTEL_SMART_ERASE_FAIL_COUNT			= 0xAC,
	SPDK_NVME_INTEL_SMART_WEAR_LEVELING_COUNT		= 0xAD,
	SPDK_NVME_INTEL_SMART_E2E_ERROR_COUNT			= 0xB8,
	SPDK_NVME_INTEL_SMART_CRC_ERROR_COUNT			= 0xC7,
	SPDK_NVME_INTEL_SMART_MEDIA_WEAR			= 0xE2,
	SPDK_NVME_INTEL_SMART_HOST_READ_PERCENTAGE		= 0xE3,
	SPDK_NVME_INTEL_SMART_TIMER				= 0xE4,
	SPDK_NVME_INTEL_SMART_THERMAL_THROTTLE_STATUS		= 0xEA,
	SPDK_NVME_INTEL_SMART_RETRY_BUFFER_OVERFLOW_COUNTER	= 0xF0,
	SPDK_NVME_INTEL_SMART_PLL_LOCK_LOSS_COUNT		= 0xF3,
	SPDK_NVME_INTEL_SMART_NAND_BYTES_WRITTEN		= 0xF4,
	SPDK_NVME_INTEL_SMART_HOST_BYTES_WRITTEN		= 0xF5,
};

struct spdk_nvme_intel_log_page_directory {
	uint8_t		version[2];
	uint8_t		reserved[384];
	uint8_t		read_latency_log_len;
	uint8_t		reserved2;
	uint8_t		write_latency_log_len;
	uint8_t		reserved3[5];
	uint8_t		temperature_statistics_log_len;
	uint8_t		reserved4[9];
	uint8_t		smart_log_len;
	uint8_t		reserved5[37];
	uint8_t		marketing_description_log_len;
	uint8_t		reserved6[69];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_intel_log_page_directory) == 512, "Incorrect size");

struct spdk_nvme_intel_rw_latency_page {
	uint16_t		major_revison;
	uint16_t		minor_revison;
	uint32_t		buckets_32us[32];
	uint32_t		buckets_1ms[31];
	uint32_t		buckets_32ms[31];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_intel_rw_latency_page) == 380, "Incorrect size");

struct spdk_nvme_intel_temperature_page {
	uint64_t		current_temperature;
	uint64_t		shutdown_flag_last;
	uint64_t		shutdown_flag_life;
	uint64_t		highest_temperature;
	uint64_t		lowest_temperature;
	uint64_t		reserved[5];
	uint64_t		specified_max_op_temperature;
	uint64_t		reserved2;
	uint64_t		specified_min_op_temperature;
	uint64_t		estimated_offset;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_intel_temperature_page) == 112, "Incorrect size");

struct spdk_nvme_intel_smart_attribute {
	uint8_t			code;
	uint8_t			reserved[2];
	uint8_t			normalized_value;
	uint8_t			reserved2;
	uint8_t			raw_value[6];
	uint8_t			reserved3;
};

struct __attribute__((packed)) spdk_nvme_intel_smart_information_page {
	struct spdk_nvme_intel_smart_attribute	attributes[13];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_intel_smart_information_page) == 156, "Incorrect size");

union spdk_nvme_intel_feat_power_governor {
	uint32_t	raw;
	struct {
		/** power governor setting : 00h = 25W 01h = 20W 02h = 10W */
		uint32_t power_governor_setting		: 8;
		uint32_t reserved	: 24;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_intel_feat_power_governor) == 4, "Incorrect size");

union spdk_nvme_intel_feat_smbus_address {
	uint32_t	raw;
	struct {
		uint32_t reserved	: 1;
		uint32_t smbus_controller_address	: 8;
		uint32_t reserved2	: 23;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_intel_feat_smbus_address) == 4, "Incorrect size");

union spdk_nvme_intel_feat_led_pattern {
	uint32_t	raw;
	struct {
		uint32_t feature_options	: 24;
		uint32_t value	: 8;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_intel_feat_led_pattern) == 4, "Incorrect size");

union spdk_nvme_intel_feat_reset_timed_workload_counters {
	uint32_t	raw;
	struct {
		/**
		 * Write Usage: 00 = NOP, 1 = Reset E2, E3,E4 counters;
		 * Read Usage: Not Supported
		 */
		uint32_t reset	: 1;
		uint32_t reserved	: 31;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_intel_feat_reset_timed_workload_counters) == 4,
		   "Incorrect size");

union spdk_nvme_intel_feat_latency_tracking {
	uint32_t	raw;
	struct {
		/**
		 * Write Usage:
		 * 00h = Disable Latency Tracking (Default)
		 * 01h = Enable Latency Tracking
		 */
		uint32_t enable	: 32;
	} bits;
};
SPDK_STATIC_ASSERT(sizeof(union spdk_nvme_intel_feat_latency_tracking) == 4, "Incorrect size");

struct spdk_nvme_intel_marketing_description_page {
	uint8_t		marketing_product[512];
	/* Spec says this log page will only write 512 bytes, but there are some older FW
	 * versions that accidentally write 516 instead.  So just pad this out to 4096 bytes
	 * to make sure users of this structure never end up overwriting unintended parts of
	 * memory.
	 */
	uint8_t		reserved[3584];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_intel_marketing_description_page) == 4096,
		   "Incorrect size");
#ifdef __cplusplus
}
#endif

#endif
