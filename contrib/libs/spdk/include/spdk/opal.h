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

#ifndef SPDK_OPAL_H
#define SPDK_OPAL_H

#include "spdk/stdinc.h"
#include "spdk/nvme.h"
#include "spdk/log.h"
#include "spdk/endian.h"
#include "spdk/string.h"
#include "spdk/opal_spec.h"

struct spdk_opal_d0_features_info {
	struct spdk_opal_d0_tper_feat tper;
	struct spdk_opal_d0_locking_feat locking;
	struct spdk_opal_d0_single_user_mode_feat single_user;
	struct spdk_opal_d0_geo_feat geo;
	struct spdk_opal_d0_datastore_feat datastore;
	struct spdk_opal_d0_v100_feat v100;
	struct spdk_opal_d0_v200_feat v200;
};

enum spdk_opal_lock_state {
	OPAL_READONLY	= 0x01,
	OPAL_RWLOCK		= 0x02,
	OPAL_READWRITE	= 0x04,
};

enum spdk_opal_user {
	OPAL_ADMIN1 = 0x0,
	OPAL_USER1 = 0x01,
	OPAL_USER2 = 0x02,
	OPAL_USER3 = 0x03,
	OPAL_USER4 = 0x04,
	OPAL_USER5 = 0x05,
	OPAL_USER6 = 0x06,
	OPAL_USER7 = 0x07,
	OPAL_USER8 = 0x08,
	OPAL_USER9 = 0x09,
};

enum spdk_opal_locking_range {
	OPAL_LOCKING_RANGE_GLOBAL = 0x0,
	OPAL_LOCKING_RANGE_1,
	OPAL_LOCKING_RANGE_2,
	OPAL_LOCKING_RANGE_3,
	OPAL_LOCKING_RANGE_4,
	OPAL_LOCKING_RANGE_5,
	OPAL_LOCKING_RANGE_6,
	OPAL_LOCKING_RANGE_7,
	OPAL_LOCKING_RANGE_8,
	OPAL_LOCKING_RANGE_9,
	OPAL_LOCKING_RANGE_10,
};

struct spdk_opal_locking_range_info {
	uint8_t locking_range_id;
	uint8_t _padding[7];
	uint64_t range_start;
	uint64_t range_length;
	bool read_lock_enabled;
	bool write_lock_enabled;
	bool read_locked;
	bool write_locked;
};

struct spdk_opal_dev;

struct spdk_opal_dev *spdk_opal_dev_construct(struct spdk_nvme_ctrlr *ctrlr);
void spdk_opal_dev_destruct(struct spdk_opal_dev *dev);

struct spdk_opal_d0_features_info *spdk_opal_get_d0_features_info(struct spdk_opal_dev *dev);

int spdk_opal_cmd_take_ownership(struct spdk_opal_dev *dev, char *new_passwd);

/**
 * synchronous function: send and then receive.
 *
 * Wait until response is received.
 */
int spdk_opal_cmd_revert_tper(struct spdk_opal_dev *dev, const char *passwd);

int spdk_opal_cmd_activate_locking_sp(struct spdk_opal_dev *dev, const char *passwd);
int spdk_opal_cmd_lock_unlock(struct spdk_opal_dev *dev, enum spdk_opal_user user,
			      enum spdk_opal_lock_state flag, enum spdk_opal_locking_range locking_range,
			      const char *passwd);
int spdk_opal_cmd_setup_locking_range(struct spdk_opal_dev *dev, enum spdk_opal_user user,
				      enum spdk_opal_locking_range locking_range_id, uint64_t range_start,
				      uint64_t range_length, const char *passwd);

int spdk_opal_cmd_get_max_ranges(struct spdk_opal_dev *dev, const char *passwd);
int spdk_opal_cmd_get_locking_range_info(struct spdk_opal_dev *dev, const char *passwd,
		enum spdk_opal_user user_id,
		enum spdk_opal_locking_range locking_range_id);
int spdk_opal_cmd_enable_user(struct spdk_opal_dev *dev, enum spdk_opal_user user_id,
			      const char *passwd);
int spdk_opal_cmd_add_user_to_locking_range(struct spdk_opal_dev *dev, enum spdk_opal_user user_id,
		enum spdk_opal_locking_range locking_range_id,
		enum spdk_opal_lock_state lock_flag, const char *passwd);
int spdk_opal_cmd_set_new_passwd(struct spdk_opal_dev *dev, enum spdk_opal_user user_id,
				 const char *new_passwd, const char *old_passwd, bool new_user);

int spdk_opal_cmd_erase_locking_range(struct spdk_opal_dev *dev, enum spdk_opal_user user_id,
				      enum spdk_opal_locking_range locking_range_id, const char *password);

int spdk_opal_cmd_secure_erase_locking_range(struct spdk_opal_dev *dev, enum spdk_opal_user user_id,
		enum spdk_opal_locking_range locking_range_id, const char *password);

struct spdk_opal_locking_range_info *spdk_opal_get_locking_range_info(struct spdk_opal_dev *dev,
		enum spdk_opal_locking_range id);
void spdk_opal_free_locking_range_info(struct spdk_opal_dev *dev, enum spdk_opal_locking_range id);
#endif
