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
 * Tracepoint library
 */

#ifndef _SPDK_TRACE_H_
#define _SPDK_TRACE_H_

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SPDK_DEFAULT_NUM_TRACE_ENTRIES	 (32 * 1024)

struct spdk_trace_entry {
	uint64_t	tsc;
	uint16_t	tpoint_id;
	uint16_t	poller_id;
	uint32_t	size;
	uint64_t	object_id;
	uint64_t	arg1;
};

/* If type changes from a uint8_t, change this value. */
#define SPDK_TRACE_MAX_OWNER (UCHAR_MAX + 1)

struct spdk_trace_owner {
	uint8_t	type;
	char	id_prefix;
};

/* If type changes from a uint8_t, change this value. */
#define SPDK_TRACE_MAX_OBJECT (UCHAR_MAX + 1)

struct spdk_trace_object {
	uint8_t	type;
	char	id_prefix;
};

#define SPDK_TRACE_MAX_GROUP_ID  16
#define SPDK_TRACE_MAX_TPOINT_ID (SPDK_TRACE_MAX_GROUP_ID * 64)
#define SPDK_TPOINT_ID(group, tpoint)	((group * 64) + tpoint)

#define SPDK_TRACE_ARG_TYPE_INT 0
#define SPDK_TRACE_ARG_TYPE_PTR 1
#define SPDK_TRACE_ARG_TYPE_STR 2

struct spdk_trace_tpoint {
	char		name[24];
	uint16_t	tpoint_id;
	uint8_t		owner_type;
	uint8_t		object_type;
	uint8_t		new_object;
	uint8_t		arg1_type;
	uint8_t		reserved;
	char		arg1_name[8];
};

struct spdk_trace_history {
	/** Logical core number associated with this structure instance. */
	int				lcore;

	/** Number of trace_entries contained in each trace_history. */
	uint64_t			num_entries;

	/**
	 * Running count of number of occurrences of each tracepoint on this
	 *  lcore.  Debug tools can use this to easily count tracepoints such as
	 *  number of SCSI tasks completed or PDUs read.
	 */
	uint64_t			tpoint_count[SPDK_TRACE_MAX_TPOINT_ID];

	/** Index to next spdk_trace_entry to fill. */
	uint64_t			next_entry;

	/**
	 * Circular buffer of spdk_trace_entry structures for tracing
	 *  tpoints on this core.  Debug tool spdk_trace reads this
	 *  buffer from shared memory to post-process the tpoint entries and
	 *  display in a human-readable format.
	 */
	struct spdk_trace_entry		entries[0];
};

#define SPDK_TRACE_MAX_LCORE		128

struct spdk_trace_flags {
	uint64_t			tsc_rate;
	uint64_t			tpoint_mask[SPDK_TRACE_MAX_GROUP_ID];
	struct spdk_trace_owner		owner[UCHAR_MAX + 1];
	struct spdk_trace_object	object[UCHAR_MAX + 1];
	struct spdk_trace_tpoint	tpoint[SPDK_TRACE_MAX_TPOINT_ID];

	/** Offset of each trace_history from the beginning of this data structure.
	 * The last one is the offset of the file end.
	 */
	uint64_t			lcore_history_offsets[SPDK_TRACE_MAX_LCORE + 1];
};
extern struct spdk_trace_flags *g_trace_flags;
extern struct spdk_trace_histories *g_trace_histories;


struct spdk_trace_histories {
	struct spdk_trace_flags flags;

	/**
	 * struct spdk_trace_history has a dynamic size determined by num_entries
	 * in spdk_trace_init. Mark array size of per_lcore_history to be 0 in uint8_t
	 * as a reminder that each per_lcore_history pointer should be gotten by
	 * proper API, instead of directly referencing by struct element.
	 */
	uint8_t	per_lcore_history[0];
};

static inline uint64_t
spdk_get_trace_history_size(uint64_t num_entries)
{
	return sizeof(struct spdk_trace_history) + num_entries * sizeof(struct spdk_trace_entry);
}

static inline uint64_t
spdk_get_trace_histories_size(struct spdk_trace_histories *trace_histories)
{
	return trace_histories->flags.lcore_history_offsets[SPDK_TRACE_MAX_LCORE];
}

static inline struct spdk_trace_history *
spdk_get_per_lcore_history(struct spdk_trace_histories *trace_histories, unsigned lcore)
{
	char *lcore_history_offset;

	if (lcore >= SPDK_TRACE_MAX_LCORE) {
		return NULL;
	}

	lcore_history_offset = (char *)trace_histories;
	lcore_history_offset += trace_histories->flags.lcore_history_offsets[lcore];

	return (struct spdk_trace_history *)lcore_history_offset;
}

void _spdk_trace_record(uint64_t tsc, uint16_t tpoint_id, uint16_t poller_id,
			uint32_t size, uint64_t object_id, uint64_t arg1);

/**
 * Record the current trace state for tracing tpoints. Debug tool can read the
 * information from shared memory to post-process the tpoint entries and display
 * in a human-readable format. This function will call spdk_get_ticks() to get
 * the current tsc to save in the tracepoint.
 *
 * \param tpoint_id Tracepoint id to record.
 * \param poller_id Poller id to record.
 * \param size Size to record.
 * \param object_id Object id to record.
 * \param arg1 Argument to record.
 */
static inline
void spdk_trace_record(uint16_t tpoint_id, uint16_t poller_id, uint32_t size,
		       uint64_t object_id, uint64_t arg1)
{
	/*
	 * Tracepoint group ID is encoded in the tpoint_id.  Lower 6 bits determine the tracepoint
	 *  within the group, the remaining upper bits determine the tracepoint group.  Each
	 *  tracepoint group has its own tracepoint mask.
	 */
	assert(tpoint_id < SPDK_TRACE_MAX_TPOINT_ID);
	if (g_trace_histories == NULL ||
	    !((1ULL << (tpoint_id & 0x3F)) & g_trace_histories->flags.tpoint_mask[tpoint_id >> 6])) {
		return;
	}

	_spdk_trace_record(0, tpoint_id, poller_id, size, object_id, arg1);
}

/**
 * Record the current trace state for tracing tpoints. Debug tool can read the
 * information from shared memory to post-process the tpoint entries and display
 * in a human-readable format.
 *
 * \param tsc Current tsc.
 * \param tpoint_id Tracepoint id to record.
 * \param poller_id Poller id to record.
 * \param size Size to record.
 * \param object_id Object id to record.
 * \param arg1 Argument to record.
 */
static inline
void spdk_trace_record_tsc(uint64_t tsc, uint16_t tpoint_id, uint16_t poller_id,
			   uint32_t size, uint64_t object_id, uint64_t arg1)
{
	/*
	 * Tracepoint group ID is encoded in the tpoint_id.  Lower 6 bits determine the tracepoint
	 *  within the group, the remaining upper bits determine the tracepoint group.  Each
	 *  tracepoint group has its own tracepoint mask.
	 */
	assert(tpoint_id < SPDK_TRACE_MAX_TPOINT_ID);
	if (g_trace_histories == NULL ||
	    !((1ULL << (tpoint_id & 0x3F)) & g_trace_histories->flags.tpoint_mask[tpoint_id >> 6])) {
		return;
	}

	_spdk_trace_record(tsc, tpoint_id, poller_id, size, object_id, arg1);
}

/**
 * Get the current tpoint mask of the given tpoint group.
 *
 * \param group_id Tpoint group id associated with the tpoint mask.
 *
 * \return current tpoint mask.
 */
uint64_t spdk_trace_get_tpoint_mask(uint32_t group_id);

/**
 * Add the specified tpoints to the current tpoint mask for the given tpoint group.
 *
 * \param group_id Tpoint group id associated with the tpoint mask.
 * \param tpoint_mask Tpoint mask which indicates which tpoints to add to the
 * current tpoint mask.
 */
void spdk_trace_set_tpoints(uint32_t group_id, uint64_t tpoint_mask);

/**
 * Clear the specified tpoints from the current tpoint mask for the given tpoint group.
 *
 * \param group_id Tpoint group id associated with the tpoint mask.
 * \param tpoint_mask Tpoint mask which indicates which tpoints to clear from
 * the current tpoint mask.
 */
void spdk_trace_clear_tpoints(uint32_t group_id, uint64_t tpoint_mask);

/**
 * Get a mask of all tracepoint groups which have at least one tracepoint enabled.
 *
 * \return a mask of all tracepoint groups.
 */
uint64_t spdk_trace_get_tpoint_group_mask(void);

/**
 * For each tpoint group specified in the group mask, enable all of its tpoints.
 *
 * \param tpoint_group_mask Tpoint group mask that indicates which tpoints to enable.
 */
void spdk_trace_set_tpoint_group_mask(uint64_t tpoint_group_mask);

/**
 * For each tpoint group specified in the group mask, disable all of its tpoints.
 *
 * \param tpoint_group_mask Tpoint group mask that indicates which tpoints to disable.
 */
void spdk_trace_clear_tpoint_group_mask(uint64_t tpoint_group_mask);

/**
 * Initialize the trace environment. Debug tool can read the information from
 * the given shared memory to post-process the tpoint entries and display in a
 * human-readable format.
 *
 * \param shm_name Name of shared memory.
 * \param num_entries Number of trace entries per lcore.
 * \return 0 on success, else non-zero indicates a failure.
 */
int spdk_trace_init(const char *shm_name, uint64_t num_entries);

/**
 * Unmap global trace memory structs.
 */
void spdk_trace_cleanup(void);

/**
 * Initialize trace flags.
 */
void spdk_trace_flags_init(void);

#define OWNER_NONE 0
#define OBJECT_NONE 0

/**
 * Register the trace owner.
 *
 * \param type Type of the trace owner.
 * \param id_prefix Prefix of id for the trace owner.
 */
void spdk_trace_register_owner(uint8_t type, char id_prefix);

/**
 * Register the trace object.
 *
 * \param type Type of the trace object.
 * \param id_prefix Prefix of id for the trace object.
 */
void spdk_trace_register_object(uint8_t type, char id_prefix);

/**
 * Register the description for the tpoint.
 *
 * \param name Name for the tpoint.
 * \param tpoint_id Id for the tpoint.
 * \param owner_type Owner type for the tpoint.
 * \param object_type Object type for the tpoint.
 * \param new_object New object for the tpoint.
 * \param arg1_type Type of arg1.
 * \param arg1_name Name of argument.
 */
void spdk_trace_register_description(const char *name, uint16_t tpoint_id, uint8_t owner_type,
				     uint8_t object_type, uint8_t new_object,
				     uint8_t arg1_type, const char *arg1_name);

struct spdk_trace_register_fn *spdk_trace_get_first_register_fn(void);

struct spdk_trace_register_fn *spdk_trace_get_next_register_fn(struct spdk_trace_register_fn
		*register_fn);

/**
 * Enable trace on specific tpoint group
 *
 * \param group_name Name of group to enable, "all" for enabling all groups.
 * \return 0 on success, else non-zero indicates a failure.
 */
int spdk_trace_enable_tpoint_group(const char *group_name);

/**
 * Disable trace on specific tpoint group
 *
 * \param group_name Name of group to disable, "all" for disabling all groups.
 * \return 0 on success, else non-zero indicates a failure.
 */
int spdk_trace_disable_tpoint_group(const char *group_name);

/**
 * Show trace mask and its usage.
 *
 * \param f File to hold the mask's information.
 * \param tmask_arg Command line option to set the trace group mask.
 */
void spdk_trace_mask_usage(FILE *f, const char *tmask_arg);

struct spdk_trace_register_fn {
	const char *name;
	uint8_t tgroup_id;
	void (*reg_fn)(void);
	struct spdk_trace_register_fn *next;
};

/**
 * Add new trace register function.
 *
 * \param reg_fn Trace register function to add.
 */
void spdk_trace_add_register_fn(struct spdk_trace_register_fn *reg_fn);

#define SPDK_TRACE_REGISTER_FN(fn, name_str, _tgroup_id)	\
	static void fn(void);					\
	struct spdk_trace_register_fn reg_ ## fn = {		\
		.name = name_str,				\
		.tgroup_id = _tgroup_id,			\
		.reg_fn = fn,					\
		.next = NULL,					\
	};							\
	__attribute__((constructor)) static void _ ## fn(void)	\
	{							\
		spdk_trace_add_register_fn(&reg_ ## fn);	\
	}							\
	static void fn(void)

#ifdef __cplusplus
}
#endif

#endif
