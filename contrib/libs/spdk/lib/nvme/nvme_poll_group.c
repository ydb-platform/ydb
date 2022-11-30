#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2021 Mellanox Technologies LTD. All rights reserved.
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


#include "nvme_internal.h"

struct spdk_nvme_poll_group *
spdk_nvme_poll_group_create(void *ctx, struct spdk_nvme_accel_fn_table *table)
{
	struct spdk_nvme_poll_group *group;

	group = calloc(1, sizeof(*group));
	if (group == NULL) {
		return NULL;
	}

	group->accel_fn_table.table_size = sizeof(struct spdk_nvme_accel_fn_table);
	if (table && table->table_size != 0) {
		group->accel_fn_table.table_size = table->table_size;
#define SET_FIELD(field) \
	if (offsetof(struct spdk_nvme_accel_fn_table, field) + sizeof(table->field) <= table->table_size) { \
		group->accel_fn_table.field = table->field; \
	} \

		SET_FIELD(submit_accel_crc32c);
		/* Do not remove this statement, you should always update this statement when you adding a new field,
		 * and do not forget to add the SET_FIELD statement for your added field. */
		SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_accel_fn_table) == 16, "Incorrect size");

#undef SET_FIELD
	}

	group->ctx = ctx;
	STAILQ_INIT(&group->tgroups);

	return group;
}

struct spdk_nvme_poll_group *
spdk_nvme_qpair_get_optimal_poll_group(struct spdk_nvme_qpair *qpair)
{
	struct spdk_nvme_transport_poll_group *tgroup;

	tgroup = nvme_transport_qpair_get_optimal_poll_group(qpair->transport, qpair);

	if (tgroup == NULL) {
		return NULL;
	}

	return tgroup->group;
}

int
spdk_nvme_poll_group_add(struct spdk_nvme_poll_group *group, struct spdk_nvme_qpair *qpair)
{
	struct spdk_nvme_transport_poll_group *tgroup;
	const struct spdk_nvme_transport *transport;

	if (nvme_qpair_get_state(qpair) != NVME_QPAIR_DISCONNECTED) {
		return -EINVAL;
	}

	STAILQ_FOREACH(tgroup, &group->tgroups, link) {
		if (tgroup->transport == qpair->transport) {
			break;
		}
	}

	/* See if a new transport has been added (dlopen style) and we need to update the poll group */
	if (!tgroup) {
		transport = nvme_get_first_transport();
		while (transport != NULL) {
			if (transport == qpair->transport) {
				tgroup = nvme_transport_poll_group_create(transport);
				if (tgroup == NULL) {
					return -ENOMEM;
				}
				tgroup->group = group;
				STAILQ_INSERT_TAIL(&group->tgroups, tgroup, link);
				break;
			}
			transport = nvme_get_next_transport(transport);
		}
	}

	return tgroup ? nvme_transport_poll_group_add(tgroup, qpair) : -ENODEV;
}

int
spdk_nvme_poll_group_remove(struct spdk_nvme_poll_group *group, struct spdk_nvme_qpair *qpair)
{
	struct spdk_nvme_transport_poll_group *tgroup;

	STAILQ_FOREACH(tgroup, &group->tgroups, link) {
		if (tgroup->transport == qpair->transport) {
			return nvme_transport_poll_group_remove(tgroup, qpair);
		}
	}

	return -ENODEV;
}

int
nvme_poll_group_connect_qpair(struct spdk_nvme_qpair *qpair)
{
	return nvme_transport_poll_group_connect_qpair(qpair);
}

int
nvme_poll_group_disconnect_qpair(struct spdk_nvme_qpair *qpair)
{
	return nvme_transport_poll_group_disconnect_qpair(qpair);
}

int64_t
spdk_nvme_poll_group_process_completions(struct spdk_nvme_poll_group *group,
		uint32_t completions_per_qpair, spdk_nvme_disconnected_qpair_cb disconnected_qpair_cb)
{
	struct spdk_nvme_transport_poll_group *tgroup;
	int64_t local_completions = 0, error_reason = 0, num_completions = 0;

	if (disconnected_qpair_cb == NULL) {
		return -EINVAL;
	}

	STAILQ_FOREACH(tgroup, &group->tgroups, link) {
		local_completions = nvme_transport_poll_group_process_completions(tgroup, completions_per_qpair,
				    disconnected_qpair_cb);
		if (local_completions < 0 && error_reason == 0) {
			error_reason = local_completions;
		} else {
			num_completions += local_completions;
			/* Just to be safe */
			assert(num_completions >= 0);
		}
	}

	return error_reason ? error_reason : num_completions;
}

void *
spdk_nvme_poll_group_get_ctx(struct spdk_nvme_poll_group *group)
{
	return group->ctx;
}

int
spdk_nvme_poll_group_destroy(struct spdk_nvme_poll_group *group)
{
	struct spdk_nvme_transport_poll_group *tgroup, *tmp_tgroup;

	STAILQ_FOREACH_SAFE(tgroup, &group->tgroups, link, tmp_tgroup) {
		STAILQ_REMOVE(&group->tgroups, tgroup, spdk_nvme_transport_poll_group, link);
		if (nvme_transport_poll_group_destroy(tgroup) != 0) {
			STAILQ_INSERT_TAIL(&group->tgroups, tgroup, link);
			return -EBUSY;
		}

	}

	free(group);

	return 0;
}

int
spdk_nvme_poll_group_get_stats(struct spdk_nvme_poll_group *group,
			       struct spdk_nvme_poll_group_stat **stats)
{
	struct spdk_nvme_transport_poll_group *tgroup;
	struct spdk_nvme_poll_group_stat *result;
	uint32_t transports_count = 0;
	/* Not all transports used by this poll group may support statistics reporting */
	uint32_t reported_stats_count = 0;
	int rc;

	assert(group);
	assert(stats);

	result = calloc(1, sizeof(*result));
	if (!result) {
		SPDK_ERRLOG("Failed to allocate memory for poll group statistics\n");
		return -ENOMEM;
	}

	STAILQ_FOREACH(tgroup, &group->tgroups, link) {
		transports_count++;
	}

	result->transport_stat = calloc(transports_count, sizeof(*result->transport_stat));
	if (!result->transport_stat) {
		SPDK_ERRLOG("Failed to allocate memory for poll group statistics\n");
		free(result);
		return -ENOMEM;
	}

	STAILQ_FOREACH(tgroup, &group->tgroups, link) {
		rc = nvme_transport_poll_group_get_stats(tgroup, &result->transport_stat[reported_stats_count]);
		if (rc == 0) {
			reported_stats_count++;
		}
	}

	if (reported_stats_count == 0) {
		free(result->transport_stat);
		free(result);
		SPDK_DEBUGLOG(nvme, "No transport statistics available\n");
		return -ENOTSUP;
	}

	result->num_transports = reported_stats_count;
	*stats = result;

	return 0;
}

void
spdk_nvme_poll_group_free_stats(struct spdk_nvme_poll_group *group,
				struct spdk_nvme_poll_group_stat *stat)
{
	struct spdk_nvme_transport_poll_group *tgroup;
	uint32_t i;
	uint32_t freed_stats __attribute__((unused)) = 0;

	assert(group);
	assert(stat);

	for (i = 0; i < stat->num_transports; i++) {
		STAILQ_FOREACH(tgroup, &group->tgroups, link) {
			if (nvme_transport_get_trtype(tgroup->transport) == stat->transport_stat[i]->trtype) {
				nvme_transport_poll_group_free_stats(tgroup, stat->transport_stat[i]);
				freed_stats++;
				break;
			}
		}
	}

	assert(freed_stats == stat->num_transports);

	free(stat->transport_stat);
	free(stat);
}
