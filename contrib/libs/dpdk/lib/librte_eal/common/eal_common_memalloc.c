#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2017-2018 Intel Corporation
 */

#include <string.h>

#include <rte_errno.h>
#include <rte_lcore.h>
#include <rte_fbarray.h>
#include <rte_memzone.h>
#include <rte_memory.h>
#include <rte_string_fns.h>
#include <rte_rwlock.h>

#include "eal_private.h"
#include "eal_internal_cfg.h"
#include "eal_memalloc.h"

struct mem_event_callback_entry {
	TAILQ_ENTRY(mem_event_callback_entry) next;
	char name[RTE_MEM_EVENT_CALLBACK_NAME_LEN];
	rte_mem_event_callback_t clb;
	void *arg;
};

struct mem_alloc_validator_entry {
	TAILQ_ENTRY(mem_alloc_validator_entry) next;
	char name[RTE_MEM_ALLOC_VALIDATOR_NAME_LEN];
	rte_mem_alloc_validator_t clb;
	int socket_id;
	size_t limit;
};

/** Double linked list of actions. */
TAILQ_HEAD(mem_event_callback_entry_list, mem_event_callback_entry);
TAILQ_HEAD(mem_alloc_validator_entry_list, mem_alloc_validator_entry);

static struct mem_event_callback_entry_list mem_event_callback_list =
	TAILQ_HEAD_INITIALIZER(mem_event_callback_list);
static rte_rwlock_t mem_event_rwlock = RTE_RWLOCK_INITIALIZER;

static struct mem_alloc_validator_entry_list mem_alloc_validator_list =
	TAILQ_HEAD_INITIALIZER(mem_alloc_validator_list);
static rte_rwlock_t mem_alloc_validator_rwlock = RTE_RWLOCK_INITIALIZER;

static struct mem_event_callback_entry *
find_mem_event_callback(const char *name, void *arg)
{
	struct mem_event_callback_entry *r;

	TAILQ_FOREACH(r, &mem_event_callback_list, next) {
		if (!strcmp(r->name, name) && r->arg == arg)
			break;
	}
	return r;
}

static struct mem_alloc_validator_entry *
find_mem_alloc_validator(const char *name, int socket_id)
{
	struct mem_alloc_validator_entry *r;

	TAILQ_FOREACH(r, &mem_alloc_validator_list, next) {
		if (!strcmp(r->name, name) && r->socket_id == socket_id)
			break;
	}
	return r;
}

bool
eal_memalloc_is_contig(const struct rte_memseg_list *msl, void *start,
		size_t len)
{
	void *end, *aligned_start, *aligned_end;
	size_t pgsz = (size_t)msl->page_sz;
	const struct rte_memseg *ms;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* for IOVA_VA, it's always contiguous */
	if (rte_eal_iova_mode() == RTE_IOVA_VA && !msl->external)
		return true;

	/* for legacy memory, it's always contiguous */
	if (internal_conf->legacy_mem)
		return true;

	end = RTE_PTR_ADD(start, len);

	/* for nohuge, we check pagemap, otherwise check memseg */
	if (!rte_eal_has_hugepages()) {
		rte_iova_t cur, expected;

		aligned_start = RTE_PTR_ALIGN_FLOOR(start, pgsz);
		aligned_end = RTE_PTR_ALIGN_CEIL(end, pgsz);

		/* if start and end are on the same page, bail out early */
		if (RTE_PTR_DIFF(aligned_end, aligned_start) == pgsz)
			return true;

		/* skip first iteration */
		cur = rte_mem_virt2iova(aligned_start);
		expected = cur + pgsz;
		aligned_start = RTE_PTR_ADD(aligned_start, pgsz);

		while (aligned_start < aligned_end) {
			cur = rte_mem_virt2iova(aligned_start);
			if (cur != expected)
				return false;
			aligned_start = RTE_PTR_ADD(aligned_start, pgsz);
			expected += pgsz;
		}
	} else {
		int start_seg, end_seg, cur_seg;
		rte_iova_t cur, expected;

		aligned_start = RTE_PTR_ALIGN_FLOOR(start, pgsz);
		aligned_end = RTE_PTR_ALIGN_CEIL(end, pgsz);

		start_seg = RTE_PTR_DIFF(aligned_start, msl->base_va) /
				pgsz;
		end_seg = RTE_PTR_DIFF(aligned_end, msl->base_va) /
				pgsz;

		/* if start and end are on the same page, bail out early */
		if (RTE_PTR_DIFF(aligned_end, aligned_start) == pgsz)
			return true;

		/* skip first iteration */
		ms = rte_fbarray_get(&msl->memseg_arr, start_seg);
		cur = ms->iova;
		expected = cur + pgsz;

		/* if we can't access IOVA addresses, assume non-contiguous */
		if (cur == RTE_BAD_IOVA)
			return false;

		for (cur_seg = start_seg + 1; cur_seg < end_seg;
				cur_seg++, expected += pgsz) {
			ms = rte_fbarray_get(&msl->memseg_arr, cur_seg);

			if (ms->iova != expected)
				return false;
		}
	}
	return true;
}

int
eal_memalloc_mem_event_callback_register(const char *name,
		rte_mem_event_callback_t clb, void *arg)
{
	struct mem_event_callback_entry *entry;
	int ret, len;
	if (name == NULL || clb == NULL) {
		rte_errno = EINVAL;
		return -1;
	}
	len = strnlen(name, RTE_MEM_EVENT_CALLBACK_NAME_LEN);
	if (len == 0) {
		rte_errno = EINVAL;
		return -1;
	} else if (len == RTE_MEM_EVENT_CALLBACK_NAME_LEN) {
		rte_errno = ENAMETOOLONG;
		return -1;
	}
	rte_rwlock_write_lock(&mem_event_rwlock);

	entry = find_mem_event_callback(name, arg);
	if (entry != NULL) {
		rte_errno = EEXIST;
		ret = -1;
		goto unlock;
	}

	entry = malloc(sizeof(*entry));
	if (entry == NULL) {
		rte_errno = ENOMEM;
		ret = -1;
		goto unlock;
	}

	/* callback successfully created and is valid, add it to the list */
	entry->clb = clb;
	entry->arg = arg;
	strlcpy(entry->name, name, RTE_MEM_EVENT_CALLBACK_NAME_LEN);
	TAILQ_INSERT_TAIL(&mem_event_callback_list, entry, next);

	ret = 0;

	RTE_LOG(DEBUG, EAL, "Mem event callback '%s:%p' registered\n",
			name, arg);

unlock:
	rte_rwlock_write_unlock(&mem_event_rwlock);
	return ret;
}

int
eal_memalloc_mem_event_callback_unregister(const char *name, void *arg)
{
	struct mem_event_callback_entry *entry;
	int ret, len;

	if (name == NULL) {
		rte_errno = EINVAL;
		return -1;
	}
	len = strnlen(name, RTE_MEM_EVENT_CALLBACK_NAME_LEN);
	if (len == 0) {
		rte_errno = EINVAL;
		return -1;
	} else if (len == RTE_MEM_EVENT_CALLBACK_NAME_LEN) {
		rte_errno = ENAMETOOLONG;
		return -1;
	}
	rte_rwlock_write_lock(&mem_event_rwlock);

	entry = find_mem_event_callback(name, arg);
	if (entry == NULL) {
		rte_errno = ENOENT;
		ret = -1;
		goto unlock;
	}
	TAILQ_REMOVE(&mem_event_callback_list, entry, next);
	free(entry);

	ret = 0;

	RTE_LOG(DEBUG, EAL, "Mem event callback '%s:%p' unregistered\n",
			name, arg);

unlock:
	rte_rwlock_write_unlock(&mem_event_rwlock);
	return ret;
}

void
eal_memalloc_mem_event_notify(enum rte_mem_event event, const void *start,
		size_t len)
{
	struct mem_event_callback_entry *entry;

	rte_rwlock_read_lock(&mem_event_rwlock);

	TAILQ_FOREACH(entry, &mem_event_callback_list, next) {
		RTE_LOG(DEBUG, EAL, "Calling mem event callback '%s:%p'\n",
			entry->name, entry->arg);
		entry->clb(event, start, len, entry->arg);
	}

	rte_rwlock_read_unlock(&mem_event_rwlock);
}

int
eal_memalloc_mem_alloc_validator_register(const char *name,
		rte_mem_alloc_validator_t clb, int socket_id, size_t limit)
{
	struct mem_alloc_validator_entry *entry;
	int ret, len;
	if (name == NULL || clb == NULL || socket_id < 0) {
		rte_errno = EINVAL;
		return -1;
	}
	len = strnlen(name, RTE_MEM_ALLOC_VALIDATOR_NAME_LEN);
	if (len == 0) {
		rte_errno = EINVAL;
		return -1;
	} else if (len == RTE_MEM_ALLOC_VALIDATOR_NAME_LEN) {
		rte_errno = ENAMETOOLONG;
		return -1;
	}
	rte_rwlock_write_lock(&mem_alloc_validator_rwlock);

	entry = find_mem_alloc_validator(name, socket_id);
	if (entry != NULL) {
		rte_errno = EEXIST;
		ret = -1;
		goto unlock;
	}

	entry = malloc(sizeof(*entry));
	if (entry == NULL) {
		rte_errno = ENOMEM;
		ret = -1;
		goto unlock;
	}

	/* callback successfully created and is valid, add it to the list */
	entry->clb = clb;
	entry->socket_id = socket_id;
	entry->limit = limit;
	strlcpy(entry->name, name, RTE_MEM_ALLOC_VALIDATOR_NAME_LEN);
	TAILQ_INSERT_TAIL(&mem_alloc_validator_list, entry, next);

	ret = 0;

	RTE_LOG(DEBUG, EAL, "Mem alloc validator '%s' on socket %i with limit %zu registered\n",
		name, socket_id, limit);

unlock:
	rte_rwlock_write_unlock(&mem_alloc_validator_rwlock);
	return ret;
}

int
eal_memalloc_mem_alloc_validator_unregister(const char *name, int socket_id)
{
	struct mem_alloc_validator_entry *entry;
	int ret, len;

	if (name == NULL || socket_id < 0) {
		rte_errno = EINVAL;
		return -1;
	}
	len = strnlen(name, RTE_MEM_ALLOC_VALIDATOR_NAME_LEN);
	if (len == 0) {
		rte_errno = EINVAL;
		return -1;
	} else if (len == RTE_MEM_ALLOC_VALIDATOR_NAME_LEN) {
		rte_errno = ENAMETOOLONG;
		return -1;
	}
	rte_rwlock_write_lock(&mem_alloc_validator_rwlock);

	entry = find_mem_alloc_validator(name, socket_id);
	if (entry == NULL) {
		rte_errno = ENOENT;
		ret = -1;
		goto unlock;
	}
	TAILQ_REMOVE(&mem_alloc_validator_list, entry, next);
	free(entry);

	ret = 0;

	RTE_LOG(DEBUG, EAL, "Mem alloc validator '%s' on socket %i unregistered\n",
		name, socket_id);

unlock:
	rte_rwlock_write_unlock(&mem_alloc_validator_rwlock);
	return ret;
}

int
eal_memalloc_mem_alloc_validate(int socket_id, size_t new_len)
{
	struct mem_alloc_validator_entry *entry;
	int ret = 0;

	rte_rwlock_read_lock(&mem_alloc_validator_rwlock);

	TAILQ_FOREACH(entry, &mem_alloc_validator_list, next) {
		if (entry->socket_id != socket_id || entry->limit > new_len)
			continue;
		RTE_LOG(DEBUG, EAL, "Calling mem alloc validator '%s' on socket %i\n",
			entry->name, entry->socket_id);
		if (entry->clb(socket_id, entry->limit, new_len) < 0)
			ret = -1;
	}

	rte_rwlock_read_unlock(&mem_alloc_validator_rwlock);

	return ret;
}
