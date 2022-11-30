#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2017 Intel Corporation
 */

#include <stdio.h>
#include <unistd.h>
#include <inttypes.h>
#include <limits.h>
#include <string.h>

#include <rte_compat.h>
#include <rte_service.h>
#include <rte_service_component.h>

#include <rte_eal.h>
#include <rte_lcore.h>
#include <rte_common.h>
#include <rte_debug.h>
#include <rte_cycles.h>
#include <rte_atomic.h>
#include <rte_memory.h>
#include <rte_malloc.h>
#include <rte_spinlock.h>

#include "eal_private.h"

#define RTE_SERVICE_NUM_MAX 64

#define SERVICE_F_REGISTERED    (1 << 0)
#define SERVICE_F_STATS_ENABLED (1 << 1)
#define SERVICE_F_START_CHECK   (1 << 2)

/* runstates for services and lcores, denoting if they are active or not */
#define RUNSTATE_STOPPED 0
#define RUNSTATE_RUNNING 1

/* internal representation of a service */
struct rte_service_spec_impl {
	/* public part of the struct */
	struct rte_service_spec spec;

	/* spin lock that when set indicates a service core is currently
	 * running this service callback. When not set, a core may take the
	 * lock and then run the service callback.
	 */
	rte_spinlock_t execute_lock;

	/* API set/get-able variables */
	int8_t app_runstate;
	int8_t comp_runstate;
	uint8_t internal_flags;

	/* per service statistics */
	/* Indicates how many cores the service is mapped to run on.
	 * It does not indicate the number of cores the service is running
	 * on currently.
	 */
	uint32_t num_mapped_cores;
	uint64_t calls;
	uint64_t cycles_spent;
} __rte_cache_aligned;

/* the internal values of a service core */
struct core_state {
	/* map of services IDs are run on this core */
	uint64_t service_mask;
	uint8_t runstate; /* running or stopped */
	uint8_t thread_active; /* indicates when thread is in service_run() */
	uint8_t is_service_core; /* set if core is currently a service core */
	uint8_t service_active_on_lcore[RTE_SERVICE_NUM_MAX];
	uint64_t loops;
	uint64_t calls_per_service[RTE_SERVICE_NUM_MAX];
} __rte_cache_aligned;

static uint32_t rte_service_count;
static struct rte_service_spec_impl *rte_services;
static struct core_state *lcore_states;
static uint32_t rte_service_library_initialized;

int32_t
rte_service_init(void)
{
	if (rte_service_library_initialized) {
		RTE_LOG(NOTICE, EAL,
			"service library init() called, init flag %d\n",
			rte_service_library_initialized);
		return -EALREADY;
	}

	rte_services = rte_calloc("rte_services", RTE_SERVICE_NUM_MAX,
			sizeof(struct rte_service_spec_impl),
			RTE_CACHE_LINE_SIZE);
	if (!rte_services) {
		RTE_LOG(ERR, EAL, "error allocating rte services array\n");
		goto fail_mem;
	}

	lcore_states = rte_calloc("rte_service_core_states", RTE_MAX_LCORE,
			sizeof(struct core_state), RTE_CACHE_LINE_SIZE);
	if (!lcore_states) {
		RTE_LOG(ERR, EAL, "error allocating core states array\n");
		goto fail_mem;
	}

	int i;
	int count = 0;
	struct rte_config *cfg = rte_eal_get_configuration();
	for (i = 0; i < RTE_MAX_LCORE; i++) {
		if (lcore_config[i].core_role == ROLE_SERVICE) {
			if ((unsigned int)i == cfg->main_lcore)
				continue;
			rte_service_lcore_add(i);
			count++;
		}
	}

	rte_service_library_initialized = 1;
	return 0;
fail_mem:
	rte_free(rte_services);
	rte_free(lcore_states);
	return -ENOMEM;
}

void
rte_service_finalize(void)
{
	if (!rte_service_library_initialized)
		return;

	rte_service_lcore_reset_all();
	rte_eal_mp_wait_lcore();

	rte_free(rte_services);
	rte_free(lcore_states);

	rte_service_library_initialized = 0;
}

/* returns 1 if service is registered and has not been unregistered
 * Returns 0 if service never registered, or has been unregistered
 */
static inline int
service_valid(uint32_t id)
{
	return !!(rte_services[id].internal_flags & SERVICE_F_REGISTERED);
}

static struct rte_service_spec_impl *
service_get(uint32_t id)
{
	return &rte_services[id];
}

/* validate ID and retrieve service pointer, or return error value */
#define SERVICE_VALID_GET_OR_ERR_RET(id, service, retval) do {          \
	if (id >= RTE_SERVICE_NUM_MAX || !service_valid(id))            \
		return retval;                                          \
	service = &rte_services[id];                                    \
} while (0)

/* returns 1 if statistics should be collected for service
 * Returns 0 if statistics should not be collected for service
 */
static inline int
service_stats_enabled(struct rte_service_spec_impl *impl)
{
	return !!(impl->internal_flags & SERVICE_F_STATS_ENABLED);
}

static inline int
service_mt_safe(struct rte_service_spec_impl *s)
{
	return !!(s->spec.capabilities & RTE_SERVICE_CAP_MT_SAFE);
}

int32_t
rte_service_set_stats_enable(uint32_t id, int32_t enabled)
{
	struct rte_service_spec_impl *s;
	SERVICE_VALID_GET_OR_ERR_RET(id, s, 0);

	if (enabled)
		s->internal_flags |= SERVICE_F_STATS_ENABLED;
	else
		s->internal_flags &= ~(SERVICE_F_STATS_ENABLED);

	return 0;
}

int32_t
rte_service_set_runstate_mapped_check(uint32_t id, int32_t enabled)
{
	struct rte_service_spec_impl *s;
	SERVICE_VALID_GET_OR_ERR_RET(id, s, 0);

	if (enabled)
		s->internal_flags |= SERVICE_F_START_CHECK;
	else
		s->internal_flags &= ~(SERVICE_F_START_CHECK);

	return 0;
}

uint32_t
rte_service_get_count(void)
{
	return rte_service_count;
}

int32_t
rte_service_get_by_name(const char *name, uint32_t *service_id)
{
	if (!service_id)
		return -EINVAL;

	int i;
	for (i = 0; i < RTE_SERVICE_NUM_MAX; i++) {
		if (service_valid(i) &&
				strcmp(name, rte_services[i].spec.name) == 0) {
			*service_id = i;
			return 0;
		}
	}

	return -ENODEV;
}

const char *
rte_service_get_name(uint32_t id)
{
	struct rte_service_spec_impl *s;
	SERVICE_VALID_GET_OR_ERR_RET(id, s, 0);
	return s->spec.name;
}

int32_t
rte_service_probe_capability(uint32_t id, uint32_t capability)
{
	struct rte_service_spec_impl *s;
	SERVICE_VALID_GET_OR_ERR_RET(id, s, -EINVAL);
	return !!(s->spec.capabilities & capability);
}

int32_t
rte_service_component_register(const struct rte_service_spec *spec,
			       uint32_t *id_ptr)
{
	uint32_t i;
	int32_t free_slot = -1;

	if (spec->callback == NULL || strlen(spec->name) == 0)
		return -EINVAL;

	for (i = 0; i < RTE_SERVICE_NUM_MAX; i++) {
		if (!service_valid(i)) {
			free_slot = i;
			break;
		}
	}

	if ((free_slot < 0) || (i == RTE_SERVICE_NUM_MAX))
		return -ENOSPC;

	struct rte_service_spec_impl *s = &rte_services[free_slot];
	s->spec = *spec;
	s->internal_flags |= SERVICE_F_REGISTERED | SERVICE_F_START_CHECK;

	rte_service_count++;

	if (id_ptr)
		*id_ptr = free_slot;

	return 0;
}

int32_t
rte_service_component_unregister(uint32_t id)
{
	uint32_t i;
	struct rte_service_spec_impl *s;
	SERVICE_VALID_GET_OR_ERR_RET(id, s, -EINVAL);

	rte_service_count--;

	s->internal_flags &= ~(SERVICE_F_REGISTERED);

	/* clear the run-bit in all cores */
	for (i = 0; i < RTE_MAX_LCORE; i++)
		lcore_states[i].service_mask &= ~(UINT64_C(1) << id);

	memset(&rte_services[id], 0, sizeof(struct rte_service_spec_impl));

	return 0;
}

int32_t
rte_service_component_runstate_set(uint32_t id, uint32_t runstate)
{
	struct rte_service_spec_impl *s;
	SERVICE_VALID_GET_OR_ERR_RET(id, s, -EINVAL);

	/* comp_runstate act as the guard variable. Use store-release
	 * memory order. This synchronizes with load-acquire in
	 * service_run and service_runstate_get function.
	 */
	if (runstate)
		__atomic_store_n(&s->comp_runstate, RUNSTATE_RUNNING,
			__ATOMIC_RELEASE);
	else
		__atomic_store_n(&s->comp_runstate, RUNSTATE_STOPPED,
			__ATOMIC_RELEASE);

	return 0;
}

int32_t
rte_service_runstate_set(uint32_t id, uint32_t runstate)
{
	struct rte_service_spec_impl *s;
	SERVICE_VALID_GET_OR_ERR_RET(id, s, -EINVAL);

	/* app_runstate act as the guard variable. Use store-release
	 * memory order. This synchronizes with load-acquire in
	 * service_run runstate_get function.
	 */
	if (runstate)
		__atomic_store_n(&s->app_runstate, RUNSTATE_RUNNING,
			__ATOMIC_RELEASE);
	else
		__atomic_store_n(&s->app_runstate, RUNSTATE_STOPPED,
			__ATOMIC_RELEASE);

	return 0;
}

int32_t
rte_service_runstate_get(uint32_t id)
{
	struct rte_service_spec_impl *s;
	SERVICE_VALID_GET_OR_ERR_RET(id, s, -EINVAL);

	/* comp_runstate and app_runstate act as the guard variables.
	 * Use load-acquire memory order. This synchronizes with
	 * store-release in service state set functions.
	 */
	if (__atomic_load_n(&s->comp_runstate, __ATOMIC_ACQUIRE) ==
			RUNSTATE_RUNNING &&
	    __atomic_load_n(&s->app_runstate, __ATOMIC_ACQUIRE) ==
			RUNSTATE_RUNNING) {
		int check_disabled = !(s->internal_flags &
			SERVICE_F_START_CHECK);
		int lcore_mapped = (__atomic_load_n(&s->num_mapped_cores,
			__ATOMIC_RELAXED) > 0);

		return (check_disabled | lcore_mapped);
	} else
		return 0;

}

static inline void
service_runner_do_callback(struct rte_service_spec_impl *s,
			   struct core_state *cs, uint32_t service_idx)
{
	void *userdata = s->spec.callback_userdata;

	if (service_stats_enabled(s)) {
		uint64_t start = rte_rdtsc();
		s->spec.callback(userdata);
		uint64_t end = rte_rdtsc();
		s->cycles_spent += end - start;
		cs->calls_per_service[service_idx]++;
		s->calls++;
	} else
		s->spec.callback(userdata);
}


/* Expects the service 's' is valid. */
static int32_t
service_run(uint32_t i, struct core_state *cs, uint64_t service_mask,
	    struct rte_service_spec_impl *s, uint32_t serialize_mt_unsafe)
{
	if (!s)
		return -EINVAL;

	/* comp_runstate and app_runstate act as the guard variables.
	 * Use load-acquire memory order. This synchronizes with
	 * store-release in service state set functions.
	 */
	if (__atomic_load_n(&s->comp_runstate, __ATOMIC_ACQUIRE) !=
			RUNSTATE_RUNNING ||
	    __atomic_load_n(&s->app_runstate, __ATOMIC_ACQUIRE) !=
			RUNSTATE_RUNNING ||
	    !(service_mask & (UINT64_C(1) << i))) {
		cs->service_active_on_lcore[i] = 0;
		return -ENOEXEC;
	}

	cs->service_active_on_lcore[i] = 1;

	if ((service_mt_safe(s) == 0) && (serialize_mt_unsafe == 1)) {
		if (!rte_spinlock_trylock(&s->execute_lock))
			return -EBUSY;

		service_runner_do_callback(s, cs, i);
		rte_spinlock_unlock(&s->execute_lock);
	} else
		service_runner_do_callback(s, cs, i);

	return 0;
}

int32_t
rte_service_may_be_active(uint32_t id)
{
	uint32_t ids[RTE_MAX_LCORE] = {0};
	int32_t lcore_count = rte_service_lcore_list(ids, RTE_MAX_LCORE);
	int i;

	if (id >= RTE_SERVICE_NUM_MAX || !service_valid(id))
		return -EINVAL;

	for (i = 0; i < lcore_count; i++) {
		if (lcore_states[ids[i]].service_active_on_lcore[id])
			return 1;
	}

	return 0;
}

int32_t
rte_service_run_iter_on_app_lcore(uint32_t id, uint32_t serialize_mt_unsafe)
{
	struct core_state *cs = &lcore_states[rte_lcore_id()];
	struct rte_service_spec_impl *s;

	SERVICE_VALID_GET_OR_ERR_RET(id, s, -EINVAL);

	/* Increment num_mapped_cores to reflect that this core is
	 * now mapped capable of running the service.
	 */
	__atomic_add_fetch(&s->num_mapped_cores, 1, __ATOMIC_RELAXED);

	int ret = service_run(id, cs, UINT64_MAX, s, serialize_mt_unsafe);

	__atomic_sub_fetch(&s->num_mapped_cores, 1, __ATOMIC_RELAXED);

	return ret;
}

static int32_t
service_runner_func(void *arg)
{
	RTE_SET_USED(arg);
	uint32_t i;
	const int lcore = rte_lcore_id();
	struct core_state *cs = &lcore_states[lcore];

	__atomic_store_n(&cs->thread_active, 1, __ATOMIC_SEQ_CST);

	/* runstate act as the guard variable. Use load-acquire
	 * memory order here to synchronize with store-release
	 * in runstate update functions.
	 */
	while (__atomic_load_n(&cs->runstate, __ATOMIC_ACQUIRE) ==
			RUNSTATE_RUNNING) {
		const uint64_t service_mask = cs->service_mask;

		for (i = 0; i < RTE_SERVICE_NUM_MAX; i++) {
			if (!service_valid(i))
				continue;
			/* return value ignored as no change to code flow */
			service_run(i, cs, service_mask, service_get(i), 1);
		}

		cs->loops++;
	}

	/* Use SEQ CST memory ordering to avoid any re-ordering around
	 * this store, ensuring that once this store is visible, the service
	 * lcore thread really is done in service cores code.
	 */
	__atomic_store_n(&cs->thread_active, 0, __ATOMIC_SEQ_CST);
	return 0;
}

int32_t
rte_service_lcore_may_be_active(uint32_t lcore)
{
	if (lcore >= RTE_MAX_LCORE || !lcore_states[lcore].is_service_core)
		return -EINVAL;

	/* Load thread_active using ACQUIRE to avoid instructions dependent on
	 * the result being re-ordered before this load completes.
	 */
	return __atomic_load_n(&lcore_states[lcore].thread_active,
			       __ATOMIC_ACQUIRE);
}

int32_t
rte_service_lcore_count(void)
{
	int32_t count = 0;
	uint32_t i;
	for (i = 0; i < RTE_MAX_LCORE; i++)
		count += lcore_states[i].is_service_core;
	return count;
}

int32_t
rte_service_lcore_list(uint32_t array[], uint32_t n)
{
	uint32_t count = rte_service_lcore_count();
	if (count > n)
		return -ENOMEM;

	if (!array)
		return -EINVAL;

	uint32_t i;
	uint32_t idx = 0;
	for (i = 0; i < RTE_MAX_LCORE; i++) {
		struct core_state *cs = &lcore_states[i];
		if (cs->is_service_core) {
			array[idx] = i;
			idx++;
		}
	}

	return count;
}

int32_t
rte_service_lcore_count_services(uint32_t lcore)
{
	if (lcore >= RTE_MAX_LCORE)
		return -EINVAL;

	struct core_state *cs = &lcore_states[lcore];
	if (!cs->is_service_core)
		return -ENOTSUP;

	return __builtin_popcountll(cs->service_mask);
}

int32_t
rte_service_start_with_defaults(void)
{
	/* create a default mapping from cores to services, then start the
	 * services to make them transparent to unaware applications.
	 */
	uint32_t i;
	int ret;
	uint32_t count = rte_service_get_count();

	int32_t lcore_iter = 0;
	uint32_t ids[RTE_MAX_LCORE] = {0};
	int32_t lcore_count = rte_service_lcore_list(ids, RTE_MAX_LCORE);

	if (lcore_count == 0)
		return -ENOTSUP;

	for (i = 0; (int)i < lcore_count; i++)
		rte_service_lcore_start(ids[i]);

	for (i = 0; i < count; i++) {
		/* do 1:1 core mapping here, with each service getting
		 * assigned a single core by default. Adding multiple services
		 * should multiplex to a single core, or 1:1 if there are the
		 * same amount of services as service-cores
		 */
		ret = rte_service_map_lcore_set(i, ids[lcore_iter], 1);
		if (ret)
			return -ENODEV;

		lcore_iter++;
		if (lcore_iter >= lcore_count)
			lcore_iter = 0;

		ret = rte_service_runstate_set(i, 1);
		if (ret)
			return -ENOEXEC;
	}

	return 0;
}

static int32_t
service_update(uint32_t sid, uint32_t lcore, uint32_t *set, uint32_t *enabled)
{
	/* validate ID, or return error value */
	if (sid >= RTE_SERVICE_NUM_MAX || !service_valid(sid) ||
	    lcore >= RTE_MAX_LCORE || !lcore_states[lcore].is_service_core)
		return -EINVAL;

	uint64_t sid_mask = UINT64_C(1) << sid;
	if (set) {
		uint64_t lcore_mapped = lcore_states[lcore].service_mask &
			sid_mask;

		if (*set && !lcore_mapped) {
			lcore_states[lcore].service_mask |= sid_mask;
			__atomic_add_fetch(&rte_services[sid].num_mapped_cores,
				1, __ATOMIC_RELAXED);
		}
		if (!*set && lcore_mapped) {
			lcore_states[lcore].service_mask &= ~(sid_mask);
			__atomic_sub_fetch(&rte_services[sid].num_mapped_cores,
				1, __ATOMIC_RELAXED);
		}
	}

	if (enabled)
		*enabled = !!(lcore_states[lcore].service_mask & (sid_mask));

	return 0;
}

int32_t
rte_service_map_lcore_set(uint32_t id, uint32_t lcore, uint32_t enabled)
{
	uint32_t on = enabled > 0;
	return service_update(id, lcore, &on, 0);
}

int32_t
rte_service_map_lcore_get(uint32_t id, uint32_t lcore)
{
	uint32_t enabled;
	int ret = service_update(id, lcore, 0, &enabled);
	if (ret == 0)
		return enabled;
	return ret;
}

static void
set_lcore_state(uint32_t lcore, int32_t state)
{
	/* mark core state in hugepage backed config */
	struct rte_config *cfg = rte_eal_get_configuration();
	cfg->lcore_role[lcore] = state;

	/* mark state in process local lcore_config */
	lcore_config[lcore].core_role = state;

	/* update per-lcore optimized state tracking */
	lcore_states[lcore].is_service_core = (state == ROLE_SERVICE);
}

int32_t
rte_service_lcore_reset_all(void)
{
	/* loop over cores, reset all to mask 0 */
	uint32_t i;
	for (i = 0; i < RTE_MAX_LCORE; i++) {
		if (lcore_states[i].is_service_core) {
			lcore_states[i].service_mask = 0;
			set_lcore_state(i, ROLE_RTE);
			/* runstate act as guard variable Use
			 * store-release memory order here to synchronize
			 * with load-acquire in runstate read functions.
			 */
			__atomic_store_n(&lcore_states[i].runstate,
				RUNSTATE_STOPPED, __ATOMIC_RELEASE);
		}
	}
	for (i = 0; i < RTE_SERVICE_NUM_MAX; i++)
		__atomic_store_n(&rte_services[i].num_mapped_cores, 0,
			__ATOMIC_RELAXED);

	return 0;
}

int32_t
rte_service_lcore_add(uint32_t lcore)
{
	if (lcore >= RTE_MAX_LCORE)
		return -EINVAL;
	if (lcore_states[lcore].is_service_core)
		return -EALREADY;

	set_lcore_state(lcore, ROLE_SERVICE);

	/* ensure that after adding a core the mask and state are defaults */
	lcore_states[lcore].service_mask = 0;
	/* Use store-release memory order here to synchronize with
	 * load-acquire in runstate read functions.
	 */
	__atomic_store_n(&lcore_states[lcore].runstate, RUNSTATE_STOPPED,
		__ATOMIC_RELEASE);

	return rte_eal_wait_lcore(lcore);
}

int32_t
rte_service_lcore_del(uint32_t lcore)
{
	if (lcore >= RTE_MAX_LCORE)
		return -EINVAL;

	struct core_state *cs = &lcore_states[lcore];
	if (!cs->is_service_core)
		return -EINVAL;

	/* runstate act as the guard variable. Use load-acquire
	 * memory order here to synchronize with store-release
	 * in runstate update functions.
	 */
	if (__atomic_load_n(&cs->runstate, __ATOMIC_ACQUIRE) !=
			RUNSTATE_STOPPED)
		return -EBUSY;

	set_lcore_state(lcore, ROLE_RTE);

	rte_smp_wmb();
	return 0;
}

int32_t
rte_service_lcore_start(uint32_t lcore)
{
	if (lcore >= RTE_MAX_LCORE)
		return -EINVAL;

	struct core_state *cs = &lcore_states[lcore];
	if (!cs->is_service_core)
		return -EINVAL;

	/* runstate act as the guard variable. Use load-acquire
	 * memory order here to synchronize with store-release
	 * in runstate update functions.
	 */
	if (__atomic_load_n(&cs->runstate, __ATOMIC_ACQUIRE) ==
			RUNSTATE_RUNNING)
		return -EALREADY;

	/* set core to run state first, and then launch otherwise it will
	 * return immediately as runstate keeps it in the service poll loop
	 */
	/* Use load-acquire memory order here to synchronize with
	 * store-release in runstate update functions.
	 */
	__atomic_store_n(&cs->runstate, RUNSTATE_RUNNING, __ATOMIC_RELEASE);

	int ret = rte_eal_remote_launch(service_runner_func, 0, lcore);
	/* returns -EBUSY if the core is already launched, 0 on success */
	return ret;
}

int32_t
rte_service_lcore_stop(uint32_t lcore)
{
	if (lcore >= RTE_MAX_LCORE)
		return -EINVAL;

	/* runstate act as the guard variable. Use load-acquire
	 * memory order here to synchronize with store-release
	 * in runstate update functions.
	 */
	if (__atomic_load_n(&lcore_states[lcore].runstate, __ATOMIC_ACQUIRE) ==
			RUNSTATE_STOPPED)
		return -EALREADY;

	uint32_t i;
	uint64_t service_mask = lcore_states[lcore].service_mask;
	for (i = 0; i < RTE_SERVICE_NUM_MAX; i++) {
		int32_t enabled = service_mask & (UINT64_C(1) << i);
		int32_t service_running = rte_service_runstate_get(i);
		int32_t only_core = (1 ==
			__atomic_load_n(&rte_services[i].num_mapped_cores,
				__ATOMIC_RELAXED));

		/* if the core is mapped, and the service is running, and this
		 * is the only core that is mapped, the service would cease to
		 * run if this core stopped, so fail instead.
		 */
		if (enabled && service_running && only_core)
			return -EBUSY;
	}

	/* Use store-release memory order here to synchronize with
	 * load-acquire in runstate read functions.
	 */
	__atomic_store_n(&lcore_states[lcore].runstate, RUNSTATE_STOPPED,
		__ATOMIC_RELEASE);

	return 0;
}

int32_t
rte_service_attr_get(uint32_t id, uint32_t attr_id, uint64_t *attr_value)
{
	struct rte_service_spec_impl *s;
	SERVICE_VALID_GET_OR_ERR_RET(id, s, -EINVAL);

	if (!attr_value)
		return -EINVAL;

	switch (attr_id) {
	case RTE_SERVICE_ATTR_CYCLES:
		*attr_value = s->cycles_spent;
		return 0;
	case RTE_SERVICE_ATTR_CALL_COUNT:
		*attr_value = s->calls;
		return 0;
	default:
		return -EINVAL;
	}
}

int32_t
rte_service_lcore_attr_get(uint32_t lcore, uint32_t attr_id,
			   uint64_t *attr_value)
{
	struct core_state *cs;

	if (lcore >= RTE_MAX_LCORE || !attr_value)
		return -EINVAL;

	cs = &lcore_states[lcore];
	if (!cs->is_service_core)
		return -ENOTSUP;

	switch (attr_id) {
	case RTE_SERVICE_LCORE_ATTR_LOOPS:
		*attr_value = cs->loops;
		return 0;
	default:
		return -EINVAL;
	}
}

int32_t
rte_service_attr_reset_all(uint32_t id)
{
	struct rte_service_spec_impl *s;
	SERVICE_VALID_GET_OR_ERR_RET(id, s, -EINVAL);

	s->cycles_spent = 0;
	s->calls = 0;
	return 0;
}

int32_t
rte_service_lcore_attr_reset_all(uint32_t lcore)
{
	struct core_state *cs;

	if (lcore >= RTE_MAX_LCORE)
		return -EINVAL;

	cs = &lcore_states[lcore];
	if (!cs->is_service_core)
		return -ENOTSUP;

	cs->loops = 0;

	return 0;
}

static void
service_dump_one(FILE *f, struct rte_service_spec_impl *s)
{
	/* avoid divide by zero */
	int calls = 1;

	if (s->calls != 0)
		calls = s->calls;
	fprintf(f, "  %s: stats %d\tcalls %"PRIu64"\tcycles %"
			PRIu64"\tavg: %"PRIu64"\n",
			s->spec.name, service_stats_enabled(s), s->calls,
			s->cycles_spent, s->cycles_spent / calls);
}

static void
service_dump_calls_per_lcore(FILE *f, uint32_t lcore)
{
	uint32_t i;
	struct core_state *cs = &lcore_states[lcore];

	fprintf(f, "%02d\t", lcore);
	for (i = 0; i < RTE_SERVICE_NUM_MAX; i++) {
		if (!service_valid(i))
			continue;
		fprintf(f, "%"PRIu64"\t", cs->calls_per_service[i]);
	}
	fprintf(f, "\n");
}

int32_t
rte_service_dump(FILE *f, uint32_t id)
{
	uint32_t i;
	int print_one = (id != UINT32_MAX);

	/* print only the specified service */
	if (print_one) {
		struct rte_service_spec_impl *s;
		SERVICE_VALID_GET_OR_ERR_RET(id, s, -EINVAL);
		fprintf(f, "Service %s Summary\n", s->spec.name);
		service_dump_one(f, s);
		return 0;
	}

	/* print all services, as UINT32_MAX was passed as id */
	fprintf(f, "Services Summary\n");
	for (i = 0; i < RTE_SERVICE_NUM_MAX; i++) {
		if (!service_valid(i))
			continue;
		service_dump_one(f, &rte_services[i]);
	}

	fprintf(f, "Service Cores Summary\n");
	for (i = 0; i < RTE_MAX_LCORE; i++) {
		if (lcore_config[i].core_role != ROLE_SERVICE)
			continue;

		service_dump_calls_per_lcore(f, i);
	}

	return 0;
}
