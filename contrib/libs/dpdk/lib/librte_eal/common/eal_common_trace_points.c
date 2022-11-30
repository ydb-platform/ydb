#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(C) 2020 Marvell International Ltd.
 */

#include <rte_trace_point_register.h>

#include <rte_eal_trace.h>

RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_void,
	lib.eal.generic.void)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_u64,
	lib.eal.generic.u64)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_u32,
	lib.eal.generic.u32)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_u16,
	lib.eal.generic.u16)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_u8,
	lib.eal.generic.u8)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_i64,
	lib.eal.generic.i64)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_i32,
	lib.eal.generic.i32)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_i16,
	lib.eal.generic.i16)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_i8,
	lib.eal.generic.i8)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_int,
	lib.eal.generic.int)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_long,
	lib.eal.generic.long)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_float,
	lib.eal.generic.float)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_double,
	lib.eal.generic.double)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_ptr,
	lib.eal.generic.ptr)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_str,
	lib.eal.generic.string)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_size_t,
	lib.eal.generic.size_t)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_generic_func,
	lib.eal.generic.func)

RTE_TRACE_POINT_REGISTER(rte_eal_trace_alarm_set,
	lib.eal.alarm.set)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_alarm_cancel,
	lib.eal.alarm.cancel)

RTE_TRACE_POINT_REGISTER(rte_eal_trace_mem_zmalloc,
	lib.eal.mem.zmalloc)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_mem_malloc,
	lib.eal.mem.malloc)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_mem_realloc,
	lib.eal.mem.realloc)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_mem_free,
	lib.eal.mem.free)

RTE_TRACE_POINT_REGISTER(rte_eal_trace_memzone_reserve,
	lib.eal.memzone.reserve)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_memzone_lookup,
	lib.eal.memzone.lookup)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_memzone_free,
	lib.eal.memzone.free)

RTE_TRACE_POINT_REGISTER(rte_eal_trace_thread_remote_launch,
	lib.eal.thread.remote.launch)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_thread_lcore_ready,
	lib.eal.thread.lcore.ready)

RTE_TRACE_POINT_REGISTER(rte_eal_trace_intr_callback_register,
	lib.eal.intr.register)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_intr_callback_unregister,
	lib.eal.intr.unregister)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_intr_enable,
	lib.eal.intr.enable)
RTE_TRACE_POINT_REGISTER(rte_eal_trace_intr_disable,
	lib.eal.intr.disable)
